//! Static analysis of SQL statements for authorization, lineage and policy
//! rewriting. Everything here is pure: it takes SQL text plus catalog metadata
//! and produces the privileges a statement needs, the tables it reads and
//! writes, column-level lineage, and (optionally) a rewritten statement with
//! row filters, column masks and SQL UDFs inlined.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    self, visit_expressions_mut, visit_relations, visit_relations_mut, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::{DatabricksDialect, GenericDialect};
use sqlparser::parser::Parser;

use super::privileges::Securable;

/// What the statement fundamentally does; drives both privilege checks and
/// the `statement_type` reported in query history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StmtKind {
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    CreateTable,
    CreateView,
    CreateSchema,
    Drop,
    Alter,
    Truncate,
    Describe,
    Show,
    Use,
    Set,
    Explain,
    #[default]
    Other,
}

impl StmtKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Merge => "MERGE",
            Self::CreateTable | Self::CreateView | Self::CreateSchema => "CREATE",
            Self::Drop => "DROP",
            Self::Alter => "ALTER",
            Self::Truncate => "TRUNCATE",
            Self::Describe => "DESCRIBE",
            Self::Show => "SHOW",
            Self::Use => "USE",
            Self::Set => "SET",
            Self::Explain => "EXPLAIN",
            Self::Other => "OTHER",
        }
    }

    pub fn is_write(self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete | Self::Merge | Self::Truncate)
    }

    /// Statement shapes the policy / session-function rewriter round-trips
    /// through the parser safely.
    pub fn is_rewritable(self) -> bool {
        matches!(self, Self::Select | Self::Insert | Self::Update | Self::Delete | Self::Merge | Self::CreateTable | Self::CreateView | Self::Explain)
    }
}

/// One column-level lineage edge: `target.column <- source_table.column`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnEdge {
    pub source_table: String,
    pub source_column: String,
    pub target_column: String,
}

#[derive(Debug, Clone, Default)]
pub struct Analysis {
    pub kind: StmtKind,
    /// Fully qualified tables read (`SELECT` needed).
    pub reads: BTreeSet<String>,
    /// Fully qualified tables written (`MODIFY` needed).
    pub writes: BTreeSet<String>,
    /// Objects created: schema (needs `CREATE_TABLE`/`CREATE_VIEW` on it) and name.
    pub creates: Vec<(Securable, String)>,
    /// Objects dropped or altered (ownership needed).
    pub owned: Vec<(Securable, String)>,
    /// Path-based access (`delta.\`s3://...\``, `parquet.\`...\``): needs
    /// `READ_FILES` on an external location covering the path.
    pub paths: BTreeSet<String>,
    /// `LOCATION` of a `CREATE TABLE`, if any.
    pub location: Option<String>,
    /// Target table of a write / CTAS / view (for lineage).
    pub target: Option<String>,
    pub column_edges: BTreeSet<ColumnEdge>,
    /// Table is being created with `CREATE OR REPLACE`.
    pub replace: bool,
    pub if_not_exists: bool,
    /// Whether the statement was parsed (false = regex fallback).
    pub parsed: bool,
}

/// Row filter / column masks for one table, all references fully qualified.
#[derive(Debug, Clone, Default)]
pub struct TablePolicy {
    pub full_name: String,
    /// All column names, in order (needed to project masked columns).
    pub columns: Vec<String>,
    /// `(function_full_name, input_columns)`.
    pub row_filter: Option<(String, Vec<String>)>,
    /// column -> `(function_full_name, using_columns)`.
    pub masks: HashMap<String, (String, Vec<String>)>,
}

/// A SQL-bodied UDF: `CREATE FUNCTION f(a INT, b STRING) RETURNS ... RETURN <expr>`.
#[derive(Debug, Clone)]
pub struct SqlFunction {
    pub full_name: String,
    pub params: Vec<String>,
    pub body: String,
}

/// Session facts substituted for `current_user()` & co.
#[derive(Debug, Clone, Default)]
pub struct SessionFacts {
    pub user: String,
    pub groups: Vec<String>,
}

pub fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    let first = Parser::parse_sql(&DatabricksDialect {}, sql);
    match first {
        Ok(s) => Ok(s),
        Err(e1) => Parser::parse_sql(&GenericDialect {}, sql).map_err(|_| e1.to_string()),
    }
}

pub fn ident_value(p: &ObjectNamePart) -> String {
    match p {
        ObjectNamePart::Identifier(i) => i.value.clone(),
        ObjectNamePart::Function(f) => f.name.value.clone(),
    }
}

fn name_parts(n: &ObjectName) -> Vec<String> {
    n.0.iter().map(ident_value).collect()
}

/// Path-based table reference: `delta.\`/x\``, `parquet.\`s3://b/k\``.
pub fn path_ref(n: &ObjectName) -> Option<(String, String)> {
    if n.0.len() != 2 {
        return None;
    }
    let fmt = ident_value(&n.0[0]).to_ascii_lowercase();
    if !matches!(fmt.as_str(), "delta" | "parquet" | "csv" | "json") {
        return None;
    }
    let path = ident_value(&n.0[1]);
    (path.contains('/') || path.contains(':')).then_some((fmt, path))
}

/// Resolve a 1/2/3-part name against the session defaults.
pub fn resolve(parts: &[String], default_catalog: &str, default_schema: &str) -> String {
    match parts.len() {
        0 => String::new(),
        1 => format!("{default_catalog}.{default_schema}.{}", parts[0]),
        2 => format!("{default_catalog}.{}.{}", parts[0], parts[1]),
        _ => parts.join("."),
    }
}

pub fn resolve_name(n: &ObjectName, default_catalog: &str, default_schema: &str) -> String {
    resolve(&name_parts(n), default_catalog, default_schema)
}

/// Statements the SQL guard lets through untouched (session/introspection).
fn passthrough_kind(s: &Statement) -> Option<StmtKind> {
    Some(match s {
        Statement::Use(_) => StmtKind::Use,
        Statement::Set(_) => StmtKind::Set,
        Statement::ShowTables { .. } | Statement::ShowColumns { .. } | Statement::ShowSchemas { .. } | Statement::ShowDatabases { .. } | Statement::ShowFunctions { .. } | Statement::ShowVariable { .. } | Statement::ShowVariables { .. } | Statement::ShowViews { .. } | Statement::ShowCreate { .. } | Statement::ShowObjects(_) | Statement::ShowStatus { .. } => StmtKind::Show,
        Statement::ExplainTable { .. } => StmtKind::Describe,
        _ => return None,
    })
}

/// CTE names visible in a query (so they are not mistaken for tables).
fn cte_names(q: &Query) -> HashSet<String> {
    let mut out = HashSet::new();
    if let Some(w) = &q.with {
        for c in &w.cte_tables {
            out.insert(c.alias.name.value.to_ascii_lowercase());
        }
    }
    out
}

struct Ctx<'a> {
    cat: &'a str,
    sch: &'a str,
}

impl Ctx<'_> {
    fn res(&self, n: &ObjectName) -> String {
        resolve_name(n, self.cat, self.sch)
    }

    /// Every relation referenced in `node`, minus CTEs and path refs.
    fn relations<V: ast::Visit>(&self, node: &V, ctes: &HashSet<String>, an: &mut Analysis) -> Vec<String> {
        let mut names = vec![];
        let _: ControlFlow<()> = visit_relations(node, |rel| {
            if let Some((_, p)) = path_ref(rel) {
                an.paths.insert(p);
                return ControlFlow::Continue(());
            }
            let parts = name_parts(rel);
            if parts.len() == 1 && ctes.contains(&parts[0].to_ascii_lowercase()) {
                return ControlFlow::Continue(());
            }
            names.push(self.res(rel));
            ControlFlow::Continue(())
        });
        names
    }

    fn reads_of_query(&self, q: &Query, an: &mut Analysis) {
        let ctes = cte_names(q);
        for r in self.relations(q, &ctes, an) {
            an.reads.insert(r);
        }
    }
}

/// Analyse one statement.
pub fn analyze_statement(stmt: &Statement, default_catalog: &str, default_schema: &str) -> Analysis {
    let cx = Ctx { cat: default_catalog, sch: default_schema };
    let mut an = Analysis { parsed: true, ..Default::default() };
    if let Some(k) = passthrough_kind(stmt) {
        an.kind = k;
        return an;
    }
    match stmt {
        Statement::Query(q) => {
            an.kind = StmtKind::Select;
            cx.reads_of_query(q, &mut an);
        }
        Statement::Explain { statement, .. } => {
            an = analyze_statement(statement, default_catalog, default_schema);
            an.kind = StmtKind::Explain;
        }
        Statement::Insert(ins) => {
            an.kind = StmtKind::Insert;
            let target = match &ins.table {
                ast::TableObject::TableName(n) => cx.res(n),
                ast::TableObject::TableFunction(f) => f.name.to_string(),
            };
            an.writes.insert(target.clone());
            an.target = Some(target.clone());
            if let Some(src) = &ins.source {
                cx.reads_of_query(src, &mut an);
                let cols: Vec<String> = ins.columns.iter().map(|c| c.value.clone()).collect();
                column_lineage(&cx, src, (!cols.is_empty()).then_some(&cols), &mut an);
            }
        }
        Statement::Update(u) => {
            an.kind = StmtKind::Update;
            let ctes = HashSet::new();
            for r in cx.relations(&u.table, &ctes, &mut an) {
                an.writes.insert(r.clone());
                an.target = Some(r);
            }
            if let Some(from) = &u.from {
                match from {
                    ast::UpdateTableFromKind::BeforeSet(t) | ast::UpdateTableFromKind::AfterSet(t) => {
                        for r in cx.relations(t, &ctes, &mut an) {
                            an.reads.insert(r);
                        }
                    }
                }
            }
            if let Some(sel) = &u.selection {
                for r in cx.relations(sel, &ctes, &mut an) {
                    an.reads.insert(r);
                }
            }
        }
        Statement::Delete(d) => {
            an.kind = StmtKind::Delete;
            let ctes = HashSet::new();
            let from = match &d.from {
                ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
            };
            for r in cx.relations(from, &ctes, &mut an) {
                an.writes.insert(r.clone());
                an.target = Some(r);
            }
            if let Some(u) = &d.using {
                for r in cx.relations(u, &ctes, &mut an) {
                    an.reads.insert(r);
                }
            }
            if let Some(sel) = &d.selection {
                for r in cx.relations(sel, &ctes, &mut an) {
                    an.reads.insert(r);
                }
            }
        }
        Statement::Merge(m) => {
            an.kind = StmtKind::Merge;
            let ctes = HashSet::new();
            for r in cx.relations(&m.table, &ctes, &mut an) {
                an.writes.insert(r.clone());
                an.target = Some(r);
            }
            for r in cx.relations(&m.source, &ctes, &mut an) {
                an.reads.insert(r);
            }
        }
        Statement::Truncate(t) => {
            an.kind = StmtKind::Truncate;
            for t in &t.table_names {
                let n = cx.res(&t.name);
                an.writes.insert(n.clone());
                an.target = Some(n);
            }
        }
        Statement::CreateTable(ct) => {
            an.kind = StmtKind::CreateTable;
            let name = cx.res(&ct.name);
            an.replace = ct.or_replace;
            an.if_not_exists = ct.if_not_exists;
            an.location = ct.location.clone();
            an.creates.push((Securable::Table, name.clone()));
            an.target = Some(name);
            if let Some(q) = &ct.query {
                cx.reads_of_query(q, &mut an);
                column_lineage(&cx, q, None, &mut an);
            }
        }
        Statement::CreateView(cv) => {
            an.kind = StmtKind::CreateView;
            let name = cx.res(&cv.name);
            an.replace = cv.or_replace;
            an.if_not_exists = cv.if_not_exists;
            an.creates.push((Securable::Table, name.clone()));
            an.target = Some(name);
            cx.reads_of_query(&cv.query, &mut an);
            let cols: Vec<String> = cv.columns.iter().map(|c| c.name.value.clone()).collect();
            column_lineage(&cx, &cv.query, (!cols.is_empty()).then_some(&cols), &mut an);
        }
        Statement::CreateSchema { schema_name, if_not_exists, .. } => {
            an.kind = StmtKind::CreateSchema;
            an.if_not_exists = *if_not_exists;
            let n = match schema_name {
                ast::SchemaName::Simple(n) => name_parts(n),
                ast::SchemaName::UnnamedAuthorization(i) => vec![i.value.clone()],
                ast::SchemaName::NamedAuthorization(n, _) => name_parts(n),
            };
            let full = if n.len() >= 2 { n.join(".") } else { format!("{default_catalog}.{}", n.join(".")) };
            an.creates.push((Securable::Schema, full));
        }
        Statement::Drop { object_type, names, .. } => {
            an.kind = StmtKind::Drop;
            for n in names {
                match object_type {
                    ast::ObjectType::Schema | ast::ObjectType::Database => {
                        let p = name_parts(n);
                        let full = if p.len() >= 2 { p.join(".") } else { format!("{default_catalog}.{}", p.join(".")) };
                        an.owned.push((Securable::Schema, full));
                    }
                    _ => an.owned.push((Securable::Table, cx.res(n))),
                }
            }
        }
        Statement::DropFunction(d) => {
            an.kind = StmtKind::Drop;
            for f in &d.func_desc {
                an.owned.push((Securable::Function, cx.res(&f.name)));
            }
        }
        Statement::AlterTable(at) => {
            an.kind = StmtKind::Alter;
            let n = cx.res(&at.name);
            an.owned.push((Securable::Table, n.clone()));
            an.target = Some(n);
        }
        Statement::AlterView { name, query, .. } => {
            an.kind = StmtKind::Alter;
            let n = cx.res(name);
            an.owned.push((Securable::Table, n.clone()));
            an.target = Some(n);
            cx.reads_of_query(query, &mut an);
        }
        Statement::AlterSchema(a) => {
            an.kind = StmtKind::Alter;
            let p = name_parts(&a.name);
            let full = if p.len() >= 2 { p.join(".") } else { format!("{default_catalog}.{}", p.join(".")) };
            an.owned.push((Securable::Schema, full));
        }
        Statement::RenameTable(rs) => {
            an.kind = StmtKind::Alter;
            for r in rs {
                an.owned.push((Securable::Table, cx.res(&r.old_name)));
            }
        }
        Statement::CreateFunction(cf) => {
            an.kind = StmtKind::CreateTable;
            an.replace = cf.or_replace;
            an.creates.push((Securable::Function, cx.res(&cf.name)));
        }
        Statement::Analyze(a) => {
            an.kind = StmtKind::Other;
            if let Some(t) = &a.table_name {
                an.reads.insert(cx.res(t));
            }
        }
        Statement::Cache { table_name, query, .. } => {
            an.kind = StmtKind::Other;
            an.reads.insert(cx.res(table_name));
            if let Some(q) = query {
                cx.reads_of_query(q, &mut an);
            }
        }
        Statement::OptimizeTable { name, .. } => {
            an.kind = StmtKind::Other;
            let n = cx.res(name);
            an.writes.insert(n.clone());
            an.target = Some(n);
        }
        _ => {
            an.kind = StmtKind::Other;
            let ctes = HashSet::new();
            for r in cx.relations(stmt, &ctes, &mut an) {
                an.reads.insert(r);
            }
        }
    }
    an
}

/// Best-effort extraction of table references when sqlparser cannot parse
/// the statement (engine-specific syntax). Anything after FROM/JOIN/INTO/
/// UPDATE/TABLE/MERGE INTO is treated as a relation; the statement is
/// classified as a write if it starts with a DML/DDL keyword.
pub fn analyze_fallback(sql: &str, default_catalog: &str, default_schema: &str) -> Analysis {
    let mut an = Analysis::default();
    let upper = sql.trim_start().to_ascii_uppercase();
    let first = upper.split_whitespace().next().unwrap_or("");
    an.kind = match first {
        "SELECT" | "WITH" | "VALUES" => StmtKind::Select,
        "INSERT" => StmtKind::Insert,
        "UPDATE" => StmtKind::Update,
        "DELETE" => StmtKind::Delete,
        "MERGE" => StmtKind::Merge,
        "CREATE" => StmtKind::CreateTable,
        "DROP" => StmtKind::Drop,
        "ALTER" => StmtKind::Alter,
        "TRUNCATE" => StmtKind::Truncate,
        "DESCRIBE" | "DESC" => StmtKind::Describe,
        "SHOW" => StmtKind::Show,
        "USE" => StmtKind::Use,
        "SET" => StmtKind::Set,
        "EXPLAIN" => StmtKind::Explain,
        _ => StmtKind::Other,
    };
    let toks: Vec<&str> = sql.split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ';').filter(|t| !t.is_empty()).collect();
    let mut i = 0;
    while i + 1 < toks.len() {
        let kw = toks[i].to_ascii_uppercase();
        let next = toks[i + 1].trim_matches('`').trim_matches('"');
        let is_ident = !next.is_empty() && next.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-' || c == '/' || c == ':') && !next.eq_ignore_ascii_case("select") && !next.eq_ignore_ascii_case("table") && !next.eq_ignore_ascii_case("if");
        if is_ident {
            let parts: Vec<String> = next.split('.').map(String::from).collect();
            let full = resolve(&parts, default_catalog, default_schema);
            match kw.as_str() {
                "FROM" | "JOIN" | "USING" => {
                    an.reads.insert(full);
                }
                "INTO" | "UPDATE" if an.kind.is_write() => {
                    an.writes.insert(full.clone());
                    an.target = Some(full);
                }
                "TABLE" | "VIEW" => match an.kind {
                    StmtKind::Drop | StmtKind::Alter => an.owned.push((Securable::Table, full)),
                    StmtKind::CreateTable => {
                        an.creates.push((Securable::Table, full.clone()));
                        an.target = Some(full);
                    }
                    StmtKind::Describe | StmtKind::Select | StmtKind::Other => {
                        an.reads.insert(full);
                    }
                    _ => {}
                },
                "SCHEMA" | "DATABASE" => {
                    let full = if parts.len() >= 2 { parts.join(".") } else { format!("{default_catalog}.{}", parts[0]) };
                    match an.kind {
                        StmtKind::Drop | StmtKind::Alter => an.owned.push((Securable::Schema, full)),
                        StmtKind::CreateTable => {
                            an.kind = StmtKind::CreateSchema;
                            an.creates.push((Securable::Schema, full));
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    if an.kind == StmtKind::Describe && an.reads.is_empty() {
        if let Some(t) = toks.get(1) {
            let t = if t.eq_ignore_ascii_case("table") || t.eq_ignore_ascii_case("extended") { toks.get(2) } else { Some(t) };
            if let Some(t) = t {
                let parts: Vec<String> = t.trim_matches('`').split('.').map(String::from).collect();
                an.reads.insert(resolve(&parts, default_catalog, default_schema));
            }
        }
    }
    an
}

/// Parse and analyse; falls back to token scanning on parse failure.
pub fn analyze(sql: &str, default_catalog: &str, default_schema: &str) -> (Option<Vec<Statement>>, Analysis) {
    match parse(sql) {
        Ok(stmts) if stmts.len() == 1 => {
            let an = analyze_statement(&stmts[0], default_catalog, default_schema);
            (Some(stmts), an)
        }
        Ok(stmts) if stmts.is_empty() => (Some(stmts), Analysis { kind: StmtKind::Other, parsed: true, ..Default::default() }),
        Ok(stmts) => {
            let mut merged = Analysis { parsed: true, ..Default::default() };
            for s in &stmts {
                let a = analyze_statement(s, default_catalog, default_schema);
                merged.kind = a.kind;
                merged.reads.extend(a.reads);
                merged.writes.extend(a.writes);
                merged.creates.extend(a.creates);
                merged.owned.extend(a.owned);
                merged.paths.extend(a.paths);
                merged.target = a.target.or(merged.target);
            }
            (Some(stmts), merged)
        }
        Err(_) => (None, analyze_fallback(sql, default_catalog, default_schema)),
    }
}

// ---------------------------------------------------------------------------
// Column lineage
// ---------------------------------------------------------------------------

/// Map of alias/table-name -> fully qualified table for the FROM clause.
fn alias_map(cx: &Ctx, from: &[TableWithJoins], ctes: &HashSet<String>) -> (HashMap<String, String>, Vec<String>) {
    let mut m = HashMap::new();
    let mut order = vec![];
    let mut add = |tf: &TableFactor| {
        if let TableFactor::Table { name, alias, .. } = tf {
            if path_ref(name).is_some() {
                return;
            }
            let parts = name_parts(name);
            if parts.len() == 1 && ctes.contains(&parts[0].to_ascii_lowercase()) {
                return;
            }
            let full = cx.res(name);
            if let Some(a) = alias {
                m.insert(a.name.value.to_ascii_lowercase(), full.clone());
            }
            m.insert(parts.last().cloned().unwrap_or_default().to_ascii_lowercase(), full.clone());
            m.insert(parts.join(".").to_ascii_lowercase(), full.clone());
            order.push(full);
        }
    };
    for t in from {
        add(&t.relation);
        for j in &t.joins {
            add(&j.relation);
        }
    }
    (m, order)
}

fn column_refs(e: &Expr, out: &mut Vec<(Option<String>, String)>) {
    match e {
        Expr::Identifier(i) => out.push((None, i.value.clone())),
        Expr::CompoundIdentifier(ids) if ids.len() >= 2 => {
            let col = ids[ids.len() - 1].value.clone();
            let qual = ids[..ids.len() - 1].iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
            out.push((Some(qual.to_ascii_lowercase()), col));
        }
        Expr::Subquery(_) => {}
        other => {
            let mut sub = vec![];
            // Collect identifiers of nested expressions via string-free traversal.
            let _: ControlFlow<()> = ast::visit_expressions(other, |x| {
                match x {
                    Expr::Identifier(i) => sub.push((None, i.value.clone())),
                    Expr::CompoundIdentifier(ids) if ids.len() >= 2 => {
                        let col = ids[ids.len() - 1].value.clone();
                        let qual = ids[..ids.len() - 1].iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                        sub.push((Some(qual.to_ascii_lowercase()), col));
                    }
                    _ => {}
                }
                ControlFlow::Continue(())
            });
            out.extend(sub);
        }
    }
}

/// Column lineage from the top-level SELECT of `q` into `target_columns`
/// (by position when given, otherwise by output name).
fn column_lineage(cx: &Ctx, q: &Query, target_columns: Option<&Vec<String>>, an: &mut Analysis) {
    let ctes = cte_names(q);
    let SetExpr::Select(sel) = q.body.as_ref() else { return };
    let (aliases, order) = alias_map(cx, &sel.from, &ctes);
    for (pos, item) in sel.projection.iter().enumerate() {
        let (expr, out_name) = match item {
            SelectItem::UnnamedExpr(e) => (e, default_output_name(e)),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => continue,
        };
        let target_col = match target_columns.and_then(|c| c.get(pos)) {
            Some(c) => c.clone(),
            None => match out_name {
                Some(n) => n,
                None => continue,
            },
        };
        let mut refs = vec![];
        column_refs(expr, &mut refs);
        for (qual, col) in refs {
            let src = match qual {
                Some(q) => aliases.get(&q).cloned(),
                None if order.len() == 1 => order.first().cloned(),
                None => None,
            };
            if let Some(src) = src {
                an.column_edges.insert(ColumnEdge { source_table: src, source_column: col, target_column: target_col.clone() });
            }
        }
    }
}

fn default_output_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Policy rewriting: row filters, column masks, SQL UDF inlining
// ---------------------------------------------------------------------------

fn ident(s: &str) -> Ident {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !s.is_empty() && !s.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        Ident::new(s)
    } else {
        Ident::with_quote('"', s)
    }
}

fn object_name(full: &str) -> ObjectName {
    ObjectName(full.split('.').map(|p| ObjectNamePart::Identifier(ident(p))).collect())
}

fn call(func: &str, args: &[String]) -> Expr {
    Expr::Function(ast::Function {
        name: object_name(func),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args.iter().map(|a| FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident(a))))).collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// Wrap `t` as `(SELECT masked_cols FROM t WHERE filter) AS alias`.
fn policy_subquery(name: &ObjectName, alias: Option<TableAlias>, pol: &TablePolicy) -> TableFactor {
    let projection: Vec<SelectItem> = if pol.masks.is_empty() {
        vec![SelectItem::Wildcard(ast::WildcardAdditionalOptions::default())]
    } else {
        pol.columns
            .iter()
            .map(|c| match pol.masks.get(c) {
                Some((f, using)) => {
                    let mut args = vec![c.clone()];
                    args.extend(using.iter().cloned());
                    SelectItem::ExprWithAlias { expr: call(f, &args), alias: ident(c) }
                }
                None => SelectItem::UnnamedExpr(Expr::Identifier(ident(c))),
            })
            .collect()
    };
    let selection = pol.row_filter.as_ref().map(|(f, cols)| call(f, cols));
    let inner = Select {
        select_token: ast::helpers::attached_token::AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection,
        exclude: None,
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: name.clone(),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                with_ordinality: false,
                partitions: vec![],
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        prewhere: None,
        selection,
        group_by: ast::GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: vec![],
        flavor: ast::SelectFlavor::Standard,
        optimizer_hint: None,
        select_modifiers: None,
    };
    let query = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(inner))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    };
    let alias = alias.or_else(|| {
        let last = name.0.last().map(ident_value).unwrap_or_default();
        Some(TableAlias { explicit: true, name: ident(&last), columns: vec![] })
    });
    TableFactor::Derived { lateral: false, subquery: Box::new(query), alias, sample: None }
}

fn str_lit(s: &str) -> Expr {
    Expr::Value(ValueWithSpan { value: SqlValue::SingleQuotedString(s.to_string()), span: sqlparser::tokenizer::Span::empty() })
}

fn bool_lit(b: bool) -> Expr {
    Expr::Value(ValueWithSpan { value: SqlValue::Boolean(b), span: sqlparser::tokenizer::Span::empty() })
}

fn fn_args(f: &ast::Function) -> Option<Vec<Expr>> {
    match &f.args {
        FunctionArguments::List(l) => l
            .args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) | FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } | FunctionArg::ExprNamed { arg: FunctionArgExpr::Expr(e), .. } => Some(e.clone()),
                _ => None,
            })
            .collect(),
        FunctionArguments::None => Some(vec![]),
        FunctionArguments::Subquery(_) => None,
    }
}

fn expr_str_arg(e: &Expr) -> Option<String> {
    match e {
        Expr::Value(ValueWithSpan { value: SqlValue::SingleQuotedString(s), .. }) | Expr::Value(ValueWithSpan { value: SqlValue::DoubleQuotedString(s), .. }) => Some(s.clone()),
        _ => None,
    }
}

/// Substitute `current_user()`, `session_user()`, `is_account_group_member(g)`,
/// `is_member(g)`, `current_catalog()`, `current_schema()`.
fn substitute_session_fn(e: &mut Expr, facts: &SessionFacts, cat: &str, sch: &str) -> bool {
    let Expr::Function(f) = e else { return false };
    let fname = name_parts(&f.name).join(".").to_ascii_lowercase();
    let args = fn_args(f);
    let replacement = match (fname.as_str(), args) {
        ("current_user" | "session_user" | "user", Some(a)) if a.is_empty() => str_lit(&facts.user),
        ("current_catalog", Some(a)) if a.is_empty() => str_lit(cat),
        ("current_schema" | "current_database", Some(a)) if a.is_empty() => str_lit(sch),
        ("is_account_group_member" | "is_member", Some(a)) if a.len() == 1 => match expr_str_arg(&a[0]) {
            Some(g) => bool_lit(facts.groups.iter().any(|x| x.eq_ignore_ascii_case(&g)) || g.eq_ignore_ascii_case("account users") || g.eq_ignore_ascii_case("users")),
            None => return false,
        },
        _ => return false,
    };
    *e = replacement;
    true
}

/// Inline a SQL UDF call: parse its body and substitute parameters.
fn inline_udf(e: &mut Expr, functions: &HashMap<String, SqlFunction>, cat: &str, sch: &str) -> bool {
    let Expr::Function(f) = e else { return false };
    let full = resolve_name(&f.name, cat, sch).to_ascii_lowercase();
    let Some(udf) = functions.get(&full) else { return false };
    let Some(args) = fn_args(f) else { return false };
    if args.len() != udf.params.len() {
        return false;
    }
    let dialect = GenericDialect {};
    let Ok(mut body) = Parser::new(&dialect).try_with_sql(&udf.body).and_then(|mut p| p.parse_expr()) else { return false };
    let params: HashMap<String, Expr> = udf.params.iter().map(|p| p.to_ascii_lowercase()).zip(args).collect();
    let _: ControlFlow<()> = visit_expressions_mut(&mut body, |x| {
        if let Expr::Identifier(i) = x {
            if let Some(v) = params.get(&i.value.to_ascii_lowercase()) {
                *x = Expr::Nested(Box::new(v.clone()));
            }
        }
        ControlFlow::Continue(())
    });
    *e = Expr::Nested(Box::new(body));
    true
}

/// Apply row filters / column masks to every read of a governed table and
/// inline SQL UDFs and session functions. Returns the rewritten SQL if
/// anything changed.
pub fn rewrite(stmts: &[Statement], policies: &HashMap<String, TablePolicy>, functions: &HashMap<String, SqlFunction>, facts: &SessionFacts, default_catalog: &str, default_schema: &str) -> Option<String> {
    let mut stmts = stmts.to_vec();
    let mut changed = false;
    for stmt in stmts.iter_mut() {
        // Never rewrite the write target itself; only reads.
        let protect: HashSet<String> = match stmt {
            Statement::Insert(i) => match &i.table {
                ast::TableObject::TableName(n) => HashSet::from([resolve_name(n, default_catalog, default_schema)]),
                _ => HashSet::new(),
            },
            Statement::Update(u) => HashSet::from([resolve_name(match &u.table.relation {
                TableFactor::Table { name, .. } => name,
                _ => return None,
            }, default_catalog, default_schema)]),
            Statement::Delete(_) | Statement::Merge(_) | Statement::AlterTable(_) | Statement::Drop { .. } | Statement::Truncate(_) => {
                // Policies cannot be applied to DML targets; the guard requires
                // MODIFY which policy-restricted readers do not have.
                continue;
            }
            _ => HashSet::new(),
        };
        if !policies.is_empty() {
            let _: ControlFlow<()> = visit_relations_mut(stmt, |_| ControlFlow::Continue(()));
            rewrite_table_factors(stmt, &mut |tf| {
                if let TableFactor::Table { name, alias, args: None, .. } = tf {
                    let full = resolve_name(name, default_catalog, default_schema);
                    if protect.contains(&full) {
                        return;
                    }
                    if let Some(pol) = policies.get(&full.to_ascii_lowercase()) {
                        *tf = policy_subquery(name, alias.clone(), pol);
                        changed = true;
                    }
                }
            });
        }
        // Expand UDFs iteratively (bodies may call other UDFs), then session fns.
        for _ in 0..5 {
            let mut any = false;
            let _: ControlFlow<()> = visit_expressions_mut(stmt, |e| {
                if inline_udf(e, functions, default_catalog, default_schema) {
                    any = true;
                }
                ControlFlow::Continue(())
            });
            if !any {
                break;
            }
            changed = true;
        }
        let _: ControlFlow<()> = visit_expressions_mut(stmt, |e| {
            if substitute_session_fn(e, facts, default_catalog, default_schema) {
                changed = true;
            }
            ControlFlow::Continue(())
        });
    }
    changed.then(|| stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(";\n"))
}

/// Visit every `TableFactor` in a statement (FROM clauses, joins, subqueries,
/// CTEs, DML sources) and let `f` replace it.
fn rewrite_table_factors(stmt: &mut Statement, f: &mut dyn FnMut(&mut TableFactor)) {
    fn in_query(q: &mut Query, f: &mut dyn FnMut(&mut TableFactor)) {
        if let Some(w) = &mut q.with {
            for c in &mut w.cte_tables {
                in_query(&mut c.query, f);
            }
        }
        in_setexpr(&mut q.body, f);
    }
    fn in_setexpr(s: &mut SetExpr, f: &mut dyn FnMut(&mut TableFactor)) {
        match s {
            SetExpr::Select(sel) => {
                for t in &mut sel.from {
                    in_twj(t, f);
                }
                if let Some(e) = &mut sel.selection {
                    in_expr(e, f);
                }
                for p in &mut sel.projection {
                    match p {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => in_expr(e, f),
                        _ => {}
                    }
                }
                if let Some(h) = &mut sel.having {
                    in_expr(h, f);
                }
            }
            SetExpr::Query(q) => in_query(q, f),
            SetExpr::SetOperation { left, right, .. } => {
                in_setexpr(left, f);
                in_setexpr(right, f);
            }
            SetExpr::Insert(st) | SetExpr::Update(st) | SetExpr::Delete(st) | SetExpr::Merge(st) => rewrite_table_factors(st, f),
            SetExpr::Values(_) | SetExpr::Table(_) => {}
        }
    }
    fn in_twj(t: &mut TableWithJoins, f: &mut dyn FnMut(&mut TableFactor)) {
        in_tf(&mut t.relation, f);
        for j in &mut t.joins {
            in_tf(&mut j.relation, f);
        }
    }
    fn in_tf(tf: &mut TableFactor, f: &mut dyn FnMut(&mut TableFactor)) {
        match tf {
            TableFactor::Derived { subquery, .. } => in_query(subquery, f),
            TableFactor::NestedJoin { table_with_joins, .. } => in_twj(table_with_joins, f),
            TableFactor::Table { .. } => f(tf),
            _ => {}
        }
    }
    fn in_expr(e: &mut Expr, f: &mut dyn FnMut(&mut TableFactor)) {
        let _: ControlFlow<()> = visit_expressions_mut(e, |x| {
            match x {
                Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => in_query(q, f),
                Expr::InSubquery { subquery, .. } => in_query(subquery, f),
                _ => {}
            }
            ControlFlow::Continue(())
        });
    }
    match stmt {
        Statement::Query(q) => in_query(q, f),
        Statement::Insert(i) => {
            if let Some(src) = &mut i.source {
                in_query(src, f);
            }
        }
        Statement::CreateTable(ct) => {
            if let Some(q) = &mut ct.query {
                in_query(q, f);
            }
        }
        Statement::CreateView(cv) => in_query(&mut cv.query, f),
        Statement::Update(u) => {
            if let Some(from) = &mut u.from {
                match from {
                    ast::UpdateTableFromKind::BeforeSet(t) | ast::UpdateTableFromKind::AfterSet(t) => {
                        for t in t {
                            in_twj(t, f);
                        }
                    }
                }
            }
            if let Some(sel) = &mut u.selection {
                in_expr(sel, f);
            }
        }
        Statement::Explain { statement, .. } => rewrite_table_factors(statement, f),
        _ => {}
    }
}

/// `SQL UDF` definition parsed from `CREATE FUNCTION ... RETURN expr`.
#[allow(clippy::type_complexity)]
pub fn parse_sql_function(stmt: &Statement, default_catalog: &str, default_schema: &str) -> Option<(SqlFunction, String, Vec<(String, String)>, bool)> {
    let Statement::CreateFunction(cf) = stmt else { return None };
    let params: Vec<(String, String)> = cf.args.as_ref().map(|a| a.iter().map(|p| (p.name.as_ref().map(|n| n.value.clone()).unwrap_or_default(), p.data_type.to_string())).collect()).unwrap_or_default();
    let body = match &cf.function_body {
        Some(ast::CreateFunctionBody::Return(e)) => e.to_string(),
        Some(ast::CreateFunctionBody::AsAfterOptions(e)) | Some(ast::CreateFunctionBody::AsBeforeOptions { body: e, .. }) => match e {
            Expr::Value(ValueWithSpan { value: SqlValue::SingleQuotedString(s), .. }) | Expr::Value(ValueWithSpan { value: SqlValue::DollarQuotedString(ast::DollarQuotedString { value: s, .. }), .. }) => s.clone(),
            other => other.to_string(),
        },
        _ => return None,
    };
    let return_type = cf.return_type.as_ref().map(|t| t.to_string()).unwrap_or_else(|| "STRING".into());
    Some((SqlFunction { full_name: resolve_name(&cf.name, default_catalog, default_schema), params: params.iter().map(|(n, _)| n.clone()).collect(), body }, return_type, params, cf.or_replace))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn an(sql: &str) -> Analysis {
        analyze(sql, "main", "default").1
    }

    #[test]
    fn select_reads() {
        let a = an("SELECT a.x, b.y FROM t1 a JOIN sales.orders b ON a.id = b.id WHERE a.z IN (SELECT z FROM main.default.t3)");
        assert_eq!(a.kind, StmtKind::Select);
        assert_eq!(a.reads.iter().cloned().collect::<Vec<_>>(), vec!["main.default.t1", "main.default.t3", "main.sales.orders"]);
        assert!(a.writes.is_empty());
    }

    #[test]
    fn cte_not_a_table() {
        let a = an("WITH c AS (SELECT * FROM src) SELECT * FROM c");
        assert_eq!(a.reads.len(), 1);
        assert!(a.reads.contains("main.default.src"));
    }

    #[test]
    fn insert_select_lineage() {
        let a = an("INSERT INTO tgt (id, total) SELECT o.id, o.amount * 2 FROM main.sales.orders o");
        assert_eq!(a.kind, StmtKind::Insert);
        assert!(a.writes.contains("main.default.tgt"));
        assert!(a.reads.contains("main.sales.orders"));
        assert!(a.column_edges.contains(&ColumnEdge { source_table: "main.sales.orders".into(), source_column: "amount".into(), target_column: "total".into() }));
        assert!(a.column_edges.contains(&ColumnEdge { source_table: "main.sales.orders".into(), source_column: "id".into(), target_column: "id".into() }));
    }

    #[test]
    fn ctas_and_drop() {
        let a = an("CREATE OR REPLACE TABLE gold.summary AS SELECT id AS k, amount FROM silver.orders");
        assert_eq!(a.kind, StmtKind::CreateTable);
        assert_eq!(a.creates, vec![(Securable::Table, "main.gold.summary".to_string())]);
        assert!(a.replace);
        assert!(a.column_edges.contains(&ColumnEdge { source_table: "main.silver.orders".into(), source_column: "id".into(), target_column: "k".into() }));
        let d = an("DROP TABLE IF EXISTS main.gold.summary");
        assert_eq!(d.owned, vec![(Securable::Table, "main.gold.summary".to_string())]);
        let s = an("DROP SCHEMA gold");
        assert_eq!(s.owned, vec![(Securable::Schema, "main.gold".to_string())]);
    }

    #[test]
    fn path_refs() {
        let a = an("SELECT * FROM delta.`s3://bucket/path`");
        assert!(a.reads.is_empty());
        assert!(a.paths.contains("s3://bucket/path"));
    }

    #[test]
    fn fallback_extraction() {
        let a = analyze_fallback("DESCRIBE HISTORY main.default.t", "main", "default");
        assert_eq!(a.kind, StmtKind::Describe);
        let a = analyze_fallback("MERGE INTO tgt USING src ON tgt.id = src.id WHEN MATCHED THEN UPDATE SET *", "main", "default");
        assert!(a.writes.contains("main.default.tgt"));
        assert!(a.reads.contains("main.default.src"));
    }

    #[test]
    fn rewrite_row_filter_and_mask() {
        let stmts = parse("SELECT id, email FROM main.default.users u WHERE id > 1").unwrap();
        let mut policies = HashMap::new();
        policies.insert(
            "main.default.users".to_string(),
            TablePolicy {
                full_name: "main.default.users".into(),
                columns: vec!["id".into(), "email".into(), "region".into()],
                row_filter: Some(("main.default.region_ok".into(), vec!["region".into()])),
                masks: HashMap::from([("email".to_string(), ("main.default.mask_email".to_string(), vec![]))]),
            },
        );
        let mut functions = HashMap::new();
        functions.insert("main.default.region_ok".to_string(), SqlFunction { full_name: "main.default.region_ok".into(), params: vec!["r".into()], body: "is_account_group_member('admins') OR r = 'EU'".into() });
        functions.insert("main.default.mask_email".to_string(), SqlFunction { full_name: "main.default.mask_email".into(), params: vec!["e".into()], body: "CASE WHEN is_account_group_member('pii') THEN e ELSE '***' END".into() });
        let facts = SessionFacts { user: "bob".into(), groups: vec!["analysts".into()] };
        let out = rewrite(&stmts, &policies, &functions, &facts, "main", "default").expect("rewritten");
        assert!(out.contains("FROM (SELECT id, (CASE WHEN false THEN (email) ELSE '***' END) AS email, region FROM main.default.users WHERE (false OR (region) = 'EU')) u WHERE id > 1"), "{out}");
    }

    #[test]
    fn rewrite_udf_and_session_fns() {
        let stmts = parse("SELECT double_it(x), current_user() FROM t").unwrap();
        let mut functions = HashMap::new();
        functions.insert("main.default.double_it".to_string(), SqlFunction { full_name: "main.default.double_it".into(), params: vec!["v".into()], body: "v * 2".into() });
        let out = rewrite(&stmts, &HashMap::new(), &functions, &SessionFacts { user: "amy".into(), groups: vec![] }, "main", "default").unwrap();
        assert_eq!(out, "SELECT ((x) * 2), 'amy' FROM t");
    }

    #[test]
    fn create_function_parses() {
        let stmts = parse("CREATE OR REPLACE FUNCTION main.default.add1(x INT) RETURNS INT RETURN x + 1").unwrap();
        let (f, rt, params, replace) = parse_sql_function(&stmts[0], "main", "default").unwrap();
        assert_eq!(f.full_name, "main.default.add1");
        assert_eq!(f.body, "x + 1");
        assert_eq!(rt, "INT");
        assert_eq!(params, vec![("x".to_string(), "INT".to_string())]);
        assert!(replace);
    }
}
