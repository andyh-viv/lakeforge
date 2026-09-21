//! Databricks SQL privilege statements.
//!
//! `GRANT`, `REVOKE`, `SHOW GRANTS` and `ALTER … OWNER TO` use Databricks'
//! own grammar (multi-word privileges such as `USE CATALOG`, securable
//! keywords such as `EXTERNAL LOCATION`), which neither DataFusion nor the
//! generic `sqlparser` dialect accept. They never reach the engine: this
//! module tokenises them and the metastore applies them.

use crate::uc::privileges::{normalize_privilege, Securable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantStmt {
    Grant { privileges: Vec<String>, securable: Securable, name: String, principal: String },
    Revoke { privileges: Vec<String>, securable: Securable, name: String, principal: String },
    ShowGrants { principal: Option<String>, securable: Securable, name: String },
    SetOwner { securable: Securable, name: String, owner: String },
}

/// Parse `sql` if it is a privilege statement; `None` means "not one of ours".
pub fn parse(sql: &str, default_catalog: &str, default_schema: &str) -> Option<Result<GrantStmt, String>> {
    let toks = tokenize(sql.trim().trim_end_matches(';'));
    let up = |i: usize| toks.get(i).map(|t| t.text.to_ascii_uppercase()).unwrap_or_default();
    match (up(0).as_str(), up(1).as_str()) {
        ("GRANT", _) => Some(parse_grant_like(&toks, true, default_catalog, default_schema)),
        ("REVOKE", _) => Some(parse_grant_like(&toks, false, default_catalog, default_schema)),
        ("SHOW", "GRANTS") | ("SHOW", "GRANT") => Some(parse_show(&toks, default_catalog, default_schema)),
        ("ALTER", _) if toks.iter().any(|t| t.text.eq_ignore_ascii_case("OWNER")) => Some(parse_owner(&toks, default_catalog, default_schema)),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct Tok {
    text: String,
    quoted: bool,
}

fn tokenize(s: &str) -> Vec<Tok> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = s.chars();
    let flush = |cur: &mut String, quoted: &mut bool, out: &mut Vec<Tok>| {
        if !cur.is_empty() || *quoted {
            out.push(Tok { text: std::mem::take(cur), quoted: *quoted });
            *quoted = false;
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '`' | '"' | '\'' => {
                for d in chars.by_ref() {
                    if d == c {
                        break;
                    }
                    cur.push(d);
                }
                quoted = true;
            }
            ',' => {
                flush(&mut cur, &mut quoted, &mut out);
                out.push(Tok { text: ",".into(), quoted: false });
            }
            c if c.is_whitespace() => flush(&mut cur, &mut quoted, &mut out),
            c => cur.push(c),
        }
    }
    flush(&mut cur, &mut quoted, &mut out);
    out
}

const PRIVILEGE_WORDS: &[&str] = &["ALL", "PRIVILEGES", "SELECT", "MODIFY", "USE", "CATALOG", "SCHEMA", "CREATE", "TABLE", "VOLUME", "FUNCTION", "MODEL", "READ", "WRITE", "EXECUTE", "MANAGE", "FILES", "BROWSE", "APPLY", "TAG", "REFRESH", "EXTERNAL", "LOCATION", "STORAGE", "CREDENTIAL", "CONNECTION", "FOREIGN", "MANAGED", "SERVICE", "USAGE", "MATERIALIZED", "VIEW", "ACCESS", "CLEAN", "ROOM", "PROVIDER", "RECIPIENT", "SHARE", "SET", "OWNER"];

fn position(toks: &[Tok], word: &str, from: usize) -> Option<usize> {
    toks.iter().enumerate().skip(from).find(|(_, t)| !t.quoted && t.text.eq_ignore_ascii_case(word)).map(|(i, _)| i)
}

fn parse_privileges(toks: &[Tok]) -> Result<Vec<String>, String> {
    let mut out = vec![];
    let mut words: Vec<String> = vec![];
    for t in toks {
        if t.text == "," {
            if !words.is_empty() {
                out.push(normalize_privilege(&words.join("_")));
                words.clear();
            }
            continue;
        }
        let w = t.text.to_ascii_uppercase();
        if !PRIVILEGE_WORDS.contains(&w.as_str()) {
            return Err(format!("Unknown privilege token '{}'", t.text));
        }
        words.push(w);
    }
    if !words.is_empty() {
        out.push(normalize_privilege(&words.join("_")));
    }
    if out.is_empty() {
        return Err("Expected at least one privilege".into());
    }
    Ok(out.into_iter().map(|p| if p == "ALL" { "ALL_PRIVILEGES".to_string() } else { p }).collect())
}

/// `[securable keyword(s)] name` starting at `i`; returns (securable, resolved name, next index).
fn parse_securable(toks: &[Tok], i: usize, default_catalog: &str, default_schema: &str) -> Result<(Securable, String, usize), String> {
    let up = |j: usize| toks.get(j).filter(|t| !t.quoted).map(|t| t.text.to_ascii_uppercase()).unwrap_or_default();
    let (sec, j) = match up(i).as_str() {
        "CATALOG" => (Some(Securable::Catalog), i + 1),
        "SCHEMA" | "DATABASE" => (Some(Securable::Schema), i + 1),
        "TABLE" | "VIEW" | "MATERIALIZED" if up(i) != "MATERIALIZED" => (Some(Securable::Table), i + 1),
        "MATERIALIZED" if up(i + 1) == "VIEW" => (Some(Securable::Table), i + 2),
        "STREAMING" if up(i + 1) == "TABLE" => (Some(Securable::Table), i + 2),
        "VOLUME" => (Some(Securable::Volume), i + 1),
        "FUNCTION" | "MODEL" => (Some(Securable::Function), i + 1),
        "EXTERNAL" if up(i + 1) == "LOCATION" => (Some(Securable::ExternalLocation), i + 2),
        "STORAGE" if up(i + 1) == "CREDENTIAL" => (Some(Securable::StorageCredential), i + 2),
        "CONNECTION" => (Some(Securable::Connection), i + 1),
        "SHARE" => (Some(Securable::Share), i + 1),
        "RECIPIENT" => (Some(Securable::Recipient), i + 1),
        "PROVIDER" => (Some(Securable::Provider), i + 1),
        "METASTORE" => return Ok((Securable::Metastore, crate::api::catalog::METASTORE_ID.to_string(), i + 1)),
        _ => (None, i),
    };
    let name_tok = toks.get(j).ok_or_else(|| "Expected a securable name".to_string())?;
    let raw = name_tok.text.trim_matches('`').to_string();
    let parts: Vec<&str> = raw.split('.').collect();
    let sec = match sec {
        Some(s) => s,
        None => match parts.len() {
            1 => Securable::Catalog,
            2 => Securable::Schema,
            _ => Securable::Table,
        },
    };
    let full = match sec {
        Securable::Schema if parts.len() == 1 => format!("{default_catalog}.{raw}"),
        Securable::Table | Securable::Volume | Securable::Function => match parts.len() {
            1 => format!("{default_catalog}.{default_schema}.{raw}"),
            2 => format!("{default_catalog}.{raw}"),
            _ => raw,
        },
        _ => raw,
    };
    Ok((sec, full, j + 1))
}

fn parse_grant_like(toks: &[Tok], grant: bool, dc: &str, ds: &str) -> Result<GrantStmt, String> {
    let on = position(toks, "ON", 1).ok_or("Expected ON")?;
    let privileges = parse_privileges(&toks[1..on])?;
    let (securable, name, next) = parse_securable(toks, on + 1, dc, ds)?;
    let kw = if grant { "TO" } else { "FROM" };
    if !toks.get(next).is_some_and(|t| !t.quoted && t.text.eq_ignore_ascii_case(kw)) {
        return Err(format!("Expected {kw} after the securable name"));
    }
    let principal = toks.get(next + 1).map(|t| t.text.clone()).filter(|s| !s.is_empty()).ok_or("Expected a principal")?;
    if toks.len() > next + 2 {
        return Err(format!("Unexpected token '{}'", toks[next + 2].text));
    }
    Ok(if grant { GrantStmt::Grant { privileges, securable, name, principal } } else { GrantStmt::Revoke { privileges, securable, name, principal } })
}

fn parse_show(toks: &[Tok], dc: &str, ds: &str) -> Result<GrantStmt, String> {
    let on = position(toks, "ON", 2).ok_or("Expected ON")?;
    let principal = match on {
        2 => None,
        3 => Some(toks[2].text.clone()),
        _ => return Err("Expected SHOW GRANTS [principal] ON securable".into()),
    };
    let (securable, name, next) = parse_securable(toks, on + 1, dc, ds)?;
    if toks.len() > next {
        return Err(format!("Unexpected token '{}'", toks[next].text));
    }
    Ok(GrantStmt::ShowGrants { principal, securable, name })
}

fn parse_owner(toks: &[Tok], dc: &str, ds: &str) -> Result<GrantStmt, String> {
    let (securable, name, mut next) = parse_securable(toks, 1, dc, ds)?;
    if toks.get(next).is_some_and(|t| t.text.eq_ignore_ascii_case("SET")) {
        next += 1;
    }
    let owner_kw = toks.get(next).is_some_and(|t| t.text.eq_ignore_ascii_case("OWNER")) && toks.get(next + 1).is_some_and(|t| t.text.eq_ignore_ascii_case("TO"));
    if !owner_kw {
        return Err("Expected OWNER TO".into());
    }
    let owner = toks.get(next + 2).map(|t| t.text.clone()).ok_or("Expected a principal after OWNER TO")?;
    Ok(GrantStmt::SetOwner { securable, name, owner })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(sql: &str) -> GrantStmt {
        parse(sql, "main", "default").expect("is a grant statement").expect("parses")
    }

    #[test]
    fn grant_multiword_privileges() {
        assert_eq!(
            p("GRANT USE CATALOG, CREATE SCHEMA ON CATALOG main TO `data-eng`"),
            GrantStmt::Grant { privileges: vec!["USE_CATALOG".into(), "CREATE_SCHEMA".into()], securable: Securable::Catalog, name: "main".into(), principal: "data-eng".into() }
        );
    }

    #[test]
    fn grant_infers_table_and_resolves_defaults() {
        assert_eq!(
            p("GRANT SELECT ON TABLE orders TO bob@x.com;"),
            GrantStmt::Grant { privileges: vec!["SELECT".into()], securable: Securable::Table, name: "main.default.orders".into(), principal: "bob@x.com".into() }
        );
        assert_eq!(
            p("GRANT USE CATALOG ON main TO bob@x.com"),
            GrantStmt::Grant { privileges: vec!["USE_CATALOG".into()], securable: Securable::Catalog, name: "main".into(), principal: "bob@x.com".into() }
        );
        assert_eq!(
            p("GRANT SELECT ON main.sales.orders TO bob@x.com"),
            GrantStmt::Grant { privileges: vec!["SELECT".into()], securable: Securable::Table, name: "main.sales.orders".into(), principal: "bob@x.com".into() }
        );
    }

    #[test]
    fn revoke_all_privileges() {
        assert_eq!(
            p("REVOKE ALL PRIVILEGES ON SCHEMA main.sales FROM `account users`"),
            GrantStmt::Revoke { privileges: vec!["ALL_PRIVILEGES".into()], securable: Securable::Schema, name: "main.sales".into(), principal: "account users".into() }
        );
    }

    #[test]
    fn show_grants_with_and_without_principal() {
        assert_eq!(p("SHOW GRANTS ON TABLE main.s.t"), GrantStmt::ShowGrants { principal: None, securable: Securable::Table, name: "main.s.t".into() });
        assert_eq!(p("SHOW GRANTS `bob` ON EXTERNAL LOCATION lake"), GrantStmt::ShowGrants { principal: Some("bob".into()), securable: Securable::ExternalLocation, name: "lake".into() });
    }

    #[test]
    fn alter_owner() {
        assert_eq!(p("ALTER TABLE main.s.t OWNER TO `alice`"), GrantStmt::SetOwner { securable: Securable::Table, name: "main.s.t".into(), owner: "alice".into() });
        assert_eq!(p("ALTER CATALOG main SET OWNER TO admins"), GrantStmt::SetOwner { securable: Securable::Catalog, name: "main".into(), owner: "admins".into() });
    }

    #[test]
    fn quoted_dotted_names() {
        assert_eq!(p("GRANT SELECT ON TABLE `main`.`my schema`.`t` TO u"), GrantStmt::Grant { privileges: vec!["SELECT".into()], securable: Securable::Table, name: "main.my schema.t".into(), principal: "u".into() });
    }

    #[test]
    fn not_a_grant() {
        assert!(parse("SELECT 1", "main", "default").is_none());
        assert!(parse("ALTER TABLE t ADD COLUMN x INT", "main", "default").is_none());
    }

    #[test]
    fn errors() {
        assert!(parse("GRANT SELECT main.s.t TO u", "main", "default").unwrap().is_err());
        assert!(parse("GRANT FLY ON TABLE t TO u", "main", "default").unwrap().is_err());
    }
}
