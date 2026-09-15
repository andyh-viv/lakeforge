import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { api, fmtDuration, type CatalogItem, type SqlQuery, type StatementResponse, type Warehouse } from '../api'
import { Badge, CodeEditor, ErrorBox, Field, Modal, ResultGrid, Spinner, useToast } from '../components'

const SAMPLE = `-- Lakeforge SQL (Forge engine, DataFusion dialect)
CREATE OR REPLACE TABLE main.default.trips AS
SELECT * FROM (VALUES (1, 'NYC', 12.5), (2, 'SFO', 30.0), (3, 'NYC', 7.25)) AS t(id, city, fare);

SELECT city, count(*) AS trips, round(sum(fare), 2) AS revenue
FROM main.default.trips GROUP BY city ORDER BY revenue DESC;`

function splitStatements(sql: string): string[] {
  const out: string[] = []
  let cur = ''
  let quote: string | null = null
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]
    if (quote) {
      cur += ch
      if (ch === quote) quote = null
      continue
    }
    if (ch === "'" || ch === '"' || ch === '`') {
      quote = ch
      cur += ch
      continue
    }
    if (ch === '-' && sql[i + 1] === '-') {
      const nl = sql.indexOf('\n', i)
      i = nl < 0 ? sql.length : nl
      cur += '\n'
      continue
    }
    if (ch === ';') {
      if (cur.trim()) out.push(cur.trim())
      cur = ''
      continue
    }
    cur += ch
  }
  if (cur.trim()) out.push(cur.trim())
  return out
}

function currentStatement(sql: string, cursor: number | null): string[] {
  const stmts = splitStatements(sql)
  if (cursor === null || stmts.length <= 1) return stmts
  let pos = 0
  for (const s of stmts) {
    const idx = sql.indexOf(s, pos)
    if (idx >= 0 && cursor <= idx + s.length + 1) return [s]
    pos = idx + s.length
  }
  return [stmts[stmts.length - 1]]
}

export default function SqlEditor() {
  const [sp] = useSearchParams()
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const warehouses = useQuery({ queryKey: ['warehouses'], queryFn: () => api.get<{ warehouses: Warehouse[] }>('/api/2.0/sql/warehouses'), refetchInterval: 10000 })
  const saved = useQuery({ queryKey: ['sql-queries'], queryFn: () => api.get<{ results: SqlQuery[] }>('/api/2.0/sql/queries') })
  const catalogs = useQuery({ queryKey: ['catalog', 'browse'], queryFn: () => api.get<{ catalogs: CatalogItem[] }>('/api/2.0/lakeforge/catalog/browse') })

  const [wh, setWh] = useState(localStorage.getItem('lakeforge.warehouse') ?? '')
  const [sql, setSql] = useState(() => sp.get('sql') ?? localStorage.getItem('lakeforge.sql.draft') ?? SAMPLE)
  const [results, setResults] = useState<{ sql: string; res: StatementResponse; ms: number }[]>([])
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<unknown>(null)
  const [queryId, setQueryId] = useState<string | null>(sp.get('query'))
  const [saveOpen, setSaveOpen] = useState(false)
  const [name, setName] = useState('')
  const [catalog, setCatalog] = useState(sp.get('catalog') ?? 'main')
  const [schema, setSchema] = useState(sp.get('schema') ?? 'default')
  const [browse, setBrowse] = useState<{ schemas?: CatalogItem[]; tables?: CatalogItem[] }>({})
  const [limit, setLimit] = useState(1000)

  useEffect(() => localStorage.setItem('lakeforge.sql.draft', sql), [sql])
  useEffect(() => {
    if (!wh && warehouses.data?.warehouses.length) setWh(warehouses.data.warehouses[0].id)
  }, [warehouses.data, wh])
  useEffect(() => {
    if (wh) localStorage.setItem('lakeforge.warehouse', wh)
  }, [wh])
  useEffect(() => {
    if (!queryId) return
    api.get<SqlQuery>(`/api/2.0/sql/queries/${queryId}`).then((q) => {
      setSql(q.query_text)
      setName(q.display_name)
      if (q.warehouse_id) setWh(q.warehouse_id)
    })
  }, [queryId])
  useEffect(() => {
    api.get<{ schemas: CatalogItem[] }>(`/api/2.0/lakeforge/catalog/browse?catalog_name=${encodeURIComponent(catalog)}`).then((r) => setBrowse((b) => ({ ...b, schemas: r.schemas })))
  }, [catalog])
  useEffect(() => {
    api.get<{ tables: CatalogItem[] }>(`/api/2.0/lakeforge/catalog/browse?catalog_name=${encodeURIComponent(catalog)}&schema_name=${encodeURIComponent(schema)}`).then((r) => setBrowse((b) => ({ ...b, tables: r.tables })))
  }, [catalog, schema, results.length])

  const run = async (onlyCurrent = false) => {
    if (!wh) {
      toast('Select a SQL warehouse', 'err')
      return
    }
    setBusy(true)
    setErr(null)
    const ta = document.querySelector<HTMLTextAreaElement>('.sql-editor textarea')
    const stmts = onlyCurrent ? currentStatement(sql, ta?.selectionStart ?? null) : splitStatements(sql)
    const selected = ta && ta.selectionStart !== ta.selectionEnd ? [sql.slice(ta.selectionStart, ta.selectionEnd)] : stmts
    const out: typeof results = []
    try {
      for (const s of selected) {
        const t0 = performance.now()
        let res = await api.post<StatementResponse>('/api/2.0/sql/statements', { warehouse_id: wh, statement: s, wait_timeout: '50s', catalog, schema, row_limit: limit, disposition: 'INLINE', format: 'JSON_ARRAY' })
        while (res.status.state === 'PENDING' || res.status.state === 'RUNNING') {
          await new Promise((r) => setTimeout(r, 500))
          res = await api.get<StatementResponse>(`/api/2.0/sql/statements/${res.statement_id}`)
        }
        out.unshift({ sql: s, res, ms: Math.round(performance.now() - t0) })
        setResults([...out])
        if (res.status.state !== 'SUCCEEDED') break
      }
      void qc.invalidateQueries({ queryKey: ['catalog'] })
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }

  const save = async () => {
    try {
      const body = { query: { display_name: name || 'Untitled query', query_text: sql, warehouse_id: wh } }
      const q = queryId ? await api.patch<SqlQuery>(`/api/2.0/sql/queries/${queryId}`, body) : await api.post<SqlQuery>('/api/2.0/sql/queries', body)
      setQueryId(q.id)
      setSaveOpen(false)
      toast('Query saved')
      void qc.invalidateQueries({ queryKey: ['sql-queries'] })
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }

  const del = async (q: SqlQuery) => {
    if (!window.confirm(`Delete saved query "${q.display_name}"?`)) return
    await api.delete(`/api/2.0/sql/queries/${q.id}`)
    if (queryId === q.id) setQueryId(null)
    void qc.invalidateQueries({ queryKey: ['sql-queries'] })
  }

  const download = (r: StatementResponse) => {
    const cols = r.manifest?.schema.columns.map((c) => c.name) ?? []
    const rows = r.result?.data_array ?? []
    const csv = [cols.join(','), ...rows.map((row) => row.map((v) => (v === null ? '' : `"${String(v).replace(/"/g, '""')}"`)).join(','))].join('\n')
    const a = document.createElement('a')
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }))
    a.download = 'result.csv'
    a.click()
  }

  const whObj = warehouses.data?.warehouses.find((w) => w.id === wh)
  return (
    <div className="page">
      {node}
      <div className="page-head">
        <div>
          <h1>SQL Editor {name && <span className="muted">· {name}</span>}</h1>
          <div className="muted small">Ctrl/Shift+Enter runs the statement under the cursor; select text to run only the selection.</div>
        </div>
        <div className="actions">
          <select value={wh} onChange={(e) => setWh(e.target.value)}>
            <option value="">— warehouse —</option>
            {(warehouses.data?.warehouses ?? []).map((w) => (
              <option key={w.id} value={w.id}>
                {w.name} · {w.cluster_size} ({w.state})
              </option>
            ))}
          </select>
          {whObj && <Badge state={whObj.state} />}
          <select value={catalog} onChange={(e) => setCatalog(e.target.value)}>
            {(catalogs.data?.catalogs ?? []).map((c) => (
              <option key={c.name} value={c.name}>{c.name}</option>
            ))}
          </select>
          <select value={schema} onChange={(e) => setSchema(e.target.value)}>
            {(browse.schemas ?? []).map((s) => (
              <option key={s.name} value={s.name}>{s.name}</option>
            ))}
          </select>
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            {[100, 1000, 10000].map((n) => <option key={n} value={n}>limit {n}</option>)}
          </select>
          <button onClick={() => { setName(name || ''); setSaveOpen(true) }}>{queryId ? 'Save' : 'Save as…'}</button>
          <button onClick={() => run(true)} disabled={busy}>Run current</button>
          <button className="primary" onClick={() => run(false)} disabled={busy}>{busy ? 'Running…' : '▶ Run all'}</button>
        </div>
      </div>
      <div className="split wide-left">
        <div className="card" style={{ maxHeight: '75vh', overflow: 'auto' }}>
          <h3>Schema browser</h3>
          <div className="muted small" style={{ marginBottom: 8 }}>{catalog}.{schema}</div>
          <div className="tree">
            {(browse.tables ?? []).map((t) => (
              <details key={t.full_name}>
                <summary className="node">▦ {t.name} <span className="muted small">{t.data_source_format ?? t.table_type}</span></summary>
                <div className="children">
                  {(t.columns ?? []).map((c) => (
                    <div key={c.name} className="node small" onClick={() => setSql((s) => s + (s.endsWith(' ') || s.endsWith('\n') || !s ? '' : ' ') + c.name)}>
                      {c.name} <span className="muted">{c.type_text}</span>
                    </div>
                  ))}
                  <div className="node small" onClick={() => setSql(`SELECT * FROM ${t.full_name} LIMIT 100;`)}>▶ preview</div>
                  <Link className="small" to={`/catalog/${t.full_name.replace(/\./g, '/')}`}>details →</Link>
                </div>
              </details>
            ))}
            {browse.tables?.length === 0 && <div className="muted small">No tables in this schema.</div>}
          </div>
          <h3 style={{ marginTop: 16 }}>Saved queries</h3>
          <div className="tree">
            {(saved.data?.results ?? []).map((q) => (
              <div key={q.id} className={`node ${q.id === queryId ? 'selected' : ''}`}>
                <span style={{ flex: 1 }} onClick={() => setQueryId(q.id)}>›_ {q.display_name}</span>
                <button className="icon small" onClick={() => del(q)}>×</button>
              </div>
            ))}
            {saved.data?.results.length === 0 && <div className="muted small">No saved queries.</div>}
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div className="sql-editor">
            <CodeEditor value={sql} onChange={setSql} language="sql" onRun={() => run(true)} minRows={8} />
          </div>
          <ErrorBox error={err} />
          {busy && <Spinner label="Executing on Forge…" />}
          {results.map((r, i) => {
            const cols = r.res.manifest?.schema.columns ?? []
            const rows = r.res.result?.data_array ?? []
            return (
              <div className="card" key={i}>
                <div className="card-head">
                  <div className="sql-status">
                    <Badge state={r.res.status.state} />
                    <span>{r.res.manifest?.total_row_count ?? rows.length} rows</span>
                    <span>{fmtDuration(r.ms)}</span>
                    <code className="small muted" title={r.sql}>{r.sql.length > 90 ? r.sql.slice(0, 90) + '…' : r.sql}</code>
                  </div>
                  <div className="actions">
                    {rows.length > 0 && <button className="sm" onClick={() => download(r.res)}>↓ CSV</button>}
                    <button className="sm" onClick={() => setResults((rs) => rs.filter((_, j) => j !== i))}>✕</button>
                  </div>
                </div>
                {r.res.status.state === 'FAILED' ? (
                  <ErrorBox error={r.res.status.error?.message ?? 'Statement failed'} />
                ) : cols.length ? (
                  <ResultGrid columns={cols.map((c) => `${c.name}`)} rows={rows} truncated={r.res.manifest?.truncated} />
                ) : (
                  <div className="muted small">Statement executed; no result set.</div>
                )}
              </div>
            )
          })}
        </div>
      </div>
      {saveOpen && (
        <Modal
          title="Save query"
          onClose={() => setSaveOpen(false)}
          footer={
            <>
              <button onClick={() => setSaveOpen(false)}>Cancel</button>
              <button className="primary" onClick={save}>Save</button>
            </>
          }
        >
          <Field label="Name"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} /></Field>
        </Modal>
      )}
    </div>
  )
}
