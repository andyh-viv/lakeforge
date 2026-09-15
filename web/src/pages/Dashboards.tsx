import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api, fmtTime, type Dashboard, type StatementResponse, type Warehouse } from '../api'
import { Badge, Card, CodeEditor, ErrorBox, Field, Modal, Page, ResultGrid, Spinner, Table, useToast } from '../components'

type VizType = 'table' | 'counter' | 'bar' | 'line'
interface Widget {
  name: string
  title: string
  dataset: string
  viz: VizType
  x?: string
  y?: string
  width: 1 | 2 | 3
}
interface Dataset {
  name: string
  query: string
}
interface Serialized {
  datasets: Dataset[]
  pages: { name: string; displayName: string; layout: { widget: Widget }[] }[]
}

function parse(s?: string): Serialized {
  try {
    const j = JSON.parse(s ?? '{}') as Partial<Serialized>
    return { datasets: j.datasets ?? [], pages: j.pages?.length ? j.pages : [{ name: 'page1', displayName: 'Page 1', layout: [] }] }
  } catch {
    return { datasets: [], pages: [{ name: 'page1', displayName: 'Page 1', layout: [] }] }
  }
}

async function runStatement(wh: string, sql: string): Promise<StatementResponse> {
  let res = await api.post<StatementResponse>('/api/2.0/sql/statements', { warehouse_id: wh, statement: sql, wait_timeout: '50s', row_limit: 5000 })
  while (res.status.state === 'PENDING' || res.status.state === 'RUNNING') {
    await new Promise((r) => setTimeout(r, 500))
    res = await api.get<StatementResponse>(`/api/2.0/sql/statements/${res.statement_id}`)
  }
  return res
}

function Viz({ w, res }: { w: Widget; res?: StatementResponse }) {
  if (!res) return <div className="muted small">Not run yet.</div>
  if (res.status.state !== 'SUCCEEDED') return <ErrorBox error={res.status.error?.message ?? res.status.state} />
  const cols = res.manifest?.schema.columns.map((c) => c.name) ?? []
  const rows = res.result?.data_array ?? []
  if (w.viz === 'table') return <ResultGrid columns={cols} rows={rows} />
  if (w.viz === 'counter') {
    const v = rows[0]?.[w.y ? cols.indexOf(w.y) : 0] ?? '—'
    return <div className="counter">{String(v)}</div>
  }
  const xi = w.x ? cols.indexOf(w.x) : 0
  const yi = w.y ? cols.indexOf(w.y) : 1
  const vals = rows.map((r) => Number(r[yi] ?? 0))
  const max = Math.max(1, ...vals)
  if (w.viz === 'line') {
    const pts = vals.map((v, i) => `${(i / Math.max(1, vals.length - 1)) * 100},${100 - (v / max) * 100}`).join(' ')
    return (
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="line-chart">
        <polyline fill="none" stroke="var(--brand)" strokeWidth="1.5" points={pts} />
      </svg>
    )
  }
  return (
    <div className="bars">
      {rows.slice(0, 40).map((r, i) => (
        <div key={i} className="bar" title={`${r[xi]}: ${r[yi]}`}>
          <div className="fill" style={{ height: `${(vals[i] / max) * 100}%` }} />
          <div className="lbl">{String(r[xi] ?? '')}</div>
        </div>
      ))}
    </div>
  )
}

function DashboardView({ id, onBack }: { id: string; onBack: () => void }) {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const d = useQuery({ queryKey: ['dashboard', id], queryFn: () => api.get<Dashboard>(`/api/2.0/lakeview/dashboards/${id}`) })
  const warehouses = useQuery({ queryKey: ['warehouses'], queryFn: () => api.get<{ warehouses: Warehouse[] }>('/api/2.0/sql/warehouses') })
  const [ser, setSer] = useState<Serialized | null>(null)
  const [wh, setWh] = useState(localStorage.getItem('lakeforge.warehouse') ?? '')
  const [results, setResults] = useState<Record<string, StatementResponse>>({})
  const [busy, setBusy] = useState(false)
  const [adding, setAdding] = useState(false)
  const [w, setW] = useState<Widget & { query: string }>({ name: '', title: '', dataset: '', viz: 'table', width: 1, query: 'SELECT 1 AS n' })
  useEffect(() => {
    if (d.data) setSer(parse(d.data.serialized_dashboard))
  }, [d.data])
  useEffect(() => {
    if (!wh && warehouses.data?.warehouses.length) setWh(warehouses.data.warehouses[0].id)
  }, [warehouses.data, wh])

  const save = async (s: Serialized) => {
    setSer(s)
    try {
      await api.patch(`/api/2.0/lakeview/dashboards/${id}`, { serialized_dashboard: JSON.stringify(s), warehouse_id: wh })
      void qc.invalidateQueries({ queryKey: ['dashboards'] })
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  const runAll = async () => {
    if (!ser || !wh) return
    setBusy(true)
    const out: Record<string, StatementResponse> = {}
    for (const ds of ser.datasets) {
      try {
        out[ds.name] = await runStatement(wh, ds.query)
      } catch (e) {
        out[ds.name] = { statement_id: '', status: { state: 'FAILED', error: { message: e instanceof Error ? e.message : String(e) } } }
      }
      setResults({ ...out })
    }
    setBusy(false)
  }
  const addWidget = async () => {
    if (!ser) return
    const name = w.name || `w${Date.now().toString(36)}`
    const dsName = `ds_${name}`
    const s: Serialized = {
      datasets: [...ser.datasets, { name: dsName, query: w.query }],
      pages: ser.pages.map((p, i) => (i === 0 ? { ...p, layout: [...p.layout, { widget: { ...w, name, dataset: dsName } }] } : p)),
    }
    setAdding(false)
    await save(s)
  }
  const removeWidget = (name: string) => {
    if (!ser) return
    const wid = ser.pages[0].layout.find((l) => l.widget.name === name)?.widget
    void save({ datasets: ser.datasets.filter((d) => d.name !== wid?.dataset), pages: ser.pages.map((p) => ({ ...p, layout: p.layout.filter((l) => l.widget.name !== name) })) })
  }
  const publish = async () => {
    try {
      await api.post(`/api/2.0/lakeview/dashboards/${id}/published`, { warehouse_id: wh, embed_credentials: true })
      toast('Dashboard published')
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  if (!d.data || !ser) return <Spinner />
  return (
    <Page
      title={<span><a href="#" onClick={(e) => { e.preventDefault(); onBack() }}>Dashboards</a> / {d.data.display_name}</span>}
      subtitle={`${ser.datasets.length} datasets · updated ${fmtTime(d.data.update_time)}`}
      actions={
        <>
          <select value={wh} onChange={(e) => setWh(e.target.value)}>
            {(warehouses.data?.warehouses ?? []).map((x) => <option key={x.id} value={x.id}>{x.name}</option>)}
          </select>
          <button onClick={() => setAdding(true)}>＋ Add visualization</button>
          <button onClick={publish}>Publish</button>
          <button className="primary" disabled={busy || !wh} onClick={runAll}>{busy ? 'Refreshing…' : '↻ Refresh all'}</button>
        </>
      }
    >
      {node}
      {ser.pages[0].layout.length === 0 && <div className="empty">Empty dashboard. Add a visualization backed by a SQL query.</div>}
      <div className="dash-grid">
        {ser.pages[0].layout.map(({ widget }) => (
          <Card key={widget.name} className={`span${widget.width}`} title={widget.title || widget.name} actions={<button className="icon" onClick={() => removeWidget(widget.name)}>×</button>}>
            <Viz w={widget} res={results[widget.dataset]} />
            <details className="small muted" style={{ marginTop: 6 }}>
              <summary>SQL</summary>
              <pre>{ser.datasets.find((x) => x.name === widget.dataset)?.query}</pre>
            </details>
          </Card>
        ))}
      </div>
      {adding && (
        <Modal title="Add visualization" onClose={() => setAdding(false)} wide footer={<><button onClick={() => setAdding(false)}>Cancel</button><button className="primary" onClick={addWidget}>Add</button></>}>
          <div className="grid2">
            <Field label="Title"><input autoFocus value={w.title} onChange={(e) => setW({ ...w, title: e.target.value })} /></Field>
            <Field label="Visualization">
              <select value={w.viz} onChange={(e) => setW({ ...w, viz: e.target.value as VizType })}>
                <option value="table">Table</option>
                <option value="counter">Counter</option>
                <option value="bar">Bar chart</option>
                <option value="line">Line chart</option>
              </select>
            </Field>
            <Field label="X column (bar/line)"><input value={w.x ?? ''} onChange={(e) => setW({ ...w, x: e.target.value })} /></Field>
            <Field label="Y column (bar/line/counter)"><input value={w.y ?? ''} onChange={(e) => setW({ ...w, y: e.target.value })} /></Field>
            <Field label="Width">
              <select value={w.width} onChange={(e) => setW({ ...w, width: Number(e.target.value) as 1 | 2 | 3 })}>
                <option value={1}>1/3</option>
                <option value={2}>2/3</option>
                <option value={3}>full</option>
              </select>
            </Field>
          </div>
          <Field label="SQL"><CodeEditor value={w.query} onChange={(q) => setW({ ...w, query: q })} language="sql" minRows={5} /></Field>
        </Modal>
      )}
    </Page>
  )
}

export default function Dashboards() {
  const [sp, setSp] = useSearchParams()
  const qc = useQueryClient()
  const id = sp.get('id')
  const list = useQuery({ queryKey: ['dashboards'], queryFn: () => api.get<{ dashboards: Dashboard[] }>('/api/2.0/lakeview/dashboards') })
  const [name, setName] = useState('')
  const [creating, setCreating] = useState(false)
  if (id) return <DashboardView id={id} onBack={() => setSp({})} />
  const create = async () => {
    const d = await api.post<Dashboard>('/api/2.0/lakeview/dashboards', { display_name: name || 'Untitled dashboard' })
    setCreating(false)
    void qc.invalidateQueries({ queryKey: ['dashboards'] })
    setSp({ id: d.dashboard_id })
  }
  return (
    <Page title="Dashboards" subtitle="Lakeview-style dashboards: SQL datasets rendered as tables, counters and charts." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create dashboard</button>}>
      {list.isLoading && <Spinner />}
      <ErrorBox error={list.error} />
      <Table
        rows={list.data?.dashboards ?? []}
        keyOf={(d) => d.dashboard_id}
        onRowClick={(d) => setSp({ id: d.dashboard_id })}
        columns={[
          { key: 'n', title: 'Name', render: (d) => <b>{d.display_name}</b> },
          { key: 's', title: 'State', render: (d) => <Badge state={d.lifecycle_state} /> },
          { key: 'p', title: 'Path', render: (d) => <code className="small">{d.path}</code> },
          { key: 'u', title: 'Updated', render: (d) => fmtTime(d.update_time) },
          { key: 'a', title: '', width: '100px', render: (d) => <button className="sm danger" onClick={async (e) => { e.stopPropagation(); if (window.confirm('Delete dashboard?')) { await api.delete(`/api/2.0/lakeview/dashboards/${d.dashboard_id}`); void qc.invalidateQueries({ queryKey: ['dashboards'] }) } }}>Delete</button> },
        ]}
        emptyText="No dashboards yet."
      />
      {creating && (
        <Modal title="New dashboard" onClose={() => setCreating(false)} footer={<><button onClick={() => setCreating(false)}>Cancel</button><button className="primary" onClick={create}>Create</button></>}>
          <Field label="Name"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && create()} /></Field>
        </Modal>
      )}
    </Page>
  )
}
