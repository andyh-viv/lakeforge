import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, fmtDuration, fmtTime, type Pipeline, type PipelineEvent, type PipelineSpec, type WorkspaceObject } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'

interface Flow {
  status: string
  dataset: string
  type?: string
  num_output_rows?: number | null
  duration_ms?: number
  error?: string
}
interface Update {
  update_id: string
  pipeline_id: string
  state: string
  cause: string
  creation_time: number
  end_time?: number
  full_refresh: boolean
  cluster_id?: string
  flows?: Record<string, Flow>
}
interface PipelineDetailData extends Pipeline {
  spec: PipelineSpec & Record<string, unknown>
  catalog?: string
  target?: string
  continuous?: boolean
  development?: boolean
  libraries?: { notebook?: { path: string }; file?: { path: string } }[]
  configuration?: Record<string, string>
}

function PipelineEditor({ initial, onClose, onSaved }: { initial?: PipelineDetailData; onClose: () => void; onSaved: (id: string) => void }) {
  const nbs = useQuery({ queryKey: ['ws-search', 'notebooks'], queryFn: () => api.get<{ objects: WorkspaceObject[] }>('/api/2.0/lakeforge/workspace/search?path=/') })
  const [f, setF] = useState({
    name: initial?.name ?? '',
    catalog: initial?.catalog ?? 'main',
    target: initial?.target ?? 'dlt',
    continuous: initial?.continuous ?? false,
    development: initial?.development ?? true,
    libraries: (initial?.libraries ?? []).map((l) => l.notebook?.path ?? l.file?.path ?? '').join('\n'),
    configuration: Object.entries(initial?.configuration ?? {}).map(([k, v]) => `${k}=${v}`).join('\n'),
  })
  const [err, setErr] = useState<unknown>(null)
  const submit = async () => {
    setErr(null)
    try {
      const body = {
        name: f.name,
        catalog: f.catalog,
        target: f.target,
        continuous: f.continuous,
        development: f.development,
        libraries: f.libraries.split('\n').map((s) => s.trim()).filter(Boolean).map((p) => (p.endsWith('.py') || p.endsWith('.sql') ? { file: { path: p } } : { notebook: { path: p } })),
        configuration: Object.fromEntries(f.configuration.split('\n').filter((l) => l.includes('=')).map((l) => { const [k, ...v] = l.split('='); return [k.trim(), v.join('=').trim()] })),
      }
      if (initial) {
        await api.put(`/api/2.0/pipelines/${initial.pipeline_id}`, body)
        onSaved(initial.pipeline_id)
      } else {
        const r = await api.post<{ pipeline_id: string }>('/api/2.0/pipelines', body)
        onSaved(r.pipeline_id)
      }
    } catch (e) {
      setErr(e)
    }
  }
  return (
    <Modal title={initial ? `Edit ${initial.name}` : 'Create pipeline'} onClose={onClose} wide footer={<><button onClick={onClose}>Cancel</button><button className="primary" disabled={!f.name.trim()} onClick={submit}>{initial ? 'Save' : 'Create'}</button></>}>
      <Field label="Pipeline name"><input autoFocus value={f.name} onChange={(e) => setF({ ...f, name: e.target.value })} /></Field>
      <div className="grid2">
        <Field label="Catalog"><input value={f.catalog} onChange={(e) => setF({ ...f, catalog: e.target.value })} /></Field>
        <Field label="Target schema"><input value={f.target} onChange={(e) => setF({ ...f, target: e.target.value })} /></Field>
      </div>
      <Field label="Source code" hint="one notebook or .sql/.py workspace path per line; use CREATE [OR REFRESH] [STREAMING|LIVE|MATERIALIZED] TABLE/VIEW ... AS SELECT">
        <textarea rows={3} value={f.libraries} onChange={(e) => setF({ ...f, libraries: e.target.value })} />
        <select value="" onChange={(e) => { if (e.target.value) setF({ ...f, libraries: (f.libraries ? f.libraries.trimEnd() + '\n' : '') + e.target.value }) }}>
          <option value="">Add notebook from workspace…</option>
          {(nbs.data?.objects ?? []).filter((o) => o.object_type === 'NOTEBOOK').map((n) => <option key={n.path} value={n.path}>{n.path}</option>)}
        </select>
      </Field>
      <Field label="Configuration" hint="key=value per line, exposed as spark.conf / ${key} in SQL"><textarea rows={2} value={f.configuration} onChange={(e) => setF({ ...f, configuration: e.target.value })} /></Field>
      <div className="actions">
        <label className="check"><input type="checkbox" checked={f.development} onChange={(e) => setF({ ...f, development: e.target.checked })} /> Development mode (reuse cluster)</label>
        <label className="check"><input type="checkbox" checked={f.continuous} onChange={(e) => setF({ ...f, continuous: e.target.checked })} /> Continuous</label>
      </div>
      <ErrorBox error={err} />
    </Modal>
  )
}

function PipelineDetail({ id }: { id: string }) {
  const qc = useQueryClient()
  const nav = useNavigate()
  const { toast, node } = useToast()
  const [tab, setTab] = useState('graph')
  const [edit, setEdit] = useState(false)
  const p = useQuery({ queryKey: ['pipeline', id], queryFn: () => api.get<PipelineDetailData>(`/api/2.0/pipelines/${id}`), refetchInterval: 3000 })
  const updates = useQuery({ queryKey: ['pipeline-updates', id], queryFn: () => api.get<{ updates: Update[] }>(`/api/2.0/pipelines/${id}/updates`), refetchInterval: 3000 })
  const events = useQuery({ queryKey: ['pipeline-events', id], queryFn: () => api.get<{ events: PipelineEvent[] }>(`/api/2.0/pipelines/${id}/events?max_results=200`), refetchInterval: 3000 })
  const act = async (path: string, body?: unknown, label?: string) => {
    try {
      await api.post(`/api/2.0/pipelines/${id}/${path}`, body ?? {})
      toast(label ?? `${path} requested`)
      void qc.invalidateQueries({ queryKey: ['pipeline', id] })
      void qc.invalidateQueries({ queryKey: ['pipeline-updates', id] })
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  if (p.isLoading) return <Spinner />
  if (!p.data) return <ErrorBox error={p.error ?? 'Pipeline not found'} />
  const pl = p.data
  const latest = updates.data?.updates[0]
  const flows = Object.values(latest?.flows ?? {})
  return (
    <Page
      title={<span><Link to="/pipelines">Delta Live Tables</Link> / {pl.name} <Badge state={pl.state} /></span>}
      subtitle={`${pl.catalog ?? 'main'}.${pl.target ?? ''} · ${pl.development ? 'development' : 'production'} · ${pl.continuous ? 'continuous' : 'triggered'} · health ${pl.health ?? '—'}`}
      actions={
        <>
          {pl.state === 'RUNNING' ? <button className="danger" onClick={() => act('stop', {}, 'Stop requested')}>■ Stop</button> : <button className="primary" onClick={() => act('updates', { full_refresh: false }, 'Update started')}>▶ Start</button>}
          <button onClick={() => act('updates', { full_refresh: true }, 'Full refresh started')}>Full refresh all</button>
          <button onClick={() => act('reset', {}, 'Reset started')}>Reset</button>
          <button onClick={() => setEdit(true)}>Settings</button>
          <button className="danger" onClick={async () => { if (window.confirm('Delete pipeline?')) { await api.delete(`/api/2.0/pipelines/${id}`); void qc.invalidateQueries({ queryKey: ['pipelines'] }); nav('/pipelines') } }}>Delete</button>
        </>
      }
    >
      {node}
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'graph', label: 'Graph' }, { id: 'updates', label: `Updates (${updates.data?.updates.length ?? 0})` }, { id: 'events', label: 'Event log' }, { id: 'settings', label: 'Settings' }]} />
      {tab === 'graph' && (
        <>
          {latest && (
            <div className="muted small">
              Latest update <code>{latest.update_id.slice(0, 8)}</code> <Badge state={latest.state} /> · {latest.cause} · {fmtTime(latest.creation_time)} {latest.cluster_id && <Link to={`/compute/${latest.cluster_id}`}>cluster</Link>}
            </div>
          )}
          {flows.length === 0 ? (
            <div className="empty">No datasets materialized yet. Start an update to build the graph from your source code.</div>
          ) : (
            <div className="dag flow">
              {flows.map((f) => (
                <div key={f.dataset} className={`task ${f.status.toLowerCase()}`}>
                  <div className="k">{f.dataset} <Badge state={f.status} /></div>
                  <div className="muted small">{f.type ?? 'TABLE'} · {f.num_output_rows ?? '—'} rows · {fmtDuration(f.duration_ms)}</div>
                  {f.error && <div className="error-box small">{f.error}</div>}
                  {f.type !== 'VIEW' && <Link className="small" to={`/catalog/${pl.catalog ?? 'main'}/${pl.target ?? 'default'}/${f.dataset}`}>open table →</Link>}
                </div>
              ))}
            </div>
          )}
        </>
      )}
      {tab === 'updates' && (
        <Table
          rows={updates.data?.updates ?? []}
          keyOf={(u) => u.update_id}
          columns={[
            { key: 'id', title: 'Update', render: (u) => <code>{u.update_id.slice(0, 8)}</code> },
            { key: 's', title: 'State', render: (u) => <Badge state={u.state} /> },
            { key: 'c', title: 'Cause', render: (u) => u.cause },
            { key: 'fr', title: 'Full refresh', render: (u) => (u.full_refresh ? 'yes' : 'no') },
            { key: 't', title: 'Started', render: (u) => fmtTime(u.creation_time) },
            { key: 'd', title: 'Duration', render: (u) => fmtDuration((u.end_time ?? Date.now()) - u.creation_time) },
            { key: 'f', title: 'Flows', render: (u) => `${Object.values(u.flows ?? {}).filter((f) => f.status === 'COMPLETED').length}/${Object.keys(u.flows ?? {}).length}` },
          ]}
          emptyText="No updates."
        />
      )}
      {tab === 'events' && (
        <Table
          rows={events.data?.events ?? []}
          keyOf={(e) => e.id}
          columns={[
            { key: 't', title: 'Time', render: (e) => fmtTime(e.timestamp), width: '200px' },
            { key: 'l', title: 'Level', render: (e) => <Badge state={e.level === 'ERROR' ? 'ERROR' : e.level === 'WARN' ? 'PENDING' : 'READY'}>{e.level}</Badge>, width: '90px' },
            { key: 'ty', title: 'Type', render: (e) => e.event_type, width: '160px' },
            { key: 'm', title: 'Message', render: (e) => e.message },
          ]}
          emptyText="No events."
        />
      )}
      {tab === 'settings' && (
        <div className="grid2">
          <Card title="Settings">
            <KV items={[['Pipeline ID', <code>{pl.pipeline_id}</code>], ['Catalog / target', `${pl.catalog}.${pl.target}`], ['Mode', pl.continuous ? 'continuous' : 'triggered'], ['Development', String(pl.development)], ['Creator', pl.creator_user_name ?? '—'], ['Source', (pl.libraries ?? []).map((l) => l.notebook?.path ?? l.file?.path).join(', ') || '—']]} />
          </Card>
          <Card title="JSON"><JsonView value={pl.spec} /></Card>
        </div>
      )}
      {edit && <PipelineEditor initial={pl} onClose={() => setEdit(false)} onSaved={() => { setEdit(false); void qc.invalidateQueries({ queryKey: ['pipeline', id] }) }} />}
    </Page>
  )
}

export default function Pipelines() {
  const { id } = useParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const list = useQuery({ queryKey: ['pipelines'], queryFn: () => api.get<{ statuses: Pipeline[] }>('/api/2.0/pipelines'), refetchInterval: 5000 })
  if (id) return <PipelineDetail id={id} />
  return (
    <Page title="Delta Live Tables" subtitle="Declarative pipelines: define tables and views in SQL or Python; Lakeforge resolves dependencies and materializes them as Delta tables." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create pipeline</button>}>
      {list.isLoading && <Spinner />}
      <ErrorBox error={list.error} />
      <Table
        rows={list.data?.statuses ?? []}
        keyOf={(p) => p.pipeline_id}
        onRowClick={(p) => nav(`/pipelines/${p.pipeline_id}`)}
        columns={[
          { key: 'n', title: 'Name', render: (p) => <b>{p.name}</b> },
          { key: 's', title: 'State', render: (p) => <Badge state={p.state} /> },
          { key: 'h', title: 'Health', render: (p) => <Badge state={p.health} /> },
          { key: 'u', title: 'Latest update', render: (p) => (p.latest_updates?.[0] ? <span><Badge state={p.latest_updates[0].state} /> {fmtTime(p.latest_updates[0].creation_time)}</span> : '—') },
          { key: 'c', title: 'Creator', render: (p) => p.creator_user_name },
        ]}
        emptyText="No pipelines yet."
      />
      {creating && <PipelineEditor onClose={() => setCreating(false)} onSaved={(pid) => { setCreating(false); void qc.invalidateQueries({ queryKey: ['pipelines'] }); nav(`/pipelines/${pid}`) }} />}
    </Page>
  )
}
