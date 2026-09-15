import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { api, fmtBytes, fmtDuration, fmtTime, type Cluster } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'

interface NodeType {
  node_type_id: string
  memory_mb: number
  num_cores: number
  description: string
  category: string
}
interface ForgeStatus {
  state: string
  driver?: { id: string; uptime_ms: number; version: string; total_slots: number; free_slots: number; running_jobs: number }
  executors: { id: string; host: string; port: number; slots: number; free_slots: number; memory_bytes: number; cpu_cores: number; running: number; completed: number; failed: number; last_heartbeat_ms: number }[]
  jobs: { job_id: string; sql: string; state: string; stages_total: number; stages_done: number; tasks_total: number; tasks_done: number; tasks_running: number; tasks_failed: number; submitted_ms: number; finished_ms?: number; error?: string }[]
}
interface ClusterEvent {
  timestamp: number
  type: string
  details?: unknown
}

const DEFAULT_FORM = { cluster_name: '', num_workers: 2, node_type_id: 'lf.small', driver_node_type_id: 'lf.small', spark_version: 'forge-0.1.x-lts', autotermination_minutes: 60, autoscale: false, min_workers: 1, max_workers: 4, spark_conf: '' }

function ClusterForm({ initial, onClose, onSaved }: { initial?: Cluster; onClose: () => void; onSaved: (id: string) => void }) {
  const nodes = useQuery({ queryKey: ['node-types'], queryFn: () => api.get<{ node_types: NodeType[] }>('/api/2.0/clusters/list-node-types') })
  const versions = useQuery({ queryKey: ['spark-versions'], queryFn: () => api.get<{ versions: { key: string; name: string }[] }>('/api/2.0/clusters/spark-versions') })
  const [f, setF] = useState(() =>
    initial
      ? {
          ...DEFAULT_FORM,
          cluster_name: initial.cluster_name,
          num_workers: initial.num_workers ?? 2,
          node_type_id: initial.node_type_id ?? 'lf.small',
          driver_node_type_id: initial.driver_node_type_id ?? initial.node_type_id ?? 'lf.small',
          spark_version: initial.spark_version ?? 'forge-0.1.x-lts',
          autotermination_minutes: initial.autotermination_minutes ?? 60,
          autoscale: !!initial.autoscale,
          min_workers: initial.autoscale?.min_workers ?? 1,
          max_workers: initial.autoscale?.max_workers ?? 4,
        }
      : DEFAULT_FORM,
  )
  const [err, setErr] = useState<unknown>(null)
  const [busy, setBusy] = useState(false)
  const submit = async () => {
    setBusy(true)
    setErr(null)
    try {
      const conf: Record<string, string> = {}
      for (const line of f.spark_conf.split('\n')) {
        const [k, ...v] = line.trim().split(/\s+/)
        if (k) conf[k] = v.join(' ')
      }
      const body: Record<string, unknown> = {
        cluster_name: f.cluster_name,
        spark_version: f.spark_version,
        node_type_id: f.node_type_id,
        driver_node_type_id: f.driver_node_type_id,
        autotermination_minutes: f.autotermination_minutes,
        spark_conf: conf,
      }
      if (f.autoscale) body.autoscale = { min_workers: f.min_workers, max_workers: f.max_workers }
      else body.num_workers = f.num_workers
      if (initial) {
        await api.post('/api/2.0/clusters/edit', { ...body, cluster_id: initial.cluster_id })
        onSaved(initial.cluster_id)
      } else {
        const r = await api.post<{ cluster_id: string }>('/api/2.0/clusters/create', body)
        onSaved(r.cluster_id)
      }
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }
  return (
    <Modal
      title={initial ? `Edit ${initial.cluster_name}` : 'Create compute'}
      onClose={onClose}
      wide
      footer={
        <>
          <button onClick={onClose}>Cancel</button>
          <button className="primary" disabled={busy || !f.cluster_name.trim()} onClick={submit}>
            {initial ? 'Save and restart' : 'Create cluster'}
          </button>
        </>
      }
    >
      <Field label="Cluster name"><input autoFocus value={f.cluster_name} onChange={(e) => setF({ ...f, cluster_name: e.target.value })} /></Field>
      <div className="grid2">
        <Field label="Forge runtime version">
          <select value={f.spark_version} onChange={(e) => setF({ ...f, spark_version: e.target.value })}>
            {(versions.data?.versions ?? []).map((v) => <option key={v.key} value={v.key}>{v.name}</option>)}
          </select>
        </Field>
        <Field label="Terminate after (minutes of inactivity)"><input type="number" value={f.autotermination_minutes} onChange={(e) => setF({ ...f, autotermination_minutes: Number(e.target.value) })} /></Field>
        <Field label="Worker type">
          <select value={f.node_type_id} onChange={(e) => setF({ ...f, node_type_id: e.target.value })}>
            {(nodes.data?.node_types ?? []).map((n) => <option key={n.node_type_id} value={n.node_type_id}>{n.node_type_id} — {n.description}</option>)}
          </select>
        </Field>
        <Field label="Driver type">
          <select value={f.driver_node_type_id} onChange={(e) => setF({ ...f, driver_node_type_id: e.target.value })}>
            {(nodes.data?.node_types ?? []).map((n) => <option key={n.node_type_id} value={n.node_type_id}>{n.node_type_id} — {n.description}</option>)}
          </select>
        </Field>
      </div>
      <Field label="Scaling">
        <label className="check"><input type="checkbox" checked={f.autoscale} onChange={(e) => setF({ ...f, autoscale: e.target.checked })} /> Enable autoscaling</label>
      </Field>
      {f.autoscale ? (
        <div className="grid2">
          <Field label="Min workers"><input type="number" value={f.min_workers} onChange={(e) => setF({ ...f, min_workers: Number(e.target.value) })} /></Field>
          <Field label="Max workers"><input type="number" value={f.max_workers} onChange={(e) => setF({ ...f, max_workers: Number(e.target.value) })} /></Field>
        </div>
      ) : (
        <Field label="Workers" hint="0 workers = single-node (driver executes tasks)"><input type="number" value={f.num_workers} onChange={(e) => setF({ ...f, num_workers: Number(e.target.value) })} /></Field>
      )}
      <Field label="Forge / Spark config" hint="one `key value` per line, e.g. forge.sql.shuffle.partitions 16">
        <textarea rows={3} value={f.spark_conf} onChange={(e) => setF({ ...f, spark_conf: e.target.value })} />
      </Field>
      <ErrorBox error={err} />
    </Modal>
  )
}

function ClusterDetail({ id }: { id: string }) {
  const qc = useQueryClient()
  const nav = useNavigate()
  const { toast, node } = useToast()
  const [tab, setTab] = useState('overview')
  const [edit, setEdit] = useState(false)
  const c = useQuery({ queryKey: ['cluster', id], queryFn: () => api.get<Cluster>(`/api/2.0/clusters/get?cluster_id=${id}`), refetchInterval: 4000 })
  const forge = useQuery({ queryKey: ['forge', id], queryFn: () => api.get<ForgeStatus>(`/api/2.0/lakeforge/clusters/forge-status?cluster_id=${id}`), refetchInterval: 4000, enabled: c.data?.state === 'RUNNING' })
  const events = useQuery({ queryKey: ['cluster-events', id], queryFn: () => api.post<{ events: ClusterEvent[] }>('/api/2.0/clusters/events', { cluster_id: id, limit: 100 }), refetchInterval: 8000 })
  const act = async (op: string, body: Record<string, unknown> = {}) => {
    try {
      await api.post(`/api/2.0/clusters/${op}`, { cluster_id: id, ...body })
      toast(`${op} requested`)
      void qc.invalidateQueries({ queryKey: ['cluster', id] })
      void qc.invalidateQueries({ queryKey: ['clusters'] })
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  if (c.isLoading) return <Spinner />
  if (!c.data) return <ErrorBox error={c.error ?? 'Cluster not found'} />
  const cl = c.data
  return (
    <Page
      title={<span><Link to="/compute">Compute</Link> / {cl.cluster_name} <Badge state={cl.state} /></span>}
      subtitle={cl.state_message}
      actions={
        <>
          {(cl.state === 'TERMINATED' || cl.state === 'ERROR') && <button className="primary" onClick={() => act('start')}>▶ Start</button>}
          {cl.state === 'RUNNING' && <button onClick={() => act('restart')}>↻ Restart</button>}
          {(cl.state === 'RUNNING' || cl.state === 'PENDING' || cl.state === 'RESIZING') && <button onClick={() => act('delete')}>■ Terminate</button>}
          <button onClick={() => setEdit(true)}>Edit</button>
          <button className="danger" onClick={async () => { if (window.confirm('Permanently delete this cluster?')) { await act('permanent-delete'); nav('/compute') } }}>Delete</button>
        </>
      }
    >
      {node}
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'overview', label: 'Configuration' }, { id: 'forge', label: 'Forge UI' }, { id: 'events', label: 'Event log' }, { id: 'json', label: 'JSON' }]} />
      {tab === 'overview' && (
        <div className="grid2">
          <Card title="Summary">
            <KV
              items={[
                ['Cluster ID', <code>{cl.cluster_id}</code>],
                ['State', <Badge state={cl.state} />],
                ['Runtime', cl.spark_version ?? '—'],
                ['Workers', cl.autoscale ? `autoscale ${cl.autoscale.min_workers}–${cl.autoscale.max_workers}` : String(cl.num_workers ?? 0)],
                ['Worker type', cl.node_type_id ?? '—'],
                ['Driver type', cl.driver_node_type_id ?? cl.node_type_id ?? '—'],
                ['Cores / Memory', `${cl.cluster_cores ?? '—'} cores · ${cl.cluster_memory_mb ? fmtBytes(cl.cluster_memory_mb * 1024 * 1024) : '—'}`],
                ['Auto-terminate', `${cl.autotermination_minutes ?? 0} min`],
                ['Creator', cl.creator_user_name ?? '—'],
                ['Started', fmtTime(cl.start_time)],
                ['Access mode', cl.data_security_mode ?? '—'],
              ]}
            />
          </Card>
          <Card title="Resize">
            <div className="actions">
              {[0, 1, 2, 4, 8].map((n) => (
                <button key={n} className="sm" disabled={cl.state !== 'RUNNING'} onClick={() => act('resize', { num_workers: n })}>
                  {n} workers
                </button>
              ))}
            </div>
            <div className="muted small" style={{ marginTop: 8 }}>Resizing adds or removes Forge executors live; running queries keep their assigned slots.</div>
          </Card>
        </div>
      )}
      {tab === 'forge' &&
        (cl.state !== 'RUNNING' ? (
          <div className="empty">Start the cluster to see the Forge driver UI.</div>
        ) : forge.data ? (
          <>
            <div className="grid4">
              <div className="stat"><div className="n">{forge.data.executors.length}</div><div className="l">executors</div></div>
              <div className="stat"><div className="n">{forge.data.driver?.free_slots ?? 0}/{forge.data.driver?.total_slots ?? 0}</div><div className="l">free task slots</div></div>
              <div className="stat"><div className="n">{forge.data.driver?.running_jobs ?? 0}</div><div className="l">running jobs</div></div>
              <div className="stat"><div className="n">{fmtDuration(forge.data.driver?.uptime_ms)}</div><div className="l">driver uptime · v{forge.data.driver?.version}</div></div>
            </div>
            <Card title="Executors">
              <Table
                rows={forge.data.executors}
                keyOf={(e) => e.id}
                columns={[
                  { key: 'id', title: 'Executor', render: (e) => <code>{e.id}</code> },
                  { key: 'addr', title: 'Address', render: (e) => `${e.host}:${e.port}` },
                  { key: 'slots', title: 'Slots (free)', render: (e) => `${e.slots} (${e.free_slots})` },
                  { key: 'res', title: 'CPU / Mem', render: (e) => `${e.cpu_cores} · ${fmtBytes(e.memory_bytes)}` },
                  { key: 'tasks', title: 'Tasks run/done/fail', render: (e) => `${e.running} / ${e.completed} / ${e.failed}` },
                  { key: 'hb', title: 'Heartbeat', render: (e) => fmtTime(e.last_heartbeat_ms) },
                ]}
              />
            </Card>
            <Card title="Recent jobs">
              <Table
                rows={forge.data.jobs}
                keyOf={(j) => j.job_id}
                columns={[
                  { key: 'id', title: 'Job', render: (j) => <code>{j.job_id.slice(0, 8)}</code> },
                  { key: 'sql', title: 'SQL', render: (j) => <code className="small">{j.sql.length > 80 ? j.sql.slice(0, 80) + '…' : j.sql}</code> },
                  { key: 'st', title: 'State', render: (j) => <Badge state={j.state} /> },
                  { key: 'stages', title: 'Stages', render: (j) => `${j.stages_done}/${j.stages_total}` },
                  { key: 'tasks', title: 'Tasks', render: (j) => `${j.tasks_done}/${j.tasks_total}${j.tasks_failed ? ` (${j.tasks_failed} failed)` : ''}` },
                  { key: 'dur', title: 'Duration', render: (j) => fmtDuration((j.finished_ms ?? Date.now()) - j.submitted_ms) },
                ]}
                emptyText="No jobs submitted yet."
              />
            </Card>
          </>
        ) : (
          <Spinner label="Contacting Forge driver…" />
        ))}
      {tab === 'events' && (
        <Table
          rows={events.data?.events ?? []}
          keyOf={(e) => `${e.timestamp}-${e.type}`}
          columns={[
            { key: 't', title: 'Time', render: (e) => fmtTime(e.timestamp), width: '200px' },
            { key: 'type', title: 'Event', render: (e) => <Badge>{e.type}</Badge>, width: '200px' },
            { key: 'd', title: 'Details', render: (e) => <code className="small">{JSON.stringify(e.details ?? {})}</code> },
          ]}
          emptyText="No events."
        />
      )}
      {tab === 'json' && <JsonView value={cl} />}
      {edit && <ClusterForm initial={cl} onClose={() => setEdit(false)} onSaved={() => { setEdit(false); void qc.invalidateQueries({ queryKey: ['cluster', id] }) }} />}
    </Page>
  )
}

export default function Compute() {
  const { id } = useParams()
  const [sp, setSp] = useSearchParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(sp.get('new') === '1')
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: () => api.get<{ clusters: Cluster[] }>('/api/2.0/clusters/list'), refetchInterval: 5000 })
  if (id) return <ClusterDetail id={id} />
  const rows = clusters.data?.clusters ?? []
  return (
    <Page title="Compute" subtitle="All-purpose Forge clusters (driver + executors). SQL warehouses are managed under SQL." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create compute</button>}>
      {clusters.isLoading && <Spinner />}
      <ErrorBox error={clusters.error} />
      <Table
        rows={rows}
        keyOf={(c) => c.cluster_id}
        onRowClick={(c) => nav(`/compute/${c.cluster_id}`)}
        columns={[
          { key: 'name', title: 'Name', render: (c) => <b>{c.cluster_name}</b> },
          { key: 'state', title: 'State', render: (c) => <Badge state={c.state} /> },
          { key: 'workers', title: 'Workers', render: (c) => (c.autoscale ? `${c.autoscale.min_workers}–${c.autoscale.max_workers}` : c.num_workers ?? 0) },
          { key: 'node', title: 'Node type', render: (c) => c.node_type_id },
          { key: 'rt', title: 'Runtime', render: (c) => c.spark_version },
          { key: 'creator', title: 'Creator', render: (c) => c.creator_user_name },
          {
            key: 'act', title: '', width: '180px',
            render: (c) => (
              <span className="actions" onClick={(e) => e.stopPropagation()}>
                {c.state === 'TERMINATED' || c.state === 'ERROR' ? (
                  <button className="sm" onClick={() => api.post('/api/2.0/clusters/start', { cluster_id: c.cluster_id }).then(() => qc.invalidateQueries({ queryKey: ['clusters'] }))}>▶ Start</button>
                ) : c.state === 'RUNNING' ? (
                  <button className="sm" onClick={() => api.post('/api/2.0/clusters/delete', { cluster_id: c.cluster_id }).then(() => qc.invalidateQueries({ queryKey: ['clusters'] }))}>■ Stop</button>
                ) : null}
              </span>
            ),
          },
        ]}
        emptyText="No clusters yet. Create one to run notebooks."
      />
      {creating && (
        <ClusterForm
          onClose={() => { setCreating(false); setSp({}) }}
          onSaved={(cid) => { setCreating(false); void qc.invalidateQueries({ queryKey: ['clusters'] }); nav(`/compute/${cid}`) }}
        />
      )}
    </Page>
  )
}
