import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, fmtDuration, fmtTime, type Experiment, type MlRun } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'

const ML = '/api/2.0/mlflow'

function MetricChart({ runId, metric }: { runId: string; metric: string }) {
  const q = useQuery({ queryKey: ['metric-history', runId, metric], queryFn: () => api.get<{ metrics: { key: string; value: number; step: number; timestamp: number }[] }>(`${ML}/metrics/get-history?run_id=${runId}&metric_key=${encodeURIComponent(metric)}`) })
  const pts = q.data?.metrics ?? []
  if (pts.length === 0) return <div className="muted small">No history.</div>
  const W = 420
  const H = 140
  const xs = pts.map((p) => p.step)
  const ys = pts.map((p) => p.value)
  const x0 = Math.min(...xs)
  const x1 = Math.max(...xs)
  const y0 = Math.min(...ys)
  const y1 = Math.max(...ys)
  const sx = (x: number) => (x1 === x0 ? W / 2 : ((x - x0) / (x1 - x0)) * (W - 20) + 10)
  const sy = (y: number) => (y1 === y0 ? H / 2 : H - 10 - ((y - y0) / (y1 - y0)) * (H - 20))
  const d = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${sx(p.step).toFixed(1)},${sy(p.value).toFixed(1)}`).join(' ')
  return (
    <div>
      <div className="small"><b>{metric}</b> · {pts.length} points · min {y0.toPrecision(4)} · max {y1.toPrecision(4)} · last {ys[ys.length - 1].toPrecision(4)}</div>
      <svg width={W} height={H} className="chart">
        <path d={d} fill="none" stroke="var(--accent)" strokeWidth={2} />
        {pts.map((p, i) => <circle key={i} cx={sx(p.step)} cy={sy(p.value)} r={2.5} fill="var(--accent)"><title>step {p.step}: {p.value}</title></circle>)}
      </svg>
    </div>
  )
}

function RunDetail({ run, onClose }: { run: MlRun; onClose: () => void }) {
  const [tab, setTab] = useState('overview')
  const [metric, setMetric] = useState(run.data.metrics[0]?.key ?? '')
  const artifacts = useQuery({ queryKey: ['artifacts', run.info.run_id], queryFn: () => api.get<{ files: { path: string; is_dir: boolean; file_size?: number }[]; root_uri: string }>(`${ML}/artifacts/list?run_id=${run.info.run_id}`), enabled: tab === 'artifacts' })
  return (
    <Modal title={<span>{run.info.run_name ?? run.info.run_id} <Badge state={run.info.status} /></span>} onClose={onClose} wide>
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'overview', label: 'Overview' }, { id: 'metrics', label: `Metrics (${run.data.metrics.length})` }, { id: 'artifacts', label: 'Artifacts' }, { id: 'json', label: 'JSON' }]} />
      {tab === 'overview' && (
        <div className="grid2">
          <Card title="Run">
            <KV items={[['Run ID', <code>{run.info.run_id}</code>], ['User', run.info.user_id ?? '—'], ['Started', fmtTime(run.info.start_time)], ['Duration', fmtDuration((run.info.end_time ?? Date.now()) - run.info.start_time)], ['Artifact URI', <code className="small">{run.info.artifact_uri}</code>]]} />
          </Card>
          <Card title="Parameters"><KV items={run.data.params.map((p) => [p.key, p.value] as [string, string])} /></Card>
          <Card title="Metrics (latest)"><KV items={run.data.metrics.map((m) => [m.key, String(m.value)] as [string, string])} /></Card>
          <Card title="Tags"><KV items={run.data.tags.map((t) => [t.key, t.value] as [string, string])} /></Card>
        </div>
      )}
      {tab === 'metrics' && (
        <div>
          <div className="actions" style={{ marginBottom: 8 }}>
            {run.data.metrics.map((m) => <button key={m.key} className={`sm ${metric === m.key ? 'primary' : ''}`} onClick={() => setMetric(m.key)}>{m.key}</button>)}
          </div>
          {metric && <MetricChart runId={run.info.run_id} metric={metric} />}
        </div>
      )}
      {tab === 'artifacts' && (
        <Table
          rows={artifacts.data?.files ?? []}
          keyOf={(f) => f.path}
          columns={[
            { key: 'p', title: 'Path', render: (f) => (f.is_dir ? `📁 ${f.path}` : <a href={`/api/2.0/mlflow-artifacts/artifacts/${encodeURIComponent(run.info.run_id)}/${f.path}`} target="_blank" rel="noreferrer">{f.path}</a>) },
            { key: 's', title: 'Size', render: (f) => (f.is_dir ? '' : `${f.file_size ?? 0} B`) },
          ]}
          emptyText="No artifacts logged."
        />
      )}
      {tab === 'json' && <JsonView value={run} />}
    </Modal>
  )
}

function ExperimentDetail({ id }: { id: string }) {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const [sel, setSel] = useState<MlRun | null>(null)
  const [filter, setFilter] = useState('')
  const [compare, setCompare] = useState<Set<string>>(new Set())
  const exp = useQuery({ queryKey: ['experiment', id], queryFn: () => api.get<{ experiment: Experiment }>(`${ML}/experiments/get?experiment_id=${id}`) })
  const runs = useQuery({
    queryKey: ['ml-runs', id, filter],
    queryFn: () => api.post<{ runs: MlRun[] }>(`${ML}/runs/search`, { experiment_ids: [id], filter: filter || undefined, max_results: 500, order_by: ['attributes.start_time DESC'] }),
    refetchInterval: 5000,
  })
  const metricKeys = useMemo(() => Array.from(new Set((runs.data?.runs ?? []).flatMap((r) => r.data.metrics.map((m) => m.key)))).slice(0, 6), [runs.data])
  const paramKeys = useMemo(() => Array.from(new Set((runs.data?.runs ?? []).flatMap((r) => r.data.params.map((p) => p.key)))).slice(0, 4), [runs.data])
  if (exp.isLoading) return <Spinner />
  if (!exp.data) return <ErrorBox error={exp.error ?? 'Experiment not found'} />
  const e = exp.data.experiment
  const rows = runs.data?.runs ?? []
  const cmp = rows.filter((r) => compare.has(r.info.run_id))
  return (
    <Page
      title={<span><Link to="/ml/experiments">Experiments</Link> / {e.name}</span>}
      subtitle={`Experiment ${e.experiment_id} · ${e.artifact_location} · ${e.lifecycle_stage}`}
      actions={
        <>
          <button onClick={async () => { const n = window.prompt('Rename experiment', e.name); if (n && n !== e.name) { await api.post(`${ML}/experiments/update`, { experiment_id: id, new_name: n }); void qc.invalidateQueries({ queryKey: ['experiment', id] }) } }}>Rename</button>
          <button className="danger" onClick={async () => { if (window.confirm('Delete experiment?')) { await api.post(`${ML}/experiments/delete`, { experiment_id: id }); toast('Deleted'); void qc.invalidateQueries({ queryKey: ['experiments'] }); window.history.back() } }}>Delete</button>
        </>
      }
    >
      {node}
      <div className="toolbar">
        <input style={{ flex: 1 }} placeholder='Filter, e.g. metrics.accuracy > 0.9 and params.model = "xgb"' value={filter} onChange={(ev) => setFilter(ev.target.value)} />
        {compare.size > 0 && <button className="sm" onClick={() => setCompare(new Set())}>Clear selection ({compare.size})</button>}
      </div>
      <ErrorBox error={runs.error} />
      <Table
        rows={rows}
        keyOf={(r) => r.info.run_id}
        onRowClick={(r) => setSel(r)}
        columns={[
          { key: 'c', title: '', width: '32px', render: (r) => <input type="checkbox" checked={compare.has(r.info.run_id)} onClick={(ev) => ev.stopPropagation()} onChange={(ev) => { const s = new Set(compare); if (ev.target.checked) s.add(r.info.run_id); else s.delete(r.info.run_id); setCompare(s) }} /> },
          { key: 'n', title: 'Run', render: (r) => <span><b>{r.info.run_name ?? r.info.run_id.slice(0, 8)}</b> <Badge state={r.info.status} /></span> },
          { key: 't', title: 'Started', render: (r) => fmtTime(r.info.start_time) },
          { key: 'd', title: 'Duration', render: (r) => fmtDuration((r.info.end_time ?? Date.now()) - r.info.start_time) },
          { key: 'u', title: 'User', render: (r) => r.info.user_id ?? '' },
          ...paramKeys.map((k) => ({ key: `p:${k}`, title: k, render: (r: MlRun) => r.data.params.find((p) => p.key === k)?.value ?? '' })),
          ...metricKeys.map((k) => ({ key: `m:${k}`, title: k, render: (r: MlRun) => { const m = r.data.metrics.find((x) => x.key === k); return m ? m.value.toPrecision(5) : '' } })),
        ]}
        emptyText="No runs logged yet. Use mlflow.start_run() from a notebook with MLFLOW_TRACKING_URI=databricks."
      />
      {cmp.length > 1 && (
        <Card title={`Compare ${cmp.length} runs`}>
          <table className="table">
            <thead><tr><th></th>{cmp.map((r) => <th key={r.info.run_id}>{r.info.run_name ?? r.info.run_id.slice(0, 8)}</th>)}</tr></thead>
            <tbody>
              {Array.from(new Set(cmp.flatMap((r) => r.data.params.map((p) => p.key)))).map((k) => <tr key={`p${k}`}><td className="muted">param {k}</td>{cmp.map((r) => <td key={r.info.run_id}>{r.data.params.find((p) => p.key === k)?.value ?? '—'}</td>)}</tr>)}
              {Array.from(new Set(cmp.flatMap((r) => r.data.metrics.map((m) => m.key)))).map((k) => {
                const vals = cmp.map((r) => r.data.metrics.find((m) => m.key === k)?.value)
                const best = Math.max(...vals.filter((v): v is number => v !== undefined))
                return <tr key={`m${k}`}><td className="muted">metric {k}</td>{vals.map((v, i) => <td key={i} style={{ fontWeight: v === best ? 700 : 400 }}>{v === undefined ? '—' : v.toPrecision(5)}</td>)}</tr>
              })}
            </tbody>
          </table>
        </Card>
      )}
      {sel && <RunDetail run={sel} onClose={() => setSel(null)} />}
    </Page>
  )
}

export default function Experiments() {
  const { id } = useParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [name, setName] = useState('')
  const [err, setErr] = useState<unknown>(null)
  const list = useQuery({ queryKey: ['experiments'], queryFn: () => api.post<{ experiments: Experiment[] }>(`${ML}/experiments/search`, { max_results: 1000 }) })
  if (id) return <ExperimentDetail id={id} />
  return (
    <Page title="Experiments" subtitle="MLflow tracking: runs, parameters, metrics and artifacts. Set MLFLOW_TRACKING_URI=databricks in notebooks." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create experiment</button>}>
      {list.isLoading && <Spinner />}
      <ErrorBox error={list.error} />
      <Table
        rows={list.data?.experiments ?? []}
        keyOf={(e) => e.experiment_id}
        onRowClick={(e) => nav(`/ml/experiments/${e.experiment_id}`)}
        columns={[
          { key: 'n', title: 'Name', render: (e) => <b>{e.name}</b> },
          { key: 'id', title: 'ID', render: (e) => <code>{e.experiment_id}</code> },
          { key: 'l', title: 'Location', render: (e) => <code className="small">{e.artifact_location}</code> },
          { key: 'c', title: 'Created', render: (e) => fmtTime(e.creation_time) },
          { key: 'u', title: 'Updated', render: (e) => fmtTime(e.last_update_time) },
        ]}
        emptyText="No experiments."
      />
      {creating && (
        <Modal title="Create experiment" onClose={() => setCreating(false)} footer={<><button onClick={() => setCreating(false)}>Cancel</button><button className="primary" disabled={!name.trim()} onClick={async () => { setErr(null); try { const r = await api.post<{ experiment_id: string }>(`${ML}/experiments/create`, { name }); void qc.invalidateQueries({ queryKey: ['experiments'] }); setCreating(false); nav(`/ml/experiments/${r.experiment_id}`) } catch (e) { setErr(e) } }}>Create</button></>}>
          <Field label="Name" hint="Workspace path names like /Users/you/my-experiment are conventional"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
    </Page>
  )
}
