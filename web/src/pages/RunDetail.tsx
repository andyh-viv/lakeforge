import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api, fmtDuration, fmtTime, type KernelEvent, type Run, type RunTask } from '../api'
import { Badge, Card, ErrorBox, JsonView, KV, Page, Spinner, Tabs, useToast } from '../components'
import { Output } from './Notebook'

interface TaskOutput {
  notebook_output?: { result?: string; truncated?: boolean }
  sql_output?: unknown
  logs?: string
  error?: string
  error_trace?: string
  metadata?: unknown
  outputs?: KernelEvent[]
  cells?: { id: string; source: string; outputs: KernelEvent[] }[]
}

function TaskOutputView({ task }: { task: RunTask }) {
  const q = useQuery({
    queryKey: ['run-output', task.run_id],
    queryFn: () => api.get<TaskOutput>(`/api/2.1/jobs/runs/get-output?run_id=${task.run_id}`),
    enabled: task.state.life_cycle_state === 'TERMINATED' || task.state.life_cycle_state === 'INTERNAL_ERROR',
    retry: false,
  })
  if (task.state.life_cycle_state !== 'TERMINATED' && task.state.life_cycle_state !== 'INTERNAL_ERROR') return <div className="muted small">Task is {task.state.life_cycle_state.toLowerCase()}…</div>
  if (q.isLoading) return <Spinner />
  if (q.error) return <ErrorBox error={q.error} />
  const o = q.data
  if (!o) return null
  return (
    <div>
      {o.error && <ErrorBox error={o.error} />}
      {o.error_trace && <pre className="traceback">{o.error_trace}</pre>}
      {o.notebook_output?.result && (
        <Card title="dbutils.notebook.exit()">
          <pre className="result">{o.notebook_output.result}</pre>
        </Card>
      )}
      {o.cells?.map((c) => (
        <div key={c.id} className="cell">
          <pre className="src small">{c.source}</pre>
          <div className="cell-out">{c.outputs?.map((ev, i) => <Output key={i} ev={ev} />)}</div>
        </div>
      ))}
      {o.outputs && !o.cells && <div className="cell-out">{o.outputs.map((ev, i) => <Output key={i} ev={ev} />)}</div>}
      {o.sql_output !== undefined && <JsonView value={o.sql_output} />}
      {o.logs && <pre className="small">{o.logs}</pre>}
      {!o.error && !o.notebook_output?.result && !o.cells && !o.outputs && o.sql_output === undefined && !o.logs && <div className="muted small">No output.</div>}
    </div>
  )
}

export default function RunDetail() {
  const { runId } = useParams()
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const run = useQuery({
    queryKey: ['run', runId],
    queryFn: () => api.get<Run>(`/api/2.1/jobs/runs/get?run_id=${runId}`),
    refetchInterval: (q) => (q.state.data?.state.life_cycle_state === 'TERMINATED' ? false : 2500),
  })
  const [sel, setSel] = useState<string | null>(null)
  const [tab, setTab] = useState('tasks')
  if (run.isLoading) return <Spinner />
  if (!run.data) return <div className="page"><ErrorBox error={run.error ?? 'Run not found'} /></div>
  const r = run.data
  const tasks = r.tasks ?? []
  const active = tasks.find((t) => t.task_key === sel) ?? tasks[0]
  const terminal = r.state.life_cycle_state === 'TERMINATED' || r.state.life_cycle_state === 'INTERNAL_ERROR'
  const failed = tasks.filter((t) => t.state.result_state && t.state.result_state !== 'SUCCESS')
  const cancel = async () => {
    await api.post('/api/2.1/jobs/runs/cancel', { run_id: r.run_id })
    toast('Cancel requested')
    void qc.invalidateQueries({ queryKey: ['run', runId] })
  }
  const repair = async () => {
    try {
      await api.post('/api/2.1/jobs/runs/repair', { run_id: r.run_id, rerun_tasks: failed.map((t) => t.task_key) })
      toast('Repair run started')
      void qc.invalidateQueries({ queryKey: ['run', runId] })
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  const t0 = Math.min(...tasks.map((t) => t.start_time || r.start_time), r.start_time)
  const t1 = Math.max(...tasks.map((t) => t.end_time || Date.now()), r.end_time || Date.now())
  const span = Math.max(1, t1 - t0)
  return (
    <Page
      title={<span>{r.job_id ? <Link to={`/workflows/${r.job_id}`}>Job {r.job_id}</Link> : <Link to="/workflows">Workflows</Link>} / Run #{r.run_id} <Badge state={r.state.result_state ?? r.state.life_cycle_state} /></span>}
      subtitle={`${r.run_name} · ${r.trigger} · started ${fmtTime(r.start_time)} · ${fmtDuration((r.end_time || Date.now()) - r.start_time)}${r.state.state_message ? ` · ${r.state.state_message}` : ''}`}
      actions={
        <>
          {!terminal && <button className="danger" onClick={cancel}>■ Cancel run</button>}
          {terminal && failed.length > 0 && <button className="primary" onClick={repair}>↻ Repair run ({failed.length} failed)</button>}
          <a className="btn" href={`/api/2.1/jobs/runs/export?run_id=${r.run_id}`} target="_blank" rel="noreferrer">Export</a>
        </>
      }
    >
      {node}
      <Card title="Timeline">
        <div className="timeline">
          {tasks.map((t) => {
            const s = t.start_time ? ((t.start_time - t0) / span) * 100 : 0
            const w = t.start_time ? (((t.end_time || Date.now()) - t.start_time) / span) * 100 : 0
            return (
              <div key={t.task_key} className="row" onClick={() => setSel(t.task_key)}>
                <div className="lbl">{t.task_key}</div>
                <div className="track">
                  <div className={`bar ${(t.state.result_state ?? t.state.life_cycle_state).toLowerCase()}`} style={{ left: `${s}%`, width: `${Math.max(0.5, w)}%` }} title={fmtDuration((t.end_time || Date.now()) - (t.start_time || Date.now()))} />
                </div>
                <div className="st"><Badge state={t.state.result_state ?? t.state.life_cycle_state} /></div>
              </div>
            )
          })}
        </div>
      </Card>
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'tasks', label: 'Tasks' }, { id: 'params', label: 'Parameters' }, { id: 'json', label: 'JSON' }]} />
      {tab === 'tasks' && (
        <div className="split">
          <div className="dag">
            {tasks.map((t) => (
              <div key={t.task_key} className={`task ${active?.task_key === t.task_key ? 'selected' : ''}`} onClick={() => setSel(t.task_key)}>
                <div className="k">{t.task_key} <Badge state={t.state.result_state ?? t.state.life_cycle_state} /></div>
                <div className="muted small">{t.notebook_task?.notebook_path ?? (t.sql_task ? 'SQL task' : 'task')}</div>
                <div className="muted small">{fmtDuration((t.end_time || Date.now()) - (t.start_time || Date.now()))}{t.depends_on?.length ? ` · ← ${t.depends_on.map((d) => d.task_key).join(', ')}` : ''}</div>
              </div>
            ))}
          </div>
          {active && (
            <Card title={<span>{active.task_key} <Badge state={active.state.result_state ?? active.state.life_cycle_state} /></span>}>
              <KV items={[['Task run ID', String(active.run_id)], ['Started', fmtTime(active.start_time)], ['Ended', fmtTime(active.end_time)], ['Message', active.state.state_message || '—']]} />
              <h4>Output</h4>
              <TaskOutputView task={active} />
            </Card>
          )}
        </div>
      )}
      {tab === 'params' && <JsonView value={{ job_parameters: r.job_parameters, overriding_parameters: r.overriding_parameters }} />}
      {tab === 'json' && <JsonView value={r} />}
    </Page>
  )
}
