import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { api, fmtDuration, fmtTime, type Cluster, type Job, type JobSettings, type JobTask, type Pipeline, type Run, type Warehouse, type WorkspaceObject } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'

type TaskKind = 'notebook' | 'sql' | 'python' | 'pipeline' | 'run_job'

interface TaskForm {
  task_key: string
  kind: TaskKind
  depends_on: string[]
  cluster_id: string
  notebook_path: string
  base_parameters: string
  warehouse_id: string
  query_text: string
  python_file: string
  pipeline_id: string
  job_id: string
  max_retries: number
  timeout_seconds: number
}

const blankTask = (n: number): TaskForm => ({
  task_key: `task_${n}`,
  kind: 'notebook',
  depends_on: [],
  cluster_id: '',
  notebook_path: '',
  base_parameters: '',
  warehouse_id: '',
  query_text: 'SELECT 1',
  python_file: '',
  pipeline_id: '',
  job_id: '',
  max_retries: 0,
  timeout_seconds: 0,
})

function fromTask(t: JobTask): TaskForm {
  const f = blankTask(0)
  f.task_key = t.task_key
  f.depends_on = (t.depends_on ?? []).map((d) => d.task_key)
  f.cluster_id = t.existing_cluster_id ?? ''
  f.max_retries = t.max_retries ?? 0
  f.timeout_seconds = t.timeout_seconds ?? 0
  if (t.notebook_task) {
    f.kind = 'notebook'
    f.notebook_path = t.notebook_task.notebook_path
    f.base_parameters = Object.entries(t.notebook_task.base_parameters ?? {}).map(([k, v]) => `${k}=${v}`).join('\n')
  } else if (t.sql_task) {
    f.kind = 'sql'
    f.warehouse_id = t.sql_task.warehouse_id ?? ''
    f.query_text = t.sql_task.query?.query_text ?? ''
  } else if (t.spark_python_task) {
    f.kind = 'python'
    f.python_file = t.spark_python_task.python_file
  } else if (t.pipeline_task) {
    f.kind = 'pipeline'
    f.pipeline_id = t.pipeline_task.pipeline_id
  } else if (t.run_job_task) {
    f.kind = 'run_job'
    f.job_id = String(t.run_job_task.job_id)
  }
  return f
}

function toTask(f: TaskForm): JobTask {
  const t: JobTask = { task_key: f.task_key, depends_on: f.depends_on.map((k) => ({ task_key: k })), max_retries: f.max_retries || undefined, timeout_seconds: f.timeout_seconds || undefined }
  if (f.kind !== 'sql' && f.kind !== 'pipeline' && f.kind !== 'run_job' && f.cluster_id) t.existing_cluster_id = f.cluster_id
  switch (f.kind) {
    case 'notebook': {
      const bp: Record<string, string> = {}
      for (const line of f.base_parameters.split('\n')) {
        const [k, ...v] = line.split('=')
        if (k.trim()) bp[k.trim()] = v.join('=').trim()
      }
      t.notebook_task = { notebook_path: f.notebook_path, base_parameters: bp }
      break
    }
    case 'sql':
      t.sql_task = { warehouse_id: f.warehouse_id || undefined, query: { query_text: f.query_text } }
      break
    case 'python':
      t.spark_python_task = { python_file: f.python_file }
      break
    case 'pipeline':
      t.pipeline_task = { pipeline_id: f.pipeline_id }
      break
    case 'run_job':
      t.run_job_task = { job_id: Number(f.job_id) }
      break
  }
  return t
}

function NotebookPicker({ value, onChange }: { value: string; onChange: (p: string) => void }) {
  const q = useQuery({ queryKey: ['ws-search', 'notebooks'], queryFn: () => api.get<{ objects: WorkspaceObject[] }>('/api/2.0/lakeforge/workspace/search?path=/') })
  const nbs = (q.data?.objects ?? []).filter((o) => o.object_type === 'NOTEBOOK')
  return (
    <>
      <input list="nb-list" value={value} onChange={(e) => onChange(e.target.value)} placeholder="/Users/you/notebook" />
      <datalist id="nb-list">{nbs.map((n) => <option key={n.path} value={n.path} />)}</datalist>
    </>
  )
}

export function JobEditor({ job, onClose, onSaved }: { job?: Job; onClose: () => void; onSaved: (id: number) => void }) {
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: () => api.get<{ clusters: Cluster[] }>('/api/2.0/clusters/list') })
  const warehouses = useQuery({ queryKey: ['warehouses'], queryFn: () => api.get<{ warehouses: Warehouse[] }>('/api/2.0/sql/warehouses') })
  const pipelines = useQuery({ queryKey: ['pipelines'], queryFn: () => api.get<{ statuses: Pipeline[] }>('/api/2.0/pipelines') })
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => api.get<{ jobs: Job[] }>('/api/2.1/jobs/list?limit=100&expand_tasks=true') })
  const [name, setName] = useState(job?.settings.name ?? 'New job')
  const [tasks, setTasks] = useState<TaskForm[]>(job?.settings.tasks?.map(fromTask) ?? [blankTask(1)])
  const [cron, setCron] = useState(job?.settings.schedule?.quartz_cron_expression ?? '')
  const [tz, setTz] = useState(job?.settings.schedule?.timezone_id ?? 'UTC')
  const [paused, setPaused] = useState(job?.settings.schedule?.pause_status === 'PAUSED')
  const [params, setParams] = useState((job?.settings.parameters ?? []).map((p) => `${p.name}=${p.default}`).join('\n'))
  const [maxConc, setMaxConc] = useState(job?.settings.max_concurrent_runs ?? 1)
  const [timeout, setTimeout_] = useState(job?.settings.timeout_seconds ?? 0)
  const [tags, setTags] = useState(Object.entries(job?.settings.tags ?? {}).map(([k, v]) => `${k}=${v}`).join('\n'))
  const [err, setErr] = useState<unknown>(null)
  const [busy, setBusy] = useState(false)
  const [cur, setCur] = useState(0)

  const kv = (s: string) => Object.fromEntries(s.split('\n').map((l) => l.split('=')).filter(([k]) => k?.trim()).map(([k, ...v]) => [k.trim(), v.join('=').trim()]))
  const submit = async () => {
    setBusy(true)
    setErr(null)
    try {
      const settings: JobSettings = {
        name,
        format: 'MULTI_TASK',
        max_concurrent_runs: maxConc,
        timeout_seconds: timeout || undefined,
        tasks: tasks.map(toTask),
        parameters: Object.entries(kv(params)).map(([n, d]) => ({ name: n, default: d })),
        tags: kv(tags),
        schedule: cron ? { quartz_cron_expression: cron, timezone_id: tz, pause_status: paused ? 'PAUSED' : 'UNPAUSED' } : null,
      }
      if (job) {
        await api.post('/api/2.1/jobs/reset', { job_id: job.job_id, new_settings: settings })
        onSaved(job.job_id)
      } else {
        const r = await api.post<{ job_id: number }>('/api/2.1/jobs/create', settings)
        onSaved(r.job_id)
      }
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }
  const t = tasks[cur]
  const upd = (patch: Partial<TaskForm>) => setTasks((ts) => ts.map((x, i) => (i === cur ? { ...x, ...patch } : x)))
  return (
    <Modal title={job ? `Edit ${job.settings.name}` : 'Create job'} onClose={onClose} wide footer={<><button onClick={onClose}>Cancel</button><button className="primary" disabled={busy || !name.trim()} onClick={submit}>{job ? 'Save' : 'Create'}</button></>}>
      <Field label="Job name"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} /></Field>
      <div className="split">
        <div>
          <h4>Tasks</h4>
          <div className="dag">
            {tasks.map((x, i) => (
              <div key={i} className={`task ${i === cur ? 'selected' : ''}`} onClick={() => setCur(i)}>
                <div className="k">{x.task_key}</div>
                <div className="muted small">{x.kind}{x.depends_on.length ? ` ← ${x.depends_on.join(', ')}` : ''}</div>
              </div>
            ))}
          </div>
          <div className="actions" style={{ marginTop: 8 }}>
            <button className="sm" onClick={() => { setTasks([...tasks, { ...blankTask(tasks.length + 1), depends_on: t ? [t.task_key] : [] }]); setCur(tasks.length) }}>＋ Add task</button>
            {tasks.length > 1 && <button className="sm danger" onClick={() => { setTasks(tasks.filter((_, i) => i !== cur)); setCur(0) }}>Remove</button>}
          </div>
        </div>
        {t && (
          <div>
            <div className="grid2">
              <Field label="Task key"><input value={t.task_key} onChange={(e) => upd({ task_key: e.target.value.replace(/[^A-Za-z0-9_-]/g, '_') })} /></Field>
              <Field label="Type">
                <select value={t.kind} onChange={(e) => upd({ kind: e.target.value as TaskKind })}>
                  <option value="notebook">Notebook</option>
                  <option value="sql">SQL</option>
                  <option value="python">Python script</option>
                  <option value="pipeline">DLT pipeline</option>
                  <option value="run_job">Run job</option>
                </select>
              </Field>
            </div>
            {t.kind === 'notebook' && (
              <>
                <Field label="Notebook path"><NotebookPicker value={t.notebook_path} onChange={(p) => upd({ notebook_path: p })} /></Field>
                <Field label="Base parameters" hint="key=value per line; read via dbutils.widgets.get"><textarea rows={2} value={t.base_parameters} onChange={(e) => upd({ base_parameters: e.target.value })} /></Field>
              </>
            )}
            {t.kind === 'sql' && (
              <>
                <Field label="Warehouse">
                  <select value={t.warehouse_id} onChange={(e) => upd({ warehouse_id: e.target.value })}>
                    <option value="">(any running)</option>
                    {(warehouses.data?.warehouses ?? []).map((w) => <option key={w.id} value={w.id}>{w.name}</option>)}
                  </select>
                </Field>
                <Field label="SQL"><textarea rows={4} className="mono" value={t.query_text} onChange={(e) => upd({ query_text: e.target.value })} /></Field>
              </>
            )}
            {t.kind === 'python' && <Field label="Python file" hint="workspace path or dbfs:/ path"><input value={t.python_file} onChange={(e) => upd({ python_file: e.target.value })} /></Field>}
            {t.kind === 'pipeline' && (
              <Field label="Pipeline">
                <select value={t.pipeline_id} onChange={(e) => upd({ pipeline_id: e.target.value })}>
                  <option value="">— select —</option>
                  {(pipelines.data?.statuses ?? []).map((p) => <option key={p.pipeline_id} value={p.pipeline_id}>{p.name}</option>)}
                </select>
              </Field>
            )}
            {t.kind === 'run_job' && (
              <Field label="Job">
                <select value={t.job_id} onChange={(e) => upd({ job_id: e.target.value })}>
                  <option value="">— select —</option>
                  {(jobs.data?.jobs ?? []).filter((j) => j.job_id !== job?.job_id).map((j) => <option key={j.job_id} value={j.job_id}>{j.settings.name}</option>)}
                </select>
              </Field>
            )}
            {(t.kind === 'notebook' || t.kind === 'python') && (
              <Field label="Cluster" hint="leave empty to auto-provision a job cluster">
                <select value={t.cluster_id} onChange={(e) => upd({ cluster_id: e.target.value })}>
                  <option value="">(new job cluster)</option>
                  {(clusters.data?.clusters ?? []).map((c) => <option key={c.cluster_id} value={c.cluster_id}>{c.cluster_name} ({c.state})</option>)}
                </select>
              </Field>
            )}
            <Field label="Depends on">
              <div className="actions">
                {tasks.filter((x) => x.task_key !== t.task_key).map((x) => (
                  <label key={x.task_key} className="check"><input type="checkbox" checked={t.depends_on.includes(x.task_key)} onChange={(e) => upd({ depends_on: e.target.checked ? [...t.depends_on, x.task_key] : t.depends_on.filter((k) => k !== x.task_key) })} /> {x.task_key}</label>
                ))}
                {tasks.length === 1 && <span className="muted small">Add another task to define dependencies.</span>}
              </div>
            </Field>
            <div className="grid2">
              <Field label="Max retries"><input type="number" value={t.max_retries} onChange={(e) => upd({ max_retries: Number(e.target.value) })} /></Field>
              <Field label="Timeout (s)"><input type="number" value={t.timeout_seconds} onChange={(e) => upd({ timeout_seconds: Number(e.target.value) })} /></Field>
            </div>
          </div>
        )}
      </div>
      <h4>Job settings</h4>
      <div className="grid2">
        <Field label="Schedule (Quartz cron)" hint="e.g. 0 0 * * * ? — leave empty for manual"><input value={cron} onChange={(e) => setCron(e.target.value)} placeholder="0 0 8 * * ?" /></Field>
        <Field label="Timezone"><input value={tz} onChange={(e) => setTz(e.target.value)} /></Field>
        <Field label="Job parameters" hint="name=default per line; reference as {{job.parameters.name}}"><textarea rows={2} value={params} onChange={(e) => setParams(e.target.value)} /></Field>
        <Field label="Tags" hint="key=value per line"><textarea rows={2} value={tags} onChange={(e) => setTags(e.target.value)} /></Field>
        <Field label="Max concurrent runs"><input type="number" value={maxConc} onChange={(e) => setMaxConc(Number(e.target.value))} /></Field>
        <Field label="Job timeout (s)"><input type="number" value={timeout} onChange={(e) => setTimeout_(Number(e.target.value))} /></Field>
      </div>
      {cron && <label className="check"><input type="checkbox" checked={paused} onChange={(e) => setPaused(e.target.checked)} /> Schedule paused</label>}
      <ErrorBox error={err} />
    </Modal>
  )
}

export function RunsTable({ runs, showJob }: { runs: Run[]; showJob?: boolean }) {
  const nav = useNavigate()
  return (
    <Table
      rows={runs}
      keyOf={(r) => r.run_id}
      onRowClick={(r) => nav(`/runs/${r.run_id}`)}
      columns={[
        { key: 'id', title: 'Run', render: (r) => <span><b>#{r.run_id}</b> {r.run_name}</span> },
        ...(showJob ? [{ key: 'job', title: 'Job', render: (r: Run) => (r.job_id ? <Link to={`/workflows/${r.job_id}`} onClick={(e) => e.stopPropagation()}>{r.job_id}</Link> : '—') }] : []),
        { key: 'state', title: 'Status', render: (r) => <Badge state={r.state.result_state ?? r.state.life_cycle_state} /> },
        { key: 'trigger', title: 'Trigger', render: (r) => r.trigger },
        { key: 'start', title: 'Start', render: (r) => fmtTime(r.start_time) },
        { key: 'dur', title: 'Duration', render: (r) => fmtDuration((r.end_time || Date.now()) - r.start_time) },
        { key: 'tasks', title: 'Tasks', render: (r) => `${(r.tasks ?? []).filter((t) => t.state.result_state === 'SUCCESS').length}/${r.tasks?.length ?? 0}` },
      ]}
      emptyText="No runs."
    />
  )
}

function JobDetail({ jobId }: { jobId: number }) {
  const qc = useQueryClient()
  const nav = useNavigate()
  const { toast, node } = useToast()
  const [tab, setTab] = useState('runs')
  const [edit, setEdit] = useState(false)
  const [runParams, setRunParams] = useState(false)
  const [pv, setPv] = useState('')
  const job = useQuery({ queryKey: ['job', jobId], queryFn: () => api.get<Job>(`/api/2.1/jobs/get?job_id=${jobId}`) })
  const runs = useQuery({ queryKey: ['runs', jobId], queryFn: () => api.get<{ runs: Run[] }>(`/api/2.1/jobs/runs/list?job_id=${jobId}&limit=50&expand_tasks=true`), refetchInterval: 4000 })
  const runNow = async (params?: Record<string, string>) => {
    try {
      const r = await api.post<{ run_id: number }>('/api/2.1/jobs/run-now', { job_id: jobId, job_parameters: params ?? {} })
      toast(`Run #${r.run_id} started`)
      setRunParams(false)
      void qc.invalidateQueries({ queryKey: ['runs'] })
      nav(`/runs/${r.run_id}`)
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  if (job.isLoading) return <Spinner />
  if (!job.data) return <ErrorBox error={job.error ?? 'Job not found'} />
  const j = job.data
  const tasks = j.settings.tasks ?? []
  return (
    <Page
      title={<span><Link to="/workflows">Workflows</Link> / {j.settings.name}</span>}
      subtitle={`Job ${j.job_id} · created by ${j.creator_user_name} · ${fmtTime(j.created_time)}`}
      actions={
        <>
          <button onClick={() => setEdit(true)}>Edit</button>
          <button onClick={() => { setPv((j.settings.parameters ?? []).map((p) => `${p.name}=${p.default}`).join('\n')); setRunParams(true) }}>Run with parameters</button>
          <button className="primary" onClick={() => runNow()}>▶ Run now</button>
          <button className="danger" onClick={async () => { if (window.confirm('Delete job?')) { await api.post('/api/2.1/jobs/delete', { job_id: jobId }); void qc.invalidateQueries({ queryKey: ['jobs'] }); nav('/workflows') } }}>Delete</button>
        </>
      }
    >
      {node}
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'runs', label: `Runs (${runs.data?.runs.length ?? 0})` }, { id: 'tasks', label: `Tasks (${tasks.length})` }, { id: 'json', label: 'JSON' }]} />
      {tab === 'runs' && <RunsTable runs={runs.data?.runs ?? []} />}
      {tab === 'tasks' && (
        <div className="grid2">
          <Card title="Task graph">
            <div className="dag">
              {tasks.map((t) => (
                <div key={t.task_key} className="task">
                  <div className="k">{t.task_key}</div>
                  <div className="muted small">
                    {t.notebook_task ? `notebook ${t.notebook_task.notebook_path}` : t.sql_task ? 'sql' : t.spark_python_task ? `python ${t.spark_python_task.python_file}` : t.pipeline_task ? 'pipeline' : t.run_job_task ? `job ${t.run_job_task.job_id}` : 'task'}
                  </div>
                  {t.depends_on?.length ? <div className="small">← {t.depends_on.map((d) => d.task_key).join(', ')}</div> : null}
                </div>
              ))}
            </div>
          </Card>
          <Card title="Schedule & parameters">
            <KV
              items={[
                ['Schedule', j.settings.schedule ? `${j.settings.schedule.quartz_cron_expression} (${j.settings.schedule.timezone_id}) ${j.settings.schedule.pause_status ?? ''}` : 'Manual'],
                ['Max concurrent runs', String(j.settings.max_concurrent_runs ?? 1)],
                ['Parameters', (j.settings.parameters ?? []).map((p) => `${p.name}=${p.default}`).join(', ') || '—'],
                ['Tags', Object.entries(j.settings.tags ?? {}).map(([k, v]) => `${k}=${v}`).join(', ') || '—'],
              ]}
            />
          </Card>
        </div>
      )}
      {tab === 'json' && <JsonView value={j} />}
      {edit && <JobEditor job={j} onClose={() => setEdit(false)} onSaved={() => { setEdit(false); void qc.invalidateQueries({ queryKey: ['job', jobId] }) }} />}
      {runParams && (
        <Modal title="Run with parameters" onClose={() => setRunParams(false)} footer={<><button onClick={() => setRunParams(false)}>Cancel</button><button className="primary" onClick={() => runNow(Object.fromEntries(pv.split('\n').filter((l) => l.includes('=')).map((l) => { const [k, ...v] = l.split('='); return [k.trim(), v.join('=').trim()] })))}>Run</button></>}>
          <Field label="Job parameters (name=value per line)"><textarea rows={5} value={pv} onChange={(e) => setPv(e.target.value)} /></Field>
        </Modal>
      )}
    </Page>
  )
}

export default function Workflows() {
  const { jobId } = useParams()
  const [sp, setSp] = useSearchParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [tab, setTab] = useState('jobs')
  const [creating, setCreating] = useState(sp.get('new') === '1')
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => api.get<{ jobs: Job[] }>('/api/2.1/jobs/list?limit=100&expand_tasks=true'), refetchInterval: 10000 })
  const runs = useQuery({ queryKey: ['runs', 'all'], queryFn: () => api.get<{ runs: Run[] }>('/api/2.1/jobs/runs/list?limit=100&expand_tasks=true'), refetchInterval: 5000 })
  useEffect(() => {
    if (sp.get('new') === '1') setCreating(true)
  }, [sp])
  if (jobId) return <JobDetail jobId={Number(jobId)} />
  const lastRun = (id: number) => runs.data?.runs.find((r) => r.job_id === id)
  return (
    <Page title="Workflows" subtitle="Multi-task jobs with notebook, SQL, Python, pipeline and run-job tasks." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create job</button>}>
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'jobs', label: 'Jobs' }, { id: 'runs', label: 'Job runs' }]} />
      {jobs.isLoading && <Spinner />}
      <ErrorBox error={jobs.error} />
      {tab === 'jobs' && (
        <Table
          rows={jobs.data?.jobs ?? []}
          keyOf={(j) => j.job_id}
          onRowClick={(j) => nav(`/workflows/${j.job_id}`)}
          columns={[
            { key: 'name', title: 'Name', render: (j) => <b>{j.settings.name}</b> },
            { key: 'id', title: 'Job ID', render: (j) => j.job_id },
            { key: 'tasks', title: 'Tasks', render: (j) => j.settings.tasks?.length ?? 0 },
            { key: 'sched', title: 'Schedule', render: (j) => (j.settings.schedule ? <code className="small">{j.settings.schedule.quartz_cron_expression}</code> : 'Manual') },
            { key: 'last', title: 'Last run', render: (j) => { const r = lastRun(j.job_id); return r ? <span><Badge state={r.state.result_state ?? r.state.life_cycle_state} /> {fmtTime(r.start_time)}</span> : '—' } },
            { key: 'creator', title: 'Creator', render: (j) => j.creator_user_name },
            {
              key: 'act', title: '', width: '120px',
              render: (j) => (
                <span className="actions" onClick={(e) => e.stopPropagation()}>
                  <button className="sm" onClick={async () => { const r = await api.post<{ run_id: number }>('/api/2.1/jobs/run-now', { job_id: j.job_id }); void qc.invalidateQueries({ queryKey: ['runs'] }); nav(`/runs/${r.run_id}`) }}>▶ Run</button>
                </span>
              ),
            },
          ]}
          emptyText="No jobs yet."
        />
      )}
      {tab === 'runs' && <RunsTable runs={runs.data?.runs ?? []} showJob />}
      {creating && <JobEditor onClose={() => { setCreating(false); setSp({}) }} onSaved={(id) => { setCreating(false); void qc.invalidateQueries({ queryKey: ['jobs'] }); nav(`/workflows/${id}`) }} />}
    </Page>
  )
}
