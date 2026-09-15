import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { api, fmtTime, type RegisteredModel, type ServedEntity, type ServingEndpoint } from '../api'
import { Badge, Card, CodeEditor, ErrorBox, Field, JsonView, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'

interface EntityForm {
  name: string
  kind: 'model' | 'external'
  entity_name: string
  entity_version: string
  workload_size: string
  scale_to_zero_enabled: boolean
  provider: string
  external_name: string
  task: string
  api_key_secret: string
}

const blankEntity = (): EntityForm => ({ name: '', kind: 'model', entity_name: '', entity_version: '1', workload_size: 'Small', scale_to_zero_enabled: true, provider: 'openai', external_name: 'gpt-4o-mini', task: 'llm/v1/chat', api_key_secret: '{{secrets/ml/openai_api_key}}' })

function toEntity(e: EntityForm): Record<string, unknown> {
  const base = { name: e.name || undefined, workload_size: e.workload_size, scale_to_zero_enabled: e.scale_to_zero_enabled }
  if (e.kind === 'external') {
    const cfgKey = `${e.provider}_config`
    return { ...base, external_model: { name: e.external_name, provider: e.provider, task: e.task, [cfgKey]: { [`${e.provider}_api_key`]: e.api_key_secret } } }
  }
  return { ...base, entity_name: e.entity_name, entity_version: e.entity_version }
}

function EndpointEditor({ existing, onClose, onSaved }: { existing?: ServingEndpoint; onClose: () => void; onSaved: (name: string) => void }) {
  const [sp] = useSearchParams()
  const models = useQuery({ queryKey: ['models'], queryFn: () => api.get<{ registered_models: RegisteredModel[] }>('/api/2.0/mlflow/registered-models/search?max_results=1000') })
  const [name, setName] = useState(existing?.name ?? (sp.get('model') ? `${sp.get('model')!.replace(/[^A-Za-z0-9_-]/g, '-')}-endpoint` : ''))
  const [entities, setEntities] = useState<EntityForm[]>(
    existing?.config?.served_entities?.map((s: ServedEntity) => ({
      ...blankEntity(),
      name: s.name,
      kind: s.external_model ? 'external' : 'model',
      entity_name: s.entity_name ?? s.model_name ?? '',
      entity_version: s.entity_version ?? s.model_version ?? '1',
      workload_size: s.workload_size ?? 'Small',
      scale_to_zero_enabled: s.scale_to_zero_enabled ?? true,
      provider: s.external_model?.provider ?? 'openai',
      external_name: s.external_model?.name ?? '',
      task: s.external_model?.task ?? 'llm/v1/chat',
    })) ?? [{ ...blankEntity(), entity_name: sp.get('model') ?? '', entity_version: sp.get('version') || '1' }],
  )
  const [traffic, setTraffic] = useState<number[]>(existing?.config?.traffic_config?.routes.map((r) => r.traffic_percentage) ?? [100])
  const [err, setErr] = useState<unknown>(null)
  const [busy, setBusy] = useState(false)
  const submit = async () => {
    setBusy(true)
    setErr(null)
    try {
      const served = entities.map(toEntity)
      const config = {
        served_entities: served,
        traffic_config: { routes: served.map((s, i) => ({ served_model_name: s.name ?? `${entities[i].entity_name}-${entities[i].entity_version}`, traffic_percentage: traffic[i] ?? 0 })) },
      }
      if (existing) {
        await api.put(`/api/2.0/serving-endpoints/${existing.name}/config`, config)
        onSaved(existing.name)
      } else {
        await api.post('/api/2.0/serving-endpoints', { name, config })
        onSaved(name)
      }
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }
  const upd = (i: number, p: Partial<EntityForm>) => setEntities((es) => es.map((e, j) => (j === i ? { ...e, ...p } : e)))
  return (
    <Modal title={existing ? `Edit ${existing.name}` : 'Create serving endpoint'} onClose={onClose} wide footer={<><button onClick={onClose}>Cancel</button><button className="primary" disabled={busy || !name.trim() || entities.length === 0} onClick={submit}>{existing ? 'Update config' : 'Create'}</button></>}>
      <Field label="Endpoint name" hint="alphanumeric, dashes and underscores"><input autoFocus disabled={!!existing} value={name} onChange={(e) => setName(e.target.value.replace(/[^A-Za-z0-9_-]/g, '-'))} /></Field>
      <h4>Served entities</h4>
      {entities.map((e, i) => (
        <Card key={i} title={<span>Entity {i + 1} <button className="sm danger" style={{ marginLeft: 8 }} onClick={() => { setEntities(entities.filter((_, j) => j !== i)); setTraffic(traffic.filter((_, j) => j !== i)) }}>remove</button></span>}>
          <div className="grid2">
            <Field label="Type">
              <select value={e.kind} onChange={(ev) => upd(i, { kind: ev.target.value as 'model' | 'external' })}>
                <option value="model">Registered model</option>
                <option value="external">External model (OpenAI, Anthropic, …)</option>
              </select>
            </Field>
            <Field label="Served name (optional)"><input value={e.name} onChange={(ev) => upd(i, { name: ev.target.value })} placeholder="auto" /></Field>
            {e.kind === 'model' ? (
              <>
                <Field label="Model">
                  <input list={`models-${i}`} value={e.entity_name} onChange={(ev) => upd(i, { entity_name: ev.target.value })} />
                  <datalist id={`models-${i}`}>{(models.data?.registered_models ?? []).map((m) => <option key={m.name} value={m.name} />)}</datalist>
                </Field>
                <Field label="Version or @alias"><input value={e.entity_version} onChange={(ev) => upd(i, { entity_version: ev.target.value })} /></Field>
              </>
            ) : (
              <>
                <Field label="Provider">
                  <select value={e.provider} onChange={(ev) => upd(i, { provider: ev.target.value })}>
                    {['openai', 'anthropic', 'cohere', 'ai21labs', 'amazon-bedrock', 'google-cloud-vertex-ai', 'palm', 'databricks-model-serving'].map((p) => <option key={p}>{p}</option>)}
                  </select>
                </Field>
                <Field label="External model name"><input value={e.external_name} onChange={(ev) => upd(i, { external_name: ev.target.value })} /></Field>
                <Field label="Task">
                  <select value={e.task} onChange={(ev) => upd(i, { task: ev.target.value })}>
                    {['llm/v1/chat', 'llm/v1/completions', 'llm/v1/embeddings'].map((t) => <option key={t}>{t}</option>)}
                  </select>
                </Field>
                <Field label="API key secret" hint="{{secrets/<scope>/<key>}} reference"><input value={e.api_key_secret} onChange={(ev) => upd(i, { api_key_secret: ev.target.value })} /></Field>
              </>
            )}
            <Field label="Workload size">
              <select value={e.workload_size} onChange={(ev) => upd(i, { workload_size: ev.target.value })}>{['Small', 'Medium', 'Large'].map((s) => <option key={s}>{s}</option>)}</select>
            </Field>
            <Field label="Traffic %"><input type="number" min={0} max={100} value={traffic[i] ?? 0} onChange={(ev) => setTraffic(traffic.map((t, j) => (j === i ? Number(ev.target.value) : t)))} /></Field>
          </div>
          <label className="check"><input type="checkbox" checked={e.scale_to_zero_enabled} onChange={(ev) => upd(i, { scale_to_zero_enabled: ev.target.checked })} /> Scale to zero</label>
        </Card>
      ))}
      <button className="sm" onClick={() => { setEntities([...entities, blankEntity()]); setTraffic([...traffic, 0]) }}>＋ Add served entity</button>
      <ErrorBox error={err} />
    </Modal>
  )
}

function EndpointDetail({ name }: { name: string }) {
  const qc = useQueryClient()
  const nav = useNavigate()
  const { toast, node } = useToast()
  const [tab, setTab] = useState('overview')
  const [edit, setEdit] = useState(false)
  const [payload, setPayload] = useState('{"dataframe_records": [{"x": 1.0}]}')
  const [resp, setResp] = useState<unknown>(null)
  const [respErr, setRespErr] = useState<unknown>(null)
  const [invoking, setInvoking] = useState(false)
  const ep = useQuery({ queryKey: ['endpoint', name], queryFn: () => api.get<ServingEndpoint & Record<string, unknown>>(`/api/2.0/serving-endpoints/${name}`), refetchInterval: 3000 })
  const metrics = useQuery({ queryKey: ['endpoint-metrics', name], queryFn: () => api.text(`/api/2.0/serving-endpoints/${name}/metrics`), refetchInterval: 5000, enabled: tab === 'metrics' })
  const entity0 = ep.data?.config?.served_entities?.[0]?.name
  const logs = useQuery({ queryKey: ['endpoint-logs', name, entity0], queryFn: () => api.get<{ logs: string }>(`/api/2.0/serving-endpoints/${name}/served-entities/${entity0}/logs`), enabled: tab === 'logs' && !!entity0, refetchInterval: 4000 })
  const build = useQuery({ queryKey: ['endpoint-build', name, entity0], queryFn: () => api.get<{ logs: string }>(`/api/2.0/serving-endpoints/${name}/served-entities/${entity0}/build-logs`), enabled: tab === 'logs' && !!entity0 })
  const invoke = async () => {
    setInvoking(true)
    setResp(null)
    setRespErr(null)
    try {
      setResp(await api.post(`/serving-endpoints/${name}/invocations`, JSON.parse(payload)))
    } catch (e) {
      setRespErr(e)
    } finally {
      setInvoking(false)
    }
  }
  if (ep.isLoading) return <Spinner />
  if (!ep.data) return <ErrorBox error={ep.error ?? 'Endpoint not found'} />
  const e = ep.data
  const entities = e.config?.served_entities ?? []
  const url = `${window.location.origin}/serving-endpoints/${name}/invocations`
  return (
    <Page
      title={<span><Link to="/ml/serving">Serving</Link> / {e.name} <Badge state={e.state?.ready === 'READY' ? 'READY' : e.state?.config_update === 'UPDATE_FAILED' ? 'ERROR' : 'PENDING'}>{e.state?.ready}</Badge></span>}
      subtitle={`${e.task ?? 'CUSTOM_MODEL_SERVING'} · config ${e.state?.config_update} · created by ${e.creator} ${fmtTime(e.creation_timestamp)}`}
      actions={
        <>
          <button onClick={() => setEdit(true)}>Edit config</button>
          <button className="danger" onClick={async () => { if (window.confirm('Delete endpoint?')) { await api.delete(`/api/2.0/serving-endpoints/${name}`); toast('Deleted'); void qc.invalidateQueries({ queryKey: ['endpoints'] }); nav('/ml/serving') } }}>Delete</button>
        </>
      }
    >
      {node}
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'overview', label: 'Overview' }, { id: 'query', label: 'Query endpoint' }, { id: 'metrics', label: 'Metrics' }, { id: 'logs', label: 'Logs' }, { id: 'json', label: 'JSON' }]} />
      {tab === 'overview' && (
        <div className="grid2">
          <Card title="Endpoint">
            <KV items={[['URL', <code className="small">{url}</code>], ['State', `${e.state?.ready} / ${e.state?.config_update}`], ['Type', String(e.endpoint_type ?? '')], ['Route optimized', String(e.route_optimized ?? false)]]} />
            <p className="muted small">Call with <code>curl -X POST {url} -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{'{"dataframe_records": [...]}'}'</code></p>
          </Card>
          <Card title="Served entities">
            <Table
              rows={entities}
              keyOf={(s) => s.name}
              columns={[
                { key: 'n', title: 'Name', render: (s) => <b>{s.name}</b> },
                { key: 'e', title: 'Entity', render: (s) => (s.external_model ? `${s.external_model.provider}/${s.external_model.name}` : <Link to={`/ml/models/${encodeURIComponent(s.entity_name ?? '')}`}>{s.entity_name} v{s.entity_version}</Link>) },
                { key: 's', title: 'Size', render: (s) => `${s.workload_size}${s.scale_to_zero_enabled ? ' · scale-to-zero' : ''}` },
                { key: 't', title: 'Traffic', render: (s) => `${e.config?.traffic_config?.routes.find((r) => r.served_model_name === s.name)?.traffic_percentage ?? 0}%` },
                { key: 'st', title: 'Deployment', render: (s) => <Badge state={s.state?.deployment === 'DEPLOYMENT_READY' ? 'READY' : 'PENDING'}>{s.state?.deployment}</Badge> },
              ]}
              emptyText="No served entities."
            />
          </Card>
        </div>
      )}
      {tab === 'query' && (
        <div className="split">
          <Card title="Request">
            <CodeEditor value={payload} onChange={setPayload} language="json" onRun={invoke} minRows={10} />
            <div className="actions" style={{ marginTop: 8 }}>
              <button className="primary" disabled={invoking || e.state?.ready !== 'READY'} onClick={invoke}>{invoking ? 'Sending…' : 'Send request'}</button>
              <button className="sm" onClick={() => setPayload('{"dataframe_split": {"columns": ["x"], "data": [[1.0], [2.0]]}}')}>dataframe_split</button>
              <button className="sm" onClick={() => setPayload('{"inputs": [[1.0, 2.0]]}')}>inputs</button>
              <button className="sm" onClick={() => setPayload('{"messages": [{"role": "user", "content": "Hello"}], "max_tokens": 64}')}>chat</button>
            </div>
            {e.state?.ready !== 'READY' && <div className="muted small">Endpoint is not ready yet.</div>}
          </Card>
          <Card title="Response">
            <ErrorBox error={respErr} />
            {resp !== null && <JsonView value={resp} />}
          </Card>
        </div>
      )}
      {tab === 'metrics' && <Card title="Prometheus metrics"><pre className="small">{metrics.data ?? '…'}</pre></Card>}
      {tab === 'logs' && (
        <div className="grid2">
          <Card title="Build logs"><pre className="small">{build.data?.logs ?? '…'}</pre></Card>
          <Card title="Server logs"><pre className="small">{logs.data?.logs ?? '…'}</pre></Card>
        </div>
      )}
      {tab === 'json' && <JsonView value={e} />}
      {edit && <EndpointEditor existing={e} onClose={() => setEdit(false)} onSaved={() => { setEdit(false); void qc.invalidateQueries({ queryKey: ['endpoint', name] }) }} />}
    </Page>
  )
}

export default function Serving() {
  const { name } = useParams()
  const [sp, setSp] = useSearchParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(sp.get('new') === '1')
  const list = useQuery({ queryKey: ['endpoints'], queryFn: () => api.get<{ endpoints: ServingEndpoint[] }>('/api/2.0/serving-endpoints'), refetchInterval: 5000 })
  if (name) return <EndpointDetail name={name} />
  return (
    <Page title="Serving" subtitle="Real-time model serving endpoints for registered models and external LLM providers." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create serving endpoint</button>}>
      {list.isLoading && <Spinner />}
      <ErrorBox error={list.error} />
      <Table
        rows={list.data?.endpoints ?? []}
        keyOf={(e) => e.name}
        onRowClick={(e) => nav(`/ml/serving/${e.name}`)}
        columns={[
          { key: 'n', title: 'Name', render: (e) => <b>{e.name}</b> },
          { key: 's', title: 'State', render: (e) => <Badge state={e.state?.ready === 'READY' ? 'READY' : e.state?.config_update === 'UPDATE_FAILED' ? 'ERROR' : 'PENDING'}>{e.state?.ready}</Badge> },
          { key: 'c', title: 'Config', render: (e) => e.state?.config_update },
          { key: 'ent', title: 'Served', render: (e) => (e.config?.served_entities ?? []).map((s) => s.external_model ? `${s.external_model.provider}/${s.external_model.name}` : `${s.entity_name} v${s.entity_version}`).join(', ') },
          { key: 'cr', title: 'Creator', render: (e) => e.creator },
          { key: 'u', title: 'Updated', render: (e) => fmtTime(e.last_updated_timestamp) },
        ]}
        emptyText="No serving endpoints."
      />
      {creating && <EndpointEditor onClose={() => { setCreating(false); setSp({}) }} onSaved={(n) => { setCreating(false); setSp({}); void qc.invalidateQueries({ queryKey: ['endpoints'] }); nav(`/ml/serving/${n}`) }} />}
    </Page>
  )
}
