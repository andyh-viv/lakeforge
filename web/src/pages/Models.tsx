import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type ReactNode } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, fmtTime, type ModelVersion, type RegisteredModel } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'

const ML = '/api/2.0/mlflow'
const STAGES = ['None', 'Staging', 'Production', 'Archived']

function ModelDetail({ name }: { name: string }) {
  const qc = useQueryClient()
  const nav = useNavigate()
  const { toast, node } = useToast()
  const [tab, setTab] = useState('versions')
  const [aliasFor, setAliasFor] = useState<ModelVersion | null>(null)
  const [alias, setAlias] = useState('champion')
  const [addVersion, setAddVersion] = useState(false)
  const [vsrc, setVsrc] = useState('')
  const [vrun, setVrun] = useState('')
  const [err, setErr] = useState<unknown>(null)
  const model = useQuery({ queryKey: ['model', name], queryFn: () => api.get<{ registered_model: RegisteredModel }>(`${ML}/registered-models/get?name=${encodeURIComponent(name)}`) })
  const versions = useQuery({ queryKey: ['model-versions', name], queryFn: () => api.get<{ model_versions: ModelVersion[] }>(`${ML}/model-versions/search?filter=${encodeURIComponent(`name='${name}'`)}&order_by=version_number DESC`) })
  const inv = () => {
    void qc.invalidateQueries({ queryKey: ['model', name] })
    void qc.invalidateQueries({ queryKey: ['model-versions', name] })
  }
  const transition = async (v: ModelVersion, stage: string) => {
    try {
      await api.post(`${ML}/model-versions/transition-stage`, { name, version: v.version, stage, archive_existing_versions: stage === 'Production' || stage === 'Staging' })
      toast(`Version ${v.version} → ${stage}`)
      inv()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  if (model.isLoading) return <Spinner />
  if (!model.data) return <ErrorBox error={model.error ?? 'Model not found'} />
  const m = model.data.registered_model
  const vs = versions.data?.model_versions ?? []
  return (
    <Page
      title={<span><Link to="/ml/models">Models</Link> / {m.name}</span>}
      subtitle={m.description || `Registered ${fmtTime(m.creation_timestamp)} · updated ${fmtTime(m.last_updated_timestamp)}`}
      actions={
        <>
          <button onClick={() => setAddVersion(true)}>＋ Register version</button>
          <button onClick={() => nav(`/ml/serving?new=1&model=${encodeURIComponent(m.name)}&version=${vs[0]?.version ?? ''}`)}>Serve this model</button>
          <button onClick={async () => { const d = window.prompt('Description', m.description ?? ''); if (d !== null) { await api.post(`${ML}/registered-models/update`, { name, description: d }); inv() } }}>Edit description</button>
          <button className="danger" onClick={async () => { if (window.confirm(`Delete model ${name} and all versions?`)) { await api.post(`${ML}/registered-models/delete`, { name }); void qc.invalidateQueries({ queryKey: ['models'] }); nav('/ml/models') } }}>Delete</button>
        </>
      }
    >
      {node}
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'versions', label: `Versions (${vs.length})` }, { id: 'aliases', label: `Aliases (${m.aliases?.length ?? 0})` }, { id: 'json', label: 'JSON' }]} />
      {tab === 'versions' && (
        <Table
          rows={vs}
          keyOf={(v) => v.version}
          columns={[
            { key: 'v', title: 'Version', render: (v) => <b>v{v.version}</b> },
            { key: 's', title: 'Stage', render: (v) => <Badge state={v.current_stage === 'Production' ? 'RUNNING' : v.current_stage === 'Staging' ? 'PENDING' : v.current_stage === 'Archived' ? 'TERMINATED' : 'READY'}>{v.current_stage}</Badge> },
            { key: 'a', title: 'Aliases', render: (v) => (m.aliases ?? []).filter((a) => a.version === v.version).map((a) => <code key={a.alias} className="alias">@{a.alias}</code>) },
            { key: 'st', title: 'Status', render: (v) => v.status ?? 'READY' },
            { key: 'r', title: 'Source run', render: (v) => (v.run_id ? <code className="small">{v.run_id.slice(0, 8)}</code> : '—') },
            { key: 'src', title: 'Source', render: (v) => <code className="small">{v.source}</code> },
            { key: 'c', title: 'Created', render: (v) => fmtTime(v.creation_timestamp) },
            {
              key: 'act', title: '', width: '260px',
              render: (v) => (
                <span className="actions">
                  <select value={v.current_stage} onChange={(e) => transition(v, e.target.value)}>{STAGES.map((s) => <option key={s}>{s}</option>)}</select>
                  <button className="sm" onClick={() => { setAliasFor(v); setAlias('champion') }}>Alias</button>
                  <button className="sm danger" onClick={async () => { if (window.confirm(`Delete version ${v.version}?`)) { await api.post(`${ML}/model-versions/delete`, { name, version: v.version }); inv() } }}>✕</button>
                </span>
              ),
            },
          ]}
          emptyText="No versions. Log a model with mlflow.sklearn.log_model(..., registered_model_name=...) or register one manually."
        />
      )}
      {tab === 'aliases' && (
        <Card title="Aliases">
          <KV items={(m.aliases ?? []).map((a) => [`@${a.alias}`, <span>v{a.version} <button className="sm danger" onClick={async () => { await api.delete(`${ML}/registered-models/alias?name=${encodeURIComponent(name)}&alias=${encodeURIComponent(a.alias)}`); inv() }}>remove</button></span>] as [string, ReactNode])} />
          {(m.aliases ?? []).length === 0 && <div className="muted small">No aliases. Aliases (e.g. @champion) give stable names to versions for serving and inference.</div>}
        </Card>
      )}
      {tab === 'json' && <JsonView value={{ model: m, versions: vs }} />}
      {aliasFor && (
        <Modal title={`Set alias on v${aliasFor.version}`} onClose={() => setAliasFor(null)} footer={<><button onClick={() => setAliasFor(null)}>Cancel</button><button className="primary" onClick={async () => { await api.post(`${ML}/registered-models/alias`, { name, alias, version: aliasFor.version }); setAliasFor(null); inv() }}>Set</button></>}>
          <Field label="Alias"><input autoFocus value={alias} onChange={(e) => setAlias(e.target.value.replace(/[^A-Za-z0-9_-]/g, ''))} /></Field>
        </Modal>
      )}
      {addVersion && (
        <Modal title="Register model version" onClose={() => setAddVersion(false)} footer={<><button onClick={() => setAddVersion(false)}>Cancel</button><button className="primary" disabled={!vsrc} onClick={async () => { setErr(null); try { await api.post(`${ML}/model-versions/create`, { name, source: vsrc, run_id: vrun || undefined }); setAddVersion(false); inv() } catch (e) { setErr(e) } }}>Register</button></>}>
          <Field label="Source URI" hint="e.g. runs:/<run_id>/model or dbfs:/models/my-model"><input autoFocus value={vsrc} onChange={(e) => setVsrc(e.target.value)} /></Field>
          <Field label="MLflow run ID (optional)"><input value={vrun} onChange={(e) => setVrun(e.target.value)} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
    </Page>
  )
}

export default function Models() {
  const { name } = useParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [n, setN] = useState('')
  const [desc, setDesc] = useState('')
  const [err, setErr] = useState<unknown>(null)
  const [filter, setFilter] = useState('')
  const list = useQuery({ queryKey: ['models'], queryFn: () => api.get<{ registered_models: RegisteredModel[] }>(`${ML}/registered-models/search?max_results=1000`) })
  if (name) return <ModelDetail name={decodeURIComponent(name)} />
  const rows = (list.data?.registered_models ?? []).filter((m) => m.name.toLowerCase().includes(filter.toLowerCase()))
  return (
    <Page title="Models" subtitle="Model registry: versions, stages and aliases. Deploy a version to a serving endpoint." actions={<button className="primary" onClick={() => setCreating(true)}>＋ Create model</button>}>
      <div className="toolbar"><input style={{ flex: 1 }} placeholder="Filter models" value={filter} onChange={(e) => setFilter(e.target.value)} /></div>
      {list.isLoading && <Spinner />}
      <ErrorBox error={list.error} />
      <Table
        rows={rows}
        keyOf={(m) => m.name}
        onRowClick={(m) => nav(`/ml/models/${encodeURIComponent(m.name)}`)}
        columns={[
          { key: 'n', title: 'Name', render: (m) => <b>{m.name}</b> },
          { key: 'lv', title: 'Latest versions', render: (m) => (m.latest_versions ?? []).map((v) => <span key={v.version} className="small">v{v.version} ({v.current_stage}) </span>) },
          { key: 'a', title: 'Aliases', render: (m) => (m.aliases ?? []).map((a) => <code key={a.alias} className="alias">@{a.alias}=v{a.version}</code>) },
          { key: 'c', title: 'Created', render: (m) => fmtTime(m.creation_timestamp) },
          { key: 'u', title: 'Updated', render: (m) => fmtTime(m.last_updated_timestamp) },
          { key: 'o', title: 'Owner', render: (m) => m.user_id ?? '' },
        ]}
        emptyText="No registered models."
      />
      {creating && (
        <Modal title="Create registered model" onClose={() => setCreating(false)} footer={<><button onClick={() => setCreating(false)}>Cancel</button><button className="primary" disabled={!n.trim()} onClick={async () => { setErr(null); try { await api.post(`${ML}/registered-models/create`, { name: n, description: desc || undefined }); void qc.invalidateQueries({ queryKey: ['models'] }); setCreating(false); nav(`/ml/models/${encodeURIComponent(n)}`) } catch (e) { setErr(e) } }}>Create</button></>}>
          <Field label="Name"><input autoFocus value={n} onChange={(e) => setN(e.target.value)} /></Field>
          <Field label="Description"><textarea rows={2} value={desc} onChange={(e) => setDesc(e.target.value)} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
    </Page>
  )
}
