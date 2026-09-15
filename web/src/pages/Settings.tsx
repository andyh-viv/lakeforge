import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { api, fmtTime, type TokenInfo } from '../api'
import { Card, ErrorBox, Field, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'
import { useAuth } from '../auth'

function TokensTab() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const toks = useQuery({ queryKey: ['my-tokens'], queryFn: () => api.get<{ token_infos: TokenInfo[] }>('/api/2.0/token/list') })
  const [gen, setGen] = useState(false)
  const [comment, setComment] = useState('')
  const [days, setDays] = useState('90')
  const [created, setCreated] = useState<string | null>(null)
  const [err, setErr] = useState<unknown>(null)
  const create = async () => {
    setErr(null)
    try {
      const r = await api.post<{ token_value: string }>('/api/2.0/token/create', { comment, lifetime_seconds: days ? Number(days) * 86400 : undefined })
      setCreated(r.token_value)
      setGen(false)
      setComment('')
      void qc.invalidateQueries({ queryKey: ['my-tokens'] })
    } catch (e) {
      setErr(e)
    }
  }
  return (
    <>
      {node}
      <Card title="Personal access tokens" actions={<button className="sm primary" onClick={() => setGen(true)}>＋ Generate new token</button>}>
        <p className="muted small">Use tokens with the Databricks CLI/SDKs: <code>DATABRICKS_HOST={window.location.origin}</code> <code>DATABRICKS_TOKEN=dapi…</code></p>
        {toks.isLoading && <Spinner />}
        <Table
          rows={toks.data?.token_infos ?? []}
          keyOf={(t) => t.token_id}
          columns={[
            { key: 'c', title: 'Comment', render: (t) => t.comment || <span className="muted">—</span> },
            { key: 'cr', title: 'Created', render: (t) => fmtTime(t.creation_time) },
            { key: 'ex', title: 'Expires', render: (t) => (t.expiry_time > 0 ? fmtTime(t.expiry_time) : 'never') },
            { key: 'a', title: '', width: '80px', render: (t) => <button className="sm danger" onClick={async () => { if (window.confirm('Revoke token?')) { await api.post('/api/2.0/token/delete', { token_id: t.token_id }); void qc.invalidateQueries({ queryKey: ['my-tokens'] }); toast('Revoked') } }}>Revoke</button> },
          ]}
          emptyText="No tokens yet."
        />
      </Card>
      {gen && (
        <Modal title="Generate token" onClose={() => setGen(false)} footer={<><button onClick={() => setGen(false)}>Cancel</button><button className="primary" onClick={create}>Generate</button></>}>
          <Field label="Comment"><input autoFocus value={comment} onChange={(e) => setComment(e.target.value)} placeholder="e.g. laptop CLI" /></Field>
          <Field label="Lifetime (days)" hint="empty = no expiry"><input type="number" min={1} value={days} onChange={(e) => setDays(e.target.value)} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
      {created && (
        <Modal title="Token created" onClose={() => setCreated(null)} footer={<button className="primary" onClick={() => { void navigator.clipboard?.writeText(created); toast('Copied') }}>Copy</button>}>
          <p>Copy this token now. It will not be shown again.</p>
          <pre className="mono" style={{ userSelect: 'all', wordBreak: 'break-all', whiteSpace: 'pre-wrap' }}>{created}</pre>
        </Modal>
      )}
    </>
  )
}

function ProfileTab() {
  const { me, refresh } = useAuth()
  const { toast, node } = useToast()
  const [pw, setPw] = useState({ old: '', new1: '', new2: '' })
  const [err, setErr] = useState<unknown>(null)
  const change = async () => {
    setErr(null)
    if (pw.new1 !== pw.new2) return setErr(new Error('Passwords do not match'))
    try {
      await api.post('/api/2.0/lakeforge/password', { old_password: pw.old, new_password: pw.new1 })
      toast('Password changed')
      setPw({ old: '', new1: '', new2: '' })
      await refresh()
    } catch (e) {
      setErr(e)
    }
  }
  return (
    <>
      {node}
      <Card title="Profile">
        <KV items={[['User', me?.user_name ?? ''], ['Display name', me?.display_name ?? ''], ['User ID', <code className="small">{me?.user_id}</code>], ['Groups', (me?.groups ?? []).join(', ')], ['Role', me?.is_admin ? 'Workspace admin' : 'User'], ['Workspace', <code className="small">{me?.workspace_id}</code>], ['Cloud', me?.cloud ?? ''], ['Version', me?.version ?? '']]} />
      </Card>
      <Card title="Change password">
        <div className="grid2">
          <Field label="Current password"><input type="password" value={pw.old} onChange={(e) => setPw({ ...pw, old: e.target.value })} /></Field>
          <span />
          <Field label="New password"><input type="password" value={pw.new1} onChange={(e) => setPw({ ...pw, new1: e.target.value })} /></Field>
          <Field label="Confirm"><input type="password" value={pw.new2} onChange={(e) => setPw({ ...pw, new2: e.target.value })} /></Field>
        </div>
        <button className="primary" disabled={!pw.new1 || pw.new1.length < 4} onClick={change}>Update password</button>
        <ErrorBox error={err} />
      </Card>
    </>
  )
}

function DeveloperTab() {
  const { me } = useAuth()
  const info = useQuery({ queryKey: ['info'], queryFn: () => api.get<Record<string, unknown>>('/api/2.0/lakeforge/info') })
  const host = window.location.origin
  return (
    <>
      <Card title="Connect">
        <p className="muted small">Lakeforge speaks the Databricks REST API, so existing tooling works by pointing it at this workspace.</p>
        <h4>Databricks CLI</h4>
        <pre className="mono small">{`databricks configure --host ${host}\n# paste a personal access token when prompted\ndatabricks clusters list\ndatabricks fs ls dbfs:/`}</pre>
        <h4>Python SDK</h4>
        <pre className="mono small">{`from databricks.sdk import WorkspaceClient\nw = WorkspaceClient(host="${host}", token="dapi...")\nfor c in w.clusters.list():\n    print(c.cluster_name, c.state)`}</pre>
        <h4>SQL statement execution</h4>
        <pre className="mono small">{`curl -s ${host}/api/2.0/sql/statements \\\n  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\\n  -d '{"warehouse_id":"<id>","statement":"SELECT 1","wait_timeout":"30s"}'`}</pre>
        <h4>Lakeforge SDK</h4>
        <pre className="mono small">{`pip install lakeforge\nimport lakeforge\nspark = lakeforge.connect("${host}", token="dapi...")\nspark.sql("SELECT 1").show()`}</pre>
      </Card>
      <Card title="Workspace">
        {info.isLoading && <Spinner />}
        <KV items={Object.entries(info.data ?? {}).filter(([k]) => k !== 'features').map(([k, v]) => [k, typeof v === 'object' ? JSON.stringify(v) : String(v)])} />
        <div className="muted small">Signed in as {me?.user_name}</div>
      </Card>
    </>
  )
}

export default function Settings() {
  const [tab, setTab] = useState('tokens')
  return (
    <Page title="User settings">
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'tokens', label: 'Access tokens' }, { id: 'profile', label: 'Profile' }, { id: 'dev', label: 'Developer' }]} />
      {tab === 'tokens' && <TokensTab />}
      {tab === 'profile' && <ProfileTab />}
      {tab === 'dev' && <DeveloperTab />}
    </Page>
  )
}
