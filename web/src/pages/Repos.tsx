import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { api, fmtTime, type Repo } from '../api'
import { Badge, Card, ErrorBox, Field, KV, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'
import { useAuth } from '../auth'

interface RepoStatus {
  branch: string
  head_commit_id?: string
  changes: { status: string; path: string }[]
  branches: string[]
  commits: { sha: string; author: string; time: number; message: string }[]
}
interface GitCred {
  credential_id: number
  git_provider: string
  git_username?: string
}

function RepoPanel({ repo, onClose }: { repo: Repo; onClose: () => void }) {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const [tab, setTab] = useState('changes')
  const [msg, setMsg] = useState('')
  const [push, setPush] = useState(true)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<unknown>(null)
  const st = useQuery({ queryKey: ['repo-status', repo.id], queryFn: () => api.get<RepoStatus>(`/api/2.0/lakeforge/repos/${repo.id}/status`) })
  const refresh = () => {
    void qc.invalidateQueries({ queryKey: ['repo-status', repo.id] })
    void qc.invalidateQueries({ queryKey: ['repos'] })
  }
  const run = async (fn: () => Promise<unknown>, ok: string) => {
    setBusy(true)
    setErr(null)
    try {
      await fn()
      toast(ok)
      refresh()
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }
  const s = st.data
  return (
    <Modal title={<span>{repo.path.split('/').pop()} <Badge>{repo.branch}</Badge></span>} onClose={onClose} wide>
      {node}
      <KV items={[['URL', <a href={repo.url} target="_blank" rel="noreferrer">{repo.url}</a>], ['Workspace path', <Link to={`/workspace?path=${encodeURIComponent(repo.path)}`}>{repo.path}</Link>], ['HEAD', <code className="small">{(s?.head_commit_id ?? repo.head_commit_id ?? '').slice(0, 12)}</code>]]} />
      <div className="actions" style={{ margin: '8px 0' }}>
        <button disabled={busy} onClick={() => run(() => api.patch(`/api/2.0/repos/${repo.id}`, { branch: s?.branch ?? repo.branch }), 'Pulled latest')}>⟳ Pull</button>
        <select disabled={busy} value={s?.branch ?? repo.branch} onChange={(e) => run(() => api.patch(`/api/2.0/repos/${repo.id}`, { branch: e.target.value.replace(/^origin\//, '') }), `Checked out ${e.target.value}`)}>
          {Array.from(new Set([repo.branch, ...(s?.branches ?? []).map((b) => b.replace(/^origin\//, ''))])).filter((b) => b && !b.includes('HEAD')).map((b) => <option key={b}>{b}</option>)}
        </select>
        <button disabled={busy} onClick={() => { const n = window.prompt('New branch name'); if (n) void run(() => api.post(`/api/2.0/lakeforge/repos/${repo.id}/branches`, { name: n }), `Created ${n}`) }}>＋ Branch</button>
      </div>
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'changes', label: `Changes (${s?.changes.length ?? 0})` }, { id: 'history', label: 'History' }]} />
      {st.isLoading && <Spinner />}
      {tab === 'changes' && (
        <>
          <Table
            rows={s?.changes ?? []}
            keyOf={(c) => c.path}
            columns={[{ key: 's', title: '', width: '40px', render: (c) => <code>{c.status}</code> }, { key: 'p', title: 'File', render: (c) => c.path }]}
            emptyText="Working tree clean."
          />
          {(s?.changes.length ?? 0) > 0 && (
            <Card title="Commit">
              <Field label="Message"><input value={msg} onChange={(e) => setMsg(e.target.value)} placeholder="Describe your change" /></Field>
              <label className="check"><input type="checkbox" checked={push} onChange={(e) => setPush(e.target.checked)} /> Push to remote</label>
              <div className="actions" style={{ marginTop: 8 }}>
                <button className="primary" disabled={busy || !msg.trim()} onClick={() => run(() => api.post(`/api/2.0/lakeforge/repos/${repo.id}/commit`, { message: msg, push }), push ? 'Committed and pushed' : 'Committed').then(() => setMsg(''))}>Commit{push ? ' & push' : ''}</button>
              </div>
            </Card>
          )}
        </>
      )}
      {tab === 'history' && (
        <Table
          rows={s?.commits ?? []}
          keyOf={(c) => c.sha}
          columns={[
            { key: 's', title: 'SHA', width: '100px', render: (c) => <code className="small">{c.sha.slice(0, 8)}</code> },
            { key: 'm', title: 'Message', render: (c) => c.message },
            { key: 'a', title: 'Author', render: (c) => c.author },
            { key: 't', title: 'When', render: (c) => fmtTime(c.time) },
          ]}
          emptyText="No commits."
        />
      )}
      <ErrorBox error={err} />
    </Modal>
  )
}

export default function Repos() {
  const qc = useQueryClient()
  const { me } = useAuth()
  const { toast, node } = useToast()
  const [sel, setSel] = useState<Repo | null>(null)
  const [adding, setAdding] = useState(false)
  const [creds, setCreds] = useState(false)
  const [f, setF] = useState({ url: '', provider: 'gitHub', path: '', branch: '' })
  const [cf, setCf] = useState({ git_provider: 'gitHub', git_username: '', personal_access_token: '' })
  const [err, setErr] = useState<unknown>(null)
  const [busy, setBusy] = useState(false)
  const repos = useQuery({ queryKey: ['repos'], queryFn: () => api.get<{ repos: Repo[] }>('/api/2.0/repos') })
  const credList = useQuery({ queryKey: ['git-creds'], queryFn: () => api.get<{ credentials: GitCred[] }>('/api/2.0/git-credentials') })
  const guessProvider = (url: string) => (url.includes('github') ? 'gitHub' : url.includes('gitlab') ? 'gitLab' : url.includes('bitbucket') ? 'bitbucketCloud' : url.includes('dev.azure') ? 'azureDevOpsServices' : 'gitHub')
  const add = async () => {
    setBusy(true)
    setErr(null)
    try {
      const name = f.url.split('/').pop()?.replace(/\.git$/, '') ?? 'repo'
      const r = await api.post<Repo>('/api/2.0/repos', { url: f.url, provider: f.provider, path: f.path || `/Repos/${me?.user_name}/${name}`, branch: f.branch || undefined })
      toast(`Cloned into ${r.path}`)
      setAdding(false)
      void qc.invalidateQueries({ queryKey: ['repos'] })
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }
  return (
    <Page
      title="Repos"
      subtitle="Git folders synced into the workspace. Clone, branch, commit and push from the UI or via the Repos API."
      actions={<><button onClick={() => setCreds(true)}>Git credentials ({credList.data?.credentials.length ?? 0})</button><button className="primary" onClick={() => setAdding(true)}>＋ Add repo</button></>}
    >
      {node}
      {repos.isLoading && <Spinner />}
      <ErrorBox error={repos.error} />
      <Table
        rows={repos.data?.repos ?? []}
        keyOf={(r) => r.id}
        onRowClick={(r) => setSel(r)}
        columns={[
          { key: 'p', title: 'Path', render: (r) => <b>{r.path}</b> },
          { key: 'u', title: 'URL', render: (r) => <code className="small">{r.url}</code> },
          { key: 'pr', title: 'Provider', render: (r) => r.provider },
          { key: 'b', title: 'Branch', render: (r) => <Badge>{r.branch}</Badge> },
          { key: 'h', title: 'HEAD', render: (r) => <code className="small">{(r.head_commit_id ?? '').slice(0, 8)}</code> },
          {
            key: 'act', title: '', width: '160px',
            render: (r) => (
              <span className="actions" onClick={(e) => e.stopPropagation()}>
                <Link className="btn sm" to={`/workspace?path=${encodeURIComponent(r.path)}`}>Open</Link>
                <button className="sm danger" onClick={async () => { if (window.confirm(`Remove repo ${r.path} from the workspace?`)) { await api.delete(`/api/2.0/repos/${r.id}`); void qc.invalidateQueries({ queryKey: ['repos'] }) } }}>Remove</button>
              </span>
            ),
          },
        ]}
        emptyText="No repos. Add one to clone a Git repository into /Repos."
      />
      {sel && <RepoPanel repo={sel} onClose={() => setSel(null)} />}
      {adding && (
        <Modal title="Add repo" onClose={() => setAdding(false)} footer={<><button onClick={() => setAdding(false)}>Cancel</button><button className="primary" disabled={busy || !f.url} onClick={add}>{busy ? 'Cloning…' : 'Create repo'}</button></>}>
          <Field label="Git repository URL"><input autoFocus value={f.url} onChange={(e) => setF({ ...f, url: e.target.value, provider: guessProvider(e.target.value) })} placeholder="https://github.com/org/repo.git" /></Field>
          <div className="grid2">
            <Field label="Provider">
              <select value={f.provider} onChange={(e) => setF({ ...f, provider: e.target.value })}>
                {['gitHub', 'gitLab', 'bitbucketCloud', 'azureDevOpsServices', 'gitHubEnterprise', 'gitLabEnterpriseEdition', 'bitbucketServer', 'awsCodeCommit'].map((p) => <option key={p}>{p}</option>)}
              </select>
            </Field>
            <Field label="Branch (optional)"><input value={f.branch} onChange={(e) => setF({ ...f, branch: e.target.value })} placeholder="default" /></Field>
          </div>
          <Field label="Workspace path" hint={`default /Repos/${me?.user_name}/<name>`}><input value={f.path} onChange={(e) => setF({ ...f, path: e.target.value })} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
      {creds && (
        <Modal title="Git credentials" onClose={() => setCreds(false)}>
          <Table
            rows={credList.data?.credentials ?? []}
            keyOf={(c) => c.credential_id}
            columns={[
              { key: 'p', title: 'Provider', render: (c) => c.git_provider },
              { key: 'u', title: 'Username', render: (c) => c.git_username ?? '' },
              { key: 'a', title: '', width: '80px', render: (c) => <button className="sm danger" onClick={async () => { await api.delete(`/api/2.0/git-credentials/${c.credential_id}`); void qc.invalidateQueries({ queryKey: ['git-creds'] }) }}>Delete</button> },
            ]}
            emptyText="No credentials. Public repos work without one; private repos need a PAT."
          />
          <h4>Add credential</h4>
          <div className="grid2">
            <Field label="Provider"><select value={cf.git_provider} onChange={(e) => setCf({ ...cf, git_provider: e.target.value })}>{['gitHub', 'gitLab', 'bitbucketCloud', 'azureDevOpsServices', 'gitHubEnterprise'].map((p) => <option key={p}>{p}</option>)}</select></Field>
            <Field label="Username"><input value={cf.git_username} onChange={(e) => setCf({ ...cf, git_username: e.target.value })} /></Field>
          </div>
          <Field label="Personal access token"><input type="password" value={cf.personal_access_token} onChange={(e) => setCf({ ...cf, personal_access_token: e.target.value })} /></Field>
          <button className="primary" disabled={!cf.personal_access_token} onClick={async () => { try { await api.post('/api/2.0/git-credentials', cf); setCf({ ...cf, personal_access_token: '' }); void qc.invalidateQueries({ queryKey: ['git-creds'] }); toast('Credential saved') } catch (e) { toast(e instanceof Error ? e.message : String(e), 'err') } }}>Save credential</button>
        </Modal>
      )}
    </Page>
  )
}
