import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { api, fmtTime, type ScimGroup, type ScimUser, type TokenInfo } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, Modal, Page, Spinner, Table, Tabs, useToast } from '../components'
import { useAuth } from '../auth'

const SCIM = '/api/2.0/preview/scim/v2'
const PATCH = 'urn:ietf:params:scim:api:messages:2.0:PatchOp'
const ENTITLEMENTS = ['workspace-access', 'databricks-sql-access', 'allow-cluster-create', 'allow-instance-pool-create']
const CONF_KEYS = ['enableTokensConfig', 'enableIpAccessLists', 'enableWebTerminal', 'enableDbfsFileBrowser', 'enableResultsDownloading', 'enableExportNotebook', 'maxTokenLifetimeDays']

type ScimUserFull = ScimUser & { groups?: { display: string; value: string }[]; entitlements?: { value: string }[]; applicationId?: string }
interface ScimList<T> {
  totalResults: number
  Resources: T[]
}
interface IpList {
  list_id: string
  label: string
  list_type: string
  ip_addresses: string[]
  enabled: boolean
}
interface InitScript {
  script_id: string
  name: string
  enabled: boolean
  position: number
  script?: string
}

function Users() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const users = useQuery({ queryKey: ['scim-users'], queryFn: () => api.get<ScimList<ScimUserFull>>(`${SCIM}/Users?count=1000`) })
  const groups = useQuery({ queryKey: ['scim-groups'], queryFn: () => api.get<ScimList<ScimGroup>>(`${SCIM}/Groups?count=1000`) })
  const [adding, setAdding] = useState(false)
  const [f, setF] = useState({ userName: '', displayName: '', password: '', admin: false })
  const [sel, setSel] = useState<ScimUserFull | null>(null)
  const [err, setErr] = useState<unknown>(null)
  const inv = () => {
    void qc.invalidateQueries({ queryKey: ['scim-users'] })
    void qc.invalidateQueries({ queryKey: ['scim-groups'] })
  }
  const adminsGroup = groups.data?.Resources.find((g) => g.displayName === 'admins')
  const patchGroup = (gid: string, op: 'add' | 'remove', uid: string) =>
    api.patch(`${SCIM}/Groups/${gid}`, { schemas: [PATCH], Operations: op === 'add' ? [{ op: 'add', path: 'members', value: [{ value: uid }] }] : [{ op: 'remove', path: `members[value eq "${uid}"]` }] })
  const create = async () => {
    setErr(null)
    try {
      const u = await api.post<ScimUserFull>(`${SCIM}/Users`, { userName: f.userName, displayName: f.displayName || f.userName, password: f.password || undefined, active: true })
      if (f.admin && adminsGroup) await patchGroup(adminsGroup.id, 'add', u.id)
      toast(`Created ${u.userName}`)
      setAdding(false)
      setF({ userName: '', displayName: '', password: '', admin: false })
      inv()
    } catch (e) {
      setErr(e)
    }
  }
  const patchUser = async (u: ScimUserFull, ops: Record<string, unknown>[], ok: string) => {
    try {
      await api.patch(`${SCIM}/Users/${u.id}`, { schemas: [PATCH], Operations: ops })
      toast(ok)
      inv()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  return (
    <>
      {node}
      <div className="toolbar"><span className="muted small">{users.data?.totalResults ?? 0} users</span><span style={{ flex: 1 }} /><button className="primary" onClick={() => setAdding(true)}>＋ Add user</button></div>
      {users.isLoading && <Spinner />}
      <ErrorBox error={users.error} />
      <Table
        rows={users.data?.Resources ?? []}
        keyOf={(u) => u.id}
        onRowClick={(u) => setSel(u)}
        columns={[
          { key: 'u', title: 'User', render: (u) => <><b>{u.displayName || u.userName}</b><div className="muted small">{u.userName}</div></> },
          { key: 'g', title: 'Groups', render: (u) => (u.groups ?? []).map((g) => <Badge key={g.value}>{g.display}</Badge>) },
          { key: 'e', title: 'Entitlements', render: (u) => (u.entitlements ?? []).map((e) => e.value).join(', ') },
          { key: 's', title: 'Status', render: (u) => <Badge state={u.active ? "ACTIVE" : "INACTIVE"}>{u.active ? "active" : "inactive"}</Badge> },
          {
            key: 'a', title: '', width: '200px',
            render: (u) => (
              <span className="actions" onClick={(e) => e.stopPropagation()}>
                {adminsGroup && ((u.groups ?? []).some((g) => g.display === 'admins')
                  ? <button className="sm" onClick={async () => { await patchGroup(adminsGroup.id, 'remove', u.id); inv() }}>Revoke admin</button>
                  : <button className="sm" onClick={async () => { await patchGroup(adminsGroup.id, 'add', u.id); inv() }}>Make admin</button>)}
                <button className="sm" onClick={() => patchUser(u, [{ op: 'replace', path: 'active', value: !u.active }], u.active ? 'Deactivated' : 'Activated')}>{u.active ? 'Deactivate' : 'Activate'}</button>
                <button className="sm danger" onClick={async () => { if (window.confirm(`Delete ${u.userName}?`)) { await api.delete(`${SCIM}/Users/${u.id}`); inv() } }}>✕</button>
              </span>
            ),
          },
        ]}
      />
      {adding && (
        <Modal title="Add user" onClose={() => setAdding(false)} footer={<><button onClick={() => setAdding(false)}>Cancel</button><button className="primary" disabled={!f.userName} onClick={create}>Create</button></>}>
          <Field label="Email / username"><input autoFocus value={f.userName} onChange={(e) => setF({ ...f, userName: e.target.value })} /></Field>
          <Field label="Display name"><input value={f.displayName} onChange={(e) => setF({ ...f, displayName: e.target.value })} /></Field>
          <Field label="Initial password" hint="leave blank to create a user who can only authenticate with tokens"><input type="password" value={f.password} onChange={(e) => setF({ ...f, password: e.target.value })} /></Field>
          <label className="check"><input type="checkbox" checked={f.admin} onChange={(e) => setF({ ...f, admin: e.target.checked })} /> Workspace admin</label>
          <ErrorBox error={err} />
        </Modal>
      )}
      {sel && (
        <Modal title={sel.userName} onClose={() => setSel(null)} wide>
          <h4>Entitlements</h4>
          <div className="actions">
            {ENTITLEMENTS.map((e) => {
              const has = (sel.entitlements ?? []).some((x) => x.value === e)
              return <label key={e} className="check"><input type="checkbox" checked={has} onChange={() => patchUser(sel, has ? [{ op: 'remove', path: `entitlements[value eq "${e}"]` }] : [{ op: 'add', path: 'entitlements', value: [{ value: e }] }], 'Updated').then(() => setSel({ ...sel, entitlements: has ? (sel.entitlements ?? []).filter((x) => x.value !== e) : [...(sel.entitlements ?? []), { value: e }] }))} /> {e}</label>
            })}
          </div>
          <h4>Reset password</h4>
          <div className="toolbar">
            <button className="sm" onClick={() => { const pw = window.prompt(`New password for ${sel.userName}`); if (pw) void patchUser(sel, [{ op: 'replace', path: 'password', value: pw }], 'Password reset') }}>Set new password…</button>
          </div>
          <h4>SCIM resource</h4>
          <JsonView value={sel} />
        </Modal>
      )}
    </>
  )
}

function Groups() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const groups = useQuery({ queryKey: ['scim-groups'], queryFn: () => api.get<ScimList<ScimGroup>>(`${SCIM}/Groups?count=1000`) })
  const users = useQuery({ queryKey: ['scim-users'], queryFn: () => api.get<ScimList<ScimUserFull>>(`${SCIM}/Users?count=1000`) })
  const [sel, setSel] = useState<ScimGroup | null>(null)
  const [addUser, setAddUser] = useState('')
  const inv = () => {
    void qc.invalidateQueries({ queryKey: ['scim-groups'] })
    void qc.invalidateQueries({ queryKey: ['scim-users'] })
  }
  const patch = async (g: ScimGroup, ops: Record<string, unknown>[], ok: string) => {
    try {
      const r = await api.patch<ScimGroup>(`${SCIM}/Groups/${g.id}`, { schemas: [PATCH], Operations: ops })
      toast(ok)
      setSel(r)
      inv()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  return (
    <>
      {node}
      <div className="toolbar"><span style={{ flex: 1 }} /><button className="primary" onClick={async () => { const n = window.prompt('Group name'); if (n) { await api.post(`${SCIM}/Groups`, { displayName: n }); inv() } }}>＋ Create group</button></div>
      {groups.isLoading && <Spinner />}
      <Table
        rows={groups.data?.Resources ?? []}
        keyOf={(g) => g.id}
        onRowClick={(g) => setSel(g)}
        columns={[
          { key: 'n', title: 'Group', render: (g) => <b>{g.displayName}</b> },
          { key: 'm', title: 'Members', render: (g) => g.members?.length ?? 0 },
          { key: 'e', title: 'Entitlements', render: (g) => (g.entitlements ?? []).map((e) => e.value).join(', ') },
          { key: 'a', title: '', width: '70px', render: (g) => ['admins', 'users'].includes(g.displayName) ? <span className="muted small">built-in</span> : <button className="sm danger" onClick={async (e) => { e.stopPropagation(); if (window.confirm(`Delete group ${g.displayName}?`)) { await api.delete(`${SCIM}/Groups/${g.id}`); inv() } }}>✕</button> },
        ]}
      />
      {sel && (
        <Modal title={sel.displayName} onClose={() => setSel(null)} wide>
          <h4>Members</h4>
          <Table
            rows={sel.members ?? []}
            keyOf={(m) => m.value}
            columns={[{ key: 'd', title: 'Member', render: (m) => m.display }, { key: 'a', title: '', width: '70px', render: (m) => <button className="sm danger" onClick={() => patch(sel, [{ op: 'remove', path: `members[value eq "${m.value}"]` }], 'Removed')}>✕</button> }]}
            emptyText="No members."
          />
          <div className="toolbar" style={{ marginTop: 8 }}>
            <select value={addUser} onChange={(e) => setAddUser(e.target.value)}>
              <option value="">Add member…</option>
              {(users.data?.Resources ?? []).filter((u) => !(sel.members ?? []).some((m) => m.value === u.id)).map((u) => <option key={u.id} value={u.id}>{u.userName}</option>)}
              {(groups.data?.Resources ?? []).filter((g) => g.id !== sel.id && !(sel.members ?? []).some((m) => m.value === g.id)).map((g) => <option key={g.id} value={g.id}>group: {g.displayName}</option>)}
            </select>
            <button className="sm primary" disabled={!addUser} onClick={() => patch(sel, [{ op: 'add', path: 'members', value: [{ value: addUser }] }], 'Added').then(() => setAddUser(''))}>Add</button>
          </div>
          <h4>Entitlements</h4>
          <div className="actions">
            {ENTITLEMENTS.map((e) => {
              const has = (sel.entitlements ?? []).some((x) => x.value === e)
              return <label key={e} className="check"><input type="checkbox" checked={has} onChange={() => patch(sel, has ? [{ op: 'remove', path: `entitlements[value eq "${e}"]` }] : [{ op: 'add', path: 'entitlements', value: [{ value: e }] }], 'Updated')} /> {e}</label>
            })}
          </div>
        </Modal>
      )}
    </>
  )
}

function ServicePrincipals() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const sps = useQuery({ queryKey: ['scim-sps'], queryFn: () => api.get<ScimList<ScimUserFull>>(`${SCIM}/ServicePrincipals?count=1000`) })
  const [secret, setSecret] = useState<{ id: string; secret: string } | null>(null)
  return (
    <>
      {node}
      <div className="toolbar"><span className="muted small">Service principals authenticate with OAuth client secrets or PATs; use them for CI/CD and Terraform.</span><span style={{ flex: 1 }} /><button className="primary" onClick={async () => { const n = window.prompt('Service principal display name'); if (n) { await api.post(`${SCIM}/ServicePrincipals`, { displayName: n, active: true }); void qc.invalidateQueries({ queryKey: ['scim-sps'] }) } }}>＋ Add service principal</button></div>
      {sps.isLoading && <Spinner />}
      <Table
        rows={sps.data?.Resources ?? []}
        keyOf={(s) => s.id}
        columns={[
          { key: 'n', title: 'Name', render: (s) => <b>{s.displayName}</b> },
          { key: 'i', title: 'Application ID', render: (s) => <code className="small">{s.applicationId}</code> },
          { key: 's', title: 'Status', render: (s) => <Badge state={s.active ? "ACTIVE" : "INACTIVE"}>{s.active ? "active" : "inactive"}</Badge> },
          {
            key: 'a', title: '', width: '200px',
            render: (s) => (
              <span className="actions">
                <button className="sm" onClick={async () => { try { const r = await api.post<{ id: string; secret: string }>(`/api/2.0/accounts/servicePrincipals/${s.id}/credentials/secrets`, {}); setSecret(r) } catch (e) { toast(e instanceof Error ? e.message : String(e), 'err') } }}>Generate secret</button>
                <button className="sm danger" onClick={async () => { if (window.confirm(`Delete ${s.displayName}?`)) { await api.delete(`${SCIM}/ServicePrincipals/${s.id}`); void qc.invalidateQueries({ queryKey: ['scim-sps'] }) } }}>✕</button>
              </span>
            ),
          },
        ]}
        emptyText="No service principals."
      />
      {secret && (
        <Modal title="OAuth secret created" onClose={() => setSecret(null)} footer={<button className="primary" onClick={() => setSecret(null)}>Done</button>}>
          <p>Copy this secret now — it is not shown again. Use it as a bearer token or as <code>DATABRICKS_TOKEN</code>.</p>
          <pre className="mono" style={{ userSelect: 'all' }}>{secret.secret}</pre>
        </Modal>
      )}
    </>
  )
}

function Tokens() {
  const qc = useQueryClient()
  const t = useQuery({ queryKey: ['token-mgmt'], queryFn: () => api.get<{ token_infos: TokenInfo[] }>('/api/2.0/token-management/tokens') })
  return (
    <>
      {t.isLoading && <Spinner />}
      <Table
        rows={t.data?.token_infos ?? []}
        keyOf={(x) => x.token_id}
        columns={[
          { key: 'c', title: 'Comment', render: (x) => x.comment || <span className="muted">—</span> },
          { key: 'u', title: 'Owner', render: (x) => x.created_by_username },
          { key: 'cr', title: 'Created', render: (x) => fmtTime(x.creation_time) },
          { key: 'ex', title: 'Expires', render: (x) => (x.expiry_time > 0 ? fmtTime(x.expiry_time) : 'never') },
          { key: 'id', title: 'ID', render: (x) => <code className="small">{x.token_id}</code> },
          { key: 'a', title: '', width: '80px', render: (x) => <button className="sm danger" onClick={async () => { if (window.confirm('Revoke this token?')) { await api.delete(`/api/2.0/token-management/tokens/${x.token_id}`); void qc.invalidateQueries({ queryKey: ['token-mgmt'] }) } }}>Revoke</button> },
        ]}
        emptyText="No personal access tokens issued."
      />
    </>
  )
}

function WorkspaceSettings() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const conf = useQuery({ queryKey: ['wsconf'], queryFn: () => api.get<Record<string, string | null>>(`/api/2.0/workspace-conf?keys=${CONF_KEYS.join(',')}`) })
  const ips = useQuery({ queryKey: ['ip-lists'], queryFn: () => api.get<{ ip_access_lists: IpList[] }>('/api/2.0/ip-access-lists') })
  const scripts = useQuery({ queryKey: ['init-scripts'], queryFn: () => api.get<{ scripts: InitScript[] }>('/api/2.0/global-init-scripts') })
  const set = async (k: string, v: string) => {
    await api.patch('/api/2.0/workspace-conf', { [k]: v })
    toast(`${k} = ${v}`)
    void qc.invalidateQueries({ queryKey: ['wsconf'] })
  }
  return (
    <>
      {node}
      <Card title="Workspace configuration">
        {conf.isLoading && <Spinner />}
        <Table
          rows={CONF_KEYS.map((k) => ({ k, v: conf.data?.[k] ?? '' }))}
          keyOf={(r) => r.k}
          columns={[
            { key: 'k', title: 'Setting', render: (r) => <code>{r.k}</code> },
            {
              key: 'v', title: 'Value',
              render: (r) => r.v === 'true' || r.v === 'false'
                ? <label className="check"><input type="checkbox" checked={r.v === 'true'} onChange={(e) => set(r.k, String(e.target.checked))} /> {r.v}</label>
                : <input style={{ width: 120 }} defaultValue={r.v} onBlur={(e) => { if (e.target.value !== r.v) void set(r.k, e.target.value) }} />,
            },
          ]}
        />
      </Card>
      <Card title="IP access lists" actions={<button className="sm primary" onClick={async () => { const label = window.prompt('Label'); const ip = label && window.prompt('CIDR(s), comma separated', '0.0.0.0/0'); if (label && ip) { await api.post('/api/2.0/ip-access-lists', { label, list_type: 'ALLOW', ip_addresses: ip.split(',').map((s) => s.trim()) }); void qc.invalidateQueries({ queryKey: ['ip-lists'] }) } }}>＋ Add list</button>}>
        <Table
          rows={ips.data?.ip_access_lists ?? []}
          keyOf={(l) => l.list_id}
          columns={[
            { key: 'l', title: 'Label', render: (l) => <b>{l.label}</b> },
            { key: 't', title: 'Type', render: (l) => <Badge state={l.list_type === "ALLOW" ? "ACTIVE" : "ERROR"}>{l.list_type}</Badge> },
            { key: 'ip', title: 'Addresses', render: (l) => l.ip_addresses.join(', ') },
            { key: 'e', title: 'Enabled', render: (l) => <input type="checkbox" checked={l.enabled} onChange={async (e) => { await api.patch(`/api/2.0/ip-access-lists/${l.list_id}`, { enabled: e.target.checked }); void qc.invalidateQueries({ queryKey: ['ip-lists'] }) }} /> },
            { key: 'a', title: '', width: '60px', render: (l) => <button className="sm danger" onClick={async () => { await api.delete(`/api/2.0/ip-access-lists/${l.list_id}`); void qc.invalidateQueries({ queryKey: ['ip-lists'] }) }}>✕</button> },
          ]}
          emptyText={`No IP access lists. Enforcement is controlled by enableIpAccessLists (${conf.data?.enableIpAccessLists ?? '…'}).`}
        />
      </Card>
      <Card title="Global init scripts" actions={<button className="sm primary" onClick={async () => { const name = window.prompt('Script name'); const body = name && window.prompt('Script body', '#!/bin/bash\n'); if (name && body) { await api.post('/api/2.0/global-init-scripts', { name, script: btoa(body), enabled: true, position: (scripts.data?.scripts.length ?? 0) }); void qc.invalidateQueries({ queryKey: ['init-scripts'] }) } }}>＋ Add script</button>}>
        <Table
          rows={scripts.data?.scripts ?? []}
          keyOf={(s) => s.script_id}
          columns={[
            { key: 'p', title: '#', width: '40px', render: (s) => s.position },
            { key: 'n', title: 'Name', render: (s) => <b>{s.name}</b> },
            { key: 'e', title: 'Enabled', render: (s) => <input type="checkbox" checked={s.enabled} onChange={async (e) => { await api.patch(`/api/2.0/global-init-scripts/${s.script_id}`, { enabled: e.target.checked }); void qc.invalidateQueries({ queryKey: ['init-scripts'] }) }} /> },
            { key: 'a', title: '', width: '60px', render: (s) => <button className="sm danger" onClick={async () => { await api.delete(`/api/2.0/global-init-scripts/${s.script_id}`); void qc.invalidateQueries({ queryKey: ['init-scripts'] }) }}>✕</button> },
          ]}
          emptyText="No global init scripts; they run on every cluster node at start."
        />
      </Card>
    </>
  )
}

export default function Admin() {
  const { me } = useAuth()
  const [tab, setTab] = useState('users')
  if (!me?.is_admin) return <Page title="Admin settings"><div className="empty">You need workspace admin privileges to view this page.</div></Page>
  return (
    <Page title="Admin settings" subtitle="Identity (SCIM users, groups, service principals), tokens, and workspace-level configuration.">
      <Tabs active={tab} onChange={setTab} tabs={[{ id: 'users', label: 'Users' }, { id: 'groups', label: 'Groups' }, { id: 'sps', label: 'Service principals' }, { id: 'tokens', label: 'Access tokens' }, { id: 'ws', label: 'Workspace settings' }]} />
      {tab === 'users' && <Users />}
      {tab === 'groups' && <Groups />}
      {tab === 'sps' && <ServicePrincipals />}
      {tab === 'tokens' && <Tokens />}
      {tab === 'ws' && <WorkspaceSettings />}
    </Page>
  )
}
