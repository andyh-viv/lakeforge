import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { api, fmtTime, qs, type SecretScope } from '../api'
import { Card, ErrorBox, Field, Modal, Page, Spinner, Table, useToast } from '../components'

interface SecretMeta {
  key: string
  last_updated_timestamp: number
}
interface Acl {
  principal: string
  permission: string
}

export default function Secrets() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const [scope, setScope] = useState<string | null>(null)
  const [newScope, setNewScope] = useState<string | null>(null)
  const [putKey, setPutKey] = useState<{ key: string; value: string } | null>(null)
  const [acl, setAcl] = useState({ principal: '', permission: 'READ' })
  const [err, setErr] = useState<unknown>(null)
  const scopes = useQuery({ queryKey: ['secret-scopes'], queryFn: () => api.get<{ scopes: SecretScope[] }>('/api/2.0/secrets/scopes/list') })
  const secrets = useQuery({ queryKey: ['secrets', scope], enabled: !!scope, queryFn: () => api.get<{ secrets: SecretMeta[] }>(`/api/2.0/secrets/list${qs({ scope })}`) })
  const acls = useQuery({ queryKey: ['secret-acls', scope], enabled: !!scope, retry: false, queryFn: () => api.get<{ items: Acl[] }>(`/api/2.0/secrets/acls/list${qs({ scope })}`) })
  const inv = () => {
    void qc.invalidateQueries({ queryKey: ['secrets', scope] })
    void qc.invalidateQueries({ queryKey: ['secret-acls', scope] })
    void qc.invalidateQueries({ queryKey: ['secret-scopes'] })
  }
  const act = async (fn: () => Promise<unknown>, ok: string) => {
    setErr(null)
    try {
      await fn()
      toast(ok)
      inv()
    } catch (e) {
      setErr(e)
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  return (
    <Page title="Secrets" subtitle="Encrypted secret scopes. Read in notebooks with dbutils.secrets.get(scope, key); values are redacted in output." actions={<button className="primary" onClick={() => setNewScope('')}>＋ Create scope</button>}>
      {node}
      <div className="split">
        <Card title="Scopes">
          {scopes.isLoading && <Spinner />}
          <ErrorBox error={scopes.error} />
          <Table
            rows={scopes.data?.scopes ?? []}
            keyOf={(s) => s.name}
            onRowClick={(s) => setScope(s.name)}
            columns={[
              { key: 'n', title: 'Scope', render: (s) => <b style={{ fontWeight: s.name === scope ? 700 : 500 }}>{s.name}</b> },
              { key: 'b', title: 'Backend', render: (s) => s.backend_type },
              { key: 'a', title: '', width: '70px', render: (s) => <button className="sm danger" onClick={(e) => { e.stopPropagation(); if (window.confirm(`Delete scope ${s.name} and all its secrets?`)) void act(() => api.post('/api/2.0/secrets/scopes/delete', { scope: s.name }), 'Scope deleted').then(() => setScope(null)) }}>✕</button> },
            ]}
            emptyText="No scopes."
          />
        </Card>
        {scope ? (
          <div>
            <Card title={<span>Secrets in <code>{scope}</code></span>} actions={<button className="sm primary" onClick={() => setPutKey({ key: '', value: '' })}>＋ Add secret</button>}>
              {secrets.isLoading && <Spinner />}
              <ErrorBox error={secrets.error} />
              <Table
                rows={secrets.data?.secrets ?? []}
                keyOf={(s) => s.key}
                columns={[
                  { key: 'k', title: 'Key', render: (s) => <code>{s.key}</code> },
                  { key: 'v', title: 'Value', render: () => <span className="muted">[REDACTED]</span> },
                  { key: 't', title: 'Updated', render: (s) => fmtTime(s.last_updated_timestamp) },
                  {
                    key: 'a', title: '', width: '130px',
                    render: (s) => (
                      <span className="actions">
                        <button className="sm" onClick={() => setPutKey({ key: s.key, value: '' })}>Rotate</button>
                        <button className="sm danger" onClick={() => { if (window.confirm(`Delete ${s.key}?`)) void act(() => api.post('/api/2.0/secrets/delete', { scope, key: s.key }), 'Deleted') }}>✕</button>
                      </span>
                    ),
                  },
                ]}
                emptyText="No secrets in this scope."
              />
              <p className="muted small">Usage: <code>dbutils.secrets.get(scope="{scope}", key="…")</code> · Spark conf: <code>{'{{secrets/' + scope + '/key}}'}</code></p>
            </Card>
            <Card title="Access control">
              {acls.error ? <div className="muted small">You need MANAGE on this scope to view ACLs.</div> : (
                <Table
                  rows={acls.data?.items ?? []}
                  keyOf={(a) => a.principal}
                  columns={[
                    { key: 'p', title: 'Principal', render: (a) => a.principal },
                    { key: 'm', title: 'Permission', render: (a) => a.permission },
                    { key: 'a', title: '', width: '70px', render: (a) => <button className="sm danger" onClick={() => act(() => api.post('/api/2.0/secrets/acls/delete', { scope, principal: a.principal }), 'ACL removed')}>✕</button> },
                  ]}
                  emptyText="No ACLs; creator has MANAGE."
                />
              )}
              <div className="toolbar" style={{ marginTop: 8 }}>
                <input placeholder="user or group" value={acl.principal} onChange={(e) => setAcl({ ...acl, principal: e.target.value })} />
                <select value={acl.permission} onChange={(e) => setAcl({ ...acl, permission: e.target.value })}>{['READ', 'WRITE', 'MANAGE'].map((p) => <option key={p}>{p}</option>)}</select>
                <button className="sm primary" disabled={!acl.principal} onClick={() => act(() => api.post('/api/2.0/secrets/acls/put', { scope, ...acl }), 'ACL saved').then(() => setAcl({ ...acl, principal: '' }))}>Grant</button>
              </div>
            </Card>
          </div>
        ) : (
          <div className="empty">Select a scope to manage its secrets.</div>
        )}
      </div>
      <ErrorBox error={err} />
      {newScope !== null && (
        <Modal title="Create secret scope" onClose={() => setNewScope(null)} footer={<><button onClick={() => setNewScope(null)}>Cancel</button><button className="primary" disabled={!newScope} onClick={() => act(() => api.post('/api/2.0/secrets/scopes/create', { scope: newScope, initial_manage_principal: undefined }), 'Scope created').then(() => { setScope(newScope); setNewScope(null) })}>Create</button></>}>
          <Field label="Scope name" hint="letters, digits, dashes, underscores, dots"><input autoFocus value={newScope} onChange={(e) => setNewScope(e.target.value.replace(/[^A-Za-z0-9_.-]/g, ''))} /></Field>
        </Modal>
      )}
      {putKey && (
        <Modal title={putKey.key ? `Rotate ${putKey.key}` : 'Add secret'} onClose={() => setPutKey(null)} footer={<><button onClick={() => setPutKey(null)}>Cancel</button><button className="primary" disabled={!putKey.key || !putKey.value} onClick={() => act(() => api.post('/api/2.0/secrets/put', { scope, key: putKey.key, string_value: putKey.value }), 'Secret stored').then(() => setPutKey(null))}>Save</button></>}>
          <Field label="Key"><input autoFocus value={putKey.key} onChange={(e) => setPutKey({ ...putKey, key: e.target.value.replace(/[^A-Za-z0-9_.-]/g, '') })} /></Field>
          <Field label="Value" hint="stored encrypted; never shown again"><textarea rows={3} value={putKey.value} onChange={(e) => setPutKey({ ...putKey, value: e.target.value })} /></Field>
        </Modal>
      )}
    </Page>
  )
}
