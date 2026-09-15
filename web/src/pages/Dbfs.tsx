import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { api, fmtBytes, fmtTime, getToken, qs, type DbfsFile } from '../api'
import { ErrorBox, Field, Modal, Page, Spinner, Table, useToast } from '../components'

function decodeBase64(s: string): string {
  try {
    return new TextDecoder().decode(Uint8Array.from(atob(s), (c) => c.charCodeAt(0)))
  } catch {
    return atob(s)
  }
}

function Preview({ file, onClose }: { file: DbfsFile; onClose: () => void }) {
  const q = useQuery({ queryKey: ['dbfs-read', file.path], queryFn: () => api.get<{ bytes_read: number; data: string }>(`/api/2.0/dbfs/read${qs({ path: file.path, length: 256 * 1024 })}`) })
  const text = q.data ? decodeBase64(q.data.data) : ''
  const binary = /[\x00-\x08\x0E-\x1F]/.test(text.slice(0, 2000))
  return (
    <Modal title={file.path} onClose={onClose} wide footer={<a className="btn" href={`/api/2.0/fs/files${file.path}`} target="_blank" rel="noreferrer">Download</a>}>
      <div className="muted small">{fmtBytes(file.file_size)} · modified {fmtTime(file.modification_time)}{q.data && q.data.bytes_read < file.file_size ? ` · showing first ${fmtBytes(q.data.bytes_read)}` : ''}</div>
      {q.isLoading && <Spinner />}
      <ErrorBox error={q.error} />
      {q.data && (binary ? <div className="empty">Binary file — use Download.</div> : <pre className="small" style={{ maxHeight: 480, overflow: 'auto' }}>{text}</pre>)}
    </Modal>
  )
}

export default function Dbfs() {
  const [sp, setSp] = useSearchParams()
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const path = sp.get('path') ?? '/'
  const [sel, setSel] = useState<DbfsFile | null>(null)
  const [mk, setMk] = useState(false)
  const [mkName, setMkName] = useState('')
  const fileInput = useRef<HTMLInputElement>(null)
  const list = useQuery({ queryKey: ['dbfs', path], queryFn: () => api.get<{ files: DbfsFile[] }>(`/api/2.0/dbfs/list${qs({ path })}`) })
  const go = (p: string) => setSp({ path: p })
  const refresh = () => void qc.invalidateQueries({ queryKey: ['dbfs', path] })
  const parts = path.split('/').filter(Boolean)
  const join = (dir: string, name: string) => `${dir.replace(/\/$/, '')}/${name}`
  const upload = async (files: FileList | null) => {
    if (!files) return
    for (const f of Array.from(files)) {
      const fd = new FormData()
      fd.append('path', join(path, f.name))
      fd.append('overwrite', 'true')
      fd.append('contents', f)
      const res = await fetch('/api/2.0/dbfs/put', { method: 'POST', body: fd, headers: { Authorization: `Bearer ${getToken() ?? ''}` } })
      if (!res.ok) toast(`Upload ${f.name} failed: ${res.status}`, 'err')
      else toast(`Uploaded ${f.name}`)
    }
    refresh()
  }
  const roots = [
    { label: 'DBFS root', p: '/' },
    { label: 'FileStore', p: '/FileStore' },
    { label: 'Volumes', p: '/Volumes' },
    { label: 'tmp', p: '/tmp' },
  ]
  return (
    <Page
      title="DBFS / Files"
      subtitle="Workspace object storage: dbfs:/ paths and Unity Catalog volumes under /Volumes/<catalog>/<schema>/<volume>."
      actions={
        <>
          <button onClick={() => setMk(true)}>＋ Folder</button>
          <button className="primary" onClick={() => fileInput.current?.click()}>⇧ Upload</button>
          <input ref={fileInput} type="file" multiple hidden onChange={(e) => upload(e.target.files)} />
        </>
      }
    >
      {node}
      <div className="toolbar">
        {roots.map((r) => <button key={r.p} className={`sm ${path === r.p ? 'primary' : ''}`} onClick={() => go(r.p)}>{r.label}</button>)}
        <span style={{ flex: 1 }} />
        <span className="muted small">python: <code>dbutils.fs.ls("dbfs:{path}")</code> · shell: <code>/dbfs{path}</code></span>
      </div>
      <div className="crumbs">
        <Link to="/dbfs" onClick={(e) => { e.preventDefault(); go('/') }}>dbfs:</Link>
        {parts.map((p, i) => (
          <span key={i}><span className="sep">/</span><a onClick={() => go('/' + parts.slice(0, i + 1).join('/'))}>{p}</a></span>
        ))}
      </div>
      {list.isLoading && <Spinner />}
      <ErrorBox error={list.error} />
      <Table
        rows={[...(list.data?.files ?? [])].sort((a, b) => Number(b.is_dir) - Number(a.is_dir) || a.path.localeCompare(b.path))}
        keyOf={(f) => f.path}
        onRowClick={(f) => (f.is_dir ? go(f.path) : setSel(f))}
        columns={[
          { key: 'n', title: 'Name', render: (f) => <span>{f.is_dir ? '📁' : '📄'} <b>{f.path.split('/').pop()}</b></span> },
          { key: 's', title: 'Size', render: (f) => (f.is_dir ? '' : fmtBytes(f.file_size)) },
          { key: 'm', title: 'Modified', render: (f) => fmtTime(f.modification_time) },
          {
            key: 'a', title: '', width: '200px',
            render: (f) => (
              <span className="actions" onClick={(e) => e.stopPropagation()}>
                {!f.is_dir && <a className="btn sm" href={`/api/2.0/fs/files${f.path}`} target="_blank" rel="noreferrer">↓</a>}
                <button className="sm" onClick={async () => { const n = window.prompt('Move / rename to', f.path); if (n && n !== f.path) { await api.post('/api/2.0/dbfs/move', { source_path: f.path, destination_path: n }); refresh() } }}>Move</button>
                <button className="sm" onClick={async () => { const n = window.prompt('Copy to', f.path + '.copy'); if (n) { await api.post('/api/2.0/dbfs/copy', { source_path: f.path, destination_path: n, recursive: f.is_dir }); refresh() } }}>Copy</button>
                <button className="sm danger" onClick={async () => { if (window.confirm(`Delete ${f.path}?`)) { await api.post('/api/2.0/dbfs/delete', { path: f.path, recursive: true }); refresh() } }}>✕</button>
              </span>
            ),
          },
        ]}
        emptyText={path.startsWith('/Volumes') && parts.length < 4 ? 'Volumes are browsed as /Volumes/<catalog>/<schema>/<volume>; create volumes in the Catalog Explorer.' : 'Empty directory. Upload files or write with dbutils.fs / spark.write.'}
      />
      {sel && <Preview file={sel} onClose={() => setSel(null)} />}
      {mk && (
        <Modal title="New folder" onClose={() => setMk(false)} footer={<><button onClick={() => setMk(false)}>Cancel</button><button className="primary" disabled={!mkName} onClick={async () => { await api.post('/api/2.0/dbfs/mkdirs', { path: join(path, mkName) }); setMk(false); setMkName(''); refresh() }}>Create</button></>}>
          <Field label="Folder name"><input autoFocus value={mkName} onChange={(e) => setMkName(e.target.value)} /></Field>
        </Modal>
      )}
    </Page>
  )
}
