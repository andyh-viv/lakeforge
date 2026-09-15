import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { api, basename, dirname, fmtTime, qs, type WorkspaceObject } from '../api'
import { useAuth } from '../auth'
import { Badge, ErrorBox, Field, Modal, Page, Spinner, Table, useToast } from '../components'

function icon(o: WorkspaceObject) {
  switch (o.object_type) {
    case 'DIRECTORY':
      return '📁'
    case 'NOTEBOOK':
      return '📓'
    case 'REPO':
      return '⑂'
    default:
      return '📄'
  }
}

export function useWorkspaceList(path: string) {
  return useQuery({
    queryKey: ['ws', path],
    queryFn: () => api.get<{ objects: WorkspaceObject[] }>(`/api/2.0/workspace/list${qs({ path })}`),
  })
}

export default function Workspace() {
  const [sp, setSp] = useSearchParams()
  const { me } = useAuth()
  const nav = useNavigate()
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const home = `/Users/${me?.user_name ?? ''}`
  const path = sp.get('path') ?? home
  const search = sp.get('q')
  const list = useWorkspaceList(path)
  const found = useQuery({
    queryKey: ['ws-search', search],
    queryFn: () => api.get<{ objects: WorkspaceObject[] }>(`/api/2.0/lakeforge/workspace/search${qs({ q: search })}`),
    enabled: !!search,
  })
  const [creating, setCreating] = useState<'notebook' | 'folder' | 'file' | null>(sp.get('new') === 'notebook' ? 'notebook' : null)
  const [name, setName] = useState('')
  const [lang, setLang] = useState('PYTHON')
  const [importing, setImporting] = useState(false)
  const [renaming, setRenaming] = useState<WorkspaceObject | null>(null)
  const [err, setErr] = useState<unknown>(null)

  const go = (p: string) => setSp({ path: p })
  const refresh = () => qc.invalidateQueries({ queryKey: ['ws'] })

  const create = async () => {
    setErr(null)
    const target = `${path.replace(/\/$/, '')}/${name.trim()}`
    try {
      if (creating === 'folder') await api.post('/api/2.0/workspace/mkdirs', { path: target })
      else if (creating === 'notebook') {
        await api.post('/api/2.0/lakeforge/notebooks', { path: target, language: lang, cells: [], create_only: true })
        nav(`/notebook${target}`)
      } else await api.post('/api/2.0/workspace/import', { path: target, format: 'AUTO', content: '', overwrite: false })
      setCreating(null)
      setName('')
      await refresh()
    } catch (e) {
      setErr(e)
    }
  }

  const del = async (o: WorkspaceObject) => {
    if (!window.confirm(`Delete ${o.path}${o.object_type === 'DIRECTORY' ? ' and everything in it' : ''}?`)) return
    try {
      await api.post('/api/2.0/workspace/delete', { path: o.path, recursive: true })
      toast(`Deleted ${basename(o.path)}`)
      await refresh()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }

  const rename = async () => {
    if (!renaming) return
    const dest = `${dirname(renaming.path)}/${name.trim()}`.replace('//', '/')
    try {
      await api.post('/api/2.0/lakeforge/workspace/move', { source_path: renaming.path, destination_path: dest })
      setRenaming(null)
      await refresh()
    } catch (e) {
      setErr(e)
    }
  }

  const onUpload = async (files: FileList | null) => {
    if (!files?.length) return
    setImporting(true)
    try {
      for (const f of Array.from(files)) {
        const buf = await f.arrayBuffer()
        const b64 = btoa(String.fromCharCode(...new Uint8Array(buf)))
        const isNb = /\.(py|sql|scala|r|ipynb)$/i.test(f.name)
        await api.post('/api/2.0/workspace/import', {
          path: `${path.replace(/\/$/, '')}/${f.name.replace(/\.(py|sql|scala|r)$/i, '')}`,
          format: isNb ? (f.name.endsWith('.ipynb') ? 'JUPYTER' : 'SOURCE') : 'AUTO',
          content: b64,
          overwrite: true,
        })
      }
      toast(`Imported ${files.length} file(s)`)
      await refresh()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    } finally {
      setImporting(false)
    }
  }

  const crumbs = path.split('/').filter(Boolean)
  const rows = search ? found.data?.objects ?? [] : list.data?.objects ?? []
  const open = (o: WorkspaceObject) => {
    if (o.object_type === 'DIRECTORY' || o.object_type === 'REPO') go(o.path)
    else if (o.object_type === 'NOTEBOOK') nav(`/notebook${o.path}`)
    else nav(`/notebook${o.path}?file=1`)
  }

  return (
    <Page
      title="Workspace"
      subtitle={
        <div className="crumbs">
          <a onClick={() => go('/')} href="#/">/</a>
          {crumbs.map((c, i) => (
            <span key={i}>
              <a href="#" onClick={(e) => { e.preventDefault(); go('/' + crumbs.slice(0, i + 1).join('/')) }}>{c}</a>
              {i < crumbs.length - 1 && ' / '}
            </span>
          ))}
        </div>
      }
      actions={
        <>
          <button onClick={() => go(home)}>Home</button>
          <button onClick={() => go('/Shared')}>Shared</button>
          <button onClick={() => go('/Repos')}>Repos</button>
          <label className="btn">
            {importing ? 'Importing…' : 'Import'}
            <input type="file" multiple style={{ display: 'none' }} onChange={(e) => onUpload(e.target.files)} />
          </label>
          <button onClick={() => { setCreating('folder'); setName('') }}>＋ Folder</button>
          <button onClick={() => { setCreating('file'); setName('') }}>＋ File</button>
          <button className="primary" onClick={() => { setCreating('notebook'); setName('Untitled Notebook') }}>＋ Notebook</button>
        </>
      }
    >
      {node}
      {search && (
        <div className="muted">
          Search results for “{search}” · <Link to="/workspace">clear</Link>
        </div>
      )}
      {(list.isLoading || found.isLoading) && <Spinner />}
      <ErrorBox error={list.error ?? found.error} />
      <Table
        rows={rows}
        keyOf={(o) => o.object_id ?? o.path}
        onRowClick={open}
        columns={[
          { key: 'name', title: 'Name', render: (o) => <span>{icon(o)} {search ? o.path : basename(o.path)}</span> },
          { key: 'type', title: 'Type', render: (o) => <Badge>{o.object_type}{o.language ? ` · ${o.language}` : ''}</Badge>, width: '180px' },
          { key: 'mod', title: 'Modified', render: (o) => fmtTime(o.modified_at), width: '200px' },
          {
            key: 'act', title: '', width: '160px',
            render: (o) => (
              <span className="actions" onClick={(e) => e.stopPropagation()}>
                <button className="sm" onClick={() => { setRenaming(o); setName(basename(o.path)) }}>Rename</button>
                <button className="sm danger" onClick={() => del(o)}>Delete</button>
              </span>
            ),
          },
        ]}
        emptyText={search ? 'No matching objects.' : 'This folder is empty.'}
      />
      {creating && (
        <Modal
          title={creating === 'notebook' ? 'Create notebook' : creating === 'folder' ? 'Create folder' : 'Create file'}
          onClose={() => setCreating(null)}
          footer={
            <>
              <button onClick={() => setCreating(null)}>Cancel</button>
              <button className="primary" onClick={create} disabled={!name.trim()}>Create</button>
            </>
          }
        >
          <Field label="Name"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && create()} /></Field>
          <Field label="Location"><input value={path} readOnly /></Field>
          {creating === 'notebook' && (
            <Field label="Default language">
              <select value={lang} onChange={(e) => setLang(e.target.value)}>
                <option value="PYTHON">Python</option>
                <option value="SQL">SQL</option>
                <option value="SCALA">Scala</option>
                <option value="R">R</option>
              </select>
            </Field>
          )}
          <ErrorBox error={err} />
        </Modal>
      )}
      {renaming && (
        <Modal
          title={`Rename ${basename(renaming.path)}`}
          onClose={() => setRenaming(null)}
          footer={
            <>
              <button onClick={() => setRenaming(null)}>Cancel</button>
              <button className="primary" onClick={rename} disabled={!name.trim()}>Rename</button>
            </>
          }
        >
          <Field label="New name"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && rename()} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
    </Page>
  )
}
