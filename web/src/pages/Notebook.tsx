import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useSearchParams } from 'react-router-dom'
import { api, basename, type Cell, type Cluster, type KernelEvent, type Notebook } from '../api'
import { Badge, CodeEditor, ErrorBox, Field, Modal, ResultGrid, Spinner, useToast } from '../components'
import { streamEvents } from '../sse'

const LANGS = ['python', 'sql', 'scala', 'r', 'markdown', 'shell']

function newCell(language: string): Cell {
  return { id: crypto.randomUUID().replace(/-/g, ''), language, source: '', outputs: [] }
}

function renderMarkdown(src: string): string {
  const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  return esc(src)
    .replace(/^### (.*)$/gm, '<h3>$1</h3>')
    .replace(/^## (.*)$/gm, '<h2>$1</h2>')
    .replace(/^# (.*)$/gm, '<h1>$1</h1>')
    .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
    .replace(/`(.+?)`/g, '<code>$1</code>')
    .replace(/\[(.+?)\]\((.+?)\)/g, '<a href="$2" target="_blank">$1</a>')
    .replace(/^- (.*)$/gm, '<li>$1</li>')
    .replace(/\n/g, '<br/>')
}

export function Output({ ev }: { ev: KernelEvent }) {
  switch (ev.type) {
    case 'stdout':
      return <pre>{ev.text}</pre>
    case 'stderr':
      return <pre className="stderr">{ev.text}</pre>
    case 'result':
      return <pre className="result">{ev.text}</pre>
    case 'exit':
      return <div className="exit">Notebook exited: {ev.value ?? ''}</div>
    case 'error':
      return (
        <pre className="traceback">
          {ev.ename}: {ev.evalue}
          {ev.traceback.length ? '\n' + ev.traceback.join('\n') : ''}
        </pre>
      )
    case 'table':
      return <ResultGrid columns={ev.columns} rows={ev.rows} truncated={ev.truncated} />
    case 'display':
      if (ev.mime === 'text/html') return <div className="html" dangerouslySetInnerHTML={{ __html: String(ev.data) }} />
      if (ev.mime.startsWith('image/')) return <img alt="output" src={`data:${ev.mime};base64,${String(ev.data)}`} />
      if (ev.mime === 'application/json') return <pre className="json">{JSON.stringify(ev.data, null, 2)}</pre>
      return <pre>{typeof ev.data === 'string' ? ev.data : JSON.stringify(ev.data)}</pre>
    case 'done':
      return null
  }
}

export default function NotebookPage() {
  const loc = useLocation()
  const [sp] = useSearchParams()
  const path = decodeURIComponent(loc.pathname.replace(/^\/notebook/, '')) || '/'
  const isFile = sp.get('file') === '1'
  const qc = useQueryClient()
  const { toast, node } = useToast()

  const nbq = useQuery({
    queryKey: ['notebook', path],
    queryFn: () => api.get<{ notebook: Notebook; language: string; path: string }>(`/api/2.0/lakeforge/notebooks?path=${encodeURIComponent(path)}`),
    enabled: !isFile,
  })
  const fileq = useQuery({
    queryKey: ['wsfile', path],
    queryFn: () => api.text(`/api/2.0/workspace-files${path}`),
    enabled: isFile,
  })
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: () => api.get<{ clusters: Cluster[] }>('/api/2.0/clusters/list'), refetchInterval: 10000 })

  const [nb, setNb] = useState<Notebook | null>(null)
  const [dirty, setDirty] = useState(false)
  const [clusterId, setClusterId] = useState<string>(() => localStorage.getItem('lakeforge.cluster') ?? '')
  const [contextId, setContextId] = useState<string | null>(null)
  const [running, setRunning] = useState<Record<string, string>>({}) // cell id -> command id
  const [active, setActive] = useState<string | null>(null)
  const [showWidgets, setShowWidgets] = useState(false)
  const [widgets, setWidgets] = useState<Record<string, string>>({})
  const [fileText, setFileText] = useState('')
  const [runAll, setRunAll] = useState(false)
  const aborts = useRef<Record<string, AbortController>>({})

  useEffect(() => {
    if (nbq.data) {
      setNb(nbq.data.notebook)
      setDirty(false)
    }
  }, [nbq.data])
  useEffect(() => {
    if (fileq.data !== undefined) setFileText(fileq.data)
  }, [fileq.data])

  // autosave
  useEffect(() => {
    if (!dirty || !nb) return
    const t = setTimeout(async () => {
      try {
        await api.put('/api/2.0/lakeforge/notebooks', { path, default_language: nb.default_language, cells: nb.cells, widgets: nb.widgets })
        setDirty(false)
      } catch (e) {
        toast(`Autosave failed: ${e instanceof Error ? e.message : e}`, 'err')
      }
    }, 1200)
    return () => clearTimeout(t)
  }, [dirty, nb, path, toast])

  const runningCluster = useMemo(() => clusters.data?.clusters.find((c) => c.cluster_id === clusterId), [clusters.data, clusterId])

  const update = (f: (n: Notebook) => Notebook) => {
    setNb((n) => (n ? f(n) : n))
    setDirty(true)
  }
  const setCell = (id: string, patch: Partial<Cell>) => update((n) => ({ ...n, cells: n.cells.map((c) => (c.id === id ? { ...c, ...patch } : c)) }))
  const addCell = (idx: number, language?: string) =>
    update((n) => {
      const cells = [...n.cells]
      cells.splice(idx, 0, newCell(language ?? n.default_language))
      return { ...n, cells }
    })
  const removeCell = (id: string) => update((n) => ({ ...n, cells: n.cells.length > 1 ? n.cells.filter((c) => c.id !== id) : n.cells }))
  const moveCell = (id: string, dir: -1 | 1) =>
    update((n) => {
      const i = n.cells.findIndex((c) => c.id === id)
      const j = i + dir
      if (i < 0 || j < 0 || j >= n.cells.length) return n
      const cells = [...n.cells]
      ;[cells[i], cells[j]] = [cells[j], cells[i]]
      return { ...n, cells }
    })

  const ensureContext = useCallback(async (): Promise<string> => {
    if (contextId) return contextId
    if (!clusterId) throw new Error('Attach a cluster first')
    const r = await api.post<{ id: string }>('/api/1.2/contexts/create', { clusterId, language: nb?.default_language ?? 'python', notebook_path: path })
    setContextId(r.id)
    return r.id
  }, [contextId, clusterId, nb?.default_language, path])

  const detach = async () => {
    if (contextId) {
      try {
        await api.post('/api/1.2/contexts/destroy', { clusterId, contextId })
      } catch {
        /* ignore */
      }
    }
    setContextId(null)
  }

  const runCell = useCallback(
    async (cell: Cell): Promise<boolean> => {
      if (cell.language === 'markdown' || !cell.source.trim()) return true
      const ctx = await ensureContext()
      const r = await api.post<{ id: string }>('/api/1.2/commands/execute', { clusterId, contextId: ctx, language: cell.language, command: cell.source })
      setRunning((m) => ({ ...m, [cell.id]: r.id }))
      setCell(cell.id, { outputs: [] })
      const outputs: KernelEvent[] = []
      let ok = true
      const ac = new AbortController()
      aborts.current[cell.id] = ac
      try {
        await streamEvents<KernelEvent>(`/api/2.0/lakeforge/commands/${r.id}/events`, (ev) => {
          if (ev.type === 'done') {
            ok = ev.status === 'Finished'
            return
          }
          if (ev.type === 'error') ok = false
          outputs.push(ev)
          setCell(cell.id, { outputs: [...outputs] })
        }, ac.signal)
      } finally {
        delete aborts.current[cell.id]
        setRunning((m) => {
          const c = { ...m }
          delete c[cell.id]
          return c
        })
      }
      void api.post('/api/2.0/lakeforge/notebooks/outputs', { path, cell_id: cell.id, outputs }).catch(() => {})
      return ok
    },
    [ensureContext, clusterId, path],
  )

  const cancelCell = async (cell: Cell) => {
    const cmd = running[cell.id]
    if (!cmd) return
    await api.post('/api/1.2/commands/cancel', { clusterId, contextId, commandId: cmd }).catch(() => {})
    aborts.current[cell.id]?.abort()
  }

  const runAllCells = async () => {
    if (!nb) return
    setRunAll(true)
    try {
      for (const c of nb.cells) {
        const ok = await runCell(c)
        if (!ok) break
      }
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    } finally {
      setRunAll(false)
    }
  }

  const clearOutputs = () => update((n) => ({ ...n, cells: n.cells.map((c) => ({ ...c, outputs: [] })) }))

  const exportSource = () => {
    window.open(`/api/2.0/workspace/export?path=${encodeURIComponent(path)}&format=SOURCE&direct_download=true`, '_blank')
  }

  const saveFile = async () => {
    try {
      await fetch(`/api/2.0/workspace-files${path}`, { method: 'PUT', headers: { Authorization: `Bearer ${localStorage.getItem('lakeforge.token')}`, 'Content-Type': 'text/plain' }, body: fileText })
      toast('Saved')
      await qc.invalidateQueries({ queryKey: ['wsfile', path] })
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }

  if (isFile) {
    return (
      <div className="page">
        {node}
        <div className="page-head">
          <div>
            <h1>{basename(path)}</h1>
            <div className="muted">
              <Link to={`/workspace?path=${encodeURIComponent(path.split('/').slice(0, -1).join('/') || '/')}`}>{path}</Link>
            </div>
          </div>
          <div className="actions">
            <button className="primary" onClick={saveFile}>Save</button>
          </div>
        </div>
        {fileq.isLoading ? <Spinner /> : <CodeEditor value={fileText} onChange={setFileText} minRows={20} />}
      </div>
    )
  }

  if (nbq.isLoading || !nb) return <div className="page">{nbq.error ? <ErrorBox error={nbq.error} /> : <Spinner label="Loading notebook…" />}</div>

  return (
    <div className="page notebook">
      {node}
      <div className="page-head">
        <div>
          <h1>
            {basename(path)} {dirty && <span className="muted small">● unsaved</span>}
          </h1>
          <div className="muted small">
            <Link to={`/workspace?path=${encodeURIComponent(path.split('/').slice(0, -1).join('/') || '/')}`}>{path}</Link> · default language: {nb.default_language}
          </div>
        </div>
        <div className="actions">
          <button onClick={exportSource}>Export</button>
          <button onClick={clearOutputs}>Clear outputs</button>
          <button onClick={() => setShowWidgets(true)}>Widgets ({Object.keys(nb.widgets ?? {}).length})</button>
        </div>
      </div>
      <div className="nb-toolbar">
        <select
          value={clusterId}
          onChange={(e) => {
            setClusterId(e.target.value)
            localStorage.setItem('lakeforge.cluster', e.target.value)
            setContextId(null)
          }}
        >
          <option value="">— attach cluster —</option>
          {(clusters.data?.clusters ?? []).map((c) => (
            <option key={c.cluster_id} value={c.cluster_id}>
              {c.cluster_name} ({c.state})
            </option>
          ))}
        </select>
        {runningCluster && <Badge state={runningCluster.state} />}
        {contextId ? (
          <button onClick={detach}>Detach kernel</button>
        ) : (
          <button disabled={!clusterId || runningCluster?.state !== 'RUNNING'} onClick={() => ensureContext().catch((e) => toast(String(e.message ?? e), 'err'))}>
            Attach
          </button>
        )}
        <button className="primary" disabled={!clusterId || runAll} onClick={runAllCells}>
          {runAll ? 'Running…' : '▶ Run all'}
        </button>
        {clusters.data?.clusters.length === 0 && (
          <span className="muted small">
            No clusters. <Link to="/compute?new=1">Create one</Link>.
          </span>
        )}
        <span className="muted small" style={{ marginLeft: 'auto' }}>Shift+Enter runs the focused cell</span>
      </div>

      <div className="add-cell">
        <button className="sm" onClick={() => addCell(0)}>＋ code</button>
        <button className="sm" onClick={() => addCell(0, 'markdown')}>＋ text</button>
      </div>
      {nb.cells.map((cell, idx) => {
        const isRunning = !!running[cell.id]
        return (
          <div key={cell.id}>
            <div className={`cell ${isRunning ? 'running' : ''} ${active === cell.id ? 'active' : ''}`} onClick={() => setActive(cell.id)}>
              <div className="cell-head">
                <span className="actions">
                  <span>Cmd {idx + 1}</span>
                  <select value={cell.language} onChange={(e) => setCell(cell.id, { language: e.target.value })}>
                    {LANGS.map((l) => (
                      <option key={l} value={l}>
                        {l === nb.default_language ? l : `%${l}`}
                      </option>
                    ))}
                  </select>
                  {isRunning && <Badge state="RUNNING">running</Badge>}
                </span>
                <span className="actions">
                  {isRunning ? (
                    <button className="sm danger" onClick={() => cancelCell(cell)}>■ Cancel</button>
                  ) : (
                    <button className="sm" disabled={!clusterId} onClick={() => runCell(cell).catch((e) => toast(String(e.message ?? e), 'err'))}>
                      ▶ Run
                    </button>
                  )}
                  <button className="sm" onClick={() => moveCell(cell.id, -1)}>↑</button>
                  <button className="sm" onClick={() => moveCell(cell.id, 1)}>↓</button>
                  <button className="sm" onClick={() => setCell(cell.id, { collapsed: !cell.collapsed })}>{cell.collapsed ? '⊞' : '⊟'}</button>
                  <button className="sm danger" onClick={() => removeCell(cell.id)}>✕</button>
                </span>
              </div>
              {!cell.collapsed &&
                (cell.language === 'markdown' && active !== cell.id && cell.source ? (
                  <div className="md-cell" dangerouslySetInnerHTML={{ __html: renderMarkdown(cell.source) }} />
                ) : (
                  <CodeEditor
                    value={cell.source}
                    language={cell.language}
                    onChange={(v) => setCell(cell.id, { source: v })}
                    onRun={() => runCell(cell).catch((e) => toast(String(e.message ?? e), 'err'))}
                    minRows={2}
                  />
                ))}
              {cell.outputs?.length > 0 && cell.language !== 'markdown' && (
                <div className="cell-out">
                  {cell.outputs.map((o, i) => (
                    <Output key={i} ev={o} />
                  ))}
                </div>
              )}
            </div>
            <div className="add-cell">
              <button className="sm" onClick={() => addCell(idx + 1)}>＋ code</button>
              <button className="sm" onClick={() => addCell(idx + 1, 'markdown')}>＋ text</button>
            </div>
          </div>
        )
      })}

      {showWidgets && (
        <Modal
          title="Notebook widgets"
          onClose={() => setShowWidgets(false)}
          footer={
            <>
              <button onClick={() => setShowWidgets(false)}>Close</button>
              <button
                className="primary"
                onClick={() => {
                  update((n) => ({ ...n, widgets: { ...n.widgets, ...widgets } }))
                  setShowWidgets(false)
                }}
              >
                Save
              </button>
            </>
          }
        >
          <div className="muted small">Widgets are exposed to code via <code>dbutils.widgets.get(name)</code>. Values set here become defaults for interactive runs.</div>
          {Object.entries({ ...(nb.widgets as Record<string, unknown>), ...widgets }).map(([k, v]) => (
            <Field key={k} label={k}>
              <input value={widgets[k] ?? (typeof v === 'object' && v && 'default' in (v as object) ? String((v as { default: unknown }).default) : String(v ?? ''))} onChange={(e) => setWidgets((w) => ({ ...w, [k]: e.target.value }))} />
            </Field>
          ))}
          <Field label="Add widget (name)">
            <input
              placeholder="name, then press Enter"
              onKeyDown={(e) => {
                if (e.key === 'Enter' && e.currentTarget.value.trim()) {
                  setWidgets((w) => ({ ...w, [e.currentTarget.value.trim()]: '' }))
                  e.currentTarget.value = ''
                }
              }}
            />
          </Field>
        </Modal>
      )}
    </div>
  )
}
