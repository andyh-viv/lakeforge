import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type ReactNode } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, fmtTime, qs, type CatalogItem, type StatementResponse, type Warehouse } from '../api'
import { Badge, Card, ErrorBox, Field, JsonView, KV, Modal, Page, ResultGrid, Spinner, Table, Tabs, useToast } from '../components'

const UC = '/api/2.1/unity-catalog'
const PRIVS: Record<string, string[]> = {
  catalog: ['ALL_PRIVILEGES', 'USE_CATALOG', 'USE_SCHEMA', 'CREATE_SCHEMA', 'CREATE_TABLE', 'CREATE_VOLUME', 'CREATE_FUNCTION', 'SELECT', 'MODIFY', 'READ_VOLUME', 'WRITE_VOLUME', 'EXECUTE', 'BROWSE'],
  schema: ['ALL_PRIVILEGES', 'USE_SCHEMA', 'CREATE_TABLE', 'CREATE_VOLUME', 'CREATE_FUNCTION', 'SELECT', 'MODIFY', 'READ_VOLUME', 'WRITE_VOLUME', 'EXECUTE', 'BROWSE'],
  table: ['ALL_PRIVILEGES', 'SELECT', 'MODIFY', 'BROWSE'],
  volume: ['ALL_PRIVILEGES', 'READ_VOLUME', 'WRITE_VOLUME', 'BROWSE'],
  function: ['ALL_PRIVILEGES', 'EXECUTE', 'BROWSE'],
}

function Grants({ type, fullName }: { type: string; fullName: string }) {
  const qc = useQueryClient()
  const key = ['grants', type, fullName]
  const g = useQuery({ queryKey: key, queryFn: () => api.get<{ privilege_assignments: { principal: string; privileges: string[] }[] }>(`${UC}/permissions/${type}/${encodeURIComponent(fullName)}`) })
  const [principal, setPrincipal] = useState('')
  const [priv, setPriv] = useState(PRIVS[type]?.[1] ?? 'SELECT')
  const change = async (p: string, add: string[], remove: string[]) => {
    await api.patch(`${UC}/permissions/${type}/${encodeURIComponent(fullName)}`, { changes: [{ principal: p, add, remove }] })
    void qc.invalidateQueries({ queryKey: key })
  }
  return (
    <Card title="Permissions">
      <Table
        rows={g.data?.privilege_assignments ?? []}
        keyOf={(a) => a.principal}
        columns={[
          { key: 'p', title: 'Principal', render: (a) => <b>{a.principal}</b> },
          { key: 'v', title: 'Privileges', render: (a) => a.privileges.map((p) => <span key={p} className="badge" style={{ marginRight: 4 }}>{p} <a onClick={() => change(a.principal, [], [p])} style={{ cursor: 'pointer' }}>×</a></span>) },
        ]}
        emptyText="No grants. Admins and owners have implicit access."
      />
      <div className="toolbar" style={{ marginTop: 8 }}>
        <input placeholder="user@example.com or group" value={principal} onChange={(e) => setPrincipal(e.target.value)} />
        <select value={priv} onChange={(e) => setPriv(e.target.value)}>{(PRIVS[type] ?? PRIVS.table).map((p) => <option key={p}>{p}</option>)}</select>
        <button className="sm primary" disabled={!principal} onClick={() => { void change(principal, [priv], []); setPrincipal('') }}>Grant</button>
      </div>
    </Card>
  )
}

function useWarehouse() {
  const w = useQuery({ queryKey: ['warehouses'], queryFn: () => api.get<{ warehouses: Warehouse[] }>('/api/2.0/sql/warehouses') })
  return w.data?.warehouses.find((x) => x.state === 'RUNNING') ?? w.data?.warehouses[0]
}

function SamplePreview({ full }: { full: string }) {
  const wh = useWarehouse()
  const q = useQuery({
    queryKey: ['sample', full, wh?.id],
    enabled: !!wh,
    retry: false,
    queryFn: async () => {
      let r = await api.post<StatementResponse>('/api/2.0/sql/statements', { warehouse_id: wh!.id, statement: `SELECT * FROM ${full} LIMIT 100`, wait_timeout: '50s', row_limit: 100 })
      while (r.status.state === 'PENDING' || r.status.state === 'RUNNING') {
        await new Promise((res) => setTimeout(res, 500))
        r = await api.get<StatementResponse>(`/api/2.0/sql/statements/${r.statement_id}`)
      }
      if (r.status.state !== 'SUCCEEDED') throw new Error(r.status.error?.message ?? r.status.state)
      return r
    },
  })
  if (!wh) return <div className="muted small">No SQL warehouse available for sampling.</div>
  if (q.isLoading) return <Spinner label="Sampling…" />
  if (q.error) return <ErrorBox error={q.error} />
  const cols = (q.data?.manifest?.schema?.columns ?? []).map((c) => c.name)
  return <ResultGrid columns={cols} rows={q.data?.result?.data_array ?? []} />
}

function Details({ type, item }: { type: 'catalog' | 'schema' | 'table' | 'volume' | 'function'; item: CatalogItem & Record<string, unknown> }) {
  const qc = useQueryClient()
  const nav = useNavigate()
  const { toast, node } = useToast()
  const [tab, setTab] = useState(type === 'table' ? 'columns' : 'overview')
  const patch = async (body: Record<string, unknown>) => {
    const path = type === 'catalog' ? `${UC}/catalogs/${item.name}` : `${UC}/${type}s/${encodeURIComponent(item.full_name)}`
    await api.patch(path, body)
    void qc.invalidateQueries({ queryKey: ['uc'] })
    toast('Saved')
  }
  const del = async () => {
    if (!window.confirm(`Drop ${type} ${item.full_name}?`)) return
    const path = type === 'catalog' ? `${UC}/catalogs/${item.name}?force=true` : `${UC}/${type}s/${encodeURIComponent(item.full_name)}${type === 'schema' ? '?force=true' : ''}`
    await api.delete(path)
    void qc.invalidateQueries({ queryKey: ['uc'] })
    nav(`/catalog/${item.full_name.split('.').slice(0, -1).join('/')}`)
  }
  const tabs = [{ id: 'overview', label: 'Overview' }, { id: 'perms', label: 'Permissions' }, { id: 'json', label: 'JSON' }]
  if (type === 'table') tabs.unshift({ id: 'columns', label: `Columns (${item.columns?.length ?? 0})` }, { id: 'sample', label: 'Sample data' })
  return (
    <Card
      title={<span>{item.full_name} <Badge>{type.toUpperCase()}</Badge></span>}
      actions={
        <>
          {type === 'table' && <Link className="btn" to={`/sql?sql=${encodeURIComponent(`SELECT * FROM ${item.full_name} LIMIT 100`)}`}>Query</Link>}
          <button className="sm" onClick={async () => { const c = window.prompt('Comment', item.comment ?? ''); if (c !== null) await patch({ comment: c }) }}>Comment</button>
          <button className="sm" onClick={async () => { const o = window.prompt('Owner', item.owner ?? ''); if (o) await patch({ owner: o }) }}>Owner</button>
          <button className="sm danger" onClick={del}>Drop</button>
        </>
      }
    >
      {node}
      <Tabs tabs={tabs} active={tab} onChange={setTab} />
      {tab === 'columns' && (
        <Table
          rows={item.columns ?? []}
          keyOf={(c) => c.name}
          columns={[
            { key: 'i', title: '#', render: (c) => c.position, width: '40px' },
            { key: 'n', title: 'Name', render: (c) => <b>{c.name}</b> },
            { key: 't', title: 'Type', render: (c) => <code>{c.type_text}</code> },
            { key: 'nl', title: 'Nullable', render: (c) => (c.nullable === false ? 'no' : 'yes') },
            { key: 'c', title: 'Comment', render: (c) => c.comment ?? '' },
          ]}
          emptyText="No column metadata (schema is inferred at query time)."
        />
      )}
      {tab === 'sample' && <SamplePreview full={item.full_name} />}
      {tab === 'overview' && (
        <KV
          items={[
            ['Owner', item.owner ?? '—'],
            ['Comment', item.comment || '—'],
            ['Created', fmtTime(item.created_at)],
            ['Updated', fmtTime(item.updated_at)],
            ...(type === 'table' ? ([['Table type', item.table_type ?? ''], ['Format', item.data_source_format ?? ''], ['Location', <code key="location" className="small">{item.storage_location ?? ''}</code>]] as [string, ReactNode][]) : []),
            ...(type === 'volume' ? ([['Volume type', String(item.volume_type ?? '')], ['Location', <code key="location" className="small">{String(item.storage_location ?? '')}</code>]] as [string, ReactNode][]) : []),
            ...(type === 'function' ? ([['Language', String(item.routine_definition_language ?? item.external_language ?? 'SQL')], ['Definition', <pre key="definition" className="small">{String(item.routine_definition ?? '')}</pre>]] as [string, ReactNode][]) : []),
            ['Properties', Object.entries(item.properties ?? {}).map(([k, v]) => `${k}=${v}`).join(', ') || '—'],
          ]}
        />
      )}
      {tab === 'perms' && <Grants type={type} fullName={item.full_name} />}
      {tab === 'json' && <JsonView value={item} />}
    </Card>
  )
}

function CreateModal({ kind, parent, onClose }: { kind: 'catalog' | 'schema' | 'table' | 'volume'; parent: string[]; onClose: () => void }) {
  const qc = useQueryClient()
  const [name, setName] = useState('')
  const [comment, setComment] = useState('')
  const [cols, setCols] = useState('id BIGINT\nname STRING')
  const [vtype, setVtype] = useState('MANAGED')
  const [loc, setLoc] = useState('')
  const [err, setErr] = useState<unknown>(null)
  const submit = async () => {
    setErr(null)
    try {
      if (kind === 'catalog') await api.post(`${UC}/catalogs`, { name, comment })
      else if (kind === 'schema') await api.post(`${UC}/schemas`, { name, catalog_name: parent[0], comment })
      else if (kind === 'volume') await api.post(`${UC}/volumes`, { name, catalog_name: parent[0], schema_name: parent[1], volume_type: vtype, storage_location: vtype === 'EXTERNAL' ? loc : undefined, comment })
      else
        await api.post(`${UC}/tables`, {
          name,
          catalog_name: parent[0],
          schema_name: parent[1],
          table_type: loc ? 'EXTERNAL' : 'MANAGED',
          data_source_format: 'DELTA',
          storage_location: loc || undefined,
          comment,
          columns: cols.split('\n').map((l) => l.trim()).filter(Boolean).map((l) => { const [n, ...t] = l.split(/\s+/); return { name: n, type_text: t.join(' ') || 'string' } }),
        })
      void qc.invalidateQueries({ queryKey: ['uc'] })
      onClose()
    } catch (e) {
      setErr(e)
    }
  }
  return (
    <Modal title={`Create ${kind}`} onClose={onClose} footer={<><button onClick={onClose}>Cancel</button><button className="primary" disabled={!name} onClick={submit}>Create</button></>}>
      <Field label="Name"><input autoFocus value={name} onChange={(e) => setName(e.target.value.replace(/[^A-Za-z0-9_]/g, '_'))} /></Field>
      <Field label="Comment"><input value={comment} onChange={(e) => setComment(e.target.value)} /></Field>
      {kind === 'table' && <Field label="Columns" hint="name TYPE per line; leave empty to infer"><textarea rows={4} className="mono" value={cols} onChange={(e) => setCols(e.target.value)} /></Field>}
      {(kind === 'table' || kind === 'volume') && <Field label="External location (optional)" hint="s3://, gs://, abfss:// or file path; empty = managed"><input value={loc} onChange={(e) => { setLoc(e.target.value); if (kind === 'volume') setVtype(e.target.value ? 'EXTERNAL' : 'MANAGED') }} /></Field>}
      <ErrorBox error={err} />
    </Modal>
  )
}

export default function Catalog() {
  const params = useParams()
  const nav = useNavigate()
  const parts = (params['*'] ?? '').split('/').filter(Boolean)
  const [cat, sch, obj] = parts
  const [create, setCreate] = useState<'catalog' | 'schema' | 'table' | 'volume' | null>(null)
  const [filter, setFilter] = useState('')
  const browse = useQuery({
    queryKey: ['uc', 'browse', cat ?? '', sch ?? ''],
    queryFn: () => api.get<{ catalogs?: CatalogItem[]; schemas?: CatalogItem[]; tables?: CatalogItem[]; volumes?: CatalogItem[]; functions?: CatalogItem[] }>(`/api/2.0/lakeforge/catalog/browse${qs({ catalog_name: cat, schema_name: sch })}`),
  })
  const detail = useQuery({
    queryKey: ['uc', 'detail', parts.join('.')],
    enabled: parts.length > 0,
    queryFn: async () => {
      if (parts.length === 1) return { type: 'catalog' as const, item: await api.get<CatalogItem & Record<string, unknown>>(`${UC}/catalogs/${cat}`) }
      if (parts.length === 2) return { type: 'schema' as const, item: await api.get<CatalogItem & Record<string, unknown>>(`${UC}/schemas/${cat}.${sch}`) }
      const full = `${cat}.${sch}.${obj}`
      const attempts: ['table' | 'volume' | 'function', string][] = [['table', 'tables'], ['volume', 'volumes'], ['function', 'functions']]
      for (const [t, p] of attempts) {
        try {
          return { type: t, item: await api.get<CatalogItem & Record<string, unknown>>(`${UC}/${p}/${encodeURIComponent(full)}`) }
        } catch {
          continue
        }
      }
      throw new Error(`${full} not found`)
    },
  })
  const crumbs = [{ label: 'Catalogs', to: '/catalog' }, ...parts.map((p, i) => ({ label: p, to: `/catalog/${parts.slice(0, i + 1).join('/')}` }))]
  const f = (xs?: CatalogItem[]) => (xs ?? []).filter((x) => x.name.toLowerCase().includes(filter.toLowerCase()))
  const list = (rows: CatalogItem[], kind: string, to: (x: CatalogItem) => string, extra?: (x: CatalogItem) => ReactNode) => (
    <Table
      rows={rows}
      keyOf={(x) => x.full_name ?? x.name}
      onRowClick={(x) => nav(to(x))}
      columns={[
        { key: 'n', title: kind, render: (x) => <b>{x.name}</b> },
        ...(extra ? [{ key: 'x', title: 'Type', render: extra }] : []),
        { key: 'o', title: 'Owner', render: (x) => x.owner ?? '' },
        { key: 'c', title: 'Comment', render: (x) => x.comment ?? '' },
        { key: 't', title: 'Updated', render: (x) => fmtTime(x.updated_at ?? x.created_at) },
      ]}
      emptyText={`No ${kind.toLowerCase()}s.`}
    />
  )
  return (
    <Page
      title="Catalog Explorer"
      subtitle="Unity-Catalog-style three-level namespace: catalog.schema.object with grants."
      actions={
        <>
          {parts.length === 0 && <button className="primary" onClick={() => setCreate('catalog')}>＋ Create catalog</button>}
          {parts.length === 1 && <button className="primary" onClick={() => setCreate('schema')}>＋ Create schema</button>}
          {parts.length === 2 && (
            <>
              <button className="primary" onClick={() => setCreate('table')}>＋ Create table</button>
              <button onClick={() => setCreate('volume')}>＋ Create volume</button>
              <Link className="btn" to={`/sql?catalog=${cat}&schema=${sch}`}>Open in SQL editor</Link>
            </>
          )}
        </>
      }
    >
      <div className="crumbs">
        {crumbs.map((c, i) => (
          <span key={c.to}>
            {i > 0 && <span className="sep">›</span>}
            <Link to={c.to}>{c.label}</Link>
          </span>
        ))}
      </div>
      <ErrorBox error={browse.error} />
      {parts.length > 0 && (detail.isLoading ? <Spinner /> : detail.error ? <ErrorBox error={detail.error} /> : detail.data && <Details type={detail.data.type} item={detail.data.item} />)}
      {parts.length < 3 && (
        <>
          <div className="toolbar"><input style={{ flex: 1 }} placeholder="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} /></div>
          {browse.isLoading && <Spinner />}
          {parts.length === 0 && list(f(browse.data?.catalogs), 'Catalog', (x) => `/catalog/${x.name}`, (x) => x.catalog_type ?? 'MANAGED_CATALOG')}
          {parts.length === 1 && list(f(browse.data?.schemas), 'Schema', (x) => `/catalog/${cat}/${x.name}`)}
          {parts.length === 2 && (
            <>
              <h4>Tables</h4>
              {list(f(browse.data?.tables), 'Table', (x) => `/catalog/${cat}/${sch}/${x.name}`, (x) => `${x.table_type ?? ''} ${x.data_source_format ?? ''}`)}
              {(browse.data?.volumes?.length ?? 0) > 0 && (<><h4>Volumes</h4>{list(f(browse.data?.volumes), 'Volume', (x) => `/catalog/${cat}/${sch}/${x.name}`)}</>)}
              {(browse.data?.functions?.length ?? 0) > 0 && (<><h4>Functions</h4>{list(f(browse.data?.functions), 'Function', (x) => `/catalog/${cat}/${sch}/${x.name}`)}</>)}
            </>
          )}
        </>
      )}
      {create && <CreateModal kind={create} parent={parts} onClose={() => setCreate(null)} />}
    </Page>
  )
}
