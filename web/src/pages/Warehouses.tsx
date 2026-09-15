import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { api, type Warehouse } from '../api'
import { Badge, ErrorBox, Field, Modal, Page, Spinner, Table, useToast } from '../components'

const SIZES = ['2X-Small', 'X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large']

export default function Warehouses() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const q = useQuery({ queryKey: ['warehouses'], queryFn: () => api.get<{ warehouses: Warehouse[] }>('/api/2.0/sql/warehouses'), refetchInterval: 5000 })
  const [creating, setCreating] = useState(false)
  const [editing, setEditing] = useState<Warehouse | null>(null)
  const [f, setF] = useState({ name: '', cluster_size: '2X-Small', auto_stop_mins: 45 })
  const [err, setErr] = useState<unknown>(null)
  const refresh = () => qc.invalidateQueries({ queryKey: ['warehouses'] })
  const act = async (w: Warehouse, op: 'start' | 'stop' | 'delete') => {
    try {
      if (op === 'delete') {
        if (!window.confirm(`Delete warehouse ${w.name}?`)) return
        await api.delete(`/api/2.0/sql/warehouses/${w.id}`)
      } else await api.post(`/api/2.0/sql/warehouses/${w.id}/${op}`)
      toast(`${op} ${w.name}`)
      await refresh()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  const submit = async () => {
    setErr(null)
    try {
      if (editing) await api.post(`/api/2.0/sql/warehouses/${editing.id}/edit`, f)
      else await api.post('/api/2.0/sql/warehouses', { ...f, enable_serverless_compute: false, warehouse_type: 'PRO' })
      setCreating(false)
      setEditing(null)
      await refresh()
    } catch (e) {
      setErr(e)
    }
  }
  return (
    <Page title="SQL Warehouses" subtitle="Each warehouse is backed by a dedicated Forge cluster sized by T-shirt size." actions={<button className="primary" onClick={() => { setF({ name: '', cluster_size: '2X-Small', auto_stop_mins: 45 }); setCreating(true) }}>＋ Create SQL warehouse</button>}>
      {node}
      {q.isLoading && <Spinner />}
      <ErrorBox error={q.error} />
      <Table
        rows={q.data?.warehouses ?? []}
        keyOf={(w) => w.id}
        columns={[
          { key: 'name', title: 'Name', render: (w) => <b>{w.name}</b> },
          { key: 'state', title: 'State', render: (w) => <Badge state={w.state} /> },
          { key: 'size', title: 'Size', render: (w) => w.cluster_size },
          { key: 'stop', title: 'Auto stop', render: (w) => `${w.auto_stop_mins} min` },
          { key: 'cluster', title: 'Cluster', render: (w) => (w.cluster_id ? <Link to={`/compute/${w.cluster_id}`}><code>{w.cluster_id}</code></Link> : '—') },
          { key: 'jdbc', title: 'JDBC', render: (w) => <code className="small">{w.jdbc_url}</code> },
          {
            key: 'act', title: '', width: '260px',
            render: (w) => (
              <span className="actions">
                {w.state === 'STOPPED' || w.state === 'DELETED' ? <button className="sm" onClick={() => act(w, 'start')}>▶ Start</button> : w.state === 'RUNNING' ? <button className="sm" onClick={() => act(w, 'stop')}>■ Stop</button> : null}
                <Link className="btn sm" to="/sql">Open editor</Link>
                <button className="sm" onClick={() => { setEditing(w); setF({ name: w.name, cluster_size: w.cluster_size, auto_stop_mins: w.auto_stop_mins }) }}>Edit</button>
                <button className="sm danger" onClick={() => act(w, 'delete')}>Delete</button>
              </span>
            ),
          },
        ]}
        emptyText="No warehouses."
      />
      {(creating || editing) && (
        <Modal
          title={editing ? `Edit ${editing.name}` : 'New SQL warehouse'}
          onClose={() => { setCreating(false); setEditing(null) }}
          footer={
            <>
              <button onClick={() => { setCreating(false); setEditing(null) }}>Cancel</button>
              <button className="primary" disabled={!f.name.trim()} onClick={submit}>{editing ? 'Save' : 'Create'}</button>
            </>
          }
        >
          <Field label="Name"><input autoFocus value={f.name} onChange={(e) => setF({ ...f, name: e.target.value })} /></Field>
          <Field label="Cluster size">
            <select value={f.cluster_size} onChange={(e) => setF({ ...f, cluster_size: e.target.value })}>
              {SIZES.map((s) => <option key={s}>{s}</option>)}
            </select>
          </Field>
          <Field label="Auto stop (minutes)"><input type="number" value={f.auto_stop_mins} onChange={(e) => setF({ ...f, auto_stop_mins: Number(e.target.value) })} /></Field>
          <ErrorBox error={err} />
        </Modal>
      )}
    </Page>
  )
}
