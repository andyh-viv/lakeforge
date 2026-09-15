import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { api, fmtTime, type Alert, type SqlQuery } from '../api'
import { Badge, ErrorBox, Field, JsonView, Modal, Page, Spinner, Table, useToast } from '../components'

const OPS = ['GREATER_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN', 'LESS_THAN_OR_EQUAL', 'EQUAL', 'NOT_EQUAL']

export default function Alerts() {
  const qc = useQueryClient()
  const { toast, node } = useToast()
  const alerts = useQuery({ queryKey: ['alerts'], queryFn: () => api.get<{ results: Alert[] }>('/api/2.0/sql/alerts'), refetchInterval: 10000 })
  const queries = useQuery({ queryKey: ['sql-queries'], queryFn: () => api.get<{ results: SqlQuery[] }>('/api/2.0/sql/queries') })
  const [creating, setCreating] = useState(false)
  const [sel, setSel] = useState<Alert | null>(null)
  const [f, setF] = useState({ display_name: '', query_id: '', column: '', op: 'GREATER_THAN', value: '0', seconds_to_retrigger: 0 })
  const [err, setErr] = useState<unknown>(null)
  const refresh = () => qc.invalidateQueries({ queryKey: ['alerts'] })
  const create = async () => {
    setErr(null)
    try {
      const num = Number(f.value)
      const threshold = Number.isNaN(num) ? { value: { string_value: f.value } } : { value: { double_value: num } }
      await api.post('/api/2.0/sql/alerts', {
        display_name: f.display_name,
        query_id: f.query_id,
        seconds_to_retrigger: f.seconds_to_retrigger,
        condition: { op: f.op, operand: { column: { name: f.column } }, threshold },
      })
      setCreating(false)
      await refresh()
    } catch (e) {
      setErr(e)
    }
  }
  const evaluate = async (a: Alert) => {
    try {
      const r = await api.post<{ state: string }>(`/api/2.0/lakeforge/sql/alerts/${a.id}/evaluate`)
      toast(`${a.display_name}: ${r.state}`)
      await refresh()
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'err')
    }
  }
  const qname = (id: string) => queries.data?.results.find((q) => q.id === id)?.display_name ?? id
  return (
    <Page title="Alerts" subtitle="Alerts evaluate a saved query and trigger when the first row of a column crosses a threshold." actions={<button className="primary" onClick={() => { setF({ display_name: '', query_id: queries.data?.results[0]?.id ?? '', column: '', op: 'GREATER_THAN', value: '0', seconds_to_retrigger: 0 }); setCreating(true) }}>＋ Create alert</button>}>
      {node}
      {alerts.isLoading && <Spinner />}
      <ErrorBox error={alerts.error} />
      <Table
        rows={alerts.data?.results ?? []}
        keyOf={(a) => a.id}
        onRowClick={setSel}
        columns={[
          { key: 'n', title: 'Name', render: (a) => <b>{a.display_name}</b> },
          { key: 's', title: 'State', render: (a) => <Badge state={a.state === 'TRIGGERED' ? 'ERROR' : a.state}>{a.state}</Badge> },
          { key: 'q', title: 'Query', render: (a) => <Link to={`/sql?query=${a.query_id}`} onClick={(e) => e.stopPropagation()}>{qname(a.query_id)}</Link> },
          { key: 't', title: 'Last triggered', render: (a) => fmtTime(a.trigger_time) },
          { key: 'u', title: 'Updated', render: (a) => fmtTime(a.update_time) },
          {
            key: 'a', title: '', width: '200px',
            render: (a) => (
              <span className="actions" onClick={(e) => e.stopPropagation()}>
                <button className="sm" onClick={() => evaluate(a)}>Evaluate now</button>
                <button className="sm danger" onClick={async () => { if (window.confirm('Delete alert?')) { await api.delete(`/api/2.0/sql/alerts/${a.id}`); await refresh() } }}>Delete</button>
              </span>
            ),
          },
        ]}
        emptyText="No alerts. Save a query in the SQL editor first, then create an alert on it."
      />
      {creating && (
        <Modal title="New alert" onClose={() => setCreating(false)} footer={<><button onClick={() => setCreating(false)}>Cancel</button><button className="primary" disabled={!f.display_name || !f.query_id || !f.column} onClick={create}>Create</button></>}>
          <Field label="Name"><input autoFocus value={f.display_name} onChange={(e) => setF({ ...f, display_name: e.target.value })} /></Field>
          <Field label="Saved query">
            <select value={f.query_id} onChange={(e) => setF({ ...f, query_id: e.target.value })}>
              <option value="">— select —</option>
              {(queries.data?.results ?? []).map((q) => <option key={q.id} value={q.id}>{q.display_name}</option>)}
            </select>
          </Field>
          <div className="grid2">
            <Field label="Column"><input value={f.column} onChange={(e) => setF({ ...f, column: e.target.value })} /></Field>
            <Field label="Operator">
              <select value={f.op} onChange={(e) => setF({ ...f, op: e.target.value })}>{OPS.map((o) => <option key={o}>{o}</option>)}</select>
            </Field>
            <Field label="Threshold"><input value={f.value} onChange={(e) => setF({ ...f, value: e.target.value })} /></Field>
            <Field label="Re-trigger after (seconds, 0 = once)"><input type="number" value={f.seconds_to_retrigger} onChange={(e) => setF({ ...f, seconds_to_retrigger: Number(e.target.value) })} /></Field>
          </div>
          <ErrorBox error={err} />
        </Modal>
      )}
      {sel && <Modal title={sel.display_name} onClose={() => setSel(null)} wide><JsonView value={sel} /></Modal>}
    </Page>
  )
}
