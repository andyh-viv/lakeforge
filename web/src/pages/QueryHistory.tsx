import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { api, fmtDuration, fmtTime, type QueryHistoryEntry } from '../api'
import { Badge, ErrorBox, JsonView, Modal, Page, Spinner, Table } from '../components'

export default function QueryHistory() {
  const q = useQuery({
    queryKey: ['sql-history'],
    queryFn: () => api.get<{ res: QueryHistoryEntry[] }>('/api/2.0/sql/history/queries?max_results=200'),
    refetchInterval: 5000,
  })
  const [sel, setSel] = useState<QueryHistoryEntry | null>(null)
  const [filter, setFilter] = useState('')
  const rows = (q.data?.res ?? []).filter((r) => !filter || r.query_text.toLowerCase().includes(filter.toLowerCase()) || (r.executed_as_user_name ?? '').includes(filter))
  return (
    <Page title="Query History" actions={<input placeholder="Filter by text or user…" value={filter} onChange={(e) => setFilter(e.target.value)} />}>
      {q.isLoading && <Spinner />}
      <ErrorBox error={q.error} />
      <Table
        rows={rows}
        keyOf={(r) => r.query_id}
        onRowClick={setSel}
        columns={[
          { key: 'sql', title: 'Statement', render: (r) => <code className="small">{r.query_text.length > 100 ? r.query_text.slice(0, 100) + '…' : r.query_text}</code> },
          { key: 'status', title: 'Status', render: (r) => <Badge state={r.status} /> },
          { key: 'user', title: 'User', render: (r) => r.executed_as_user_name },
          { key: 'start', title: 'Started', render: (r) => fmtTime(r.query_start_time_ms) },
          { key: 'dur', title: 'Duration', render: (r) => fmtDuration(r.duration) },
          { key: 'rows', title: 'Rows', render: (r) => r.metrics?.rows_produced_count ?? r.rows_produced ?? '—' },
        ]}
        emptyText="No queries executed yet."
      />
      {sel && (
        <Modal title="Query details" onClose={() => setSel(null)} wide>
          <pre className="json">{sel.query_text}</pre>
          {sel.error_message && <ErrorBox error={sel.error_message} />}
          <JsonView value={sel} />
        </Modal>
      )}
    </Page>
  )
}
