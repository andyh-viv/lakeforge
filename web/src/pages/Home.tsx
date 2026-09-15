import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api, fmtTime, type Cluster, type Job, type LakeforgeInfo, type Pipeline, type Run, type Warehouse } from '../api'
import { Badge, Card, Page, Table } from '../components'
import { useAuth } from '../auth'

export default function Home() {
  const { me } = useAuth()
  const info = useQuery({ queryKey: ['info'], queryFn: () => api.get<LakeforgeInfo>('/api/2.0/lakeforge/info'), refetchInterval: 15000 })
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: () => api.get<{ clusters: Cluster[] }>('/api/2.0/clusters/list'), refetchInterval: 10000 })
  const warehouses = useQuery({ queryKey: ['warehouses'], queryFn: () => api.get<{ warehouses: Warehouse[] }>('/api/2.0/sql/warehouses') })
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => api.get<{ jobs: Job[] }>('/api/2.1/jobs/list?limit=100') })
  const runs = useQuery({ queryKey: ['runs', 'recent'], queryFn: () => api.get<{ runs: Run[] }>('/api/2.1/jobs/runs/list?limit=8'), refetchInterval: 10000 })
  const pipelines = useQuery({ queryKey: ['pipelines'], queryFn: () => api.get<{ statuses: Pipeline[] }>('/api/2.0/pipelines') })

  const running = (clusters.data?.clusters ?? []).filter((c) => c.state === 'RUNNING').length
  return (
    <Page title={`Welcome, ${me?.display_name || me?.user_name}`} subtitle={info.data ? `${info.data.name} ${info.data.version} · ${info.data.engine.name} engine (${info.data.engine.sql} + ${info.data.engine.table_format}) · cloud: ${info.data.cloud}` : ''}>
      <div className="grid4">
        <div className="stat">
          <div className="n">{running}</div>
          <div className="l">running clusters · <Link to="/compute">{clusters.data?.clusters.length ?? 0} total</Link></div>
        </div>
        <div className="stat">
          <div className="n">{warehouses.data?.warehouses.length ?? 0}</div>
          <div className="l"><Link to="/sql/warehouses">SQL warehouses</Link></div>
        </div>
        <div className="stat">
          <div className="n">{jobs.data?.jobs.length ?? 0}</div>
          <div className="l"><Link to="/workflows">workflows</Link></div>
        </div>
        <div className="stat">
          <div className="n">{pipelines.data?.statuses.length ?? 0}</div>
          <div className="l"><Link to="/pipelines">DLT pipelines</Link></div>
        </div>
      </div>
      <div className="grid2">
        <Card title="Get started">
          <div className="actions" style={{ flexDirection: 'column', alignItems: 'stretch' }}>
            <Link className="btn" to="/workspace?new=notebook">＋ Create a notebook</Link>
            <Link className="btn" to="/sql">›_ Open the SQL editor</Link>
            <Link className="btn" to="/compute?new=1">⚙ Create a cluster</Link>
            <Link className="btn" to="/workflows?new=1">⇶ Create a workflow</Link>
            <Link className="btn" to="/catalog">▦ Browse the catalog</Link>
          </div>
        </Card>
        <Card title="Recent job runs" actions={<Link to="/workflows">View all</Link>}>
          <Table
            rows={runs.data?.runs ?? []}
            keyOf={(r) => r.run_id}
            columns={[
              { key: 'name', title: 'Run', render: (r) => <Link to={`/runs/${r.run_id}`}>{r.run_name || `run ${r.run_id}`}</Link> },
              { key: 'state', title: 'State', render: (r) => <Badge state={r.state.result_state ?? r.state.life_cycle_state} /> },
              { key: 'start', title: 'Started', render: (r) => fmtTime(r.start_time) },
            ]}
            emptyText="No runs yet."
          />
        </Card>
      </div>
      <Card title="Platform features">
        <div className="actions">{(info.data?.features ?? []).map((f) => <Badge key={f}>{f}</Badge>)}</div>
      </Card>
    </Page>
  )
}
