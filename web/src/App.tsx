import { NavLink, Navigate, Route, Routes, useNavigate } from 'react-router-dom'
import { useState, type FormEvent } from 'react'
import { useAuth } from './auth'
import { Spinner } from './components'
import Home from './pages/Home'
import Workspace from './pages/Workspace'
import NotebookPage from './pages/Notebook'
import SqlEditor from './pages/SqlEditor'
import Catalog from './pages/Catalog'
import Compute from './pages/Compute'
import Warehouses from './pages/Warehouses'
import Workflows from './pages/Workflows'
import RunDetail from './pages/RunDetail'
import Pipelines from './pages/Pipelines'
import Experiments from './pages/Experiments'
import Models from './pages/Models'
import Serving from './pages/Serving'
import Repos from './pages/Repos'
import Dbfs from './pages/Dbfs'
import Admin from './pages/Admin'
import Settings from './pages/Settings'
import QueryHistory from './pages/QueryHistory'
import Dashboards from './pages/Dashboards'
import Alerts from './pages/Alerts'
import Secrets from './pages/Secrets'

const NAV: { group: string; items: { to: string; label: string; ico: string; admin?: boolean }[] }[] = [
  {
    group: 'Workspace',
    items: [
      { to: '/', label: 'Home', ico: '⌂' },
      { to: '/workspace', label: 'Workspace', ico: '▤' },
      { to: '/repos', label: 'Repos', ico: '⑂' },
      { to: '/catalog', label: 'Catalog', ico: '▦' },
      { to: '/dbfs', label: 'DBFS / Files', ico: '▣' },
    ],
  },
  {
    group: 'SQL',
    items: [
      { to: '/sql', label: 'SQL Editor', ico: '›_' },
      { to: '/sql/dashboards', label: 'Dashboards', ico: '▥' },
      { to: '/sql/alerts', label: 'Alerts', ico: '!' },
      { to: '/sql/history', label: 'Query History', ico: '⟲' },
      { to: '/sql/warehouses', label: 'SQL Warehouses', ico: '⌸' },
    ],
  },
  {
    group: 'Data Engineering',
    items: [
      { to: '/workflows', label: 'Workflows', ico: '⇶' },
      { to: '/pipelines', label: 'Delta Live Tables', ico: '△' },
      { to: '/compute', label: 'Compute', ico: '⚙' },
    ],
  },
  {
    group: 'Machine Learning',
    items: [
      { to: '/ml/experiments', label: 'Experiments', ico: '◔' },
      { to: '/ml/models', label: 'Models', ico: '◈' },
      { to: '/ml/serving', label: 'Serving', ico: '⇪' },
    ],
  },
  {
    group: 'Security',
    items: [
      { to: '/secrets', label: 'Secrets', ico: '⚿' },
      { to: '/admin', label: 'Admin', ico: '☰', admin: true },
      { to: '/settings', label: 'Settings', ico: '⚙' },
    ],
  },
]

function Login() {
  const { login } = useAuth()
  const [u, setU] = useState('admin@lakeforge.local')
  const [p, setP] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setErr(null)
    try {
      await login(u, p)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }
  return (
    <div className="login">
      <form className="box" onSubmit={submit}>
        <div className="brand">
          <span className="logo">▲</span> Lakeforge
        </div>
        <div className="muted">Sign in to your workspace</div>
        <label className="field">
          <span>Email</span>
          <input value={u} onChange={(e) => setU(e.target.value)} autoComplete="username" />
        </label>
        <label className="field">
          <span>Password</span>
          <input type="password" value={p} onChange={(e) => setP(e.target.value)} autoComplete="current-password" autoFocus />
        </label>
        {err && <div className="error-box">{err}</div>}
        <button className="primary" disabled={busy}>
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
      </form>
    </div>
  )
}

function Shell() {
  const { me, logout } = useAuth()
  const nav = useNavigate()
  const [q, setQ] = useState('')
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="logo">▲</span> Lakeforge
        </div>
        <nav className="nav">
          {NAV.map((g) => (
            <div key={g.group}>
              <div className="group">{g.group}</div>
              {g.items
                .filter((i) => !i.admin || me?.is_admin)
                .map((i) => (
                  <NavLink key={i.to} to={i.to} end={i.to === '/' || i.to === '/sql'} className={({ isActive }) => (isActive ? 'active' : '')}>
                    <span className="ico">{i.ico}</span>
                    {i.label}
                  </NavLink>
                ))}
            </div>
          ))}
        </nav>
        <div className="sidebar-foot">
          <div className="user">{me?.display_name || me?.user_name}</div>
          <div>{me?.user_name}</div>
          <div className="muted small">
            workspace {me?.workspace_id} · {me?.cloud} · v{me?.version}
          </div>
          <button className="link" onClick={logout} style={{ marginTop: 6, color: '#ffb199' }}>
            Sign out
          </button>
        </div>
      </aside>
      <div className="main">
        <div className="topbar">
          <form
            className="search"
            onSubmit={(e) => {
              e.preventDefault()
              if (q.trim()) nav(`/workspace?q=${encodeURIComponent(q.trim())}`)
            }}
          >
            <input placeholder="Search workspace objects, tables, jobs…" value={q} onChange={(e) => setQ(e.target.value)} />
          </form>
        </div>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/workspace" element={<Workspace />} />
          <Route path="/notebook/*" element={<NotebookPage />} />
          <Route path="/repos" element={<Repos />} />
          <Route path="/catalog/*" element={<Catalog />} />
          <Route path="/dbfs" element={<Dbfs />} />
          <Route path="/sql" element={<SqlEditor />} />
          <Route path="/sql/dashboards" element={<Dashboards />} />
          <Route path="/sql/alerts" element={<Alerts />} />
          <Route path="/sql/history" element={<QueryHistory />} />
          <Route path="/sql/warehouses" element={<Warehouses />} />
          <Route path="/workflows" element={<Workflows />} />
          <Route path="/workflows/:jobId" element={<Workflows />} />
          <Route path="/runs/:runId" element={<RunDetail />} />
          <Route path="/pipelines" element={<Pipelines />} />
          <Route path="/pipelines/:id" element={<Pipelines />} />
          <Route path="/compute" element={<Compute />} />
          <Route path="/compute/:id" element={<Compute />} />
          <Route path="/ml/experiments" element={<Experiments />} />
          <Route path="/ml/experiments/:id" element={<Experiments />} />
          <Route path="/ml/models" element={<Models />} />
          <Route path="/ml/models/:name" element={<Models />} />
          <Route path="/ml/serving" element={<Serving />} />
          <Route path="/ml/serving/:name" element={<Serving />} />
          <Route path="/secrets" element={<Secrets />} />
          <Route path="/admin" element={<Admin />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </div>
  )
}

export default function App() {
  const { me, loading } = useAuth()
  if (loading) return <Spinner label="Connecting to workspace…" />
  if (!me) return <Login />
  return <Shell />
}
