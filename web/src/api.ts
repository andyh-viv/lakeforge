// Thin REST client for the Lakeforge control plane (Databricks-compatible API).

const TOKEN_KEY = 'lakeforge.token'

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}
export function setToken(t: string | null) {
  if (t) localStorage.setItem(TOKEN_KEY, t)
  else localStorage.removeItem(TOKEN_KEY)
}

export class ApiError extends Error {
  status: number
  code: string
  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

type Json = Record<string, unknown>

async function request<T>(method: string, path: string, body?: unknown, raw = false): Promise<T> {
  const headers: Record<string, string> = {}
  const tok = getToken()
  if (tok) headers['Authorization'] = `Bearer ${tok}`
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  const res = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) })
  if (res.status === 401 && !path.endsWith('/lakeforge/login')) {
    setToken(null)
    window.dispatchEvent(new Event('lakeforge:logout'))
  }
  if (!res.ok) {
    let code = `HTTP_${res.status}`
    let message = res.statusText
    try {
      const j = (await res.json()) as Json
      code = String(j.error_code ?? code)
      message = String(j.message ?? j.error ?? message)
    } catch {
      /* non-json error body */
    }
    throw new ApiError(res.status, code, message)
  }
  if (raw) return (await res.text()) as unknown as T
  const text = await res.text()
  return (text ? JSON.parse(text) : {}) as T
}

export const api = {
  get: <T = Json>(path: string) => request<T>('GET', path),
  post: <T = Json>(path: string, body?: unknown) => request<T>('POST', path, body ?? {}),
  put: <T = Json>(path: string, body?: unknown) => request<T>('PUT', path, body ?? {}),
  patch: <T = Json>(path: string, body?: unknown) => request<T>('PATCH', path, body ?? {}),
  delete: <T = Json>(path: string, body?: unknown) => request<T>('DELETE', path, body),
  text: (path: string) => request<string>('GET', path, undefined, true),
}

export function qs(params: Record<string, string | number | boolean | undefined | null>): string {
  const p = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== '') p.set(k, String(v))
  const s = p.toString()
  return s ? `?${s}` : ''
}

// ---------------------------------------------------------------- types

export interface Me {
  user_id: string
  user_name: string
  display_name: string
  is_admin: boolean
  groups: string[]
  workspace_id: string
  cloud: string
  version: string
}

export interface WorkspaceObject {
  object_id: number
  object_type: 'NOTEBOOK' | 'DIRECTORY' | 'FILE' | 'REPO' | 'LIBRARY'
  path: string
  language?: string
  created_at?: number
  modified_at?: number
}

export interface Cell {
  id: string
  language: string
  source: string
  outputs: KernelEvent[]
  collapsed?: boolean
  title?: string | null
}

export interface Notebook {
  default_language: string
  cells: Cell[]
  widgets: Record<string, unknown>
}

export type KernelEvent =
  | { type: 'stdout'; text: string }
  | { type: 'stderr'; text: string }
  | { type: 'display'; mime: string; data: unknown }
  | { type: 'table'; columns: string[]; rows: unknown[][]; truncated: boolean }
  | { type: 'result'; text: string }
  | { type: 'error'; ename: string; evalue: string; traceback: string[] }
  | { type: 'exit'; value: string | null }
  | { type: 'done'; status: string }

export interface Cluster {
  cluster_id: string
  cluster_name: string
  state: string
  state_message?: string
  num_workers?: number
  autoscale?: { min_workers: number; max_workers: number } | null
  spark_version?: string
  node_type_id?: string
  driver_node_type_id?: string
  autotermination_minutes?: number
  creator_user_name?: string
  cluster_cores?: number
  cluster_memory_mb?: number
  start_time?: number
  data_security_mode?: string
}

export interface Warehouse {
  id: string
  name: string
  state: string
  cluster_size: string
  cluster_id?: string
  auto_stop_mins: number
  creator_name?: string
  jdbc_url?: string
  health?: { status: string; message: string }
  num_clusters?: number
}

export interface StatementResponse {
  statement_id: string
  status: { state: string; error?: { message: string; error_code?: string } }
  manifest?: {
    schema: { column_count: number; columns: { name: string; type_text: string; type_name: string; position: number }[] }
    total_row_count: number
    truncated?: boolean
  }
  result?: { data_array?: (string | null)[][]; row_count: number; chunk_index?: number; next_chunk_index?: number | null }
}

export interface Job {
  job_id: number
  created_time: number
  creator_user_name: string
  settings: JobSettings
}

export interface JobSettings {
  name: string
  format?: string
  max_concurrent_runs?: number
  tasks?: JobTask[]
  schedule?: { quartz_cron_expression: string; timezone_id: string; pause_status?: string } | null
  parameters?: { name: string; default: string }[]
  tags?: Record<string, string>
  timeout_seconds?: number
  email_notifications?: Record<string, string[]>
}

export interface JobTask {
  task_key: string
  depends_on?: { task_key: string }[]
  existing_cluster_id?: string
  notebook_task?: { notebook_path: string; base_parameters?: Record<string, string> }
  sql_task?: { warehouse_id?: string; query?: { query_id?: string; query_text?: string } }
  python_wheel_task?: Record<string, unknown>
  spark_python_task?: { python_file: string; parameters?: string[] }
  pipeline_task?: { pipeline_id: string }
  run_job_task?: { job_id: number }
  condition_task?: Record<string, unknown>
  timeout_seconds?: number
  max_retries?: number
}

export interface Run {
  run_id: number
  job_id: number
  run_name: string
  run_type?: string
  start_time: number
  end_time?: number
  state: { life_cycle_state: string; result_state?: string; state_message?: string }
  tasks?: RunTask[]
  creator_user_name?: string
  trigger?: string
  run_page_url?: string
  execution_duration?: number
  job_parameters?: { name: string; default?: unknown; value?: unknown }[]
  overriding_parameters?: unknown
}

export interface RunTask {
  task_key: string
  run_id: number
  state: { life_cycle_state: string; result_state?: string; state_message?: string }
  start_time?: number
  end_time?: number
  notebook_task?: { notebook_path: string }
  sql_task?: Record<string, unknown>
  depends_on?: { task_key: string }[]
}

export interface Pipeline {
  pipeline_id: string
  name: string
  state: string
  health?: string
  creator_user_name?: string
  latest_updates?: { update_id: string; state: string; creation_time: number }[]
  cluster_id?: string | null
}

export interface PipelineSpec {
  id?: string
  name: string
  target?: string
  catalog?: string
  continuous?: boolean
  development?: boolean
  libraries?: { notebook?: { path: string }; file?: { path: string } }[]
  configuration?: Record<string, string>
  storage?: string
}

export interface PipelineEvent {
  id: string
  timestamp: string
  level: string
  event_type: string
  message: string
  details?: unknown
}

export interface Experiment {
  experiment_id: string
  name: string
  artifact_location: string
  lifecycle_stage: string
  creation_time: number
  last_update_time: number
  tags: { key: string; value: string }[]
}

export interface MlRun {
  info: {
    run_id: string
    run_name?: string
    experiment_id: string
    status: string
    start_time: number
    end_time?: number
    user_id?: string
    artifact_uri?: string
  }
  data: { metrics: { key: string; value: number; step?: number }[]; params: { key: string; value: string }[]; tags: { key: string; value: string }[] }
}

export interface RegisteredModel {
  name: string
  creation_timestamp: number
  last_updated_timestamp: number
  description?: string | null
  user_id?: string
  latest_versions?: ModelVersion[]
  aliases?: { alias: string; version: string }[]
  tags?: { key: string; value: string }[]
}

export interface ModelVersion {
  name: string
  version: string
  creation_timestamp: number
  current_stage: string
  status?: string
  run_id?: string
  source?: string
  description?: string | null
  aliases?: string[]
}

export interface ServingEndpoint {
  name: string
  id?: string
  creator?: string
  creation_timestamp?: number
  last_updated_timestamp?: number
  state?: { ready: string; config_update: string }
  config?: {
    served_entities?: ServedEntity[]
    served_models?: ServedEntity[]
    traffic_config?: { routes: { served_model_name: string; traffic_percentage: number }[] }
  }
  task?: string
  tags?: { key: string; value: string }[]
}

export interface ServedEntity {
  name: string
  entity_name?: string
  entity_version?: string
  model_name?: string
  model_version?: string
  workload_size?: string
  scale_to_zero_enabled?: boolean
  external_model?: { name: string; provider: string; task: string }
  state?: { deployment: string; deployment_state_message: string }
}

export interface Repo {
  id: number
  url: string
  provider: string
  path: string
  branch: string
  head_commit_id?: string
}

export interface ScimUser {
  id: string
  userName: string
  displayName?: string
  active: boolean
  emails?: { value: string; primary?: boolean }[]
  groups?: { display: string; value: string }[]
  entitlements?: { value: string }[]
}

export interface ScimGroup {
  id: string
  displayName: string
  members?: { display: string; value: string }[]
  entitlements?: { value: string }[]
}

export interface TokenInfo {
  token_id: string
  comment?: string
  creation_time: number
  expiry_time: number
  created_by_username?: string
}

export interface SecretScope {
  name: string
  backend_type: string
}

export interface CatalogItem {
  name: string
  full_name: string
  comment?: string
  owner?: string
  created_at?: number
  updated_at?: number
  catalog_type?: string
  table_type?: string
  data_source_format?: string
  storage_location?: string
  columns?: { name: string; type_text: string; type_name: string; nullable?: boolean; position: number; comment?: string }[]
  properties?: Record<string, string>
}

export interface SqlQuery {
  id: string
  display_name: string
  query_text: string
  warehouse_id?: string
  description?: string
  owner_user_name?: string
  create_time?: string
  update_time?: string
  tags?: string[]
}

export interface QueryHistoryEntry {
  query_id: string
  query_text: string
  status: string
  duration: number
  query_start_time_ms: number
  query_end_time_ms?: number
  executed_as_user_name?: string
  error_message?: string | null
  rows_produced?: number
  statement_type?: string
  endpoint_id?: string
  metrics?: { rows_produced_count?: number; total_time_ms?: number }
}

export interface DbfsFile {
  path: string
  is_dir: boolean
  file_size: number
  modification_time: number
}

export interface Dashboard {
  dashboard_id: string
  display_name: string
  path?: string
  create_time?: string
  update_time?: string
  warehouse_id?: string
  serialized_dashboard?: string
  lifecycle_state?: string
}

export interface Alert {
  id: string
  display_name: string
  query_id: string
  state: string
  condition?: unknown
  create_time?: string
  update_time?: string
  trigger_time?: string
}

export interface LakeforgeInfo {
  name: string
  version: string
  cloud: string
  engine: { name: string; sql: string; table_format: string }
  features: string[]
  public_url: string
  uptime_secs: number
  workspace_id: string
}

// ---------------------------------------------------------------- helpers

export function fmtTime(ms?: number | string | null): string {
  if (!ms) return '—'
  const d = typeof ms === 'string' ? new Date(ms) : new Date(ms)
  if (Number.isNaN(d.getTime())) return String(ms)
  return d.toLocaleString()
}

export function fmtDuration(ms?: number | null): string {
  if (ms === undefined || ms === null) return '—'
  if (ms < 1000) return `${ms} ms`
  const s = Math.round(ms / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${s % 60}s`
  const h = Math.floor(m / 60)
  return `${h}h ${m % 60}m`
}

export function fmtBytes(n?: number): string {
  if (n === undefined) return '—'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`
  return `${(n / 1024 ** 3).toFixed(2)} GB`
}

export function basename(p: string): string {
  const parts = p.split('/').filter(Boolean)
  return parts[parts.length - 1] ?? '/'
}

export function dirname(p: string): string {
  const parts = p.split('/').filter(Boolean)
  parts.pop()
  return '/' + parts.join('/')
}
