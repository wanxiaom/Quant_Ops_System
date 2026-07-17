export type AgentStatus = 'online' | 'offline' | 'busy' | 'error'
export type UserRole = 'your_username' | 'operator' | 'researcher'

export interface Agent {
  node_id: string
  hostname?: string
  ip?: string
  os_type?: 'linux' | 'windows' | string
  status: AgentStatus
  last_heartbeat?: string
  cpu_load?: number
  mem_usage?: number
  version?: string
  concurrency_used?: number
  concurrency_limit?: number
  running_tasks?: number
  tags?: string[]
  created_at?: string
}

export interface AgentCreatePayload {
  node_id: string
  hostname: string
  ip: string
  os_type: 'linux' | 'windows'
  concurrency_limit?: number
  tags?: string[]
}

export interface Task {
  task_id: string
  name: string
  description?: string
  node_type: string
  script_path: string
  cron_expr: string
  timeout_sec: number
  enabled: 0 | 1
  target_node_id?: string
  task_type?: 'cron' | 'dag' | 'manual'
  target_os?: 'linux' | 'windows'
  target_node_tag?: string
  schedule_status?: 'waiting' | 'running' | 'success' | 'failed' | 'timeout'
  trigger_mode?: 'cron' | 'dependency' | 'manual'
  created_at?: string
  last_run_at?: string
  retry_count?: number
  retry_interval_sec?: number
  concurrency_limit?: number
  dag_parents?: string[]
  env_vars?: Array<{ key: string; value: string }>
  log_level?: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'
  alert_on_failure?: boolean
}

export interface CreateTaskPayload {
  task_id?: string
  name?: string
  description?: string
  task_type?: Task['task_type']
  node_type?: string
  script_path?: string
  cron_expr?: string
  timeout_sec?: number
  enabled?: 0 | 1
  target_node_id?: string
  target_os?: Task['target_os']
  target_node_tag?: string
  schedule_status?: Task['schedule_status']
  trigger_mode?: Task['trigger_mode']
  last_run_at?: string
  retry_count?: number
  retry_interval_sec?: number
  concurrency_limit?: number
  dag_parents?: string[]
  env_vars?: Task['env_vars']
  log_level?: Task['log_level']
  alert_on_failure?: boolean
}

export interface TaskExecution {
  exec_id: string
  task_id: string
  node_id?: string
  status: number
  exit_code: number | null
  start_time: string
  end_time: string | null
  log_path?: string
  params?: string
  trigger_mode?: string
  stdout?: string
  stderr?: string
}

export interface TriggerTaskResponse {
  code: number
  message: string
  exec_id: string
  execution?: TaskExecution
}

export interface AuthUser {
  user_id: string
  name: string
  email: string
  role: UserRole
  permissions: string[]
}

export interface LoginPayload {
  username: string
  password: string
}

export interface LoginResponse {
  token: string
  user: AuthUser
}

export interface DashboardMetrics {
  onlineAgents: number
  totalAgents: number
  todayTasks: number
  todaySuccessRate: number
  runningDags: number
  completedDags: number
  avgDuration: number
  agentHealth: {
    online: number
    busy: number
    offline: number
    error: number
  }
  platformDist: Array<{ os: string; count: number }>
  taskTrend: Array<{ date: string; total: number; success: number }>
  recentErrors: Array<{
    task_id: string
    task_name: string
    time: string
    status: string
  }>
}

export interface DagGraph {
  id: string
  name: string
  description: string
  canvas: { width: number; height: number }
  nodes: Array<Record<string, unknown>>
  edges: Array<{ from: string; to: string }>
}

export type LogLevel = 'INFO' | 'WARN' | 'ERROR'
export type LogStatus = 'waiting' | 'running' | 'success' | 'failed' | 'timeout'

export interface LogDetail {
  log_id: string
  exec_id: string
  task_id: string
  task_name: string
  agent_id: string
  level: LogLevel
  time: string
  status: LogStatus
  stdout: string
  stderr: string
}

export interface LogEntry {
  log_id: string
  exec_id: string
  task_id: string
  task_name: string
  agent_id: string
  level: LogLevel
  time: string
  status: LogStatus
}

export interface LogSearchParams {
  keyword?: string
  task_name?: string
  agent_id?: string
  level?: LogLevel
  status?: LogStatus
  date_range?: string[]
}

export interface ManagedUser {
  user_id: string
  username: string
  name: string
  email: string
  role: UserRole
  status: 'active' | 'disabled'
  last_login_at?: string
  created_at?: string
}

export interface SettingsUser {
  user_id: string
  name: string
  email: string
  role: UserRole
  status: 'active' | 'disabled'
  last_login_at: string
}

export interface RolePermission {
  role: UserRole
  permissions: string[]
}

export interface AlertRule {
  rule_id: string
  name: string
  enabled: boolean
  event: 'task_failed' | 'task_timeout' | 'agent_offline'
  severity: 'info' | 'warning' | 'critical'
  channels: string[]
}

export interface NotificationChannel {
  channel_id: string
  name: string
  type: 'webhook' | 'email'
  enabled: boolean
  target: string
}

export interface SystemParameters {
  global_concurrency_limit: number
  heartbeat_timeout_sec: number
  log_retention_days: number
  max_retry_count: number
}

export interface SettingsSnapshot {
  alertRules: AlertRule[]
  notificationChannels: NotificationChannel[]
  systemParameters: SystemParameters
}

export interface PipelineCanvasSize {
  width: number
  height: number
}

export interface PipelineNode {
  task_id: string
  x: number
  y: number
}

export interface PipelineEdge {
  from: string
  to: string
}

export interface PipelineSchedule {
  enabled: boolean
  cron_expr: string
}

export interface Pipeline {
  pipeline_id: string
  name: string
  description: string
  enabled: boolean
  canvas: PipelineCanvasSize
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  entry_task_ids?: string[]
  schedule?: PipelineSchedule
  created_at?: string
  updated_at?: string
}

export interface PipelineSummary {
  pipeline_id: string
  name: string
  description: string
  enabled: boolean
  task_count: number
  edge_count: number
  updated_at?: string
}

export interface SavePipelinePayload {
  name: string
  description?: string
  enabled?: boolean
  canvas: PipelineCanvasSize
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  entry_task_ids?: string[]
  schedule?: PipelineSchedule
}

export interface PublishPipelinePayload {
  previous_node_ids?: string[]
}

export interface PublishPipelineResult {
  pipeline_id: string
  updated_tasks: string[]
  cleared_tasks: string[]
  skipped_tasks: string[]
}

export type DagStatus = 'success' | 'running' | 'waiting' | 'failed'

export interface DagNodeView {
  id: string
  name: string
  status: DagStatus
  x: number
  y: number
  latestExecId: string
  agent: string
  duration: string
}

export interface DagEdgeView {
  from: string
  to: string
}

export interface DagGraphView {
  id: string
  name: string
  description: string
  canvas: PipelineCanvasSize
  nodes: DagNodeView[]
  edges: DagEdgeView[]
}
