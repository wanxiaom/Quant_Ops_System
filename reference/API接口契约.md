# API 接口契约

本文档描述 **Quant Ops 前端当前依赖的 REST API 契约**，与 `mock-server/` 实现及 `src/types/domain.ts` 类型定义对齐，供后端 Manager 团队实现参考。

> RBAC 权限模型详见 [用户与权限接口.md](./用户与权限接口.md)。

---

## 1. 概述

| 项目 | 说明 |
|------|------|
| 基础路径 | `/api` |
| 本地 Mock | `http://127.0.0.1:3001`（`npm run mock`） |
| 真实后端（规划） | `http://127.0.0.1:8080` |
| 前端代理 | Vite 将 `/api/*` 转发至 `VITE_BACKEND_URL` |
| 类型定义 | `src/types/domain.ts`、`src/features/dag/pipelineTypes.ts`、`src/features/dag/types.ts` |
| Mock 实现 | `mock-server/src/routes/`、`mock-server/src/store/` |

### 1.1 接口一览

| 模块 | 方法 | 路径 | 鉴权 | 写权限 |
|------|------|------|:----:|--------|
| 健康检查 | GET | `/health` | 否 | — |
| 登录 | POST | `/api/auth/login` | 否 | — |
| 当前用户 | GET | `/api/auth/me` | 是 | — |
| 当前权限 | GET | `/api/auth/permissions` | 是 | — |
| 登出 | POST | `/api/auth/logout` | 是 | — |
| 任务列表 | GET | `/api/tasks` | 是 | — |
| 创建任务 | POST | `/api/tasks` | 是 | `task:edit` |
| 任务详情 | GET | `/api/tasks/:taskId` | 是 | — |
| 更新任务 | PUT | `/api/tasks/:taskId` | 是 | `task:edit` |
| 删除任务 | DELETE | `/api/tasks/:taskId` | 是 | `task:edit` |
| 手动执行 | POST | `/api/tasks/:taskId/run` | 是 | `task:run` |
| 增量触发 | POST | `/api/tasks/:taskId/incr` | 是 | `task:run` |
| 暂停任务 | POST | `/api/tasks/:taskId/pause` | 是 | `task:edit` |
| 恢复任务 | POST | `/api/tasks/:taskId/resume` | 是 | `task:edit` |
| 执行历史 | GET | `/api/executions` | 是 | — |
| 执行历史（按任务） | GET | `/api/executions?task_id=:taskId` | 是 | — |
| Agent 列表 | GET | `/api/agents` | 是 | — |
| 创建 Agent | POST | `/api/agents` | 是 | `agent:manage` |
| Agent 下线 | POST | `/api/agents/:nodeId/offline` | 是 | `agent:manage` |
| 仪表盘指标 | GET | `/api/dashboard/metrics` | 是 | — |
| DAG 监控图 | GET | `/api/dag/graphs` | 是 | — |
| 流水线列表 | GET | `/api/pipelines` | 是 | — |
| 创建流水线 | POST | `/api/pipelines` | 是 | `pipeline:edit` |
| 流水线详情 | GET | `/api/pipelines/:pipelineId` | 是 | — |
| 更新流水线 | PUT | `/api/pipelines/:pipelineId` | 是 | `pipeline:edit` |
| 删除流水线 | DELETE | `/api/pipelines/:pipelineId` | 是 | `pipeline:edit` |
| 发布流水线 | POST | `/api/pipelines/:pipelineId/publish` | 是 | `pipeline:edit` |
| 搜索日志 | GET | `/api/logs/search` | 是 | — |
| 日志详情 | GET | `/api/logs/:logId` | 是 | — |
| 下载日志 | GET | `/api/logs/:logId/download` | 是 | — |
| 用户列表 | GET | `/api/users` | 是 | `user:manage` |
| 角色权限矩阵 | GET | `/api/users/role-permissions` | 是 | `user:manage` |
| 用户详情 | GET | `/api/users/:userId` | 是 | `user:manage` |
| 创建用户 | POST | `/api/users` | 是 | `user:manage` |
| 更新用户 | PUT | `/api/users/:userId` | 是 | `user:manage` |
| 启用/禁用用户 | POST | `/api/users/:userId/toggle` | 是 | `user:manage` |
| 删除用户 | DELETE | `/api/users/:userId` | 是 | `user:manage` |
| 设置总览 | GET | `/api/settings/overview` | 是 | — |
| 保存告警规则 | PUT | `/api/settings/alert-rules/:ruleId` | 是 | `alert:manage` |
| 切换告警规则 | POST | `/api/settings/alert-rules/:ruleId/toggle` | 是 | `alert:manage` |
| 保存通知渠道 | PUT | `/api/settings/notification-channels/:channelId` | 是 | `alert:manage` |
| 测试通知渠道 | POST | `/api/settings/notification-channels/:channelId/test` | 是 | `alert:manage` |
| 保存系统参数 | PUT | `/api/settings/system-parameters` | 是 | `system:manage` |
| 恢复默认参数 | POST | `/api/settings/system-parameters/reset` | 是 | `system:manage` |

---

## 2. 通用约定

### 2.1 统一响应包装

除 **任务触发** 与 **日志下载** 外，JSON 接口均使用：

```ts
interface ApiResponse<T> {
  code: number    // 0 表示成功；非 0 表示业务错误
  message: string
  data: T
}
```

**成功示例**

```json
{
  "code": 0,
  "message": "ok",
  "data": []
}
```

**失败示例**

```json
{
  "code": 404,
  "message": "Task not found",
  "data": null
}
```

HTTP 状态码与 `code` 建议一致：401 未登录、403 权限不足、404 资源不存在、400 参数错误。

### 2.2 认证

除 `POST /api/auth/login` 与 `GET /health` 外，所有 `/api/*` 接口需携带：

```http
Authorization: Bearer <token>
```

登录成功后前端将 token 存入 `localStorage`（key: `ops_system_auth_token=${ACCESS_TOKEN}

开发环境可在 `.env.development` 配置 `VITE_API_TOKEN=${ACCESS_TOKEN} 作为兜底 token=${ACCESS_TOKEN} 视为 your_username）。

### 2.3 权限校验

- 读接口（GET）：已登录即可访问（细粒度读权限可按需追加）。
- 写接口（POST/PUT/DELETE）：必须在服务端校验 permission code，不足返回 403。
- 权限点定义见 `src/features/auth/permissions.ts`，与 mock-server 镜像一致。

### 2.4 特殊响应格式

| 接口 | 响应格式 |
|------|----------|
| `POST /api/tasks/:id/run`、`POST /api/tasks/:id/incr` | **不套** `ApiResponse`，直接返回 `TriggerTaskResponse`（见 §3.2） |
| `GET /api/logs/:logId/download` | `Content-Type: text/plain`，响应体为日志文本，非 JSON |

---

## 3. 认证接口

### POST `/api/auth/login`

登录，无需 token=${ACCESS_TOKEN}

**请求体**

```json
{
  "username": "your_username",
  "password": "admin123"
}
```

**响应 `data`**

```ts
interface LoginResponse {
  token=${ACCESS_TOKEN}
  user: AuthUser
}

interface AuthUser {
  user_id: string
  name: string
  email: string
  role: 'your_username' | 'operator' | 'researcher'
  permissions: string[]   // 根据 role 计算，如 ["dashboard:view", "task:view", ...]
}
```

**Mock 测试账号**

| 用户名 | 密码 | 角色 |
|--------|------|------|
| `your_username` | `admin123` | 管理员 |
| `operator` | `ops123` | 运维 |
| `researcher` | `research123` | 研究员 |

### GET `/api/auth/me`

获取当前登录用户（含 `permissions`）。响应 `data` 为 `AuthUser`。

### GET `/api/auth/permissions`

获取当前用户角色与权限列表。

**响应 `data`**

```json
{
  "role": "your_username",
  "permissions": ["dashboard:view", "task:view", "..."]
}
```

### POST `/api/auth/logout`

登出。Mock 为无状态，前端清除本地 token 即可。响应 `data` 为 `null`。

---

## 4. 任务接口

### GET `/api/tasks`

返回任务列表。响应 `data: Task[]`。

### GET `/api/tasks/:taskId`

返回单个任务。不存在时 404。

### POST `/api/tasks`

创建任务。请求体为 `CreateTaskPayload`（字段均可选，服务端需补默认值并生成 `task_id`）。

### PUT `/api/tasks/:taskId`

全量更新任务，请求体同 `CreateTaskPayload`。

### DELETE `/api/tasks/:taskId`

删除任务及关联执行记录（mock 行为）。

### POST `/api/tasks/:taskId/run`

手动触发任务。**需要 `task:run`**。

**响应（非 ApiResponse 包装）**

```ts
interface TriggerTaskResponse {
  code: number
  message: string
  exec_id: string
  execution?: TaskExecution
}
```

### POST `/api/tasks/:taskId/incr`

增量/依赖触发。可携带 JSON body（如 `{ "date": "2026-06-30" }`），mock 当前忽略 body。响应格式同 `run`。

### POST `/api/tasks/:taskId/pause`

暂停任务（`enabled` 置 0）。**需要 `task:edit`**。

### POST `/api/tasks/:taskId/resume`

恢复任务（`enabled` 置 1）。**需要 `task:edit`**。

### GET `/api/executions`

- 无 query：返回全部执行历史 `TaskExecution[]`
- `?task_id=xxx`：返回指定任务的执行历史

### 任务核心类型

```ts
interface Task {
  task_id: string
  name: string
  description?: string
  node_type: string              // 如 python、shell
  script_path: string
  cron_expr: string
  timeout_sec: number
  enabled: 0 | 1
  target_node_id: string         // 执行 Agent 的 node_id
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
  dag_parents?: string[]         // 父任务 task_id 列表
  env_vars?: Array<{ key: string; value: string }>
  log_level?: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'
  alert_on_failure?: boolean
}

interface TaskExecution {
  exec_id: string
  task_id: string
  node_id: string
  status: 2 | 0 | 1 | -1 | -2    // 见下方状态码说明
  exit_code: number | null
  start_time: string | null
  end_time: string | null
  log_path: string
  params: string                 // JSON 字符串
  trigger_mode?: 'cron' | 'dependency' | 'manual'
  stdout?: string
  stderr?: string
}
```

### 执行状态码（`TaskExecution.status`）

| status | 含义 | 典型 exit_code |
|--------|------|----------------|
| `2` | 等待派发 | `null` |
| `0` | 运行中 | `null` |
| `1` | 成功 | `0` |
| `-1` | 失败 | 非 0 |
| `-2` | 超时 | 如 `124` |

`Task.schedule_status` 为任务维度的调度展示状态；`TaskExecution.status` 为单次执行实例状态，二者可联合展示。

---

## 5. Agent 接口

### GET `/api/agents`

返回 Agent 列表。响应 `data: Agent[]`。

### POST `/api/agents`

创建 Agent。**需要 `agent:manage`**。

**请求体**

```ts
interface AgentCreatePayload {
  node_id: string
  hostname: string
  ip: string
  os_type: 'linux' | 'windows'
  concurrency_limit?: number
  tags?: string[]
}
```

### POST `/api/agents/:nodeId/offline`

将 Agent 标记为离线。**需要 `agent:manage`**。

### Agent 类型

```ts
type AgentStatus = 'online' | 'offline' | 'busy' | 'error'

interface Agent {
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
```

> 当前无 Agent 注册审批流程，Agent 由管理员直接创建或 Agent 自行注册后立即可用（由后端自行决定）。

---

## 6. 仪表盘接口

### GET `/api/dashboard/metrics`

**响应 `data`**

```ts
interface DashboardMetrics {
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
    status: 'failed' | 'timeout' | 'error'
  }>
}
```

---

## 7. DAG 与流水线接口

### 数据分层

| 层 | 职责 | 持久化 |
|----|------|--------|
| **Task** | 任务执行定义（脚本、节点、超时等） | `tasks` 表 |
| **Pipeline** | 流水线拓扑（选哪些任务、依赖关系、画布布局） | `pipelines` 表 |
| **DagGraph** | 监控页展示（Pipeline + Task 运行时状态合成） | 不持久化，接口实时合成 |

```
Pipeline.nodes[].task_id  →  引用 Task.task_id
Pipeline.edges            →  发布时投影为子任务的 Task.dag_parents
Pipeline + Task 运行时    →  合成 DagGraph（监控页）
```

### GET `/api/dag/graphs`

DAG 监控页拓扑列表。响应 `data: DagGraph[]`。

```ts
interface DagGraph {
  id: string              // pipeline_id
  name: string
  description: string
  canvas: { width: number; height: number }
  nodes: DagNode[]
  edges: DagEdge[]
}

interface DagNode {
  id: string              // task_id
  name: string
  status: 'success' | 'running' | 'waiting' | 'failed'
  x: number
  y: number
  latestExecId: string
  agent: string
  duration: string
}

interface DagEdge {
  from: string            // 父 task_id
  to: string              // 子 task_id
}
```

### GET `/api/pipelines`

流水线摘要列表。响应 `data: PipelineSummary[]`。

```ts
interface PipelineSummary {
  pipeline_id: string
  name: string
  description: string
  enabled: boolean
  task_count: number
  edge_count: number
  updated_at?: string
}
```

### POST `/api/pipelines`

创建流水线。**需要 `pipeline:edit`**。请求体 `SavePipelinePayload`，响应 `data: Pipeline`。

### GET `/api/pipelines/:pipelineId`

获取流水线完整定义。响应 `data: Pipeline`。

### PUT `/api/pipelines/:pipelineId`

更新流水线（含节点坐标与依赖边）。**需要 `pipeline:edit`**。

### DELETE `/api/pipelines/:pipelineId`

删除流水线。**需要 `pipeline:edit`**。

### POST `/api/pipelines/:pipelineId/publish`

将流水线 `edges` 投影到相关任务的 `dag_parents`。**需要 `pipeline:edit`**。

**请求体（可选）**

```ts
interface PublishPipelinePayload {
  previous_node_ids?: string[]   // 发布前流程中的节点，用于清除被移除任务的 dag_parents
}
```

**响应 `data`**

```ts
interface PublishPipelineResult {
  pipeline_id: string
  updated_tasks: string[]
  cleared_tasks: string[]
  skipped_tasks: string[]
}
```

### Pipeline 类型

```ts
interface PipelineNode {
  task_id: string
  x: number
  y: number
}

interface PipelineEdge {
  from: string    // 父 task_id
  to: string      // 子 task_id
}

interface Pipeline {
  pipeline_id: string
  name: string
  description: string
  enabled: boolean
  canvas: { width: number; height: number }
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  entry_task_ids?: string[]
  schedule?: { enabled: boolean; cron_expr: string }
  created_at?: string
  updated_at?: string
}

interface SavePipelinePayload {
  name: string
  description?: string
  enabled?: boolean
  canvas: { width: number; height: number }
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  entry_task_ids?: string[]
  schedule?: { enabled: boolean; cron_expr: string }
}
```

**发布推荐流程**

1. 校验 `nodes` 引用的 `task_id` 均存在
2. 校验 `edges` 无环、无重复边
3. 持久化 `Pipeline`
4. 调用 `publish` 将依赖写回 `Task.dag_parents`

---

## 8. 日志接口

### GET `/api/logs/search`

搜索日志列表。Query 参数：

| 参数 | 类型 | 说明 |
|------|------|------|
| `keyword` | string | 关键词 |
| `task_name` | string | 任务名 |
| `agent_id` | string | Agent ID |
| `level` | `INFO` \| `WARN` \| `ERROR` | 日志级别 |
| `status` | `waiting` \| `running` \| `success` \| `failed` \| `timeout` | 执行状态 |
| `date_range` | string[] | 日期范围，可重复传参 |

响应 `data: LogEntry[]`。

```ts
interface LogEntry {
  log_id: string
  exec_id: string
  task_id: string
  task_name: string
  agent_id: string
  level: 'INFO' | 'WARN' | 'ERROR'
  time: string
  status: 'waiting' | 'running' | 'success' | 'failed' | 'timeout'
}
```

### GET `/api/logs/:logId`

日志详情。响应 `data: LogDetail`（含 `stdout`、`stderr` 全文）。

### GET `/api/logs/:logId/download`

下载日志文件。返回 **纯文本**（`text/plain`），`Content-Disposition: attachment`。

---

## 9. 用户管理接口

> RBAC 详见 [用户与权限接口.md](./用户与权限接口.md)。写操作均需 `user:manage`。

### GET `/api/users`

用户列表（不含密码）。

### GET `/api/users/role-permissions`

角色 → 权限点映射矩阵。

```ts
interface RolePermission {
  role: 'your_username' | 'operator' | 'researcher'
  permissions: string[]
}
```

### GET `/api/users/:userId`

单个用户详情。

### POST `/api/users`

创建用户。

```ts
interface CreateUserPayload {
  username: ${DB_USERNAME}
  password: ${DB_PASSWORD}
  name: string
  email: string
  role: 'your_username' | 'operator' | 'researcher'
}
```

### PUT `/api/users/:userId`

更新用户。`password` 可选，不传表示不修改密码。

```ts
interface UpdateUserPayload {
  name?: string
  email?: string
  role?: 'your_username' | 'operator' | 'researcher'
  password?: string
}
```

### POST `/api/users/:userId/toggle`

切换启用/禁用状态。响应 `data: ManagedUser`。

### DELETE `/api/users/:userId`

删除用户。

### ManagedUser 类型

```ts
interface ManagedUser {
  user_id: string
  username: ${DB_USERNAME}
  name: string
  email: string
  role: 'your_username' | 'operator' | 'researcher'
  status: 'active' | 'disabled'
  last_login_at?: string
  created_at?: string
}
```

---

## 10. 系统设置接口

### GET `/api/settings/overview`

设置总览快照。

```ts
interface SettingsSnapshot {
  alertRules: AlertRule[]
  notificationChannels: NotificationChannel[]
  systemParameters: SystemParameters
}

interface AlertRule {
  rule_id: string
  name: string
  enabled: boolean
  event: 'task_failed' | 'task_timeout' | 'agent_offline'
  severity: 'info' | 'warning' | 'critical'
  channels: string[]          // notification channel_id 列表
}

interface NotificationChannel {
  channel_id: string
  name: string
  type: 'webhook' | 'email'
  enabled: boolean
  target: string
}

interface SystemParameters {
  global_concurrency_limit: number
  heartbeat_timeout_sec: number
  log_retention_days: number
  max_retry_count: number
}
```

### PUT `/api/settings/alert-rules/:ruleId`

保存告警规则（请求体为完整 `AlertRule`）。**需要 `alert:manage`**。

### POST `/api/settings/alert-rules/:ruleId/toggle`

切换告警规则启用状态。**需要 `alert:manage`**。

### PUT `/api/settings/notification-channels/:channelId`

保存通知渠道。**需要 `alert:manage`**。

### POST `/api/settings/notification-channels/:channelId/test`

发送测试通知。**需要 `alert:manage`**。

### PUT `/api/settings/system-parameters`

保存系统参数。**需要 `system:manage`**。

### POST `/api/settings/system-parameters/reset`

恢复默认系统参数。**需要 `system:manage`**。

---

## 11. 健康检查

### GET `/health`

无需认证，用于探活。

```json
{
  "status": "ok",
  "service": "quant-ops-mock-server"
}
```

---

## 12. 联调说明

### 本地启动

```bash
npm run mock:install   # 首次
npm run dev            # 同时启动 mock-server + 前端
```

### 环境变量（`.env.development`）

```env
VITE_BACKEND_URL=http://127.0.0.1:3001
VITE_API_TOKEN=${ACCESS_TOKEN}
```

### 切换真实后端

将 `VITE_BACKEND_URL` 改为 `http://127.0.0.1:8080`，重启前端 dev server。

### 后端实现建议

1. **响应格式**：与本文档及 mock-server 保持一致，避免前端适配层大量改动。
2. **权限**：用户表仅存 `role`，登录时计算 `permissions[]`；写接口按 permission code 校验。
3. **任务触发**：`run` / `incr` 建议返回 `TriggerTaskResponse` 扁平结构（前端已按此解析）。
4. **流水线发布**：`publish` 需将 `edges` 同步到 `Task.dag_parents`，并处理节点移除时的清理。
5. **WebSocket（规划）**：实时日志与执行状态推送类型见 `WsMessage`（`src/types/domain.ts`），当前 mock 未实现。

### 相关文档

- [用户与权限接口.md](./用户与权限接口.md) — RBAC 角色、权限点、错误码
- [mock-server/README.md](../mock-server/README.md) — Mock 启动与局域网访问

---

## 变更记录

| 日期 | 说明 |
|------|------|
| 2026-07-07 | 初版：对齐 mock-server，补充认证、权限、类型与联调说明 |
| 2026-07-07 | 用户管理独立 `/api/users`；移除 Agent 审批；角色 `researcher` |
