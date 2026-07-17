# DAG 按日状态对接文档

本文档面向 **后端 Manager** 团队，说明 DAG 监控页将「流程拓扑」与「某日执行状态」拆分后的接口约定。

| 项目 | 路径 |
| --- | --- |
| 前端页面 | `/dag`（`src/views/DagView.vue`） |
| 拓扑 API | `src/api/dag.ts` → `dagApi.graphs` |
| 按日状态 API | `src/api/dag.ts` → `dagApi.dayStatus` |
| Store | `src/stores/dag.ts` |
| 类型 | `src/features/dag/types.ts` → `DagGraph` / `DagDayStatusResult` |
| Mock | `mock-server/src/routes/dag.ts` |

---

## 0. 为什么拆分

| 旧做法（已废弃） | 问题 |
| --- | --- |
| `GET /api/dag/graphs?date=` 一次带拓扑+当日状态 | 拓扑与运行态耦合；切日期也要重拉坐标/边 |
| 前端对每个节点打 `data-days` 再拼日态 | N+1 请求；`graphs?date=` 实际无用 |

**目标模式（C 方案）**

```text
GET /api/dag/graphs
  → 只返回流程图：节点位置、依赖边、任务名等拓扑信息

GET /api/dag/graphs/:pipelineId/day-status?date=YYYY-MM-DD
  → 只返回该流程在指定日期下，各任务的执行状态 / 最近执行

前端：拓扑画板 + 按 task_id merge 日态 → 节点颜色与详情
```

- 切换日期 / 触发执行后刷新：只需打 **day-status**
- 编辑 / 发布后：再打 **graphs**（拓扑可能变了）+ day-status

---

## 1. 权限

与现有 DAG 监控一致：

| 权限 code | 说明 |
| --- | --- |
| `pipeline:view` | 查看拓扑与按日状态 |
| `pipeline:run` | 执行流程 / 节点（仍走 `POST /api/tasks/:taskId/run`） |

---

## 2. 接口一：流程拓扑（保持，语义收窄）

### `GET /api/dag/graphs`

**不再接受 / 不再依赖 `date` 参数。** 若收到 `date`，可忽略，勿依赖它改节点状态。

**响应 `data: DagGraph[]`**

```ts
interface DagGraph {
  id: string                 // = pipeline_id
  name: string
  description: string
  canvas: { width: number; height: number }
  nodes: DagNode[]
  edges: DagEdge[]
}

interface DagNode {
  id: string                 // = task_id
  name: string
  status: 'success' | 'running' | 'waiting' | 'failed'
  // ↑ 拓扑占位即可（如当前 schedule_status）；监控页按日着色以 day-status 为准
  x: number
  y: number
  latestExecId: string       // 可占位
  agent: string              // 执行节点 node_id，便于面板展示
  duration: string           // 可占位 "-"
}

interface DagEdge {
  from: string               // 父 task_id
  to: string                 // 子 task_id
}
```

**实现要点**

1. 只返回 **已启用** 流水线（与现网一致）
2. 节点集合 = 该 Pipeline 上的 tasks；边 = Pipeline.edges
3. **不要**为了「某一天」去扫执行表改 `status`

---

## 3. 接口二：流程按日状态（新增）

### `GET /api/dag/graphs/:pipelineId/day-status`

#### 3.1 Query

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `date` | string | **是** | 交易日，`YYYY-MM-DD` |

**示例**

```http
GET /api/dag/graphs/pipeline_market_eod/day-status?date=2026-07-13
Authorization: Bearer <token>
```

#### 3.2 响应

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "pipeline_id": "pipeline_market_eod",
    "date": "2026-07-13",
    "nodes": [
      {
        "task_id": "task_market_stock_daily",
        "status": "success",
        "exec_id": "exec_1001",
        "start_time": "2026-07-13T09:05:00",
        "end_time": "2026-07-13T09:12:30",
        "duration": "7m30s"
      },
      {
        "task_id": "task_market_index_daily",
        "status": "missing"
      }
    ]
  }
}
```

```ts
type DagDayNodeStatus =
  | 'success'
  | 'running'
  | 'waiting'
  | 'failed'
  | 'timeout'
  | 'missing'

interface DagDayNodeStatusItem {
  task_id: string
  status: DagDayNodeStatus
  exec_id?: string           // 该日最近一次（或最终态）关联执行
  start_time?: string
  end_time?: string
  duration?: string          // 可读字符串即可，如 "12s" / "3m20s"
}

interface DagDayStatusResult {
  pipeline_id: string
  date: string               // 回显请求的 YYYY-MM-DD
  nodes: DagDayNodeStatusItem[]
}
```

#### 3.3 状态语义（建议与 `data-days` 对齐）

| status | 含义 |
| --- | --- |
| `success` | 该日有成功完成的执行 |
| `running` | 该日最新执行仍在跑 |
| `waiting` | 该日已触发但仍在等待调度 |
| `failed` | 该日最新终态为失败 |
| `timeout` | 该日最新终态为超时（前端画板会映射为失败色） |
| `missing` | 该日无执行记录（前端画板映射为等待色，「无执行」） |

前端画板映射：

| 接口 status | 节点展示 |
| --- | --- |
| success | 成功 |
| running | 运行中 |
| waiting / missing | 等待 |
| failed / timeout | 失败 |

#### 3.4 错误约定

| HTTP | 场景 |
| --- | --- |
| 400 | 缺少 `date` 或格式非法 |
| 404 | `pipelineId` 不存在或未启用 |
| 401/403 | 鉴权 / 权限不足 |

#### 3.5 后端实现建议

1. 校验流水线存在且启用；取出 `pipeline.nodes[].task_id` 列表  
2. **按任务集合 + 单日** 查执行记录（不要对每个 task 再暴露成 N 次 HTTP）  
3. 判定「属于该日」的规则建议与 `GET /api/tasks/:taskId/data-days` 一致，例如：  
   - 用执行参数里的 `trade_date`（`YYYYMMDD` / `YYYY-MM-DD`）优先  
   - 或用 `start_time` / `end_time` 落在该自然日  
4. 每个 `task_id` **必须返回 1 条**（即使 `missing`），数组顺序建议与流程节点顺序一致  
5. 可复用 data-days 内部逻辑，但响应应裁成「单日 + 流程内节点」

---

## 4. 与其它接口的边界

| 接口 | 用途 | 本方案是否替代 |
| --- | --- | --- |
| `GET /api/dag/graphs` | 拓扑 | 否，继续用，但去掉 date |
| `GET /api/tasks/:id/data-days` | 任务详情「近 30 日」 | **不应用 N 次拼 DAG 日态** |
| `POST /api/tasks/:id/run` | 触发执行 | 不变；执行流程 = 打入口节点 run |
| `GET /api/executions` | 执行历史列表 | 不变 |

执行流程 / 执行下游成功后，前端只会 **刷新 day-status**（不重拉拓扑）。

---

## 5. 前端调用时序

```text
进入 DAG 监控页
  1. GET /api/dag/graphs
  2. GET /api/pipelines（编辑用摘要，可并存）
  3. GET /api/tasks
  4. GET /api/dag/graphs/:id/day-status?date=<当前日期选择器>

切换流程 / 切换日期
  → 只打 day-status

保存并发布 / 删除流程
  → 重拉 graphs + day-status

触发执行（流程 / 下游 / 立即执行）
  → POST /api/tasks/:taskId/run
  → 再打 day-status
```

---

## 6. 联调检查清单

- [ ] `GET /api/dag/graphs` 忽略 `date`，返回稳定拓扑  
- [ ] `GET .../day-status?date=` 缺 date 返回 400  
- [ ] 不存在的 pipelineId 返回 404  
- [ ] `nodes` 覆盖流程内全部 task_id，无记录为 `missing`  
- [ ] status 枚举与上表一致（尤其 `timeout` / `missing`）  
- [ ] 同一流程切两个日期，状态与执行记录一致  
- [ ] 触发 run 后再次 day-status，对应节点可变为 `running` / `success`

---

## 7. Mock 对照

Mock 已按本约定实现：

- `GET /api/dag/graphs` → 纯拓扑  
- `GET /api/dag/graphs/:pipelineId/day-status?date=` → 用各任务 `data-days` 取单日合成  

联调前可用 `npm run dev:mock` 对照前端行为。
