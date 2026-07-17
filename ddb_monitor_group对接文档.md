# 数据监控分组（ddb_monitor_group）对接文档

本文档面向 **后端 Manager / 数据平台** 团队，说明监控业务分组的独立主数据表、REST API 及与现有 `ddb_monitor` 的关联改造。前端已实现分组管理弹窗与接口驱动下拉。

| 项目 | 路径 |
| --- | --- |
| 前端分组管理 | `src/features/data-monitor/components/MonitorGroupManager.vue` |
| 前端 API | `src/api/ddbMonitorGroups.ts` |
| 类型定义 | `src/types/domain.ts` → `DdbMonitorGroup*` |
| Mock 实现 | `mock-server/src/routes/ddbMonitorGroups.ts`、`mock-server/src/store/ddbMonitorGroups.ts` |

> 监控配置、快照矩阵等接口的 `group_name` 参数已统一改为 **`group_id`**，详见 [数据监控对接文档](./数据监控对接文档.md)。

---

## 1. 设计目标

| 问题 | 方案 |
| --- | --- |
| 分组由业务方维护，非固定枚举 | 独立主数据表 `ddb_monitor_group` |
| 展示名可改、查询键应稳定 | `group_id`（英文 code）+ `display_name`（展示名） |
| 概览/配置按组筛选 | 监控项、快照冗余 `group_id`，按 `group_id` 精确过滤 |
| 删除分组防误操作 | 仅当 `monitor_count = 0` 时允许物理删除 |

---

## 2. 数据模型

### 2.1 分组主数据表 `ddb_monitor_group`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `group_id` | varchar(64) PK | 是 | 稳定英文 code，创建后**不可修改**，如 `market`、`alpha_v2` |
| `display_name` | varchar(64) | 是 | 界面展示名，如「行情」「Alpha 因子」 |
| `sort_order` | int | 是 | 排序，越小越靠前；下拉与概览默认分组取启用项中最小值 |
| `enabled` | tinyint(1) | 是 | `1` 启用（出现在下拉），`0` 禁用 |
| `description` | varchar(512) | 否 | 备注 |
| `created_at` | datetime | 否 | |
| `updated_at` | datetime | 否 | |

**`group_id` 校验建议**：`^[a-z][a-z0-9_]{0,63}$`（小写字母开头，仅字母数字下划线）。

### 2.2 监控配置表 `ddb_monitor`（字段变更）

| 变更 | 说明 |
| --- | --- |
| 删除 `group_name` | 不再存自由文本或中文组名 |
| 新增 `group_id` | FK → `ddb_monitor_group.group_id` |

### 2.3 统计快照表 `ddb_monitor_snapshot`（字段变更）

| 变更 | 说明 |
| --- | --- |
| 删除 `group_name` | |
| 新增 `group_id` | 冗余，便于 `WHERE group_id = ? AND date BETWEEN ? AND ?` |

**索引建议**：`ddb_monitor(group_id, enabled)`、`ddb_monitor_snapshot(group_id, date)`。

### 2.4 历史数据迁移（一次性）

若现有库使用英文/中文混存的 `group_name`：

```sql
-- 示例：从现有 distinct group_name 灌入分组表（按实际数据调整）
INSERT INTO ddb_monitor_group (group_id, display_name, sort_order, enabled)
VALUES
  ('market', '行情', 10, 1),
  ('factor', '因子', 20, 1);

-- 监控项、快照批量更新 group_id（需根据旧 group_name 映射）
UPDATE ddb_monitor SET group_id = 'market' WHERE group_name IN ('market', '行情');
UPDATE ddb_monitor_snapshot s
  JOIN ddb_monitor m ON s.monitor_id = m.monitor_id
  SET s.group_id = m.group_id;
```

---

## 3. REST API

基础路径：`/api/ddb-monitor-groups`  
响应包装：与全局一致 `{ code, message, data }`，`code=0` 为成功。

### 3.1 接口一览

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| GET | `/api/ddb-monitor-groups` | `data:view` | 分组列表 |
| POST | `/api/ddb-monitor-groups` | `data:manage` | 新建分组 |
| PATCH | `/api/ddb-monitor-groups/:groupId` | `data:manage` | 更新展示名、排序、启用状态 |
| DELETE | `/api/ddb-monitor-groups/:groupId` | `data:manage` | 删除分组（无监控项引用时） |

### 3.2 GET `/api/ddb-monitor-groups`

**Query**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `enabled` | `0` \| `1` | 可选；不传则返回全部（含禁用，供管理弹窗使用） |

**Response `data`**

```json
[
  {
    "group_id": "market",
    "display_name": "行情",
    "sort_order": 10,
    "enabled": 1,
    "description": "A 股日频行情相关表",
    "monitor_count": 3,
    "created_at": "2026-06-01T10:00:00+08:00",
    "updated_at": "2026-06-20T10:00:00+08:00"
  }
]
```

- `monitor_count`：当前关联的监控项数量（`SELECT COUNT(*) FROM ddb_monitor WHERE group_id = ?`）
- 列表按 `sort_order ASC, group_id ASC` 排序

### 3.3 POST `/api/ddb-monitor-groups`

**Body**

```json
{
  "group_id": "alpha_v2",
  "display_name": "Alpha 因子 V2",
  "sort_order": 25,
  "enabled": 1,
  "description": "可选备注"
}
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `group_id` | 是 | 唯一，符合命名规则 |
| `display_name` | 是 | 非空 |
| `sort_order` | 否 | 默认 `0` 或当前最大值 + 10 |
| `enabled` | 否 | 默认 `1` |
| `description` | 否 | |

**错误**

| HTTP | 场景 |
| --- | --- |
| 400 | `group_id` 格式非法或 `display_name` 为空 |
| 409 | `group_id` 已存在 |

### 3.4 PATCH `/api/ddb-monitor-groups/:groupId`

**Body**（部分更新，至少一项）

```json
{
  "display_name": "行情数据",
  "sort_order": 5,
  "enabled": 1,
  "description": "更新备注"
}
```

- **不允许修改 `group_id`**
- 禁用分组（`enabled=0`）时：已有监控项保留原 `group_id`，新建/编辑监控项的下拉不再出现该组

### 3.5 DELETE `/api/ddb-monitor-groups/:groupId`

**规则**

- 当 `monitor_count > 0` 时返回 **409**，`message` 如：`该分组下仍有 3 个监控项，请先迁移或删除`
- `monitor_count = 0` 时物理删除

---

## 4. 对现有监控接口的影响

以下接口 Query / Body 中的 **`group_name` 改为 `group_id`**（精确匹配 `ddb_monitor_group.group_id`）：

| 接口 | 参数 |
| --- | --- |
| `GET /api/ddb-monitors` | `group_id` |
| `GET /api/ddb-monitors/snapshots` | `group_id` |
| `POST /api/ddb-monitors` | body.`group_id` |
| `PUT /api/ddb-monitors/:monitorId` | body.`group_id` |

**响应体**中监控项、快照对象的 `group_name` 字段改为 **`group_id`**。前端通过分组列表将 `group_id` 映射为 `display_name` 展示。

**示例**

```http
GET /api/ddb-monitors?group_id=market&enabled=1&page=1&page_size=500
GET /api/ddb-monitors/snapshots?group_id=market&date_from=2026-06-27&date_to=2026-07-10
```

---

## 5. 业务规则摘要

| 操作 | 规则 |
| --- | --- |
| 新建分组 | `group_id` 全局唯一，创建后不可改 |
| 改展示名 | 只更新 `display_name`，不影响已有监控项 |
| 禁用分组 | 下拉隐藏，历史数据保留 |
| 删除分组 | 仅 `monitor_count = 0` |
| 监控项归属 | 创建/编辑时必须传已存在且启用的 `group_id`（或允许选禁用组仅编辑场景，由产品决定；当前前端仅列出 `enabled=1`） |

---

## 6. 联调检查清单

- [ ] `GET /api/ddb-monitor-groups` 返回 `monitor_count`，排序正确
- [ ] `POST` 重复 `group_id` 返回 409
- [ ] `PATCH` 可改 `display_name` / `sort_order` / `enabled`，不可改 `group_id`
- [ ] `DELETE` 有监控项时 409，无监控项时成功
- [ ] `GET /api/ddb-monitors?group_id=market` 与库内英文 code 一致时可命中
- [ ] 快照接口 `group_id` 过滤与监控项一致
- [ ] 迁移后旧 `group_name` 参数不再使用（或短期兼容并打 deprecation 日志）

---

## 7. 前端行为说明（供后端理解联调预期）

1. 进入 `/data-monitor` 时先请求 `GET /api/ddb-monitor-groups?enabled=1` 填充下拉。
2. 概览 Tab 默认选中 `sort_order` 最小的启用分组，请求 `group_id=...`。
3. 配置 Tab 工具栏「管理分组」打开弹窗，请求不带 `enabled` 过滤的完整列表。
4. 表格、矩阵中的「分组」列显示 `display_name`，接口始终传 `group_id`。
