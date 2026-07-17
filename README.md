## 脱敏与开源说明

本仓库为个人学习、技术交流与工程能力展示所整理的脱敏版本，不包含原实习单位或相关公司的内部业务数据、客户信息、数据库账号、服务器地址、API Key、访问令牌及生产环境配置。

为保护公司知识产权和业务安全，本项目已删除或重构以下内容：

* 公司内部真实业务数据及数据样例；
* 与生产环境相关的连接配置和部署信息；
* 数据清洗、加工及处理过程中的核心业务规则；
* 具有商业敏感性的关键算法、策略参数和处理逻辑；
* 内部接口、认证信息及其他不适合公开的内容。

仓库中部分模块仅保留通用架构、接口定义、任务调度流程和示例实现。涉及内部核心处理逻辑的部分，已使用简化实现、占位接口、伪数据或说明性代码替代。因此，本仓库在功能完整性、运行结果和处理能力方面与原生产系统并不完全一致。

本项目仅用于展示个人在 C++/Python 开发、并发处理、数据库交互、任务调度和工程化设计等方面的实践能力，不代表原实习单位的正式产品，也不包含任何未经授权的公司源代码、业务数据或技术资料。

# 分布式量化运维管理系统（Quant Ops System）

本项目用于统一调度 Linux、Windows 节点上的量化数据更新脚本。C++ Manager 管理任务、DAG、Cron、节点心跳和日志，Python Agent 隔离执行脚本，业务数据最终面向 DolphinDB。

> 状态更新时间：2026-07-16  
> 当前阶段：调度、DAG、日志、用户、任务管理、数据监控和前端对接接口已形成可运行闭环；数据脚本仍按 dry-run 优先，生产写库需显式开启。

## 系统架构

1. **Manager**：C++20、libhv、ormpp；MySQL 保存任务和执行实例，Redis 保存 Agent 心跳。
2. **Agent**：Linux Agent 运行米筐、DolphinDB和投研任务；Windows Agent 运行 WindPy 任务。
3. **调度能力**：HTTP、WebSocket、Token 鉴权、Cron、DAG、手动/增量触发、目标节点派发。
4. **监控能力**：子进程日志批量上传、按执行 ID 落盘、WebSocket 实时推送。
5. **前端**：Vue 管理前端已接入任务管理、DAG 监控、日志中心、数据监控、用户管理等页面。

## 当前已实现

- C++ Manager 已可编译运行。
- MySQL + Redis 基础设施已配置。
- 已实现任务创建、列表、查看、编辑、删除、启用/禁用、批量操作、手动执行、执行下游和带参增量执行。
- 任务支持业务类型：运维任务、数据下载任务、因子计算任务、模型训练任务，并支持按类型筛选。
- 已实现 Cron、DAG 成功触发、按日期运行状态、前端拖拽编排、节点位置保存、流程重置和多流水线。
- 已实现日志中心分页、搜索、状态筛选、时间筛选、详情查看和下载。
- 已实现用户登录、用户管理、角色权限、Token 鉴权和基础会话控制。
- 已实现 DolphinDB 数据监控：库表同步、分组配置、库表配置、行数快照、按频率产数日判断、异常重算、启用/禁用和编辑接口。
- 数据监控支持日频/周频/月频；非产数日 0 行可判定正常，产数日 0 行或行数异常波动会判定为异常。
- Linux、Windows Agent 均已完成端到端任务测试。
- 日志可按任务保存并通过 WebSocket 实时查看。

### 当前主 DAG

~~~text
task_market_stock_daily
  → task_basic_financial_daily
  → task_derived_metrics_daily
  → task_consensus_rating_daily
  → task_consensus_rolling_daily
  → task_ah_info_daily
  → task_stock_info_daily
  → task_cj_quant_factors_daily
  → task_fz_quant_factors_daily
  → task_research_factors_daily
~~~

ST、资产配置、ETF、指数和周线行情为独立任务。所有任务均已按运行环境固定到 Linux `node_001` 或 Windows `windows-worker-01`。


## 当前管理端功能总览（2026-07-16）

### 任务管理

- 任务表支持 `task_category`，稳定枚举为 `ops`、`data_download`、`factor_compute`、`model_training`。
- 前端展示中文：运维任务、数据下载任务、因子计算任务、模型训练任务。
- 创建、编辑、列表、详情、筛选接口均返回中文展示值和英文枚举值。
- 任务列表支持按任务类型筛选。

### DAG 监控

- 支持多流水线，新建、编辑、删除、保存并发布、执行流程。
- 支持前端拖拽节点位置，位置保存在 `pipelines.nodes` JSON 中。
- `run` 表示执行当前任务并继续触发下游；`incr` 表示只执行单任务。
- `run` / `incr` 均支持日期参数，前端日期可传 `date: "YYYY-MM-DD"`；业务脚本侧按任务需要转换为 `trade_date`。
- DAG 按日状态接口返回选定日期下的节点状态；状态优先取该日期最近一次成功执行，没有成功则取最近一次执行记录。

### 日志中心

- 支持服务端分页、任务名搜索、执行节点筛选、状态筛选、时间范围筛选。
- 支持日志详情、下载、统计卡片和刷新。

### 数据监控

- 支持 DolphinDB 库表同步、库表分组、库表配置、快照矩阵、单项重算、批量重算。
- 库表配置支持 `frequency`：`daily`、`weekly`、`monthly`，默认 `daily`。
- 周频按每周最后一个交易日判断是否应产数；月频按每月最后一个交易日判断是否应产数。
- 非产数日 `row_count=0` 可显示为正常；产数日 `row_count=0` 仍显示为异常。
- 支持行数波动异常检测：以历史成功产数日的中位数为基线，日频默认阈值 30%，周频 40%，月频 50%。
- 每个监控项可配置 `anomaly_check_enabled`、`anomaly_threshold_pct`、`anomaly_window_size`、`anomaly_min_samples`。
- 启用/禁用和编辑已改为轻量 POST 接口：`POST /api/ddb-monitors/:monitorId/enabled`、`POST /api/ddb-monitors/:monitorId/update`；旧 `PUT/PATCH /api/ddb-monitors/:monitorId` 已移除。

### 用户与权限

- 已实现登录、Token 鉴权、用户列表、创建、编辑、启用/禁用、删除等用户管理能力。
- 后端按角色返回权限集合，前端按权限控制页面和操作入口。

## 数据模块进度

| 顺序 | 模块 | 数据源 | 当前状态 |
| --- | --- | --- | --- |
| 1 | 股票/ETF/指数行情 | 米筐 | 已完成隔离写库测试；1d stock/ETF、1m stock、1w stock 与正式表一致，指数类存在少量新增行需复核 |
| 2 | 财务数据 | 米筐 | 已完成隔离写库测试；三张财务表各口径与重新拉取后的正式表一致 |
| 3 | 衍生财务指标 | 米筐 | 已完成隔离写库测试；因上游财报数据更新，与正式表存在预期差异，待业务确认 |
| 4 | 一致预测评级/滚动 | 万得 | 已完成 Windows 直接写 DolphinDB 测试；评级和滚动预测存在少量字段差异，待业务确认 |
| 5 | AH 股信息 | 万得 | 已完成隔离写库测试，与正式表一致 |
| 6 | 股票信息（附件1） | 米筐 + DolphinDB | 六张表已完成隔离写库测试；tradable、turnover 一致，shares/free_float 相关字段存在差异待复核 |
| 7 | CJ 量化因子 | quant_db | Windows 升级版 Connector 已接入并完成隔离写库测试；与正式表存在差异待业务复核 |
| 8 | FZ 量化因子 | FZ MySQL | 已完成隔离写库测试；部分正式表为空值或数据源更新导致差异，待业务确认 |
| 9 | 投研因子（附件2） | 米筐 + DolphinDB | 已完成隔离写库测试；高频因子一致，其余多张因子表存在上游/口径差异待复核 |
| 已接入 | 资产配置 | Wind WSD | 已完成 Windows 写库测试；`prev_close` schema 对齐已修复，Wind 指标类仍有差异待复核 |
| 已接入 | ST 股票 | Wind + MySQL | 已完成 dry-run、snapshot、insert/delete parquet 验证；尚未执行 MySQL 真实写库验证 |
| 已接入 | 成分股指数 | quant_db | 已验证直接写 DolphinDB，6:30 定时日更；支持测试表写入 |

## 写库测试与安全约束

开发和测试默认禁止正式写库。DolphinDB 脚本只有显式传入 `--enable-write --test-write` 才写入同库 `<table>_test`；测试表按正式表 schema 自动创建，按测试日期先删后写并校验行数。测试完成后可删除 `_test` 表，下次测试会自动重建。

除 ST 股票外，当前所有已接入脚本均已完成隔离写库测试，并与正式表做过全量对比。对比报告保存在 `quant_ops_system/comparison_reports/`，详细结论和命令见 `quant_ops_system/README.md`、`quant_ops_system/TEST_WRITE_COMMANDS.md`。

ST 股票写入 MySQL `stock_app.st_stock_list`，不采用 DolphinDB `_test` 表模式；当前只完成 dry-run、snapshot 和 insert/delete parquet 验证，真实写库前必须先备份 MySQL 表，写入后再次 dry-run 确认 insert/delete 均为 0。

## 目录结构

~~~text
quant_ops_system/
├── README.md                         启动、测试与运维命令
├── TASK_INVENTORY.md                 任务、节点、Cron、输入输出和目标表
├── config/                           系统配置
├── db/migrations/
│   ├── 001_init.sql                  MySQL 表结构
│   └── 002_tasks.sql                 全部任务、节点、Cron 和最终 DAG
├── docker/docker-compose.yml         MySQL 8 与 Redis 7 开发环境
├── requirements/                     Agent、vendor 和环境差异依赖清单
├── src/
│   ├── manager_cpp/                  C++20 Manager 源码、build 和任务日志
│   ├── agent/                        Linux / Windows Agent
│   └── scripts/
│       ├── market_data/              行情、AH、资产配置任务
│       ├── basic_data/               财务、一致预测、ST 任务
│       ├── factor_data/              CJ、FZ 因子任务
│       ├── connectors/               正式 Connector
│       ├── my_utils/、wind_utils/    公共数据工具
│       ├── vendor_basic_data/        股票信息 vendor 代码
│       └── vendor_research_factors/  投研因子 vendor 代码
│       └── index_components_data/    成份股指数
├── F:/DolphinDB/                     Linux dry-run parquet 验收数据
├── data/、logs/、.venv/              保留的运行数据、日志和本地环境
└── iguana/、ormpp/、libhv/           C++ Manager 第三方依赖及构建产物
~~~

本轮整理只移除了 FastAPI/SQLite/PoC 旧原型、重复的 `local_update` 代码、重复 Connector、旧测试下载脚本和中文规划占位入口。运行生成物、parquet、日志、虚拟环境、当前 Manager build 及第三方依赖均保留。


## 调度默认参数更新（2026-07-07）

`quant_ops.tasks` 表已新增 `default_params` 字段，用于配置 Cron 和手动 `/run` 触发时的默认执行参数。当前已通过 `quant_ops_system/db/migrations/003_default_params.sql` 应用到 8.11 服务器 MySQL。

- Cron 触发不再固定写入空参数 `{}`，而是读取每个任务自己的 `default_params`。
- DAG 子任务以自己的 `default_params` 为基础，只继承父任务中的通用参数：`trade_date`、`start_date`、`end_date`、`dry_run`、`enable_write`、`test_write`、`force`。
- 当前大多数任务默认仍为 dry-run：`{"force":true,"dry_run":true}`，不会写正式表。生产写库必须逐项把对应任务切换为 `{"force":true,"enable_write":true}` 并完成回滚预案。
- `force=true` 控制重新下载/重新生成或覆盖已有日期数据；`enable_write=true` 才控制正式写库。

Manager 已重新编译，重启 Manager 后该逻辑生效。详细说明见 [quant_ops_system/README.md](quant_ops_system/README.md)。

全部任务、节点、时间、输入、输出及目标表见 [任务清单](quant_ops_system/TASK_INVENTORY.md)。具体启动、测试和运维命令见 [quant_ops_system/README.md](quant_ops_system/README.md)。
