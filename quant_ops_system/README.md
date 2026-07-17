# 量化运维调度系统（Quant Ops System）

本文档记录当前可运行的开发环境。系统尚未生产就绪，所有业务测试应保持 dry-run；生产写库必须显式开启。

> 状态更新时间：2026-07-16

## 组件

- C++ Manager：src/manager_cpp/main.cpp
- Linux Agent：src/agent/linux_agent.py
- Windows Agent：src/agent/windows_agent.py
- MySQL / Redis：docker/docker-compose.yml
- 数据库迁移：db/migrations/
- WebSocket 客户端：项目根目录 ../ws_client.py

Manager 默认监听 ，MySQL 映射到 ，Redis 映射到 。


## 当前已实现功能

### 系统与权限

- Token 鉴权：请求需携带 `-`。
- 用户管理：支持登录、用户列表、创建、编辑、启用/禁用、删除；后端按角色返回权限集合。
- 时间统一：Manager 与 MySQL 会话均按北京时间返回和写入，避免 UTC/本地时间混用。

### 任务管理

- 支持任务列表、详情、创建、编辑、删除、启用/禁用、批量选择、立即执行、增量执行。
- 支持任务业务类型 `task_category`：`ops`、`data_download`、`factor_compute`、`model_training`。
- 接口返回中文展示值和英文枚举值；前端显示中文，筛选可传中文或英文枚举。
- 任务执行接口：
  - `POST /api/tasks/{task_id}/run`：执行当前任务，成功后按 DAG 继续触发下游。
  - `POST /api/tasks/{task_id}/incr`：只执行当前任务，不触发下游。
  - 两者均支持日期参数，推荐前端传 `date: "YYYY-MM-DD"`。

### DAG 监控

- 支持多流水线，流水线定义保存在 `pipelines` 表。
- 节点坐标保存在 `pipelines.nodes` JSON；前端拖拽后保存并发布即可持久化。
- 边关系保存在 `dag_edges`，`pipeline_id` 与 `pipelines.pipeline_id` 保持一致。
- 支持新建流程、编辑流程、添加任务、连边模式、删除节点、保存并发布、执行流程、重置视图。
- DAG 按日状态按选定日期返回节点状态：优先取该日期最近一次成功执行记录；没有成功时取最近一次执行记录。

### 日志中心

- 支持服务端分页、任务名搜索、执行节点筛选、状态筛选、时间范围筛选。
- 支持日志总量、成功、运行中、失败/超时统计。
- 支持日志详情查看和下载。

### 数据监控

- 支持 DolphinDB 库表同步、库表分组管理、库表配置管理、快照矩阵、单项重算和批量重算。
- 库表配置支持 `frequency`：`daily`、`weekly`、`monthly`，默认 `daily`。
- 周频按每周最后一个交易日判断产数日，月频按每月最后一个交易日判断产数日。
- 非产数日 `row_count=0` 可显示为正常；产数日 `row_count=0` 显示为异常。
- 支持行数波动异常检测：以历史成功产数日中位数为基线，默认阈值为日频 30%、周频 40%、月频 50%。
- 每个监控项支持配置：
  - `anomaly_check_enabled`：是否启用行数波动检测。
  - `anomaly_threshold_pct`：自定义阈值，`0` 表示按频率默认。
  - `anomaly_window_size`：历史窗口，默认 `20`。
  - `anomaly_min_samples`：最少样本数，默认 `5`。
- 快照接口会返回 `anomaly`、`anomalyBaselineRowCount`、`anomalyDeviationPct`、`anomalyThresholdPct`、`anomalyReason` 等字段，便于前端展示异常原因。
- 库表启用/禁用和编辑统一使用两个轻量 POST action 接口；旧 `PUT/PATCH /api/ddb-monitors/:monitorId` 已移除。

### 测试 DAG 任务

- `src/scripts/test/dag_test_task.py` 提供简单测试任务，用于验证多 DAG 队列、下游触发和日期参数传递。
- `db/migrations/005_dag_test_tasks.sql` 写入测试任务和测试流水线。

## 快速启动

以下命令均以 quant_ops_system 为当前目录。

### 1. 启动 MySQL 和 Redis

~~~bash
cd /home/wanxm/ops_maintenance/quant_ops_system/docker
sudo docker-compose up -d
~~~

首次初始化按顺序执行 `db/migrations/` 下的迁移脚本；当前包括 `001_init.sql` 到 `010_ddb_monitor_anomaly_threshold.sql`。`001` 创建基础表结构，后续迁移依次补充任务默认参数、用户、测试 DAG、流水线 ID 一致性、北京时间、任务业务类型、数据监控频率和数据监控行数波动异常阈值。

### 2. 编译并启动 Manager

~~~bash
cd /home/wanxm/ops_maintenance/quant_ops_system/src/manager_cpp
/home/wanxm/miniconda3/envs/cpp_env/bin/cmake --build build -j2

cd build
DB_HOST=- DB_PORT=- REDIS_HOST=- REDIS_PORT=- API_PORT=- ./manager_cpp
~~~

重新编译后必须重启正在运行的 Manager。

### 3. 启动 Linux Agent

米筐初始化当前由 `src/scripts/runtime.py` 统一处理：优先读取环境变量，未提供时使用项目内置默认 license `${RQ_LICENSE_KEY}`。环境变量仍可用于临时覆盖：

- 官方 RQDATAC_CONF / RQDATAC2_CONF
- RQDATAC_LICENSE
- RQDATAC_USERNAME、RQDATAC_PASSWORD，可选 RQDATAC_ADDR

普通启动：

~~~bash
cd /home/wanxm/ops_maintenance/quant_ops_system/src/agent
MANAGER_URL=- NODE_ID=node_001 TOKEN=${ACCESS_TOKEN} python3 linux_agent.py
~~~

Linux Agent 默认节点 ID 为 `node_001`。RQData、DolphinDB、股票信息、FZ 和投研因子任务均固定派发到该节点。CJ 已迁移到 Windows Agent。

### 4. 启动 Windows Agent

确保 Windows 已同步仓库，并安装 WindPy、CJ 升级版 Connector 和对应依赖。Wind 一致预测、AH、资产配置、ST 与 CJ 任务均固定到该节点：

~~~bat
cd C:\Users\your_username\Desktop\src
$env:MANAGER_URL="-"
$env:NODE_ID="windows-worker-01"
python .\agent\windows_agent.py
~~~

### 5. WebSocket 日志

在项目根目录执行：

~~~bash
python3 ws_client.py
~~~

客户端已对本机连接设置 proxy=None。连接成功后没有新输出是正常现象。

## DolphinDB 隔离测试写入

直接写 DolphinDB 的处理脚本统一支持 `--test-write`。除指数成分股外，测试写入必须
同时显式传入 `--enable-write --test-write`，目标固定为与正式表同库的
`<table>_test`，不允许通过自由表名写入其他表。测试表首次使用时按正式表 Schema
创建；同一测试日期采用先删后写并校验行数。

全部命令和目标测试表见 [TEST_WRITE_COMMANDS.md](TEST_WRITE_COMMANDS.md)。这些命令
用于人工验证，不应加入生产 Cron。

股票信息和投研因子通过 vendor 表基类隔离输出表；其行情、交易日及其他输入依赖仍
读取正式表。AH 溢价、一致预测评级和滚动预测已改为由 Windows 节点直接写入
DolphinDB，测试模式分别写入 `ah_premium_test`、`consensus_rating_test` 和
`consensus_rolling_test`。ST 股票写 MySQL，不采用本规范。

除 ST 股票外，当前已完成各数据脚本的隔离写库测试，并使用对比脚本与正式表按测试日期完成全量比对。测试结论见下节；测试表清理后可通过同一命令自动重建。

## 写库测试结果（截至 2026-06-26）

除 ST 股票外，所有已接入脚本均已执行 `--enable-write --test-write` 隔离写库测试，并按相同测试日期与正式表做全量对比。部分差异来自数据源在正式表历史写入后发生更新，或正式表当时为空/后续补写导致，不一定代表脚本错误；生产启用前仍需按业务口径逐项确认。

### 行情与 Wind 资产配置

- `market_data_1d`
  - ETF，`20240521`：与正式表一致。
  - index，`20260608`：测试表多 1 行。
  - stock，`20240520`：与正式表一致。
- `market_data_1m`
  - index，`20240521`：测试表多 720 行。
  - stock，`20240520`：与正式表一致。
- `market_data_1w`
  - index，`20240524`：测试表多 23 行。
  - stock，`20240520`：与正式表一致。
- 资产配置 Wind WSD，`20240521`
  - `market_data_1d_wind.index_test`：formal 16 行、test 14 行，formal 多 2 行，14 行共有记录存在 82 个单元格差异。
  - `wind_data.indicator_1d_test`：formal/test 均 48 行；共有 6 个主键组合，5 行、10 个单元格存在差异；`field` 和 `data` 字段存在列级差异。

### 财务、衍生指标与一致预测

- `financial_data`，`20240521`
  - `income_statement`：`mrq0_test`、`ttm0_test`、`lyr0_test` 均与正式表一致。
  - `balance_sheet`：`mrq0_test`、`ttm0_test`、`lyr0_test` 均与正式表一致。
  - `cashflow_statement`：`mrq0_test`、`ttm0_test`、`lyr0_test` 均与正式表一致。三张表原始差异较大，正式表重新拉取后已一致，原因是数据仍在更新。
- `consensus`，`20240521`
  - `consensus_rating_test`：2 条数据 4 个值不同，差异字段为评级均值类字段，测试值为 `nan`。
  - `consensus_rolling_test`：77 条数据 145 个值不同。
- `derived_financial_metrics`，`20240521`
  - `valuation_metrics_test`、`operational_metrics_test`、`cashflow_metrics_test`、`financial_metrics_test`、`growth_metrics_test` 均存在差异；整体差异原因是财报数据后续有更新。

### 基础数据、股票信息与量化因子

- `basic_data.ah_info.ah_premium_test`，`20240521`：与正式表一致。
- `stock_info`，`20240521`
  - `ex_factor_test`：1 条记录存在差异。
  - `ex_quote_test`：1 条记录的复权行情字段存在差异。
  - `market_value_test`：157 条记录的 `free_float_value` 存在差异。
  - `shares_test`：157 条记录的 `free_float` 存在差异。
  - `tradable_test`、`turnover_test`：与正式表一致。
- `research_factors`，`20240521`
  - `high_frequency_factors_test`：与正式表一致。
  - `emotion_factors_test`：3,527 条记录的 `corr_price_turn_prior_10D` 存在差异。
  - `growth_factors_test`：5,116 行、53,095 个单元格差异。
  - `new_factors_test`：5,116 行、78,411 个单元格差异。
  - `quality_factors_test`：5,116 行、108,234 个单元格差异。
  - `reverse_factors_test`：5,086 行、5,822 个单元格差异。
  - `trend_factors_test`：817 行、817 个单元格差异，集中在 `overnight_mom20`。
  - `value_factors_test`：5,116 行、34,165 个单元格差异。
  - 差异主要受上游更新、指标定义和依赖数据重算影响。
- `fz_quant_factors.daily_factors_test`，`20240521`：1,511 行、4,268 个单元格差异；`12` 分析师自身预期调整、`1510`、`12` 具体购买、`1487` 预期惯性、`1271` 正式表为空值。
- `cj_quant_factors.daily_factors_test`，`20240521`：4,787 行、6,443 个单元格差异。

### 尚未完成

- ST 股票写入 `stock_app.st_stock_list`，不是 DolphinDB `_test` 表模式；目前已完成 dry-run、snapshot 和 insert/delete parquet 验证流程，尚需在确认当前交易日口径后执行 MySQL 真实写库验证，并在写后再次 dry-run 确认 insert/delete 均为 0。

## 后续工作计划

1. 对上述差异进行业务复核，给每类差异标记处理结论：数据源更新导致、正式表历史为空、字段口径差异、脚本问题或需忽略。
2. 对 ST 股票执行一次受控真实写库验证：先备份 `stock_app.st_stock_list`，写入后重新 dry-run，比对 insert/delete 是否归零。
3. 固化回归流程：保留 `TEST_WRITE_COMMANDS.md` 中的测试命令，补充一键批量对比脚本或清单，统一输出 `summary.json` 汇总表。
4. 清理测试表策略：测试完成后删除 `_test` 表；再次测试时由脚本按正式表 schema 自动重建。
5. 生产启用前逐项检查节点定向、Cron、DAG 参数继承、Windows/Linux Python 环境、Wind/RQData/CJ/FZ 凭据和 DolphinDB 写入权限。
6. 将仍写在脚本中的 license `${RQ_LICENSE_KEY}`、源库账号和数据库密码迁移到受控配置或密钥管理；保留本地开发默认值但避免生产明文散落。
7. 设计正式上线回滚方案：每个写库任务保留测试日期、目标表、写入行数、对比报告和恢复方式。



## 节点分配

- Linux `node_001`：RQData、DolphinDB、股票/ETF/指数行情、财务、衍生指标、股票信息、FZ、投研因子。
- Windows `windows-worker-01`：Wind 一致预测、AH、资产配置、ST，以及使用 Windows 升级版 Connector 的 CJ。
- 当前任务表已无公共队列任务。

## 任务业务类型 task_category

任务管理接口支持业务类型字段，前端创建/编辑任务时可以提供 `task_category` 或 `taskCategory`。后端兼容中文展示值和英文枚举，最终入库存储为稳定英文枚举。

可选值：

| 前端展示 | 推荐提交值 |
| --- | --- |
| 运维任务 | `ops` |
| 数据下载任务 | `data_download` |
| 因子计算任务 | `factor_compute` |
| 模型训练任务 | `model_training` |

创建/编辑示例：

```json
{
  "task_id": "task_example",
  "name": "Example Task",
  "node_type": "python",
  "task_category": "数据下载任务",
  "script_path": "src/scripts/example.py"
}
```

`GET /api/tasks` 和 `GET /api/tasks/{task_id}` 返回：

- `task_category` / `taskCategory`：中文展示值，例如 `数据下载任务`。
- `task_category_code` / `taskCategoryCode`：英文枚举，例如 `data_download`。
- `task_category_label` / `taskCategoryLabel`：中文展示值。

任务列表支持按类型筛选：`GET /api/tasks?task_category=data_download`，也兼容中文查询值。

## 任务默认参数 default_params

`tasks` 表已新增 `default_params TEXT` 字段，用于保存 Cron 和手动 `/run` 触发时的默认执行参数。Manager 创建 `task_instances` 时会把该字段写入实例的 `params`，Agent 再把 JSON 参数转换为脚本命令行参数。

当前实现规则：

- `POST /api/tasks/{task_id}/run`：执行当前任务，并在成功后按 `pipeline_id` 继续执行下游。请求参数覆盖任务自己的 `default_params`；支持 `date: "YYYY-MM-DD"` 或 `trade_date: "YYYYMMDD"`，并将日期继续传给下游任务。
- `POST /api/tasks/{task_id}/incr`：只执行当前任务，不触发下游。请求参数覆盖任务自己的 `default_params`，适合增量触发或补数运行。
- DAG 页面“执行下游”请求示例：`{"pipeline_id":"main_daily_dag","date":"2026-07-15"}`。页面“立即执行”继续调用 `/incr`。
- `/run` 和 `/incr` 返回前端 `TriggerTaskResponse` 结构：顶层包含 `code`、`message`、`exec_id`，并带 `execution` 对象，字段对应新创建的 `task_instances`。
- Cron 定时触发：使用任务自己的 `default_params`。
- DAG 子任务：以子任务自己的 `default_params` 为基础，只继承父任务中的通用参数：`trade_date`、`start_date`、`end_date`、`dry_run`、`enable_write`、`test_write`、`force`，避免 CJ 的 `connector_*`、Windows `output_dir`、ST 的 `compare_db` 等专有参数污染下游任务。

当前库已执行 `db/migrations/003_default_params.sql`，安全默认值如下：

~~~text
行情、财务、一致预测、AH、股票信息、CJ/FZ、投研因子：{"force":true,"dry_run":true}
资产配置、周频行情：{"dry_run":true}
ST 股票：{"dry_run":true,"compare_db":true,"force":true,"output_dir":"C:/quant_data/DolphinDB"}
指数成分股：{"force":true}
~~~

其中 `force=true` 主要控制重新下载/重新生成已有日期的 parquet 或覆盖已有日期数据；真正控制是否写正式表的是 `enable_write=true`。因此当前 Cron 大多数仍处于 dry-run 安全模式，不会写正式表。

生产切换示例：

~~~bash
MYSQL_PWD=${DB_PASSWORD} mysql -h127.0.0.1 -P13306 -uroot -Dquant_ops -e "
UPDATE tasks
SET default_params='{\"force\":true,\"enable_write\":true}'
WHERE task_id IN (
  'task_market_stock_daily',
  'task_market_index_daily',
  'task_market_etf_daily'
);
"
~~~

切换主 DAG 全链路时，应按任务逐项确认 Windows/Linux 环境、数据源可用性、测试写库对比结果和回滚方案后，再把对应任务的 `default_params` 从 `dry_run` 改为 `enable_write`。ST 股票涉及 MySQL 当前有效清单，必须单独执行备份、真实写入和二次 dry-run 验证。

## DAG

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

只有父任务退出码为 0 时才创建子任务。失败或人工终止不会继续触发 DAG。CJ 已在 Windows Agent 验证成功并恢复到主 DAG；父任务参数会在 Linux 与 Windows 节点之间继续继承。ST、指数成分股、资产配置、ETF、指数和周线行情仍为独立任务。

完整任务、节点、Cron/DAG 时间、输入、输出及目标表见 [TASK_INVENTORY.md](TASK_INVENTORY.md)。



## 交易日历

Manager 启动时会自动创建 `trade_calendar` 表，`/api/tasks/:taskId/data-days` 优先从这张表读取最近交易日；表为空或查询失败时才回退到工作日近似。

~~~sql
CREATE TABLE IF NOT EXISTS trade_calendar (
  trade_date VARCHAR(10) PRIMARY KEY,
  source VARCHAR(64) NOT NULL DEFAULT 'mysql',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
~~~

## 数据监控接口与配置

### 库表配置列表

~~~http
GET /api/ddb-monitors?page=1&page_size=15&group_id=market_data&keyword=index&enabled=1
~~~

返回字段包含：

- `frequency` / `frequencyCode`：中文展示值和英文枚举。
- `anomaly_check_enabled` / `anomalyCheckEnabled`。
- `anomaly_threshold_pct` / `anomalyThresholdPct`。
- `anomaly_window_size` / `anomalyWindowSize`。
- `anomaly_min_samples` / `anomalyMinSamples`。

### 启用 / 禁用库表

~~~http
POST /api/ddb-monitors/:monitorId/enabled
~~~

请求体：

~~~json
{ "enabled": 1 }
~~~

禁用传 `0`。成功判断统一使用 `code === 0`。

### 编辑库表配置

~~~http
POST /api/ddb-monitors/:monitorId/update
~~~

请求体沿用编辑表单字段，支持局部更新：

~~~json
{
  "database": "dfs://market_data_1d",
  "table_name": "index",
  "date_column": "date",
  "date_format": "YYYY-MM-DD",
  "group_id": "market_data",
  "frequencyCode": "daily",
  "anomalyCheckEnabled": 1,
  "anomalyThresholdPct": 0,
  "anomalyWindowSize": 20,
  "anomalyMinSamples": 5,
  "where_extra": "",
  "related_task_ids": [],
  "enabled": 1,
  "description": ""
}
~~~

成功返回轻量结果；前端保存成功后刷新列表即可。

### 快照矩阵

~~~http
GET /api/ddb-monitors/snapshots?date_from=2026-07-03&date_to=2026-07-16&group_id=market_data&keyword=index
~~~

每个单元格返回：

- `status`：前端展示状态，已包含频率产数日和行数波动异常判断。
- `raw_status` / `rawStatus`：数据库原始状态。
- `expected_data_day` / `expectedDataDay`：该日期是否应产数。
- `anomaly`：是否为行数波动异常。
- `anomalyBaselineRowCount`：历史中位数基线。
- `anomalyDeviationPct`：偏离比例，例如 `0.411` 表示 41.1%。
- `anomalyThresholdPct`：本次使用阈值。
- `anomalySampleCount`：参与基线计算的历史样本数。
- `anomalyReason`：异常原因说明。

状态判断顺序：

1. 非产数日且 `raw_status=zero,row_count=0`，展示为 `success`。
2. 产数日且 `row_count=0`，展示为异常。
3. 产数日且 `raw_status=success,row_count>0`，进入行数波动检测。
4. 行数偏离历史中位数超过阈值时展示为异常，并返回 `anomaly=true`。

### 重算

~~~http
POST /api/ddb-monitors/:monitorId/recount?date=YYYY-MM-DD
POST /api/ddb-monitors/recount-batch?date=YYYY-MM-DD
~~~

重算只更新快照真实行数和基础状态；快照接口展示时再根据频率和异常阈值计算最终展示状态。

## 状态码

| 状态 | 含义 |
| --- | --- |
| 2 | Ready |
| 0 | Running |
| 1 | Success |
| -1 | Failed |
| -2 | Timeout |

## 日志

从 src/manager_cpp/build 启动 Manager 时，任务日志位于：

~~~text
src/manager_cpp/build/logs/tasks/<exec_id>.log
~~~

实时查看：

~~~bash
tail -f src/manager_cpp/build/logs/tasks/<exec_id>.log
~~~

查询任务状态：

~~~bash
MYSQL_PWD=${DB_PASSWORD} mysql --protocol=TCP -h127.0.0.1 -P13306 -uroot -Dquant_ops -e "SELECT exec_id,task_id,node_id,status,exit_code,start_time,end_time,log_path FROM task_instances ORDER BY start_time DESC LIMIT 20;"
~~~

## Dry-run 与生产写入

vendor 入口默认 ENV_TYPE=development。连接层会拦截 append!、tableInsert、建表、INSERT、UPDATE 和 DELETE。

只有显式传入 --enable-write 才进入生产写入模式。生产启用前必须完成 Schema、行数、字段、主键、重复数据和回滚验证。

## 已知限制

- CJ 依赖 Windows 专用升级版 Connector；需要归档升级包并确保 `.pyd` 与 Agent Python 小版本严格匹配。
- 投研因子耗时较长，单个因子计算期间日志可能暂时无新增内容。
- Agent 配置、代理、凭据和 Python 环境尚未完全配置化。
- 项目内仍有按用户要求保留的默认 license `${RQ_LICENSE_KEY}`/源库配置；后续生产化应迁移到密钥管理或受控配置。
- Manager 的任务领取需要进一步实现事务化和幂等。
- 前端页面已接入核心功能，但仍缺少完整自动化回归测试和告警闭环。
