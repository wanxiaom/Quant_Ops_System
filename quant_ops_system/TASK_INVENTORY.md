# 任务运行清单

> 更新时间：2026-06-25  
> 数据来源：当前 `quant_ops.tasks`、`dag_edges`、迁移 SQL 与业务脚本。  
> 当前阶段：全部脚本已完成 dry-run 初验；DolphinDB 隔离测试写入正在逐项验证，AH 与一致预测评级已完全一致，滚动预测写入完整但部分历史字段值存在差异；生产写入仍未开放；全部任务已固定执行节点。

## 调度关系

主 DAG：

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

独立任务：`task_market_etf_daily`、`task_market_index_daily`、`task_market_data_weekly`、`task_asset_allocation_daily`、`task_st_stock_daily`、`task_index_components_daily`。

## 测试写入规范

- 直接写 DolphinDB 的任务使用 `--enable-write --test-write`，仅写 `<table>_test`。
- 指数成分股使用 `--test-write`，写入 `index_info/index_component_test`。
- 测试表按正式表 Schema 创建，并按测试日期覆盖写入。
- vendor 股票信息和投研因子只隔离输出表，输入依赖仍读取正式表。
- Wind 一致预测和 AH 已由 Windows 节点直接写 DolphinDB，测试模式写同库 `_test` 表。
- ST 股票目标为 MySQL，不使用 DolphinDB `_test` 表。
- 具体命令见 `TEST_WRITE_COMMANDS.md`。

## 全部任务

| 任务 ID | 数据源/用途 | 当前节点配置 | 时间/触发 | 主要输入 | dry-run 输出 | 生产目标 |
| --- | --- | --- | --- | --- | --- | --- |
| `task_market_stock_daily` | RQData 股票多频行情 | `node_001` Linux | 每日 22:15；主 DAG 起点 | `trade_date`、`frequencies`、`force` | `F:/DolphinDB/market_data_<freq>/stock/YYYY/YYYYMMDD.parquet` | DolphinDB `dfs://market_data_<freq>/stock`，默认 1d/60m/30m/15m/5m/1m |
| `task_market_etf_daily` | RQData ETF 多频行情 | `node_001` Linux | 每日 22:15，独立 Cron | 同上 | `F:/DolphinDB/market_data_<freq>/etf/...` | DolphinDB `dfs://market_data_<freq>/etf` |
| `task_market_index_daily` | RQData 指数多频行情 | `node_001` Linux | 每日 22:15，独立 Cron | 同上 | `F:/DolphinDB/market_data_<freq>/index/...` | DolphinDB `dfs://market_data_<freq>/index` |
| `task_market_data_weekly` | RQData 股票/指数自然周行情 | `node_001` Linux | 每周日 22:15，独立 Cron | `start_date`、`end_date`、`tables` | `F:/DolphinDB/market_data_1w/{stock|index}/YYYY/*.parquet` | DolphinDB `dfs://market_data_1w/{stock|index}` |
| `task_basic_financial_daily` | RQData 三大财务报表 | `node_001` Linux | 股票行情成功后 DAG | `trade_date`、`force` | 项目 `F:/DolphinDB/{income_statement|balance_sheet|cashflow_statement}/{mrq0|ttm0|lyr0}/...` | 同名 DolphinDB 库表 |
| `task_derived_metrics_daily` | RQData 衍生财务指标 | `node_001` Linux | 财务数据成功后 DAG | `trade_date`、`force` | `F:/DolphinDB/derived_financial_metrics/<table>/...` | `dfs://derived_financial_metrics/{valuation_metrics|operational_metrics|cashflow_metrics|financial_metrics|growth_metrics}` |
| `task_consensus_rating_daily` | Wind 一致预测评级 | `windows-worker-01` Windows | 衍生指标成功后 DAG | `trade_date`、`force` | `C:/quant_data/DolphinDB/consensus/consensus_rating/...` | DolphinDB `dfs://consensus/consensus_rating`；测试表 `consensus_rating_test` |
| `task_consensus_rolling_daily` | Wind 一致预测滚动值 | `windows-worker-01` Windows | 评级成功后 DAG | `trade_date`、`force` | `C:/quant_data/DolphinDB/consensus/consensus_rolling/...` | DolphinDB `dfs://consensus/consensus_rolling`；测试表 `consensus_rolling_test` |
| `task_ah_info_daily` | Wind AH 溢价 | `windows-worker-01` Windows | 滚动一致预测成功后 DAG | `trade_date`、`force` | `C:/quant_data/DolphinDB/ah_info/ah_premium/...` | DolphinDB `dfs://ah_info/ah_premium`；测试表 `ah_premium_test` |
| `task_stock_info_daily` | RQData + DolphinDB 股票信息 | `node_001` Linux | AH 成功后 DAG | `trade_date` 或日期范围、`force` | 当前主要为日志/内存预览 | DolphinDB `dfs://stock_info/{shares|ex_factor|market_value|turnover|ex_quote|tradable}` |
| `task_fz_quant_factors_daily` | 方正源 MySQL 因子 | `node_001` Linux | CJ 成功后 DAG | `trade_date` 或日期范围、`force` | 项目 `F:/DolphinDB/fz_quant_factors/daily_factors/...` | DolphinDB `dfs://fz_quant_factors/daily_factors` |
| `task_research_factors_daily` | RQData + DolphinDB 投研因子 | `node_001` Linux | FZ 成功后 DAG | `trade_date` 或日期范围、`force` | 当前主要为日志；development 模式禁止写库 | DolphinDB `dfs://research_factors/{quality_factors|growth_factors|value_factors|trend_factors|reverse_factors|emotion_factors|high_frequency_factors|new_factors}` |
| `task_cj_quant_factors_daily` | 长江 quant_db 因子 | `windows-worker-01`，Windows 升级版 Connector | 股票信息成功后 DAG | `trade_date`、Connector 超时/重试参数、`force` | `C:/quant_data/DolphinDB/cj_quant_factors/daily_factors/...`；已验证 5,348 行、302 列 | DolphinDB `dfs://cj_quant_factors/daily_factors` |
| `task_asset_allocation_daily` | Wind WSD 资产配置指标 | `windows-worker-01` | 每日 07:30，独立 Cron | 单日/日期范围，默认回看 3 日 | `C:/quant_data/DolphinDB/{market_data_1d_wind/index|wind_data/indicator_1d}/<rule>/...` | DolphinDB `dfs://market_data_1d_wind/index`、`dfs://wind_data/indicator_1d` |
| `task_st_stock_daily` | Wind ST 名单与 MySQL 差异 | `windows-worker-01` | 每日 08:00，独立 Cron  | `trade_date`、`compare_db`、`force` | `C:/quant_data/DolphinDB/st_stock_list/snapshot|diff/YYYY/*.parquet` | MySQL `stock_app.st_stock_list`；只允许显式 `enable_write` |

## 节点与时间待办


- ST 当前保持手动测试；生产时间可在确认 Wind 数据就绪后单独设定，例如 07:45。
- CJ 已恢复进入主 DAG；需保持 Windows Agent 的 Python 版本和升级版 `.pyd` 一致。
- 所有生产写入必须经过测试库、Schema、幂等、回滚和数据质量验收。
