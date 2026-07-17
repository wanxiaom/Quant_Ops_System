# DolphinDB 测试写库命令

> 以下命令仅用于人工测试，不应加入生产 Cron。除指数成分股外，必须同时传入
> `--enable-write --test-write`；测试数据只写入与正式表同库的 `<table>_test`。
> 本文命令尚未执行。

测试日期统一以 `20240521` 为例。Linux 命令均从 `quant_ops_system` 根目录运行。

如果 parquet 已经存在，只测试数据库写入时应增加 `--upload-only`，此模式不会初始化
RQData，也不受登录机器数额度影响；不要同时传入 `--force`：

```bash
python3 src/scripts/market_data/market_data_stock.py \
  --trade-date 20240520 --frequencies 1m --upload-only \
  --enable-write --test-write
```

`--upload-only` 已同时支持 stock、ETF 和 index。若 parquet 不存在，仍需先释放
RQData 登录额度后执行正常下载命令。

## RQData 行情

### 股票日频：`market_data_1d/stock_test`

```bash
python3 src/scripts/market_data/market_data_stock.py \
  --trade-date 20240521 --frequencies 1d --force \
  --enable-write --test-write
```

股票分钟行情使用 `datetime`，测试表按正式分钟表 Schema 创建：

```bash
python3 src/scripts/market_data/market_data_stock.py \
  --trade-date 20240521 --frequencies 1m --force \
  --enable-write --test-write
```

### ETF 日频：`market_data_1d/etf_test`

```bash
python3 src/scripts/market_data/market_data_etf.py \
  --trade-date 20240521 --frequencies 1d --force \
  --enable-write --test-write
```

ETF 分钟行情同样使用 `datetime`。运行前要求对应频率数据库中已经存在正式
`etf` 表，例如 `dfs://market_data_1m/etf`；测试工具需要读取正式表 Schema
创建 `etf_test`：

```bash
python3 src/scripts/market_data/market_data_etf.py \
  --trade-date 20240521 --frequencies 1m --force \
  --enable-write --test-write
```

### 指数日频：`market_data_1d/index_test`

```bash
python3 src/scripts/market_data/market_data_index.py \
  --trade-date 20240521 --frequencies 1d --force \
  --enable-write --test-write
```

### 自然周行情：`market_data_1w/{stock_test,index_test}`

```bash
python3 src/scripts/market_data/market_data_1w.py \
  --start-date 20240520 --end-date 20240526 \
  --tables stock index --enable-write --test-write
```

## 财务数据

三大报表命令会写入：

- `income_statement/{mrq0_test,ttm0_test,lyr0_test}`
- `balance_sheet/{mrq0_test,ttm0_test,lyr0_test}`
- `cashflow_statement/{mrq0_test,ttm0_test,lyr0_test}`

```bash
python3 src/scripts/basic_data/basic_financial_data.py \
  --trade-date 20240521 --force --enable-write --test-write
```

衍生指标命令会写入 `derived_financial_metrics` 库中的：

- `valuation_metrics_test`
- `operational_metrics_test`
- `cashflow_metrics_test`
- `financial_metrics_test`
- `growth_metrics_test`

```bash
python3 src/scripts/basic_data/derived_financial_metrics.py \
  --trade-date 20240521 --force --enable-write --test-write
```

## 股票信息与投研因子

股票信息写入 `stock_info` 库中的：

- `shares_test`
- `ex_factor_test`
- `market_value_test`
- `turnover_test`
- `ex_quote_test`
- `tradable_test`

```bash
python3 src/scripts/vendor_basic_data/run_stock_info.py \
  --trade-date 20240521 --force --enable-write --test-write
```

投研因子写入 `research_factors` 库中各正式输出表对应的 `_test` 表：

```bash
python3 src/scripts/vendor_research_factors/run_research_factors.py \
  --trade-date 20240521 --force --enable-write --test-write
```

测试表只隔离输出；行情、交易日、股票信息等输入仍读取正式依赖表。

## 外部量化因子

### 方正：`fz_quant_factors/daily_factors_test`

```bash
python3 src/scripts/factor_data/fz_quant_factors.py \
  --trade-date 20240521 --force --enable-write --test-write
```

### 长江（Windows）：`cj_quant_factors/daily_factors_test`

```powershell
python .\src\scripts\factor_data\cj_quant_factors.py `
  --trade-date 20240521 `
  --output-dir C:/quant_data/DolphinDB `
  --force --connector-timeout 120 --connector-retries 1 `
  --enable-write --test-write
```

## Wind 资产配置（Windows）

写入：

- `market_data_1d_wind/index_test`
- `wind_data/indicator_1d_test`

```powershell
python .\src\scripts\market_data\download_data_from_wind_wsd.py `
  --trade-date 20240521 `
  --output-dir C:/quant_data/DolphinDB `
  --enable-write --test-write
```

## 指数成分股

写入 `index_info/index_component_test`。该任务原本没有 dry-run，测试写入只需显式
指定 `--test-write`：

```bash
python3 src/scripts/index_components_data/index_components_data.py \
  --trade-date 20240521 --force --test-write
```

## Wind AH 与一致预测（Windows）

三个任务已直接写 DolphinDB，测试目标为：

- `dfs://ah_info/ah_premium_test`
- `dfs://consensus/consensus_rating_test`
- `dfs://consensus/consensus_rolling_test`

下载并写测试表：

```powershell
python .\src\scripts\market_data\ah_info.py `
  --trade-date 20240521 --force --enable-write --test-write

python .\src\scripts\basic_data\consensus_rating.py `
  --trade-date 20240521 --force --enable-write --test-write

python .\src\scripts\basic_data\consensus_rolling.py `
  --trade-date 20240521 --force --enable-write --test-write
```

已有 parquet 时可跳过 Wind 下载：

```powershell
python .\src\scripts\market_data\ah_info.py `
  --trade-date 20240521 --upload-only --enable-write --test-write

python .\src\scripts\basic_data\consensus_rating.py `
  --trade-date 20240521 --upload-only --enable-write --test-write

python .\src\scripts\basic_data\consensus_rolling.py `
  --trade-date 20240521 --upload-only --enable-write --test-write
```

## 全量对比

把表名替换为对应正式表和测试表：

```bash
python3 src/scripts/market_data/compare_ddb_tables.py \
  --trade-date 20240521 \
  --database market_data_1d \
  --formal-table stock \
  --test-table stock_test \
  --fail-on-difference
```

不带 `--fail-on-difference` 时，即使存在差异也返回退出码 0，适合先生成报告查看。

## 不属于 DolphinDB 测试表的任务

ST 股票任务写入 MySQL `stock_app.st_stock_list`，继续使用现有 dry-run、snapshot 和
insert/delete parquet 验证流程，不使用 `_test` 表规范。
