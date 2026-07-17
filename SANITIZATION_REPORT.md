# Sanitization Report

## Scope

- Project root: <PROJECT_ROOT>
- Full project scan with emphasis on quant_ops_system, database connector folders, agent files, scripts, docs, and config-like files.

## Modified Files

- .env.example
- .gitignore
- config.example.yaml
- index_components_data.py
- quant_ops_system\README.md
- quant_ops_system\src\agent\linux_agent.py
- quant_ops_system\src\agent\windows_agent.py
- quant_ops_system\src\manager_cpp\main.cpp
- quant_ops_system\src\scripts\basic_data\basic_financial_data.py
- quant_ops_system\src\scripts\basic_data\consensus_rating.py
- quant_ops_system\src\scripts\basic_data\consensus_rolling.py
- quant_ops_system\src\scripts\basic_data\derived_financial_metrics.py
- quant_ops_system\src\scripts\basic_data\st_stock_list.py
- quant_ops_system\src\scripts\data_monitor\ddb_monitor_count.py
- quant_ops_system\src\scripts\ddb_test_utils.py
- quant_ops_system\src\scripts\factor_data\cj_quant_factors.py
- quant_ops_system\src\scripts\factor_data\fz_quant_factors.py
- quant_ops_system\src\scripts\index_components_data\index_components_data.py
- quant_ops_system\src\scripts\market_data\ah_info.py
- quant_ops_system\src\scripts\market_data\compare_ddb_tables.py
- quant_ops_system\src\scripts\market_data\download_data_from_wind_wsd.py
- quant_ops_system\src\scripts\market_data\market_data_1w.py
- quant_ops_system\src\scripts\market_data\market_data_etf.py
- quant_ops_system\src\scripts\market_data\market_data_index.py
- quant_ops_system\src\scripts\market_data\market_data_stock.py
- quant_ops_system\src\scripts\my_utils\trade_date_pre.py
- quant_ops_system\src\scripts\vendor_basic_data\lib\accounts.py
- quant_ops_system\src\scripts\vendor_research_factors\data_api\ddb_data_api.py
- quant_ops_system\src\scripts\vendor_research_factors\data_api\hc_data_api.py
- quant_ops_system\src\scripts\vendor_research_factors\data_api\tl_data_api.py
- quant_ops_system\src\scripts\vendor_research_factors\data_api\vnpy_data_api.py
- quant_ops_system\src\scripts\vendor_research_factors\data_api\wdb_data_api.py
- quant_ops_system\src\scripts\vendor_research_factors\lib\accounts.py
- quant_ops_system\src\scripts\wind_utils\wind_code.py
- reference\API接口契约.md
- reference\用户与权限接口.md
- 数据库连接\dolphin\dolphin_db_connector.hpp
- 用户模块对接文档.md
- 运维设计方案.html

## Deleted Paths Or Types

- quant_ops_system\iguana\.git
- quant_ops_system\iguana\.git\logs
- quant_ops_system\iguana\data
- quant_ops_system\libhv\.git
- quant_ops_system\libhv\.git\logs
- quant_ops_system\ormpp\.git
- quant_ops_system\ormpp\.git\logs

## Sensitive Types Detected

- API/token-like values
- Username/account-like values
- Database password-like values
- Database/server host configuration
- DolphinDB connection configuration
- IPv4 addresses
- Domain/server-address-like values
- Business/trading/backtest/order/position-related wording

## Placeholders Used

- ${API_KEY}
- ${ACCESS_TOKEN}
- ${DB_USERNAME}
- ${DB_PASSWORD}
- ${DB_HOST}
- ${DB_PORT}
- ${DB_NAME}
- ${REDIS_HOST}
- ${REDIS_PORT}
- ${CLICKHOUSE_HOST}
- ${DOLPHINDB_HOST}
- ${SERVER_HOST}
- ${SERVER_PORT}
- 127.0.0.1
- example.com
- your_username
- your_password
- example_project

## Verification Results

- Verification was run locally only; no remote server, database, or external interface was accessed.
- See final task summary for syntax and format check result.

## Manual Review Remaining

- Third-party vendored source trees under quant_ops_system/libhv, quant_ops_system/iguana, and quant_ops_system/ormpp contain public examples and protocol/domain text. They were not rewritten broadly to avoid breaking vendored code.
- Business-domain source files should receive a human read-through before publishing because automated detection cannot fully distinguish domain terminology from sensitive strategy logic.

## Recommended Upload

- Sanitized source files, docs, examples, .gitignore, .env.example, config.example.yaml, and SANITIZATION_REPORT.md.

## Not Recommended Upload

- Any regenerated .env, logs, runtime directories, datasets, database dumps, binary model files, certificate/key files, nested .git metadata, cache folders, and private chat/session folders.

Additional strategy/factor directories removed after verification:
- quant_ops_system\src\scripts\vendor_research_factors
- quant_ops_system\src\scripts\factor_data


RQ/Ricequant license handling:
- RQ license and credential-like values replaced with RQ_LICENSE_KEY/RQ_USERNAME/RQ_PASSWORD placeholders.

- RQ init calls now read RQ_USERNAME, RQ_PASSWORD, and RQ_LICENSE_KEY from environment placeholders.

- DolphinDB defaults were normalized to host, port, username, and password placeholders.

- Remaining DolphinDB host, port, username, and password defaults were replaced with safe placeholders.

## Final Verification Update

- Python AST syntax check: passed for 49 project Python files, excluding vendored third-party trees.
- JSON format check: passed; no JSON files remained to parse in the sanitized project scope.
- YAML parse check: skipped because no local YAML parser was available and no dependency installation/network access was allowed.
- RQ/Ricequant license scan: no unplaceholdered license values detected.
- DolphinDB DDB legacy variable scan: no legacy DDB host, port, username, or password defaults remained in source files.
- Blocked upload artifact scan: no .env, key/certificate/token/log/data/database/model artifact files remained, except the intended .env.example.
- Residual focused risk: quant_ops_system/src/manager_cpp/main.cpp contains permission-check function names with admin terminology; reviewed as code identifiers, not credentials.