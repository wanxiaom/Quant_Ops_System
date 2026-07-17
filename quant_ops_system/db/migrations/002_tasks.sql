USE quant_ops;

-- 当前系统的全部调度任务。重复执行时更新为本文件定义的最终配置。
INSERT INTO tasks
    (task_id, name, node_type, script_path, cron_expr, timeout_sec, enabled, target_node_id, default_params)
VALUES
    ('task_market_stock_daily', 'Daily Market Data - Stock (All Frequencies)', 'python',
     'src/scripts/market_data/market_data_stock.py', '15 22 * * *', 7200, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_market_etf_daily', 'Daily Market Data - ETF', 'python',
     'src/scripts/market_data/market_data_etf.py', '15 22 * * *', 7200, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_market_index_daily', 'Daily Market Data - Index (All Frequencies)', 'python',
     'src/scripts/market_data/market_data_index.py', '15 22 * * *', 7200, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_market_data_weekly', 'Weekly Market Data - Stock & Index', 'python',
     'src/scripts/market_data/market_data_1w.py', '15 22 * * 0', 3600, 1, 'node_001', '{"dry_run":true}'),
    ('task_basic_financial_daily', 'Daily Financial Data (Balance, CashFlow, Income)', 'python',
     'src/scripts/basic_data/basic_financial_data.py', '', 7200, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_derived_metrics_daily', 'Derived Financial Metrics', 'python',
     'src/scripts/basic_data/derived_financial_metrics.py', '', 7200, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_consensus_rating_daily', 'Wind Consensus Rating', 'python',
     'src/scripts/basic_data/consensus_rating.py', '', 7200, 1, 'windows-worker-01', '{"force":true,"dry_run":true}'),
    ('task_consensus_rolling_daily', 'Wind Consensus Rolling Forecast', 'python',
     'src/scripts/basic_data/consensus_rolling.py', '', 7200, 1, 'windows-worker-01', '{"force":true,"dry_run":true}'),
    ('task_ah_info_daily', 'AH Premium Daily', 'python',
     'src/scripts/market_data/ah_info.py', '', 3600, 1, 'windows-worker-01', '{"force":true,"dry_run":true}'),
    ('task_stock_info_daily', 'Vendor Stock Info Daily', 'python',
     'src/scripts/vendor_basic_data/run_stock_info.py', '', 7200, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_cj_quant_factors_daily', 'CJ Quant Factors Daily', 'python',
     'src/scripts/factor_data/cj_quant_factors.py', '', 21600, 1, 'windows-worker-01', '{"force":true,"dry_run":true}'),
    ('task_fz_quant_factors_daily', 'FZ Quant Factors Daily', 'python',
     'src/scripts/factor_data/fz_quant_factors.py', '', 10800, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_research_factors_daily', 'Vendor Research Factors Daily', 'python',
     'src/scripts/vendor_research_factors/run_research_factors.py', '', 21600, 1, 'node_001', '{"force":true,"dry_run":true}'),
    ('task_asset_allocation_daily', 'Wind Asset Allocation Daily', 'python',
     'src/scripts/market_data/download_data_from_wind_wsd.py', '30 7 * * *', 7200, 1, 'windows-worker-01', '{"dry_run":true}'),
    ('task_st_stock_daily', 'Wind ST Stock List Daily', 'python',
     'src/scripts/basic_data/st_stock_list.py', '0 8 * * *', 1800, 1, 'windows-worker-01', '{"dry_run":true,"compare_db":true,"force":true,"output_dir":"C:/quant_data/DolphinDB"}'),
    ('task_index_components_daily', 'Ricequant Index Components Daily', 'python',
     'src/scripts/index_components_data/index_components_data.py', '30 6 * * *', 3600, 1, 'node_001', '{"force":true}'),
    ('task_data_monitor_count', 'DolphinDB Data Monitor Count', 'python',
     'src/scripts/data_monitor/ddb_monitor_count.py', '0 8 * * *', 7200, 1, 'node_001', '{"sync_monitors":false}')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    node_type = VALUES(node_type),
    script_path = VALUES(script_path),
    cron_expr = VALUES(cron_expr),
    timeout_sec = VALUES(timeout_sec),
    enabled = VALUES(enabled),
    target_node_id = VALUES(target_node_id),
    default_params = VALUES(default_params);

-- 清理旧迁移阶段出现过的主链路边，再写入当前最终 DAG。
DELETE FROM dag_edges
WHERE (parent_task_id = 'task_market_stock_daily' AND child_task_id = 'task_basic_financial_daily')
   OR (parent_task_id = 'task_basic_financial_daily' AND child_task_id = 'task_derived_metrics_daily')
   OR (parent_task_id = 'task_derived_metrics_daily' AND child_task_id = 'task_consensus_rating_daily')
   OR (parent_task_id = 'task_consensus_rating_daily' AND child_task_id = 'task_consensus_rolling_daily')
   OR (parent_task_id = 'task_consensus_rolling_daily' AND child_task_id = 'task_ah_info_daily')
   OR (parent_task_id = 'task_ah_info_daily' AND child_task_id = 'task_stock_info_daily')
   OR (parent_task_id = 'task_stock_info_daily' AND child_task_id = 'task_research_factors_daily')
   OR (parent_task_id = 'task_stock_info_daily' AND child_task_id = 'task_fz_quant_factors_daily')
   OR (parent_task_id = 'task_stock_info_daily' AND child_task_id = 'task_cj_quant_factors_daily')
   OR (parent_task_id = 'task_cj_quant_factors_daily' AND child_task_id = 'task_fz_quant_factors_daily')
   OR (parent_task_id = 'task_fz_quant_factors_daily' AND child_task_id = 'task_research_factors_daily');

INSERT INTO dag_edges (pipeline_id, parent_task_id, child_task_id) VALUES
    ('main_daily_dag', 'task_market_stock_daily', 'task_basic_financial_daily'),
    ('main_daily_dag', 'task_basic_financial_daily', 'task_derived_metrics_daily'),
    ('main_daily_dag', 'task_derived_metrics_daily', 'task_consensus_rating_daily'),
    ('main_daily_dag', 'task_consensus_rating_daily', 'task_consensus_rolling_daily'),
    ('main_daily_dag', 'task_consensus_rolling_daily', 'task_ah_info_daily'),
    ('main_daily_dag', 'task_ah_info_daily', 'task_stock_info_daily'),
    ('main_daily_dag', 'task_stock_info_daily', 'task_cj_quant_factors_daily'),
    ('main_daily_dag', 'task_cj_quant_factors_daily', 'task_fz_quant_factors_daily'),
    ('main_daily_dag', 'task_fz_quant_factors_daily', 'task_research_factors_daily');
