USE quant_ops;

SET @has_default_params := (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'tasks'
      AND COLUMN_NAME = 'default_params'
);
SET @ddl := IF(
    @has_default_params = 0,
    'ALTER TABLE tasks ADD COLUMN default_params TEXT COMMENT ''默认执行参数(JSON格式)，用于 Cron/手动触发生成 task_instances.params'' AFTER target_node_id',
    'SELECT 1'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE tasks SET default_params = '{"force":true,"dry_run":true}'
WHERE task_id IN (
    'task_market_stock_daily',
    'task_market_etf_daily',
    'task_market_index_daily',
    'task_basic_financial_daily',
    'task_derived_metrics_daily',
    'task_consensus_rating_daily',
    'task_consensus_rolling_daily',
    'task_ah_info_daily',
    'task_stock_info_daily',
    'task_cj_quant_factors_daily',
    'task_fz_quant_factors_daily',
    'task_research_factors_daily'
);

UPDATE tasks SET default_params = '{"dry_run":true}'
WHERE task_id IN (
    'task_market_data_weekly',
    'task_asset_allocation_daily'
);

UPDATE tasks SET default_params = '{"dry_run":true,"compare_db":true,"force":true,"output_dir":"C:/quant_data/DolphinDB"}'
WHERE task_id = 'task_st_stock_daily';

UPDATE tasks SET default_params = '{"force":true}'
WHERE task_id = 'task_index_components_daily';
