-- Add business category for task management.
USE quant_ops;

SET @has_task_category := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'tasks'
    AND COLUMN_NAME = 'task_category'
);

SET @sql := IF(
  @has_task_category = 0,
  'ALTER TABLE tasks ADD COLUMN task_category VARCHAR(32) NOT NULL DEFAULT ''ops'' COMMENT ''任务业务类型: ops/data_download/factor_compute/model_training'' AFTER node_type',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE tasks
SET task_category = 'ops'
WHERE task_category IS NULL OR task_category = '';

UPDATE tasks
SET task_category = 'data_download'
WHERE task_id IN (
  'task_market_stock_daily',
  'task_market_index_daily',
  'task_market_etf_daily',
  'task_basic_financial_daily',
  'task_stock_info_daily',
  'task_ah_info_daily',
  'task_st_stock_daily',
  'task_index_components_daily',
  'task_asset_allocation_daily'
);

UPDATE tasks
SET task_category = 'factor_compute'
WHERE task_id IN (
  'task_derived_metrics_daily',
  'task_consensus_rating_daily',
  'task_consensus_rolling_daily',
  'task_cj_quant_factors_daily',
  'task_fz_quant_factors_daily',
  'task_research_factors_daily'
);

UPDATE tasks
SET task_category = 'ops'
WHERE task_id LIKE 'task_test_%'
   OR task_id = 'task_data_monitor_count';
