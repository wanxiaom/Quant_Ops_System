USE quant_ops;

-- Remove the historical main_daily_dag -> default compatibility mapping.
DELETE legacy
FROM dag_edges AS legacy
JOIN dag_edges AS current_edge
  ON current_edge.pipeline_id = 'main_daily_dag'
 AND current_edge.parent_task_id = legacy.parent_task_id
 AND current_edge.child_task_id = legacy.child_task_id
WHERE legacy.pipeline_id IN ('default', '');

UPDATE dag_edges
SET pipeline_id = 'main_daily_dag'
WHERE pipeline_id IN ('default', '');

UPDATE pipelines
SET entry_task_ids = '["task_market_stock_daily"]'
WHERE pipeline_id = 'main_daily_dag'
  AND (entry_task_ids IS NULL OR entry_task_ids = '' OR entry_task_ids = '[]');

-- parent_task_id needs its own index after pipeline_id becomes the first PK column.
SET @has_parent_index = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'dag_edges'
      AND index_name = 'idx_dag_edges_parent'
);
SET @add_parent_index_sql = IF(
    @has_parent_index = 0,
    'ALTER TABLE dag_edges ADD INDEX idx_dag_edges_parent (parent_task_id)',
    'SELECT 1'
);
PREPARE add_parent_index_stmt FROM @add_parent_index_sql;
EXECUTE add_parent_index_stmt;
DEALLOCATE PREPARE add_parent_index_stmt;

ALTER TABLE dag_edges
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (pipeline_id, parent_task_id, child_task_id),
    MODIFY pipeline_id VARCHAR(64) NOT NULL;

ALTER TABLE dag_edges ALTER COLUMN pipeline_id DROP DEFAULT;
