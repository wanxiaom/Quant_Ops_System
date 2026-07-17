-- 数据监控行数波动异常检测配置
-- anomaly_threshold_pct = 0 表示按频率使用默认阈值：日频 30%，周频 40%，月频 50%。
-- anomaly_window_size 表示历史基线窗口，anomaly_min_samples 表示最少有效样本数。

SET @db_name := DATABASE();

SET @sql := (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE ddb_monitor ADD COLUMN anomaly_check_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT ''是否启用行数波动异常检测'' AFTER enabled',
    'SELECT 1'
  )
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = @db_name
    AND TABLE_NAME = 'ddb_monitor'
    AND COLUMN_NAME = 'anomaly_check_enabled'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE ddb_monitor ADD COLUMN anomaly_threshold_pct DECIMAL(8,4) NOT NULL DEFAULT 0 COMMENT ''行数波动异常阈值，0表示按频率默认'' AFTER anomaly_check_enabled',
    'SELECT 1'
  )
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = @db_name
    AND TABLE_NAME = 'ddb_monitor'
    AND COLUMN_NAME = 'anomaly_threshold_pct'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE ddb_monitor ADD COLUMN anomaly_window_size INT NOT NULL DEFAULT 20 COMMENT ''行数波动基线历史窗口'' AFTER anomaly_threshold_pct',
    'SELECT 1'
  )
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = @db_name
    AND TABLE_NAME = 'ddb_monitor'
    AND COLUMN_NAME = 'anomaly_window_size'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE ddb_monitor ADD COLUMN anomaly_min_samples INT NOT NULL DEFAULT 5 COMMENT ''行数波动基线最少样本数'' AFTER anomaly_window_size',
    'SELECT 1'
  )
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = @db_name
    AND TABLE_NAME = 'ddb_monitor'
    AND COLUMN_NAME = 'anomaly_min_samples'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE ddb_monitor
SET anomaly_check_enabled = COALESCE(anomaly_check_enabled, 1),
    anomaly_threshold_pct = CASE WHEN anomaly_threshold_pct IS NULL OR anomaly_threshold_pct < 0 THEN 0 ELSE anomaly_threshold_pct END,
    anomaly_window_size = CASE WHEN anomaly_window_size IS NULL OR anomaly_window_size <= 0 THEN 20 ELSE anomaly_window_size END,
    anomaly_min_samples = CASE WHEN anomaly_min_samples IS NULL OR anomaly_min_samples <= 0 THEN 5 ELSE anomaly_min_samples END;
