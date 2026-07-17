-- Add data frequency to DolphinDB monitor config.
USE quant_ops;

SET @has_frequency := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'ddb_monitor'
    AND COLUMN_NAME = 'frequency'
);

SET @sql := IF(
  @has_frequency = 0,
  'ALTER TABLE ddb_monitor ADD COLUMN frequency VARCHAR(16) NOT NULL DEFAULT ''daily'' COMMENT ''数据频率: daily/weekly/monthly'' AFTER date_format',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE ddb_monitor
SET frequency = 'daily'
WHERE frequency IS NULL OR frequency = '';
