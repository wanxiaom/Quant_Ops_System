-- 创建量化运维系统数据库
CREATE DATABASE IF NOT EXISTS quant_ops CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE quant_ops;

-- 1. Agent 节点注册表
-- 记录分布式节点的健康状态与资源使用情况
CREATE TABLE IF NOT EXISTS agents (
    node_id VARCHAR(64) PRIMARY KEY COMMENT '节点唯一标识',
    ip VARCHAR(64) COMMENT '节点IP地址',
    os_type VARCHAR(32) COMMENT '操作系统类型',
    status VARCHAR(16) DEFAULT 'offline' COMMENT '节点状态: online/offline',
    last_heartbeat DATETIME COMMENT '最后一次心跳时间',
    cpu_load FLOAT COMMENT 'CPU使用率',
    mem_usage FLOAT COMMENT '内存使用率',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '首次注册时间'
);

-- 2. 调度任务配置表
-- 记录需要执行的 ETL 脚本与计算任务元信息
CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(64) PRIMARY KEY COMMENT '任务唯一标识',
    name VARCHAR(128) NOT NULL COMMENT '任务名称',
    node_type VARCHAR(32) DEFAULT 'python' COMMENT '任务运行环境(python/shell/c++)',
    task_category VARCHAR(32) NOT NULL DEFAULT 'ops' COMMENT '任务业务类型: ops/data_download/factor_compute/model_training',
    script_path VARCHAR(256) NOT NULL COMMENT '脚本的绝对或相对路径',
    cron_expr VARCHAR(64) COMMENT 'Cron 定时表达式',
    timeout_sec INT DEFAULT 3600 COMMENT '执行超时时间(秒)',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用(1:是, 0:否)',
    target_node_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '指定执行节点；空字符串表示公共队列',
    default_params TEXT COMMENT '默认执行参数(JSON格式)，用于 Cron/手动触发生成 task_instances.params',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 3. DAG 任务依赖表
-- 描述任务间的先后触发关系
CREATE TABLE IF NOT EXISTS dag_edges (
    pipeline_id VARCHAR(64) NOT NULL COMMENT '所属 Pipeline ID',
    parent_task_id VARCHAR(64) NOT NULL COMMENT '前置任务ID',
    child_task_id VARCHAR(64) NOT NULL COMMENT '后置(下游)任务ID',
    PRIMARY KEY (pipeline_id, parent_task_id, child_task_id),
    KEY idx_dag_edges_parent (parent_task_id),
    KEY idx_dag_edges_child (child_task_id),
    FOREIGN KEY (parent_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (child_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

-- 4. 任务执行实例表
-- Agent 拉取并上报结果的载体
CREATE TABLE IF NOT EXISTS task_instances (
    exec_id VARCHAR(64) PRIMARY KEY COMMENT '执行批次UUID',
    task_id VARCHAR(64) NOT NULL COMMENT '关联的任务ID',
    node_id VARCHAR(64) COMMENT '被分配执行的节点ID',
    status INT DEFAULT 2 COMMENT '状态: 2=Ready(待派发), 0=Running(运行中), 1=Success(成功), -1=Failed(失败), -2=Timeout(超时)',
    exit_code INT COMMENT '进程退出码',
    start_time DATETIME NULL COMMENT '实际开始时间',
    end_time DATETIME NULL COMMENT '实际结束时间',
    log_path VARCHAR(256) COMMENT '日志落地存储路径',
    params TEXT COMMENT '任务执行参数(JSON格式)',
    pipeline_id VARCHAR(64) NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);