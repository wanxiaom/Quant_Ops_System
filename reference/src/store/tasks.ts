import type { CreateTaskPayload, Task, TaskExecution, TriggerTaskResponse } from '../types.js'
import { dagGraphs } from './dag.js'
import { mergeTasksWithDagGraphs } from '../lib/syncDagTasks.js'
import { logStore } from './logs.js'

const now = new Date('2026-06-30T09:30:00+08:00').getTime()
const minutesAgo = (minutes: number) => new Date(now - minutes * 60 * 1000).toISOString()

let tasks: Task[] = mergeTasksWithDagGraphs(
  [
  {
    task_id: 'task_market_stock_daily',
    name: '股票行情日频同步',
    description: '采集 A 股日频行情、复权因子和成交统计。',
    task_type: 'cron',
    node_type: 'python',
    script_path: 'python src/scripts/market_data/market_data_stock.py',
    cron_expr: '15 22 * * *',
    timeout_sec: 7200,
    enabled: 1,
    target_node_id: 'node_001',
    target_os: 'linux',
    target_node_tag: 'dolphindb-node',
    schedule_status: 'success',
    trigger_mode: 'cron',
    created_at: '2026-06-12T10:12:00+08:00',
    last_run_at: minutesAgo(58),
    retry_count: 2,
    retry_interval_sec: 300,
    concurrency_limit: 1,
    dag_parents: [],
    env_vars: [{ key: 'MARKET', value: 'CN' }],
    log_level: 'INFO',
    alert_on_failure: true,
  },
  {
    task_id: 'task_basic_financial_daily',
    name: '财务三表更新',
    description: '更新资产负债表、现金流量表、利润表。',
    task_type: 'dag',
    node_type: 'python',
    script_path: 'python src/scripts/basic_data/basic_financial_data.py',
    cron_expr: '',
    timeout_sec: 7200,
    enabled: 1,
    target_node_id: 'node_001',
    target_os: 'linux',
    target_node_tag: 'dolphindb-node',
    schedule_status: 'success',
    trigger_mode: 'dependency',
    created_at: '2026-06-13T11:20:00+08:00',
    last_run_at: minutesAgo(45),
    retry_count: 1,
    retry_interval_sec: 180,
    concurrency_limit: 1,
    dag_parents: ['task_market_stock_daily'],
    env_vars: [],
    log_level: 'INFO',
    alert_on_failure: true,
  },
  {
    task_id: 'task_consensus_rating_daily',
    name: 'Wind 一致预期评级',
    description: '拉取 Wind 终端依赖的一致预期评级数据。',
    task_type: 'manual',
    node_type: 'python',
    script_path: 'python src/scripts/basic_data/consensus_rating.py',
    cron_expr: '',
    timeout_sec: 7200,
    enabled: 1,
    target_node_id: 'windows-worker-01',
    target_os: 'windows',
    target_node_tag: 'wind-server',
    schedule_status: 'running',
    trigger_mode: 'manual',
    created_at: '2026-06-14T14:30:00+08:00',
    last_run_at: minutesAgo(8),
    retry_count: 1,
    retry_interval_sec: 300,
    concurrency_limit: 1,
    dag_parents: [],
    env_vars: [{ key: 'WIND_PROFILE', value: 'prod' }],
    log_level: 'INFO',
    alert_on_failure: true,
  },
  {
    task_id: 'task_cj_quant_factors_daily',
    name: '长江因子入库',
    description: '处理长江证券量化因子文件并写入因子库。',
    task_type: 'dag',
    node_type: 'python',
    script_path: 'python src/scripts/factor_data/cj_quant_factors.py',
    cron_expr: '',
    timeout_sec: 21600,
    enabled: 1,
    target_node_id: 'windows-worker-01',
    target_os: 'windows',
    target_node_tag: 'wind-server',
    schedule_status: 'waiting',
    trigger_mode: 'dependency',
    created_at: '2026-06-15T09:45:00+08:00',
    last_run_at: minutesAgo(1440),
    retry_count: 2,
    retry_interval_sec: 600,
    concurrency_limit: 1,
    dag_parents: ['task_consensus_rating_daily'],
    env_vars: [],
    log_level: 'WARN',
    alert_on_failure: true,
  },
  {
    task_id: 'task_research_factors_daily',
    name: '投研因子供应商同步',
    description: '同步外部投研因子并生成质量校验报告。',
    task_type: 'cron',
    node_type: 'python',
    script_path: 'python src/scripts/vendor_research_factors/run_research_factors.py',
    cron_expr: '30 6 * * 1-5',
    timeout_sec: 21600,
    enabled: 0,
    target_node_id: 'node_002',
    target_os: 'linux',
    target_node_tag: 'factor-node',
    schedule_status: 'failed',
    trigger_mode: 'cron',
    created_at: '2026-06-16T16:05:00+08:00',
    last_run_at: minutesAgo(210),
    retry_count: 3,
    retry_interval_sec: 600,
    concurrency_limit: 1,
    dag_parents: [],
    env_vars: [{ key: 'QUALITY_GATE', value: 'strict' }],
    log_level: 'ERROR',
    alert_on_failure: true,
  },
  ],
  dagGraphs,
)

let executions: TaskExecution[] = [
  {
    exec_id: 'exec_market_20260630_2230',
    task_id: 'task_market_stock_daily',
    node_id: 'node_001',
    status: 1,
    exit_code: 0,
    start_time: minutesAgo(80),
    end_time: minutesAgo(58),
    log_path: '/logs/exec_market_20260630_2230.log',
    params: '{"trigger":"cron"}',
    trigger_mode: 'cron',
    stdout: '行情文件校验完成\n写入 bars_daily 4,982,120 rows\n任务成功',
    stderr: '',
  },
  {
    exec_id: 'exec_consensus_20260630_0922',
    task_id: 'task_consensus_rating_daily',
    node_id: 'windows-worker-01',
    status: 0,
    exit_code: null,
    start_time: minutesAgo(8),
    end_time: null,
    log_path: '/logs/exec_consensus_20260630_0922.log',
    params: '{"trigger":"manual"}',
    trigger_mode: 'manual',
    stdout: '连接 Wind 终端成功\n读取一致预期评级批次 20260630\n正在写入 staging 表...',
    stderr: '',
  },
  {
    exec_id: 'exec_financial_20260630_2245',
    task_id: 'task_basic_financial_daily',
    node_id: 'node_001',
    status: 1,
    exit_code: 0,
    start_time: minutesAgo(57),
    end_time: minutesAgo(45),
    log_path: '/logs/exec_financial_20260630_2245.log',
    params: '{"trigger":"dependency","parent":"task_market_stock_daily"}',
    trigger_mode: 'dependency',
    stdout: '上游 task_market_stock_daily 已成功\n同步财务三表 1,284,331 rows\n依赖任务完成',
    stderr: '',
  },
  {
    exec_id: 'exec_research_20260630_0600',
    task_id: 'task_research_factors_daily',
    node_id: 'node_002',
    status: -1,
    exit_code: 2,
    start_time: minutesAgo(240),
    end_time: minutesAgo(210),
    log_path: '/logs/exec_research_20260630_0600.log',
    params: '{"trigger":"cron"}',
    trigger_mode: 'cron',
    stdout: '下载供应商清单完成\n开始校验字段映射',
    stderr: 'ERROR: missing required column factor_value_adj',
  },
]

function normalizeTask(payload: CreateTaskPayload, source?: Task): Task {
  const taskType = payload.task_type ?? source?.task_type ?? (payload.cron_expr ? 'cron' : 'manual')
  const triggerMode =
    payload.trigger_mode ?? source?.trigger_mode ?? (taskType === 'dag' ? 'dependency' : taskType)

  return {
    task_id: payload.task_id ?? source?.task_id ?? `task_${Date.now()}`,
    name: payload.name ?? source?.name ?? '未命名任务',
    description: payload.description ?? source?.description ?? '',
    node_type: payload.node_type ?? source?.node_type ?? 'python',
    script_path: payload.script_path ?? source?.script_path ?? '',
    cron_expr: payload.cron_expr ?? source?.cron_expr ?? '',
    timeout_sec: payload.timeout_sec ?? source?.timeout_sec ?? 3600,
    enabled: payload.enabled ?? source?.enabled ?? 1,
    target_node_id: payload.target_node_id ?? source?.target_node_id ?? '',
    task_type: taskType,
    target_os: payload.target_os ?? source?.target_os ?? 'linux',
    target_node_tag: payload.target_node_tag ?? source?.target_node_tag ?? '',
    schedule_status: payload.schedule_status ?? source?.schedule_status ?? 'waiting',
    trigger_mode: triggerMode,
    created_at: source?.created_at ?? new Date().toISOString(),
    last_run_at: payload.last_run_at ?? source?.last_run_at,
    retry_count: payload.retry_count ?? source?.retry_count ?? 0,
    retry_interval_sec: payload.retry_interval_sec ?? source?.retry_interval_sec ?? 300,
    concurrency_limit: payload.concurrency_limit ?? source?.concurrency_limit ?? 1,
    dag_parents: payload.dag_parents ?? source?.dag_parents ?? [],
    env_vars: payload.env_vars ?? source?.env_vars ?? [],
    log_level: payload.log_level ?? source?.log_level ?? 'INFO',
    alert_on_failure: payload.alert_on_failure ?? source?.alert_on_failure ?? true,
  }
}

function createExecId(taskId: string, trigger: 'manual' | 'dependency' | 'cron') {
  return `${taskId}_${trigger}_${Math.floor(Date.now() / 1000)}`
}

function createRandomResult(): {
  scheduleStatus: NonNullable<Task['schedule_status']>
  executionStatus: TaskExecution['status']
  exitCode: number
} {
  const values = [
    { scheduleStatus: 'success', executionStatus: 1, exitCode: 0 },
    { scheduleStatus: 'failed', executionStatus: -1, exitCode: 1 },
    { scheduleStatus: 'timeout', executionStatus: -2, exitCode: 124 },
  ] as const

  return values[Math.floor(Math.random() * values.length)]
}

function createTriggerResponse(taskId: string, trigger: 'manual' | 'dependency' | 'cron'): TriggerTaskResponse {
  const execId = createExecId(taskId, trigger)
  const task = tasks.find((item) => item.task_id === taskId)

  if (task) {
    const startTime = new Date(Date.now() - 2500).toISOString()
    const endTime = new Date().toISOString()
    const { scheduleStatus, executionStatus, exitCode } = createRandomResult()
    const failed = scheduleStatus === 'failed'
    const timeout = scheduleStatus === 'timeout'
    const execution: TaskExecution = {
      exec_id: execId,
      task_id: taskId,
      node_id: task.target_node_id,
      status: executionStatus,
      exit_code: exitCode,
      start_time: startTime,
      end_time: endTime,
      log_path: `/logs/${execId}.log`,
      params: JSON.stringify({ trigger }),
      trigger_mode: trigger,
      stdout: [
        `任务 ${task.name} 已派发到 ${task.target_node_id}`,
        `执行命令 ${task.script_path}`,
        timeout ? '任务执行超时，已停止等待 Agent 回调' : failed ? '任务执行失败，等待人工排查' : '任务执行成功，exit_code=0',
      ].join('\n'),
      stderr: timeout
        ? `TIMEOUT: ${task.name} 超过演示阈值，模拟返回 exit_code=124`
        : failed
          ? `ERROR: ${task.name} 执行失败，模拟返回 exit_code=1`
          : '',
    }
    const nextTask: Task = {
      ...task,
      schedule_status: scheduleStatus,
      last_run_at: endTime,
    }

    tasks = tasks.map((item) =>
      item.task_id === taskId
        ? nextTask
        : item,
    )

    executions = [execution, ...executions]
    logStore.appendExecutionLog(nextTask, execution)

    return {
      code: 0,
      message: 'Mock task executed',
      exec_id: execId,
      execution,
    }
  }

  return {
    code: 0,
    message: 'Mock task executed',
    exec_id: execId,
  }
}

export const taskStore = {
  list() {
    return [...tasks]
  },

  detail(taskId: string) {
    return tasks.find((task) => task.task_id === taskId)
  },

  create(payload: CreateTaskPayload) {
    const task = normalizeTask(payload)
    tasks = [task, ...tasks.filter((item) => item.task_id !== task.task_id)]
  },

  update(taskId: string, payload: CreateTaskPayload) {
    tasks = tasks.map((task) => (task.task_id === taskId ? normalizeTask(payload, task) : task))
  },

  remove(taskId: string) {
    tasks = tasks.filter((task) => task.task_id !== taskId)
    executions = executions.filter((execution) => execution.task_id !== taskId)
  },

  run(taskId: string) {
    return createTriggerResponse(taskId, 'manual')
  },

  runIncremental(taskId: string) {
    return createTriggerResponse(taskId, 'dependency')
  },

  pause(taskId: string) {
    tasks = tasks.map((task) => (task.task_id === taskId ? { ...task, enabled: 0 } : task))
  },

  resume(taskId: string) {
    tasks = tasks.map((task) => (task.task_id === taskId ? { ...task, enabled: 1 } : task))
  },

  executions(taskId: string) {
    return executions.filter((execution) => execution.task_id === taskId)
  },

  allExecutions() {
    return [...executions]
  },

  replaceAll(nextTasks: Task[]) {
    tasks = [...nextTasks]
  },
}
