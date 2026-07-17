import type { LogDetail, LogEntry, LogLevel, LogSearchParams, LogStatus, Task, TaskExecution } from '../types.js'

let logDetails: LogDetail[] = [
  {
    log_id: 'log_market_20260630_2230',
    exec_id: 'exec_market_20260630_2230',
    task_id: 'task_market_stock_daily',
    task_name: '股票行情日频同步',
    agent_id: 'node_001',
    level: 'INFO',
    time: '2026-06-30 08:10:22',
    status: 'success',
    stdout: [
      '[08:10:01] pull market calendar: trade day confirmed',
      '[08:10:05] fetch A-share quotes: 5384 rows',
      '[08:10:16] write factor snapshot: completed',
      '[08:10:22] task finished with exit_code=0',
    ].join('\n'),
    stderr: '',
  },
  {
    log_id: 'log_financial_20260630_2245',
    exec_id: 'exec_financial_20260630_2245',
    task_id: 'task_basic_financial_daily',
    task_name: '财务三表更新',
    agent_id: 'node_001',
    level: 'INFO',
    time: '2026-06-30 08:33:19',
    status: 'success',
    stdout: [
      '[08:21:00] load balance sheet delta',
      '[08:27:44] cash flow statement synchronized',
      '[08:33:19] income statement synchronized, affected=214',
    ].join('\n'),
    stderr: '',
  },
  {
    log_id: 'log_consensus_20260630_0922',
    exec_id: 'exec_consensus_20260630_0922',
    task_id: 'task_consensus_rating_daily',
    task_name: 'Wind 一致预期评级',
    agent_id: 'windows-worker-01',
    level: 'WARN',
    time: '2026-06-30 09:22:41',
    status: 'running',
    stdout: [
      '[09:22:02] connect Wind terminal',
      '[09:22:18] load consensus rating batch: 20260630',
      '[09:22:41] writing staging table',
    ].join('\n'),
    stderr: '[09:22:41] Wind terminal response is slower than baseline, keep waiting',
  },
  {
    log_id: 'log_cj_quant_20260629_0930',
    exec_id: 'exec_cj_quant_20260629_0930',
    task_id: 'task_cj_quant_factors_daily',
    task_name: '长江因子入库',
    agent_id: 'windows-worker-01',
    level: 'ERROR',
    time: '2026-06-29 09:30:15',
    status: 'failed',
    stdout: [
      '[09:29:59] load vendor factor package',
      '[09:30:10] validate factor schema',
    ].join('\n'),
    stderr: [
      '[09:30:15] ValueError: missing required column factor_code',
      '[09:30:15] task failed with exit_code=1',
    ].join('\n'),
  },
  {
    log_id: 'log_research_20260630_0600',
    exec_id: 'exec_research_20260630_0600',
    task_id: 'task_research_factors_daily',
    task_name: '投研因子供应商同步',
    agent_id: 'node_002',
    level: 'WARN',
    time: '2026-06-30 06:00:00',
    status: 'timeout',
    stdout: [
      '[05:50:00] download vendor factor manifest',
      '[05:56:31] compare quality baseline',
      '[05:59:50] waiting external vendor response',
    ].join('\n'),
    stderr: '[06:00:00] timeout after 720 seconds',
  },
]

function createEntry(detail: LogDetail): LogEntry {
  return {
    log_id: detail.log_id,
    exec_id: detail.exec_id,
    task_id: detail.task_id,
    task_name: detail.task_name,
    agent_id: detail.agent_id,
    level: detail.level,
    time: detail.time,
    status: detail.status,
  }
}

function matchesKeyword(detail: LogDetail, keyword?: string) {
  if (!keyword) {
    return true
  }

  const normalized = keyword.toLowerCase()
  return [detail.exec_id, detail.task_id, detail.task_name, detail.agent_id, detail.stdout, detail.stderr].some((item) =>
    item.toLowerCase().includes(normalized),
  )
}

function matchesDateRange(detail: LogDetail, range?: string[]) {
  if (!range || range.length !== 2) {
    return true
  }

  const date = detail.time.slice(0, 10)
  return date >= range[0] && date <= range[1]
}

function buildDownloadText(detail: LogDetail) {
  return [
    `exec_id=${detail.exec_id}`,
    `task=${detail.task_name}`,
    `agent=${detail.agent_id}`,
    `status=${detail.status}`,
    `time=${detail.time}`,
    '',
    '[stdout]',
    detail.stdout || '(empty)',
    '',
    '[stderr]',
    detail.stderr || '(empty)',
  ].join('\n')
}

function formatLogTime(value?: string | null) {
  if (!value) {
    return new Date().toISOString().slice(0, 19).replace('T', ' ')
  }

  return value.slice(0, 19).replace('T', ' ')
}

function toLogStatus(status: TaskExecution['status']): LogStatus {
  if (status === 0) {
    return 'running'
  }

  if (status === -2) {
    return 'timeout'
  }

  if (status === -1) {
    return 'failed'
  }

  return 'success'
}

function toLogLevel(task: Task, execution: TaskExecution): LogLevel {
  if (execution.stderr) {
    return 'ERROR'
  }

  return task.log_level === 'WARN' || task.log_level === 'ERROR' ? task.log_level : 'INFO'
}

function buildExecutionStdout(task: Task, execution: TaskExecution) {
  return [
    `任务 ${task.name} 已派发到 ${execution.node_id || task.target_node_id || 'unknown-node'}`,
    `执行命令 ${task.script_path}`,
    execution.status === 1 ? '任务执行成功，exit_code=0' : '任务执行结束，详见 stderr',
  ].join('\n')
}

function buildExecutionStderr(task: Task, execution: TaskExecution) {
  if (execution.status !== -1 && execution.status !== -2) {
    return ''
  }

  return [
    `ERROR: 任务 ${task.name} 执行失败`,
    `exit_code=${execution.exit_code ?? 1}`,
    execution.status === -2 ? '执行超时，已触发告警检查' : '模拟执行返回失败，用于联调日志链路',
  ].join('\n')
}

export const logStore = {
  search(params: LogSearchParams = {}) {
    return logDetails
      .filter((detail) => matchesKeyword(detail, params.keyword))
      .filter((detail) => !params.task_name || detail.task_name.includes(params.task_name) || detail.task_id.includes(params.task_name))
      .filter((detail) => !params.agent_id || detail.agent_id.includes(params.agent_id))
      .filter((detail) => !params.level || detail.level === params.level)
      .filter((detail) => !params.status || detail.status === params.status)
      .filter((detail) => matchesDateRange(detail, params.date_range))
      .map(createEntry)
  },

  detail(logId: string) {
    const detail = logDetails.find((item) => item.log_id === logId)
    if (!detail) {
      const error = new Error('Log not found') as Error & { status?: number }
      error.status = 404
      throw error
    }
    return { ...detail }
  },

  download(logId: string) {
    return buildDownloadText(logStore.detail(logId))
  },

  appendExecutionLog(task: Task, execution: TaskExecution) {
    const detail: LogDetail = {
      log_id: `log_${execution.exec_id}`,
      exec_id: execution.exec_id,
      task_id: task.task_id,
      task_name: task.name,
      agent_id: execution.node_id || task.target_node_id || '',
      level: toLogLevel(task, execution),
      time: formatLogTime(execution.end_time || execution.start_time),
      status: toLogStatus(execution.status),
      stdout: execution.stdout || buildExecutionStdout(task, execution),
      stderr: execution.stderr || buildExecutionStderr(task, execution),
    }
    const entry = createEntry(detail)

    logDetails = [detail, ...logDetails.filter((item) => item.exec_id !== execution.exec_id)]
    return {
      detail: { ...detail },
      entry: { ...entry },
    }
  },
}
