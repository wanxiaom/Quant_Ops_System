import type { Agent, DashboardMetrics, Task, TaskExecution } from '../types.js'

function formatDisplayTime(iso: string) {
  const date = new Date(iso)
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}

function sameCalendarDay(left: Date, right: Date) {
  return (
    left.getFullYear() === right.getFullYear() &&
    left.getMonth() === right.getMonth() &&
    left.getDate() === right.getDate()
  )
}

function getReferenceDate(executions: TaskExecution[], tasks: Task[]) {
  const timestamps = [
    ...executions.map((execution) => execution.start_time).filter((value): value is string => Boolean(value)),
    ...tasks.map((task) => task.last_run_at).filter((value): value is string => Boolean(value)),
  ].map((value) => new Date(value).getTime())

  if (!timestamps.length) {
    return new Date()
  }

  return new Date(Math.max(...timestamps))
}

function buildTaskTrend(executions: TaskExecution[], referenceDate: Date) {
  const trend: DashboardMetrics['taskTrend'] = []

  for (let offset = 6; offset >= 0; offset -= 1) {
    const day = new Date(referenceDate)
    day.setDate(day.getDate() - offset)

    const dayExecutions = executions.filter(
      (execution) => execution.start_time && sameCalendarDay(new Date(execution.start_time), day),
    )

    const pad = (value: number) => String(value).padStart(2, '0')
    trend.push({
      date: `${pad(day.getMonth() + 1)}-${pad(day.getDate())}`,
      total: dayExecutions.length,
      success: dayExecutions.filter((execution) => execution.status === 1).length,
    })
  }

  return trend
}

export function buildDashboardMetrics(
  agents: Agent[],
  tasks: Task[],
  executions: TaskExecution[],
): DashboardMetrics {
  const agentHealth = {
    online: agents.filter((agent) => agent.status === 'online').length,
    busy: agents.filter((agent) => agent.status === 'busy').length,
    offline: agents.filter((agent) => agent.status === 'offline').length,
    error: agents.filter((agent) => agent.status === 'error').length,
  }

  const referenceDate = getReferenceDate(executions, tasks)
  const todayExecutions = executions.filter(
    (execution) => execution.start_time && sameCalendarDay(new Date(execution.start_time), referenceDate),
  )
  const todayTasks = todayExecutions.length
  const todaySuccessRate = todayTasks
    ? Math.round((todayExecutions.filter((execution) => execution.status === 1).length / todayTasks) * 100)
    : 100

  const dagTasks = tasks.filter((task) => task.task_type === 'dag')
  const completedExecutions = executions.filter(
    (execution) => execution.start_time && execution.end_time && execution.status === 1,
  )
  const avgDuration =
    completedExecutions.length > 0
      ? Math.round(
          completedExecutions.reduce((sum, execution) => {
            const start = new Date(execution.start_time!).getTime()
            const end = new Date(execution.end_time!).getTime()
            return sum + (end - start) / 1000
          }, 0) / completedExecutions.length,
        )
      : 0

  const recentErrors = tasks
    .filter((task) => task.schedule_status === 'failed' || task.schedule_status === 'timeout')
    .sort(
      (left, right) =>
        new Date(right.last_run_at || 0).getTime() - new Date(left.last_run_at || 0).getTime(),
    )
    .slice(0, 10)
    .map((task) => ({
      task_id: task.task_id,
      task_name: task.name,
      time: task.last_run_at ? formatDisplayTime(task.last_run_at) : '-',
      status: task.schedule_status as 'failed' | 'timeout',
    }))

  return {
    onlineAgents: agentHealth.online,
    totalAgents: agents.length,
    todayTasks,
    todaySuccessRate,
    runningDags: dagTasks.filter((task) => task.schedule_status === 'running').length,
    completedDags: dagTasks.filter((task) => task.schedule_status === 'success').length,
    avgDuration,
    agentHealth,
    platformDist: [
      { os: 'Linux', count: agents.filter((agent) => agent.os_type === 'linux').length },
      { os: 'Windows', count: agents.filter((agent) => agent.os_type === 'windows').length },
    ],
    taskTrend: buildTaskTrend(executions, referenceDate),
    recentErrors,
  }
}
