import { buildDashboardMetrics } from '../lib/buildDashboardMetrics.js'
import { agentStore } from './agents.js'
import { taskStore } from './tasks.js'

export function getDashboardMetrics() {
  return buildDashboardMetrics(agentStore.list(), taskStore.list(), taskStore.allExecutions())
}
