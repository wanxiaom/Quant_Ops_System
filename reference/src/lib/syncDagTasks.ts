import type { DagGraph } from '../types.js'
import type { Task } from '../types.js'

type DagStatus = 'success' | 'running' | 'waiting' | 'failed'

interface DagNodeLike {
  id: string
  name: string
  status: DagStatus
  agent: string
}

const scheduleStatusMap: Record<DagStatus, NonNullable<Task['schedule_status']>> = {
  success: 'success',
  running: 'running',
  waiting: 'waiting',
  failed: 'failed',
}

function resolveTargetOs(agent: string): 'linux' | 'windows' {
  const normalized = agent.toLowerCase()
  return normalized.includes('windows') || normalized.includes('win') ? 'windows' : 'linux'
}

function createTaskFromDagNode(node: DagNodeLike, parents: string[]): Task {
  const hasParents = parents.length > 0

  return {
    task_id: node.id,
    name: node.name,
    description: `所属 DAG 节点：${node.name}`,
    task_type: hasParents ? 'dag' : 'manual',
    node_type: 'python',
    script_path: `python src/scripts/${node.id}.py`,
    cron_expr: '',
    timeout_sec: 7200,
    enabled: 1,
    target_node_id: node.agent,
    target_os: resolveTargetOs(node.agent),
    target_node_tag: '',
    schedule_status: scheduleStatusMap[node.status],
    trigger_mode: hasParents ? 'dependency' : 'manual',
    created_at: '2026-06-20T09:00:00+08:00',
    retry_count: 1,
    retry_interval_sec: 300,
    concurrency_limit: 1,
    dag_parents: parents,
    env_vars: [],
    log_level: 'INFO',
    alert_on_failure: true,
  }
}

export function mergeTasksWithDagGraphs(baseTasks: Task[], graphs: DagGraph[]): Task[] {
  const taskMap = new Map(baseTasks.map((task) => [task.task_id, { ...task }]))

  for (const graph of graphs) {
    for (const node of graph.nodes) {
      const parents = [
        ...new Set(graph.edges.filter((edge) => edge.to === node.id).map((edge) => edge.from)),
      ]
      const existing = taskMap.get(node.id)

      if (existing) {
        taskMap.set(node.id, {
          ...existing,
          dag_parents: parents.length > 0 ? parents : existing.dag_parents || [],
          ...(parents.length > 0
            ? { task_type: 'dag' as const, trigger_mode: 'dependency' as const }
            : {}),
        })
        continue
      }

      taskMap.set(node.id, createTaskFromDagNode(node, parents))
    }
  }

  return Array.from(taskMap.values())
}
