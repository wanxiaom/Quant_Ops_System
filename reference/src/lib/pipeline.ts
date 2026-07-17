import type { DagGraphView, DagStatus, Pipeline, PipelineEdge, PipelineNode, Task } from '../types.js'

export function pipelineToSummary(pipeline: Pipeline) {
  return {
    pipeline_id: pipeline.pipeline_id,
    name: pipeline.name,
    description: pipeline.description,
    enabled: pipeline.enabled,
    task_count: pipeline.nodes.length,
    edge_count: pipeline.edges.length,
    updated_at: pipeline.updated_at,
  }
}

function defaultRuntime(task: Task) {
  const status = (task.schedule_status || 'waiting') as DagStatus

  return {
    task_id: task.task_id,
    name: task.name,
    status,
    agent: task.target_node_id || '-',
    latest_exec_id: status === 'waiting' ? '待触发' : `${task.task_id}_runtime`,
    duration: status === 'running' ? '运行中' : '-',
  }
}

export function pipelineToDagGraph(pipeline: Pipeline, tasks: Task[]): DagGraphView {
  const taskMap = new Map(tasks.map((task) => [task.task_id, task]))

  return {
    id: pipeline.pipeline_id,
    name: pipeline.name,
    description: pipeline.description,
    canvas: pipeline.canvas,
    nodes: pipeline.nodes.map((node) => {
      const task = taskMap.get(node.task_id)
      const runtime = task ? defaultRuntime(task) : undefined

      return {
        id: node.task_id,
        name: runtime?.name || task?.name || node.task_id,
        status: runtime?.status || 'waiting',
        x: node.x,
        y: node.y,
        latestExecId: runtime?.latest_exec_id || '待触发',
        agent: runtime?.agent || '-',
        duration: runtime?.duration || '-',
      }
    }),
    edges: pipeline.edges.map((edge) => ({ from: edge.from, to: edge.to })),
  }
}

export function projectPipelineEdgesToTaskParents(edges: PipelineEdge[]) {
  const parentMap = new Map<string, string[]>()

  edges.forEach((edge) => {
    const parents = parentMap.get(edge.to) || []
    if (!parents.includes(edge.from)) {
      parents.push(edge.from)
    }
    parentMap.set(edge.to, parents)
  })

  return parentMap
}

export function resolveEntryTaskIds(nodes: PipelineNode[], edges: PipelineEdge[]) {
  const childIds = new Set(edges.map((edge) => edge.to))
  return nodes.map((node) => node.task_id).filter((taskId) => !childIds.has(taskId))
}

export function applyPipelineToTasks(
  pipeline: Pick<Pipeline, 'nodes' | 'edges'>,
  tasks: Task[],
  options: {
    previousNodeIds?: string[]
    reservedNodeIds?: Set<string>
  } = {},
): Task[] {
  const nodeIds = new Set(pipeline.nodes.map((node) => node.task_id))
  const parentMap = projectPipelineEdgesToTaskParents(pipeline.edges)
  const previousNodeIds = options.previousNodeIds || []
  const reservedNodeIds = options.reservedNodeIds || new Set<string>()

  const removedNodeIds = new Set(
    previousNodeIds.filter((taskId) => !nodeIds.has(taskId) && !reservedNodeIds.has(taskId)),
  )

  return tasks.map((task) => {
    if (removedNodeIds.has(task.task_id)) {
      const fallbackType = task.cron_expr ? 'cron' : 'manual'
      return {
        ...task,
        dag_parents: [],
        task_type: fallbackType,
        trigger_mode: fallbackType,
      }
    }

    if (!nodeIds.has(task.task_id)) {
      return task
    }

    const parents = parentMap.get(task.task_id) || []
    const hasParents = parents.length > 0

    return {
      ...task,
      dag_parents: parents,
      ...(hasParents
        ? { task_type: 'dag' as const, trigger_mode: 'dependency' as const }
        : {}),
    }
  })
}

export function collectClearedTaskIds(
  pipeline: Pick<Pipeline, 'nodes'>,
  options: {
    previousNodeIds?: string[]
    reservedNodeIds?: Set<string>
  } = {},
) {
  const nodeIds = new Set(pipeline.nodes.map((node) => node.task_id))
  const previousNodeIds = options.previousNodeIds || []
  const reservedNodeIds = options.reservedNodeIds || new Set<string>()

  return previousNodeIds.filter((taskId) => !nodeIds.has(taskId) && !reservedNodeIds.has(taskId))
}

export function hasDependencyCycle(tasks: Task[], taskId: string, parents: string[]) {
  const parentMap = new Map(tasks.map((task) => [task.task_id, [...(task.dag_parents || [])]]))
  parentMap.set(taskId, parents)

  function visit(current: string, seen: Set<string>): boolean {
    if (current === taskId && seen.size > 0) {
      return true
    }

    if (seen.has(current)) {
      return false
    }

    seen.add(current)
    return (parentMap.get(current) || []).some((parent) => visit(parent, new Set(seen)))
  }

  return parents.some((parent) => visit(parent, new Set()))
}

export function validatePipeline(
  pipeline: Pick<Pipeline, 'nodes' | 'edges'>,
  tasks: Task[],
) {
  const errors: string[] = []
  const taskIds = new Set(tasks.map((task) => task.task_id))
  const nodeIds = pipeline.nodes.map((node) => node.task_id)
  const uniqueNodeIds = new Set(nodeIds)

  nodeIds.forEach((taskId) => {
    if (!taskIds.has(taskId)) {
      errors.push(`节点引用了不存在的任务：${taskId}`)
    }
  })

  if (uniqueNodeIds.size !== nodeIds.length) {
    errors.push('同一流水线中不能重复添加相同任务节点')
  }

  const edgeKeys = new Set<string>()
  pipeline.edges.forEach((edge) => {
    if (!uniqueNodeIds.has(edge.from) || !uniqueNodeIds.has(edge.to)) {
      errors.push(`边 ${edge.from} → ${edge.to} 引用了不在流水线中的节点`)
    }
    if (edge.from === edge.to) {
      errors.push(`任务 ${edge.from} 不能依赖自身`)
    }

    const key = `${edge.from}->${edge.to}`
    if (edgeKeys.has(key)) {
      errors.push(`存在重复依赖：${edge.from} → ${edge.to}`)
    }
    edgeKeys.add(key)
  })

  const parentMap = projectPipelineEdgesToTaskParents(pipeline.edges)
  parentMap.forEach((parents, taskId) => {
    if (hasDependencyCycle(tasks, taskId, parents)) {
      errors.push(`任务 ${taskId} 的依赖关系存在环路`)
    }
  })

  return { valid: errors.length === 0, errors }
}
