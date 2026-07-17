import {
  applyPipelineToTasks,
  collectClearedTaskIds,
  pipelineToDagGraph,
  pipelineToSummary,
  resolveEntryTaskIds,
  validatePipeline,
} from '../lib/pipeline.js'
import { dagGraphs } from './dag.js'
import { taskStore } from './tasks.js'
import type { DagGraphView, Pipeline, PublishPipelinePayload, PublishPipelineResult, SavePipelinePayload } from '../types.js'

function formatNow() {
  const now = new Date()
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
}

function graphToPipeline(graph: (typeof dagGraphs)[number]): Pipeline {
  const timestamp = '2026-06-30 10:00:00'
  const nodes = graph.nodes.map((node) => ({
    task_id: node.id as string,
    x: node.x as number,
    y: node.y as number,
  }))
  const edges = graph.edges.map((edge) => ({ from: edge.from, to: edge.to }))

  return {
    pipeline_id: graph.id,
    name: graph.name,
    description: graph.description,
    enabled: true,
    canvas: graph.canvas,
    nodes,
    edges,
    entry_task_ids: resolveEntryTaskIds(nodes, edges),
    created_at: timestamp,
    updated_at: timestamp,
  }
}

let pipelines: Pipeline[] = dagGraphs.map(graphToPipeline)

function assertValid(pipeline: Pick<Pipeline, 'nodes' | 'edges'>) {
  const result = validatePipeline(pipeline, taskStore.list())
  if (!result.valid) {
    const error = new Error(result.errors.join('；')) as Error & { status?: number }
    error.status = 400
    throw error
  }
}

export const pipelineStore = {
  list() {
    return pipelines.map(pipelineToSummary)
  },

  listEnabled() {
    return pipelines.filter((pipeline) => pipeline.enabled)
  },

  detail(pipelineId: string) {
    const pipeline = pipelines.find((item) => item.pipeline_id === pipelineId)
    return pipeline ? { ...pipeline } : undefined
  },

  create(payload: SavePipelinePayload) {
    assertValid(payload)

    const timestamp = formatNow()
    const pipeline: Pipeline = {
      pipeline_id: `pipeline_${Date.now()}`,
      name: payload.name,
      description: payload.description || '',
      enabled: payload.enabled ?? true,
      canvas: payload.canvas,
      nodes: payload.nodes,
      edges: payload.edges,
      entry_task_ids: payload.entry_task_ids || resolveEntryTaskIds(payload.nodes, payload.edges),
      schedule: payload.schedule,
      created_at: timestamp,
      updated_at: timestamp,
    }

    pipelines = [pipeline, ...pipelines]
    return { ...pipeline }
  },

  update(pipelineId: string, payload: SavePipelinePayload) {
    const current = pipelines.find((item) => item.pipeline_id === pipelineId)
    if (!current) {
      const error = new Error('Pipeline not found') as Error & { status?: number }
      error.status = 404
      throw error
    }

    assertValid(payload)

    const updated: Pipeline = {
      ...current,
      name: payload.name,
      description: payload.description || '',
      enabled: payload.enabled ?? current.enabled,
      canvas: payload.canvas,
      nodes: payload.nodes,
      edges: payload.edges,
      entry_task_ids: payload.entry_task_ids || resolveEntryTaskIds(payload.nodes, payload.edges),
      schedule: payload.schedule ?? current.schedule,
      updated_at: formatNow(),
    }

    pipelines = pipelines.map((item) => (item.pipeline_id === pipelineId ? updated : item))
    return { ...updated }
  },

  remove(pipelineId: string) {
    const exists = pipelines.some((item) => item.pipeline_id === pipelineId)
    if (!exists) {
      const error = new Error('Pipeline not found') as Error & { status?: number }
      error.status = 404
      throw error
    }

    pipelines = pipelines.filter((item) => item.pipeline_id !== pipelineId)
  },

  publish(pipelineId: string, payload?: PublishPipelinePayload): PublishPipelineResult {
    const pipeline = pipelines.find((item) => item.pipeline_id === pipelineId)
    if (!pipeline) {
      const error = new Error('Pipeline not found') as Error & { status?: number }
      error.status = 404
      throw error
    }

    assertValid(pipeline)

    const nodeIds = new Set(pipeline.nodes.map((node) => node.task_id))
    const taskIds = new Set(taskStore.list().map((task) => task.task_id))
    const skippedTasks = pipeline.nodes
      .map((node) => node.task_id)
      .filter((taskId) => !taskIds.has(taskId))

    const reservedNodeIds = new Set<string>()
    pipelines.forEach((item) => {
      if (item.pipeline_id === pipelineId) {
        return
      }
      item.nodes.forEach((node) => reservedNodeIds.add(node.task_id))
    })

    const applyOptions = {
      previousNodeIds: payload?.previous_node_ids || [],
      reservedNodeIds,
    }
    const clearedTasks = collectClearedTaskIds(pipeline, applyOptions)
    const nextTasks = applyPipelineToTasks(pipeline, taskStore.list(), applyOptions)
    taskStore.replaceAll(nextTasks)

    const updatedTasks = nextTasks
      .filter((task) => nodeIds.has(task.task_id))
      .map((task) => task.task_id)

    return {
      pipeline_id: pipelineId,
      updated_tasks: updatedTasks,
      cleared_tasks: clearedTasks,
      skipped_tasks: skippedTasks,
    }
  },

  toDagGraphs(): DagGraphView[] {
    const tasks = taskStore.list()
    return pipelineStore.listEnabled().map((pipeline) => pipelineToDagGraph(pipeline, tasks))
  },
}
