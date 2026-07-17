/**
 * Pipeline（流水线）= 用户可编辑的 DAG 定义层。
 * DagGraph = 监控页展示层（Pipeline + Task 运行时状态合成）。
 */

export interface PipelineCanvasSize {
  width: number
  height: number
}

/** 流水线节点：只引用已有任务，不重复存任务配置 */
export interface PipelineNode {
  task_id: string
  x: number
  y: number
}

export interface PipelineEdge {
  from: string
  to: string
}

export interface PipelineSchedule {
  enabled: boolean
  cron_expr: string
}

export interface Pipeline {
  pipeline_id: string
  name: string
  description: string
  enabled: boolean
  canvas: PipelineCanvasSize
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  /** 显式入口节点；未填时由 edges 自动推导（无入边的节点） */
  entry_task_ids?: string[]
  schedule?: PipelineSchedule
  created_at?: string
  updated_at?: string
}

export interface PipelineSummary {
  pipeline_id: string
  name: string
  description: string
  enabled: boolean
  task_count: number
  edge_count: number
  updated_at?: string
}

export interface SavePipelinePayload {
  name: string
  description?: string
  enabled?: boolean
  canvas: PipelineCanvasSize
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  entry_task_ids?: string[]
  schedule?: PipelineSchedule
}

export interface PublishPipelinePayload {
  /** 发布前该流程已存在的节点 ID，用于清除被移除任务的 dag_parents */
  previous_node_ids?: string[]
}

export interface PublishPipelineResult {
  pipeline_id: string
  updated_tasks: string[]
  cleared_tasks: string[]
  skipped_tasks: string[]
}
