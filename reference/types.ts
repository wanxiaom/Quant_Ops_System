export type DagStatus = 'success' | 'running' | 'waiting' | 'failed'

export interface DagNode {
  id: string
  name: string
  status: DagStatus
  x: number
  y: number
  latestExecId: string
  agent: string
  duration: string
}

export interface DagEdge {
  from: string
  to: string
}

export interface DagCanvasSize {
  width: number
  height: number
}

export interface DagGraph {
  id: string
  name: string
  description: string
  canvas: DagCanvasSize
  nodes: DagNode[]
  edges: DagEdge[]
}

export const dagStatusMeta: Record<DagStatus, { label: string; type: 'success' | 'primary' | 'info' | 'danger' }> = {
  success: { label: '成功', type: 'success' },
  running: { label: '运行中', type: 'primary' },
  waiting: { label: '等待', type: 'info' },
  failed: { label: '失败', type: 'danger' },
}
