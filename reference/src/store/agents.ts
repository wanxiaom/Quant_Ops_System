import type { Agent, AgentCreatePayload } from '../types.js'

function formatNow() {
  const now = new Date()
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
}

let agents: Agent[] = [
  {
    node_id: 'node_001',
    hostname: 'quant-linux-01',
    ip: '192.168.8.11',
    os_type: 'linux',
    status: 'busy',
    last_heartbeat: '2026-06-30 15:05:12',
    cpu_load: 46,
    mem_usage: 62,
    version: 'agent-0.3.1',
    concurrency_used: 2,
    concurrency_limit: 4,
    running_tasks: 2,
    tags: ['rqdata', 'dolphindb', 'factor'],
    created_at: '2026-06-20 09:12:00',
  },
  {
    node_id: 'windows-worker-01',
    hostname: 'quant-win-01',
    ip: '192.168.8.21',
    os_type: 'windows',
    status: 'online',
    last_heartbeat: '2026-06-30 15:05:10',
    cpu_load: 18,
    mem_usage: 48,
    version: 'agent-0.3.1',
    concurrency_used: 0,
    concurrency_limit: 2,
    running_tasks: 0,
    tags: ['wind', 'cj-connector'],
    created_at: '2026-06-20 10:35:00',
  },
  {
    node_id: 'node_002',
    hostname: 'quant-linux-02',
    ip: '192.168.8.12',
    os_type: 'linux',
    status: 'offline',
    last_heartbeat: '2026-06-30 14:18:35',
    cpu_load: 0,
    mem_usage: 0,
    version: 'agent-0.2.8',
    concurrency_used: 0,
    concurrency_limit: 4,
    running_tasks: 0,
    tags: ['backup', 'research'],
    created_at: '2026-06-21 14:18:00',
  },
  {
    node_id: 'windows-worker-02',
    hostname: 'quant-win-02',
    ip: '192.168.8.22',
    os_type: 'windows',
    status: 'error',
    last_heartbeat: '2026-06-30 15:00:41',
    cpu_load: 91,
    mem_usage: 86,
    version: 'agent-0.3.0',
    concurrency_used: 1,
    concurrency_limit: 2,
    running_tasks: 1,
    tags: ['wind', 'st-stock'],
    created_at: '2026-06-22 11:07:00',
  },
]

export const agentStore = {
  list() {
    return [...agents]
  },

  create(payload: AgentCreatePayload) {
    if (agents.some((agent) => agent.node_id === payload.node_id)) {
      const error = new Error('节点 ID 已存在') as Error & { status?: number }
      error.status = 409
      throw error
    }

    const agent: Agent = {
      node_id: payload.node_id,
      hostname: payload.hostname,
      ip: payload.ip,
      os_type: payload.os_type,
      status: 'offline',
      last_heartbeat: '-',
      cpu_load: 0,
      mem_usage: 0,
      version: '-',
      concurrency_used: 0,
      concurrency_limit: payload.concurrency_limit ?? 4,
      running_tasks: 0,
      tags: payload.tags ?? [],
      created_at: formatNow(),
    }

    agents = [agent, ...agents]
    return agent
  },

  offline(nodeId: string) {
    agents = agents.map((agent) =>
      agent.node_id === nodeId
        ? {
            ...agent,
            status: 'offline',
            concurrency_used: 0,
            running_tasks: 0,
            cpu_load: 0,
            mem_usage: 0,
          }
        : agent,
    )
  },
}
