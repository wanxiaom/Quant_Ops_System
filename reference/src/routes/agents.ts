import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { requirePermission } from '../lib/auth.js'
import { PERMISSIONS } from '../lib/permissions.js'
import { fail, ok } from '../lib/response.js'
import { agentStore } from '../store/agents.js'
import type { AgentCreatePayload } from '../types.js'

export const agentRoutes = new Hono()

agentRoutes.get('/', async (c) => {
  await delay(300)
  return c.json(ok(agentStore.list()))
})

agentRoutes.post('/', requirePermission(PERMISSIONS.AGENT_MANAGE), async (c) => {
  await delay(300)
  const payload = await c.req.json<AgentCreatePayload>()

  try {
    return c.json(ok(agentStore.create(payload), 'Mock agent created successfully'))
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 400
    return c.json(fail(error instanceof Error ? error.message : 'Agent 创建失败'), status)
  }
})

agentRoutes.post('/:nodeId/offline', requirePermission(PERMISSIONS.AGENT_MANAGE), async (c) => {
  await delay(300)
  agentStore.offline(c.req.param('nodeId'))
  return c.json(ok(null, 'Mock agent offline successfully'))
})
