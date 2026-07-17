import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { requirePermission } from '../lib/auth.js'
import { PERMISSIONS } from '../lib/permissions.js'
import { fail, ok } from '../lib/response.js'
import { pipelineStore } from '../store/pipelines.js'
import type { PublishPipelinePayload, SavePipelinePayload } from '../types.js'

export const pipelineRoutes = new Hono()

pipelineRoutes.get('/', async (c) => {
  await delay(220)
  return c.json(ok(pipelineStore.list()))
})

pipelineRoutes.post('/', requirePermission(PERMISSIONS.PIPELINE_EDIT), async (c) => {
  await delay(260)
  const payload = await c.req.json<SavePipelinePayload>()

  try {
    return c.json(ok(pipelineStore.create(payload), 'Pipeline created successfully'))
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 400
    return c.json(fail(error instanceof Error ? error.message : 'Pipeline 创建失败'), status)
  }
})

pipelineRoutes.get('/:pipelineId', async (c) => {
  await delay(220)
  const pipeline = pipelineStore.detail(c.req.param('pipelineId'))
  if (!pipeline) {
    return c.json(fail('Pipeline not found'), 404)
  }
  return c.json(ok(pipeline))
})

pipelineRoutes.put('/:pipelineId', requirePermission(PERMISSIONS.PIPELINE_EDIT), async (c) => {
  await delay(260)
  const payload = await c.req.json<SavePipelinePayload>()

  try {
    return c.json(ok(pipelineStore.update(c.req.param('pipelineId'), payload), 'Pipeline updated successfully'))
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 400
    return c.json(fail(error instanceof Error ? error.message : 'Pipeline 更新失败'), status)
  }
})

pipelineRoutes.delete('/:pipelineId', requirePermission(PERMISSIONS.PIPELINE_EDIT), async (c) => {
  await delay(220)

  try {
    pipelineStore.remove(c.req.param('pipelineId'))
    return c.json(ok(null, 'Pipeline deleted successfully'))
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 404
    return c.json(fail(error instanceof Error ? error.message : 'Pipeline 删除失败'), status)
  }
})

pipelineRoutes.post('/:pipelineId/publish', requirePermission(PERMISSIONS.PIPELINE_EDIT), async (c) => {
  await delay(300)

  try {
    const payload = await c.req.json<PublishPipelinePayload>().catch(() => ({}))
    return c.json(ok(pipelineStore.publish(c.req.param('pipelineId'), payload), 'Pipeline published successfully'))
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 400
    return c.json(fail(error instanceof Error ? error.message : 'Pipeline 发布失败'), status)
  }
})
