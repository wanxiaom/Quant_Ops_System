import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { requirePermission } from '../lib/auth.js'
import { PERMISSIONS } from '../lib/permissions.js'
import { fail, ok } from '../lib/response.js'
import { taskStore } from '../store/tasks.js'
import type { CreateTaskPayload } from '../types.js'

export const taskRoutes = new Hono()

taskRoutes.get('/', async (c) => {
  await delay()
  return c.json(ok(taskStore.list()))
})

taskRoutes.post('/', requirePermission(PERMISSIONS.TASK_EDIT), async (c) => {
  await delay()
  const payload = await c.req.json<CreateTaskPayload>()
  taskStore.create(payload)
  return c.json(ok(null, 'Mock task created successfully'))
})

taskRoutes.get('/:taskId', async (c) => {
  await delay()
  const task = taskStore.detail(c.req.param('taskId'))
  if (!task) {
    return c.json(fail('Task not found'), 404)
  }
  return c.json(ok(task))
})

taskRoutes.put('/:taskId', requirePermission(PERMISSIONS.TASK_EDIT), async (c) => {
  await delay()
  const payload = await c.req.json<CreateTaskPayload>()
  taskStore.update(c.req.param('taskId'), payload)
  return c.json(ok(null, 'Mock task updated successfully'))
})

taskRoutes.delete('/:taskId', requirePermission(PERMISSIONS.TASK_EDIT), async (c) => {
  await delay()
  taskStore.remove(c.req.param('taskId'))
  return c.json(ok(null, 'Mock task deleted successfully'))
})

taskRoutes.post('/:taskId/run', requirePermission(PERMISSIONS.TASK_RUN), async (c) => {
  await delay()
  return c.json(taskStore.run(c.req.param('taskId')))
})

taskRoutes.post('/:taskId/incr', requirePermission(PERMISSIONS.TASK_RUN), async (c) => {
  await delay()
  return c.json(taskStore.runIncremental(c.req.param('taskId')))
})

taskRoutes.post('/:taskId/pause', requirePermission(PERMISSIONS.TASK_EDIT), async (c) => {
  await delay()
  taskStore.pause(c.req.param('taskId'))
  return c.json(ok(null, 'Mock task paused successfully'))
})

taskRoutes.post('/:taskId/resume', requirePermission(PERMISSIONS.TASK_EDIT), async (c) => {
  await delay()
  taskStore.resume(c.req.param('taskId'))
  return c.json(ok(null, 'Mock task resumed successfully'))
})

export const executionRoutes = new Hono()

executionRoutes.get('/', async (c) => {
  await delay()
  const taskId = c.req.query('task_id')
  if (!taskId) {
    return c.json(ok(taskStore.allExecutions()))
  }
  return c.json(ok(taskStore.executions(taskId)))
})
