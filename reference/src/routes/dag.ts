import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { ok } from '../lib/response.js'
import { pipelineStore } from '../store/pipelines.js'

export const dagRoutes = new Hono()

dagRoutes.get('/graphs', async (c) => {
  await delay(220)
  return c.json(ok(pipelineStore.toDagGraphs()))
})
