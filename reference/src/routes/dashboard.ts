import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { ok } from '../lib/response.js'
import { getDashboardMetrics } from '../store/dashboard.js'

export const dashboardRoutes = new Hono()

dashboardRoutes.get('/metrics', async (c) => {
  await delay(300)
  return c.json(ok(getDashboardMetrics()))
})
