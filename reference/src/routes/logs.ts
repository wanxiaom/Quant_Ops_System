import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { fail, ok } from '../lib/response.js'
import { logStore } from '../store/logs.js'
import type { LogLevel, LogSearchParams, LogStatus } from '../types.js'

export const logRoutes = new Hono()

logRoutes.get('/search', async (c) => {
  await delay()

  const dateRange = c.req.queries('date_range') ?? []
  const params: LogSearchParams = {
    keyword: c.req.query('keyword') || undefined,
    task_name: c.req.query('task_name') || undefined,
    agent_id: c.req.query('agent_id') || undefined,
    level: (c.req.query('level') as LogLevel | undefined) || undefined,
    status: (c.req.query('status') as LogStatus | undefined) || undefined,
    date_range: dateRange.length > 0 ? dateRange : undefined,
  }

  return c.json(ok(logStore.search(params)))
})

logRoutes.get('/:logId/download', async (c) => {
  await delay()

  try {
    const text = logStore.download(c.req.param('logId'))
    return c.text(text, 200, {
      'Content-Type': 'text/plain; charset=utf-8',
      'Content-Disposition': `attachment; filename="${c.req.param('logId')}.log"`,
    })
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 404
    return c.json(fail(error instanceof Error ? error.message : 'Log not found'), status)
  }
})

logRoutes.get('/:logId', async (c) => {
  await delay()

  try {
    return c.json(ok(logStore.detail(c.req.param('logId'))))
  } catch (error) {
    const status = (error as Error & { status?: number }).status ?? 404
    return c.json(fail(error instanceof Error ? error.message : 'Log not found'), status)
  }
})
