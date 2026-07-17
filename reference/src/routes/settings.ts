import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { requirePermission } from '../lib/auth.js'
import { PERMISSIONS } from '../lib/permissions.js'
import { ok } from '../lib/response.js'
import { settingsStore } from '../store/settings.js'
import type { AlertRule, NotificationChannel, SystemParameters } from '../types.js'

export const settingsRoutes = new Hono()

settingsRoutes.get('/overview', async (c) => {
  await delay()
  return c.json(ok(settingsStore.snapshot()))
})

settingsRoutes.put('/alert-rules/:ruleId', requirePermission(PERMISSIONS.ALERT_MANAGE), async (c) => {
  await delay()
  const rule = await c.req.json<AlertRule>()
  settingsStore.saveAlertRule(rule)
  return c.json(ok(null, 'Mock alert rule saved successfully'))
})

settingsRoutes.post('/alert-rules/:ruleId/toggle', requirePermission(PERMISSIONS.ALERT_MANAGE), async (c) => {
  await delay()
  settingsStore.toggleAlertRule(c.req.param('ruleId'))
  return c.json(ok(null, 'Mock alert rule toggled successfully'))
})

settingsRoutes.put('/notification-channels/:channelId', requirePermission(PERMISSIONS.ALERT_MANAGE), async (c) => {
  await delay()
  const channel = await c.req.json<NotificationChannel>()
  settingsStore.saveNotificationChannel(channel)
  return c.json(ok(null, 'Mock notification channel saved successfully'))
})

settingsRoutes.post('/notification-channels/:channelId/test', requirePermission(PERMISSIONS.ALERT_MANAGE), async (c) => {
  await delay()
  return c.json(ok(null, 'Mock notification sent successfully'))
})

settingsRoutes.put('/system-parameters', requirePermission(PERMISSIONS.SYSTEM_MANAGE), async (c) => {
  await delay()
  const payload = await c.req.json<SystemParameters>()
  settingsStore.saveSystemParameters(payload)
  return c.json(ok(null, 'Mock system parameters saved successfully'))
})

settingsRoutes.post('/system-parameters/reset', requirePermission(PERMISSIONS.SYSTEM_MANAGE), async (c) => {
  await delay()
  settingsStore.resetSystemParameters()
  return c.json(ok(null, 'Mock system parameters reset successfully'))
})
