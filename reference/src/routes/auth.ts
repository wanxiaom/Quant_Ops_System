import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { authMiddleware, getAuthUser, requirePermission } from '../lib/auth.js'
import { PERMISSIONS } from '../lib/permissions.js'
import { fail, ok } from '../lib/response.js'
import { authStore } from '../store/auth.js'
import type { LoginPayload } from '../types.js'

export const authRoutes = new Hono()

authRoutes.post('/login', async (c) => {
  await delay(220)
  const payload = await c.req.json<LoginPayload>()

  try {
    return c.json(ok(authStore.login(payload)))
  } catch (error) {
    return c.json(fail(error instanceof Error ? error.message : '登录失败'), 401)
  }
})

authRoutes.get('/me', authMiddleware, async (c) => {
  await delay(120)
  const header = c.req.header('Authorization') || ''
  const token = header.startsWith('Bearer ') ? header.slice(7) : ''

  try {
    return c.json(ok(authStore.me(token)))
  } catch (error) {
    return c.json(fail(error instanceof Error ? error.message : '未登录'), 401)
  }
})

authRoutes.get('/permissions', authMiddleware, async (c) => {
  await delay(80)
  const user = getAuthUser(c)
  return c.json(
    ok({
      role: user?.role,
      permissions: user?.permissions || [],
    }),
  )
})

authRoutes.post('/logout', authMiddleware, async (c) => {
  await delay(120)
  return c.json(ok(null))
})
