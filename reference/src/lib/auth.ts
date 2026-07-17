import type { Context, Next } from 'hono'

import type { PermissionCode } from './permissions.js'
import { authStore } from '../store/auth.js'

const PUBLIC_SUFFIXES = ['/login']

export async function authMiddleware(c: Context, next: Next) {
  if (c.req.method === 'OPTIONS' || PUBLIC_SUFFIXES.some((suffix) => c.req.path.endsWith(suffix))) {
    await next()
    return
  }

  const header = c.req.header('Authorization') || ''
  const token = header.startsWith('Bearer ') ? header.slice(7) : ''
  const user = token ? authStore.resolveUserByToken(token) : undefined

  if (!user) {
    return c.json({ code: 401, message: 'Unauthorized', data: null }, 401)
  }

  c.set('authUser', user)
  c.set('permissions', new Set(user.permissions))

  await next()
}

export function requirePermission(...codes: PermissionCode[]) {
  return async (c: Context, next: Next) => {
    const permissions = c.get('permissions') as Set<string> | undefined

    if (!permissions || !codes.some((code) => permissions.has(code))) {
      return c.json({ code: 403, message: 'Forbidden: insufficient permissions', data: null }, 403)
    }

    await next()
  }
}

export function getAuthUser(c: Context) {
  return c.get('authUser') as ReturnType<typeof authStore.resolveUserByToken>
}
