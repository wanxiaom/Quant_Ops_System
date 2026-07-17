import { Hono } from 'hono'

import { delay } from '../lib/delay.js'
import { requirePermission } from '../lib/auth.js'
import { PERMISSIONS } from '../lib/permissions.js'
import { fail, ok } from '../lib/response.js'
import { usersStore } from '../store/users.js'
import type { UserRole } from '../types.js'

export const userRoutes = new Hono()

userRoutes.get('/', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  return c.json(ok(usersStore.list()))
})

userRoutes.get('/role-permissions', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  return c.json(ok(usersStore.rolePermissions()))
})

userRoutes.get('/:userId', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  const user = usersStore.findUser(c.req.param('userId'))
  if (!user) {
    return c.json(fail('用户不存在', 404), 404)
  }

  const { password: _, ...publicUser } = user
  return c.json(ok(publicUser))
})

userRoutes.post('/', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  const body = await c.req.json<{
    username: string
    password: string
    name: string
    email: string
    role: UserRole
  }>()

  try {
    const user = usersStore.create(body)
    return c.json(ok(user, '用户创建成功'))
  } catch (error) {
    return c.json(fail(error instanceof Error ? error.message : '创建用户失败', 400), 400)
  }
})

userRoutes.put('/:userId', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  const body = await c.req.json<{
    name?: string
    email?: string
    role?: UserRole
    password?: string
  }>()

  try {
    const user = usersStore.update(c.req.param('userId'), body)
    return c.json(ok(user, '用户信息已更新'))
  } catch (error) {
    return c.json(fail(error instanceof Error ? error.message : '更新用户失败', 404), 404)
  }
})

userRoutes.post('/:userId/toggle', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  try {
    const user = usersStore.toggleStatus(c.req.param('userId'))
    return c.json(ok(user, '用户状态已更新'))
  } catch (error) {
    return c.json(fail(error instanceof Error ? error.message : '更新用户状态失败', 404), 404)
  }
})

userRoutes.delete('/:userId', requirePermission(PERMISSIONS.USER_MANAGE), async (c) => {
  await delay()
  try {
    usersStore.remove(c.req.param('userId'))
    return c.json(ok(null, '用户已删除'))
  } catch (error) {
    return c.json(fail(error instanceof Error ? error.message : '删除用户失败', 404), 404)
  }
})
