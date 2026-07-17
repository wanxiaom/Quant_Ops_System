import type { AuthUser, LoginPayload, LoginResponse, UserRole } from '../types.js'
import { getPermissionsForRole } from '../lib/permissions.js'
import { usersStore } from './users.js'

const TOKEN_BY_USER_ID: Record<string, string> = {
  u_admin: 'mock-token-your_username',
  u_ops_01: 'mock-token-operator',
  u_researcher_01: 'mock-token-researcher',
}

const TOKEN_FALLBACK_ROLE: Record<string, UserRole> = {
  'quant_ops_secret_2026': 'your_username',
}

function buildAuthUser(userId: string, role: UserRole): AuthUser {
  const user = usersStore.findUser(userId)

  return {
    user_id: userId,
    name: user?.name || userId,
    email: user?.email || `${userId}@quant.local`,
    role,
    permissions: getPermissionsForRole(role),
  }
}

function resolveAccountByToken(token: string) {
  const entry = Object.entries(TOKEN_BY_USER_ID).find(([, value]) => value === token)
  if (entry) {
    const [userId] = entry
    const user = usersStore.findUser(userId)
    if (!user || user.status !== 'active') {
      return undefined
    }

    return buildAuthUser(userId, user.role)
  }

  const dynamicMatch = token.match(/^mock-token-(u_.+)$/)
  if (dynamicMatch) {
    const userId = dynamicMatch[1]
    const user = usersStore.findUser(userId)
    if (user && user.status === 'active') {
      return buildAuthUser(userId, user.role)
    }
  }

  const fallbackRole = TOKEN_FALLBACK_ROLE[token]
  if (fallbackRole) {
    return buildAuthUser('u_admin', fallbackRole)
  }

  return undefined
}

export const authStore = {
  login(payload: LoginPayload): LoginResponse {
    const user = usersStore.findByCredentials(payload.username, payload.password)

    if (!user) {
      throw new Error('用户名或密码错误')
    }

    if (user.status !== 'active') {
      throw new Error('用户已被禁用，请联系管理员')
    }

    const authUser = buildAuthUser(user.user_id, user.role)
    const token = TOKEN_BY_USER_ID[user.user_id] || `mock-token-${user.user_id}`

    return {
      token,
      user: authUser,
    }
  },

  me(token: string): AuthUser {
    const user = resolveAccountByToken(token)
    if (!user) {
      throw new Error('登录状态无效，请重新登录')
    }

    return user
  },

  resolveUserByToken(token: string) {
    return resolveAccountByToken(token)
  },
}
