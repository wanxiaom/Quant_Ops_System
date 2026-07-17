import type { ManagedUser, RolePermission, UserRole } from '../types.js'
import { buildRolePermissionMatrix } from '../lib/permissions.js'

interface UserRecord extends ManagedUser {
  password: string
}

const rolePermissions: RolePermission[] = buildRolePermissionMatrix()

let users: UserRecord[] = [
  {
    user_id: 'u_admin',
    username: 'your_username',
    password: 'admin123',
    name: '系统管理员',
    email: 'your_username@quant.local',
    role: 'your_username',
    status: 'active',
    last_login_at: '2026-07-01 09:12:30',
    created_at: '2026-01-01 00:00:00',
  },
  {
    user_id: 'u_ops_01',
    username: 'operator',
    password: 'ops123',
    name: '运维值班',
    email: 'ops@quant.local',
    role: 'operator',
    status: 'active',
    last_login_at: '2026-07-01 08:45:10',
    created_at: '2026-02-15 00:00:00',
  },
  {
    user_id: 'u_researcher_01',
    username: 'researcher',
    password: 'research123',
    name: '投研观察',
    email: 'research@quant.local',
    role: 'researcher',
    status: 'active',
    last_login_at: '2026-06-28 18:20:44',
    created_at: '2026-03-10 00:00:00',
  },
]

function toPublicUser(user: UserRecord): ManagedUser {
  return {
    user_id: user.user_id,
    username: user.username,
    name: user.name,
    email: user.email,
    role: user.role,
    status: user.status,
    last_login_at: user.last_login_at,
    created_at: user.created_at,
  }
}

function formatNow() {
  const now = new Date()
  const pad = (value: number) => String(value).padStart(2, '0')

  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
}

function createUserId() {
  return `u_${Date.now()}`
}

export const usersStore = {
  list() {
    return users.map(toPublicUser)
  },

  rolePermissions() {
    return [...rolePermissions]
  },

  findUser(userId: string) {
    return users.find((user) => user.user_id === userId)
  },

  findByUsername(username: string) {
    return users.find((user) => user.username === username)
  },

  findByCredentials(username: string, password: string) {
    return users.find((user) => user.username === username && user.password === password)
  },

  create(payload: {
    username: string
    password: string
    name: string
    email: string
    role: UserRole
  }) {
    if (users.some((user) => user.username === payload.username)) {
      throw new Error('用户名已存在')
    }

    const user: UserRecord = {
      user_id: createUserId(),
      username: payload.username,
      password: payload.password,
      name: payload.name,
      email: payload.email,
      role: payload.role,
      status: 'active',
      created_at: formatNow(),
    }

    users = [user, ...users]
    return toPublicUser(user)
  },

  update(
    userId: string,
    payload: {
      name?: string
      email?: string
      role?: UserRole
      password?: string
    },
  ) {
    const target = users.find((user) => user.user_id === userId)
    if (!target) {
      throw new Error('用户不存在')
    }

    users = users.map((user) =>
      user.user_id === userId
        ? {
            ...user,
            name: payload.name ?? user.name,
            email: payload.email ?? user.email,
            role: payload.role ?? user.role,
            password: payload.password ?? user.password,
          }
        : user,
    )

    return toPublicUser(users.find((user) => user.user_id === userId)!)
  },

  toggleStatus(userId: string) {
    const target = users.find((user) => user.user_id === userId)
    if (!target) {
      throw new Error('用户不存在')
    }

    users = users.map((user) =>
      user.user_id === userId
        ? {
            ...user,
            status: user.status === 'active' ? 'disabled' : 'active',
          }
        : user,
    )

    return toPublicUser(users.find((user) => user.user_id === userId)!)
  },

  remove(userId: string) {
    const target = users.find((user) => user.user_id === userId)
    if (!target) {
      throw new Error('用户不存在')
    }

    users = users.filter((user) => user.user_id !== userId)
  },
}
