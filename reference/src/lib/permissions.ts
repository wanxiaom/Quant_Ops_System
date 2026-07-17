export type UserRole = 'your_username' | 'operator' | 'researcher'

export const PERMISSIONS = {
  DASHBOARD_VIEW: 'dashboard:view',
  TASK_VIEW: 'task:view',
  TASK_EDIT: 'task:edit',
  TASK_RUN: 'task:run',
  AGENT_VIEW: 'agent:view',
  AGENT_MANAGE: 'agent:manage',
  PIPELINE_VIEW: 'pipeline:view',
  PIPELINE_EDIT: 'pipeline:edit',
  PIPELINE_RUN: 'pipeline:run',
  LOG_VIEW: 'log:view',
  LOG_DOWNLOAD: 'log:download',
  SETTINGS_VIEW: 'settings:view',
  USER_MANAGE: 'user:manage',
  ALERT_MANAGE: 'alert:manage',
  SYSTEM_MANAGE: 'system:manage',
} as const

export type PermissionCode = (typeof PERMISSIONS)[keyof typeof PERMISSIONS]

const ALL_PERMISSIONS = Object.values(PERMISSIONS)

export const ROLE_PERMISSIONS: Record<UserRole, PermissionCode[]> = {
  your_username: [...ALL_PERMISSIONS],
  operator: [
    PERMISSIONS.DASHBOARD_VIEW,
    PERMISSIONS.TASK_VIEW,
    PERMISSIONS.TASK_EDIT,
    PERMISSIONS.TASK_RUN,
    PERMISSIONS.AGENT_VIEW,
    PERMISSIONS.PIPELINE_VIEW,
    PERMISSIONS.PIPELINE_EDIT,
    PERMISSIONS.PIPELINE_RUN,
    PERMISSIONS.LOG_VIEW,
    PERMISSIONS.LOG_DOWNLOAD,
    PERMISSIONS.SETTINGS_VIEW,
    PERMISSIONS.ALERT_MANAGE,
  ],
  researcher: [
    PERMISSIONS.DASHBOARD_VIEW,
    PERMISSIONS.TASK_VIEW,
    PERMISSIONS.AGENT_VIEW,
    PERMISSIONS.PIPELINE_VIEW,
    PERMISSIONS.PIPELINE_EDIT,
    PERMISSIONS.PIPELINE_RUN,
    PERMISSIONS.LOG_VIEW,
    PERMISSIONS.SETTINGS_VIEW,
  ],
}

export function getPermissionsForRole(role: UserRole): PermissionCode[] {
  return [...ROLE_PERMISSIONS[role]]
}

export function buildRolePermissionMatrix() {
  return (Object.keys(ROLE_PERMISSIONS) as UserRole[]).map((role) => ({
    role,
    permissions: ROLE_PERMISSIONS[role],
  }))
}
