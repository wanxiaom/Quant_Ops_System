import type { AlertRule, NotificationChannel, SettingsSnapshot, SystemParameters } from '../types.js'

let alertRules: AlertRule[] = [
  {
    rule_id: 'rule_task_failed',
    name: '任务失败告警',
    enabled: true,
    event: 'task_failed',
    severity: 'critical',
    channels: ['channel_ops_webhook', 'channel_ops_email'],
  },
  {
    rule_id: 'rule_task_timeout',
    name: '任务超时告警',
    enabled: true,
    event: 'task_timeout',
    severity: 'warning',
    channels: ['channel_ops_webhook'],
  },
  {
    rule_id: 'rule_agent_offline',
    name: 'Agent 离线告警',
    enabled: false,
    event: 'agent_offline',
    severity: 'critical',
    channels: ['channel_ops_email'],
  },
]

let notificationChannels: NotificationChannel[] = [
  {
    channel_id: 'channel_ops_webhook',
    name: '运维群 Webhook',
    type: 'webhook',
    enabled: true,
    target: 'https://hooks.quant.local/ops',
  },
  {
    channel_id: 'channel_ops_email',
    name: '运维邮件组',
    type: 'email',
    enabled: true,
    target: 'quant-ops@quant.local',
  },
]

const defaultSystemParameters: SystemParameters = {
  global_concurrency_limit: 32,
  heartbeat_timeout_sec: 30,
  log_retention_days: 30,
  max_retry_count: 3,
}

let systemParameters: SystemParameters = { ...defaultSystemParameters }

function createSnapshot(): SettingsSnapshot {
  return {
    alertRules: [...alertRules],
    notificationChannels: [...notificationChannels],
    systemParameters: { ...systemParameters },
  }
}

export const settingsStore = {
  snapshot() {
    return createSnapshot()
  },

  saveAlertRule(rule: AlertRule) {
    alertRules = [rule, ...alertRules.filter((item) => item.rule_id !== rule.rule_id)]
  },

  toggleAlertRule(ruleId: string) {
    alertRules = alertRules.map((rule) =>
      rule.rule_id === ruleId ? { ...rule, enabled: !rule.enabled } : rule,
    )
  },

  saveNotificationChannel(channel: NotificationChannel) {
    notificationChannels = [
      channel,
      ...notificationChannels.filter((item) => item.channel_id !== channel.channel_id),
    ]
  },

  saveSystemParameters(payload: SystemParameters) {
    systemParameters = { ...payload }
  },

  resetSystemParameters() {
    systemParameters = { ...defaultSystemParameters }
  },
}
