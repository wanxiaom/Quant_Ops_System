import { networkInterfaces } from 'node:os'

import { serve } from '@hono/node-server'
import { Hono } from 'hono'
import { cors } from 'hono/cors'
import { logger } from 'hono/logger'

import { authMiddleware } from './lib/auth.js'
import { agentRoutes } from './routes/agents.js'
import { authRoutes } from './routes/auth.js'
import { dagRoutes } from './routes/dag.js'
import { dashboardRoutes } from './routes/dashboard.js'
import { logRoutes } from './routes/logs.js'
import { pipelineRoutes } from './routes/pipelines.js'
import { settingsRoutes } from './routes/settings.js'
import { userRoutes } from './routes/users.js'
import { executionRoutes, taskRoutes } from './routes/tasks.js'

const port = Number(process.env.MOCK_SERVER_PORT || 3001)
const listenOnLan = process.argv.includes('--host') || process.env.MOCK_SERVER_HOST === '0.0.0.0'
const hostname = listenOnLan ? '0.0.0.0' : process.env.MOCK_SERVER_HOST || '127.0.0.1'

const app = new Hono()

app.use('*', logger())
app.use(
  '*',
  cors({
    origin: (origin) => {
      if (!origin || listenOnLan) {
        return origin || '*'
      }

      const allowed = ['http://127.0.0.1:5173', 'http://localhost:5173']
      return allowed.includes(origin) ? origin : allowed[0]
    },
    allowHeaders: ['Authorization', 'Content-Type'],
    allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  }),
)

app.get('/health', (c) => c.json({ status: 'ok', service: 'quant-ops-mock-server' }))

app.route('/api/auth', authRoutes)

const protectedApi = new Hono()
protectedApi.use('*', authMiddleware)
protectedApi.route('/tasks', taskRoutes)
protectedApi.route('/executions', executionRoutes)
protectedApi.route('/agents', agentRoutes)
protectedApi.route('/dashboard', dashboardRoutes)
protectedApi.route('/dag', dagRoutes)
protectedApi.route('/pipelines', pipelineRoutes)
protectedApi.route('/logs', logRoutes)
protectedApi.route('/users', userRoutes)
protectedApi.route('/settings', settingsRoutes)

app.route('/api', protectedApi)

app.notFound((c) => c.json({ code: 404, message: `Route not found: ${c.req.path}`, data: null }, 404))

function getLanAddresses() {
  const addresses: string[] = []

  for (const interfaces of Object.values(networkInterfaces())) {
    for (const item of interfaces ?? []) {
      if (item.family === 'IPv4' && !item.internal) {
        addresses.push(item.address)
      }
    }
  }

  return addresses
}

const server = serve(
  {
    fetch: app.fetch,
    port,
    hostname,
  },
  (info) => {
    console.log(`[mock-server] listening on http://127.0.0.1:${info.port}`)
    if (listenOnLan) {
      const lanAddresses = getLanAddresses()
      if (lanAddresses.length) {
        lanAddresses.forEach((address) => {
          console.log(`[mock-server] LAN access: http://${address}:${info.port}`)
        })
      } else {
        console.log(`[mock-server] LAN mode enabled (0.0.0.0:${info.port})`)
      }
    }
    console.log('[mock-server] login accounts:')
    console.log('  your_username / admin123      -> your_username')
    console.log('  operator / ops123     -> operator')
    console.log('  researcher / research123 -> researcher')
    console.log('[mock-server] dev token: quant_ops_secret_2026 (your_username)')
  },
)

server.on('error', (error: NodeJS.ErrnoException) => {
  if (error.code === 'EADDRINUSE') {
    console.error(`[mock-server] 端口 ${port} 已被占用，请先结束旧进程或改用其他端口：`)
    console.error(`  MOCK_SERVER_PORT=3002 npm run mock`)
    process.exit(1)
  }

  throw error
})
