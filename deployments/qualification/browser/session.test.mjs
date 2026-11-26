import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { once } from 'node:events'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import test from 'node:test'
import { chromium } from 'playwright-core'

const web = fileURLToPath(new URL('../../../web/', import.meta.url))
const requireWeb = createRequire(path.join(web, 'package.json'))
const { preview } = await import(pathToFileURL(requireWeb.resolve('vite')))

test('sign in, restore a protected page on reload, and sign out', { timeout: 60000 }, async t => {
  assert.ok(process.env.DTS_E2E_SERVER_BINARY, 'Build cmd/server and set DTS_E2E_SERVER_BINARY to its path')
  const binary = path.resolve(process.env.DTS_E2E_SERVER_BINARY)
  const directory = await mkdtemp(path.join(os.tmpdir(), 'dts-browser-e2e-'))
  const portProbe = net.createServer()
  portProbe.listen(0, '127.0.0.1')
  await once(portProbe, 'listening')
  const port = portProbe.address().port
  await new Promise(resolve => portProbe.close(resolve))
  const password = randomBytes(24).toString('hex')
  const backend = spawn(binary, [], {cwd: directory, windowsHide: true, env: {
    ...Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith('DTS_'))),
    DTS_STATE_FILE: ':memory:', DTS_CDC_SPOOL_STORAGE: 'metadata', DTS_AUTH_REQUIRED: 'true',
    DTS_AUTH_SECRET: randomBytes(32).toString('hex'), DTS_MASTER_KEY: randomBytes(32).toString('hex'),
    DTS_BOOTSTRAP_ADMIN_PASSWORD: password, DTS_ADDR: '127.0.0.1:' + port,
  }, stdio: 'inherit'})
  const stopped = once(backend, 'exit')
  t.after(async () => {
    if (backend.exitCode === null && backend.signalCode === null) backend.kill()
    await stopped
    await rm(directory, {recursive: true, force: true})
  })
  const target = 'http://127.0.0.1:' + port
  let ready = false
  for (let attempt = 0; attempt < 100 && backend.exitCode === null; attempt++) {
    if (await fetch(target + '/readyz').then(r => r.ok).catch(() => false)) { ready = true; break }
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  assert.ok(ready && backend.exitCode === null, 'Disposable backend readiness failed')
  const ui = await preview({root: web, configFile: false, preview: {
    host: '127.0.0.1', port: 0, proxy: {'/api': {target, ws: true}, '/healthz': target},
  }})
  let browser
  try {
    browser = await chromium.launch(process.env.DTS_E2E_CHROME ? {executablePath: process.env.DTS_E2E_CHROME} : {channel: 'chrome'})
    const page = await browser.newPage()
    const errors = []
    page.on('pageerror', e => errors.push(e.message))
    const origin = 'http://127.0.0.1:' + ui.httpServer.address().port
    await page.goto(origin + '/datasources')
    await page.waitForURL(url => url.pathname === '/login')
    await page.getByPlaceholder('请输入密码').fill(password)
    await page.getByRole('button', {name: '登录控制台'}).click()
    await page.waitForURL(url => url.pathname === '/datasources')
    await page.getByRole('heading', {level: 1, name: '数据源', exact: true}).waitFor()
    const cookies = await page.context().cookies()
    assert.ok(cookies.some(c => c.httpOnly), 'Expected an HttpOnly session cookie')
    await page.reload()
    await page.getByRole('heading', {level: 1, name: '数据源', exact: true}).waitFor()
    await page.goto(origin + '/settings')
    await page.getByPlaceholder('例如：值班巡检').fill('browser qualification')
    await page.getByRole('button', {name:'创建令牌'}).click()
    const reveal = page.getByRole('dialog', {name:'保存个人 API 令牌'})
    const pat = await reveal.getByRole('textbox').inputValue()
    const cliStatus = async () => (await fetch(target+'/api/v1/auth/me', {headers:{Authorization:'Bearer '+pat}})).status
    assert.equal(await cliStatus(), 200)
    await reveal.getByRole('button', {name:'已保存'}).click()
    await page.reload()
    const item = page.getByRole('row').filter({hasText:'browser qualification'})
    await item.getByRole('button', {name:'撤销'}).click()
    await page.getByRole('button', {name:/^(确定|确认|OK)$/}).click()
    await item.waitFor({state:'detached'})
    assert.equal(await cliStatus(), 401)
    // Record real connections and end the first one after the handshake.
    await page.addInitScript(() => {
      window.testEventSockets = []
      window.WebSocket = class extends WebSocket {
        constructor(...args) {
          super(...args)
          if (this.url.includes('/api/v1/ws')) window.testEventSockets.push(this)
        }
      }
    })
    await page.goto(origin + '/workers')
    await page.waitForFunction(() => window.testEventSockets.at(-1)?.readyState === WebSocket.OPEN)
    const refresh = page.waitForResponse(r => new URL(r.url()).pathname === '/api/v1/workers' && r.status() === 200)
    await page.evaluate(() => window.testEventSockets[0].close(4000, 'test connection loss'))
    await page.waitForFunction(() => window.testEventSockets.length === 2 && window.testEventSockets[1].readyState === WebSocket.OPEN)
    await refresh
    // Keep the operator stream open while a second signed-in session
    // revokes it, as when an administrator signs out another device.
    const currentID = await page.evaluate(async () => {
      const response = await fetch('/api/v1/auth/sessions')
      return (await response.json()).current_id
    })
    assert.ok(currentID)
    const operator = await fetch(target+'/api/v1/auth/login', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({username:'admin',password}),
    })
    assert.equal(operator.status, 200)
    const second = (await operator.json()).token
    await page.evaluate(() => {
      window.testEventSockets.at(-1).addEventListener('close', e => { window.testCloseCode = e.code })
    })
    const revoked = await fetch(target+'/api/v1/auth/sessions/'+encodeURIComponent(currentID), {
      method:'DELETE', headers:{Authorization:'Bearer '+second},
    })
    assert.equal(revoked.status, 204)
    await page.waitForURL(url => url.pathname === '/login')
    assert.equal(await page.evaluate(() => window.testCloseCode), 1008)
    assert.equal(await page.evaluate(async () => (await fetch('/api/v1/auth/me')).status), 401)
    const other = await fetch(target+'/api/v1/auth/me', {headers:{Authorization:'Bearer '+second}})
    assert.equal(other.status, 200)
    await page.getByPlaceholder('请输入密码').fill(password)
    await page.getByRole('button', {name:'登录控制台'}).click()
    await page.waitForURL(url => url.pathname === '/workers')
    await page.getByRole('button', {name: '退出', exact: true}).click()
    await page.waitForURL(url => url.pathname === '/login')
    await page.goto(origin + '/datasources')
    await page.waitForURL(url => url.pathname === '/login')
    assert.deepEqual(errors, [])
  } finally {
    await browser?.close()
    await ui.close()
  }
})
