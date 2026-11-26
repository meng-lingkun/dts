import { beforeEach, afterEach, test, mock } from 'node:test'
import assert from 'node:assert/strict'
import { api, apiBlob, credentialRevision, credentialsChanged, discardLegacyCredentials, openEventSocket, snapshotReader, subscribeToEvents } from '../src/api/client.ts'

beforeEach(() => {
  const values = new Map([['dts_session_token', 'session-old'], ['dts_api_token', 'static-old']])
  Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: {
    getItem: key => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, String(value)),
    removeItem: key => values.delete(key),
  } })
  Object.defineProperty(globalThis, 'window', { configurable: true, value: new EventTarget() })
})
afterEach(() => mock.restoreAll())

test('a stale JSON 401 cannot log out a newly established session', async () => {
  let respond
  mock.method(globalThis, 'fetch', () => new Promise(resolve => { respond = resolve }))
  const request = api('/api/v1/migrations').catch(error => error)
  credentialsChanged()
  const current = credentialRevision()
  respond(new Response('{"error":"expired"}', { status: 401 }))
  assert.equal((await request).status, 401)
  assert.equal(credentialRevision(), current)
})

test('a stale report download 401 cannot clear new credentials', async () => {
  let respond
  mock.method(globalThis, 'fetch', () => new Promise(resolve => { respond = resolve }))
  const request = apiBlob('/api/v1/report').catch(error => error)
  credentialsChanged()
  const current = credentialRevision()
  respond(new Response('{"error":"expired"}', { status: 401 }))
  assert.equal((await request).status, 401)
  assert.equal(credentialRevision(), current)
})

test('invalid login input does not revoke an existing session', async () => {
  const current = credentialRevision()
  mock.method(globalThis, 'fetch', async () => new Response('{"error":"invalid password"}', { status: 401 }))
  await assert.rejects(api('/api/v1/auth/login', { method: 'POST' }))
  assert.equal(credentialRevision(), current)
})

test('Headers and tuple headers survive authentication injection', async () => {
  const sent = []
  mock.method(globalThis, 'fetch', async (_path, init) => {
    sent.push(new Headers(init.headers))
    assert.equal(init.credentials, 'same-origin')
    assert.equal(init.mode, 'same-origin')
    return new Response('{}')
  })
  await api('/api/v1/migrations', { headers: new Headers({ 'X-Trace': 'headers-value' }) })
  await api('/api/v1/migrations', { headers: [['X-Trace', 'tuple-value']] })
  assert.equal(sent[0].get('X-Trace'), 'headers-value')
  assert.equal(sent[1].get('X-Trace'), 'tuple-value')
  assert.equal(sent[0].get('Authorization'), null)
  assert.equal(sent[0].get('X-DTS-Browser'), '1')
  assert.equal(sent[0].get('X-DTS-API-Token'), null)
})

test('current-session 401 invalidates credentials and sends one auth event', async () => {
  const current = credentialRevision()
  let events = 0
  window.addEventListener('dts:unauthorized', () => events++)
  mock.method(globalThis, 'fetch', async () => new Response('{"error":"expired"}', { status: 401 }))
  await assert.rejects(api('/api/v1/migrations'))
  assert.equal(credentialRevision(), current + 1)
  assert.equal(events, 1)
})

test('network failures preserve credentials for retry', async () => {
  const current = credentialRevision()
  mock.method(globalThis, 'fetch', async () => { throw new TypeError('network disconnected') })
  await assert.rejects(api('/api/v1/migrations'), /network disconnected/)
  assert.equal(credentialRevision(), current)
})

test('successful HTML or malformed JSON is rejected rather than treated as empty data', async () => {
  mock.method(globalThis, 'fetch', async () => new Response('<html>proxy error</html>'))
  await assert.rejects(api('/api/v1/migrations'), /JSON/)
})

test('upgrade discards both legacy secrets', () => {
  discardLegacyCredentials()
  assert.equal(localStorage.getItem('dts_session_token'), null)
  assert.equal(localStorage.getItem('dts_api_token'), null)
})

test('event socket sends only the public subprotocol', () => {
  const socket = mock.fn()
  Object.defineProperty(globalThis, 'location', {configurable:true, value:{host:'dts.test',protocol:'https:'}})
  Object.defineProperty(globalThis, 'WebSocket', {configurable:true, value:socket})
  openEventSocket('task 1')
  assert.deepEqual(socket.mock.calls[0].arguments,['wss://dts.test/api/v1/ws?task_id=task%201',['dts.v1']])
})

test('event feed refreshes after a disconnect and stops retrying on logout', t => {
  t.mock.timers.enable({apis:['setTimeout']})
  const sockets = []
  class Socket {
    constructor() { sockets.push(this) }
    close() {}
  }
  Object.defineProperty(globalThis, 'location', {configurable:true, value:{host:'dts.test',protocol:'https:'}})
  Object.defineProperty(globalThis, 'WebSocket', {configurable:true, value:Socket})
  const messages = []
  const refresh = mock.fn()
  const stop = subscribeToEvents(message => messages.push(message), refresh)
  sockets[0].onopen()
  assert.equal(refresh.mock.callCount(), 0)
  sockets[0].onclose({code:1006})
  t.mock.timers.tick(1000)
  sockets[1].onopen()
  assert.equal(refresh.mock.callCount(), 1)
  sockets[0].onmessage({data:'{"stale":true}'})
  sockets[1].onmessage({data:'{"rows":1}'})
  assert.deepEqual(messages, [{rows:1}])
  sockets[1].onclose({code:1006})
  window.dispatchEvent(new Event('dts:unauthorized'))
  t.mock.timers.tick(30000)
  assert.equal(sockets.length, 2)
  stop()
})

test('revoked event sessions sign out once and a stale socket cannot invalidate a new session', t => {
  t.mock.timers.enable({apis:['setTimeout']})
  const sockets=[]
  class Socket { constructor(){sockets.push(this)} close(){} }
  Object.defineProperty(globalThis,'location',{configurable:true,value:{host:'dts.test',protocol:'https:'}})
  Object.defineProperty(globalThis,'WebSocket',{configurable:true,value:Socket})
  const unauthorized=mock.fn()
  window.addEventListener('dts:unauthorized',unauthorized)
  const stop=subscribeToEvents(()=>{},()=>{})
  credentialsChanged()
  sockets[0].onclose({code:1008})
  t.mock.timers.tick(1000)
  assert.equal(sockets.length,2)
  assert.equal(unauthorized.mock.callCount(),0)
  sockets[1].onclose({code:1013})
  t.mock.timers.tick(2000)
  assert.equal(sockets.length,3)
  assert.equal(unauthorized.mock.callCount(),0)
  sockets[2].onclose({code:1008})
  assert.equal(unauthorized.mock.callCount(),1)
  t.mock.timers.tick(30000)
  assert.equal(sockets.length,3)
  stop()
})

test('a stale list read or a late response after navigation cannot overwrite the view', async () => {
  const requests = []
  mock.method(globalThis, 'fetch', (_url, init) => new Promise(resolve => requests.push({resolve, signal:init.signal})))
  const rows = []
  const failures = []
  const reader = snapshotReader('/api/v1/workers', value => rows.push(value), error => failures.push(error))
  const older = reader.load()
  const newer = reader.load()
  assert.equal(requests[0].signal.aborted, true)
  requests[1].resolve(new Response('[2]'))
  await newer
  requests[0].resolve(new Response('[1]'))
  await older
  const pending = reader.load()
  reader.close()
  assert.equal(requests[2].signal.aborted, true)
  requests[2].resolve(new Response('[3]'))
  await pending
  assert.deepEqual(rows, [[2]])
  assert.deepEqual(failures, [])
})
