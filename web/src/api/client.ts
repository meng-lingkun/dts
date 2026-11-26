export class ApiError extends Error {
  status: number
  retryAfterSeconds: number
  code:string
  requestId:string
  constructor(message:string,status:number,retryAfterSeconds=0,code='',requestId=''){
    super(message);this.status=status;this.retryAfterSeconds=retryAfterSeconds;this.code=code;this.requestId=requestId
  }
}

let revision=0
export function credentialRevision():number{return revision}
export function credentialsChanged(){
  revision++
  window.dispatchEvent(new Event('dts:credentials-changed'))
}

export function browserSessionChanged(){
  credentialsChanged()
  window.dispatchEvent(new Event('dts:session-changed'))
}

export function discardLegacyCredentials(){
  // Some privacy modes deny storage access. Cookie authentication is independent.
  try {
    localStorage.removeItem('dts_session_token')
    localStorage.removeItem('dts_api_token')
  } catch { /* storage unavailable */ }
}

async function authenticatedFetch(path:string,init:RequestInit={},json=false):Promise<Response>{
  const requestRevision=revision
  const headers=new Headers(init.headers)
  if(json&&!headers.has('Content-Type'))headers.set('Content-Type','application/json')
  const login=path.split('?')[0]==='/api/v1/auth/login'
  headers.set('X-DTS-Browser','1')
  const res=await fetch(path,{...init,headers,credentials:'same-origin',mode:'same-origin'})
  if(res.status===401&&!login&&requestRevision===revision){
    credentialsChanged()
    window.dispatchEvent(new Event('dts:unauthorized'))
  }
  return res
}

function retryAfter(res:Response):number{
  const seconds=Number(res.headers.get('Retry-After'))
  return Number.isFinite(seconds)&&seconds>0?seconds:0
}

export async function api<T>(path:string,init:RequestInit={}):Promise<T>{
  const res=await authenticatedFetch(path,init,true)
  if(res.status===204)return undefined as T
  let data:any
  try{data=await res.json()}catch{
    throw new ApiError(res.ok?'服务返回了无效的 JSON 响应':`HTTP ${res.status}`,res.status,retryAfter(res))
  }
  if(!res.ok)throw responseError(res,data)
  return data as T
}

export async function apiBlob(path:string,init:RequestInit={}):Promise<{blob:Blob;filename:string;sha256:string;hmac:string;ed25519:string;keyId:string;fingerprint:string}>{
  const res=await authenticatedFetch(path,init)
  if(!res.ok){
    const data=await res.json().catch(()=>null)
    throw responseError(res,data)
  }
  const cd=res.headers.get('Content-Disposition')||''
  const match=cd.match(/filename="?([^";]+)"?/i)
  return {blob:await res.blob(),filename:match?.[1]||'dts-report',sha256:res.headers.get('X-DTS-Content-SHA256')||'',hmac:res.headers.get('X-DTS-HMAC-SHA256')||'',ed25519:res.headers.get('X-DTS-Ed25519-Signature')||'',keyId:res.headers.get('X-DTS-Ed25519-Key-ID')||'',fingerprint:res.headers.get('X-DTS-Public-Key-Fingerprint-SHA256')||''}
}

function responseError(res:Response,data:any):ApiError{
  return new ApiError(data?.error||`HTTP ${res.status}`,res.status,retryAfter(res),data?.code||'',res.headers.get('X-Request-ID')||data?.request_id||'')
}

export function openEventSocket(taskId=''):WebSocket{
  const scheme=location.protocol==='https:'?'wss':'ws'
  const query=taskId?`?task_id=${encodeURIComponent(taskId)}`:''
  return new WebSocket(`${scheme}://${location.host}/api/v1/ws${query}`,['dts.v1'])
}

// A dropped socket leaves a gap in the event history. Fetch the current
// snapshot on reconnect before waiting for the next periodic update.
export function subscribeToEvents(
  onEvent: (message: Record<string, any>) => void,
  refresh: () => void,
): () => void {
  let current: WebSocket | undefined
  let retry: ReturnType<typeof setTimeout> | undefined
  let delay = 1000
  let stopped = false
  let disconnected = false

  function scheduleRetry() {
    if (stopped || retry !== undefined) return
    disconnected = true
    retry = setTimeout(() => {
      retry = undefined
      connect()
    }, delay)
    delay = Math.min(delay * 2, 30000)
  }
  function connect() {
    let socket: WebSocket
    const socketRevision = credentialRevision()
    try { socket = openEventSocket() } catch { scheduleRetry(); return }
    current = socket
    socket.onopen = () => {
      if (stopped || current !== socket) return
      if (disconnected) refresh()
    }
    socket.onmessage = event => {
      if (stopped || current !== socket) return
      let message
      try { message = JSON.parse(event.data) } catch { return }
      if (!message || typeof message !== 'object' || Array.isArray(message)) return
      delay = 1000
      onEvent(message)
    }
    socket.onclose = event => {
      if (stopped || current !== socket) return
      current = undefined
      if (event.code === 1008 && socketRevision === credentialRevision()) {
        credentialsChanged()
        window.dispatchEvent(new Event('dts:unauthorized'))
        return
      }
      scheduleRetry()
    }
  }
  function stop() {
    stopped = true
    clearTimeout(retry)
    current?.close()
    current = undefined
    window.removeEventListener('dts:unauthorized', stop)
  }
  window.addEventListener('dts:unauthorized', stop)
  connect()
  return stop
}

// List snapshots may be requested by the poller, the reconnect handler and
// the refresh button. Only the most recent request can update the view.
export function snapshotReader<T>(
  path: string,
  accept: (value: T) => void,
  failed: (message: string) => void,
) {
  let active: AbortController | undefined
  let disposed = false
  function close() {
    disposed = true
    active?.abort()
  }
  async function load() {
    if (disposed) return
    active?.abort()
    const controller = new AbortController()
    active = controller
    const timeout = setTimeout(() => controller.abort(new Error('读取超时，请刷新重试')), 12000)
    try {
      const result = await api<T>(path, { signal: controller.signal })
      if (active === controller && !disposed && !controller.signal.aborted) accept(result)
    } catch (error) {
      if (active === controller && !disposed) {
        const cause = controller.signal.aborted ? controller.signal.reason : error
        failed(cause instanceof Error ? cause.message : '读取失败')
      }
    } finally {
      clearTimeout(timeout)
      if (active === controller) active = undefined
    }
  }
  return { load, close }
}
