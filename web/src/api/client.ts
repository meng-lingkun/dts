export class ApiError extends Error {
  status: number
  constructor(message:string,status:number){super(message);this.status=status}
}

export function sessionToken(): string {
  return localStorage.getItem('qmigration_session_token') || ''
}
export function apiToken(): string {
  return localStorage.getItem('qmigration_api_token') || import.meta.env.VITE_QMIGRATION_API_TOKEN || ''
}
export function setSessionToken(token:string){
  if(token) localStorage.setItem('qmigration_session_token',token)
  else localStorage.removeItem('qmigration_session_token')
}
export function clearCredentials(){
  localStorage.removeItem('qmigration_session_token')
  localStorage.removeItem('qmigration_api_token')
}

export async function api<T>(path:string,init:RequestInit={}):Promise<T>{
  const session=sessionToken(), staticToken=apiToken()
  const headers:Record<string,string>={'Content-Type':'application/json',...((init.headers||{}) as Record<string,string>)}
  if(session) headers['Authorization']=`Bearer ${session}`
  else if(staticToken) headers['X-QMigration-API-Token']=staticToken
  const res=await fetch(path,{...init,headers})
  const data=await res.json().catch(()=>({}))
  if(!res.ok){
    if(res.status===401){clearCredentials();window.dispatchEvent(new Event('qmigration:unauthorized'))}
    throw new ApiError(data.error||`HTTP ${res.status}`,res.status)
  }
  return data as T
}


export async function apiBlob(path:string,init:RequestInit={}):Promise<{blob:Blob;filename:string;sha256:string;hmac:string;ed25519:string;keyId:string;fingerprint:string}>{
  const session=sessionToken(), staticToken=apiToken()
  const headers:Record<string,string>={...((init.headers||{}) as Record<string,string>)}
  if(session) headers['Authorization']=`Bearer ${session}`
  else if(staticToken) headers['X-QMigration-API-Token']=staticToken
  const res=await fetch(path,{...init,headers})
  if(!res.ok){
    const data=await res.json().catch(()=>({})) as any
    if(res.status===401){clearCredentials();window.dispatchEvent(new Event('qmigration:unauthorized'))}
    throw new ApiError(data.error||`HTTP ${res.status}`,res.status)
  }
  const cd=res.headers.get('Content-Disposition')||''
  const match=cd.match(/filename="?([^";]+)"?/i)
  return {blob:await res.blob(),filename:match?.[1]||'qmigration-report',sha256:res.headers.get('X-QMigration-Content-SHA256')||'',hmac:res.headers.get('X-QMigration-HMAC-SHA256')||'',ed25519:res.headers.get('X-QMigration-Ed25519-Signature')||'',keyId:res.headers.get('X-QMigration-Ed25519-Key-ID')||'',fingerprint:res.headers.get('X-QMigration-Public-Key-Fingerprint-SHA256')||''}
}

function base64url(value:string):string{
  const bytes=new TextEncoder().encode(value)
  let binary=''
  bytes.forEach(b=>binary+=String.fromCharCode(b))
  return btoa(binary).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')
}

export function openEventSocket(taskId=''):WebSocket{
  const scheme=location.protocol==='https:'?'wss':'ws'
  const query=taskId?`?task_id=${encodeURIComponent(taskId)}`:''
  const token=sessionToken()||apiToken()
  const protocols=token?['qmigration.v1',`auth.${base64url(token)}`]:['qmigration.v1']
  return new WebSocket(`${scheme}://${location.host}/api/v1/ws${query}`,protocols)
}
