import { createApp } from 'vue'
import { createPinia } from 'pinia'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import App from './App.vue'
import router from './router'
import { useAuthStore } from './stores/auth'
import { discardLegacyCredentials } from './api/client'
import './style.css'

discardLegacyCredentials()
// Cookies are shared across tabs. Tear down streams and re-read identity after
// another tab switches accounts or logs out. No credentials enter the channel.
if ('BroadcastChannel' in window) {
  const sessions = new BroadcastChannel('dts-browser-session')
  window.addEventListener('dts:session-changed', () => sessions.postMessage('refresh'))
  sessions.onmessage = event => { if (event.data === 'refresh') window.location.reload() }
}
const app=createApp(App)
const pinia=createPinia()
app.use(pinia)
router.beforeEach(async to=>{
  if(to.meta.public)return true
  const auth=useAuthStore(pinia)
  if(!auth.identity){
    try{if(!(await auth.refresh()))return {path:'/login',query:{redirect:to.fullPath}}}
    catch{return {path:'/login',query:{redirect:to.fullPath,auth_error:'unavailable'}}}
  }
  if(to.meta.requiresAdmin&&auth.identity?.role!=='admin')return '/'
  return true
})
window.addEventListener('dts:unauthorized',()=>{
  const auth=useAuthStore(pinia)
  auth.invalidate()
  if(router.currentRoute.value.path!=='/login')void router.replace({path:'/login',query:{redirect:router.currentRoute.value.fullPath}})
})
window.addEventListener('dts:credentials-changed',()=>useAuthStore(pinia).invalidate())
app.use(router).mount('#app')
