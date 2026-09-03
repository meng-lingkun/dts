import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import App from './App.vue'
import router from './router'
import { useAuthStore } from './stores/auth'
import './style.css'

const app=createApp(App)
const pinia=createPinia()
app.use(pinia)
router.beforeEach(async to=>{
  if(to.meta.public)return true
  const auth=useAuthStore(pinia)
  if(!auth.identity&&!(await auth.refresh()))return {path:'/login',query:{redirect:to.fullPath}}
  if(to.meta.requiresAdmin&&auth.identity?.role!=='admin')return '/'
  return true
})
window.addEventListener('qmigration:unauthorized',()=>{
  const auth=useAuthStore(pinia)
  auth.logout()
  if(router.currentRoute.value.path!=='/login')void router.replace({path:'/login',query:{redirect:router.currentRoute.value.fullPath}})
})
app.use(router).use(ElementPlus).mount('#app')
