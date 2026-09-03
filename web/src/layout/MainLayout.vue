<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const route=useRoute(),router=useRouter(),auth=useAuthStore()
const appVersion=__APP_VERSION__
const mobileOpen=ref(false)
const pageTitle=computed(()=>String(route.meta.title||'控制台'))
const groups=[
  {label:'概览',items:[{to:'/',label:'运行概览',mark:'概'}]},
  {label:'迁移工作台',items:[{to:'/datasources',label:'数据源',mark:'源'},{to:'/migrations',label:'迁移任务',mark:'迁'},{to:'/validation',label:'校验中心',mark:'验'},{to:'/cutover',label:'割接中心',mark:'割'}]},
  {label:'平台运维',items:[{to:'/monitoring',label:'监控中心',mark:'监'},{to:'/workers',label:'Worker 节点',mark:'W'},{to:'/alerts',label:'告警中心',mark:'警'},{to:'/audit',label:'操作审计',mark:'审'}]},
]
function active(path:string){return path==='/'?route.path==='/':route.path.startsWith(path)}
async function logout(){auth.logout();await router.replace('/login')}
watch(()=>route.fullPath,()=>{mobileOpen.value=false})
</script>

<template>
  <div class="shell">
    <button v-if="mobileOpen" class="nav-backdrop" aria-label="关闭导航" @click="mobileOpen=false"></button>
    <aside class="sidebar" :class="{open:mobileOpen}">
      <div class="brand"><div class="logo">Q</div><div><b>QMigration</b><small>Database Migration</small></div></div>
      <nav aria-label="主导航">
        <div v-for="group in groups" :key="group.label" class="nav-group">
          <span class="nav-label">{{group.label}}</span>
          <RouterLink v-for="item in group.items" :key="item.to" :to="item.to" :class="{active:active(item.to)}"><i>{{item.mark}}</i><span>{{item.label}}</span></RouterLink>
        </div>
        <div class="nav-group"><span class="nav-label">系统</span><RouterLink v-if="auth.identity?.role==='admin'" to="/users" :class="{active:active('/users')}"><i>权</i><span>用户与权限</span></RouterLink><RouterLink to="/settings" :class="{active:active('/settings')}"><i>设</i><span>访问设置</span></RouterLink></div>
      </nav>
      <div class="sidebar-footer"><span class="status-dot"></span><div><b>控制面在线</b><small>{{appVersion}}</small></div></div>
    </aside>
    <main>
      <header class="topbar">
        <div class="topbar-title"><button class="menu-button" aria-label="打开导航" @click="mobileOpen=true">☰</button><div><h2>{{pageTitle}}</h2><span>QMigration 统一数据库迁移平台</span></div></div>
        <div class="topbar-actions"><el-tag v-if="auth.identity" effect="plain">{{auth.identity.username}} · {{auth.identity.role}}</el-tag><el-button v-if="auth.identity&&!auth.identity.open_mode" text @click="logout">退出</el-button></div>
      </header>
      <section class="content"><RouterView /></section>
    </main>
  </div>
</template>
