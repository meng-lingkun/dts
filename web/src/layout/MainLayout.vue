<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api, clearCredentials } from '../api/client'

const route=useRoute(),router=useRouter()
const identity=ref<{username:string;role:string;open_mode?:boolean}|null>(null)
async function loadIdentity(){
  try{identity.value=await api('/api/v1/auth/me')}
  catch{clearCredentials();await router.replace('/login')}
}
async function logout(){clearCredentials();await router.replace('/login')}
onMounted(loadIdentity)
</script>
<template><div class="shell"><aside class="sidebar"><div class="brand"><div class="logo">Q</div><div><b>QMigration</b><small>Database Migration</small></div></div><nav><RouterLink to="/" :class="{active:route.path==='/'}">首页</RouterLink><RouterLink to="/datasources" :class="{active:route.path.startsWith('/datasources')}">数据源</RouterLink><RouterLink to="/migrations" :class="{active:route.path.startsWith('/migrations')}">迁移任务</RouterLink><RouterLink to="/validation" :class="{active:route.path.startsWith('/validation')}">校验中心</RouterLink><RouterLink to="/cutover" :class="{active:route.path.startsWith('/cutover')}">割接中心</RouterLink><RouterLink to="/monitoring" :class="{active:route.path.startsWith('/monitoring')}">监控中心</RouterLink><RouterLink to="/workers" :class="{active:route.path.startsWith('/workers')}">Worker 节点</RouterLink><RouterLink to="/alerts" :class="{active:route.path.startsWith('/alerts')}">告警中心</RouterLink><RouterLink to="/audit" :class="{active:route.path.startsWith('/audit')}">操作审计</RouterLink><RouterLink v-if="identity?.role==='admin'" to="/users" :class="{active:route.path.startsWith('/users')}">用户与权限</RouterLink><RouterLink to="/settings" :class="{active:route.path.startsWith('/settings')}">访问设置</RouterLink></nav></aside><main><header><div><h2>QMigration 统一数据库迁移工具</h2><span>Unified Native Engine · Full Load · CDC · Validate · Cutover · Rollback</span></div><div style="display:flex;align-items:center;gap:12px"><el-tag v-if="identity" type="info">{{identity.username}} · {{identity.role}}</el-tag><el-tag type="success">V0.15 Unified Dev1</el-tag><el-button v-if="identity && !identity.open_mode" size="small" text @click="logout">退出</el-button></div></header><section class="content"><RouterView /></section></main></div></template>
