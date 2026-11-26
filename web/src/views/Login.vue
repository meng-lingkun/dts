<script setup lang="ts">
import { onMounted,reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { api, browserSessionChanged } from '../api/client'
import { useAuthStore } from '../stores/auth'

const router=useRouter()
const auth=useAuthStore()
const loading=ref(false)
const oidc=ref(false)
const passwordLogin=ref(true)
onMounted(async()=>{
  try{
    const modes=await api<{oidc:boolean;password_login:boolean}>('/api/v1/auth/providers')
    oidc.value=modes.oidc
    passwordLogin.value=modes.password_login
  }catch{oidc.value=false}
})
const form=reactive({username:'admin',password:''})
async function login(){
  if(loading.value)return
  loading.value=true
  try{
    await api('/api/v1/auth/login',{method:'POST',body:JSON.stringify(form)})
    browserSessionChanged()
    if(!(await auth.refresh()))throw new Error('登录状态校验失败，请重试')
    ElMessage.success('登录成功')
    const redirect=typeof router.currentRoute.value.query.redirect==='string'?router.currentRoute.value.query.redirect:'/'
    await router.replace(redirect)
  }catch(e:any){ElMessage.error(e.message)}finally{loading.value=false}
}
async function reconnect(){
  if(loading.value)return
  loading.value=true
  try{if(await auth.refresh())await router.replace(typeof router.currentRoute.value.query.redirect==='string'?router.currentRoute.value.query.redirect:'/');else ElMessage.error('凭据已失效，请重新登录')}
  catch{ElMessage.error('暂时无法连接服务器，凭据已保留')}
  finally{loading.value=false}
}
</script>
<template>
  <div class="login-page">
    <section class="login-hero">
      <div class="login-hero-mark">Q</div>
      <p class="eyebrow">UNIFIED DATA MOBILITY</p>
      <h1>让数据库迁移<br>可观测、可验证、可回退</h1>
      <p>从全量迁移、CDC 增量同步到割接与校验，在同一个控制面安全完成。</p>
      <div class="login-features"><span>端到端进度</span><span>多引擎支持</span><span>操作审计</span></div>
    </section>
    <section class="login-card panel">
      <el-alert v-if="router.currentRoute.value.query.auth_error==='unavailable'" type="warning" :closable="false" title="暂时无法校验身份，原有凭据已保留。"><el-button :loading="loading" @click="reconnect">重新连接</el-button></el-alert>
      <div class="login-brand"><div class="logo">Q</div><div><h2>欢迎回来</h2><p>登录 DTS 管理控制台</p></div></div>
      <el-form v-if="passwordLogin" label-position="top" @keyup.enter="login">
        <el-form-item label="用户名"><el-input v-model="form.username" size="large" autocomplete="username" placeholder="请输入用户名"/></el-form-item>
        <el-form-item label="密码"><el-input v-model="form.password" size="large" type="password" show-password autocomplete="current-password" placeholder="请输入密码"/></el-form-item>
        <el-button type="primary" size="large" class="login-submit" :loading="loading" :disabled="!form.username||!form.password" @click="login">登录控制台</el-button>
      </el-form>
      <a v-if="oidc" href="/api/v1/auth/oidc/start">使用企业账号登录</a>
      <el-alert v-if="!oidc&&!passwordLogin" type="error" :closable="false" title="企业登录配置不可用，请联系管理员"/>
      <div v-if="passwordLogin" class="login-security"><span class="status-dot"></span><span>首次登录后请立即在“用户与权限”中修改默认密码</span></div>
    </section>
  </div>
</template>
