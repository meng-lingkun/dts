<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { api, setSessionToken } from '../api/client'
import { useAuthStore } from '../stores/auth'

const router=useRouter()
const auth=useAuthStore()
const loading=ref(false)
const form=reactive({username:'admin',password:''})
async function login(){
  loading.value=true
  try{
    const r=await api<{token:string}>('/api/v1/auth/login',{method:'POST',body:JSON.stringify(form)})
    setSessionToken(r.token)
    await auth.refresh()
    ElMessage.success('登录成功')
    const redirect=typeof router.currentRoute.value.query.redirect==='string'?router.currentRoute.value.query.redirect:'/'
    await router.replace(redirect)
  }catch(e:any){ElMessage.error(e.message)}finally{loading.value=false}
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
      <div class="login-brand"><div class="logo">Q</div><div><h2>欢迎回来</h2><p>登录 QMigration 管理控制台</p></div></div>
      <el-form label-position="top" @keyup.enter="login">
        <el-form-item label="用户名"><el-input v-model="form.username" size="large" autocomplete="username" placeholder="请输入用户名"/></el-form-item>
        <el-form-item label="密码"><el-input v-model="form.password" size="large" type="password" show-password autocomplete="current-password" placeholder="请输入密码"/></el-form-item>
        <el-button type="primary" size="large" class="login-submit" :loading="loading" :disabled="!form.username||!form.password" @click="login">登录控制台</el-button>
      </el-form>
      <div class="login-security"><span class="status-dot"></span><span>首次登录后请立即在“用户与权限”中修改默认密码</span></div>
    </section>
  </div>
</template>
