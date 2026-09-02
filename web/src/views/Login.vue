<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { api, setSessionToken } from '../api/client'

const router=useRouter()
const loading=ref(false)
const form=reactive({username:'admin',password:''})
async function login(){
  loading.value=true
  try{
    const r=await api<{token:string}>('/api/v1/auth/login',{method:'POST',body:JSON.stringify(form)})
    setSessionToken(r.token)
    ElMessage.success('登录成功')
    await router.replace('/')
  }catch(e:any){ElMessage.error(e.message)}finally{loading.value=false}
}
</script>
<template>
  <div class="login-page">
    <div class="login-card panel">
      <div class="brand login-brand"><div class="logo">Q</div><div><h2>QMigration</h2><p>Unified Database Migration Engine</p></div></div>
      <el-form label-position="top" @keyup.enter="login">
        <el-form-item label="用户名"><el-input v-model="form.username" autocomplete="username"/></el-form-item>
        <el-form-item label="密码"><el-input v-model="form.password" type="password" show-password autocomplete="current-password"/></el-form-item>
        <el-button type="primary" style="width:100%" :loading="loading" @click="login">登录</el-button>
      </el-form>
      <p class="muted" style="margin-top:16px">首次部署可通过 QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD 创建管理员账号。</p>
    </div>
  </div>
</template>
