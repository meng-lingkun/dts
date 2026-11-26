<script setup lang="ts">
import { onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { useAuthStore } from '../stores/auth'
import SessionList from '../components/SessionList.vue'
import PersonalTokens from '../components/PersonalTokens.vue'

const auth=useAuthStore()
onMounted(async()=>{
  try { await auth.refresh() } catch { ElMessage.error('暂时无法校验身份，请重试') }
})
</script>

<template>
  <div class="page-title"><h1>访问设置</h1></div>
  <div class="panel" style="max-width:820px">
    <el-descriptions v-if="auth.identity" :column="2" border style="margin-bottom:20px">
      <el-descriptions-item label="当前身份">{{auth.identity.username}}</el-descriptions-item>
      <el-descriptions-item label="角色">{{auth.identity.role}}</el-descriptions-item>
      <el-descriptions-item label="开放模式">{{auth.identity.open_mode?'是':'否'}}</el-descriptions-item>
    </el-descriptions>
    <el-alert type="info" :closable="false" title="自动化客户端可使用管理员配置的 DTS_RBAC_TOKENS。"/>
    <template v-if="auth.identity?.authenticated"><h3>我的登录会话</h3><SessionList endpoint="/api/v1/auth/sessions"/></template>
    <PersonalTokens v-if="auth.identity?.personal_tokens_enabled"/>
    <el-divider/>
    <el-descriptions :column="1" border>
      <el-descriptions-item label="Admin">用户、权限及全部 DTS 管理权限</el-descriptions-item>
      <el-descriptions-item label="DBA">数据源、迁移、割接、回切等数据库操作权限</el-descriptions-item>
      <el-descriptions-item label="Operator">启动、暂停、恢复和校验等日常任务操作</el-descriptions-item>
      <el-descriptions-item label="Viewer">业务资源只读</el-descriptions-item>
    </el-descriptions>
  </div>
</template>
