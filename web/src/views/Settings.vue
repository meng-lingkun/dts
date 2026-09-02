<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { api, apiToken, clearCredentials, setSessionToken } from '../api/client'
const token=ref(apiToken())
const identity=ref<any>(null)
async function load(){try{identity.value=await api('/api/v1/auth/me')}catch{identity.value=null}}
function save(){localStorage.setItem('qmigration_api_token',token.value.trim());setSessionToken('');ElMessage.success('静态 API Token 已保存；后续请求将使用该 Token');load()}
function clear(){clearCredentials();token.value='';identity.value=null;ElMessage.success('浏览器中的登录凭据已清除')}
onMounted(load)
</script>
<template><div><div class="page-title"><div><h1>访问设置</h1><p>推荐日常使用 QMigration 账号登录；静态 RBAC Token 用于自动化、运维和灾备访问。</p></div></div><div class="panel" style="max-width:820px"><el-descriptions v-if="identity" :column="2" border style="margin-bottom:20px"><el-descriptions-item label="当前身份">{{identity.username}}</el-descriptions-item><el-descriptions-item label="角色">{{identity.role}}</el-descriptions-item><el-descriptions-item label="开放模式">{{identity.open_mode?'是':'否'}}</el-descriptions-item></el-descriptions><el-form label-width="150"><el-form-item label="静态 API Token"><el-input v-model="token" type="password" show-password placeholder="Admin / DBA / Operator / Viewer Token"/></el-form-item><el-form-item><el-button type="primary" @click="save">改用静态 Token</el-button><el-button @click="clear">清除所有凭据</el-button></el-form-item></el-form><el-alert type="info" :closable="false" title="生产首启：QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD；自动化：QMIGRATION_RBAC_TOKENS=admin:tokenA,dba:tokenB,..."/><el-divider/><el-descriptions :column="1" border><el-descriptions-item label="Admin">用户、权限及全部 QMigration 管理权限</el-descriptions-item><el-descriptions-item label="DBA">数据源、迁移、割接、回切等数据库操作权限</el-descriptions-item><el-descriptions-item label="Operator">启动/暂停/恢复/校验等日常任务操作；不能修改数据源或执行割接/回切</el-descriptions-item><el-descriptions-item label="Viewer">业务资源只读，不能读取用户管理接口</el-descriptions-item></el-descriptions></div></div></template>
