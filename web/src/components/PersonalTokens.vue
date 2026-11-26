<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api } from '../api/client'

type Token = {id:string;name:string;created_at:string;expires_at:string}
const endpoint='/api/v1/auth/personal-tokens'
const items=ref<Token[]>([])
const name=ref('')
const days=ref(30)
const loading=ref(false)
const creating=ref(false)
const raw=ref('')
const visible=ref(false)

async function load(){
  loading.value=true
  try{items.value=(await api<{tokens:Token[]}>(endpoint)).tokens}
  catch(error){ElMessage.error(String(error))}
  finally{loading.value=false}
}
async function create(){
  if(!name.value.trim()||creating.value)return
  creating.value=true
  try{
    const expires_at=new Date(Date.now()+days.value*86400000).toISOString()
    const response=await api<{token:string}>(endpoint,{method:'POST',body:JSON.stringify({name:name.value.trim(),expires_at})})
    raw.value=response.token
    visible.value=true
    name.value=''
    await load()
  }catch(error){ElMessage.error(String(error))}
  finally{creating.value=false}
}
async function revoke(token:Token){
  try{
    await ElMessageBox.confirm('使用此令牌的客户端将立即失去访问权限。','撤销 '+token.name)
    await api(endpoint+'/'+encodeURIComponent(token.id),{method:'DELETE'})
    await load()
  }catch(error){if(error!=='cancel'&&error!=='close')ElMessage.error(String(error))}
}
onMounted(load)
</script>
<template>
  <h3>我的 API 令牌</h3>
  <el-form inline @submit.prevent="create">
    <el-form-item label="名称"><el-input v-model="name" maxlength="80" placeholder="例如：值班巡检"/></el-form-item>
    <el-form-item label="有效期"><el-select v-model="days" style="width:110px" aria-label="有效期">
      <el-option v-for="d in [7,30,90,365]" :key="d" :value="d" :label="d+' 天'"/>
    </el-select></el-form-item>
    <el-button type="primary" native-type="submit" :disabled="!name.trim()" :loading="creating">创建令牌</el-button>
  </el-form>
  <el-table :data="items" v-loading="loading" empty-text="没有有效的个人 API 令牌">
    <el-table-column prop="name" label="名称"/>
    <el-table-column label="创建时间"><template #default="{row}">{{ new Date(row.created_at).toLocaleString() }}</template></el-table-column>
    <el-table-column label="到期时间"><template #default="{row}">{{ new Date(row.expires_at).toLocaleString() }}</template></el-table-column>
    <el-table-column label="操作" width="80"><template #default="{row}"><el-button text type="danger" @click="revoke(row)">撤销</el-button></template></el-table-column>
  </el-table>
  <el-dialog v-model="visible" title="保存个人 API 令牌" destroy-on-close @closed="raw=''">
    <p>请复制令牌并存入客户端的凭据存储。关闭后无法再次查看。</p>
    <el-input aria-label="新建的 API 令牌" :model-value="raw" readonly/>
    <template #footer><el-button type="primary" @click="visible=false">已保存</el-button></template>
  </el-dialog>
</template>
