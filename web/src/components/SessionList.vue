<script setup lang="ts">
import { onMounted,ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api,browserSessionChanged } from '../api/client'
import { useRouter } from 'vue-router'

type Session={id:string;created_at:string;expires_at:string}
const props=defineProps<{endpoint:string}>()
const router=useRouter()
const sessions=ref<Session[]>([])
const current=ref('')
const loading=ref(false)
async function load(){
  loading.value=true
  try {
    const response=await api<{sessions:Session[];current_id:string}>(props.endpoint)
    sessions.value=response.sessions
    current.value=response.current_id
  } catch(error){ElMessage.error(String(error))}
  finally{loading.value=false}
}
async function revoke(session:Session){
  const isCurrent=session.id===current.value
  try{
    await ElMessageBox.confirm(isCurrent?'撤销当前会话并退出登录？':'撤销此登录会话？','撤销会话')
    await api(props.endpoint+'/'+encodeURIComponent(session.id),{method:'DELETE'})
    if(isCurrent){
      browserSessionChanged()
      await router.replace('/login')
    } else {await load()}
  } catch(error){if(error!=='cancel' && error!=='close')ElMessage.error(String(error))}
}
onMounted(load)
</script>
<template>
  <el-table :data="sessions" v-loading="loading" empty-text="没有已登记的有效会话">
    <el-table-column label="会话"><template #default="{row}">{{ row.id.slice(0,10) }} {{ row.id===current?'（当前）':'' }}</template></el-table-column>
    <el-table-column label="登记时间"><template #default="{row}">{{ new Date(row.created_at).toLocaleString() }}</template></el-table-column>
    <el-table-column label="到期时间"><template #default="{row}">{{ new Date(row.expires_at).toLocaleString() }}</template></el-table-column>
    <el-table-column label="操作" width="80"><template #default="{row}"><el-button text type="danger" @click="revoke(row)">撤销</el-button></template></el-table-column>
  </el-table>
</template>
