<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api } from '../api/client'
type Migration={id:string;name:string;status:string;mode:string;cdc_lag_ms?:number;progress?:number}
const tasks=ref<Migration[]>([])
async function load(){tasks.value=(await api<Migration[]>('/api/v1/migrations')).filter(t=>t.mode!=='FULL')}
async function post(t:Migration,path:string,body:any={}){try{if(path==='cutover'||path==='rollback')await ElMessageBox.confirm(`确认执行 ${path==='cutover'?'正式割接':'正式回切'}：${t.name}？此操作会改变迁移生命周期。`,'高风险操作',{type:'warning',confirmButtonText:'确认执行'});await api(`/api/v1/migrations/${t.id}/${path}`,{method:'POST',body:JSON.stringify(body)});ElMessage.success('操作已提交');await load()}catch(e:any){if(e!=='cancel')ElMessage.error(e.message||String(e))}}
onMounted(load)
</script>
<template><div><div class="page-title"><div><h1>割接中心</h1><p>CDC 追平 · 割接门禁 · 反向同步 · 回切</p></div><el-button @click="load">刷新</el-button></div><div class="panel"><el-table :data="tasks"><el-table-column prop="name" label="任务" min-width="220"/><el-table-column prop="status" label="状态" min-width="180"/><el-table-column label="CDC 延迟" width="130"><template #default="s">{{s.row.cdc_lag_ms||0}} ms</template></el-table-column><el-table-column label="操作" min-width="440"><template #default="s"><el-button size="small" @click="post(s.row,'ready-cutover',{max_lag_ms:5000})">进入割接就绪</el-button><el-button size="small" type="danger" @click="post(s.row,'cutover')">执行割接</el-button><el-button size="small" @click="post(s.row,'rollback/prepare')">准备回切</el-button><el-button size="small" @click="post(s.row,'rollback/ready',{max_lag_ms:5000})">回切就绪</el-button><el-button size="small" type="warning" @click="post(s.row,'rollback')">执行回切</el-button></template></el-table-column></el-table></div></div></template>
