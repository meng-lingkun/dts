<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { api } from '../api/client'
type Alert={id:string;severity:string;title:string;message:string;task_id?:string;acknowledged:boolean;created_at:string}
const items=ref<Alert[]>([])
async function load(){items.value=await api('/api/v1/alerts')}
async function ack(id:string){try{await api(`/api/v1/alerts/${id}/ack`,{method:'POST'});await load()}catch(e:any){ElMessage.error(e.message)}}
onMounted(load)
</script>
<template><div><div class="page-title"><div><h1>告警中心</h1><p>迁移失败、数据校验不一致等关键事件</p></div><el-button @click="load">刷新</el-button></div><div class="panel"><el-table :data="items"><el-table-column label="级别" width="100"><template #default="s"><el-tag :type="s.row.severity==='critical'?'danger':'warning'">{{s.row.severity}}</el-tag></template></el-table-column><el-table-column prop="title" label="标题" width="180"/><el-table-column prop="message" label="内容" min-width="300"/><el-table-column prop="task_id" label="任务" min-width="160"/><el-table-column prop="created_at" label="时间" width="200"/><el-table-column label="状态" width="110"><template #default="s"><el-tag v-if="s.row.acknowledged" type="success">已确认</el-tag><el-button v-else size="small" @click="ack(s.row.id)">确认</el-button></template></el-table-column></el-table></div></div></template>
