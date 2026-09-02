<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { api } from '../api/client'
type Audit={id:string;actor:string;action:string;resource_type:string;resource_id?:string;detail?:string;remote_addr?:string;created_at:string}
const items=ref<Audit[]>([])
async function load(){items.value=await api('/api/v1/audit')}
onMounted(load)
</script>
<template><div><div class="page-title"><div><h1>操作审计</h1><p>记录任务创建、启动、暂停、割接和告警确认等操作</p></div><el-button @click="load">刷新</el-button></div><div class="panel"><el-table :data="items"><el-table-column prop="actor" label="用户" width="130"/><el-table-column prop="action" label="操作" width="150"/><el-table-column prop="resource_type" label="资源类型" width="130"/><el-table-column prop="resource_id" label="资源 ID" min-width="170"/><el-table-column prop="detail" label="详情" min-width="220"/><el-table-column prop="remote_addr" label="来源" width="180"/><el-table-column prop="created_at" label="时间" width="200"/></el-table></div></div></template>
