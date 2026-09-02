<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { api } from '../api/client'
type Migration={id:string;name:string;status:string;validation_enabled?:boolean;progress?:number}
type Validation={id:string;task_id:string;table_id:string;chunk_id:string;status:string;source_rows:number;target_rows:number;source_checksum:string;target_checksum:string;last_error?:string}
const tasks=ref<Migration[]>([]), selected=ref(''), results=ref<Validation[]>([]), loading=ref(false)
async function load(){tasks.value=await api('/api/v1/migrations');if(!selected.value&&tasks.value.length)selected.value=tasks.value[0].id;if(selected.value)await loadResults()}
async function loadResults(){if(!selected.value)return;results.value=await api(`/api/v1/migrations/${selected.value}/validations`)}
async function validate(){if(!selected.value)return;loading.value=true;try{await api(`/api/v1/migrations/${selected.value}/validate`,{method:'POST'});ElMessage.success('已启动数据校验');await load()}catch(e:any){ElMessage.error(e.message)}finally{loading.value=false}}
async function repair(){if(!selected.value)return;try{await api(`/api/v1/migrations/${selected.value}/repair`,{method:'POST',body:JSON.stringify({})});ElMessage.success('异常 Chunk 已重新进入修复流程');await load()}catch(e:any){ElMessage.error(e.message)}}
onMounted(load)
</script>
<template><div><div class="page-title"><div><h1>校验中心</h1><p>Row Count · Chunk Checksum · 异常 Chunk 修复</p></div><el-button @click="load">刷新</el-button></div><div class="panel"><div style="display:flex;gap:12px;align-items:center;margin-bottom:16px"><el-select v-model="selected" filterable style="width:420px" @change="loadResults"><el-option v-for="t in tasks" :key="t.id" :label="`${t.name} · ${t.status}`" :value="t.id"/></el-select><el-button type="primary" :loading="loading" @click="validate">执行校验</el-button><el-button type="warning" @click="repair">修复异常 Chunk</el-button></div><el-table :data="results"><el-table-column prop="table_id" label="表" min-width="160"/><el-table-column prop="chunk_id" label="Chunk" min-width="160"/><el-table-column label="状态" width="120"><template #default="s"><el-tag :type="s.row.status==='SUCCESS'?'success':s.row.status==='MISMATCH'?'danger':'warning'">{{s.row.status}}</el-tag></template></el-table-column><el-table-column prop="source_rows" label="源行数" width="120"/><el-table-column prop="target_rows" label="目标行数" width="120"/><el-table-column prop="source_checksum" label="源 Checksum" min-width="190"/><el-table-column prop="target_checksum" label="目标 Checksum" min-width="190"/><el-table-column prop="last_error" label="错误" min-width="240"/></el-table></div></div></template>
