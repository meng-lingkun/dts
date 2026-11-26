<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api } from '../api/client'
import { useAuthStore } from '../stores/auth'
import { canManageDatabase, canRunCutoverAction } from '../lib/access'
import { confirmWriteFreeze, ensureWriteFreeze, type LifecycleOperation } from '../lib/lifecycle'
type Migration={id:string;name:string;status:string;mode:string;source_datasource_id:string;target_datasource_id:string;cdc_lag_ms?:number;progress?:number}
const tasks=ref<Migration[]>([])
const auth=useAuthStore(),pending=reactive(new Set<string>())
const operations=ref<LifecycleOperation[]>([]),evidenceOpen=ref(false),evidenceTask=ref('')
async function evidence(t:Migration){try{operations.value=await api<LifecycleOperation[]>(`/api/v1/migrations/${t.id}/operations`);evidenceTask.value=t.name;evidenceOpen.value=true}catch(e:any){ElMessage.error(e.message)}}
async function load(){try{tasks.value=(await api<Migration[]>('/api/v1/migrations')).filter(t=>t.mode!=='FULL')}catch(e:any){ElMessage.error(e.message||'读取割接任务失败')}}
async function post(t:Migration,path:string,body:any={}){
  if(pending.has(t.id)||!canRunCutoverAction(auth.identity?.role,t.status,path))return
  pending.add(t.id)
  try{
    let sourceType:string|undefined
    if(path.endsWith('/freeze')||path==='cutover'||path==='rollback'){
      const sourceID=path.startsWith('rollback')?t.target_datasource_id:t.source_datasource_id
      const sources=await api<{id:string;type:string}[]>('/api/v1/datasources')
      sourceType=sources.find(s=>s.id===sourceID)?.type
    }
    if(path==='cutover/freeze'||path==='rollback/freeze'){await confirmWriteFreeze(t.id,path==='cutover/freeze'?'forward':'reverse',sourceType);ElMessage.success('已记录停写水位，请保持停写并等待目标追平');await load();return}
    if(path==='cutover'||path==='rollback'){
      await ensureWriteFreeze(t.id,path==='cutover'?'forward':'reverse',sourceType)
      await ElMessageBox.confirm(`确认执行 ${path==='cutover'?'正式割接':'正式回切'}：${t.name}？外部停写必须继续保持，服务端将核验最终水位。`,'高风险操作',{type:'warning',confirmButtonText:'确认执行'})
    }
    await api(`/api/v1/migrations/${t.id}/${path}`,{method:'POST',body:JSON.stringify(body)})
    ElMessage.success('操作已完成');await load()
  }catch(e:any){if(e!=='cancel'&&e!=='close')ElMessage.error(e.message||String(e))}
  finally{pending.delete(t.id)}
}
onMounted(load)
</script>
<template><div><div class="page-title"><div><h1>割接中心</h1><p>CDC 追平 · 割接门禁 · 反向同步 · 回切</p></div><el-button @click="load">刷新</el-button></div><div class="panel">
  <el-alert v-if="!canManageDatabase(auth.identity?.role)" type="info" :closable="false" title="当前身份只读；割接与回切需要 Admin 或 DBA 权限。"/>
  <el-alert type="info" :closable="false" title="流程：外部关闭业务写入 → 记录停写及最终水位 → 保持停写、等待 CDC 追平 → 执行割接或回切。停写确认有效期 15 分钟；延迟指标不能替代最终水位证明。"/>
  <el-table :data="tasks"><el-table-column prop="name" label="任务" min-width="220"/><el-table-column prop="status" label="状态" min-width="180"/><el-table-column label="CDC 延迟" width="130"><template #default="s">{{s.row.cdc_lag_ms??'未知'}} ms</template></el-table-column>
    <el-table-column label="证据" width="110"><template #default="s"><el-button size="small" @click="evidence(s.row)">执行记录</el-button></template></el-table-column>
    <el-table-column v-if="canManageDatabase(auth.identity?.role)" label="操作" min-width="600"><template #default="s">
      <el-button size="small" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'ready-cutover')" @click="post(s.row,'ready-cutover',{max_lag_ms:5000})">进入割接就绪</el-button>
      <el-button size="small" type="danger" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'cutover')" @click="post(s.row,'cutover')">执行割接</el-button>
      <el-button size="small" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'cutover/freeze')" @click="post(s.row,'cutover/freeze')">记录源库停写</el-button>
      <el-button size="small" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'rollback/prepare')" @click="post(s.row,'rollback/prepare')">准备回切</el-button>
      <el-button size="small" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'rollback/ready')" @click="post(s.row,'rollback/ready',{max_lag_ms:5000})">回切就绪</el-button>
      <el-button size="small" type="warning" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'rollback')" @click="post(s.row,'rollback')">执行回切</el-button>
      <el-button size="small" :disabled="pending.has(s.row.id)||!canRunCutoverAction(auth.identity?.role,s.row.status,'rollback/freeze')" @click="post(s.row,'rollback/freeze')">记录回切停写</el-button>
    </template></el-table-column>
  </el-table>
</div><el-drawer v-model="evidenceOpen" :title="`${evidenceTask} · 持久执行证据`" size="640px">
  <el-empty v-if="!operations.length" description="尚无停写或执行记录"/>
  <div v-for="o in operations" :key="o.id" style="margin-bottom:28px">
    <h3>{{o.direction==='forward'?'割接':'回切'}} · {{o.phase}}</h3>
    <p>操作号：{{o.id}} · {{o.actor}}</p><p>停写依据：{{o.freeze_evidence||'尚未确认停写'}}</p>
    <p v-if="o.table_lock_proof">停写方式：PostgreSQL 所选源表共享锁</p>
    <p>有效期至：{{o.expires_at}}</p><p>冻结水位：{{o.barrier?.position_type}} {{o.barrier?.position_value||'未采集'}}</p>
    <p>已应用水位：{{o.applied?.position_value||'尚未验证'}} · 验证时间：{{o.verified_at}}</p>
    <el-alert v-if="o.last_error" :title="o.last_error" type="error" :closable="false"/>
    <p>复制资源清理：{{o.cleanup_status||'尚未执行'}}</p><el-alert v-if="o.cleanup_error" :title="o.cleanup_error" type="warning" :closable="false"/>
    <el-table :data="o.steps"><el-table-column prop="name" label="步骤"/><el-table-column prop="status" label="结果"/><el-table-column prop="error" label="错误"/></el-table>
  </div>
</el-drawer></div></template>
