import { ElMessageBox } from 'element-plus'
import { h } from 'vue'
import { api } from '../api/client'

export type LifecycleOperation={id:string;direction:string;phase:string;actor:string;freeze_evidence:string;table_lock_proof?:string;confirmed_at:string;expires_at:string;barrier:{position_type:string;position_value:string};applied:{position_value:string};verified_at:string;last_error?:string;cleanup_status?:string;cleanup_error?:string;steps:{name:string;status:string;error?:string}[]}
const attempts=new Map<string,{evidence:string;key:string}>()

export async function confirmWriteFreeze(id:string,direction:'forward'|'reverse',sourceType?:string){
  let holdTableLocks=false
  const description=`${direction==='forward'?'源库':'当前业务写入库'}的业务写入口须已关闭，存量写事务须已排空。请保持停写至操作完成，并填写停写工单、执行人及验证依据（10–2000 字）。`
  const message=(sourceType==='postgresql'||sourceType==='polardb_postgresql')
    ? h('div', [h('p',description),h('label',[h('input',{type:'checkbox',onChange:(e:Event)=>{holdTableLocks=(e.target as HTMLInputElement).checked}}),' 在所选源表上持有共享锁，最长 15 分钟。期间数据库会阻塞这些表上的写入。'])])
    : description
  const {value}=await ElMessageBox.prompt(
    message,
    '记录外部停写确认',
    {inputType:'textarea',inputValidator:(v:string)=>v.trim().length>=10&&v.trim().length<=2000||'请填写 10–2000 字的停写依据',confirmButtonText:'已完成停写，采集水位',cancelButtonText:'取消'},
  )
  const evidence=value.trim(),scope=`${id}/${direction}/${holdTableLocks}`
  let attempt=attempts.get(scope)
  if(!attempt||attempt.evidence!==evidence){attempt={evidence,key:crypto.randomUUID()};attempts.set(scope,attempt)}
  const result=await api<LifecycleOperation>(`/api/v1/migrations/${id}/${direction==='forward'?'cutover':'rollback'}/freeze`,{method:'POST',body:JSON.stringify({writes_frozen:true,hold_table_locks:holdTableLocks,evidence,idempotency_key:attempt.key})})
  attempts.delete(scope)
  return result
}

export async function ensureWriteFreeze(id:string,direction:'forward'|'reverse',sourceType?:string){
  const operations=await api<LifecycleOperation[]>(`/api/v1/migrations/${id}/operations`)
  const op=operations.find(o=>o.direction===direction)
  if(op?.phase==='NEEDS_REVIEW')throw new Error('存在结果不确定的持久步骤，请先核查执行证据')
  if(!op?.freeze_evidence||Date.parse(op.expires_at)<=Date.now())await confirmWriteFreeze(id,direction,sourceType)
}
