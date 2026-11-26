export function canOperateMigration(role?:string):boolean{
  return role==='admin'||role==='dba'||role==='operator'
}

export function canManageDatabase(role?:string):boolean{
  return role==='admin'||role==='dba'
}

const cutoverStates:Record<string,string>={
  'ready-cutover':'CDC_CATCHING_UP',
  'cutover':'READY_FOR_CUTOVER',
  'cutover/freeze':'READY_FOR_CUTOVER',
  'rollback/prepare':'FINISHED',
  'rollback/ready':'ROLLBACK_SYNCING',
  'rollback':'ROLLBACK_READY',
  'rollback/freeze':'ROLLBACK_READY',
}

export function canRunCutoverAction(role:string|undefined,status:string,action:string):boolean{
  return canManageDatabase(role)&&Object.hasOwn(cutoverStates,action)&&cutoverStates[action]===status
}
