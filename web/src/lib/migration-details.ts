const resources={tables:'tables',chunks:'chunks',checks:'precheck',assessment:'assessment',objects:'schema-objects/plan',validations:'validations',positions:'cdc',spool:'cdc/spool',deadLetters:'cdc/dlq',conflicts:'cdc/conflicts',jobs:'engine-jobs',logs:'logs?limit=500'} as const
type DetailKey=keyof typeof resources

export async function loadMigrationDetails(taskID:string,read:(path:string)=>Promise<unknown>,privileged:boolean){
  const values={} as Record<DetailKey,unknown|null>
  const errors:DetailKey[]=[]
  await Promise.all((Object.keys(resources) as DetailKey[]).map(async key=>{
    if(key==='deadLetters'&&!privileged){values[key]=null;return}
    try{values[key]=await read(`/api/v1/migrations/${encodeURIComponent(taskID)}/${resources[key]}`)}
    catch{values[key]=null;errors.push(key)}
  }))
  return {taskID,values,errors}
}
