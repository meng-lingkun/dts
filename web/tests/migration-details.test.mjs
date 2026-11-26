import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadMigrationDetails } from '../src/lib/migration-details.ts'

test('failed spool and table reads remain unknown and do not look healthy', async () => {
  const result=await loadMigrationDetails('task',async path=>{
    if(path.endsWith('/cdc/spool')||path.endsWith('/tables'))throw new Error('metadata unavailable')
    return []
  },true)
  assert.equal(result.values.spool,null)
  assert.equal(result.values.tables,null)
  assert.deepEqual(result.errors.sort(),['spool','tables'])
  assert.deepEqual(result.values.chunks,[])
})

test('unprivileged detail views never request retained business row images', async () => {
  const paths=[]
  const result=await loadMigrationDetails('task/encoded',async path=>{paths.push(path);return []},false)
  assert.equal(paths.some(path=>path.includes('/cdc/dlq')),false)
  assert.equal(paths.every(path=>path.startsWith('/api/v1/migrations/task%2Fencoded/')),true)
  assert.equal(result.values.deadLetters,null)
  assert.deepEqual(result.errors,[])
})
