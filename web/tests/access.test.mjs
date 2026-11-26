import { test } from 'node:test'
import assert from 'node:assert/strict'
import { canOperateMigration, canManageDatabase, canRunCutoverAction } from '../src/lib/access.ts'

test('UI permissions fail closed for viewer, missing and unknown roles', () => {
  for (const role of ['viewer', undefined, 'invalid']) {
    assert.equal(canOperateMigration(role), false)
    assert.equal(canManageDatabase(role), false)
    assert.equal(canRunCutoverAction(role, 'READY_FOR_CUTOVER', 'cutover'), false)
  }
  assert.equal(canOperateMigration('operator'), true)
  assert.equal(canManageDatabase('operator'), false)
})

test('cutover controls require both privilege and the exact lifecycle state', () => {
  const states = {'ready-cutover':'CDC_CATCHING_UP',cutover:'READY_FOR_CUTOVER','cutover/freeze':'READY_FOR_CUTOVER','rollback/prepare':'FINISHED','rollback/ready':'ROLLBACK_SYNCING',rollback:'ROLLBACK_READY','rollback/freeze':'ROLLBACK_READY'}
  for (const [action, state] of Object.entries(states)) {
    for (const role of ['admin', 'dba']) {
      assert.equal(canRunCutoverAction(role, state, action), true)
      assert.equal(canRunCutoverAction(role, 'FAILED', action), false)
      assert.equal(canRunCutoverAction(role, state, 'unknown-operation'), false)
    }
    assert.equal(canRunCutoverAction('operator', state, action), false)
  }
})
