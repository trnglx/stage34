import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync, readFileSync, chmodSync, symlinkSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {EventSpool} from '../dist/spool.js';
const event = () => ({schema_version:1, task_id:'task1',attempt_id:'attempt1',generation:1,
  node_id:'mac',provider:'codex',sdk_version:'0.167.0',sdk_sha256:'a'.repeat(64),
  cli_version:'0.167.0',cli_sha256:'b'.repeat(64),policy_sha256:'c'.repeat(64),
  status:'completed',native_type:'turn.completed',session_id:'native1'});
function fixture(t) {
  const root=mkdtempSync(join(tmpdir(),'event-spool-')); t.after(()=>rmSync(root,{recursive:true,force:true}));
  return root;
}
test('durable ordered replay and acknowledgement survive reopen with stable identities',t=>{
  const root=fixture(t); let spool=new EventSpool(root);
  const first=spool.append(event()); const second=spool.append({...event(),status:'failed'});
  spool.close(); spool=new EventSpool(root);
  assert.deepEqual(spool.pending(),[first,second]);
  assert.throws(()=>spool.acknowledge(first.event_id,second.sha256),/mismatch/);
  assert.deepEqual(spool.pending(),[first,second]);
  spool.acknowledge(first.event_id,first.sha256); spool.acknowledge(first.event_id,first.sha256);
  spool.close(); spool=new EventSpool(root); assert.deepEqual(spool.pending(),[second]); spool.close();
});
test('raw provider content and extra metadata fields never reach disk',async t=>{
  const root=fixture(t); const spool=new EventSpool(root);
  await spool.publish({...event(),prompt:'PRIVATE_SECRET'}, {text:'PRIVATE_SECRET'});
  assert.equal(JSON.stringify(spool.pending()).includes('PRIVATE_SECRET'),false);
  spool.close(); assert.equal(readFileSync(join(root,'events.sqlite3')).includes(Buffer.from('PRIVATE_SECRET')),false);
});
test('invalid provider metadata and exhausted capacity fail closed without consuming entries',t=>{
  const spool=new EventSpool(fixture(t),1);
  assert.throws(()=>spool.append({...event(),code:'PRIVATE_SECRET'}),/invalid_event/);
  assert.throws(()=>spool.append({...event(),generation:0}),/invalid_event/);
  assert.throws(()=>spool.append({...event(),usage:{input_tokens:NaN}}),/invalid_usage/);
  const first=spool.append(event()); assert.throws(()=>spool.append(event()),/capacity/);
  assert.deepEqual(spool.pending(),[first]);
  spool.acknowledge(first.event_id,first.sha256); assert.ok(spool.append(event())); spool.close();
});
test('refuses public directories and redirected database files',t=>{
  const root=fixture(t); chmodSync(root,0o755); assert.throws(()=>new EventSpool(root),/unsafe_spool_directory/);
  chmodSync(root,0o700); symlinkSync(join(root,'elsewhere'),join(root,'events.sqlite3'));
  assert.throws(()=>new EventSpool(root));
});
