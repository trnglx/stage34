import {test} from 'node:test';
import assert from 'node:assert/strict';
import {guardAttempt} from '../dist/host-deadline.js';
const contract=()=>({identity:{node_id:'mac',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64)},
 command:{id:'C1',sha256:'b'.repeat(64)},deadline_at_ms:1100,max_runtime_ms:1000});
function fixture(){let wall=100,mono=0;const calls=[];
 return {calls,time:{wall:()=>wall,monotonic:()=>mono,wait:async ms=>{calls.push(['wait',ms]);wall+=ms;mono+=ms;}},
 boundary:{fence:()=>calls.push(['container-fence']),stopPhysical:async()=>{calls.push(['stop']);return {stopped:true};}},
 gate:{fence:()=>calls.push(['entry-fence'])},setWall:v=>{wall=v;}};}
test('host deadline fences native entry and container before confirming physical stop',async()=>{
 const f=fixture();assert.deepEqual(await guardAttempt(contract(),f.boundary,f.gate,f.time),{stopped:true});
 assert.deepEqual(f.calls.slice(-3),[['entry-fence'],['container-fence'],['stop']]);
 assert.equal(f.calls.filter(c=>c[0]==='wait').reduce((sum,c)=>sum+c[1],0),1000);
});
test('restart with expired wall deadline fences immediately',async()=>{
 const f=fixture();f.setWall(1200);await guardAttempt(contract(),f.boundary,f.gate,f.time);
 assert.equal(f.calls.some(c=>c[0]==='wait'),false);
});
test('wall clock rollback cannot extend the guardian beyond monotonic remaining time',async()=>{
 const f=fixture();let elapsed=0;f.time.wait=async ms=>{elapsed+=ms;f.setWall(0);};f.time.monotonic=()=>elapsed;
 await guardAttempt(contract(),f.boundary,f.gate,f.time);assert.equal(elapsed,1000);
});
test('lost stop acknowledgement retains supervision and retries fenced stop',async()=>{
 const f=fixture();f.setWall(1200);let attempts=0;
 f.boundary.stopPhysical=async()=>{attempts++;if(attempts===1)throw new Error('ack_lost');return {stopped:true};};
 await guardAttempt(contract(),f.boundary,f.gate,f.time);assert.equal(attempts,2);
 assert.equal(f.calls.filter(c=>c[0]==='entry-fence').length,2);
});
test('entry-fence storage failure still stops physically and prevents a success receipt until durable',async()=>{
 const f=fixture();f.setWall(1200);let attempts=0;f.gate.fence=()=>{if(++attempts===1)throw new Error('disk_full');};
 await guardAttempt(contract(),f.boundary,f.gate,f.time);
 assert.equal(f.calls.filter(c=>c[0]==='stop').length,2);assert.equal(attempts,2);
});

test('each available launch barrier is persisted even when the other storage fails',async()=>{
 for(const failed of ['entry','container']) {
  const f=fixture();f.setWall(1200);let recovered=false;let entry=false;let container=false;let retries=0;
  f.gate.fence=()=>{if(failed==='entry'&&!recovered)throw new Error('entry_storage');entry=true;};
  f.boundary.fence=()=>{if(failed==='container'&&!recovered)throw new Error('control_storage');container=true;};
  f.time.wait=async()=>{
   assert.equal(failed==='entry'?container:entry,true,'the healthy barrier must persist before retry');
   assert.equal(f.calls.filter(c=>c[0]==='stop').length,1);
   recovered=true;assert.equal(++retries,1);
  };
  await guardAttempt(contract(),f.boundary,f.gate,f.time);
  assert.equal(retries,1);assert.equal(entry&&container,true);
  assert.equal(f.calls.filter(c=>c[0]==='stop').length,2);
 }
});
