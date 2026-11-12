import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {WorkerService} from '../dist/service.js';
const job=()=>({task_id:'task1',attempt_id:'attempt1',generation:1,node_id:'mac',provider:'codex',model:'example',
 policy_sha256:'a'.repeat(64),base_sha:'b'.repeat(40),workspace:'/isolated/work',sandbox:'workspace-write',timeout_ms:10000,prompt:'PRIVATE'});
const request=(id,operation,args)=>({schema_version:1,request_id:id,operation,arguments:args});
function fixture(t,execute){const root=mkdtempSync(join(tmpdir(),'worker-service-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
 return {root,driver:{execute,fence:async()=>{},events:async()=>[],inspect:async()=>({state:'unknown'})}};}
test('durable command receipt survives restart without a second native start',async t=>{
 let starts=0;const {root,driver}=fixture(t,async()=>{starts++;return {state:'applied',session_id:'native1'};});
 let worker=new WorkerService(root,'mac',1,driver);const req=request('C-start','dispatch',{operation:'start',job:job()});
 const first=await worker.handle(req);assert.equal(first.ok,true);assert.equal(first.data.state,'applied');worker.close();
 worker=new WorkerService(root,'mac',1,driver);assert.deepEqual(await worker.handle(req),first);assert.equal(starts,1);worker.close();
});
test('conflicting replay and generation reuse cannot replace native attempt',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'applied',session_id:'native1'}));const worker=new WorkerService(root,'mac',1,driver);
 await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));
 assert.equal((await worker.handle(request('C-start','dispatch',{operation:'start',job:{...job(),prompt:'changed'}}))).code,'idempotency_conflict');
 assert.equal((await worker.handle(request('C-other','dispatch',{operation:'start',job:{...job(),generation:2}}))).code,'attempt_binding_conflict');worker.close();
});
test('unknown start consumes capacity and requires confirmed stop',async t=>{
 let stopping=false;const {root,driver}=fixture(t,async op=>op==='cancel'?{state:'applied',stopped:stopping}:{state:'unknown'});
 const worker=new WorkerService(root,'mac',1,driver);await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));
 assert.equal((await worker.handle(request('C-second','dispatch',{operation:'start',job:{...job(),attempt_id:'attempt2'}}))).code,'worker_capacity');
 assert.equal((await worker.handle(request('C-cancel','cancel',{attempt_id:'attempt1'}))).data.state,'unknown');
 stopping=true;assert.equal((await worker.handle(request('C-cancel-confirmed','cancel',{attempt_id:'attempt1'}))).data.state,'applied');
 assert.equal((await worker.handle(request('C-late','dispatch',{operation:'start',job:job()}))).code,'attempt_cancelled');worker.close();
});
test('concurrent identical command does not start a second operation',async t=>{
 let release;let calls=0;const wait=new Promise(resolve=>{release=resolve;});
 const {root,driver}=fixture(t,async()=>{calls++;await wait;return {state:'applied',session_id:'native1'};});const worker=new WorkerService(root,'mac',1,driver);
 const req=request('C-start','dispatch',{operation:'start',job:job()});const first=worker.handle(req);
 assert.equal((await worker.handle(req)).code,'effect_unknown');assert.equal(calls,1);release();assert.equal((await first).ok,true);worker.close();
});
test('node revocation refuses new effects without claiming all processes stopped',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'applied',session_id:'native1'}));const worker=new WorkerService(root,'mac',1,driver);
 const revoked=await worker.handle(request('C-revoke','revoke',{}));assert.equal(revoked.data.requires_stop_reconciliation,true);
 assert.equal((await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}))).code,'node_revoked');
 assert.equal(JSON.stringify(await worker.handle(request('health','health',{}))).includes('PRIVATE'),false);worker.close();
});

test('a second journal owner is refused before it can rewrite active execution',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'applied',session_id:'native1'}));
 const worker=new WorkerService(root,'mac',1,driver);
 assert.throws(()=>new WorkerService(root,'mac',1,driver),/worker_already_running/);
 worker.close();
 const replacement=new WorkerService(root,'mac',1,driver);replacement.close();
});

test('revocation shares the durable mutating request namespace',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'applied',session_id:'native1'}));const worker=new WorkerService(root,'mac',1,driver);
 await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));
 assert.equal((await worker.handle(request('C-start','revoke',{}))).code,'idempotency_conflict');
 assert.equal((await worker.handle(request('health','health',{}))).data.revoked,false);worker.close();
});

test('reconnect restores a proven latest command into the durable job state',async t=>{
 const {root,driver}=fixture(t,async op=>op==='pause'?{state:'applied',stopped:true}:{state:'applied',session_id:'native1'});
 driver.inspect=async(_job,command)=>command?{state:'applied',session_id:'native1',command_id:command.id,command_sha256:command.sha256}:{state:'unknown'};
 let worker=new WorkerService(root,'mac',1,driver);
 await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));worker.close();
 worker=new WorkerService(root,'mac',1,driver);
 const recovered=await worker.handle(request('C-recover','reconnect',{attempt_id:'attempt1',command_id:'C-start'}));
 assert.equal(recovered.ok,true);
 const paused=await worker.handle(request('C-pause','dispatch',{operation:'pause',attempt_id:'attempt1'}));
 assert.equal(paused.ok,true,JSON.stringify(paused));assert.equal(paused.data.state,'applied');worker.close();
});

test('active cancellation is durable and a late start result cannot resurrect the job',async t=>{
 let release;const wait=new Promise(resolve=>{release=resolve;});let fenced=false;
 const {root,driver}=fixture(t,async op=>{
  if(op==='cancel')return {state:'applied',stopped:fenced};
  await wait;return {state:'applied',session_id:'native1'};
 });driver.fence=async()=>{fenced=true;};
 const worker=new WorkerService(root,'mac',1,driver);
 const starting=worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));
 try {
  const stop=await worker.handle(request('C-stop','cancel',{attempt_id:'attempt1'}));
  assert.equal(stop.data.state,'applied');
  assert.equal((await worker.handle(request('C-stop','revoke',{}))).code,'idempotency_conflict');
 } finally {release();await starting;worker.close();}
 const recovered=new WorkerService(root,'mac',1,driver);
 assert.equal((await recovered.handle(request('health','health',{}))).data.occupied,0);
 assert.equal((await recovered.handle(request('C-late','dispatch',{operation:'start',job:job()}))).code,'attempt_cancelled');recovered.close();
});

test('journal ownership is released by the OS after an ungraceful process exit',async t=>{
 const {spawn}=await import('node:child_process');const {once}=await import('node:events');
 const {root,driver}=fixture(t,async()=>({state:'unknown'}));
 const module=new URL('../dist/service.js',import.meta.url).href;
 const script=`import {WorkerService} from ${JSON.stringify(module)};
 const worker=new WorkerService(process.argv[1],'mac',1,{execute:async()=>({state:'unknown'}),fence:async()=>{},events:async()=>[],inspect:async()=>({state:'unknown'})});
 process.stdout.write('ready\\n');setInterval(()=>{void worker.handle({schema_version:1,request_id:'health',operation:'health',arguments:{}});},1000);`;
 const child=spawn(process.execPath,['--input-type=module','-e',script,root],{stdio:['ignore','pipe','pipe']});
 const exited=once(child,'exit');
 try {
  const ready=await Promise.race([once(child.stdout,'data'),new Promise((_,reject)=>setTimeout(()=>reject(new Error('owner_start_timeout')),2000))]);
  assert.match(String(ready[0]),/ready/);
  assert.throws(()=>new WorkerService(root,'mac',1,driver),/worker_already_running/);
 } finally {child.kill('SIGKILL');await exited;}
 const recovered=new WorkerService(root,'mac',1,driver);recovered.close();
});

test('a stale inspection cannot rewind a newer command or disclose private driver fields',async t=>{
 const {root,driver}=fixture(t,async op=>op==='pause'?{state:'applied',stopped:true,private_payload:'PRIVATE'}:
  {state:'applied',session_id:'native1',private_payload:'PRIVATE'});
 let inspectCalls=0;driver.inspect=async()=>{inspectCalls++;return {state:'unknown'};};
 const worker=new WorkerService(root,'mac',1,driver);
 assert.equal(JSON.stringify(await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}))).includes('PRIVATE'),false);
 assert.equal(JSON.stringify(await worker.handle(request('C-pause','dispatch',{operation:'pause',attempt_id:'attempt1'}))).includes('PRIVATE'),false);
 assert.equal((await worker.handle(request('recover-old','reconnect',{attempt_id:'attempt1',command_id:'C-start'}))).code,'stale_command');
 assert.equal(inspectCalls,0);worker.close();
});

test('a rejected same-process contender must not release the owner lock to another process',async t=>{
 const {spawnSync}=await import('node:child_process');
 const {root,driver}=fixture(t,async()=>({state:'unknown'}));
 const worker=new WorkerService(root,'mac',1,driver);
 try {
  assert.throws(()=>new WorkerService(root,'mac',1,driver),/worker_already_running/);
  const module=new URL('../dist/service.js',import.meta.url).href;
  const script=`import {WorkerService} from ${JSON.stringify(module)};
   try {const candidate=new WorkerService(process.argv[1],'mac',1,{execute:async()=>({state:'unknown'}),fence:async()=>{},events:async()=>[],inspect:async()=>({state:'unknown'})});candidate.close();process.stdout.write('acquired');}
   catch(error){process.stdout.write(error.message);}`;
  const result=spawnSync(process.execPath,['--input-type=module','-e',script,root],{encoding:'utf8',timeout:3000});
  assert.equal(result.status,0,result.stderr);assert.match(result.stdout,/worker_already_running/);
 } finally {worker.close();}
});

test('a worker journal cannot silently be reopened as a different node',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'unknown'}));
 const worker=new WorkerService(root,'mac',1,driver);worker.close();
 assert.throws(()=>new WorkerService(root,'different-node',1,driver),/worker_identity_conflict/);
 const recovered=new WorkerService(root,'mac',1,driver);recovered.close();
});

test('an uncertain cancellation retains its tombstone across a late start and restart',async t=>{
 let release;const wait=new Promise(resolve=>{release=resolve;});
 const {root,driver}=fixture(t,async op=>{
  if(op==='cancel')return {state:'unknown'};
  await wait;return {state:'applied',session_id:'native1'};
 });
 let worker=new WorkerService(root,'mac',1,driver);
 const starting=worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));
 const cancellation=await worker.handle(request('C-stop','cancel',{attempt_id:'attempt1'}));
 assert.equal(cancellation.data.state,'unknown');release();
 assert.equal((await starting).data.state,'unknown');worker.close();
 worker=new WorkerService(root,'mac',1,driver);
 assert.equal((await worker.handle(request('C-resume','dispatch',{operation:'resume',attempt_id:'attempt1'}))).code,'attempt_cancel_requested');
 assert.equal((await worker.handle(request('health','health',{}))).data.occupied,1);worker.close();
});

test('reconnect without matching durable command proof cannot unlock a quarantined job',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'applied',session_id:'native1'}));
 driver.inspect=async(_job,command)=>({state:'applied',session_id:'native1',command_id:command.id,command_sha256:'0'.repeat(64)});
 let worker=new WorkerService(root,'mac',1,driver);
 await worker.handle(request('C-start','dispatch',{operation:'start',job:job()}));worker.close();
 worker=new WorkerService(root,'mac',1,driver);
 assert.equal((await worker.handle(request('recover','reconnect',{attempt_id:'attempt1',command_id:'C-start'}))).data.state,'unknown');
 assert.equal((await worker.handle(request('C-pause','dispatch',{operation:'pause',attempt_id:'attempt1'}))).code,'illegal_transition');worker.close();
});

test('receipt storage failure after driver success is an unknown effect, not a refusal',async t=>{
 const {DatabaseSync}=await import('node:sqlite');let calls=0;
 const {root,driver}=fixture(t,async()=>{calls++;return {state:'applied',session_id:'native1'};});
 const worker=new WorkerService(root,'mac',1,driver);
 const fault=new DatabaseSync(join(root,'worker.sqlite3'));
 fault.exec("CREATE TRIGGER fail_receipt BEFORE UPDATE ON command BEGIN SELECT RAISE(FAIL,'disk full'); END");
 const req=request('C-start','dispatch',{operation:'start',job:job()});
 const result=await worker.handle(req);
 assert.equal(result.ok,true);assert.equal(result.code,'effect_unknown');
 assert.equal((await worker.handle(req)).code,'effect_unknown');assert.equal(calls,1);
 fault.close();worker.close();
});

test('an exact node revocation replays its durable receipt after restart',async t=>{
 const {root,driver}=fixture(t,async()=>{throw new Error('unexpected native effect');});
 let worker=new WorkerService(root,'mac',1,driver);
 const command=request('C-revoke-replay','revoke',{});
 const first=await worker.handle(command);assert.equal(first.ok,true);worker.close();
 worker=new WorkerService(root,'mac',1,driver);
 try {assert.deepEqual(await worker.handle(command),first);} finally {worker.close();}
});

test('a full-manifest cancellation fences a start that has not reached this worker',async t=>{
 let starts=0;let fences=0;
 const {root,driver}=fixture(t,async operation=>{if(operation==='start')starts++;return {state:'applied',stopped:true};});
 driver.fence=async()=>{fences++;};let worker=new WorkerService(root,'mac',1,driver);
 const stop=request('C-cancel-before-start','cancel',{attempt_id:'attempt1',job:job()});
 const cancelled=await worker.handle(stop);assert.equal(cancelled.ok,true,JSON.stringify(cancelled));
 assert.equal(cancelled.data.state,'applied');assert.equal(fences,1);worker.close();
 worker=new WorkerService(root,'mac',1,driver);
 try {
  assert.deepEqual(await worker.handle(stop),cancelled);
  assert.equal((await worker.handle(request('C-late-start','dispatch',{operation:'start',job:job()}))).code,'attempt_cancelled');
  assert.equal(starts,0);assert.equal((await worker.handle(request('H','health',{}))).data.occupied,0);
 } finally {worker.close();}
});

test('worker journal identity persists across reboot and gates every requested effect',async t=>{
 let starts=0;const {root,driver}=fixture(t,async()=>{starts++;return {state:'applied',session_id:'native1'};});
 let worker=new WorkerService(root,'mac',1,driver);const first=(await worker.handle(request('H1','health',{}))).data.journal_id;worker.close();
 worker=new WorkerService(root,'mac',1,driver);
 try {
  assert.equal((await worker.handle(request('H2','health',{}))).data.journal_id,first);
  const refused=await worker.handle(request('C-wrong-journal','dispatch',{operation:'start',job:job(),expected_journal_id:'00000000-0000-0000-0000-000000000000'}));
  assert.equal(refused.code,'worker_identity_conflict');assert.equal(starts,0);
  assert.equal((await worker.handle(request('C-enrolled','dispatch',{operation:'start',job:job(),expected_journal_id:first}))).ok,true);
 } finally {worker.close();}
});


test('checkpoint capability is advertised only by a supporting native driver',async t=>{
 const {root,driver}=fixture(t,async()=>({state:'unknown'}));
 const worker=new WorkerService(root,'mac',1,driver);
 try {
  assert.equal((await worker.handle(request('H-old','health',{}))).data.checkpoint_restore,false);
  driver.checkpoint_restore=true;
  assert.equal((await worker.handle(request('H-new','health',{}))).data.checkpoint_restore,true);
 } finally {worker.close();}
});
