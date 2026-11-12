import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {NativeContainerDriver} from '../dist/native-driver.js';
import {NativeEntryGate} from '../dist/entry-gate.js';
import {serveEntryGate} from '../dist/entry-rpc.js';
import {EventSpool} from '../dist/spool.js';
import {WorkerService} from '../dist/service.js';
const command=(name,digit)=>({command_id:name,command_sha256:digit.repeat(64)});
async function fixture(t,{guardianAlive=true,mode='',provider='codex',brokered=false,network='none'}={}) {
 const root=mkdtempSync(join(process.platform==='darwin'?'/tmp':tmpdir(),'nd-'));const entry=join(root,'entry');mkdirSync(entry,{mode:0o700});
 const gate=new NativeEntryGate(join(root,'entry.sqlite3'));const server=await serveEntryGate(join(entry,'gate.sock'),gate);
 t.after(async()=>{await new Promise(resolve=>server.close(resolve));gate.close();rmSync(root,{recursive:true,force:true});});
 const source=join(root,'mirror');mkdirSync(source,{mode:0o700});
 const git=(...args)=>execFileSync('/usr/bin/git',['-c','core.hooksPath=/dev/null',...args],{env:{PATH:'/usr/bin:/bin',HOME:root,GIT_CONFIG_NOSYSTEM:'1',GIT_CONFIG_GLOBAL:'/dev/null'},encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
 git('init',source);writeFileSync(join(source,'task.txt'),'source\n');git('-C',source,'add','task.txt');git('-C',source,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','base');
 const base=git('-C',source,'rev-parse','HEAD');const credential=join(root,'provider.env');writeFileSync(credential,(provider==='codex'?'CODEX_API_KEY':'ANTHROPIC_API_KEY')+'=fixture-not-a-key\n',{mode:0o600});
 const state=join(root,'docker.json');writeFileSync(state,JSON.stringify({calls:[],container:null,starts:0,mode}));
 const executable=join(root,'docker.cjs');writeFileSync(executable,`#!${process.execPath}
const fs=require('node:fs'),path=require('node:path');const file=path.join(__dirname,'docker.json');const state=JSON.parse(fs.readFileSync(file));const args=process.argv.slice(2);state.calls.push(args);let output='';let failed=false;
if(args[3]==='ls')output=state.container?.id??'';
else if(args[3]==='inspect')output=JSON.stringify(state.container);
else if(args[3]==='create'){
 const labels={},mounts={};for(let i=0;i<args.length;i++){
  if(args[i]==='--label'){const pair=args[++i].split('=');labels[pair[0]]=pair[1];}
  else if(args[i]==='--mount'){const parts=args[++i].split(',');const source=parts.find(p=>p.startsWith('src='))?.slice(4),dest=parts.find(p=>p.startsWith('dst='))?.slice(4);mounts[dest]=source;}
 }
 state.container={id:'d'.repeat(64),image:args.find(v=>/^sha256:[a-f0-9]{64}$/.test(v)),running:false,labels,mounts};output=state.container.id;
}else if(args[3]==='start'){
 state.starts++;state.container.running=true;const control=JSON.parse(fs.readFileSync(path.join(state.container.mounts['/run/orch-control'],'command.json')));
 state.lastInput=control.input;state.lastTicket=control.entry_ticket;
 fs.writeFileSync(path.join(state.container.mounts['/run/orch-output'],control.command.id+'.ready.json'),JSON.stringify({schema_version:1,command_id:control.command.id,command_sha256:control.command.sha256,attempt_id:control.identity.attempt_id,phase:'running',session_id:'native-fixture-1'}),{mode:0o600});
 output=state.container.id;if(state.mode==='lost-start-ack')failed=true;
}else if(args[3]==='stop')state.container.running=false;else if(args[3]==='rm')state.container=null;else failed=true;
fs.writeFileSync(file,JSON.stringify(state));if(failed)process.exit(1);process.stdout.write(output);
`,{mode:0o700});
 const runtime={provider,sdk_version:provider==='codex'?'0.167.0':'0.3.319',sdk_sha256:'c'.repeat(64),cli_path:'/approved/'+provider,cli_version:'1.2.3',cli_sha256:'e'.repeat(64)};
 const profile={container:{executable,endpoint:'unix:///fixture.sock',image_id:'sha256:'+'f'.repeat(64),state_root:join(root,'containers'),uid:process.getuid(),gid:process.getgid()||1000,cpus:1,memory_mb:512,pids_limit:32,network:'none',max_runtime_ms:10000},
  source_repository:source,git_executable:'/usr/bin/git',workspace_contract_root:'/logical/work',policy_sha256:'a'.repeat(64),readiness_ms:1000,
  providers:{[provider]:{runtime,models:['fixture-model'],credential_file:credential}}};
 const resources={entry_gate:gate,entry_database:join(root,'entry.sqlite3'),entry_directory:entry,boot_directory:root,node_id:'mac'};
 const guards=[];const launcher=async(_root,contract,_container,database)=>{assert.equal(database,resources.entry_database);let exit;
  const handle={pid:123,deadline_at_ms:contract.deadline_at_ms,alive:()=>guardianAlive,exited:new Promise(resolve=>{exit=resolve;}),fail(){guardianAlive=false;exit({code:1,signal:null});}};
  guards.push(handle);return handle;};
 profile.container.network=network;
 if(brokered){profile.providers[provider].brokered=true;delete profile.providers[provider].credential_file;}
 const driver=new NativeContainerDriver(profile,resources,launcher);
 const job={task_id:'task1',attempt_id:'attempt1',generation:1,node_id:'mac',provider,model:'fixture-model',policy_sha256:profile.policy_sha256,base_sha:base,workspace:'/logical/work/attempt1',sandbox:'workspace-write',timeout_ms:10000,prompt:'PRIVATE_TASK_PROMPT'};
 return {root,state,profile,resources,guards,driver,job,read:()=>JSON.parse(readFileSync(state,'utf8')),signal:new AbortController().signal};
}

test('brokered Codex refuses a named network before workspace, guardian or Docker launch',async t=>{
 const f=await fixture(t,{brokered:true,network:'egress-net'});t.after(()=>f.driver.close());
 const job={...f.job,inference:{schema_version:1},allowed_tools:['Bash'],max_turns:1,max_budget_usd:0.1};
 await assert.rejects(f.driver.execute('start',job,command('C-network','1'),f.signal),/native_policy_denied/);
 assert.equal(f.driver.codex_constraints,false);assert.equal(f.guards.length,0);assert.equal(f.read().calls.length,0);
});

test('brokered Codex network-none starts without a provider credential mount',async t=>{
 const f=await fixture(t,{brokered:true});t.after(()=>f.driver.close());
 const job={...f.job,inference:{schema_version:1},allowed_tools:['Read'],max_turns:1,max_budget_usd:0.1};
 assert.equal(f.driver.codex_constraints,true);
 assert.equal((await f.driver.execute('start',job,command('C-offline','1'),f.signal)).state,'applied');
 const create=f.read().calls.find(args=>args.includes('create'));
 assert.equal(create[create.indexOf('--network')+1],'none');assert.equal(create.includes('--env-file'),false);
});
test('native driver composes exact source, guardian, ticket, session continuation and confirmed cancellation',async t=>{
 const f=await fixture(t);const start=command('C1','1');
 assert.equal((await f.driver.execute('start',f.job,start,f.signal)).session_id,'native-fixture-1');assert.equal(f.guards.length,1);
 let state=f.read();assert.equal(readFileSync(join(state.container.mounts['/workspace'],'task.txt'),'utf8'),'source\n');
 assert.equal(f.resources.entry_gate.consume({...f.job,command_id:'C1',command_sha256:start.command_sha256},state.lastTicket,'9'.repeat(64)),true);
 assert.equal(JSON.stringify(state.calls).includes(f.job.prompt),false);assert.equal(JSON.stringify(state.calls).includes('fixture-not-a-key'),false);
 assert.equal((await f.driver.execute('pause',f.job,command('C2','2'),f.signal)).stopped,true);
 assert.equal((await f.driver.execute('resume',f.job,{...command('C3','3'),native_session:'native-fixture-1'},f.signal)).state,'applied');
 await f.driver.execute('pause',f.job,command('C4','4'),f.signal);
 assert.equal((await f.driver.execute('reply',f.job,{...command('C5','5'),native_session:'native-fixture-1',reply:'approved reply'},f.signal)).state,'applied');
 assert.equal(f.read().lastInput.prompt,'approved reply');assert.equal(f.guards.length,1);
 const cancelled=await f.driver.execute('cancel',f.job,command('C6','6'),f.signal);assert.equal(cancelled.stopped,true);
 assert.equal((await f.driver.inspect(f.job,{id:'C6',sha256:'6'.repeat(64),operation:'cancel'})).stopped,true);
 await assert.rejects(f.driver.execute('resume',f.job,{...command('C7','7'),native_session:'native-fixture-1'},f.signal),/native_continuation_unavailable/);
 assert.equal(f.read().starts,3);
});
test('an unavailable guardian or denied policy never reaches Docker create',async t=>{
 const f=await fixture(t,{guardianAlive:false});
 await assert.rejects(f.driver.execute('start',{...f.job,model:'unapproved'},command('C0','0'),f.signal),/native_policy_denied/);
 await assert.rejects(f.driver.execute('start',f.job,command('C1','1'),f.signal),/guardian_unavailable/);
 assert.equal(f.read().calls.length,0);
 await assert.rejects(f.driver.fence(f.job,{id:'invalid command',sha256:'bad'}),/invalid_driver_command/);assert.equal(f.read().calls.length,0);
});

test('Claude uses the same session and stop protocol with its own runtime and credential binding',async t=>{
 const f=await fixture(t,{provider:'claude'});const first=await f.driver.execute('start',f.job,command('C1','1'),f.signal);
 assert.equal(first.session_id,'native-fixture-1');assert.equal(f.read().lastInput.runtime.provider,'claude');
 await f.driver.execute('pause',f.job,command('C2','2'),f.signal);
 const resumed=await f.driver.execute('resume',f.job,{...command('C3','3'),native_session:first.session_id},f.signal);
 assert.equal(resumed.session_id,first.session_id);assert.equal(f.guards.length,1);
 assert.equal((await f.driver.execute('cancel',f.job,command('C4','4'),f.signal)).stopped,true);
});

test('unexpected guardian exit fences the attempt and attempts physical stopping',async t=>{
 const f=await fixture(t);await f.driver.execute('start',f.job,command('C1','1'),f.signal);f.guards[0].fail();
 const until=Date.now()+3000;let stopped=false;
 while(Date.now()<until){try {if(f.read().container?.running===false){stopped=true;break;}} catch {}await new Promise(resolve=>setTimeout(resolve,25));}
 assert.equal(stopped,true);
 assert.throws(()=>f.resources.entry_gate.issue({...f.job,command_id:'C-late',command_sha256:'8'.repeat(64)}),/attempt_fenced/);
 await f.driver.execute('cancel',f.job,command('C2','2'),f.signal);
});
test('lost container start ACK reconciles the same session without another start',async t=>{
 const f=await fixture(t,{mode:'lost-start-ack'});const start=command('C1','1');
 await assert.rejects(f.driver.execute('start',f.job,start,f.signal),/container_effect_unknown/);
 const receipt=await f.driver.inspect(f.job,{id:'C1',sha256:start.command_sha256,operation:'start'});
 assert.equal(receipt.state,'applied');assert.equal(receipt.session_id,'native-fixture-1');assert.equal(f.read().starts,1);
 await assert.rejects(f.driver.execute('start',f.job,command('C2','2'),f.signal),/native_attempt_already_started/);assert.equal(f.read().starts,1);
});

test('the actual worker service reconciles an initial SQL NULL session after a lost start ACK',async t=>{
 const f=await fixture(t,{mode:'lost-start-ack'});const service=new WorkerService(join(f.root,'journal'),'mac',1,f.driver);t.after(()=>service.close());
 const first=await service.handle({schema_version:1,request_id:'C1',operation:'dispatch',arguments:{operation:'start',job:f.job}});
 assert.equal(first.data.state,'unknown');assert.equal(f.read().starts,1);
 const recovered=await service.handle({schema_version:1,request_id:'R1',operation:'reconnect',arguments:{attempt_id:f.job.attempt_id,command_id:'C1'}});
 assert.equal(recovered.data.state,'applied');assert.equal(recovered.data.session_id,'native-fixture-1');assert.equal(f.read().starts,1);
 const replay=await service.handle({schema_version:1,request_id:'C1',operation:'dispatch',arguments:{operation:'start',job:f.job}});
 assert.equal(replay.data.session_id,'native-fixture-1');assert.equal(f.read().starts,1);
});
test('event replay preserves IDs and hashes until exact acknowledgements',async t=>{
 const f=await fixture(t);await f.driver.execute('start',f.job,command('C1','1'),f.signal);
 const output=f.read().container.mounts['/run/orch-output'];const spool=new EventSpool(join(output,'events'));
 const runtime=f.profile.providers.codex.runtime;
 const event=spool.append({schema_version:1,task_id:f.job.task_id,attempt_id:f.job.attempt_id,generation:1,node_id:'mac',provider:'codex',
  sdk_version:runtime.sdk_version,sdk_sha256:runtime.sdk_sha256,cli_version:runtime.cli_version,cli_sha256:runtime.cli_sha256,policy_sha256:f.job.policy_sha256,status:'running',native_type:'thread.started',session_id:'native-fixture-1'});spool.close();
 const first=await f.driver.events(f.job,[],100);assert.equal(first[0].event_id,event.event_id);assert.equal(first[0].sha256,event.sha256);
 assert.deepEqual(await f.driver.events(f.job,[],100),first);
 assert.deepEqual(await f.driver.events(f.job,[{event_id:event.event_id,sha256:event.sha256}],100),[]);
});

test('a changed ready receipt cannot replace the command hash or already bound session',async t=>{
 const f=await fixture(t);await f.driver.execute('start',f.job,command('C1','1'),f.signal);
 const path=join(f.read().container.mounts['/run/orch-output'],'C1.ready.json');const original=JSON.parse(readFileSync(path,'utf8'));
 for(const change of [{command_sha256:'8'.repeat(64)},{command_id:'C-other'},{session_id:'another-session'}]){
  writeFileSync(path,JSON.stringify({...original,...change}));
  assert.equal((await f.driver.inspect(f.job,{id:'C1',sha256:'1'.repeat(64),operation:'start'})).state,'unknown');
 }
});

test('driver close drains an active guardian-exit reaction and prevents later execution',async t=>{
 let release;const waiting=new Promise(resolve=>{release=resolve;});t.after(()=>release());
 const f=await fixture(t);await f.driver.execute('start',f.job,command('C1','1'),f.signal);
 let entered;const started=new Promise(resolve=>{entered=resolve;});f.driver.emergency=async()=>{entered();await waiting;};
 f.guards[0].fail();await started;let done=false;const closing=f.driver.close().then(()=>{done=true;});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(done,false);
 await assert.rejects(f.driver.execute('cancel',f.job,command('C2','2'),f.signal),/native_driver_closed/);
 release();await closing;assert.equal(f.read().starts,1);assert.equal(f.driver.close(),f.driver.close());
});


test('native checkpoint restores the committed head and delivers continuation context',async t=>{
 const f=await fixture(t);
 const git=(...args)=>execFileSync('/usr/bin/git',['-c','core.hooksPath=/dev/null',...args],{encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
 writeFileSync(join(f.profile.source_repository,'task.txt'),'checkpoint state\n');
 git('-C',f.profile.source_repository,'add','task.txt');
 git('-C',f.profile.source_repository,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','checkpoint');
 const head=git('-C',f.profile.source_repository,'rev-parse','HEAD');
 const checkpoint={id:'CK-test',head_sha:head,candidate:'',handoff_ref:'semantic-handoff',dirty_digest:'',body_sha:''};
 await f.driver.execute('start',{...f.job,checkpoint},command('C1','1'),f.signal);
 const state=f.read();
 assert.equal(readFileSync(join(state.container.mounts['/workspace'],'task.txt'),'utf8'),'checkpoint state\n');
 assert.ok(state.lastInput.prompt.includes(head));
 await f.driver.close();
});

test('native checkpoint refuses unrestorable dirty state before launching tools',async t=>{
 const f=await fixture(t);
 await assert.rejects(f.driver.execute('start',{...f.job,checkpoint:{id:'CK-test',head_sha:f.job.base_sha,candidate:'',handoff_ref:'',dirty_digest:'a'.repeat(64),body_sha:''}},command('C1','1'),f.signal),/checkpoint/);
 assert.equal(f.read().starts,0);
});


test('missing checkpoint objects never reach guardian or Docker startup',async t=>{
 const f=await fixture(t);
 await assert.rejects(f.driver.execute('start',{...f.job,checkpoint:{id:'CK-test',head_sha:'0'.repeat(40),candidate:'',handoff_ref:'',dirty_digest:'',body_sha:''}},command('C1','1'),f.signal),/workspace_git_failed/);
 assert.equal(f.guards.length,0);assert.equal(f.read().starts,0);
});
