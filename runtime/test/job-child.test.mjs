import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {runContainerJob} from '../dist/job-child.js';

test('production entry refuses native startup without a host-issued ticket',async t=>{
 const {options}=fixture(t);
 const result=await runContainerJob(options);
 assert.equal(result.phase,'failed');assert.equal(result.code,'entry_ticket_required');
});
function fixture(t){
 const root=mkdtempSync(join(tmpdir(),'native-child-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
 const controlRoot=join(root,'control');mkdirSync(controlRoot,{mode:0o700});
 const job={task_id:'task1',attempt_id:'attempt1',node_id:'node1',generation:1,provider:'codex',policy_sha256:'a'.repeat(64),
  base_sha:'b'.repeat(40),workspace:'/untrusted-namespace',model:'example',sandbox:'workspace-write',timeout_ms:1000,prompt:'PRIVATE'};
 const runtime={provider:'codex',sdk_version:'0.167.0',sdk_sha256:'c'.repeat(64),cli_path:'/approved/codex',cli_version:'0.167.0',cli_sha256:'d'.repeat(64)};
 const request={identity:{node_id:job.node_id,attempt_id:job.attempt_id,generation:1,policy_sha256:job.policy_sha256},
  command:{id:'C-start',sha256:'e'.repeat(64)},input:{job,runtime,prompt:job.prompt}};
 writeFileSync(join(controlRoot,'command.json'),JSON.stringify(request));
 return {request,options:{controlRoot,outputRoot:join(root,'output'),workspace:'/workspace',commandId:'C-start'}};
}
function worker(start){return {start,inspect:()=>({session_id:'native1',state:'completed'}),cancel(){},close(){}};}
test('native intent is durable before execution and duplicate container entry does not call the SDK twice',async t=>{
 const {options}=fixture(t);let calls=0;
 const factory=async()=>worker(async context=>{calls++;assert.equal(context.workspace,'/workspace');
  assert.equal(JSON.parse(readFileSync(join(options.outputRoot,'C-start.intent.json'),'utf8')).attempt_id,'attempt1');});
 const result=await runContainerJob(options,factory);assert.equal(result.phase,'completed');
 assert.equal(JSON.stringify(result).includes('PRIVATE'),false);
 assert.equal((await runContainerJob(options,factory)).code,'command_already_consumed');assert.equal(calls,1);
});
test('cancel barrier and wrong command ID refuse execution before constructing a runtime',async t=>{
 const {options}=fixture(t);let calls=0;const factory=async()=>{calls++;return worker(async()=>{});};
 await assert.rejects(runContainerJob({...options,commandId:'C-old'},factory),/stale_command/);
 writeFileSync(join(options.controlRoot,'cancelled.json'),'{}');
 await assert.rejects(runContainerJob(options,factory),/attempt_cancelled/);assert.equal(calls,0);
});
test('cancellation arriving during runtime verification is rechecked before startup',async t=>{
 const {options}=fixture(t);let starts=0;
 const result=await runContainerJob(options,async()=>{writeFileSync(join(options.controlRoot,'cancelled.json'),'{}');return worker(async()=>{starts++;});});
 assert.equal(result.code,'attempt_cancelled');assert.equal(starts,0);
});
test('provider-private exception text never enters child receipts',async t=>{
 const {options}=fixture(t);
 const result=await runContainerJob(options,async()=>worker(async()=>{throw new Error('PRIVATE_PROVIDER_FAILURE');}));
 assert.equal(result.phase,'failed');assert.equal(result.code,'worker_execution_failed');
 assert.equal(readFileSync(join(options.outputRoot,'C-start.receipt.json'),'utf8').includes('PRIVATE_PROVIDER_FAILURE'),false);
});

test('a termination signal during runtime verification prevents a later startup',async t=>{
 const {options}=fixture(t);let starts=0;
 const result=await runContainerJob(options,async()=>{process.emit('SIGTERM');return worker(async()=>{starts++;});});
 assert.equal(starts,0);assert.equal(result.phase,'cancelled');assert.equal(result.code,'worker_shutdown_requested');
});

test('a provider gets only its own credential environment and Codex supports the API-key fallback',async t=>{
 const {options}=fixture(t);const keys=['CODEX_API_KEY','OPENAI_API_KEY','ANTHROPIC_API_KEY','CLAUDE_CODE_OAUTH_TOKEN'];
 const saved=Object.fromEntries(keys.map(key=>[key,process.env[key]]));
 t.after(()=>{for(const key of keys){if(saved[key]===undefined)delete process.env[key];else process.env[key]=saved[key];}});
 delete process.env.CODEX_API_KEY;process.env.OPENAI_API_KEY='FAKE_CODEX_KEY';
 process.env.ANTHROPIC_API_KEY='FAKE_CLAUDE_KEY';process.env.CLAUDE_CODE_OAUTH_TOKEN='FAKE_CLAUDE_TOKEN';
 let environment;
 await runContainerJob(options,async(_runtime,given)=>{environment=given;return worker(async()=>{});});
 assert.equal(Object.hasOwn(environment,'ANTHROPIC_API_KEY'),false);
 assert.equal(Object.hasOwn(environment,'CLAUDE_CODE_OAUTH_TOKEN'),false);
 assert.equal(environment.CODEX_API_KEY,'FAKE_CODEX_KEY');
});

test('brokered container entry strips credentials and supplies fixed external sandbox composition',async t=>{
 const {options,request}=fixture(t);request.input.job.inference={schema_version:1};
 writeFileSync(join(options.controlRoot,'command.json'),JSON.stringify(request));
 const keys=['CODEX_API_KEY','OPENAI_API_KEY','ANTHROPIC_API_KEY','CLAUDE_CODE_OAUTH_TOKEN'];
 const saved=Object.fromEntries(keys.map(key=>[key,process.env[key]]));
 t.after(()=>{for(const key of keys){if(saved[key]===undefined)delete process.env[key];else process.env[key]=saved[key];}});
 for(const key of keys)process.env[key]='SYNTHETIC_PROVIDER_CANARY';
 let environment,gateway;
 const result=await runContainerJob(options,async(_runtime,env,_root,transport)=>{
  environment=env;gateway=transport;return worker(async()=>{});
 });
 assert.equal(result.phase,'completed');
 for(const key of keys)assert.equal(Object.hasOwn(environment,key),false);
 assert.deepEqual(gateway,{socketPath:join(options.controlRoot,'inference.sock'),toolServer:'/opt/orch/file_tools.py',python:'/usr/bin/python3',externalSandbox:true});
});

test('a resumed session does not publish readiness before new progress and readiness is immutable',async t=>{
 const {options,request}=fixture(t);request.input.native_session='native-existing';
 writeFileSync(join(options.controlRoot,'command.json'),JSON.stringify(request));
 let release;const waiting=new Promise(resolve=>{release=resolve;});let state='starting';
 const running=runContainerJob(options,async()=>({start:async()=>{await waiting;state='completed';},inspect:()=>({session_id:'native-existing',state}),cancel(){},close(){}}));
 const {existsSync,statSync}=await import('node:fs');const ready=join(options.outputRoot,'C-start.ready.json');
 try {
  await new Promise(resolve=>setTimeout(resolve,35));assert.equal(existsSync(ready),false);
  state='running';const limit=Date.now()+1000;
  while(!existsSync(ready)&&Date.now()<limit)await new Promise(resolve=>setTimeout(resolve,5));
  assert.equal(existsSync(ready),true);const inode=statSync(ready).ino;
  await new Promise(resolve=>setTimeout(resolve,40));assert.equal(statSync(ready).ino,inode);
 } finally {release();await running;}
});

test('the event-loop watchdog exits when an asynchronous runtime never settles',async t=>{
 const {options,request}=fixture(t);request.input.job.timeout_ms=1;
 writeFileSync(join(options.controlRoot,'command.json'),JSON.stringify(request));
 const {spawn}=await import('node:child_process');const {once}=await import('node:events');
 const module=new URL('../dist/job-child.js',import.meta.url).href;
 const script=`import {executeContainerJob} from ${JSON.stringify(module)};
 await executeContainerJob(JSON.parse(process.argv[1]),async()=>({start:async()=>{setInterval(()=>{},1000);await new Promise(()=>{});},
 inspect:()=>({session_id:'native1',state:'running'}),cancel(){},close(){}}));`;
 const child=spawn(process.execPath,['--input-type=module','-e',script,JSON.stringify(options)],{stdio:'ignore'});
 let timer;
 try {
  const [code]=await Promise.race([once(child,'exit'),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('hard_deadline_failed')),7500);})]);
  assert.equal(code,124);
  assert.equal(JSON.parse(readFileSync(join(options.outputRoot,'C-start.receipt.json'),'utf8')).code,'worker_watchdog_timeout');
 } finally {clearTimeout(timer);if(child.exitCode===null)child.kill('SIGKILL');}
});
