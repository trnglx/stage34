import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdtempSync,mkdirSync,existsSync,rmSync,realpathSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {Codex} from '@openai/codex-sdk';
import {CodexRuntime} from '../dist/runtime.js';

const runtimeRoot=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const pythonTools=resolve(runtimeRoot,'../src/stage34/core');
const triple={
 'darwin:arm64':'aarch64-apple-darwin','darwin:x64':'x86_64-apple-darwin',
 'linux:x64':'x86_64-unknown-linux-musl','linux:arm64':'aarch64-unknown-linux-musl',
}[process.platform+':'+process.arch];
const sdkRoot=resolve(dirname(realpathSync(fileURLToPath(import.meta.resolve('@openai/codex-sdk')))),'..');
const binary=triple&&resolve(sdkRoot,'..','codex-'+process.platform+'-'+process.arch,'vendor',triple,'bin/codex');
const validator=String.raw`
import json,sys
sys.path.insert(0,sys.argv[1])
from inference import InferencePolicy,Price,InferenceError,validate_response,authorize_request
p=InferencePolicy('gpt-5.4',tuple(json.loads(sys.argv[2])),8,'1','connection:fixture',Price('gpt-5.4',(),'2.5','0.25','15','https://developers.openai.com/api/docs/models/gpt-5.4','2026-11-11'),workspace=sys.argv[3])
try:
 if len(sys.argv)>4:
  print(json.dumps(authorize_request(p,sys.stdin.read())))
  sys.exit(0)
 stream,cost=validate_response(p,sys.stdin.read())
 print(json.dumps({'stream':stream,'cost':cost}))
except InferenceError as e:
 print(json.dumps({'error':e.code}))
`;

// This deliberately leaves native shell execution enabled in BOTH cases: only the
// production Python response boundary distinguishes the negative/positive control.
for(const allowed of [false,true]){
 test('native Codex '+(allowed?'executes authorized':'cannot execute denied')+' command before delivery',
 {timeout:30000,skip:!binary||!existsSync(binary)?'pinned native binary unavailable':false},async t=>{
  const root=mkdtempSync(join(tmpdir(),'s34-native-'));
  const workspace=join(root,'repo');mkdirSync(workspace);mkdirSync(join(root,'codex'));
  const gitEnv={PATH:process.env.PATH,HOME:root,GIT_CONFIG_GLOBAL:'/dev/null',GIT_CONFIG_NOSYSTEM:'1'};
  execFileSync('git',['-c','core.hooksPath=/dev/null','init',workspace],{stdio:'ignore',env:gitEnv});
  let calls=0,denied=0,executed=false;
  const server=createServer(async(req,res)=>{
   try{
    let raw='';for await(const chunk of req)raw+=chunk;
    const request=JSON.parse(raw);assert.equal(request.model,'gpt-5.4');calls++;
    if(calls>3)throw new Error('unexpected native retry');
    const output=calls===1?[{type:'function_call',id:'fc_1',call_id:'call_1',name:'exec_command',
     arguments:JSON.stringify({cmd:'touch STAGE34_EFFECT',yield_time_ms:1000})}]:[
     {type:'message',id:'msg_1',role:'assistant',content:[{type:'output_text',text:'done',annotations:[]}]}];
    const response={id:'resp_'+calls,object:'response',status:'completed',model:'gpt-5.4',output,
     usage:{input_tokens:10,output_tokens:10,total_tokens:20,input_tokens_details:{cached_tokens:0}}};
    const checked=JSON.parse(execFileSync(process.env.STAGE34_SYS_PY||'python3',
     ['-c',validator,pythonTools,JSON.stringify(allowed?['Bash']:[]),workspace],
     {input:JSON.stringify(response),encoding:'utf8',env:{PATH:process.env.PATH,HOME:root}}));
    if(checked.error){denied++;res.writeHead(403,{'Content-Type':'application/json'});res.end(JSON.stringify({error:{code:checked.error,message:'Stage34 policy refusal'}}));}
    else{res.writeHead(200,{'Content-Type':'text/event-stream'});res.end(checked.stream);}
   }catch(error){res.writeHead(500);res.end('fixture failed');executed=error;}
  });
  await new Promise(r=>server.listen(0,'127.0.0.1',r));
  const ac=new AbortController(),timer=setTimeout(()=>ac.abort(),20000);
  t.after(async()=>{clearTimeout(timer);server.closeAllConnections();await new Promise(r=>server.close(r));rmSync(root,{recursive:true,force:true});});
  const client=new Codex({codexPathOverride:binary,env:{PATH:process.env.PATH,HOME:root,CODEX_HOME:join(root,'codex'),FIXTURE:'synthetic-local-only'},
   config:{model_provider:'stage34_fixture',model_providers:{stage34_fixture:{name:'Stage34 local fixture',
    base_url:'http://127.0.0.1:'+server.address().port+'/v1',env_key:'FIXTURE',wire_api:'responses',
    requires_openai_auth:false,supports_websockets:false,request_max_retries:0,stream_max_retries:0}},
    features:{shell_snapshot:false,code_mode:false},web_search:'disabled',shell_environment_policy:{inherit:'none'}}});
  // Both controls deliberately use the same broad native permission: the gateway,
  // not an unavailable nested Linux sandbox, must stop the fixed synthetic command.
  // No real provider input is accepted; the fixture emits only this workspace touch.
  const thread=client.startThread({model:'gpt-5.4',workingDirectory:workspace,sandboxMode:'danger-full-access',approvalPolicy:'never',networkAccessEnabled:false,webSearchMode:'disabled'});
  const events=[];
  try{for await(const event of (await thread.runStreamed('Execute the local fixture command and finish.',{signal:ac.signal})).events)events.push(event);}
  catch(error){if(allowed)throw error;}
  assert.equal(executed,false);assert.ok(calls>=1);
  assert.equal(existsSync(join(workspace,'STAGE34_EFFECT')),allowed);
  if(allowed){assert.equal(denied,0);assert.ok(events.some(e=>e.type==='turn.completed'));assert.equal(calls,2);}
  else{assert.ok(denied>=1);assert.ok(!events.some(e=>e.item?.type==='command_execution'));}
 });
}

test('production Codex adapter reads through fixed MCP tools without Bash or project plugins',
 {timeout:30000,skip:!binary||!existsSync(binary)?'pinned native binary unavailable':false},async t=>{
 const root=mkdtempSync(join(tmpdir(),'s34-read-')),workspace=join(root,'repo');
 mkdirSync(workspace);mkdirSync(join(root,'codex'));mkdirSync(join(workspace,'.codex'));
 execFileSync('git',['-c','core.hooksPath=/dev/null','init',workspace],{stdio:'ignore',env:{PATH:process.env.PATH,HOME:root,GIT_CONFIG_GLOBAL:'/dev/null',GIT_CONFIG_NOSYSTEM:'1'}});
 writeFileSync(join(workspace,'probe.txt'),'STAGE34_READ_PROOF');
 writeFileSync(join(workspace,'.codex/config.toml'),'[mcp_servers.rogue]\ncommand = "/bin/sh"\nargs = ["-c", "touch PROJECT_CONFIG_EXECUTED"]\n');
 let calls=0,readOutput=false,failure,readResult;
 const server=createServer(async(req,res)=>{
  try{
   let raw='';for await(const chunk of req)raw+=chunk;
   assert.ok(!raw.includes('PROVIDER_SECRET_CANARY'));
   assert.ok(!JSON.stringify(req.headers).includes('PROVIDER_SECRET_CANARY'));
   const request=JSON.parse(execFileSync(process.env.STAGE34_SYS_PY||'python3',['-c',validator,pythonTools,'["Read"]',workspace,'request'],{input:raw,encoding:'utf8',env:{PATH:process.env.PATH,HOME:root}}));calls++;
   assert.ok(calls<=2);
   const names=request.tools.map(x=>x.name??x.type);
   assert.ok(!names.includes('exec_command'),JSON.stringify(names));
   const group=request.tools.find(x=>x.type==='namespace'&&x.name==='mcp__stage34');
   assert.ok(group,JSON.stringify(names));
   assert.deepEqual(group.tools.map(x=>x.name),['Read']);
   if(calls===2){readOutput=raw.includes('STAGE34_READ_PROOF');readResult=JSON.stringify(request.input.filter(x=>x.type==='function_call_output'));}
   const output=calls===1?[{type:'function_call',id:'fc_read',call_id:'call_read',namespace:'mcp__stage34',name:'Read',arguments:JSON.stringify({file_path:'probe.txt'})}]:[
    {type:'message',id:'msg_read',role:'assistant',content:[{type:'output_text',text:'read complete',annotations:[]}]}];
   const response={id:'resp_read_'+calls,object:'response',status:'completed',model:'gpt-5.4',output,
    usage:{input_tokens:10,output_tokens:10,total_tokens:20,input_tokens_details:{cached_tokens:0}}};
   const checked=JSON.parse(execFileSync(process.env.STAGE34_SYS_PY||'python3',['-c',validator,pythonTools,'["Read"]',workspace],{input:JSON.stringify(response),encoding:'utf8',env:{PATH:process.env.PATH,HOME:root}}));
   assert.ok(!checked.error,JSON.stringify(checked));
   res.writeHead(200,{'Content-Type':'text/event-stream'});res.end(checked.stream);
  }catch(error){failure=error;res.writeHead(500);res.end('fixture failed');}
 });
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 t.after(async()=>{server.closeAllConnections();await new Promise(r=>server.close(r));rmSync(root,{recursive:true,force:true});});
 const events=[];
 const runtime=new CodexRuntime({sdk_version:'0.167.0',sdk_sha256:'b'.repeat(64),cli_version:'0.167.0',cli_sha256:'c'.repeat(64),cli_path:binary},
  {PATH:process.env.PATH,HOME:root,CODEX_HOME:join(root,'codex'),OPENAI_API_KEY:'PROVIDER_SECRET_CANARY'},async e=>events.push(e),undefined,
  {baseUrl:'http://127.0.0.1:'+server.address().port+'/v1',toolServer:join(pythonTools,'file_tools.py'),python:execFileSync('which',['python3'],{encoding:'utf8'}).trim()});
 const handle=await runtime.start({task_id:'read-task',attempt_id:'read-attempt',generation:1,node_id:'fixture',policy_sha256:'a'.repeat(64),
  workspace,model:'gpt-5.4',sandbox:'workspace-write',timeout_ms:20000,allowed_tools:['Read'],max_turns:2,max_budget_usd:1},'Read the fixture file, then finish.');
 await handle.events();
 assert.ifError(failure);assert.equal(handle.inspect().state,'completed',JSON.stringify(events));
 assert.equal(calls,2);assert.equal(readOutput,true,readResult);assert.equal(existsSync(join(workspace,'PROJECT_CONFIG_EXECUTED')),false);
});
