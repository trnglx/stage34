import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,existsSync,chmodSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {spawn} from 'node:child_process';
import {createConnection} from 'node:net';
const executable=fileURLToPath(new URL('../dist/worker-daemon.js',import.meta.url));
function setup(t,change={}) {
 const root=mkdtempSync(join(process.platform==='darwin'?'/tmp':tmpdir(),'wd-'));const path=join(root,'config.json');
 const config={schema_version:1,host:{state_root:join(root,'host'),node_id:'fixture-node',slots:1},
  native:{container:{executable:'/nonexistent/fixture-docker',endpoint:'unix:///fixture.sock',image_id:'sha256:'+'a'.repeat(64),state_root:join(root,'containers'),
    uid:process.getuid(),gid:process.getgid()||1000,cpus:1,memory_mb:512,pids_limit:32,network:'none',max_runtime_ms:1000},
   source_repository:join(root,'unused-mirror'),git_executable:'/usr/bin/git',workspace_contract_root:'/logical/work',policy_sha256:'b'.repeat(64),readiness_ms:100,providers:{},...change}};
 writeFileSync(path,JSON.stringify(config),{mode:0o600});
 const children=[];t.after(async()=>{for(const child of children){
  if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),1000);await new Promise(resolve=>child.once('close',resolve));clearTimeout(timer);}
 }rmSync(root,{recursive:true,force:true});});
 return {root,path,config,launch:()=>{
  const child=spawn(process.execPath,[executable,'--config',path],{stdio:['ignore','pipe','pipe']});children.push(child);
  let output='';let errors='';const finished=new Promise((resolve,reject)=>{child.on('error',reject);child.on('close',code=>resolve({code,output,errors}));});
  const ready=new Promise((resolve,reject)=>{
   const timer=setTimeout(()=>reject(new Error('daemon_readiness_timeout')),3000);
   child.stdout.on('data',chunk=>{output+=chunk;const end=output.indexOf('\n');if(end>=0){clearTimeout(timer);try {resolve(JSON.parse(output.slice(0,end)));}catch(error){reject(error);}}});
   child.once('close',()=>{clearTimeout(timer);reject(new Error('daemon_closed_before_readiness'));});
  });ready.catch(()=>{});child.stderr.on('data',chunk=>{errors+=chunk;});
  return {child,ready,finished};
 }};
}
function health(path){return new Promise((resolve,reject)=>{
 const socket=createConnection(path);let bytes=Buffer.alloc(0);const timer=setTimeout(()=>{socket.destroy();reject(new Error('health_timeout'));},3000);
 socket.on('error',error=>{clearTimeout(timer);reject(error);});socket.on('connect',()=>{
  const body=Buffer.from(JSON.stringify({schema_version:1,request_id:'H1',operation:'health',arguments:{}}));const header=Buffer.alloc(4);header.writeUInt32BE(body.length);socket.write(Buffer.concat([header,body]));
 });socket.on('data',chunk=>{bytes=Buffer.concat([bytes,chunk]);if(bytes.length>=4&&bytes.length===bytes.readUInt32BE(0)+4){clearTimeout(timer);socket.destroy();resolve(JSON.parse(bytes.subarray(4).toString()));}});
});}
test('daemon serves a private ready endpoint and SIGTERM removes it after graceful shutdown',async t=>{
 const f=setup(t);const run=f.launch();const endpoint=await run.ready;
 assert.equal(endpoint.node_id,'fixture-node');assert.equal((await health(endpoint.worker_socket)).ok,true);
 assert.equal(existsSync(join(f.config.host.state_root,'endpoint.json')),true);
 run.child.kill('SIGTERM');const exit=await run.finished;assert.equal(exit.code,0,exit.errors);
 assert.equal(existsSync(join(f.config.host.state_root,'endpoint.json')),false);
});
test('a second daemon cannot replace the active endpoint or its journal ownership',async t=>{
 const f=setup(t);const first=f.launch();const endpoint=await first.ready;const second=f.launch();
 const rejected=await second.finished;assert.equal(rejected.code,1);assert.equal(rejected.output,'');
 assert.equal((await health(endpoint.worker_socket)).ok,true);first.child.kill('SIGINT');assert.equal((await first.finished).code,0);
});
test('invalid and world-readable configuration fail without disclosing private input',async t=>{
 const f=setup(t,{policy_sha256:'PRIVATE_CONFIGURATION_VALUE'});const rejected=await f.launch().finished;
 assert.equal(rejected.code,1);assert.equal(rejected.output,'');assert.equal(rejected.errors.includes('PRIVATE_CONFIGURATION_VALUE'),false);
 assert.equal(existsSync(join(f.config.host.state_root,'endpoint.json')),false);
 f.config.native.policy_sha256='b'.repeat(64);writeFileSync(f.path,JSON.stringify(f.config));
 chmodSync(f.path,0o644);const run=f.launch();
 const permissions=await Promise.race([run.finished,run.ready.then(()=>({code:0,output:'unexpected_readiness'}))]);
 assert.equal(permissions.code,1);assert.equal(permissions.output,'');
});

test('loss of the startup output pipe does not terminate a serving daemon',async t=>{
 const f=setup(t);const run=f.launch();run.child.stdout.destroy();const path=join(f.config.host.state_root,'endpoint.json');
 const until=Date.now()+3000;while(!existsSync(path)&&Date.now()<until)await new Promise(resolve=>setTimeout(resolve,10));
 assert.equal(existsSync(path),true);const endpoint=JSON.parse(readFileSync(path,'utf8'));
 assert.equal((await health(endpoint.worker_socket)).ok,true);
 run.child.kill('SIGTERM');assert.equal((await run.finished).code,0);
});
