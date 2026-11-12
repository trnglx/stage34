import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,lstatSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {createConnection} from 'node:net';
import {fileURLToPath} from 'node:url';
import {serveWorkerSocket} from '../dist/worker-server.js';
import {WorkerService} from '../dist/service.js';
const shim=fileURLToPath(new URL('../../src/stage34/core/worker_rpc.py',import.meta.url));
const request=(id,operation,args={})=>({schema_version:1,request_id:id,operation,arguments:args});
const job=()=>({task_id:'task1',attempt_id:'attempt1',generation:1,node_id:'mac',provider:'codex',model:'example',
 policy_sha256:'a'.repeat(64),base_sha:'b'.repeat(40),workspace:'/isolated/work',sandbox:'workspace-write',timeout_ms:10000,prompt:'PRIVATE'});
async function setup(t,handler){
 const root=mkdtempSync(join(tmpdir(),'worker-rpc-'));const path=join(root,'worker.sock');
 const server=await serveWorkerSocket(path,handler);
 t.after(async()=>{await new Promise(resolve=>server.close(resolve));handler.close?.();rmSync(root,{recursive:true,force:true});});
 return {root,path,server};
}
function rpc(path,input){return new Promise((resolve,reject)=>{
 const child=spawn(process.env.STAGE34_TEST_PYTHON||'python3',[shim,'--socket',path],{stdio:['pipe','pipe','pipe']});
 let output='';let errors='';const timeout=setTimeout(()=>{child.kill('SIGKILL');reject(new Error('shim_timeout'));},5000);
 child.stdout.on('data',value=>{output+=value;if(output.length>1024*1024)child.kill('SIGKILL');});child.stderr.on('data',value=>{errors+=value;});
 child.on('error',error=>{clearTimeout(timeout);reject(error);});
 child.on('close',code=>{clearTimeout(timeout);resolve({code,output,errors});});child.stdin.end(JSON.stringify(input));
});}
test('real Python shim and worker journal replay one dispatch without a second effect',async t=>{
 const root=mkdtempSync(join(tmpdir(),'worker-journal-'));t.after(()=>rmSync(root,{recursive:true,force:true}));let starts=0;
 const worker=new WorkerService(root,'mac',1,{execute:async()=>{starts++;return {state:'applied',session_id:'native1'};},fence:async()=>{},events:async()=>[],inspect:async()=>({state:'unknown'})});
 const {path}=await setup(t,worker);assert.equal(lstatSync(path).mode&0o777,0o600);
 const command=request('C1','dispatch',{operation:'start',job:job()});
 const first=await rpc(path,command);const replay=await rpc(path,command);
 assert.equal(first.code,0,first.errors);assert.equal(replay.code,0,replay.errors);
 assert.deepEqual(JSON.parse(first.output),JSON.parse(replay.output));assert.equal(JSON.parse(first.output).data.state,'applied');assert.equal(starts,1);
});
test('handler failure after an effect is unknown to the real Python shim',async t=>{
 let calls=0;const {path}=await setup(t,{handle:async()=>{calls++;throw new Error('PRIVATE_EFFECT_FAILURE');}});
 const result=await rpc(path,request('C1','cancel',{attempt_id:'attempt1'}));
 assert.equal(result.code,74);assert.equal(result.output,'');assert.equal(result.errors.includes('PRIVATE'),false);assert.equal(calls,1);
});
test('malformed, oversized and concatenated frames never reach the worker',async t=>{
 let calls=0;const {path}=await setup(t,{handle:async()=>{calls++;return {};}});
 for(const frame of [Buffer.from([0,16,0,1]),Buffer.from([0,0,0,0]),Buffer.concat([Buffer.from([0,0,0,1]),Buffer.from('{')]),Buffer.concat([Buffer.from([0,0,0,2]),Buffer.from('{}extra')])]){
  await new Promise((resolve,reject)=>{
   const socket=createConnection(path);const timeout=setTimeout(()=>{socket.destroy();reject(new Error('frame_not_refused'));},1000);
   socket.on('error',()=>{});socket.on('connect',()=>socket.write(frame));socket.on('close',()=>{clearTimeout(timeout);resolve();});
  });
 }
 assert.equal(calls,0);
});

test('disconnected clients cannot accumulate more than sixteen unfinished handlers',async t=>{
 let calls=0;let release;const waiting=new Promise(resolve=>{release=resolve;});
 const {path}=await setup(t,{handle:async req=>{calls++;await waiting;return {schema_version:1,request_id:req.request_id,ok:true,code:'ok',data:null};}});
 t.after(()=>release());
 const payload=Buffer.from(JSON.stringify(request('C1','health')));const header=Buffer.alloc(4);header.writeUInt32BE(payload.length);
 for(let i=0;i<17;i++)await new Promise((resolve,reject)=>{
  const socket=createConnection(path);const timer=setTimeout(()=>{socket.destroy();reject(new Error('client_not_closed'));},1000);
  socket.on('error',()=>{});socket.on('connect',()=>socket.end(Buffer.concat([header,payload])));
  socket.on('close',()=>{clearTimeout(timer);resolve();});
 });
 assert.equal(calls,16);
 release();await new Promise(resolve=>setImmediate(resolve));
 const accepted=await rpc(path,request('C2','health'));assert.equal(accepted.code,0,accepted.errors);assert.equal(calls,17);
});

test('drain keeps journal ownership until disconnected in-flight effects finish',async t=>{
 let entered;const started=new Promise(resolve=>{entered=resolve;});let release;const waiting=new Promise(resolve=>{release=resolve;});
 const {path,server}=await setup(t,{handle:async req=>{entered();await waiting;return {schema_version:1,request_id:req.request_id,ok:true,code:'ok',data:null};}});
 t.after(()=>release());
 const payload=Buffer.from(JSON.stringify(request('C1','health')));const header=Buffer.alloc(4);header.writeUInt32BE(payload.length);
 await new Promise(resolve=>{const socket=createConnection(path);socket.on('error',()=>{});socket.on('connect',()=>socket.end(Buffer.concat([header,payload])));socket.on('close',resolve);});
 await started;let finished=false;const draining=server.drain().then(()=>{finished=true;});
 assert.equal(server.drain() instanceof Promise,true);
 await new Promise(resolve=>setImmediate(resolve));assert.equal(finished,false);
 release();await draining;assert.equal(finished,true);
});
