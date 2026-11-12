import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs,{mkdtempSync,rmSync,lstatSync,readFileSync,existsSync,writeFileSync} from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {createConnection} from 'node:net';
import {WorkerHost} from '../dist/worker-host.js';
import {consumeEntryTicket} from '../dist/entry-rpc.js';
const config=root=>({state_root:root,node_id:'mac',slots:1});
const driver=()=>({execute:async()=>({state:'applied',session_id:'native1'}),events:async()=>[],fence:async()=>{},inspect:async()=>({state:'unknown'})});
const req=(id,operation,args={})=>({schema_version:1,request_id:id,operation,arguments:args});
const job=()=>({task_id:'task1',attempt_id:'attempt1',generation:1,node_id:'mac',provider:'codex',model:'example',
 policy_sha256:'a'.repeat(64),base_sha:'b'.repeat(40),workspace:'/isolated/work',sandbox:'workspace-write',timeout_ms:10000,prompt:'PRIVATE'});
const hosts=new Map();
// macOS's default temporary path can exceed the UNIX socket address limit.
function root(t){const path=mkdtempSync(join(process.platform==='darwin'?'/tmp':tmpdir(),'wh-'));hosts.set(path,[]);t.after(async()=>{
 try {for(const host of hosts.get(path))await host.close();} finally {hosts.delete(path);rmSync(path,{recursive:true,force:true});}
});return path;}
async function start(path,factory){const host=await WorkerHost.start(config(path),factory);hosts.get(path).push(host);return host;}
function rpc(path,request){return new Promise((resolve,reject)=>{
 const socket=createConnection(path);let bytes=Buffer.alloc(0);const timer=setTimeout(()=>{socket.destroy();reject(new Error('rpc_timeout'));},3000);
 socket.on('error',error=>{clearTimeout(timer);reject(error);});socket.on('connect',()=>{
  const payload=Buffer.from(JSON.stringify(request));const header=Buffer.alloc(4);header.writeUInt32BE(payload.length);socket.write(Buffer.concat([header,payload]));
 });
 socket.on('data',chunk=>{bytes=Buffer.concat([bytes,chunk]);if(bytes.length>=4&&bytes.length===4+bytes.readUInt32BE(0)){clearTimeout(timer);socket.destroy();resolve(JSON.parse(bytes.subarray(4).toString()));}});
});}
test('host publishes a ready private management endpoint separate from the job entry socket',async t=>{
 const path=root(t);let resources;const host=await start(path,given=>{resources=given;return driver();});
 t.after(()=>host.close());
 assert.equal(lstatSync(host.endpoint.worker_socket).mode&0o777,0o600);
 assert.notEqual(dirname(host.endpoint.worker_socket),resources.entry_directory);
 assert.deepEqual(JSON.parse(readFileSync(join(path,'endpoint.json'),'utf8')),host.endpoint);
 assert.equal((await rpc(host.endpoint.worker_socket,req('health','health'))).data.node_id,'mac');
 const binding={node_id:'mac',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64),command_id:'C1',command_sha256:'b'.repeat(64)};
 const token=resources.entry_gate.issue(binding);
 assert.equal(await consumeEntryTicket(join(resources.entry_directory,'gate.sock'),binding,token,'c'.repeat(64)),true);
});
test('a second host cannot invoke a driver or replace the active endpoint',async t=>{
 const path=root(t);const first=await start(path,driver);t.after(()=>first.close());let created=0;
 await assert.rejects(WorkerHost.start(config(path),()=>{created++;return driver();}),/worker_already_running/);
 assert.equal(created,0);assert.equal(JSON.parse(readFileSync(join(path,'endpoint.json'),'utf8')).boot_id,first.endpoint.boot_id);
 assert.equal((await rpc(first.endpoint.worker_socket,req('health','health'))).ok,true);
});
test('fresh boot paths preserve command receipts and quarantine running work without restarting it',async t=>{
 const path=root(t);let starts=0;const make=()=>({...driver(),execute:async()=>{starts++;return {state:'applied',session_id:'native1'};}});
 const first=await start(path,make);const command=req('C1','dispatch',{operation:'start',job:job()});
 assert.equal((await rpc(first.endpoint.worker_socket,command)).ok,true);await first.close();assert.equal(existsSync(join(path,'endpoint.json')),false);
 const second=await start(path,make);t.after(()=>second.close());
 assert.notEqual(first.endpoint.boot_id,second.endpoint.boot_id);
 assert.equal((await rpc(second.endpoint.worker_socket,command)).data.state,'applied');assert.equal(starts,1);
 assert.equal((await rpc(second.endpoint.worker_socket,req('health','health'))).data.occupied,1);
});
test('shutdown retains exclusive ownership while a disconnected effect is in flight',async t=>{
 let release;const waiting=new Promise(resolve=>{release=resolve;});t.after(()=>release());
 const path=root(t);let entered;const started=new Promise(resolve=>{entered=resolve;});
 const host=await start(path,()=>({...driver(),execute:async()=>{entered();await waiting;return {state:'applied',session_id:'native1'};}}));
 const pending=rpc(host.endpoint.worker_socket,req('C1','dispatch',{operation:'start',job:job()}));
 await started;let closed=false;const closing=host.close().then(()=>{closed=true;});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(closed,false);
 await assert.rejects(WorkerHost.start(config(path),driver),/worker_already_running/);
 release();assert.equal((await pending).ok,true);await closing;assert.equal(closed,true);
 const next=await start(path,driver);await next.close();
});
test('failed startup releases ownership and never publishes a ready endpoint',async t=>{
 const path=root(t);await assert.rejects(WorkerHost.start(config(path),()=>{throw new Error('driver_failed');}),/driver_failed/);
 assert.equal(existsSync(join(path,'endpoint.json')),false);
 const next=await start(path,driver);await next.close();
});

test('failed startup clears a stale descriptor from a previous dead boot',async t=>{
 const path=root(t);writeFileSync(join(path,'endpoint.json'),JSON.stringify({schema_version:1,boot_id:'B-old',worker_socket:join(path,'dead.sock')}),{mode:0o600});
 await assert.rejects(WorkerHost.start(config(path),()=>{throw new Error('driver_failed');}),/driver_failed/);
 assert.equal(existsSync(join(path,'endpoint.json')),false);
 const next=await start(path,driver);await next.close();
});

test('a directory fsync failure after publication removes the dead ready descriptor',async t=>{
 const path=root(t);const original=fs.fsyncSync;let directorySyncs=0;
 fs.fsyncSync=fd=>{if(fs.fstatSync(fd).isDirectory()){directorySyncs++;throw new Error('injected_directory_io');}return original(fd);};
 syncBuiltinESMExports();
 try {await assert.rejects(WorkerHost.start(config(path),driver),/injected_directory_io/);}
 finally {fs.fsyncSync=original;syncBuiltinESMExports();}
 assert.ok(directorySyncs>=1);assert.equal(existsSync(join(path,'endpoint.json')),false);
 const next=await start(path,driver);await next.close();
});

test('shutdown waits for driver background cleanup before releasing journal ownership',async t=>{
 let release;const waiting=new Promise(resolve=>{release=resolve;});t.after(()=>release());
 const path=root(t);let called=false;const host=await start(path,()=>({...driver(),close:async()=>{called=true;await waiting;}}));
 let done=false;const closing=host.close().then(()=>{done=true;});await new Promise(resolve=>setImmediate(resolve));
 assert.equal(called,true);assert.equal(done,false);
 await assert.rejects(WorkerHost.start(config(path),driver),/worker_already_running/);
 release();await closing;const next=await start(path,driver);await next.close();
});

test('late failed startup closes the constructed driver before releasing ownership',async t=>{
 const path=root(t);const original=fs.fsyncSync;let failed=false;let closed=0;
 fs.fsyncSync=fd=>{if(!failed&&fs.fstatSync(fd).isDirectory()){failed=true;throw new Error('injected_directory_io');}return original(fd);};syncBuiltinESMExports();
 try {await assert.rejects(WorkerHost.start(config(path),()=>({...driver(),close:async()=>{closed++;}})),/injected_directory_io/);}
 finally {fs.fsyncSync=original;syncBuiltinESMExports();}
 assert.equal(closed,1);const next=await start(path,driver);await next.close();
});

test('failed startup retains journal ownership until asynchronous driver cleanup finishes',async t=>{
 const path=root(t);let release;const waiting=new Promise(resolve=>{release=resolve;});
 let entered;const cleanupStarted=new Promise(resolve=>{entered=resolve;});
 const original=fs.fsyncSync;let failOnce=true;let contender;
 fs.fsyncSync=fd=>{if(failOnce&&fs.fstatSync(fd).isDirectory()){failOnce=false;throw new Error('injected_publication_failure');}return original(fd);};
 syncBuiltinESMExports();
 const starting=WorkerHost.start(config(path),()=>({...driver(),close:async()=>{entered();await waiting;}}));
 const failed=starting.then(()=>new Error('startup unexpectedly succeeded'),error=>error);
 try {
  const cleanup=await Promise.race([cleanupStarted.then(()=>true),new Promise(resolve=>{const timer=setTimeout(()=>resolve(false),2000);timer.unref();})]);
  assert.equal(cleanup,true,'driver cleanup did not start');fs.fsyncSync=original;syncBuiltinESMExports();
  let error;try {contender=await WorkerHost.start(config(path),driver);} catch(caught){error=caught;}
  if(contender)await contender.close();
  assert.match(error?.message??'',/worker_already_running/,'failed startup released ownership before cleanup completed');
 } finally {fs.fsyncSync=original;syncBuiltinESMExports();release();await failed;}
});
