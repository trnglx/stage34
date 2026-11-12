import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createConnection} from 'node:net';
import {NativeEntryGate} from '../dist/entry-gate.js';
import {serveEntryGate,consumeEntryTicket} from '../dist/entry-rpc.js';
test('restricted host socket permits original child replay and refuses another child',async t=>{
 const root=mkdtempSync(join(tmpdir(),'entry-rpc-'));const gate=new NativeEntryGate(join(root,'gate.sqlite3'));const path=join(root,'gate.sock');
 const server=await serveEntryGate(path,gate);
 t.after(async()=>{await new Promise(resolve=>server.close(resolve));gate.close();rmSync(root,{recursive:true,force:true});});
 const binding={node_id:'mac',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64),command_id:'C1',command_sha256:'b'.repeat(64)};
 const token=gate.issue(binding);
 assert.equal(await consumeEntryTicket(path,binding,token,'c'.repeat(64)),true);
 assert.equal(await consumeEntryTicket(path,binding,token,'c'.repeat(64)),true);
 assert.equal(await consumeEntryTicket(path,binding,token,'d'.repeat(64)),false);
 gate.fence(binding);assert.equal(await consumeEntryTicket(path,binding,token,'c'.repeat(64)),false);
});
test('missing host socket fails closed rather than claiming refusal or success',async()=>{
 await assert.rejects(consumeEntryTicket('/private/tmp/nonexistent-orch-entry.sock',{},'a','b'),/entry_ack_unknown/);
});
test('partial-frame clients cannot exceed the host connection admission cap',async t=>{
 const root=mkdtempSync(join(tmpdir(),'entry-cap-'));const gate=new NativeEntryGate(join(root,'gate.sqlite3'));const path=join(root,'gate.sock');
 const server=await serveEntryGate(path,gate);const clients=[];
 t.after(async()=>{for(const client of clients)client.destroy();await new Promise(resolve=>server.close(resolve));gate.close();rmSync(root,{recursive:true,force:true});});
 const dropped=new Promise(resolve=>server.once('drop',resolve));
 for(let i=0;i<17;i++){
  const client=createConnection(path);clients.push(client);client.on('error',()=>{});
  await new Promise(resolve=>client.once('connect',resolve));client.write('{');
 }
 const admitted=await Promise.race([dropped.then(()=>true),new Promise(resolve=>{const timer=setTimeout(()=>resolve(false),500);timer.unref();})]);
 assert.equal(admitted,true,'connection cap absent');
 assert.ok(await new Promise(resolve=>server.getConnections((error,count)=>resolve(!error&&count<=16))));
});
