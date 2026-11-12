import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {InferenceQueue} from '../dist/inference-queue.js';
import {InferenceRelay} from '../dist/inference-relay.js';

async function fixture(t){
 const root=mkdtempSync(join(tmpdir(),'s34-'));const control=join(root,'control');mkdirSync(control,{mode:0o700});
 const queue=new InferenceQueue(join(root,'private'),'attempt1');const socket=join(control,'i.sock');await queue.listen(socket);
 const relay=new InferenceRelay();await relay.listen(socket);
 t.after(async()=>{await relay.close();await queue.close();rmSync(root,{recursive:true,force:true});});
 return {root,queue,relay,socket};
}
async function queued(queue){
 const until=Date.now()+1000;
 while(Date.now()<until){const calls=queue.poll();if(calls.length)return calls[0];await new Promise(r=>setTimeout(r,5));}
 throw new Error('queue did not receive request');
}

test('model response is withheld until controller stages AND releases it',async t=>{
 const {queue,relay}=await fixture(t);let finished=false;
 const pending=fetch(relay.baseUrl+'/responses',{method:'POST',body:JSON.stringify({model:'fixture',input:'data'})}).then(async r=>{finished=true;return {status:r.status,text:await r.text()};});
 const call=await queued(queue);assert.equal(call.state,'pending');assert.equal(finished,false);
 const response='data: {"type":"response.completed"}\n\n';const sha=queue.stage(call.id,Buffer.from(response).toString('base64'),200).response_sha256;
 assert.equal(finished,false);assert.equal(queue.poll()[0].state,'staged');
 queue.release(call.id,sha);assert.deepEqual(await pending,{status:200,text:response});
 const duplicate=await fetch(relay.baseUrl+'/responses',{method:'POST',body:JSON.stringify({model:'fixture',input:'data'})});
 assert.equal(duplicate.status,409);assert.ok((await duplicate.text()).includes('inference_delivery_unknown'));
});

test('denied staged tool response never reaches the native client',async t=>{
 const {queue,relay}=await fixture(t);
 const pending=fetch(relay.baseUrl+'/responses',{method:'POST',body:'{"input":"fixture"}'});
 const call=await queued(queue);const payload='FORBIDDEN_TOOL_EFFECT';
 const sha=queue.stage(call.id,Buffer.from(payload).toString('base64'),200).response_sha256;
 queue.release(call.id,sha,'permission_denied');const response=await pending;
 assert.equal(response.status,403);assert.ok(!(await response.text()).includes(payload));
 assert.equal(relay.errorCode,'permission_denied');
});

test('staged response survives queue restart and conflicts cannot replace it',async t=>{
 const root=mkdtempSync(join(tmpdir(),'s34-persist-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
 const module=new URL('../dist/inference-queue.js',import.meta.url).href;
 const script='import {InferenceQueue} from '+JSON.stringify(module)+';const q=new InferenceQueue('+JSON.stringify(root)+',"attempt1");const id=q.enqueue(Buffer.from("eyJpbnB1dCI6ImZpeHR1cmUifQ==","base64"));const hash=q.stage(id,Buffer.from("response").toString("base64"),200).response_sha256;process.stdout.write(JSON.stringify({id,hash}));process.exit(0);';
 const {id,hash}=JSON.parse(execFileSync(process.execPath,['--input-type=module','-e',script],{encoding:'utf8'}));
 const queue=new InferenceQueue(root,'attempt1');
 assert.equal(queue.poll()[0].response_sha256,hash);
 assert.throws(()=>queue.stage(id,Buffer.from('changed').toString('base64'),200),/inference_response_conflict/);
 queue.release(id,hash);assert.equal(queue.poll().length,0);await queue.close();
});
