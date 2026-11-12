import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readFileSync,chmodSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {NativeEntryGate} from '../dist/entry-gate.js';
const binding=()=>({node_id:'mac',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64),command_id:'C1',command_sha256:'b'.repeat(64)});
function setup(t){const root=mkdtempSync(join(tmpdir(),'native-entry-'));t.after(()=>rmSync(root,{recursive:true,force:true}));return join(root,'entries.sqlite3');}
test('native entry survives restart and only the original child can replay its acknowledgement',t=>{
 const path=setup(t);let gate=new NativeEntryGate(path);const ticket=gate.issue(binding());
 assert.equal(gate.consume(binding(),ticket,'c'.repeat(64)),true);gate.close();
 gate=new NativeEntryGate(path);assert.equal(gate.consume(binding(),ticket,'c'.repeat(64)),true);
 assert.equal(gate.consume(binding(),ticket,'d'.repeat(64)),false);assert.throws(()=>gate.issue(binding()));gate.close();
 assert.equal(readFileSync(path).includes(Buffer.from(ticket)),false);
});
test('ticket cannot cross command, policy, node, or attempt bindings',t=>{
 const gate=new NativeEntryGate(setup(t));const ticket=gate.issue(binding());
 for(const change of [{command_id:'C2'},{command_sha256:'e'.repeat(64)},{policy_sha256:'e'.repeat(64)},{node_id:'pc'},{attempt_id:'attempt2'},{generation:2}])
  assert.equal(gate.consume({...binding(),...change},ticket,'c'.repeat(64)),false);
 assert.equal(gate.consume(binding(),'f'.repeat(64),'c'.repeat(64)),false);
 assert.equal(gate.consume(binding(),ticket,'c'.repeat(64)),true);gate.close();
});
test('cancellation fences both consumed tickets and future commands durably',t=>{
 const path=setup(t);let gate=new NativeEntryGate(path);const ticket=gate.issue(binding());
 assert.equal(gate.consume(binding(),ticket,'c'.repeat(64)),true);gate.fence(binding());gate.close();
 gate=new NativeEntryGate(path);assert.equal(gate.consume(binding(),ticket,'c'.repeat(64)),false);
 assert.throws(()=>gate.issue({...binding(),command_id:'C2'}),/attempt_fenced/);
 const next={...binding(),generation:2};assert.equal(gate.consume(next,gate.issue(next),'d'.repeat(64)),true);gate.close();
});
test('two host connections serialize competing native children',t=>{
 const path=setup(t);const first=new NativeEntryGate(path);const second=new NativeEntryGate(path);const ticket=first.issue(binding());
 assert.equal(first.consume(binding(),ticket,'c'.repeat(64)),true);assert.equal(second.consume(binding(),ticket,'d'.repeat(64)),false);
 first.close();second.close();
});
test('unsafe database permissions are refused',t=>{
 const path=setup(t);const gate=new NativeEntryGate(path);gate.close();chmodSync(path,0o644);
 assert.throws(()=>new NativeEntryGate(path),/unsafe_entry_state/);
});
