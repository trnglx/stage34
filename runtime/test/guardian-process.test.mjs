import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,readdirSync,lstatSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {launchGuardian} from '../dist/guardian-process.js';
import {NativeEntryGate} from '../dist/entry-gate.js';

function fixture(t){
 const root=mkdtempSync(join(tmpdir(),'guardian-process-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
 const identity={node_id:'mac',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64)};
 const command={id:'C1',sha256:'b'.repeat(64)};const image='sha256:'+'c'.repeat(64);
 const state=join(root,'docker.json');const executable=join(root,'docker.cjs');
 writeFileSync(state,JSON.stringify({calls:[],id:'d'.repeat(64),image,running:true,labels:{'orch.node':'mac','orch.attempt':'attempt1','orch.generation':'1',
  'orch.policy':identity.policy_sha256,'orch.command':command.id,'orch.command_sha':command.sha256}}));
 writeFileSync(executable,`#!${process.execPath}
const fs=require('node:fs'),path=require('node:path');const file=path.join(__dirname,'docker.json');const state=JSON.parse(fs.readFileSync(file));
const args=process.argv.slice(2);state.calls.push(args);let output='';
if(args[3]==='ls')output=state.id;else if(args[3]==='inspect')output=JSON.stringify(state);else if(args[3]==='stop')state.running=false;else process.exit(1);
fs.writeFileSync(file,JSON.stringify(state));process.stdout.write(output);
`,{mode:0o700});
 return {root,state,identity,command,container:{executable,endpoint:'unix:///private/fixture.sock',image_id:image,state_root:join(root,'worker'),
  uid:process.getuid()||1000,gid:process.getgid()||1000,cpus:1,memory_mb:512,pids_limit:32,network:'none',max_runtime_ms:1000},
  contract:{identity,command,deadline_at_ms:Date.now()+800,max_runtime_ms:1000},entry:join(root,'entry.sqlite3'),guards:join(root,'guardians')};
}
test('a separate host guardian acknowledges readiness then fences and verifies stopping at its deadline',async t=>{
 const f=fixture(t);const gate=new NativeEntryGate(f.entry);const binding={...f.identity,command_id:f.command.id,command_sha256:f.command.sha256};
 const ticket=gate.issue(binding);gate.close();
 const guard=await launchGuardian(f.guards,f.contract,f.container,f.entry);
 assert.notEqual(guard.pid,process.pid);assert.equal(guard.alive(),true);
 const timer=setTimeout(()=>process.kill(guard.pid,'SIGKILL'),4000);
 const exit=await guard.exited;clearTimeout(timer);assert.equal(exit.code,0);assert.equal(guard.alive(),false);
 const state=JSON.parse(readFileSync(f.state,'utf8'));assert.equal(state.running,false);assert.ok(state.calls.some(args=>args[3]==='stop'));
 const resumed=new NativeEntryGate(f.entry);try {assert.equal(resumed.consume(binding,ticket,'e'.repeat(64)),false);} finally {resumed.close();}
 assert.equal(lstatSync(join(f.guards,readdirSync(f.guards)[0])).mode&0o777,0o600);
 await assert.rejects(launchGuardian(f.guards,{...f.contract,deadline_at_ms:Date.now()+1000},f.container,f.entry),/guardian_already_claimed/);
});
test('failed guardian initialization never grants readiness or invokes Docker and cannot auto-retry',async t=>{
 const f=fixture(t);await assert.rejects(launchGuardian(f.guards,f.contract,{...f.container,uid:0},f.entry),/guardian_readiness_unknown/);
 assert.equal(JSON.parse(readFileSync(f.state,'utf8')).calls.length,0);
 await assert.rejects(launchGuardian(f.guards,f.contract,f.container,f.entry),/guardian_already_claimed/);
});
test('expired or malformed guardian contracts are refused before launching a child',async t=>{
 const f=fixture(t);
 await assert.rejects(launchGuardian(f.guards,{...f.contract,deadline_at_ms:1},f.container,f.entry),/guardian_deadline_expired/);
 await assert.rejects(launchGuardian(f.guards,{...f.contract,identity:{...f.identity,attempt_id:'../escape'}},f.container,f.entry),/invalid_host_deadline/);
 assert.equal(JSON.parse(readFileSync(f.state,'utf8')).calls.length,0);
});

test('loss of the readiness pipe cannot terminate a protecting host guardian',async t=>{
 const f=fixture(t);const path=join(f.root,'configuration.json');
 writeFileSync(path,JSON.stringify({...f.contract,container:f.container,entry_database:f.entry}),{mode:0o600});
 const child=spawn(process.execPath,[fileURLToPath(new URL('../dist/host-deadline.js',import.meta.url)),path],{stdio:['ignore','pipe','ignore']});
 child.stdout.destroy();const timer=setTimeout(()=>child.kill('SIGKILL'),4000);
 const code=await new Promise((resolve,reject)=>{child.on('error',reject);child.on('close',resolve);});clearTimeout(timer);
 assert.equal(code,0);assert.equal(JSON.parse(readFileSync(f.state,'utf8')).running,false);
});
