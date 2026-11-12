import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,realpathSync,lstatSync,writeFileSync,readFileSync,rmSync,chmodSync,readdirSync,symlinkSync,existsSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {ContainerSupervisor} from '../dist/container.js';
import {guardAttempt} from '../dist/host-deadline.js';
const job=()=>({node_id:'node1',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64)});
const command=()=>({id:'C-start',sha256:'b'.repeat(64)});
function fixture(t,mode='') {
 const root=mkdtempSync(join(tmpdir(),'container-boundary-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
 const executable=join(root,'docker-fixture.cjs');const state=join(root,'state.json');
 writeFileSync(state,JSON.stringify({calls:[],container:null,mode}));
 writeFileSync(executable,`#!${process.execPath}
const fs=require('node:fs'),path=require('node:path');const file=path.join(__dirname,'state.json');
const state=JSON.parse(fs.readFileSync(file,'utf8'));const args=process.argv.slice(2);state.calls.push(args);
const operation=args[3];let result='';let fail=false;
if(operation==='ls')result=state.container?.id??'';
else if(operation==='inspect')result=JSON.stringify(state.container);
else if(operation==='create'){
 const labels={};for(let i=0;i<args.length;i++)if(args[i]==='--label'){const [key,value]=args[++i].split('=');labels[key]=value;}
 state.container={id:'d'.repeat(64),image:args.find(value=>/^sha256:[a-f0-9]{64}$/.test(value)),running:false,labels};result=state.container.id;
}else if(operation==='start'){state.container.running=true;result=state.container.id;if(state.mode==='lost-start-ack')fail=true;}
else if(operation==='stop'){if(state.mode!=='stuck-stop')state.container.running=false;result=state.container.id;}
else if(operation==='rm'){state.container=null;}
else fail=true;
fs.writeFileSync(file,JSON.stringify(state));if(fail)process.exit(1);process.stdout.write(result);
`,{mode:0o700});
 const profile={executable,endpoint:'unix:///private/docker.sock',image_id:'sha256:'+'e'.repeat(64),state_root:join(root,'worker'),
 uid:process.getuid?.()||1000,gid:process.getgid?.()||1000,cpus:1,memory_mb:1024,pids_limit:64,network:'none',max_runtime_ms:30000};
 return {profile,state,read:()=>JSON.parse(readFileSync(state,'utf8'))};
}

async function entryFixture(t,f) {
 const path=join(dirname(f.state),'entry');mkdirSync(path,{mode:0o700});const server=createServer(socket=>socket.destroy());
 await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(join(path,'gate.sock'),resolve);});chmodSync(join(path,'gate.sock'),0o600);
 t.after(()=>new Promise(resolve=>server.close(resolve)));return path;
}

test('container launch independently refuses inference on a named network',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor({...f.profile,network:'egress-net'});
 await assert.rejects(supervisor.launch(job(),command(),{job:{provider:'codex',sandbox:'workspace-write',inference:{schema_version:1}}}),/inference_network_denied/);
 assert.equal(f.read().calls.length,0);
});

test('credential rotation after preflight cannot change the environment consumed by Docker',async t=>{
 const f=fixture(t);const entry=await entryFixture(t,f);const credential=join(dirname(f.state),'provider.env');
 writeFileSync(credential,'CODEX_API_KEY=validated-secret\n',{mode:0o600});
 const supervisor=new ContainerSupervisor({...f.profile,entry_directory:entry,credential_file:credential});
 const original=supervisor.inspect.bind(supervisor);let rotated=false;
 supervisor.inspect=async(...args)=>{if(!rotated){writeFileSync(credential,'UNVALIDATED_ENV=changed-after-check\n');rotated=true;}return original(...args);};
 await supervisor.launch(job(),command(),{entry_ticket:'f'.repeat(64),job:{provider:'codex',sandbox:'workspace-write'}});
 const create=f.read().calls.find(args=>args[3]==='create');const consumed=create[create.indexOf('--env-file')+1];
 assert.notEqual(consumed,credential);
 assert.equal(readFileSync(consumed,'utf8'),'CODEX_API_KEY=validated-secret\n');
 assert.equal(JSON.stringify(create).includes('validated-secret'),false);
});

test('native launch mounts only the entry socket, propagates its ticket and keeps credentials out of argv',async t=>{
 const f=fixture(t);const entry=await entryFixture(t,f);const credential=join(dirname(f.state),'provider.env');
 writeFileSync(credential,'CODEX_API_KEY=PRIVATE_PROVIDER_SECRET\n',{mode:0o600});
 const supervisor=new ContainerSupervisor({...f.profile,entry_directory:entry,credential_file:credential});
 await supervisor.launch(job(),command(),{entry_ticket:'f'.repeat(64),job:{provider:'codex',sandbox:'read-only',timeout_ms:1000}});
 const create=f.read().calls.find(args=>args[3]==='create');
 assert.ok(create.includes('--env-file'));const snapshot=create[create.indexOf('--env-file')+1];assert.notEqual(snapshot,credential);
 assert.equal(readFileSync(snapshot,'utf8'),'CODEX_API_KEY=PRIVATE_PROVIDER_SECRET\n');assert.equal(lstatSync(snapshot).mode&0o777,0o600);
 assert.equal(lstatSync(dirname(snapshot)).mode&0o777,0o700);
 assert.ok(create.some(value=>value.includes('dst=/run/orch-entry,readonly')));
 assert.ok(create.some(value=>value.includes('dst=/workspace,readonly')));
 assert.equal(JSON.stringify(create).includes('PRIVATE_PROVIDER_SECRET'),false);assert.equal(JSON.stringify(create).includes('f'.repeat(64)),false);
 const roots=readdirSync(join(f.profile.state_root,'jobs'));const control=JSON.parse(readFileSync(join(f.profile.state_root,'jobs',roots[0],'control','command.json'),'utf8'));
 assert.equal(control.entry_ticket,'f'.repeat(64));
 rmSync(entry,{recursive:true,force:true});rmSync(credential);
 assert.equal((await supervisor.stopPhysical(job())).stopped,true);
});

test('native credentials and socket preflight fail before contacting Docker',async t=>{
 const f=fixture(t);const entry=await entryFixture(t,f);const credential=join(dirname(f.state),'provider.env');
 const profile={...f.profile,entry_directory:entry,credential_file:credential};const input={entry_ticket:'f'.repeat(64),job:{provider:'codex',sandbox:'workspace-write'}};
 for(const value of ['ANTHROPIC_API_KEY=other-provider\n','CODEX_API_KEY=first\nCODEX_API_KEY=second\n','CODEX_API_KEY=one\nOPENAI_API_KEY=two\n','CODEX_API_KEY=\n']) {
  writeFileSync(credential,value,{mode:0o600});await assert.rejects(new ContainerSupervisor(profile).launch(job(),command(),input),/invalid_provider_credentials/);
 }
 writeFileSync(credential,'CODEX_API_KEY=value\n');chmodSync(credential,0o644);
 await assert.rejects(new ContainerSupervisor(profile).launch(job(),command(),input),/unsafe_provider_credentials/);
 chmodSync(credential,0o600);writeFileSync(join(entry,'worker.sock'),'not-a-consume-endpoint');
 await assert.rejects(new ContainerSupervisor(profile).launch(job(),command(),input),/unsafe_entry_mount/);
 assert.equal(f.read().calls.length,0);
});

test('host guardian physically stops the real boundary while its fence storage is failing',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);await supervisor.launch(job(),command(),{});
 const original=supervisor.fence.bind(supervisor);let storageFailed=true;let retries=0;
 supervisor.fence=(...args)=>{if(storageFailed)throw new Error('disk_full');return original(...args);};
 const time={wall:()=>2000,monotonic:()=>0,wait:async()=>{
  assert.equal(f.read().container.running,false,'physical stop must precede fence storage recovery');
  storageFailed=false;assert.ok(++retries<2);
 }};
 await guardAttempt({identity:job(),command:command(),deadline_at_ms:1000,max_runtime_ms:1000},supervisor,{fence(){}},time);
 assert.equal(retries,1);assert.ok(f.read().calls.some(args=>args[3]==='stop'));
});

test('physical stopping and durable fencing ignore job-writable directory permissions and paths',async t=>{
 for(const damage of ['permissions','missing','symlink']) {
  const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);await supervisor.launch(job(),command(),{});
  const root=join(f.profile.state_root,'jobs',readdirSync(join(f.profile.state_root,'jobs'))[0]);
  for(const name of ['workspace','home','output']) {
   const path=join(root,name);
   if(damage==='permissions')chmodSync(path,0o777);
   else {rmSync(path,{recursive:true,force:true});if(damage==='symlink')symlinkSync(tmpdir(),path);}
  }
  supervisor.fence(job(),{id:'C-stop',sha256:'f'.repeat(64)});
  assert.equal(existsSync(join(root,'control','cancelled.json')),true);
  assert.equal((await supervisor.stopPhysical(job())).stopped,true);
  assert.equal(f.read().container.running,false);
  assert.ok(f.read().calls.some(args=>args[3]==='stop'));
 }
});

test('inspection of an absent container never provisions job directories',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);
 assert.equal(await supervisor.inspect(job()),null);
 assert.equal(existsSync(join(f.profile.state_root,'jobs')),false);
});

test('container creation pins the image and applies resource, privilege and mount boundaries',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);
 assert.equal((await supervisor.launch(job(),command(),{prompt:'PRIVATE_INPUT'})).running,true);
 const create=f.read().calls.find(args=>args[3]==='create');
 for(const flag of ['--pull=never','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','--log-driver=none'])assert.ok(create.includes(flag));
 assert.equal(create[create.indexOf('--user')+1],`${f.profile.uid}:${f.profile.gid}`);
 assert.equal(create[create.indexOf('--network')+1],'none');assert.ok(create.includes(f.profile.image_id));
 assert.equal(JSON.stringify(create).includes('PRIVATE_INPUT'),false);
 assert.ok(create.some(value=>value.includes('dst=/run/orch-control,readonly')));
 assert.equal(create.some(value=>value.includes('docker.sock')&&value.includes('type=bind')),false);
 assert.equal(f.read().calls.filter(args=>args[3]==='start').length,1);
 await supervisor.launch(job(),command(),{prompt:'PRIVATE_INPUT'});
 assert.equal(f.read().calls.filter(args=>args[3]==='start').length,1);
});

test('a permanent cancellation barrier survives reconstruction and prevents a late start',async t=>{
 const f=fixture(t);new ContainerSupervisor(f.profile).fence(job(),{id:'C-stop',sha256:'f'.repeat(64)});
 await assert.rejects(new ContainerSupervisor(f.profile).launch(job(),command(),{}),/attempt_fenced/);
 assert.equal(f.read().calls.length,0);
});

test('start success with a lost CLI acknowledgement remains unknown and is inspected by identity',async t=>{
 const f=fixture(t,'lost-start-ack');const supervisor=new ContainerSupervisor(f.profile);
 await assert.rejects(supervisor.launch(job(),command(),{}),/container_effect_unknown/);
 assert.equal((await supervisor.inspect(job())).running,true);
 assert.equal(f.read().calls.filter(args=>args[3]==='start').length,1);
});

test('a stop acknowledgement requires a verified stopped container',async t=>{
 const f=fixture(t,'stuck-stop');const supervisor=new ContainerSupervisor(f.profile);
 await supervisor.launch(job(),command(),{});
 await assert.rejects(supervisor.stop(job(),{id:'C-stop',sha256:'f'.repeat(64)},true),/container_effect_unknown/);
 await assert.rejects(supervisor.launch(job(),command(),{}),/attempt_fenced/);
 const state=f.read();state.mode='';writeFileSync(f.state,JSON.stringify(state));
 assert.equal((await supervisor.stop(job(),{id:'C-stop',sha256:'f'.repeat(64)},true)).stopped,true);
});

test('a foreign container cannot be stopped even if its name matches',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);await supervisor.launch(job(),command(),{});
 const state=f.read();state.container.labels['orch.attempt']='different';writeFileSync(f.state,JSON.stringify(state));
 await assert.rejects(supervisor.stop(job(),{id:'C-stop',sha256:'f'.repeat(64)},true),/container_identity_conflict/);
 assert.equal(f.read().calls.filter(args=>args[3]==='stop').length,0);
});

test('unsafe or mutable container profiles are refused before invoking Docker',t=>{
 const f=fixture(t);
 for(const changes of [{uid:0},{network:'host'},{network:'bridge'},{endpoint:'tcp://example.invalid:2375'},
  {image_id:'example:latest'},{cpus:Infinity},{memory_mb:0},{pids_limit:-1}]){
  assert.throws(()=>new ContainerSupervisor({...f.profile,...changes}),/invalid_container_profile/);
 }
 assert.equal(f.read().calls.length,0);
});

test('container UID must match the private bind-mount owner',t=>{
 const f=fixture(t);
 assert.throws(()=>new ContainerSupervisor({...f.profile,uid:f.profile.uid+1}),/invalid_container_profile/);
 assert.throws(()=>new ContainerSupervisor({...f.profile,gid:0}),/invalid_container_profile/);
});

test('pause validates command identity before creating a barrier or contacting Docker',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);
 await assert.rejects(supervisor.stop(job(),{id:'invalid command',sha256:'bad'},false),/invalid_container_command/);
 assert.equal(f.read().calls.length,0);
});

test('a stopped container command ID cannot be reused with a different command hash',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor(f.profile);await supervisor.launch(job(),command(),{});
 await supervisor.stop(job(),{id:'C-pause',sha256:'f'.repeat(64)},false);
 await assert.rejects(supervisor.launch(job(),{id:'C-start',sha256:'0'.repeat(64)},{}),/container_command_conflict/);
});

test('an external watchdog bounds the container command independently of the SDK event loop',async t=>{
 const f=fixture(t);const supervisor=new ContainerSupervisor({...f.profile,max_runtime_ms:1000});
 await supervisor.launch(job(),command(),{job:{timeout_ms:500}});
 const create=f.read().calls.find(args=>args[3]==='create');
 assert.equal(create[create.indexOf('--entrypoint')+1],'/usr/bin/timeout');
 const afterImage=create.slice(create.indexOf(f.profile.image_id)+1);
 assert.deepEqual(afterImage.slice(0,5),['--signal=TERM','--kill-after=5s','0.5s','/usr/local/bin/node','/opt/orch/dist/job-child.js']);
});

test('Linux supervisor volume mapping preserves narrow job mounts and identity-only stopping',async t=>{
 const f=fixture(t);const entry=await entryFixture(t,f);const root=realpathSync(dirname(f.state));
 const supervisor=new ContainerSupervisor({...f.profile,state_root:join(root,'worker'),entry_directory:realpathSync(entry),storage:{volume:'approved-worker-state',root}});
 await supervisor.launch(job(),command(),{entry_ticket:'f'.repeat(64),job:{provider:'codex',sandbox:'read-only'}});
 const create=f.read().calls.find(args=>args[3]==='create');const mounts=create.filter((_,index)=>create[index-1]==='--mount');
 assert.equal(mounts.length,5);assert.ok(mounts.every(value=>value.startsWith('type=volume,src=approved-worker-state,')&&value.includes('volume-subpath=')));
 assert.ok(mounts.some(value=>value.includes('dst=/run/orch-entry,volume-subpath=entry,volume-nocopy,readonly')));
 assert.ok(mounts.some(value=>value.includes('dst=/workspace,')&&value.endsWith(',readonly')));
 assert.equal(mounts.some(value=>value.includes('volume-subpath=.,')),false);
 rmSync(entry,{recursive:true,force:true});chmodSync(root,0o777);
 try {assert.equal((await supervisor.stopPhysical(job())).stopped,true);} finally {chmodSync(root,0o700);}
});
