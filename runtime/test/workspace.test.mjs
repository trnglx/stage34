import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync,statSync,existsSync,readdirSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {WorkspacePreparer} from '../dist/workspace.js';
const identity=()=>({node_id:'mac',attempt_id:'attempt1',generation:1,policy_sha256:'a'.repeat(64)});
function setup(t){
 const root=mkdtempSync(join(tmpdir(),'orch-source-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
 const source=join(root,'mirror');mkdirSync(source,{mode:0o700});
 const git=(...args)=>execFileSync('/usr/bin/git',['-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false',...args],{
  env:{PATH:'/usr/bin:/bin',HOME:root,GIT_CONFIG_NOSYSTEM:'1',GIT_CONFIG_GLOBAL:'/dev/null'},encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
 git('init',source);writeFileSync(join(source,'task.txt'),'base\n');git('-C',source,'add','task.txt');
 git('-C',source,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','base');
 const base=git('-C',source,'rev-parse','HEAD');
 return {root,source,git,base,profile:{state_root:join(root,'worker'),source_repository:source,git_executable:'/usr/bin/git'}};
}
test('checkout pins exact base, disables source hooks and isolates Git object files',async t=>{
 const f=setup(t);const hookRoot=join(f.root,'hooks');mkdirSync(hookRoot,{mode:0o700});
 writeFileSync(join(hookRoot,'post-checkout'),'#!/bin/sh\ntouch '+join(f.root,'HOOK_RAN')+'\n',{mode:0o700});
 f.git('-C',f.source,'config','core.hooksPath',hookRoot);
 f.git('-C',f.source,'config','uploadpack.packObjectsHook','touch '+join(f.root,'UPLOAD_HOOK_RAN'));
 const preparer=new WorkspacePreparer(f.profile);const prepared=await preparer.prepare(identity(),f.base);
 assert.equal(f.git('-C',prepared.workspace,'rev-parse','HEAD'),f.base);assert.equal(readFileSync(join(prepared.workspace,'task.txt'),'utf8'),'base\n');
 assert.equal(existsSync(join(f.root,'HOOK_RAN')),false);assert.equal(existsSync(join(f.root,'UPLOAD_HOOK_RAN')),false);
 const relative=join('objects',f.base.slice(0,2),f.base.slice(2));
 const sourceObject=statSync(join(f.source,'.git',relative));const jobObject=statSync(join(prepared.workspace,'.git',relative));
 assert.notEqual(sourceObject.ino,jobObject.ino);assert.equal(jobObject.nlink,1);
 assert.equal(existsSync(join(prepared.workspace,'.git','objects','info','alternates')),false);
 writeFileSync(join(prepared.workspace,'task.txt'),'job changes\n');assert.equal(readFileSync(join(f.source,'task.txt'),'utf8'),'base\n');
 const record=JSON.parse(readFileSync(join(prepared.workspace,'..','source.json'),'utf8'));assert.equal(record.phase,'prepared');assert.equal(record.base_sha,f.base);
});
test('replay refuses to overwrite changed workspace or retry an interrupted preparation',async t=>{
 const f=setup(t);const preparer=new WorkspacePreparer(f.profile);const prepared=await preparer.prepare(identity(),f.base);
 writeFileSync(join(prepared.workspace,'task.txt'),'keep my changes\n');
 await assert.rejects(preparer.prepare(identity(),f.base),/workspace_not_empty/);assert.equal(readFileSync(join(prepared.workspace,'task.txt'),'utf8'),'keep my changes\n');
 const failing=new WorkspacePreparer({...f.profile,git_executable:'/nonexistent/orch-git'});
 await assert.rejects(failing.prepare({...identity(),attempt_id:'attempt2'},f.base),/workspace_git_failed/);
 await assert.rejects(preparer.prepare({...identity(),attempt_id:'attempt2'},f.base),/workspace_already_claimed/);
});
test('an advanced mirror tip cannot replace the requested historical base',async t=>{
 const f=setup(t);writeFileSync(join(f.source,'task.txt'),'later tip\n');f.git('-C',f.source,'add','task.txt');
 f.git('-C',f.source,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','later');
 assert.notEqual(f.git('-C',f.source,'rev-parse','HEAD'),f.base);
 let prepared;await assert.doesNotReject(async()=>{prepared=await new WorkspacePreparer(f.profile).prepare(identity(),f.base);});
 assert.equal(readFileSync(join(prepared.workspace,'task.txt'),'utf8'),'base\n');
 assert.equal(f.git('-C',prepared.workspace,'rev-parse','HEAD'),f.base);
});
test('concurrent preparation for one attempt produces only one completed workspace',async t=>{
 const f=setup(t);const preparer=new WorkspacePreparer(f.profile);
 const results=await Promise.allSettled([preparer.prepare(identity(),f.base),preparer.prepare(identity(),f.base)]);
 assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(results.filter(r=>r.status==='rejected').length,1);
});
test('invalid bindings and missing base commits never yield a prepared manifest',async t=>{
 const f=setup(t);const preparer=new WorkspacePreparer(f.profile);
 await assert.rejects(preparer.prepare({...identity(),attempt_id:'../escape'},f.base),/invalid_workspace_binding/);
 await assert.rejects(preparer.prepare(identity(),'f'.repeat(40)),/workspace_git_failed/);
 const roots=readdirSync(join(f.profile.state_root,'jobs'));assert.equal(roots.length,1);
 assert.equal(JSON.parse(readFileSync(join(f.profile.state_root,'jobs',roots[0],'source.json'),'utf8')).phase,'preparing');
});
