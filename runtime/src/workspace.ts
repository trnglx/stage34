/** One-shot host preparation from a trusted local mirror, before native tools can run. */
import {execFile} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {constants,mkdirSync,lstatSync,realpathSync,readdirSync,openSync,closeSync,writeFileSync,fsyncSync,renameSync} from 'node:fs';
import {join,isAbsolute} from 'node:path';
import type {ContainerIdentity} from './container.js';

export interface WorkspaceProfile {state_root:string;source_repository:string;git_executable:string;}
export class WorkspaceError extends Error {}
const id=(value:unknown):value is string=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(value);
function privateDirectory(path:string):string {
  mkdirSync(path,{recursive:true,mode:0o700});const info=lstatSync(path);
  if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new WorkspaceError('unsafe_workspace_state');
  return realpathSync(path);
}
function durable(path:string,value:unknown,exclusive:boolean):void {
  const target=exclusive?path:path+'.'+randomUUID();
  const fd=openSync(target,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try {writeFileSync(fd,JSON.stringify(value));fsyncSync(fd);} finally {closeSync(fd);}
  if(!exclusive)renameSync(target,path);
  const parent=openSync(join(path,'..'),constants.O_RDONLY);try {fsyncSync(parent);} finally {closeSync(parent);}
}

export class WorkspacePreparer {
  private readonly profile:Readonly<WorkspaceProfile>;
  constructor(profile:WorkspaceProfile) {
    if(!isAbsolute(profile.state_root)||!isAbsolute(profile.source_repository)||!isAbsolute(profile.git_executable))throw new WorkspaceError('invalid_workspace_profile');
    const source=lstatSync(profile.source_repository);
    if(!source.isDirectory()||source.isSymbolicLink()||source.uid!==process.getuid?.()||(source.mode&0o022)!==0)throw new WorkspaceError('untrusted_source_repository');
    this.profile=Object.freeze({...profile,state_root:privateDirectory(profile.state_root),source_repository:realpathSync(profile.source_repository)});
  }
  private async git(args:string[],home:string):Promise<string> {
    return new Promise((resolve,reject)=>execFile(this.profile.git_executable,[
      '-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false','-c','core.attributesFile=/dev/null',
      '-c','protocol.ext.allow=never','-c','submodule.recurse=false',...args
    ],{timeout:30_000,maxBuffer:1024*1024,env:{PATH:'/usr/bin:/bin',HOME:home,LANG:'C',
      GIT_CONFIG_NOSYSTEM:'1',GIT_CONFIG_SYSTEM:'/dev/null',GIT_CONFIG_GLOBAL:'/dev/null',
      GIT_NO_REPLACE_OBJECTS:'1',GIT_TERMINAL_PROMPT:'0'}},(error,stdout)=>{
      if(error)reject(new WorkspaceError('workspace_git_failed'));else resolve(stdout.trim());
    }));
  }
  /** Never retries or re-checks out an existing attempt: its files may already have model changes. */
  async prepare(job:ContainerIdentity,baseSha:string,checkpointHead?:string):Promise<Readonly<{workspace:string;base_sha:string}>> {
    if(!id(job.node_id)||!id(job.attempt_id)||!Number.isSafeInteger(job.generation)||job.generation<1||
      !/^[a-f0-9]{64}$/.test(job.policy_sha256)||!/^[a-f0-9]{40}$/.test(baseSha))throw new WorkspaceError('invalid_workspace_binding');
    const head=checkpointHead??baseSha;
    if(!/^[a-f0-9]{40}$/.test(head))throw new WorkspaceError('invalid_checkpoint_head');
    if(checkpointHead!==undefined) {
      // Verify provenance in the trusted local mirror before creating a workspace.
      await this.git(['-C',this.profile.source_repository,'merge-base','--is-ancestor',baseSha,head],this.profile.state_root);
    }
    const key=createHash('sha256').update(JSON.stringify([job.node_id,job.attempt_id,job.generation])).digest('hex').slice(0,32);
    const root=privateDirectory(join(this.profile.state_root,'jobs',key));
    const workspace=privateDirectory(join(root,'workspace'));
    if(readdirSync(workspace).length)throw new WorkspaceError('workspace_not_empty');
    const manifest={schema_version:1,...job,base_sha:baseSha,head_sha:head};
    const marker=join(root,'source.json');
    try {durable(marker,{...manifest,phase:'preparing'},true);}
    catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')throw new WorkspaceError('workspace_already_claimed');throw error;}
    const home=privateDirectory(join(root,'git-home'));const template=privateDirectory(join(root,'git-template'));
    // Local copying avoids source upload-pack hooks; no hardlinks or alternates may couple job writes to the mirror.
    await this.git(['clone','--local','--no-hardlinks','--dissociate','--no-checkout','--no-recurse-submodules',
      '--template='+template,'--',this.profile.source_repository,workspace],home);
    await this.git(['-C',workspace,'checkout','--detach',head],home);
    if(await this.git(['-C',workspace,'rev-parse','--verify','HEAD'],home)!==head)throw new WorkspaceError('workspace_base_mismatch');
    durable(marker,{...manifest,phase:'prepared'},false);
    return Object.freeze({workspace,base_sha:baseSha});
  }
}
