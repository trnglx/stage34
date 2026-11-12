/** Docker process boundary. Calls stay outside controller/journal transactions. */
import {execFile} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {constants,mkdirSync,lstatSync,realpathSync,readdirSync,openSync,fstatSync,writeFileSync,fsyncSync,closeSync,renameSync,readFileSync} from 'node:fs';
import {join,isAbsolute} from 'node:path';
import {storageMount,validateVolumeStorage,type VolumeStorage} from './storage-mount.js';

export interface ContainerProfile {
  executable:string;
  endpoint:string;
  image_id:string;
  state_root:string;
  uid:number;
  gid:number;
  cpus:number;
  memory_mb:number;
  pids_limit:number;
  network:string;
  max_runtime_ms:number;
  entry_directory?:string;
  credential_file?:string;
  /** Worker runs in Linux with this Docker volume mounted at root; never mount the whole volume into jobs. */
  storage?:VolumeStorage;
}
export interface ContainerIdentity {node_id:string;attempt_id:string;generation:number;policy_sha256:string;}
export interface ContainerCommand {id:string;sha256:string;}
export interface ContainerState {id:string;running:boolean;command_id:string;command_sha256:string;}
export class ContainerError extends Error {
  constructor(readonly code:string) {super(code);}
}
const identity=(value:unknown):value is string=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(value);
const sha=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);

function privateDirectory(path:string):string {
  mkdirSync(path,{recursive:true,mode:0o700});
  const info=lstatSync(path);
  if (!info.isDirectory()||info.isSymbolicLink()||(info.mode&0o077)!==0||info.uid!==process.getuid?.()) throw new ContainerError('unsafe_container_state');
  return realpathSync(path);
}

function atomic(path:string,value:unknown):void {
  const temp=path+'.'+randomUUID();
  const fd=openSync(temp,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try {writeFileSync(fd,JSON.stringify(value));fsyncSync(fd);} finally {closeSync(fd);}
  renameSync(temp,path);
  const parent=openSync(join(path,'..'),constants.O_RDONLY);
  try {fsyncSync(parent);} finally {closeSync(parent);}
}

/** Validate only at launch; unavailable credentials/sockets must never prevent physical stopping. */
function launchBindings(profile:Readonly<ContainerProfile>,input:unknown):{ticket?:string;readOnly:boolean;credentials?:string} {
  const value=input as {entry_ticket?:unknown;job?:{provider?:unknown;sandbox?:unknown;inference?:unknown}}|null;
  if(value?.job?.inference!==undefined&&profile.network!=='none')throw new ContainerError('inference_network_denied');
  let credentials:string|undefined;
  if(profile.entry_directory) {
    const info=lstatSync(profile.entry_directory);const socket=lstatSync(join(profile.entry_directory,'gate.sock'));
    if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==profile.uid||(info.mode&0o077)!==0||
      !socket.isSocket()||socket.uid!==profile.uid||(socket.mode&0o077)!==0||socket.nlink!==1||
      readdirSync(profile.entry_directory).join(',')!=='gate.sock')throw new ContainerError('unsafe_entry_mount');
    if(typeof value?.entry_ticket!=='string'||!sha(value.entry_ticket)||
      !['claude','codex'].includes(String(value.job?.provider))||
      !['read-only','workspace-write'].includes(String(value.job?.sandbox)))throw new ContainerError('invalid_native_launch');
  }
  if(profile.credential_file) {
    if(!profile.entry_directory)throw new ContainerError('entry_mount_required');
    const fd=openSync(profile.credential_file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
    try {
      const info=fstatSync(fd);
      if(!info.isFile()||info.uid!==profile.uid||info.nlink!==1||(info.mode&0o077)!==0||info.size>16384)throw new ContainerError('unsafe_provider_credentials');
      const text=readFileSync(fd,'utf8');const after=fstatSync(fd);
      if(info.size!==after.size||info.mtimeMs!==after.mtimeMs||info.ctimeMs!==after.ctimeMs||/[\0\r]/.test(text))throw new ContainerError('unsafe_provider_credentials');
      const allowed=value?.job?.provider==='codex'?['CODEX_API_KEY','OPENAI_API_KEY']:['ANTHROPIC_API_KEY','CLAUDE_CODE_OAUTH_TOKEN'];
      const seen=new Set<string>();
      for(const line of text.split('\n').filter(Boolean)) {
        const split=line.indexOf('=');const key=line.slice(0,split);const credential=line.slice(split+1);
        if(split<1||!allowed.includes(key)||seen.has(key)||!credential||credential.length>8192||/[\s]/.test(credential))throw new ContainerError('invalid_provider_credentials');
        seen.add(key);
      }
      if(seen.size!==1)throw new ContainerError('invalid_provider_credentials');
      credentials=text;
    } finally {closeSync(fd);}
  }
  return {ticket:profile.entry_directory?value!.entry_ticket as string:undefined,readOnly:value?.job?.sandbox==='read-only',credentials};
}

/** Docker consumes the validated bytes, not a mutable source pathname. This directory is never mounted. */
function credentialSnapshot(root:string,command:ContainerCommand,text:string):string {
  const directory=privateDirectory(join(root,'credentials'));const path=join(directory,command.id+'.'+command.sha256+'.env');
  let fd:number;
  try {fd=openSync(path,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);}
  catch(error) {
    if((error as NodeJS.ErrnoException).code!=='EEXIST')throw error;
    const existing=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
    try {
      const info=fstatSync(existing);
      if(!info.isFile()||info.nlink!==1||info.uid!==process.getuid?.()||(info.mode&0o077)!==0||info.size>16384||readFileSync(existing,'utf8')!==text)throw new ContainerError('credential_snapshot_conflict');
    } finally {closeSync(existing);}
    return path;
  }
  try {writeFileSync(fd,text);fsyncSync(fd);} finally {closeSync(fd);}
  const parent=openSync(directory,constants.O_RDONLY);try {fsyncSync(parent);} finally {closeSync(parent);}
  return path;
}

export class ContainerSupervisor {
  private readonly profile:Readonly<ContainerProfile>;
  private readonly root:string;
  constructor(profile:ContainerProfile) {
    validateVolumeStorage(profile.storage);
    if (!isAbsolute(profile.executable)||!isAbsolute(profile.state_root)||/[\0\r\n,]/.test(profile.state_root)||
        !/^unix:\/\/\//.test(profile.endpoint)||/[\0\r\n]/.test(profile.endpoint)||
        !/^sha256:[a-f0-9]{64}$/.test(profile.image_id)||!Number.isSafeInteger(profile.uid)||profile.uid<1||profile.uid!==process.getuid?.()||
        !Number.isSafeInteger(profile.gid)||profile.gid<1||!Number.isFinite(profile.cpus)||profile.cpus<0.1||profile.cpus>8||
        !Number.isSafeInteger(profile.memory_mb)||profile.memory_mb<256||profile.memory_mb>32768||
        !Number.isSafeInteger(profile.pids_limit)||profile.pids_limit<16||profile.pids_limit>512||
        !Number.isSafeInteger(profile.max_runtime_ms)||profile.max_runtime_ms<1||profile.max_runtime_ms>3_600_000||
        [profile.entry_directory,profile.credential_file].some(path=>path!==undefined&&(!isAbsolute(path)||/[\0\r\n,]/.test(path)))||
        !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$/.test(profile.network)||['host','bridge','default'].includes(profile.network)) {
      throw new ContainerError('invalid_container_profile');
    }
    this.profile=Object.freeze({...profile,...(profile.storage?{storage:Object.freeze({...profile.storage})}:{})});
    this.root=privateDirectory(profile.state_root);
    privateDirectory(join(this.root,'docker-config'));
  }

  /** Identity lookup must not touch paths that a job can chmod, remove or replace. */
  private address(job:ContainerIdentity) {
    if (!identity(job.node_id)||!identity(job.attempt_id)||!Number.isSafeInteger(job.generation)||job.generation<1||!sha(job.policy_sha256)) throw new ContainerError('invalid_container_identity');
    const key=createHash('sha256').update(JSON.stringify([job.node_id,job.attempt_id,job.generation])).digest('hex').slice(0,32);
    return {name:'orch-'+key,root:join(this.root,'jobs',key)};
  }

  private placement(job:ContainerIdentity) {
    const address=this.address(job);
    const root=privateDirectory(address.root);
    return {...address,root,workspace:privateDirectory(join(root,'workspace')),home:privateDirectory(join(root,'home')),
      control:privateDirectory(join(root,'control')),output:privateDirectory(join(root,'output'))};
  }

  private async cli(arguments_:string[]):Promise<string> {
    const profile=this.profile;
    return await new Promise<string>((resolve,reject)=>{
      execFile(profile.executable,['--host',profile.endpoint,...arguments_],{timeout:20_000,maxBuffer:1024*1024,
        env:{PATH:'/usr/bin:/bin',DOCKER_CONFIG:join(this.root,'docker-config'),LANG:'C'},windowsHide:true},(error,stdout)=>{
        if(error)reject(new ContainerError('container_effect_unknown'));else resolve(stdout.trim());
      });
    });
  }

  async inspect(job:ContainerIdentity):Promise<ContainerState|null> {
    const placement=this.address(job);
    // list first to distinguish absence from inspect/daemon failure without reading Config.Env.
    const ids=await this.cli(['container','ls','--all','--quiet','--no-trunc','--filter','name=^/'+placement.name+'$']);
    if(!ids)return null;
    if(!/^[a-f0-9]{64}$/.test(ids))throw new ContainerError('container_identity_conflict');
    const format='{"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}}}';
    let state:unknown;
    try {state=JSON.parse(await this.cli(['container','inspect','--format',format,placement.name]));}
    catch(error){if(error instanceof ContainerError)throw error;throw new ContainerError('invalid_container_receipt');}
    const row=state as {id?:unknown;image?:unknown;running?:unknown;labels?:Record<string,unknown>};
    if(!row||row.id!==ids||row.image!==this.profile.image_id||typeof row.running!=='boolean'||
        row.labels?.['orch.node']!==job.node_id||row.labels?.['orch.attempt']!==job.attempt_id||
        row.labels?.['orch.generation']!==String(job.generation)||row.labels?.['orch.policy']!==job.policy_sha256||
        !identity(row.labels?.['orch.command'])||!sha(row.labels?.['orch.command_sha']))throw new ContainerError('container_identity_conflict');
    return {id:ids,running:row.running,command_id:row.labels['orch.command'],command_sha256:row.labels['orch.command_sha']};
  }

  fence(job:ContainerIdentity,command:ContainerCommand):void {
    if(!identity(command.id)||!sha(command.sha256))throw new ContainerError('invalid_container_command');
    const placement=this.address(job);
    const control=privateDirectory(join(privateDirectory(placement.root),'control'));
    atomic(join(control,'cancelled.json'),{...job,command_id:command.id,command_sha256:command.sha256});
  }

  private fenced(control:string):boolean {
    try {readFileSync(join(control,'cancelled.json'));return true;}
    catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return false;throw new ContainerError('unsafe_container_state');}
  }

  async launch(job:ContainerIdentity,command:ContainerCommand,input:unknown):Promise<ContainerState> {
    if(!identity(command.id)||!sha(command.sha256))throw new ContainerError('invalid_container_command');
    const requested=(input as {job?:{timeout_ms?:unknown}}|null)?.job?.timeout_ms;
    const runtimeMs=requested??this.profile.max_runtime_ms;
    if(typeof runtimeMs!=='number'||!Number.isSafeInteger(runtimeMs)||runtimeMs<1||runtimeMs>this.profile.max_runtime_ms)throw new ContainerError('invalid_container_deadline');
    const bindings=launchBindings(this.profile,input);
    const placement=this.placement(job);
    const mounts=[
      ...(this.profile.entry_directory?[storageMount(this.profile.storage,this.profile.entry_directory,'/run/orch-entry',true)]:[]),
      ...[[placement.workspace,'/workspace',bindings.readOnly],[placement.home,'/home/worker',false],
        [placement.control,'/run/orch-control',true],[placement.output,'/run/orch-output',false]].map(
          ([source,target,readOnly])=>storageMount(this.profile.storage,source as string,target as string,readOnly as boolean))
    ];
    if(this.fenced(placement.control))throw new ContainerError('attempt_fenced');
    const credentialFile=bindings.credentials===undefined?undefined:credentialSnapshot(placement.root,command,bindings.credentials);
    const existing=await this.inspect(job);
    if(existing){
      if(existing.command_id===command.id&&existing.command_sha256!==command.sha256)throw new ContainerError('container_command_conflict');
      if(existing.command_id===command.id&&existing.command_sha256===command.sha256)return existing;
      if(existing.running)throw new ContainerError('container_busy');
      await this.cli(['container','rm',existing.id]);
    }
    // Mounted as a directory: atomic replacement remains visible to a late container start.
    atomic(join(placement.control,'command.json'),{identity:job,command,input,...(bindings.ticket?{entry_ticket:bindings.ticket}:{})});
    const profile=this.profile;
    const labels={'orch.node':job.node_id,'orch.attempt':job.attempt_id,'orch.generation':String(job.generation),
      'orch.policy':job.policy_sha256,'orch.command':command.id,'orch.command_sha':command.sha256};
    const arguments_=['container','create','--name',placement.name,'--pull=never','--restart=no','--read-only',
      '--cap-drop=ALL','--security-opt=no-new-privileges','--init','--no-healthcheck','--log-driver=none',
      '--user',`${profile.uid}:${profile.gid}`,'--cpus',String(profile.cpus),'--memory',`${profile.memory_mb}m`,
      '--memory-swap',`${profile.memory_mb}m`,'--pids-limit',String(profile.pids_limit),'--network',profile.network,
      '--workdir','/workspace','--entrypoint','/usr/bin/timeout','--tmpfs','/tmp:rw,nosuid,nodev,size=256m'];
    for(const [key,value] of Object.entries(labels))arguments_.push('--label',key+'='+value);
    if(credentialFile)arguments_.push('--env-file',credentialFile);
    for(const mount of mounts)arguments_.push('--mount',mount);
    arguments_.push(profile.image_id,'--signal=TERM','--kill-after=5s',`${runtimeMs/1000}s`,
      '/usr/local/bin/node','/opt/orch/dist/job-child.js','--command-id',command.id);
    await this.cli(arguments_);
    if(this.fenced(placement.control))throw new ContainerError('attempt_fenced');
    const created=await this.inspect(job);
    if(!created||created.command_id!==command.id||created.command_sha256!==command.sha256)throw new ContainerError('container_identity_conflict');
    await this.cli(['container','start',created.id]);
    const started=await this.inspect(job);
    if(!started)throw new ContainerError('container_effect_unknown');
    return started;
  }

  async stop(job:ContainerIdentity,command:ContainerCommand,permanent:boolean):Promise<{stopped:true;container_id:string|null}> {
    if(!identity(command.id)||!sha(command.sha256))throw new ContainerError('invalid_container_command');
    if(permanent)this.fence(job,command);
    else {
      // A paused command also invalidates an old launch's input before stopping it.
      const control=privateDirectory(join(privateDirectory(this.address(job).root),'control'));
      atomic(join(control,'command.json'),{identity:job,command,paused:true});
    }
    return this.stopPhysical(job);
  }

  /** Emergency host supervision only: stopped proof does not imply a durable launch fence. */
  async stopPhysical(job:ContainerIdentity):Promise<{stopped:true;container_id:string|null}> {
    const before=await this.inspect(job);
    if(!before)return {stopped:true,container_id:null};
    if(before.running)await this.cli(['container','stop','--signal=SIGTERM','--timeout=5',before.id]);
    const after=await this.inspect(job);
    if(after?.running)throw new ContainerError('container_effect_unknown');
    return {stopped:true,container_id:after?.id??null};
  }
}
