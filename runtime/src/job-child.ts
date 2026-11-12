/** Immutable container entrypoint. An intent is consumed before native SDK startup. */
import {constants,existsSync,lstatSync,mkdirSync,openSync,closeSync,writeFileSync,readFileSync,fsyncSync,renameSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {randomUUID,randomBytes} from 'node:crypto';
import {consumeEntryTicket} from './entry-rpc.js';
import {fileURLToPath} from 'node:url';
import {LocalWorker} from './worker.js';
import type {ApprovedRuntime} from './provenance.js';
import type {WorkerJob} from './service.js';
import type {ContainerIdentity,ContainerCommand} from './container.js';

interface Control {
  identity:ContainerIdentity;
  command:ContainerCommand;
  input:{job:WorkerJob;runtime:ApprovedRuntime;native_session?:string;prompt:string};
  entry_ticket?:string;
}
export interface JobOutput {
  schema_version:1;command_id:string;command_sha256:string;attempt_id:string;
  phase:'running'|'completed'|'failed'|'cancelled'|'unknown';session_id?:string;code?:string;
}
interface Execution {
  start(context:WorkerJob,prompt:string,session?:string):Promise<void>;
  inspect():{session_id:string|null;state:string}|null;
  cancel():void;
  close():void|Promise<void>;
}
type Factory=(runtime:ApprovedRuntime,environment:Record<string,string>,outputRoot:string,gateway?:{socketPath:string;toolServer:string;python:string;externalSandbox?:true})=>Promise<Execution>;
class JobError extends Error {}
const id=(value:unknown):value is string=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(value);
const sha=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);

function write(path:string,value:unknown):void {
  const temp=path+'.'+randomUUID();
  const fd=openSync(temp,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);
  try {writeFileSync(fd,JSON.stringify(value));fsyncSync(fd);} finally {closeSync(fd);}
  renameSync(temp,path);
  const parent=openSync(dirname(path),constants.O_RDONLY);
  try {fsyncSync(parent);} finally {closeSync(parent);}
}

function control(root:string,commandId:string):Control {
  if(existsSync(join(root,'cancelled.json')))throw new JobError('attempt_cancelled');
  const path=join(root,'command.json');const info=lstatSync(path);
  if(!info.isFile()||info.isSymbolicLink()||info.size>1024*1024)throw new JobError('invalid_control');
  let value:Control;
  try {value=JSON.parse(readFileSync(path,'utf8')) as Control;} catch {throw new JobError('invalid_control');}
  if(!value||!value.identity||!value.command||!value.input||value.command.id!==commandId||!sha(value.command.sha256))throw new JobError('stale_command');
  const {job,runtime,prompt}=value.input;
  if(!job||!runtime||!id(job.attempt_id)||!id(job.task_id)||!id(job.node_id)||
      job.attempt_id!==value.identity.attempt_id||job.node_id!==value.identity.node_id||
      job.generation!==value.identity.generation||job.policy_sha256!==value.identity.policy_sha256||
      !sha(job.policy_sha256)||!Number.isSafeInteger(job.generation)||job.generation<1||
      !Number.isSafeInteger(job.timeout_ms)||job.timeout_ms<1||job.timeout_ms>3_600_000||
      !['claude','codex'].includes(job.provider)||job.provider!==runtime.provider||
      typeof prompt!=='string'||Buffer.byteLength(prompt)>256*1024||
      (value.input.native_session!==undefined&&!id(value.input.native_session)))throw new JobError('invalid_job_binding');
  return value;
}

/** CLI uses fixed container paths; injected factories exist only for offline contract tests. */
export async function runContainerJob(options:{controlRoot:string;outputRoot:string;workspace:string;commandId:string},
  factory?:Factory):Promise<JobOutput> {
  if(!id(options.commandId))throw new JobError('invalid_command_id');
  const request=control(options.controlRoot,options.commandId);
  const {job,runtime,prompt,native_session}=request.input;
  mkdirSync(options.outputRoot,{recursive:true,mode:0o700});
  const directory=lstatSync(options.outputRoot);
  if(!directory.isDirectory()||directory.isSymbolicLink()||directory.uid!==process.getuid?.()||(directory.mode&0o077)!==0)throw new JobError('unsafe_output');
  const base={schema_version:1 as const,command_id:options.commandId,command_sha256:request.command.sha256,attempt_id:job.attempt_id};
  const intent=join(options.outputRoot,options.commandId+'.intent.json');
  let fd:number;
  try {fd=openSync(intent,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);}
  catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')return {...base,phase:'unknown',code:'command_already_consumed'};throw error;}
  try {writeFileSync(fd,JSON.stringify(base));fsyncSync(fd);} finally {closeSync(fd);}
  const parent=openSync(options.outputRoot,constants.O_RDONLY);try {fsyncSync(parent);} finally {closeSync(parent);}
  const environment:Record<string,string>={PATH:'/usr/local/bin:/usr/bin:/bin',HOME:'/home/worker',
    CODEX_HOME:'/home/worker/.codex',CLAUDE_CONFIG_DIR:'/home/worker/.claude'};
  const keys=runtime.provider==='codex'?['CODEX_API_KEY','OPENAI_API_KEY']:['ANTHROPIC_API_KEY','CLAUDE_CODE_OAUTH_TOKEN'];
  for(const key of job.inference?[]:keys) {
    if(process.env[key])environment[key]=process.env[key]!;
  }
  if(runtime.provider==='codex'&&!environment.CODEX_API_KEY&&environment.OPENAI_API_KEY)environment.CODEX_API_KEY=environment.OPENAI_API_KEY;
  let worker:Execution|undefined;
  let cancellationRequested=false;
  let readyWritten=false;
  let timer:ReturnType<typeof setInterval>|undefined;
  let publicationFailed=false;
  const cancel=()=>{cancellationRequested=true;worker?.cancel();};
  process.on('SIGTERM',cancel);process.on('SIGINT',cancel);
  const receiptPath=join(options.outputRoot,options.commandId+'.receipt.json');
  try {
    if(cancellationRequested)throw new JobError('worker_shutdown_requested');
    if(!factory) {
      if(typeof request.entry_ticket!=='string'||!/^[a-f0-9]{64}$/.test(request.entry_ticket))throw new JobError('entry_ticket_required');
      // The nonce exists only in this process, never in argv, environment, or job storage.
      // A lost ACK consumes the ticket conservatively; no new process may retry startup.
      const allowed=await consumeEntryTicket('/run/orch-entry/gate.sock',{
        ...request.identity,command_id:request.command.id,command_sha256:request.command.sha256
      },request.entry_ticket,randomBytes(32).toString('hex'));
      if(!allowed)throw new JobError('entry_ticket_denied');
    }
    const create=factory??((approved,env,root,gateway)=>LocalWorker.create(approved,env,root,gateway));
    worker=await create(runtime,environment,join(options.outputRoot,'events'),job.inference?{socketPath:join(options.controlRoot,'inference.sock'),toolServer:'/opt/orch/file_tools.py',python:'/usr/bin/python3',externalSandbox:true}:undefined);
    if(cancellationRequested)throw new JobError('worker_shutdown_requested');
    // Verification may take time; recheck the durable barrier immediately before native startup.
    control(options.controlRoot,options.commandId);
    const context={...job,workspace:options.workspace};
    const publishReady=()=>{
      const view=worker?.inspect();
      if(!readyWritten&&view?.session_id&&id(view.session_id)&&['running','completed'].includes(view.state)) {
        write(join(options.outputRoot,options.commandId+'.ready.json'),{...base,phase:'running',session_id:view.session_id});
        readyWritten=true;
      }
    };
    timer=setInterval(()=>{
      try {publishReady();} catch {publicationFailed=true;worker?.cancel();}
    },10);
    await worker.start(context,prompt,native_session);
    if(publicationFailed)throw new JobError('receipt_storage_failed');
    publishReady();
    const view=worker.inspect();
    const phase=view?.state==='completed'?'completed':view?.state==='cancelled'?'cancelled':'failed';
    const result:JobOutput={...base,phase,...(view?.session_id&&id(view.session_id)?{session_id:view.session_id}:{})};
    write(receiptPath,result);return result;
  } catch(error) {
    worker?.cancel();
    const result:JobOutput={...base,phase:cancellationRequested?'cancelled':'failed',code:error instanceof JobError?error.message:'worker_execution_failed'};
    write(receiptPath,result);return result;
  } finally {
    if(timer)clearInterval(timer);
    process.removeListener('SIGTERM',cancel);process.removeListener('SIGINT',cancel);
    try {await worker?.close();} catch { /* Parent confirms container stop independently. */ }
  }
}

/** Cooperative event-loop watchdog; an independent container watchdog enforces the OS bound. */
export async function executeContainerJob(options:{controlRoot:string;outputRoot:string;workspace:string;commandId:string},factory?:Factory):Promise<never> {
  let request:Control;
  try {request=control(options.controlRoot,options.commandId);} catch {process.exit(1);}
  const timeout=setTimeout(()=>{
    try {
      write(join(options.outputRoot,options.commandId+'.receipt.json'),{schema_version:1,command_id:options.commandId,
        command_sha256:request.command.sha256,attempt_id:request.identity.attempt_id,phase:'failed',code:'worker_watchdog_timeout'});
    } finally {process.exit(124);}
  },request.input.job.timeout_ms+5000);
  timeout.unref();
  try {
    const result=await runContainerJob(options,factory);
    // No asynchronous output is outstanding: receipts and SQLite events are fsynced before returning.
    process.exit(result.phase==='completed'?0:1);
  } catch {process.exit(1);}
}

if(process.argv[1]===fileURLToPath(import.meta.url)) {
  const index=process.argv.indexOf('--command-id');
  const commandId=index>=0?process.argv[index+1]:undefined;
  if(!commandId)process.exit(2);
  await executeContainerJob({controlRoot:'/run/orch-control',outputRoot:'/run/orch-output',workspace:'/workspace',commandId});
}
