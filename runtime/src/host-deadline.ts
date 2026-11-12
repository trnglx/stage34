/** Runs in a dedicated host process, outside the job's UID/PID namespace. */
import {readFileSync,lstatSync} from 'node:fs';
import {isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {ContainerSupervisor,type ContainerProfile,type ContainerIdentity,type ContainerCommand} from './container.js';
import {NativeEntryGate,type EntryBinding} from './entry-gate.js';

export interface DeadlineContract {
  identity:ContainerIdentity;command:ContainerCommand;
  deadline_at_ms:number;max_runtime_ms:number;
}
interface Boundary {
  fence(identity:ContainerIdentity,command:ContainerCommand):void;
  stopPhysical(identity:ContainerIdentity):Promise<{stopped:true}>;
}
interface Gate {fence(binding:EntryBinding):void;}
interface Clock {wall():number;monotonic():number;wait(ms:number):Promise<void>;}
const clock:Clock={wall:()=>Date.now(),monotonic:()=>performance.now(),wait:ms=>new Promise(resolve=>setTimeout(resolve,ms))};

export function validateDeadlineContract(contract:DeadlineContract):void {
  const id=(value:unknown)=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(value);
  const sha=(value:unknown)=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
  if(!contract||!contract.identity||!contract.command||
    ![contract.identity.node_id,contract.identity.attempt_id,contract.command.id].every(id)||
    ![contract.identity.policy_sha256,contract.command.sha256].every(sha)||
    !Number.isSafeInteger(contract.identity.generation)||contract.identity.generation<1||
    !Number.isSafeInteger(contract.deadline_at_ms)||contract.deadline_at_ms<1||
    !Number.isSafeInteger(contract.max_runtime_ms)||contract.max_runtime_ms<1||contract.max_runtime_ms>3_600_000)throw new Error('invalid_host_deadline');
}

/** A missing Docker acknowledgement never releases capacity or reports a confirmed stop. */
export async function guardAttempt(contract:DeadlineContract,boundary:Boundary,gate:Gate,time:Clock=clock):Promise<{stopped:true}> {
  validateDeadlineContract(contract);
  // The wall deadline survives restart; monotonic elapsed time prevents clock rollback extending this guardian.
  const remaining=Math.max(0,Math.min(contract.max_runtime_ms,contract.deadline_at_ms-time.wall()));
  const monotonicEnd=time.monotonic()+remaining;
  while(time.wall()<contract.deadline_at_ms&&time.monotonic()<monotonicEnd) {
    await time.wait(Math.max(1,Math.min(250,contract.deadline_at_ms-time.wall(),monotonicEnd-time.monotonic())));
  }
  const binding={...contract.identity,command_id:contract.command.id,command_sha256:contract.command.sha256};
  for(;;) {
    let entryFenced=false;let containerFenced=false;
    try {gate.fence(binding);entryFenced=true;} catch { /* Attempt the other barrier independently. */ }
    try {boundary.fence(contract.identity,contract.command);containerFenced=true;} catch { /* Still attempt physical stop. */ }
    try {
      const result=await boundary.stopPhysical(contract.identity);
      if(entryFenced&&containerFenced&&result.stopped)return {stopped:true};
    } catch { /* Unknown external effect: retain the guard and retry only the fenced stop. */ }
    await time.wait(1000);
  }
}

interface GuardianConfiguration extends DeadlineContract {container:ContainerProfile;entry_database:string;}
function configuration(path:string):GuardianConfiguration {
  if(!isAbsolute(path))throw new Error('unsafe_guardian_configuration');
  const info=lstatSync(path);
  if(!info.isFile()||info.isSymbolicLink()||info.nlink!==1||info.uid!==process.getuid?.()||(info.mode&0o077)!==0||info.size>16384)throw new Error('unsafe_guardian_configuration');
  return JSON.parse(readFileSync(path,'utf8')) as GuardianConfiguration;
}

if(process.argv[1]===fileURLToPath(import.meta.url)) {
  try {
    const path=process.argv[2];if(!path)throw new Error();
    const request=configuration(path);
    validateDeadlineContract(request);
    const supervisor=new ContainerSupervisor(request.container);
    let gate:NativeEntryGate|undefined;
    // Persistence may be unavailable at startup; physical stopping must still proceed.
    const fence={fence(binding:EntryBinding){gate??=new NativeEntryGate(request.entry_database);gate.fence(binding);}};
    try {
      const guarding=guardAttempt(request,supervisor,fence);
      // A detached guardian must keep protecting the job if its parent's ACK pipe disappears.
      process.stdout.on('error',()=>{});
      process.stdout.write(JSON.stringify({schema_version:1,status:'ready',attempt_id:request.identity.attempt_id,
        generation:request.identity.generation,deadline_at_ms:request.deadline_at_ms})+'\n');
      await guarding;
    } finally {gate?.close();}
    process.exitCode=0;
  } catch {process.stderr.write('host_guardian_failed\n');process.exitCode=1;}
}
