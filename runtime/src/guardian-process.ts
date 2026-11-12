/** Launch a detached host guardian before a native job. Unknown readiness never authorizes execution. */
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdirSync,lstatSync,openSync,closeSync,writeFileSync,fsyncSync,constants} from 'node:fs';
import {join,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {validateDeadlineContract,type DeadlineContract} from './host-deadline.js';
import type {ContainerProfile} from './container.js';

export interface GuardianHandle {
  readonly pid:number;readonly deadline_at_ms:number;
  alive():boolean;
  readonly exited:Promise<{code:number|null;signal:NodeJS.Signals|null}>;
}

function directory(path:string):void {
  mkdirSync(path,{recursive:true,mode:0o700});const info=lstatSync(path);
  if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new Error('unsafe_guardian_state');
}

export async function launchGuardian(root:string,contract:DeadlineContract,container:ContainerProfile,entryDatabase:string):Promise<GuardianHandle> {
  validateDeadlineContract(contract);
  if(!isAbsolute(root)||!isAbsolute(entryDatabase))throw new Error('invalid_guardian_configuration');
  if(contract.deadline_at_ms<=Date.now())throw new Error('guardian_deadline_expired');
  directory(root);
  const key=createHash('sha256').update(JSON.stringify([contract.identity.node_id,contract.identity.attempt_id,contract.identity.generation])).digest('hex');
  const path=join(root,key+'.json');
  let fd:number;
  try {fd=openSync(path,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);}
  catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')throw new Error('guardian_already_claimed');throw error;}
  try {writeFileSync(fd,JSON.stringify({...contract,container,entry_database:entryDatabase}));fsyncSync(fd);} finally {closeSync(fd);}
  const parent=openSync(root,constants.O_RDONLY);try {fsyncSync(parent);} finally {closeSync(parent);}
  const child=spawn(process.execPath,[fileURLToPath(new URL('./host-deadline.js',import.meta.url)),path],{
    detached:true,stdio:['ignore','pipe','pipe'],windowsHide:true,env:{PATH:'/usr/bin:/bin',HOME:root,LANG:'C'}
  });
  const exited=new Promise<{code:number|null;signal:NodeJS.Signals|null}>(resolve=>child.once('close',(code,signal)=>resolve({code,signal})));
  try {
    await new Promise<void>((resolve,reject)=>{
      let output=Buffer.alloc(0);let errors=0;let settled=false;
      const finish=(error?:Error)=>{
        if(settled)return;settled=true;clearTimeout(timer);
        child.stdout.removeAllListeners('data');child.stderr.removeAllListeners('data');
        if(error)reject(error);else resolve();
      };
      const timer=setTimeout(()=>finish(new Error('guardian_readiness_unknown')),Math.min(3000,contract.deadline_at_ms-Date.now()));
      child.once('error',()=>finish(new Error('guardian_readiness_unknown')));
      child.once('exit',()=>finish(new Error('guardian_readiness_unknown')));
      child.stdout.on('data',chunk=>{
        if(output.length+chunk.length>4096){finish(new Error('invalid_guardian_receipt'));return;}
        output=Buffer.concat([output,chunk]);const end=output.indexOf(10);if(end<0)return;
        try {
          const value=JSON.parse(output.subarray(0,end).toString('utf8'));
          if(end!==output.length-1||value.schema_version!==1||value.status!=='ready'||
            value.attempt_id!==contract.identity.attempt_id||value.generation!==contract.identity.generation||
            value.deadline_at_ms!==contract.deadline_at_ms||contract.deadline_at_ms<=Date.now())throw new Error();
          finish();
        } catch {finish(new Error('invalid_guardian_receipt'));}
      });
      child.stderr.on('data',chunk=>{errors+=chunk.length;if(errors>4096)finish(new Error('invalid_guardian_receipt'));});
    });
    if(!child.pid||child.exitCode!==null||child.signalCode!==null)throw new Error('guardian_readiness_unknown');
    return Object.freeze({pid:child.pid,deadline_at_ms:contract.deadline_at_ms,
      alive:()=>child.exitCode===null&&child.signalCode===null,exited});
  } finally {
    // Never kill a possibly protecting guardian on a lost readiness ACK.
    child.stdout.destroy();child.stderr.destroy();child.unref();
  }
}
