/** Explicit private-config entrypoint. Readiness means RPC serving, not SDK/image acceptance. */
import {constants,openSync,fstatSync,closeSync,lstatSync,readFileSync} from 'node:fs';
import {dirname,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {WorkerHost,type WorkerHostConfiguration} from './worker-host.js';
import {NativeContainerDriver,type NativeDriverProfile} from './native-driver.js';

interface Configuration {schema_version:1;host:WorkerHostConfiguration;native:NativeDriverProfile;}
function configuration(path:string):Configuration {
  if(!isAbsolute(path))throw new Error('invalid_daemon_configuration');
  const parent=lstatSync(dirname(path));
  if(!parent.isDirectory()||parent.isSymbolicLink()||parent.uid!==process.getuid?.()||(parent.mode&0o077)!==0)throw new Error('unsafe_daemon_configuration');
  const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try {
    const info=fstatSync(fd);
    if(!info.isFile()||info.uid!==process.getuid?.()||info.nlink!==1||(info.mode&0o077)!==0||info.size>65536)throw new Error('unsafe_daemon_configuration');
    const value=JSON.parse(readFileSync(fd,'utf8')) as Configuration;
    if(!value||value.schema_version!==1||Object.keys(value).sort().join(',')!=='host,native,schema_version'||!value.host||!value.native)throw new Error('invalid_daemon_configuration');
    return value;
  } finally {closeSync(fd);}
}

async function daemon(path:string):Promise<void> {
  const config=configuration(path);let host:WorkerHost|undefined;let stopping=false;
  let resolveStop!:()=>void;let rejectStop!:(error:unknown)=>void;
  const stopped=new Promise<void>((resolve,reject)=>{resolveStop=resolve;rejectStop=reject;});
  const stop=()=>{
    if(stopping)return;stopping=true;
    if(host)void host.close().then(resolveStop,rejectStop);
  };
  process.on('SIGTERM',stop);process.on('SIGINT',stop);
  try {
    host=await WorkerHost.start(config.host,resources=>new NativeContainerDriver(config.native,resources));
    if(stopping){await host.close();return;}
    // An OS service keeps serving when its invoking shell/readiness reader goes away.
    process.stdout.on('error',()=>{});process.stdout.write(JSON.stringify(host.endpoint)+'\n');
    await stopped;
  } finally {
    process.removeListener('SIGTERM',stop);process.removeListener('SIGINT',stop);
    if(host)await host.close();
  }
}

if(process.argv[1]===fileURLToPath(import.meta.url)) {
  if(process.argv.length!==4||process.argv[2]!=='--config') {
    process.stderr.write('usage: worker-daemon --config /absolute/private.json\n');process.exitCode=2;
  } else {
    try {await daemon(process.argv[3]!);} catch {process.stderr.write('worker_daemon_failed\n');process.exitCode=1;}
  }
}
