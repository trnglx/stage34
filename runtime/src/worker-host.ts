/** Worker daemon lifecycle. A trusted native driver is supplied by the host composition root. */
import {mkdirSync,mkdtempSync,lstatSync,openSync,closeSync,writeFileSync,readFileSync,fsyncSync,renameSync,unlinkSync,constants} from 'node:fs';
import {join,isAbsolute} from 'node:path';
import {randomUUID} from 'node:crypto';
import type {Server} from 'node:net';
import {WorkerService,type ManagedDriver} from './service.js';
import {NativeEntryGate} from './entry-gate.js';
import {serveEntryGate} from './entry-rpc.js';
import {serveWorkerSocket,type WorkerSocketServer} from './worker-server.js';

export interface WorkerHostConfiguration {state_root:string;node_id:string;slots:number;}
export interface WorkerHostResources {
  entry_gate:NativeEntryGate;entry_database:string;entry_directory:string;boot_directory:string;node_id:string;
}
export type DriverFactory=(resources:WorkerHostResources)=>Promise<ManagedDriver>|ManagedDriver;
export interface WorkerEndpoint {schema_version:1;node_id:string;boot_id:string;worker_socket:string;}

function directory(path:string):void {
  mkdirSync(path,{recursive:true,mode:0o700});const info=lstatSync(path);
  if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new Error('unsafe_worker_host_state');
}
function publish(root:string,endpoint:WorkerEndpoint):void {
  const temp=join(root,'.endpoint-'+randomUUID());
  const fd=openSync(temp,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);
  try {writeFileSync(fd,JSON.stringify(endpoint));fsyncSync(fd);} finally {closeSync(fd);}
  renameSync(temp,join(root,'endpoint.json'));
  const parent=openSync(root,constants.O_RDONLY);try {fsyncSync(parent);} finally {closeSync(parent);}
}
const closeServer=(server:Server)=>new Promise<void>(resolve=>server.close(()=>resolve()));

/** Called only while this process owns the journal. A boot-specific removal cannot unpublish another boot. */
function unpublish(root:string,expectedBoot?:string):void {
  const path=join(root,'endpoint.json');
  try {
    if(expectedBoot&&JSON.parse(readFileSync(path,'utf8')).boot_id!==expectedBoot)return;
    unlinkSync(path);
  } catch(error) {if((error as NodeJS.ErrnoException).code==='ENOENT')return;throw error;}
  const fd=openSync(root,constants.O_RDONLY);try {fsyncSync(fd);} finally {closeSync(fd);}
}

export class WorkerHost {
  private shutdown:Promise<void>|undefined;
  private constructor(readonly endpoint:Readonly<WorkerEndpoint>,private readonly root:string,
    private readonly worker:WorkerService,private readonly gate:NativeEntryGate,
    private readonly management:WorkerSocketServer,private readonly entry:Server,private readonly driver:ManagedDriver) {}

  static async start(config:WorkerHostConfiguration,factory:DriverFactory):Promise<WorkerHost> {
    if(!isAbsolute(config.state_root)||typeof factory!=='function')throw new Error('invalid_worker_host_config');
    directory(config.state_root);directory(join(config.state_root,'journal'));
    let driver:ManagedDriver|undefined;
    const ready=()=>{if(!driver)throw new Error('worker_not_ready');return driver;};
    const deferred:ManagedDriver={execute:(...args)=>ready().execute(...args),events:(...args)=>ready().events(...args),
      fence:(...args)=>ready().fence(...args),inspect:(...args)=>ready().inspect(...args)};
    // Acquire ownership before opening shared entry state, invoking a driver or publishing sockets.
    const worker=new WorkerService(join(config.state_root,'journal'),config.node_id,config.slots,deferred);
    let gate:NativeEntryGate|undefined;let entry:Server|undefined;let management:WorkerSocketServer|undefined;let bootId:string|undefined;
    try {
      // The previous owner is gone; never leave its stale readiness descriptor through a failed boot.
      unpublish(config.state_root);
      gate=new NativeEntryGate(join(config.state_root,'entry.sqlite3'));
      directory(join(config.state_root,'boots'));
      const boot=mkdtempSync(join(config.state_root,'boots','B-'));
      bootId=boot.slice(boot.lastIndexOf('/')+1);
      const entryDirectory=join(boot,'entry');const managementDirectory=join(boot,'management');
      directory(entryDirectory);directory(managementDirectory);
      entry=await serveEntryGate(join(entryDirectory,'gate.sock'),gate);
      driver=await factory({entry_gate:gate,entry_database:join(config.state_root,'entry.sqlite3'),entry_directory:entryDirectory,boot_directory:boot,node_id:config.node_id});
      if(!driver||!['execute','events','fence','inspect'].every(method=>typeof driver![method as keyof ManagedDriver]==='function'))throw new Error('invalid_native_driver');
      const endpoint:WorkerEndpoint={schema_version:1,node_id:config.node_id,boot_id:bootId,worker_socket:join(managementDirectory,'worker.sock')};
      management=await serveWorkerSocket(endpoint.worker_socket,worker);
      publish(config.state_root,endpoint);
      return new WorkerHost(Object.freeze(endpoint),config.state_root,worker,gate,management,entry,driver);
    } catch(error) {
      if(management)await management.drain();
      if(entry)await closeServer(entry);
      try {try {await driver?.close?.();} finally {if(bootId)unpublish(config.state_root,bootId);}}
      finally {try {gate?.close();} finally {worker.close();}}
      throw error;
    }
  }

  /** Does not claim native containers stopped: the durable journal quarantines them on restart. */
  close():Promise<void> {
    if(this.shutdown)return this.shutdown;
    this.shutdown=(async()=>{
      await this.management.drain();
      await closeServer(this.entry);
      try {await this.driver.close?.();}
      finally {try {unpublish(this.root,this.endpoint.boot_id);}
        finally {try {this.gate.close();} finally {this.worker.close();}}}
    })();
    return this.shutdown;
  }
}
