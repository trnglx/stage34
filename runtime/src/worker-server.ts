/** Private SSH-shim endpoint. Never mount this socket or its directory inside a job. */
import {createServer,type Server} from 'node:net';
import {chmodSync,lstatSync} from 'node:fs';
import {dirname,isAbsolute} from 'node:path';
import type {WorkerRequest,WorkerResponse} from './service.js';

interface Handler {handle(request:WorkerRequest):Promise<WorkerResponse>;}
export interface WorkerSocketServer extends Server {
  /** Stop admission and wait for sockets and effects before releasing journal ownership. */
  drain():Promise<void>;
}
const MAX=1024*1024;

/** Bind a fresh per-boot endpoint only after acquiring the worker journal's ownership lock. */
export async function serveWorkerSocket(path:string,worker:Handler):Promise<WorkerSocketServer> {
  if(!isAbsolute(path)||Buffer.byteLength(path)>100)throw new Error('unsafe_worker_socket');
  const parent=lstatSync(dirname(path));
  if(!parent.isDirectory()||parent.isSymbolicLink()||parent.uid!==process.getuid?.()||(parent.mode&0o077)!==0)throw new Error('unsafe_worker_socket');
  let pending=0;let draining=false;let drained:Promise<void>|undefined;
  const idle=new Set<()=>void>();
  const server=createServer(socket=>{
    if(draining){socket.destroy();return;}
    let bytes=Buffer.alloc(0);let expected:number|undefined;let dispatched=false;
    const timer=setTimeout(()=>socket.destroy(),25_000);timer.unref();
    socket.on('close',()=>clearTimeout(timer));socket.on('error',()=>{});
    socket.on('data',chunk=>{
      if(dispatched){socket.destroy();return;}
      if(bytes.length+chunk.length>MAX+4){socket.destroy();return;}
      bytes=Buffer.concat([bytes,chunk]);
      if(expected===undefined&&bytes.length>=4){
        expected=bytes.readUInt32BE(0);
        if(expected<1||expected>MAX){socket.destroy();return;}
      }
      if(expected===undefined||bytes.length<expected+4)return;
      if(bytes.length!==expected+4){socket.destroy();return;}
      let request:WorkerRequest;
      try {request=JSON.parse(bytes.subarray(4).toString('utf8')) as WorkerRequest;}
      catch {socket.destroy();return;}
      if(draining||pending>=16){socket.destroy();return;}
      dispatched=true;bytes=Buffer.alloc(0);pending++;
      void Promise.resolve().then(()=>worker.handle(request)).then(response=>{
        if(socket.destroyed)return;
        const payload=Buffer.from(JSON.stringify(response));
        if(payload.length>MAX){socket.destroy();return;}
        const header=Buffer.alloc(4);header.writeUInt32BE(payload.length);
        socket.end(Buffer.concat([header,payload]));
      }).catch(()=>{
        // A handler can fail after an effect: no definitive refusal may be manufactured here.
        socket.destroy();
      }).finally(()=>{
        pending--;
        if(pending===0){for(const notify of idle)notify();idle.clear();}
      });
    });
  }) as WorkerSocketServer;
  server.drain=()=>{
    if(drained)return drained;
    draining=true;
    const closed=new Promise<void>(resolve=>server.close(()=>resolve()));
    const finished=pending===0?Promise.resolve():new Promise<void>(resolve=>idle.add(resolve));
    drained=Promise.all([closed,finished]).then(()=>{});
    return drained;
  };
  server.maxConnections=16;
  await new Promise<void>((resolve,reject)=>{
    const failed=(error:Error)=>{server.close();reject(error);};server.once('error',failed);
    server.listen({path,backlog:16},()=>{
      try {chmodSync(path,0o600);server.removeListener('error',failed);resolve();}
      catch(error){server.close();reject(error);}
    });
  });
  return server;
}
