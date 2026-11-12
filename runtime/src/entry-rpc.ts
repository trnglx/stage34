/** Deliberately narrow socket: a job can consume a ticket, never issue one or control workers. */
import {createServer,createConnection,type Server} from 'node:net';
import {chmodSync,lstatSync} from 'node:fs';
import {dirname,isAbsolute} from 'node:path';
import type {NativeEntryGate,EntryBinding} from './entry-gate.js';

const MAX=4096;
export async function serveEntryGate(path:string,gate:NativeEntryGate):Promise<Server> {
  if(!isAbsolute(path)||Buffer.byteLength(path)>100)throw new Error('unsafe_entry_socket');
  const parent=lstatSync(dirname(path));
  if(!parent.isDirectory()||parent.isSymbolicLink()||parent.uid!==process.getuid?.()||(parent.mode&0o077)!==0)throw new Error('unsafe_entry_socket');
  const server=createServer(socket=>{
    let bytes=Buffer.alloc(0);let answered=false;
    const timer=setTimeout(()=>socket.destroy(),2000);timer.unref();
    socket.on('close',()=>clearTimeout(timer));socket.on('error',()=>{});
    socket.on('data',chunk=>{
      if(answered)return;
      if(bytes.length+chunk.length>MAX){socket.destroy();return;}
      bytes=Buffer.concat([bytes,chunk]);
      const end=bytes.indexOf(10);if(end<0)return;
      answered=true;let allowed=false;
      try {
        const request=JSON.parse(bytes.subarray(0,end).toString('utf8'));
        if(end===bytes.length-1&&request?.operation==='consume'&&Object.keys(request).sort().join(',')==='binding,nonce,operation,token'&&
          typeof request.token==='string'&&typeof request.nonce==='string')allowed=gate.consume(request.binding,request.token,request.nonce);
      } catch { /* Fail closed without exposing ticket or database diagnostics. */ }
      socket.end(JSON.stringify({allowed})+'\n');
    });
  });
  server.maxConnections=16;
  await new Promise<void>((resolve,reject)=>{
    const failed=(error:Error)=>{server.close();reject(error);};
    server.once('error',failed);
    server.listen({path,backlog:16},()=>{
      try {chmodSync(path,0o600);server.removeListener('error',failed);resolve();}
      catch(error){server.close();reject(error);}
    });
  });
  return server;
}

export async function consumeEntryTicket(path:string,binding:EntryBinding,token:string,nonce:string):Promise<boolean> {
  const frame=JSON.stringify({operation:'consume',binding,token,nonce})+'\n';
  if(Buffer.byteLength(frame)>MAX)throw new Error('invalid_entry_request');
  return new Promise<boolean>((resolve,reject)=>{
    const socket=createConnection(path);let bytes=Buffer.alloc(0);let settled=false;
    const finish=(error?:Error,value=false)=>{
      if(settled)return;settled=true;clearTimeout(timer);socket.destroy();
      if(error)reject(error);else resolve(value);
    };
    const timer=setTimeout(()=>finish(new Error('entry_ack_unknown')),2000);timer.unref();
    socket.on('connect',()=>socket.write(frame));
    socket.on('error',()=>finish(new Error('entry_ack_unknown')));
    socket.on('end',()=>finish(new Error('entry_ack_unknown')));
    socket.on('data',chunk=>{
      if(bytes.length+chunk.length>MAX){finish(new Error('invalid_entry_ack'));return;}
      bytes=Buffer.concat([bytes,chunk]);const end=bytes.indexOf(10);if(end<0)return;
      try {
        const value=JSON.parse(bytes.subarray(0,end).toString('utf8'));
        if(end!==bytes.length-1||Object.keys(value).join(',')!=='allowed'||typeof value.allowed!=='boolean')throw new Error();
        finish(undefined,value.allowed);
      } catch {finish(new Error('invalid_entry_ack'));}
    });
  });
}
