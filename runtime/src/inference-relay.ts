/** Job-local loopback bridge to an attempt-specific Unix socket. Contains no provider credential. */
import {createServer,request,type Server} from 'node:http';
import {lstatSync} from 'node:fs';
import {INFERENCE_MAX} from './inference-queue.js';

export class InferenceRelay {
  private server:Server|undefined;
  private readonly requests=new Set<ReturnType<typeof request>>();
  private blocked:string|undefined;
  baseUrl='';
  get errorCode():string|undefined{return this.blocked;}
  async listen(socketPath:string):Promise<void> {
    const info=lstatSync(socketPath);
    if(!info.isSocket()||info.isSymbolicLink()||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new Error('unsafe_inference_socket');
    const server=createServer(async(req,res)=>{
      if(req.method!=='POST'||req.url!=='/v1/responses'){res.writeHead(404);res.end();return;}
      try {
        const chunks:Buffer[]=[];let length=0;
        for await(const part of req){length+=part.length;if(length>INFERENCE_MAX)throw new Error('too_large');chunks.push(Buffer.from(part));}
        const upstream=request({socketPath,path:'/v1/responses',method:'POST',headers:{'content-type':'application/json'}},answer=>{
          const output:Buffer[]=[];let size=0;
          answer.on('data',part=>{size+=part.length;if(size>INFERENCE_MAX){upstream.destroy();return;}output.push(Buffer.from(part));});
          answer.on('end',()=>{
            const body=Buffer.concat(output);
            if(answer.statusCode!==200){try{this.blocked=JSON.parse(body.toString('utf8')).error?.code;}catch{this.blocked='provider_error';}}
            if(!res.destroyed){res.writeHead(answer.statusCode??502,{'content-type':answer.statusCode===200?'text/event-stream':'application/json'});res.end(body);}
          });
          answer.on('error',()=>{if(!res.destroyed)res.destroy();});
        });
        this.requests.add(upstream);
        upstream.once('close',()=>this.requests.delete(upstream));
        upstream.once('error',()=>{this.blocked='provider_error';if(!res.destroyed){res.writeHead(503);res.end('{"error":{"code":"provider_error"}}');}});
        res.once('close',()=>{if(!res.writableEnded)upstream.destroy();});
        upstream.end(Buffer.concat(chunks));
      }catch{if(!res.destroyed){res.writeHead(400);res.end();}}
    });
    server.maxConnections=4;server.requestTimeout=30_000;
    await new Promise<void>((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',()=>{server.removeListener('error',reject);resolve();});});
    const address=server.address();if(!address||typeof address==='string')throw new Error('invalid_inference_listener');
    this.baseUrl='http://127.0.0.1:'+address.port+'/v1';this.server=server;
  }
  async close():Promise<void> {
    for(const req of this.requests)req.destroy();
    if(this.server){this.server.closeAllConnections();await new Promise<void>(resolve=>this.server!.close(()=>resolve()));this.server=undefined;}
  }
}
