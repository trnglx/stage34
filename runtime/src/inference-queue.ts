/** Private, durable job-to-controller inference relay. Never opens provider network connections. */
import {createServer,type Server,type ServerResponse} from 'node:http';
import {createHash} from 'node:crypto';
import {chmodSync,mkdirSync,lstatSync,existsSync} from 'node:fs';
import {dirname,isAbsolute} from 'node:path';
import {privateDatabase} from './service.js';
import type {DatabaseSync} from 'node:sqlite';

export const INFERENCE_MAX=256*1024;
export interface InferenceCall {id:string;body_b64:string;body_sha256:string;state:'pending'|'staged'|'released'|'cancelled';response_b64?:string;response_sha256?:string;status?:number;}
interface Row {id:string;body:string;state:InferenceCall['state'];response:string|null;response_sha:string|null;status:number|null;decision:string|null;delivery:number;}
const hash=(raw:string|Buffer)=>createHash('sha256').update(raw).digest('hex');
const validId=(value:unknown):value is string=>typeof value==='string'&&/^I-[a-f0-9]{64}$/.test(value);
const validSha=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
function privateRoot(root:string):void {
  if(!isAbsolute(root))throw new Error('unsafe_inference_state');
  mkdirSync(root,{recursive:true,mode:0o700});const info=lstatSync(root);
  if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new Error('unsafe_inference_state');
}

export class InferenceQueue {
  private readonly db:DatabaseSync;
  private server:Server|undefined;
  private closed=false;
  private readonly waiters=new Map<string,Set<()=>void>>();
  constructor(root:string,readonly attempt:string) {
    privateRoot(root);
    if(!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(attempt))throw new Error('invalid_inference_identity');
    this.db=privateDatabase(root,'inference.sqlite3');
    this.db.exec('PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;');
    try {
      this.db.exec('BEGIN IMMEDIATE');
      const version=this.db.prepare('PRAGMA user_version').get()!.user_version;
      if(version!==0&&version!==1)throw new Error('unsupported_inference_schema');
      if(version===0){
        if(this.db.prepare("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").get())throw new Error('invalid_inference_schema');
        this.db.exec(`CREATE TABLE binding(attempt TEXT PRIMARY KEY);
          CREATE TABLE request(id TEXT PRIMARY KEY,body TEXT NOT NULL,state TEXT NOT NULL,
          response TEXT,response_sha TEXT,status INTEGER,decision TEXT,delivery INTEGER NOT NULL DEFAULT 0);PRAGMA user_version=1;`);
        this.db.prepare('INSERT INTO binding VALUES(?)').run(attempt);
      }
      for(const [table,columns] of [['binding','attempt'],['request','id,body,state,response,response_sha,status,decision,delivery']]){
        const actual=this.db.prepare('PRAGMA table_info('+table+')').all().map(r=>r.name).join(',');
        if(actual!==columns)throw new Error('invalid_inference_schema');
      }
      const prior=this.db.prepare('SELECT attempt FROM binding').all();
      if(prior.length!==1||prior[0]?.attempt!==attempt)throw new Error('inference_attempt_mismatch');
      this.db.exec('COMMIT');
    }catch(error){try{this.db.exec('ROLLBACK');}finally{this.db.close();}throw error;}
  }
  private row(id:string):Row {
    if(!validId(id))throw new Error('invalid_inference_identity');
    const row=this.db.prepare('SELECT * FROM request WHERE id=?').get(id) as unknown as Row|undefined;
    if(!row)throw new Error('inference_request_not_found');return row;
  }
  private notify(id:string):void {for(const resolve of this.waiters.get(id)??[])resolve();this.waiters.delete(id);}
  enqueue(body:Buffer):string {
    if(this.closed||body.length<1||body.length>INFERENCE_MAX)throw new Error('inference_request_refused');
    // Stable across HTTP retries: a lost local ACK must not buy another model request.
    const id='I-'+hash(this.attempt+'\0'+body.toString('base64'));
    this.db.prepare("INSERT OR IGNORE INTO request VALUES(?,?,'pending',NULL,NULL,NULL,NULL,0)").run(id,body.toString('base64'));
    return id;
  }
  poll():InferenceCall[] {
    if(this.closed)return [];
    const row=this.db.prepare("SELECT * FROM request WHERE state IN ('pending','staged') ORDER BY rowid LIMIT 1").get() as unknown as Row|undefined;
    if(!row)return [];
    return [{id:row.id,body_b64:row.body,body_sha256:hash(Buffer.from(row.body,'base64')),state:row.state,
      ...(row.response===null?{}:{response_b64:row.response,response_sha256:row.response_sha!,status:row.status!})}];
  }
  stage(id:string,responseB64:string,status:number):{response_sha256:string} {
    if(this.closed||typeof responseB64!=='string'||!/^[A-Za-z0-9+/]*={0,2}$/.test(responseB64)||![200,400,403,409,429,500,502,503].includes(status))throw new Error('invalid_inference_response');
    const body=Buffer.from(responseB64,'base64');
    if(body.length>INFERENCE_MAX||body.toString('base64')!==responseB64)throw new Error('invalid_inference_response');
    const sha=hash(body);const row=this.row(id);
    if(row.state!=='pending') {
      if(row.response_sha!==sha||row.status!==status)throw new Error('inference_response_conflict');
      return {response_sha256:sha};
    }
    this.db.prepare("UPDATE request SET state='staged',response=?,response_sha=?,status=? WHERE id=? AND state='pending'").run(responseB64,sha,status,id);
    return {response_sha256:sha};
  }
  release(id:string,sha:string,code=''):void {
    const row=this.row(id);
    if(this.closed||!validSha(sha)||row.response_sha!==sha||!['staged','released'].includes(row.state)||!/^([a-z][a-z0-9_]{0,79})?$/.test(code)||(row.state==='released'&&row.decision!==code))throw new Error('inference_release_refused');
    this.db.prepare("UPDATE request SET state='released',decision=? WHERE id=? AND state='staged'").run(code,id);this.notify(id);
  }
  private async deliver(id:string,res:ServerResponse):Promise<void> {
    let row=this.row(id);
    if(!['released','cancelled'].includes(row.state)) {
      await new Promise<void>(resolve=>{
        const waiters=this.waiters.get(id)??new Set();waiters.add(resolve);this.waiters.set(id,waiters);
        res.once('close',()=>{waiters.delete(resolve);resolve();});
        // No awaits between the state check and registration, so release cannot be missed.
      });
      if(this.closed)return;
      row=this.row(id);
    }
    if(res.destroyed)return;
    if(row.state!=='released') {res.writeHead(403,{'content-type':'application/json'});res.end('{"error":{"code":"cancelled","message":"Stage34 request cancelled"}}');return;}
    if(row.delivery){res.writeHead(409,{'content-type':'application/json'});res.end('{"error":{"code":"inference_delivery_unknown","message":"Stage34 response already consumed"}}');return;}
    this.db.prepare('UPDATE request SET delivery=1 WHERE id=?').run(id);
    if(row.decision){res.writeHead(403,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:row.decision,message:'Stage34 policy refused the request'}}));return;}
    res.writeHead(row.status!,{'content-type':row.status===200?'text/event-stream':'application/json','cache-control':'no-store'});
    res.end(Buffer.from(row.response!,'base64'));
  }
  async listen(socketPath:string):Promise<void> {
    if(this.server||this.closed||!isAbsolute(socketPath)||Buffer.byteLength(socketPath)>100||existsSync(socketPath))throw new Error('unsafe_inference_socket');
    privateRoot(dirname(socketPath));
    const server=createServer(async(req,res)=>{
      if(req.method!=='POST'||req.url!=='/v1/responses'){res.writeHead(404);res.end();return;}
      try {
        const chunks:Buffer[]=[];let length=0;
        for await(const chunk of req){length+=chunk.length;if(length>INFERENCE_MAX)throw new Error('too_large');chunks.push(Buffer.from(chunk));}
        const id=this.enqueue(Buffer.concat(chunks));await this.deliver(id,res);
      } catch {if(!res.destroyed){res.writeHead(503,{'content-type':'application/json'});res.end('{"error":{"code":"provider_error","message":"Stage34 inference unavailable"}}');}}
    });
    server.maxConnections=4;server.requestTimeout=30_000;server.headersTimeout=10_000;
    await new Promise<void>((resolve,reject)=>{server.once('error',reject);server.listen(socketPath,()=>{chmodSync(socketPath,0o600);server.removeListener('error',reject);resolve();});});
    this.server=server;
  }
  cancel():void {
    if(this.closed)return;
    this.db.prepare("UPDATE request SET state='cancelled' WHERE state IN ('pending','staged')").run();
    for(const id of [...this.waiters.keys()])this.notify(id);
  }
  async close():Promise<void> {
    if(this.closed)return;this.cancel();this.closed=true;
    if(this.server){this.server.closeAllConnections();await new Promise<void>(resolve=>this.server!.close(()=>resolve()));}
    this.db.close();
  }
}
