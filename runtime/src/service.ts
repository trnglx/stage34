/** Worker-side journal. It records execution effects, never authoritative task acceptance. */
import {DatabaseSync} from 'node:sqlite';
import {createHash,randomUUID} from 'node:crypto';
import {mkdirSync,lstatSync,openSync,closeSync,constants} from 'node:fs';
import {join,isAbsolute} from 'node:path';
import type {AttemptContext} from './runtime.js';
import type {Provider} from './events.js';

export interface WorkerJob extends AttemptContext {
  provider: Provider;
  base_sha: string;
  prompt: string;
  inference?: {schema_version:1};
  checkpoint?: {id:string;head_sha:string;candidate:string;handoff_ref:string;dirty_digest:string;body_sha:string};
}
export interface EffectReceipt {
  state: 'applied' | 'unknown';
  session_id?: string;
  stopped?: boolean;
  command_id?: string;
  command_sha256?: string;
}
export interface EventEnvelope {event_id:string;sequence:number;payload_json:string;sha256:string;}
export interface ManagedDriver {
  readonly checkpoint_restore?: boolean;
  readonly codex_constraints?: boolean;
  inference?(job:WorkerJob,action:string,args:Record<string,unknown>):Promise<Record<string,unknown>>;
  /** Called after RPC drain, before entry/journal ownership is released. Must await background work. */
  close?():Promise<void>|void;
  execute(operation:string,job:WorkerJob,arguments_:Record<string,unknown>,signal:AbortSignal):Promise<EffectReceipt>;
  events(job:WorkerJob,acknowledgements:readonly {event_id:string;sha256:string}[],limit:number):Promise<EventEnvelope[]>;
  /** Persist a launch barrier that survives this service and is checked by native starts. */
  fence(job:WorkerJob,command:{id:string;sha256:string}):Promise<void>;
  inspect(job:WorkerJob,command?:{id:string;operation:string;sha256:string;session_id?:string}):Promise<EffectReceipt>;
}
export interface WorkerRequest {
  schema_version:1;
  request_id:string;
  operation:string;
  arguments:Record<string,unknown>;
}
export interface WorkerResponse {
  schema_version:1;request_id:string;ok:boolean;code:string;data:unknown;
}
class WorkerError extends Error {}
const identity=(value:unknown):value is string=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(value);
const response=(id:string,ok:boolean,code:string,data:unknown=null):WorkerResponse=>({schema_version:1,request_id:id,ok,code,data});

function canonical(value:unknown):string {
  if (Array.isArray(value)) return '['+value.map(canonical).join(',')+']';
  if (value!==null&&typeof value==='object') {
    return '{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical((value as Record<string,unknown>)[key])).join(',')+'}';
  }
  const result=JSON.stringify(value);
  if (result===undefined || (typeof value==='number'&&!Number.isFinite(value))) throw new WorkerError('invalid_request');
  return result;
}

function validateJob(value:unknown,node:string):WorkerJob {
  if (!value||typeof value!=='object'||Array.isArray(value)) throw new WorkerError('invalid_job');
  const job=value as WorkerJob;
  if (![job.task_id,job.attempt_id,job.node_id,job.model].every(identity)||job.node_id!==node||
      !['claude','codex'].includes(job.provider)||!Number.isSafeInteger(job.generation)||job.generation<1||
      !/^[a-f0-9]{40}$/.test(job.base_sha)||!/^[a-f0-9]{64}$/.test(job.policy_sha256)||
      typeof job.prompt!=='string'||Buffer.byteLength(job.prompt)>256*1024||
      typeof job.workspace!=='string'||!job.workspace.startsWith('/')||job.workspace.includes('\0')||
      !['read-only','workspace-write'].includes(job.sandbox)||
      !Number.isSafeInteger(job.timeout_ms)||job.timeout_ms<1||job.timeout_ms>3_600_000) throw new WorkerError('invalid_job');
  return JSON.parse(canonical(job)) as WorkerJob;
}

export function privateDatabase(root:string,name:string):DatabaseSync {
  const path=join(root,name);
  try {
    // Closing an unrelated FD for an existing POSIX-lock file would drop this process's locks.
    const fd=openSync(path,constants.O_CREAT|constants.O_EXCL|constants.O_RDWR|constants.O_NOFOLLOW|constants.O_NONBLOCK,0o600);
    closeSync(fd);
  } catch(error) {
    if ((error as NodeJS.ErrnoException).code!=='EEXIST') throw error;
  }
  const info=lstatSync(path);
  if (!info.isFile()||info.nlink!==1||info.uid!==process.getuid?.()||(info.mode&0o077)!==0) throw new WorkerError('unsafe_worker_state');
  return new DatabaseSync(path);
}

type CommandRow={id:string;attempt:string;sha256:string;operation:string;sequence:number;response:string|null;state:string};

export class WorkerService {
  private readonly db:DatabaseSync;
  private readonly owner:DatabaseSync;
  private readonly active=new Map<string,Map<string,AbortController>>();
  private closed=false;

  constructor(root:string,readonly node_id:string,readonly slots:number,private readonly driver:ManagedDriver) {
    if (!isAbsolute(root)||!identity(node_id)||!Number.isSafeInteger(slots)||slots<1||slots>8) throw new WorkerError('invalid_worker_config');
    mkdirSync(root,{recursive:true,mode:0o700});
    const parent=lstatSync(root);
    if (!parent.isDirectory()||parent.isSymbolicLink()||parent.uid!==process.getuid?.()||(parent.mode&0o077)!==0) throw new WorkerError('unsafe_worker_state');
    this.owner=privateDatabase(root,'owner.sqlite3');
    try {
      // A separate OS-backed SQLite lock survives across awaits and is released on process death.
      this.owner.exec('PRAGMA busy_timeout=0;PRAGMA journal_mode=DELETE;BEGIN EXCLUSIVE;');
    } catch {
      this.owner.close();
      throw new WorkerError('worker_already_running');
    }
    let database:DatabaseSync|undefined;
    try {
      database=privateDatabase(root,'worker.sqlite3');
      database.exec('PRAGMA busy_timeout=5000;PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;');
      const version=database.prepare('PRAGMA user_version').get()!.user_version;
      if (version!==0&&version!==1) throw new WorkerError('unsupported_worker_schema');
      database.exec(`BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS job(attempt TEXT PRIMARY KEY,manifest TEXT NOT NULL,state TEXT NOT NULL,session TEXT);
        CREATE TABLE IF NOT EXISTS command(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,
        attempt TEXT NOT NULL,sha256 TEXT NOT NULL,operation TEXT NOT NULL,state TEXT NOT NULL,response TEXT);
        CREATE TABLE IF NOT EXISTS setting(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        PRAGMA user_version=1;`);
      // Reject incompatible prerelease journals rather than guessing a migration.
      if (!database.prepare('PRAGMA table_info(command)').all().some(row=>row.name==='sequence')) throw new WorkerError('unsupported_worker_schema');
      const enrolled=database.prepare("SELECT value FROM setting WHERE key='node_id'").get();
      if (enrolled&&enrolled.value!==node_id) throw new WorkerError('worker_identity_conflict');
      if (!enrolled) {
        if ((database.prepare('SELECT count(*) AS n FROM job').get()!.n as number)>0) throw new WorkerError('worker_identity_unbound');
        database.prepare("INSERT INTO setting VALUES('node_id',?)").run(node_id);
      }
      if(!database.prepare("SELECT 1 FROM setting WHERE key='journal_id'").get())
        database.prepare("INSERT INTO setting VALUES('journal_id',?)").run(randomUUID());
      database.exec("UPDATE command SET state='unknown' WHERE state='executing';"+
        "UPDATE job SET state='unknown' WHERE state IN ('starting','running');COMMIT;");
      this.db=database;
    } catch(error) {
      database?.close();
      this.owner.close();
      throw error;
    }
  }

  private job(attempt:unknown):WorkerJob {
    if (!identity(attempt)) throw new WorkerError('invalid_attempt');
    const row=this.db.prepare('SELECT manifest FROM job WHERE attempt=?').get(attempt);
    if (!row) throw new WorkerError('attempt_not_found');
    return JSON.parse(row.manifest as string) as WorkerJob;
  }

  private prior(id:string,hash:string):WorkerResponse|undefined {
    const row=this.db.prepare('SELECT * FROM command WHERE id=?').get(id);
    if (!row) return;
    if (row.sha256!==hash) throw new WorkerError('idempotency_conflict');
    return row.response!==null?JSON.parse(row.response as string) as WorkerResponse:
      response(id,true,'effect_unknown',{state:'unknown',attempt_id:row.attempt});
  }

  private finish(id:string,job:WorkerJob,operation:string,receipt:EffectReceipt):WorkerResponse {
    const current=this.db.prepare('SELECT state,session FROM job WHERE attempt=?').get(job.attempt_id)!;
    // Project trusted driver receipts too: private payloads must never escape through extra fields.
    let effect:EffectReceipt={state:receipt?.state==='applied'?'applied':'unknown'};
    if (identity(receipt?.session_id)) effect.session_id=receipt.session_id;
    if (typeof receipt?.stopped==='boolean') effect.stopped=receipt.stopped;
    if (receipt?.session_id!==undefined&&!identity(receipt.session_id)) effect={state:'unknown'};
    if (effect.state==='applied'&&['pause','cancel'].includes(operation)&&effect.stopped!==true) effect={state:'unknown'};
    if (effect.state==='applied'&&operation==='start'&&!effect.session_id) effect={state:'unknown'};
    if (effect.session_id&&current.session&&effect.session_id!==current.session) effect={state:'unknown'};
    const cancelled=!!this.db.prepare('SELECT 1 FROM setting WHERE key=?').get('cancel:'+job.attempt_id);
    if (cancelled&&operation!=='cancel') effect={state:'unknown'};
    const next=effect.state==='unknown'?'unknown':operation==='cancel'?'cancelled':operation==='pause'?'paused':'running';
    const answer=response(id,true,effect.state==='unknown'?'effect_unknown':'ok',{attempt_id:job.attempt_id,...effect});
    this.db.exec('BEGIN IMMEDIATE');
    try {
      if (current.state!=='cancelled') this.db.prepare('UPDATE job SET state=?,session=coalesce(?,session) WHERE attempt=?')
        .run(next,effect.session_id??null,job.attempt_id);
      this.db.prepare('UPDATE command SET state=?,response=? WHERE id=?').run(effect.state,canonical(answer),id);
      this.db.exec('COMMIT');
    } catch(error) {this.db.exec('ROLLBACK');throw error;}
    return answer;
  }

  private async reconnect(id:string,args:Record<string,unknown>):Promise<WorkerResponse> {
    const job=this.job(args.attempt_id);
    if (args.command_id===undefined) {
      const effect=await this.driver.inspect(job);
      return response(id,true,'ok',{attempt_id:job.attempt_id,state:effect.state==='applied'?'applied':'unknown'});
    }
    if (!identity(args.command_id)) throw new WorkerError('invalid_command');
    const command=this.db.prepare('SELECT * FROM command WHERE id=? AND attempt=?').get(args.command_id,job.attempt_id) as CommandRow|undefined;
    if (!command) throw new WorkerError('command_not_found');
    const latest=this.db.prepare('SELECT id FROM command WHERE attempt=? ORDER BY sequence DESC LIMIT 1').get(job.attempt_id)!;
    if (latest.id!==command.id) throw new WorkerError('stale_command');
    if (this.active.get(job.attempt_id)?.size) return response(id,true,'effect_unknown',{state:'unknown',attempt_id:job.attempt_id});
    const current=this.db.prepare('SELECT session FROM job WHERE attempt=?').get(job.attempt_id)!;
    if(current.session!==null&&!identity(current.session))throw new WorkerError('invalid_native_session');
    const effect=await this.driver.inspect(job,{id:command.id,operation:command.operation,sha256:command.sha256,
      ...(current.session===null?{}:{session_id:current.session as string})});
    if (effect.command_id!==command.id||effect.command_sha256!==command.sha256) return response(id,true,'effect_unknown',{state:'unknown',attempt_id:job.attempt_id});
    // Recheck after the asynchronous inspection: another command may have started meanwhile.
    if (this.active.get(job.attempt_id)?.size||this.db.prepare('SELECT id FROM command WHERE attempt=? ORDER BY sequence DESC LIMIT 1').get(job.attempt_id)!.id!==command.id) {
      return response(id,true,'effect_unknown',{state:'unknown',attempt_id:job.attempt_id});
    }
    const recovered=this.finish(command.id,job,command.operation,effect);
    return response(id,true,recovered.code,{...recovered.data as object,command_id:command.id});
  }

  async handle(request:WorkerRequest):Promise<WorkerResponse> {
    const id=identity(request?.request_id)?request.request_id:'invalid';
    let effectStarted=false;
    let attemptId:string|undefined;
    try {
      if (this.closed) throw new WorkerError('worker_closed');
      if (!request||request.schema_version!==1||!identity(request.request_id)||!request.arguments||
          typeof request.arguments!=='object'||Array.isArray(request.arguments)||Buffer.byteLength(canonical(request))>1024*1024) throw new WorkerError('invalid_request');
      const args=request.arguments;
      if(args.expected_journal_id!==undefined&&args.expected_journal_id!==this.db.prepare("SELECT value FROM setting WHERE key='journal_id'").get()!.value)
        throw new WorkerError('worker_identity_conflict');
      if (request.operation==='health'||request.operation==='register_node') {
        const occupied=this.db.prepare("SELECT count(*) AS n FROM job WHERE state NOT IN ('cancelled','completed','failed')").get()!.n;
        return response(id,true,'ok',{node_id:this.node_id,journal_id:this.db.prepare("SELECT value FROM setting WHERE key='journal_id'").get()!.value,
          slots:this.slots,occupied,checkpoint_restore:this.driver.checkpoint_restore===true,codex_constraints:this.driver.codex_constraints===true,revoked:!!this.db.prepare("SELECT 1 FROM setting WHERE key='revoked'").get()});
      }
      if(request.operation==='inference') {
        const job=this.job(args.attempt_id);
        if(!this.driver.inference||!job.inference||this.db.prepare("SELECT 1 FROM setting WHERE key='revoked'").get()||this.db.prepare('SELECT 1 FROM setting WHERE key=?').get('cancel:'+job.attempt_id))throw new WorkerError('inference_unavailable');
        if(!['poll','stage','release'].includes(String(args.action)))throw new WorkerError('invalid_request');
        return response(id,true,'ok',await this.driver.inference(job,String(args.action),args));
      }
      if (request.operation==='reconnect') return await this.reconnect(id,args);
      if (request.operation==='subscribe_events') {
        const job=this.job(args.attempt_id);
        const limit=args.limit??100;
        const acknowledgements=args.acknowledgements??[];
        if (!Number.isSafeInteger(limit)||Number(limit)<1||Number(limit)>1000||!Array.isArray(acknowledgements)||acknowledgements.length>1000||
            acknowledgements.some(ack=>!ack||!identity(ack.event_id)||typeof ack.sha256!=='string'||!/^[a-f0-9]{64}$/.test(ack.sha256))) throw new WorkerError('invalid_event_ack');
        return response(id,true,'ok',{events:await this.driver.events(job,acknowledgements,Number(limit))});
      }
      if (!['dispatch','cancel','revoke'].includes(request.operation)) throw new WorkerError('capability_unavailable');
      const hash=createHash('sha256').update(canonical(request)).digest('hex');
      const prior=this.prior(id,hash);
      if (prior) return prior;
      if (request.operation==='revoke') {
        const answer=response(id,true,'revoked',{requires_stop_reconciliation:true});
        this.db.exec('BEGIN IMMEDIATE');
        try {
          this.db.prepare("INSERT OR REPLACE INTO setting VALUES('revoked','1')").run();
          this.db.prepare("INSERT INTO command(id,attempt,sha256,operation,state,response) VALUES(?, '@node',?,'revoke','applied',?)")
            .run(id,hash,canonical(answer));
          this.db.exec('COMMIT');
        } catch(error) {this.db.exec('ROLLBACK');throw error;}
        for (const running of this.active.values()) for (const abort of running.values()) abort.abort();
        return answer;
      }
      const operation=request.operation==='cancel'?'cancel':args.operation;
      if (typeof operation!=='string'||!['start','pause','resume','reply','cancel'].includes(operation)) throw new WorkerError('invalid_operation');
      const job=operation==='start'||(operation==='cancel'&&args.job!==undefined)?validateJob(args.job,this.node_id):this.job(args.attempt_id);
      if(operation==='cancel'&&args.job!==undefined&&args.attempt_id!==job.attempt_id)throw new WorkerError('attempt_binding_conflict');
      attemptId=job.attempt_id;
      const current=this.db.prepare('SELECT * FROM job WHERE attempt=?').get(job.attempt_id);
      if (current&&current.manifest!==canonical(job)) throw new WorkerError('attempt_binding_conflict');
      if (this.db.prepare("SELECT 1 FROM setting WHERE key='revoked'").get()&&operation!=='cancel') throw new WorkerError('node_revoked');
      if (current?.state==='cancelled') throw new WorkerError('attempt_cancelled');
      const cancellation=!!this.db.prepare('SELECT 1 FROM setting WHERE key=?').get('cancel:'+job.attempt_id);
      if (cancellation&&operation!=='cancel') throw new WorkerError('attempt_cancel_requested');
      if (this.active.get(job.attempt_id)?.size&&operation!=='cancel') throw new WorkerError('worker_busy');
      if (operation==='start') {
        if (current) throw new WorkerError('attempt_already_started');
        const count=this.db.prepare("SELECT count(*) AS n FROM job WHERE state NOT IN ('cancelled','completed','failed')").get()!.n as number;
        if (count>=this.slots) throw new WorkerError('worker_capacity');
      }
      const expected:Record<string,string>={pause:'running',resume:'paused',reply:'paused'};
      if (operation in expected&&current?.state!==expected[operation]) throw new WorkerError('illegal_transition');
      const abort=new AbortController();
      this.db.exec('BEGIN IMMEDIATE');
      try {
        if (!current) this.db.prepare("INSERT INTO job VALUES(?,?,'starting',NULL)").run(job.attempt_id,canonical(job));
        this.db.prepare("INSERT INTO command(id,attempt,sha256,operation,state,response) VALUES(?,?,?,?,'executing',NULL)").run(id,job.attempt_id,hash,operation);
        if (operation==='cancel') this.db.prepare('INSERT OR REPLACE INTO setting VALUES(?,?)').run('cancel:'+job.attempt_id,id);
        this.db.exec('COMMIT');
      } catch(error) {this.db.exec('ROLLBACK');throw error;}
      if (operation==='cancel') for (const pending of this.active.get(job.attempt_id)?.values()??[]) pending.abort();
      const active=this.active.get(job.attempt_id)??new Map<string,AbortController>();
      active.set(id,abort);
      this.active.set(job.attempt_id,active);
      let effect:EffectReceipt;
      effectStarted=true;
      try {
        if (operation==='cancel') await this.driver.fence(job,{id,sha256:hash});
        effect=await this.driver.execute(operation,job,{...args,command_id:id,command_sha256:hash,native_session:current?.session},abort.signal);
      } catch {effect={state:'unknown'};}
      finally {
        active.delete(id);
        if (!active.size) this.active.delete(job.attempt_id);
      }
      return this.finish(id,job,operation,effect);
    } catch(error) {
      // Storage/serialization failure after an external effect cannot become a definitive refusal.
      if (effectStarted) return response(id,true,'effect_unknown',{state:'unknown',attempt_id:attemptId});
      return response(id,false,error instanceof WorkerError?error.message:'worker_storage_or_protocol_error');
    }
  }

  close():void {
    if (this.active.size) throw new WorkerError('worker_busy');
    if (this.closed) return;
    this.db.close();
    this.owner.close();
    this.closed=true;
  }
}
