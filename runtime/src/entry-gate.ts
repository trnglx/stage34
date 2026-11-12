/** Host-only durable native-entry authorization. Never mount this database in a job. */
import {DatabaseSync} from 'node:sqlite';
import {createHash,randomBytes} from 'node:crypto';
import {constants,openSync,closeSync,lstatSync} from 'node:fs';
import {isAbsolute} from 'node:path';

export interface EntryBinding {
  node_id:string;attempt_id:string;generation:number;policy_sha256:string;
  command_id:string;command_sha256:string;
}
const digest=(value:string)=>createHash('sha256').update(value).digest('hex');
function key(binding:EntryBinding):string {
  if(!binding||![binding.node_id,binding.attempt_id,binding.command_id].every(v=>typeof v==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(v))||
    !Number.isSafeInteger(binding.generation)||binding.generation<1||
    ![binding.policy_sha256,binding.command_sha256].every(v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v)))throw new Error('invalid_entry_binding');
  return JSON.stringify([binding.node_id,binding.attempt_id,binding.generation,binding.policy_sha256,binding.command_id,binding.command_sha256]);
}

export class NativeEntryGate {
  private readonly db:DatabaseSync;
  constructor(path:string) {
    if(!isAbsolute(path))throw new Error('unsafe_entry_state');
    const parent=lstatSync(path.slice(0,path.lastIndexOf('/'))||'/');
    if(!parent.isDirectory()||parent.isSymbolicLink()||parent.uid!==process.getuid?.()||(parent.mode&0o077)!==0)throw new Error('unsafe_entry_state');
    try {const fd=openSync(path,constants.O_CREAT|constants.O_EXCL|constants.O_RDWR|constants.O_NOFOLLOW,0o600);closeSync(fd);}
    catch(error){if((error as NodeJS.ErrnoException).code!=='EEXIST')throw error;}
    const info=lstatSync(path);
    if(!info.isFile()||info.nlink!==1||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new Error('unsafe_entry_state');
    this.db=new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS entries(binding TEXT PRIMARY KEY,token_hash TEXT NOT NULL,nonce_hash TEXT,revoked INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS fences(attempt TEXT PRIMARY KEY);`);
  }
  /** Issue once. Losing the plaintext ticket requires a new supervised command, never rotation. */
  issue(binding:EntryBinding):string {
    const bindingKey=key(binding);const token=randomBytes(32).toString('hex');
    this.db.exec('BEGIN IMMEDIATE');
    try {
      if(this.fenced(binding))throw new Error('attempt_fenced');
      this.db.prepare('INSERT INTO entries(binding,token_hash) VALUES(?,?)').run(bindingKey,digest(token));
      this.db.exec('COMMIT');return token;
    } catch(error){this.db.exec('ROLLBACK');throw error;}
  }
  /** Same in-memory child nonce may replay a lost ACK; a different child is denied. */
  consume(binding:EntryBinding,token:string,nonce:string):boolean {
    const bindingKey=key(binding);
    if(!/^[a-f0-9]{64}$/.test(token)||!/^[a-f0-9]{64}$/.test(nonce))return false;
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const row=this.db.prepare('SELECT token_hash,nonce_hash,revoked FROM entries WHERE binding=?').get(bindingKey);
      const allowed=!!row&&!row.revoked&&!this.fenced(binding)&&row.token_hash===digest(token)&&
        (row.nonce_hash===null||row.nonce_hash===digest(nonce));
      if(allowed)this.db.prepare('UPDATE entries SET nonce_hash=? WHERE binding=?').run(digest(nonce),bindingKey);
      this.db.exec('COMMIT');return allowed;
    } catch(error){this.db.exec('ROLLBACK');throw error;}
  }
  /** Fence all current and future commands for this exact generation before stopping it. */
  fence(binding:EntryBinding):void {
    key(binding);
    this.db.prepare('INSERT OR IGNORE INTO fences(attempt) VALUES(?)').run(this.attempt(binding));
  }
  private attempt(binding:EntryBinding):string {return JSON.stringify([binding.node_id,binding.attempt_id,binding.generation]);}
  private fenced(binding:EntryBinding):boolean {return !!this.db.prepare('SELECT 1 FROM fences WHERE attempt=?').get(this.attempt(binding));}
  close():void {this.db.close();}
}
