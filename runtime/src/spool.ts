/** Durable metadata outbox. Native payloads never enter the database. */
import {DatabaseSync} from 'node:sqlite';
import {mkdirSync, lstatSync, openSync, closeSync, constants} from 'node:fs';
import {join, isAbsolute} from 'node:path';
import {randomUUID, createHash} from 'node:crypto';
import type {WorkerEvent, Publish} from './runtime.js';

export interface StoredEvent {
  event_id: string;
  sequence: number;
  payload: WorkerEvent;
  sha256: string;
}
const statuses = new Set(['starting', 'running', 'completed', 'failed', 'permission_denied',
  'authentication_failed', 'rate_limited', 'cancelled', 'informational']);
const nativeTypes = new Set(['thread.started', 'turn.started', 'turn.completed', 'turn.failed', 'error',
  'item.started', 'item.updated', 'item.completed', 'system', 'assistant', 'user', 'result',
  'auth_status', 'rate_limit_event', 'stream_event', 'tool_progress', 'tool_use_summary',
  'prompt_suggestion', 'bridge.error']);
const codes = new Set(['provider_error', 'permission_denied', 'authentication_failed', 'rate_limited',
  'runtime_timeout', 'cancelled', 'invalid_session', 'session_conflict', 'missing_native_session',
  'stream_ended_without_result', 'error_during_execution', 'error_max_turns',
  'error_max_budget_usd', 'error_max_structured_output_retries']);

function metadata(event: WorkerEvent): WorkerEvent {
  const id = (value: unknown) => typeof value === 'string' && /^[a-zA-Z0-9_.:-]{1,128}$/.test(value);
  const hash = (value: unknown) => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
  if (event.schema_version !== 1 || !statuses.has(event.status) || !nativeTypes.has(event.native_type) ||
      !['claude', 'codex'].includes(event.provider) ||
      ![event.task_id, event.attempt_id, event.node_id, event.sdk_version, event.cli_version].every(id) ||
      ![event.sdk_sha256, event.cli_sha256, event.policy_sha256].every(hash) ||
      !Number.isSafeInteger(event.generation) || event.generation < 1 ||
      (event.session_id !== undefined && !id(event.session_id)) ||
      (event.model !== undefined && !id(event.model)) ||
      (event.code !== undefined && !codes.has(event.code))) throw new Error('invalid_event_metadata');
  const result: WorkerEvent = {
    schema_version: 1, task_id: event.task_id, attempt_id: event.attempt_id, generation: event.generation,
    node_id: event.node_id, provider: event.provider, sdk_version: event.sdk_version,
    sdk_sha256: event.sdk_sha256, cli_version: event.cli_version, cli_sha256: event.cli_sha256,
    policy_sha256: event.policy_sha256, status: event.status, native_type: event.native_type,
  };
  if (event.session_id !== undefined) result.session_id = event.session_id;
  if (event.model !== undefined) result.model = event.model;
  if (event.code !== undefined) result.code = event.code;
  if (event.usage !== undefined) {
    const u = event.usage;
    const values = [u.input_tokens, u.output_tokens, u.cached_input_tokens, u.cache_write_input_tokens];
    if (!values.every(n => Number.isSafeInteger(n) && n >= 0) ||
        !['turn', 'native_cumulative'].includes(u.accounting)) throw new Error('invalid_usage');
    result.usage = {input_tokens: u.input_tokens, output_tokens: u.output_tokens,
      cached_input_tokens: u.cached_input_tokens, cache_write_input_tokens: u.cache_write_input_tokens,
      accounting: u.accounting};
  }
  return result;
}

/** Trusted worker owns root and its parent. Not an isolation boundary against its Unix owner. */
export class EventSpool {
  private readonly db: DatabaseSync;
  readonly publish: Publish;
  constructor(root: string, private readonly maxPending = 10_000) {
    if (!isAbsolute(root) || !Number.isSafeInteger(maxPending) || maxPending < 1) throw new Error('invalid_spool_config');
    mkdirSync(root, {mode: 0o700, recursive: true});
    const dir = lstatSync(root);
    if (!dir.isDirectory() || dir.isSymbolicLink() || (dir.mode & 0o077) !== 0 ||
        dir.uid !== process.getuid?.()) throw new Error('unsafe_spool_directory');
    const path = join(root, 'events.sqlite3');
    const fd = openSync(path, constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
    closeSync(fd);
    const file = lstatSync(path);
    if (!file.isFile() || file.nlink !== 1 || (file.mode & 0o077) !== 0 || file.uid !== dir.uid) {
      throw new Error('unsafe_spool_file');
    }
    this.db = new DatabaseSync(path);
    try {
      this.db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;');
      const version = this.db.prepare('PRAGMA user_version').get()!.user_version;
      if (version !== 0 && version !== 1) throw new Error('unsupported_spool_schema');
      this.db.exec(`CREATE TABLE IF NOT EXISTS outbox (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL, sha256 TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0
        CHECK (acknowledged IN (0,1)));
        PRAGMA user_version=1;`);
    } catch (error) { this.db.close(); throw error; }
    this.publish = async (event, _privatePayload) => { this.append(event); };
  }

  append(event: WorkerEvent): StoredEvent {
    const payload = metadata(event);
    const serialized = JSON.stringify(payload);
    const sha256 = createHash('sha256').update(serialized).digest('hex');
    const event_id = randomUUID();
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const count = this.db.prepare('SELECT count(*) AS n FROM outbox WHERE acknowledged=0').get()!.n as number;
      if (count >= this.maxPending) throw new Error('spool_capacity_reached');
      const row = this.db.prepare('INSERT INTO outbox(event_id,payload,sha256) VALUES (?,?,?)')
        .run(event_id, serialized, sha256);
      this.db.exec('COMMIT');
      return {event_id, sequence: Number(row.lastInsertRowid), payload, sha256};
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }

  pending(limit = 100): StoredEvent[] {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error('invalid_batch_limit');
    return this.db.prepare('SELECT sequence,event_id,payload,sha256 FROM outbox WHERE acknowledged=0 ORDER BY sequence LIMIT ?')
      .all(limit).map(row => {
        const serialized = row.payload as string;
        if (createHash('sha256').update(serialized).digest('hex') !== row.sha256) throw new Error('corrupt_spool_event');
        const parsed = JSON.parse(serialized);
        if (JSON.stringify(metadata(parsed)) !== serialized) throw new Error('corrupt_spool_event');
        return {event_id: row.event_id as string, sequence: row.sequence as number,
          payload: parsed, sha256: row.sha256 as string};
      });
  }

  acknowledge(event_id: string, sha256: string): void {
    const row = this.db.prepare('SELECT sha256 FROM outbox WHERE event_id=?').get(event_id);
    if (!row || row.sha256 !== sha256) throw new Error('acknowledgement_mismatch');
    this.db.prepare('UPDATE outbox SET acknowledged=1 WHERE event_id=? AND sha256=?').run(event_id, sha256);
  }

  close(): void { this.db.close(); }
}
