if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import hashlib
import hmac
import json
import re
import secrets
from . import supervisor_lease as leases

class EventError(Exception):

    def __init__(self, code):
        self.code = code
        super().__init__(code)
BINDING_FIELDS = ('task_id', 'attempt_id', 'generation', 'node_id', 'provider', 'sdk_version', 'sdk_sha256', 'cli_version', 'cli_sha256', 'policy_sha256')
STATUSES = ('starting', 'running', 'completed', 'failed', 'permission_denied', 'authentication_failed', 'rate_limited', 'cancelled', 'informational')
NATIVE_TYPES = ('thread.started', 'turn.started', 'turn.completed', 'turn.failed', 'error', 'item.started', 'item.updated', 'item.completed', 'system', 'assistant', 'user', 'result', 'auth_status', 'rate_limit_event', 'stream_event', 'tool_progress', 'tool_use_summary', 'prompt_suggestion', 'bridge.error')
CODES = ('provider_error', 'permission_denied', 'authentication_failed', 'rate_limited', 'runtime_timeout', 'cancelled', 'invalid_session', 'session_conflict', 'missing_native_session', 'stream_ended_without_result', 'error_during_execution', 'error_max_turns', 'error_max_budget_usd', 'error_max_structured_output_retries')

def _schema(conn):
    row = conn.execute('SELECT version FROM worker_schema WHERE id=1').fetchone()
    if row is None or row[0] != 1:
        raise EventError('unavailable_schema')

def install(conn):
    """Explicit additive migration; called by trusted setup, never by an agent event."""
    with leases._write(conn):
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'worker_%'")}
        if tables:
            if tables != {'worker_schema', 'worker_grant', 'worker_event'}:
                raise EventError('unavailable_schema')
            _schema(conn)
            return
        conn.execute('CREATE TABLE worker_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL)')
        conn.execute('INSERT INTO worker_schema VALUES(1,1)')
        conn.execute('CREATE TABLE worker_grant(attempt TEXT PRIMARY KEY REFERENCES attempt(id), binding TEXT NOT NULL, capability_sha TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, CHECK(revoked IN (0,1)))')
        conn.execute('CREATE TABLE worker_event(event_id TEXT PRIMARY KEY, attempt TEXT NOT NULL REFERENCES worker_grant(attempt), sequence INTEGER NOT NULL, sha256 TEXT NOT NULL, payload TEXT NOT NULL, received_at REAL NOT NULL, UNIQUE(attempt,sequence))')

def _binding(value):
    if not isinstance(value, dict) or set(value) != set(BINDING_FIELDS):
        raise EventError('invalid_binding')
    for key, item in value.items():
        if key == 'generation':
            if type(item) is not int or not 1 <= item <= 2 ** 53 - 1:
                raise EventError('invalid_binding')
        elif not isinstance(item, str) or not re.fullmatch('[a-f0-9]{64}' if key.endswith('sha256') else '[a-zA-Z0-9_.:-]{1,128}', item):
            raise EventError('invalid_binding')
    if value['provider'] not in ('claude', 'codex'):
        raise EventError('invalid_binding')
    return json.dumps(value, sort_keys=True, separators=(',', ':'))

def _current(conn, binding, now):
    row = conn.execute('SELECT a.task,a.gen,a.state,a.lease_until,t.gen AS current_gen,t.status FROM attempt a JOIN task t ON t.id=a.task WHERE a.id=?', (binding['attempt_id'],)).fetchone()
    if row is None or row['task'] != binding['task_id'] or row['gen'] != binding['generation'] or (row['current_gen'] != row['gen']):
        raise EventError('stale_generation')
    if row['state'] not in ('launching', 'running', 'waiting', 'submitted') or row['status'] in ('done', 'cancelled'):
        raise EventError('attempt_ended')
    if row['state'] in ('launching', 'running') and row['lease_until'] is not None and (row['lease_until'] <= now):
        raise EventError('attempt_expired')

def grant(conn, supervisor, binding, *, persist, now=None):
    """Trusted supervisor verifies runtime approval and node identity before calling.

    Persist privately before commit; failures roll back capability issuance. One grant
    per attempt prevents accidental replacement while a worker may still be running.
    """
    encoded = _binding(binding)
    token = secrets.token_urlsafe(32)
    with leases._write(conn):
        timestamp = leases._now(now)
        _schema(conn)
        leases.assert_current(conn, supervisor, timestamp)
        _current(conn, binding, timestamp)
        if conn.execute('SELECT 1 FROM worker_grant WHERE attempt=?', (binding['attempt_id'],)).fetchone():
            raise EventError('grant_exists')
        conn.execute('INSERT INTO worker_grant VALUES(?,?,?,0,?)', (binding['attempt_id'], encoded, hashlib.sha256(token.encode('ascii')).hexdigest(), timestamp))
        if not callable(persist) or persist(token) is not None:
            raise EventError('capability_persist_failed')
        ready = leases._now(now)
        if ready < timestamp:
            raise EventError('clock_regression')
        leases.assert_current(conn, supervisor, ready)
        _current(conn, binding, ready)
    return token

def revoke(conn, supervisor, attempt_id, now=None):
    with leases._write(conn):
        _schema(conn)
        leases.assert_current(conn, supervisor, leases._now(now))
        conn.execute('UPDATE worker_grant SET revoked=1 WHERE attempt=?', (attempt_id,))

def _payload(raw):
    if not isinstance(raw, str) or len(raw.encode('utf8')) > 16384:
        raise EventError('invalid_event')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise EventError('invalid_event')
            result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: (_ for _ in ()).throw(EventError('invalid_event')))
    except (ValueError, RecursionError) as error:
        raise EventError('invalid_event') from error
    required = set(BINDING_FIELDS) | {'schema_version', 'status', 'native_type'}
    if not isinstance(value, dict) or not required <= set(value) or set(value) - required - {'session_id', 'model', 'code', 'usage'}:
        raise EventError('invalid_event')
    _binding({key: value[key] for key in BINDING_FIELDS})
    if type(value['schema_version']) is not int or value['schema_version'] != 1 or value['status'] not in STATUSES or (value['native_type'] not in NATIVE_TYPES):
        raise EventError('invalid_event')
    for key in ('session_id', 'model'):
        if key in value and (not isinstance(value[key], str) or not re.fullmatch('[a-zA-Z0-9_.:-]{1,128}', value[key])):
            raise EventError('invalid_event')
    if 'code' in value and value['code'] not in CODES:
        raise EventError('invalid_event')
    if 'usage' in value:
        usage = value['usage']
        keys = {'input_tokens', 'output_tokens', 'cached_input_tokens', 'cache_write_input_tokens', 'accounting'}
        if not isinstance(usage, dict) or set(usage) != keys or usage['accounting'] not in ('turn', 'native_cumulative'):
            raise EventError('invalid_event')
        if any((type(usage[key]) is not int or not 0 <= usage[key] <= 2 ** 53 - 1 for key in keys - {'accounting'})):
            raise EventError('invalid_event')
    return value

def ingest(conn, token, envelope, now=None):
    """Authenticate, fence and deduplicate atomically; ACK only after durable COMMIT."""
    if not isinstance(token, str) or not re.fullmatch('[a-zA-Z0-9_-]{43}', token):
        raise EventError('invalid_capability')
    if not isinstance(envelope, dict) or set(envelope) != {'event_id', 'sequence', 'payload_json', 'sha256'}:
        raise EventError('invalid_event')
    eid, sequence, raw, sha = (envelope[key] for key in ('event_id', 'sequence', 'payload_json', 'sha256'))
    if not isinstance(eid, str) or not re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', eid):
        raise EventError('invalid_event')
    if type(sequence) is not int or not 1 <= sequence <= 2 ** 53 - 1 or (not isinstance(sha, str)):
        raise EventError('invalid_event')
    value = _payload(raw)
    if hashlib.sha256(raw.encode('utf8')).hexdigest() != sha:
        raise EventError('event_hash_mismatch')
    with leases._write(conn):
        timestamp = leases._now(now)
        _schema(conn)
        row = conn.execute('SELECT * FROM worker_grant WHERE attempt=?', (value['attempt_id'],)).fetchone()
        if row is None or row['revoked'] or (not hmac.compare_digest(row['capability_sha'], hashlib.sha256(token.encode('ascii')).hexdigest())):
            raise EventError('invalid_capability')
        if row['binding'] != _binding({key: value[key] for key in BINDING_FIELDS}):
            raise EventError('binding_mismatch')
        _current(conn, value, timestamp)
        prior = conn.execute('SELECT * FROM worker_event WHERE event_id=? OR (attempt=? AND sequence=?)', (eid, value['attempt_id'], sequence)).fetchall()
        if prior:
            if len(prior) != 1 or any((prior[0][key] != expected for key, expected in [('event_id', eid), ('attempt', value['attempt_id']), ('sequence', sequence), ('payload', raw), ('sha256', sha)])):
                raise EventError('event_conflict')
        else:
            count = conn.execute('SELECT count(*) FROM worker_event WHERE attempt=?', (value['attempt_id'],)).fetchone()[0]
            if count >= 100000:
                raise EventError('event_capacity_reached')
            conn.execute('INSERT INTO worker_event VALUES(?,?,?,?,?,?)', (eid, value['attempt_id'], sequence, sha, raw, timestamp))
    return {'event_id': eid, 'sha256': sha}
