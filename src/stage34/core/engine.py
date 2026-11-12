if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import hashlib
import json
import math
import re
from pathlib import PurePosixPath
import secrets
from . import control_api as api
from . import supervisor_lease as leases

def install(conn):
    api.install(conn)
    with leases._write(conn):
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'engine_%'")}
        expected = {'engine_schema', 'engine_node', 'engine_assignment', 'engine_command', 'engine_request', 'engine_evidence'}
        if names:
            if names != expected:
                raise api.APIError('unavailable_schema')
            _schema(conn)
            return
        conn.execute('CREATE TABLE engine_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL)')
        conn.execute('INSERT INTO engine_schema VALUES(1,1)')
        conn.execute('CREATE TABLE engine_node(id TEXT PRIMARY KEY, config TEXT NOT NULL, enabled INTEGER NOT NULL, last_seen REAL NOT NULL, pressure REAL NOT NULL, CHECK(enabled IN (0,1)))')
        conn.execute('CREATE TABLE engine_assignment(attempt TEXT PRIMARY KEY REFERENCES attempt(id), node TEXT NOT NULL REFERENCES engine_node(id), provider TEXT NOT NULL, model TEXT NOT NULL, base_sha TEXT NOT NULL, policy_sha TEXT NOT NULL, desired TEXT NOT NULL, slot_state TEXT NOT NULL, session TEXT)')
        conn.execute("CREATE TABLE engine_command(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE, attempt TEXT NOT NULL REFERENCES engine_assignment(attempt), operation TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, created_at REAL NOT NULL, ack_sha TEXT, CHECK(state IN ('pending','acknowledged','unknown','superseded')))")
        conn.execute('CREATE TABLE engine_request(id TEXT PRIMARY KEY, task TEXT NOT NULL REFERENCES task(id), attempt TEXT NOT NULL REFERENCES attempt(id), kind TEXT NOT NULL, candidate_sha TEXT NOT NULL, state TEXT NOT NULL, created_at REAL NOT NULL)')
        conn.execute('CREATE TABLE engine_evidence(id TEXT PRIMARY KEY, task TEXT NOT NULL REFERENCES task(id), attempt TEXT NOT NULL REFERENCES attempt(id), candidate_sha TEXT NOT NULL, base_sha TEXT NOT NULL, policy_sha TEXT NOT NULL, kind TEXT NOT NULL, outcome TEXT NOT NULL, receipt_sha TEXT NOT NULL, created_at REAL NOT NULL)')

def _json(value):
    return api._canonical(value)

def _identity(value):
    return isinstance(value, str) and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', value) is not None

def _sha(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{40}', value) is not None

def _schema(conn):
    row = conn.execute('SELECT version FROM engine_schema WHERE id=1').fetchone()
    if row is None or row[0] != 1:
        raise api.APIError('unavailable_schema')

def configure_node(conn, supervisor, node_id, config, now=None):
    """Trusted enrollment: provider binaries/workspaces must be independently approved.

    Changing a node with live assignments is refused. No host discovery or automatic
    trust-on-first-use. This function is not exposed as an agent tool.
    """
    required = {'slots', 'lanes', 'providers', 'workspace', 'base_sha', 'heartbeat_ttl', 'max_pressure'}
    if not _identity(node_id) or not isinstance(config, dict) or set(config) != required:
        raise api.APIError('invalid_node')
    if type(config['slots']) is not int or not 1 <= config['slots'] <= 8:
        raise api.APIError('invalid_node')
    if not isinstance(config['workspace'], str) or not config['workspace'].startswith('/') or str(PurePosixPath(config['workspace'])) != config['workspace'] or ('..' in PurePosixPath(config['workspace']).parts) or (config['workspace'] == '/') or (not _sha(config['base_sha'])):
        raise api.APIError('invalid_node')
    if not isinstance(config['lanes'], list) or not config['lanes'] or any((not _identity(v) for v in config['lanes'])):
        raise api.APIError('invalid_node')
    providers = config['providers']
    if not isinstance(providers, dict) or not providers or set(providers) - {'claude', 'codex'}:
        raise api.APIError('invalid_node')
    for value in providers.values():
        if not isinstance(value, dict) or set(value) != {'models', 'sdk_version', 'sdk_sha256', 'cli_version', 'cli_sha256'}:
            raise api.APIError('invalid_node')
        if not isinstance(value['models'], list) or not value['models'] or any((not _identity(v) for v in value['models'])):
            raise api.APIError('invalid_node')
        if any((not isinstance(value[k], str) or not re.fullmatch('[a-f0-9]{64}', value[k]) for k in ('sdk_sha256', 'cli_sha256'))):
            raise api.APIError('invalid_node')
        if any((not _identity(value[k]) for k in ('sdk_version', 'cli_version'))):
            raise api.APIError('invalid_node')
    for key, lower, upper in [('heartbeat_ttl', 1, 300), ('max_pressure', 0.01, 1)]:
        value = config[key]
        if isinstance(value, bool) or not isinstance(value, (float, int)) or (not math.isfinite(value)) or (not lower <= value <= upper):
            raise api.APIError('invalid_node')
    with leases._write(conn):
        stamp = leases._now(now)
        _schema(conn)
        leases.assert_current(conn, supervisor, stamp)
        live = conn.execute("SELECT 1 FROM engine_assignment WHERE node=? AND slot_state!='released'", (node_id,)).fetchone()
        if live:
            raise api.APIError('node_busy')
        conn.execute('INSERT INTO engine_node VALUES(?,?,1,?,0) ON CONFLICT(id) DO UPDATE SET config=excluded.config,enabled=1,last_seen=excluded.last_seen,pressure=0', (node_id, _json(config), stamp))

def observe_node(conn, supervisor, node_id, pressure, now=None):
    """Trusted transport monitor, not an unauthenticated worker heartbeat."""
    if isinstance(pressure, bool) or not isinstance(pressure, (int, float)) or (not math.isfinite(pressure)) or (not 0 <= pressure <= 1):
        raise api.APIError('invalid_pressure')
    with leases._write(conn):
        stamp = leases._now(now)
        _schema(conn)
        leases.assert_current(conn, supervisor, stamp)
        if conn.execute('UPDATE engine_node SET last_seen=?,pressure=? WHERE id=? AND last_seen<=?', (stamp, pressure, node_id, stamp)).rowcount != 1:
            raise api.APIError('node_not_found_or_clock_regression')

def touch_node(conn, supervisor, node_id, now=None):
    """Authenticated reachability only: retain pressure and enrollment decisions."""
    with leases._write(conn):
        stamp = leases._now(now)
        _schema(conn)
        leases.assert_current(conn, supervisor, stamp)
        if conn.execute('UPDATE engine_node SET last_seen=? WHERE id=? AND last_seen<=?', (stamp, node_id, stamp)).rowcount != 1:
            raise api.APIError('node_not_found_or_clock_regression')

class Engine:

    def __init__(self, authority, conn):
        self.c, self.conn = (authority, conn)

    def handlers(self):
        _schema(self.conn)
        return {'dispatch_ready': self.dispatch_ready, 'pause': self.pause, 'resume_task': self.resume_task, 'cancel_attempt': self.cancel_attempt, 'reply_to_question': self.reply_to_question, 'inspect_evidence': self.inspect_evidence, 'request_review': self.request_review, 'request_owner_approval': self.request_owner_approval}

    def _current(self, tx, args, extra=()):
        if set(args) != {'task_id', 'attempt_id', 'generation'} | set(extra) or not _identity(args['task_id']) or (not _identity(args['attempt_id'])):
            raise api.APIError('invalid_request')
        if type(args['generation']) is not int or args['generation'] < 1:
            raise api.APIError('invalid_request')
        task = self.c._need_task(tx, args['task_id'])
        attempt = tx.one('SELECT * FROM attempt WHERE id=? AND task=?', (args['attempt_id'], task['id']))
        if attempt is None or task['gen'] != args['generation'] or attempt['gen'] != args['generation']:
            raise api.APIError('stale_generation')
        if attempt['state'] not in self.c.LIVE:
            raise api.APIError('attempt_ended')
        assignment = tx.one('SELECT * FROM engine_assignment WHERE attempt=?', (attempt['id'],))
        if assignment is None:
            raise api.APIError('not_sdk_attempt')
        return (task, attempt, assignment)

    def _command(self, tx, attempt, operation, payload):
        cid = 'C-' + secrets.token_hex(12)
        tx.q("INSERT INTO engine_command(id,attempt,operation,payload,state,created_at,ack_sha) VALUES(?,?,?,?, 'pending',?,NULL)", (cid, attempt, operation, _json(payload), tx.now))
        return {'command_id': cid, 'attempt_id': attempt, 'operation': operation, 'state': 'pending'}

    def dispatch_ready(self, tx, args):
        _schema(tx.conn)
        if set(args) != {'task_id', 'node_id', 'provider', 'model'} or any((not _identity(v) for v in args.values())):
            raise api.APIError('invalid_request')
        node = tx.one('SELECT * FROM engine_node WHERE id=?', (args['node_id'],))
        if node is None or not node['enabled']:
            raise api.APIError('node_unavailable')
        config = json.loads(node['config'])
        if tx.now < node['last_seen'] or tx.now - node['last_seen'] > config['heartbeat_ttl'] or node['pressure'] >= config['max_pressure']:
            raise api.APIError('node_unhealthy')
        runtime = config['providers'].get(args['provider'])
        if runtime is None or args['model'] not in runtime['models']:
            raise api.APIError('capability_unavailable')
        task = self.c._need_task(tx, args['task_id'])
        if task['lane'] not in config['lanes']:
            raise api.APIError('lane_mismatch')
        count = tx.one("SELECT count(*) AS n FROM engine_assignment WHERE node=? AND slot_state!='released'", (node['id'],))['n']
        if count >= config['slots']:
            raise api.APIError('node_capacity')
        handoff = tx.one("SELECT * FROM provider_fallback WHERE task=? AND state='pending' ORDER BY created_at DESC LIMIT 1", (task['id'],))
        if handoff is not None:
            old = tx.one('SELECT a.gen,a.state,e.node,e.slot_state,e.desired FROM attempt a JOIN engine_assignment e ON e.attempt=a.id WHERE a.id=?', (handoff['old_attempt'],))
            cp = tx.one('SELECT * FROM checkpoint WHERE id=?', (handoff['checkpoint'],))
            if old is None or old['gen'] != task['gen'] or old['state'] != 'cancelled' or (old['slot_state'] != 'released') or (old['desired'] != 'cancelled'):
                raise api.APIError('handoff_not_stopped')
            if cp is None or cp['attempt'] != handoff['old_attempt'] or cp['contract_rev'] != task['contract_rev']:
                raise api.APIError('contract_changed')
            if old['node'] != args['node_id']:
                raise api.APIError('handoff_binding_mismatch')
            open_ops = tx.one("SELECT count(*) AS n FROM ext_op WHERE attempt=? AND state IN ('sending','unknown','conflict')", (handoff['old_attempt'],))['n']
            if open_ops:
                raise api.APIError('unreconciled_external_effects')
            if args['provider'] != handoff['alternate_provider'] or args['model'] != handoff['alternate_model']:
                raise api.APIError('handoff_binding_mismatch')
        nonce = self.c.new_nonce()
        result = self.c.reserve_in_tx(tx, task['id'], task['role'], nonce, lane=task['lane'], workspace=config['workspace'])
        tx.q('UPDATE attempt SET workspace=? WHERE id=?', (config['workspace'] + '/' + result.attempt, result.attempt))
        tx.q("INSERT INTO engine_assignment VALUES(?,?,?,?,?,?,?, 'allocated',NULL)", (result.attempt, node['id'], args['provider'], args['model'], config['base_sha'], self.c.PROFILE.digest, 'running'))
        payload = {'task_id': task['id'], 'generation': result.gen, 'node_id': node['id'], 'provider': args['provider'], 'model': args['model'], 'base_sha': config['base_sha'], 'policy_sha256': self.c.PROFILE.digest, **runtime}
        if handoff is not None:
            cp = tx.one('SELECT * FROM checkpoint WHERE id=?', (handoff['checkpoint'],))
            tx.q("UPDATE provider_fallback SET state='consumed', consumed_at=?, new_attempt=? WHERE id=?", (tx.now, result.attempt, handoff['id']))
        else:
            cp = None
        if cp is not None:
            payload['checkpoint'] = {'id': cp['id'], 'head_sha': cp['head_sha'], 'candidate': cp['candidate'], 'handoff_ref': cp['handoff_ref'], 'dirty_digest': cp['dirty_digest'], 'body_sha': cp['body_sha']}
        command = self._command(tx, result.attempt, 'start', payload)
        return dict(command, task_id=task['id'], generation=result.gen, node_id=node['id'])

    def _control(self, tx, args, operation, desired):
        _schema(tx.conn)
        _, attempt, assignment = self._current(tx, args)
        expected = {'pause': 'running', 'resume': 'paused'}[operation]
        if assignment['desired'] != expected:
            raise api.APIError('illegal_transition')
        tx.q('UPDATE engine_assignment SET desired=? WHERE attempt=?', (desired, attempt['id']))
        return self._command(tx, attempt['id'], operation, {'generation': attempt['gen']})

    def pause(self, tx, args):
        return self._control(tx, args, 'pause', 'paused')

    def resume_task(self, tx, args):
        return self._control(tx, args, 'resume', 'running')

    def cancel_attempt(self, tx, args):
        _schema(tx.conn)
        if set(args) != {'task_id', 'attempt_id', 'generation'} or type(args['generation']) is not int:
            raise api.APIError('invalid_request')
        attempt = tx.one('SELECT * FROM attempt WHERE id=? AND task=? AND gen=?', (args['attempt_id'], args['task_id'], args['generation']))
        if attempt is None:
            raise api.APIError('stale_generation')
        assignment = tx.one('SELECT * FROM engine_assignment WHERE attempt=?', (attempt['id'],))
        if assignment is None or assignment['slot_state'] == 'released':
            raise api.APIError('attempt_ended')
        if assignment['desired'] == 'cancelled':
            raise api.APIError('illegal_transition')
        tx.q("UPDATE engine_assignment SET desired='cancelled' WHERE attempt=?", (attempt['id'],))
        return self._command(tx, attempt['id'], 'cancel', {'generation': attempt['gen']})

    def reply_to_question(self, tx, args):
        _schema(tx.conn)
        _, attempt, assignment = self._current(tx, args, ('reply',))
        self.c._check_text('reply', args['reply'], self.c.MAX_TEXT, required=True)
        if assignment['desired'] != 'paused':
            raise api.APIError('reply_requires_paused_attempt')
        tx.q("UPDATE engine_assignment SET desired='running' WHERE attempt=?", (attempt['id'],))
        return self._command(tx, attempt['id'], 'reply', {'generation': attempt['gen'], 'reply': args['reply']})

    def _request(self, tx, args, kind):
        _schema(tx.conn)
        task, attempt, _ = self._current(tx, args, ('candidate_sha',))
        if not _sha(args['candidate_sha']):
            raise api.APIError('invalid_candidate')
        rid = 'R-' + secrets.token_hex(12)
        tx.q("INSERT INTO engine_request VALUES(?,?,?,?,?,'pending',?)", (rid, task['id'], attempt['id'], kind, args['candidate_sha'], tx.now))
        return {'request_id': rid, 'kind': kind, 'candidate_sha': args['candidate_sha'], 'state': 'pending'}

    def request_review(self, tx, args):
        return self._request(tx, args, 'review')

    def request_owner_approval(self, tx, args):
        return self._request(tx, args, 'owner_approval')

    def inspect_evidence(self, tx, args):
        _schema(self.conn)
        if set(args) != {'task_id'} or not _identity(args['task_id']):
            raise api.APIError('invalid_request')
        if self.c.get(self.conn, args['task_id']) is None:
            raise api.APIError('not_found')
        receipts = [dict(row) for row in self.conn.execute('SELECT * FROM engine_evidence WHERE task=? ORDER BY created_at,id', (args['task_id'],))]
        requests = [dict(row) for row in self.conn.execute('SELECT * FROM engine_request WHERE task=? ORDER BY created_at,id', (args['task_id'],))]
        return {'task_id': args['task_id'], 'receipts': receipts, 'requests': requests}

def acknowledge_command(authority, conn, supervisor, command_id, *, outcome, native_session=None, now=None):
    """Trusted transport attests process action; never an SDK/model-callable method.

    Non-applied/uncertain effects quarantine the slot and require reconciliation.
    Confirmed cancellation may release a superseded attempt's physical slot without
    changing its replacement's lifecycle. Commands are acknowledged in sequence.
    """
    if outcome not in ('applied', 'unknown') or not _identity(command_id):
        raise api.APIError('invalid_acknowledgement')
    if native_session is not None and (not _identity(native_session)):
        raise api.APIError('native_session_mismatch')
    payload = {'outcome': outcome, 'native_session': native_session}
    digest = hashlib.sha256(_json(payload).encode('ascii')).hexdigest()
    answer = {}

    def body(tx):
        _schema(conn)
        row = tx.one('SELECT * FROM engine_command WHERE id=?', (command_id,))
        if row is None:
            raise api.APIError('command_not_found')
        if row['state'] == 'superseded':
            raise api.APIError('command_superseded')
        if row['state'] == 'acknowledged':
            if row['ack_sha'] != digest:
                raise api.APIError('acknowledgement_conflict')
            answer.update(command_id=command_id, state='acknowledged')
            tx.audit = False
            return authority.Result(True, 'noop', '')
        earlier = tx.one("SELECT 1 FROM engine_command WHERE attempt=? AND sequence<? AND state NOT IN ('acknowledged','superseded')", (row['attempt'], row['sequence']))
        if earlier and row['operation'] != 'cancel':
            raise api.APIError('command_out_of_order')
        assignment = tx.one('SELECT * FROM engine_assignment WHERE attempt=?', (row['attempt'],))
        attempt = tx.one('SELECT * FROM attempt WHERE id=?', (row['attempt'],))
        if row['operation'] != 'start' and native_session is not None and (native_session != assignment['session']):
            raise api.APIError('native_session_mismatch')
        if outcome == 'unknown':
            tx.q("UPDATE engine_command SET state='unknown' WHERE id=?", (command_id,))
            tx.q("UPDATE engine_assignment SET slot_state='quarantined' WHERE attempt=?", (row['attempt'],))
            answer.update(command_id=command_id, state='unknown')
            return authority.Result(True, 'ok', '')
        if row['operation'] == 'cancel':
            tx.q("UPDATE engine_command SET state='superseded',ack_sha=? WHERE attempt=? AND sequence<? AND state IN ('pending','unknown')", (digest, row['attempt'], row['sequence']))
        if row['operation'] == 'cancel' and (tx.task(attempt['task'])['gen'] != attempt['gen'] or attempt['state'] not in authority.LIVE):
            result = authority.Result(True, 'ok', '')
        else:
            result = authority.sdk_transition_in_tx(tx, attempt['task'], attempt['id'], attempt['gen'], row['operation'], native_session)
        tx.q("UPDATE engine_command SET state='acknowledged',ack_sha=? WHERE id=?", (digest, command_id))
        if row['operation'] == 'cancel':
            tx.q("UPDATE engine_assignment SET slot_state='released' WHERE attempt=?", (row['attempt'],))
        else:
            tx.q("UPDATE engine_assignment SET slot_state='allocated',session=coalesce(?,session) WHERE attempt=?", (native_session, row['attempt']))
        answer.update(command_id=command_id, state='acknowledged')
        return result
    with authority.mutation_guard(conn, lambda stamp: leases.assert_current(conn, supervisor, stamp)):
        result = authority._run(conn, 'sdk_command_ack', 'transport', now, body)
    if not result.ok:
        raise api.APIError(result.code)
    return answer
