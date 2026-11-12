if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import hashlib
import json
import re
import secrets
import sqlite3
import time
SCHEMA_V4 = 4
SCHEMA_CURRENT = 5
SETTLE_WINDOW_S = 60.0
SENDING_TTL_S = 120.0
MAX_PROBES = 10
STATES = ('intent', 'sending', 'confirmed', 'failed', 'unknown', 'reconciled_absent', 'conflict', 'abandoned')
PROBE_RESULTS = ('found', 'absent', 'ambiguous', 'error')
TOKEN_RE = re.compile('^[XV]-[0-9a-f]{10}$')
KINDS = {'gh_dispatch_eval': {'resend_after_absent': True, 'approval': False}, 'gh_run_cancel': {'resend_after_absent': True, 'approval': False}, 'gh_dispatch_agent': {'resend_after_absent': True, 'approval': False}, 'gh_push_verify': {'resend_after_absent': True, 'approval': False}, 'gh_dispatch_verify': {'resend_after_absent': True, 'approval': False}, 'gh_dispatch_suite': {'resend_after_absent': True, 'approval': False}, 'gh_dispatch_drill': {'resend_after_absent': True, 'approval': False}, 'gh_push_branch': {'resend_after_absent': True, 'approval': False}, 'gh_pr_create': {'resend_after_absent': True, 'approval': False}, 'gh_pr_merge': {'resend_after_absent': False, 'approval': True}, 'jira_create': {'resend_after_absent': False, 'approval': False}, 'jira_transition': {'resend_after_absent': False, 'approval': False}}
V4_DDL = "\nCREATE TABLE ext_op(\n  id TEXT PRIMARY KEY,\n  kind TEXT NOT NULL, target TEXT NOT NULL,\n  task TEXT REFERENCES task(id), attempt TEXT REFERENCES attempt(id), gen INTEGER,\n  idem_key TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,\n  state TEXT NOT NULL CHECK(state IN ('intent','sending','confirmed','failed','unknown',\n                                       'reconciled_absent','conflict','abandoned')),\n  remote_id TEXT, remote_url TEXT, parent_op TEXT REFERENCES ext_op(id),\n  sends INTEGER NOT NULL DEFAULT 0, probes INTEGER NOT NULL DEFAULT 0,\n  approval TEXT, reason TEXT NOT NULL DEFAULT '',\n  created_at REAL NOT NULL, sent_at REAL, settled_at REAL, reconcile_after REAL);\nCREATE INDEX ext_op_open ON ext_op(state) WHERE state IN ('intent','sending','unknown','reconciled_absent','conflict');\nCREATE TABLE ext_probe(\n  id INTEGER PRIMARY KEY, op TEXT NOT NULL REFERENCES ext_op(id), ts REAL NOT NULL,\n  method TEXT NOT NULL, result TEXT NOT NULL CHECK(result IN ('found','absent','ambiguous','error')),\n  observed TEXT NOT NULL DEFAULT '');\nCREATE TABLE approval(\n  id TEXT PRIMARY KEY, action TEXT NOT NULL, target TEXT NOT NULL, bound_sha TEXT NOT NULL,\n  source TEXT NOT NULL, created_at REAL NOT NULL, consumed_by TEXT, revoked_at REAL);\nCREATE TABLE checkpoint(\n  id TEXT PRIMARY KEY, task TEXT NOT NULL, attempt TEXT NOT NULL, gen INTEGER NOT NULL, seq INTEGER NOT NULL,\n  kind TEXT NOT NULL CHECK(kind IN ('semantic','mechanical')),\n  cause TEXT NOT NULL CHECK(cause IN ('agent','periodic','stop','usage_limit','lease_lost','handback','pm')),\n  workspace TEXT NOT NULL, head_sha TEXT NOT NULL DEFAULT '', dirty_digest TEXT NOT NULL DEFAULT '',\n  contract_rev INTEGER NOT NULL, candidate TEXT, handoff_ref TEXT NOT NULL DEFAULT '',\n  body_sha TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, UNIQUE(attempt, seq));\nCREATE TABLE adapter(\n  name TEXT PRIMARY KEY, contract_version INTEGER NOT NULL,\n  status TEXT NOT NULL CHECK(status IN ('conformant','degraded','unavailable','unchecked')),\n  capabilities TEXT NOT NULL, conformance_digest TEXT NOT NULL DEFAULT '',\n  checked_at REAL, runtime_version TEXT NOT NULL DEFAULT '');\nALTER TABLE attempt ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-local';\nALTER TABLE attempt ADD COLUMN death TEXT NOT NULL DEFAULT '';\nALTER TABLE attempt ADD COLUMN resumable_after REAL;\nALTER TABLE attempt ADD COLUMN runtime_version TEXT NOT NULL DEFAULT '';\nALTER TABLE run ADD COLUMN transcript_path TEXT NOT NULL DEFAULT '';\nALTER TABLE task ADD COLUMN contract_rev INTEGER NOT NULL DEFAULT 0;\nALTER TABLE verify_run ADD COLUMN ext_op TEXT;\n\n"
V5_DDL = "\nCREATE TABLE provider_fallback(\n  id TEXT PRIMARY KEY,\n  task TEXT NOT NULL REFERENCES task(id),\n  old_attempt TEXT NOT NULL REFERENCES attempt(id),\n  alternate_provider TEXT NOT NULL,\n  alternate_model TEXT NOT NULL,\n  checkpoint TEXT NOT NULL REFERENCES checkpoint(id),\n  new_attempt TEXT REFERENCES attempt(id),\n  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','consumed','cancelled')),\n  created_at REAL NOT NULL, consumed_at REAL);\nCREATE UNIQUE INDEX provider_fallback_open ON provider_fallback(task) WHERE state='pending';\n"

def migrate(conn):
    """Atomically upgrade released v3/v4 databases to v5; repeated upgrades are safe."""
    from .controller import Unavailable
    conn.execute('BEGIN IMMEDIATE')
    try:
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        if version not in (3, 4, SCHEMA_CURRENT):
            raise Unavailable('unavailable_schema', 'unsupported outbox migration source')
        if version == 3:
            for stmt in V4_DDL.split(';'):
                if stmt.strip():
                    conn.execute(stmt)
        if version < SCHEMA_CURRENT:
            for stmt in V5_DDL.split(';'):
                if stmt.strip():
                    conn.execute(stmt)
        conn.execute('SELECT id, task, old_attempt, checkpoint, state FROM provider_fallback LIMIT 0')
        _import_verifier_runs(conn)
        conn.execute("UPDATE meta SET value=? WHERE key='schema'", (str(SCHEMA_CURRENT),))
        conn.execute('PRAGMA user_version=%d' % SCHEMA_CURRENT)
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')

def _import_verifier_runs(conn):
    """Adopt pre-outbox verifier intents without re-dispatching uncertain legacy effects."""
    columns = {r[1] for r in conn.execute('PRAGMA table_info(verify_run)')}
    if 'ext_op' not in columns:
        conn.execute('ALTER TABLE verify_run ADD COLUMN ext_op TEXT')
    for run in conn.execute("SELECT * FROM verify_run WHERE lane='pc' AND ext_op IS NULL AND state IN ('intent','running','lost')").fetchall():
        token = run['id']
        if not TOKEN_RE.fullmatch(token):
            raise ValueError('invalid legacy verifier token')
        request = {'verify_id': token, 'target_sha': run['target_sha'], '_workflow': 'candidate-verify.yml', 'legacy_import': True}
        body = json.dumps(request, sort_keys=True, separators=(',', ':'))
        state = 'confirmed' if run['remote_ref'] else 'unknown'
        conn.execute('INSERT OR IGNORE INTO ext_op(id,kind,target,idem_key,request_digest,state,remote_id,sends,created_at,sent_at) VALUES (?,?,?,?,?,?,?,?,?,?)', (token, 'gh_dispatch_verify', 'candidate-verify.yml', idem_key_for('gh_dispatch_verify', 'candidate-verify.yml', request), body, state, run['remote_ref'] or None, 1, run['intent_at'], run['started_at'] or run['intent_at']))
        conn.execute('UPDATE verify_run SET ext_op=? WHERE id=?', (token, token))

def new_token():
    return 'X-' + secrets.token_hex(5)

def idem_key_for(kind, target, request, task=None, gen=None):
    canonical = json.dumps(request, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(('%s|%s|%s|%s|%s' % (kind, target, canonical, task or '', gen or '')).encode('utf-8')).hexdigest()
APPROVAL_ACTIONS = {'gh_pr_merge': 'pr-merge'}
ENDED_ATTEMPT_STATES = ('accepted', 'abandoned', 'cancelled')

def _stale_binding(conn, task, attempt, gen):
    """None if (task, attempt, gen) names the task's CURRENT live attempt; else a refusal code.

    S5-R4: the op must be compared against the task's current generation and live-attempt
    ownership, not the historical `attempt.gen` (which never changes once the task advances).
    """
    if not task and (not attempt) and (gen is None):
        return None
    if not (task and attempt and (gen is not None)):
        return 'missing_binding'
    t = conn.execute('SELECT gen FROM task WHERE id=?', (task,)).fetchone()
    if t is None:
        return 'missing_binding'
    a = conn.execute('SELECT gen, state FROM attempt WHERE id=? AND task=?', (attempt, task)).fetchone()
    if a is None:
        return 'missing_binding'
    if a['gen'] != t['gen'] or a['gen'] != gen:
        return 'stale_generation'
    if a['state'] in ENDED_ATTEMPT_STATES:
        return 'stale_generation'
    return None

def intent(conn, kind, target, request, task=None, attempt=None, gen=None, approval_id=None, now=None, op_token=None):
    """Create an external-operation intent, or return the existing op for the same idem_key.

    Returns Result(ok, code, message, op_id) — the op id is carried in the `task` field.
    """
    from .controller import Result
    now = time.time() if now is None else now
    if kind not in KINDS:
        return Result(False, 'invalid_field', 'unknown kind %s' % kind)
    if not isinstance(target, str) or not target:
        return Result(False, 'invalid_field', 'target required')
    stale = _stale_binding(conn, task, attempt, gen)
    if stale is not None:
        return Result(False, stale, 'attempt %s is not the current live generation %s of task %s' % (attempt, gen, task))
    request_json = json.dumps(request, sort_keys=True, separators=(',', ':'))
    key = idem_key_for(kind, target, request, task, gen)
    existing = conn.execute('SELECT id, state FROM ext_op WHERE idem_key=?', (key,)).fetchone()
    if existing is not None:
        return Result(True, 'existing', 'op %s (%s)' % (existing['id'], existing['state']), existing['id'])
    if KINDS[kind]['approval']:
        if approval_id is None:
            return Result(False, 'approval_missing', 'kind %s needs an approval' % kind)
        ap = conn.execute('SELECT * FROM approval WHERE id=?', (approval_id,)).fetchone()
        if ap is None:
            return Result(False, 'approval_missing', 'no such approval %s' % approval_id)
        if ap['consumed_by'] or ap['revoked_at'] is not None:
            return Result(False, 'approval_missing', 'approval %s already consumed or revoked' % approval_id)
        if ap['action'] != APPROVAL_ACTIONS.get(kind):
            return Result(False, 'approval_stale', 'approval action %s does not authorize %s' % (ap['action'], kind))
        if ap['target'] != target:
            return Result(False, 'approval_stale', 'approval target %s != %s' % (ap['target'], target))
        if ap['bound_sha'] != request.get('head_sha', request.get('sha', '')):
            return Result(False, 'approval_stale', 'approval bound sha does not match the requested head')
    token = op_token or new_token()
    if not isinstance(token, str) or not TOKEN_RE.fullmatch(token):
        return Result(False, 'invalid_field', 'invalid operation token')
    if token.startswith('V-') and (kind != 'gh_dispatch_verify' or conn.execute('SELECT 1 FROM verify_run WHERE id=?', (token,)).fetchone() is None):
        return Result(False, 'invalid_field', 'verifier token must name its existing verify_run')
    conn.execute('BEGIN IMMEDIATE')
    try:
        conn.execute("INSERT INTO ext_op(id, kind, target, task, attempt, gen, idem_key, request_digest, state, approval, created_at) VALUES (?,?,?,?,?,?,?,?, 'intent', ?, ?)", (token, kind, target, task, attempt, gen, key, request_json, approval_id, now))
    except sqlite3.IntegrityError:
        conn.execute('ROLLBACK')
        existing = conn.execute('SELECT id FROM ext_op WHERE idem_key=?', (key,)).fetchone()
        if existing:
            return Result(True, 'existing', 'existing op', existing['id'])
        raise
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return Result(True, 'ok', 'op %s' % token, token)

def mark_sending(conn, op_id, now=None):
    """intent|reconciled_absent -> sending. The only path that sends; any other state is refused.

    S5-R2: the precondition is enforced by a conditional update inside the transaction, so two
    racing senders cannot both be granted permission. S5-R3: absence probes are per send attempt.
    S5-R1: a revoked/consumed approval refuses the send; the approval is reserved atomically here.
    S5-R4: the binding is re-validated against the task's current generation.

    The returned `gen` field carries the send epoch (`sends` value) — pass it to settle/reconcile
    so a delayed result cannot settle a later send attempt.
    """
    from .controller import Result
    now = time.time() if now is None else now
    conn.execute('BEGIN IMMEDIATE')
    try:
        bound = conn.execute('SELECT task, attempt, gen, approval,kind,state FROM ext_op WHERE id=?', (op_id,)).fetchone()
        if bound is not None and bound['state'] == 'reconciled_absent' and (not KINDS[bound['kind']]['resend_after_absent']):
            conn.execute('ROLLBACK')
            return Result(False, 'resend_forbidden', 'kind does not permit resending')
        if bound is not None:
            stale = _stale_binding(conn, bound['task'], bound['attempt'], bound['gen'])
            if stale is not None:
                conn.execute('ROLLBACK')
                return Result(False, stale, 'op %s belongs to a superseded generation' % op_id)
            if bound['approval']:
                ap = conn.execute('SELECT id, consumed_by, revoked_at FROM approval WHERE id=?', (bound['approval'],)).fetchone()
                if ap is None or ap['revoked_at'] is not None or (ap['consumed_by'] and ap['consumed_by'] != op_id):
                    conn.execute('ROLLBACK')
                    return Result(False, 'approval_stale', 'approval %s is revoked or already consumed' % (bound['approval'] or ''))
        cur = conn.execute("UPDATE ext_op SET state='sending', sent_at=?, sends=sends+1 WHERE id=? AND state IN ('intent','reconciled_absent')", (now, op_id))
        if cur.rowcount == 0:
            op = conn.execute('SELECT state FROM ext_op WHERE id=?', (op_id,)).fetchone()
            conn.execute('ROLLBACK')
            return Result(False, 'not_found' if op is None else 'resend_forbidden', 'no such op %s' % op_id if op is None else 'op %s is %s: send only from intent or reconciled_absent' % (op_id, op['state']))
        conn.execute("DELETE FROM ext_probe WHERE op=? AND result='absent'", (op_id,))
        if bound is not None and bound['approval']:
            conn.execute('UPDATE approval SET consumed_by=? WHERE id=?', (op_id, bound['approval']))
        epoch = conn.execute('SELECT sends FROM ext_op WHERE id=?', (op_id,)).fetchone()['sends']
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return Result(True, 'ok', 'op %s -> sending' % op_id, task=op_id, gen=epoch)

def settle(conn, op_id, outcome, remote_id='', remote_url='', reason='', now=None, epoch=None):
    """sending -> confirmed(remote_id)/failed(reason)/unknown(reconcile_after).

    S5-R2: the precondition is checked inside the transaction (no read/write race), and when an
    `epoch` (the `sends` value returned by mark_sending) is supplied, the result is fenced to that
    send attempt — a delayed result for a prior send cannot settle a later one.
    S5-R1: a confirmed success consumes the op's approval exactly once.
    """
    from .controller import Result
    now = time.time() if now is None else now
    if outcome not in ('confirmed', 'failed', 'unknown'):
        return Result(False, 'invalid_field', 'outcome must be confirmed/failed/unknown')
    if epoch is None:
        return Result(False, 'missing_epoch', 'a send epoch is required to settle')
    conn.execute('BEGIN IMMEDIATE')
    try:
        op = conn.execute('SELECT * FROM ext_op WHERE id=?', (op_id,)).fetchone()
        if op is None:
            conn.execute('ROLLBACK')
            return Result(False, 'not_found', 'no such op %s' % op_id)
        if epoch is not None and op['sends'] != epoch:
            conn.execute('ROLLBACK')
            return Result(False, 'stale_send', 'op %s result is for send %s, current send is %s' % (op_id, epoch, op['sends']))
        if op['state'] != 'sending':
            conn.execute('ROLLBACK')
            return Result(False, 'illegal_transition', 'op %s is %s: settle only from sending' % (op_id, op['state']))
        if outcome == 'confirmed':
            conn.execute("UPDATE ext_op SET state='confirmed', remote_id=?, remote_url=?, settled_at=? WHERE id=?", (remote_id, remote_url, now, op_id))
            if op['approval']:
                conn.execute('UPDATE approval SET consumed_by=? WHERE id=?', (op_id, op['approval']))
        elif outcome == 'failed':
            conn.execute("UPDATE ext_op SET state='failed', reason=?, settled_at=? WHERE id=?", (reason, now, op_id))
        else:
            conn.execute("UPDATE ext_op SET state='unknown', reason=?, reconcile_after=? WHERE id=?", (reason, now + SETTLE_WINDOW_S, op_id))
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return Result(True, 'ok', 'op %s -> %s' % (op_id, outcome))

def reconcile(conn, op_id, probe_result, method='probe', observed='', remote_id='', now=None, epoch=None):
    """Apply one read-only probe result to an `unknown` (or stale `sending`) op.

    found -> confirmed (consuming the approval); absent (>=2, settle_window apart, same send) ->
    reconciled_absent; ambiguous -> conflict; error -> stays unknown, probes+1. S5-R2: the state is
    read and transitioned in one transaction, and an `epoch` (the `sends` value from mark_sending)
    fences the probe observation to that send attempt.
    """
    from .controller import Result
    now = time.time() if now is None else now
    if probe_result not in PROBE_RESULTS:
        return Result(False, 'invalid_field', 'probe result must be %s' % '/'.join(PROBE_RESULTS))
    if epoch is None:
        return Result(False, 'missing_epoch', 'a send epoch is required to reconcile')
    conn.execute('BEGIN IMMEDIATE')
    try:
        op = conn.execute('SELECT * FROM ext_op WHERE id=?', (op_id,)).fetchone()
        if op is None:
            conn.execute('ROLLBACK')
            return Result(False, 'not_found', 'no such op %s' % op_id)
        if epoch is not None and op['sends'] != epoch:
            conn.execute('ROLLBACK')
            return Result(False, 'stale_send', 'op %s probe is for send %s, current send is %s' % (op_id, epoch, op['sends']))
        state = op['state']
        if state == 'sending' and op['sent_at'] is not None and (now - op['sent_at'] > SENDING_TTL_S):
            state = 'unknown'
        if state not in ('unknown', 'reconciled_absent'):
            conn.execute('ROLLBACK')
            return Result(False, 'illegal_transition', 'op %s is %s: reconcile only from unknown' % (op_id, state))
        conn.execute('INSERT INTO ext_probe(op, ts, method, result, observed) VALUES (?,?,?,?,?)', (op_id, now, method, probe_result, observed))
        conn.execute('UPDATE ext_op SET probes=probes+1 WHERE id=?', (op_id,))
        if probe_result == 'found':
            conn.execute("UPDATE ext_op SET state='confirmed', remote_id=?, settled_at=? WHERE id=?", (remote_id or op_id, now, op_id))
            if op['approval']:
                conn.execute('UPDATE approval SET consumed_by=? WHERE id=?', (op_id, op['approval']))
        elif probe_result == 'ambiguous':
            conn.execute("UPDATE ext_op SET state='conflict' WHERE id=?", (op_id,))
        elif probe_result == 'absent' and (not KINDS[op['kind']]['resend_after_absent']):
            conn.execute("UPDATE ext_op SET state='unknown',reason='manual_reconcile_required',reconcile_after=? WHERE id=?", (now + SETTLE_WINDOW_S, op_id))
        elif probe_result == 'absent':
            rows = conn.execute("SELECT ts FROM ext_probe WHERE op=? AND result='absent' ORDER BY ts", (op_id,)).fetchall()
            if len(rows) >= 2 and rows[-1]['ts'] - rows[0]['ts'] >= SETTLE_WINDOW_S:
                conn.execute("UPDATE ext_op SET state='reconciled_absent', settled_at=? WHERE id=?", (now, op_id))
            else:
                conn.execute('UPDATE ext_op SET reconcile_after=? WHERE id=?', (now + SETTLE_WINDOW_S, op_id))
        else:
            conn.execute('UPDATE ext_op SET reconcile_after=? WHERE id=?', (now + SETTLE_WINDOW_S, op_id))
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    final = conn.execute('SELECT state FROM ext_op WHERE id=?', (op_id,)).fetchone()['state']
    return Result(True, 'ok', 'op %s -> %s' % (op_id, final))

def create_approval(conn, action, target, bound_sha, source, now=None):
    from .controller import Result
    now = time.time() if now is None else now
    if action not in ('pr-merge',):
        return Result(False, 'invalid_field', 'unknown approval action %s' % action)
    aid = 'AP-' + hashlib.sha256(('%s|%s|%s|%s' % (action, target, bound_sha, source)).encode()).hexdigest()[:10]
    conn.execute('INSERT INTO approval(id, action, target, bound_sha, source, created_at) VALUES (?,?,?,?,?,?)', (aid, action, target, bound_sha, source, now))
    return Result(True, 'ok', 'approval %s' % aid, aid)
