if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import json
import re
import time
import uuid
from pathlib import Path
SCHEMA_V3 = 3
SEVERITIES = ('block', 'critical', 'important', 'medium', 'minor', 'test_gap')
STATUSES = ('open', 'fixed', 'verified', 'waived', 'rejected')
PHASES = ('review', 'verify', 'post_accept', 'post_merge', 'prod')
FINDING_TRANSITIONS = {'open': {'from': (), 'to': 'open', 'who': ('reviewer', 'verifier', 'mutation')}, 'fix': {'from': ('open',), 'to': 'fixed', 'who': ('builder',)}, 'verify': {'from': ('fixed',), 'to': 'verified', 'who': ('verifier',)}, 'reopen': {'from': ('fixed',), 'to': 'open', 'who': ('verifier', 'reviewer')}, 'waive': {'from': ('open', 'fixed'), 'to': 'waived', 'who': ('pm',)}, 'reject': {'from': ('open',), 'to': 'rejected', 'who': ('pm',)}}
V3_DDL = "\nCREATE TABLE finding(\n  id TEXT PRIMARY KEY,\n  task TEXT NOT NULL REFERENCES task(id),\n  candidate TEXT NOT NULL REFERENCES candidate(id),\n  ref TEXT NOT NULL DEFAULT 'none',\n  severity TEXT NOT NULL CHECK(severity IN ('block','critical','important','medium','minor','test_gap')),\n  source TEXT NOT NULL,\n  location TEXT NOT NULL DEFAULT '',\n  repro TEXT NOT NULL DEFAULT '',\n  status TEXT NOT NULL CHECK(status IN ('open','fixed','verified','waived','rejected')),\n  phase TEXT NOT NULL CHECK(phase IN ('review','verify','post_accept','post_merge','prod')),\n  fix_candidate TEXT REFERENCES candidate(id),\n  waiver_reason TEXT NOT NULL DEFAULT '',\n  waived_by TEXT NOT NULL DEFAULT '',\n  created_at REAL NOT NULL,\n  updated_at REAL NOT NULL);\nCREATE TABLE finding_event(\n  id INTEGER PRIMARY KEY,\n  finding TEXT, ts REAL, from_status TEXT, to_status TEXT, actor TEXT, candidate TEXT, code TEXT);\nCREATE TABLE review(\n  id TEXT PRIMARY KEY,\n  candidate TEXT NOT NULL REFERENCES candidate(id),\n  head_sha TEXT NOT NULL,\n  role TEXT NOT NULL,\n  round INTEGER NOT NULL,\n  state TEXT NOT NULL CHECK(state IN ('started','done','partial')),\n  coverage TEXT NOT NULL DEFAULT '[]',\n  findings INTEGER NOT NULL DEFAULT 0,\n  started_at REAL,\n  ended_at REAL,\n  UNIQUE(candidate, role, round));\nCREATE TABLE integration(\n  id TEXT PRIMARY KEY,\n  branch TEXT NOT NULL,\n  sha TEXT NOT NULL,\n  prev_sha TEXT NOT NULL DEFAULT '',\n  candidates TEXT NOT NULL DEFAULT '[]',\n  state TEXT NOT NULL CHECK(state IN ('pending','verifying','verified','failed')),\n  created_at REAL NOT NULL);\n"

def migrate(conn):
    """Idempotently migrate a v2 controller DB to schema v3. Refuses any version but 2/3."""
    from .controller import Unavailable
    conn.execute('BEGIN IMMEDIATE')
    try:
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        if version == SCHEMA_V3:
            conn.execute('COMMIT')
            return
        if version != 2:
            raise Unavailable('unavailable_schema', 'cannot migrate user_version %s to %d' % (version, SCHEMA_V3))
        for stmt in V3_DDL.split(';'):
            if stmt.strip():
                conn.execute(stmt)
        conn.execute('PRAGMA user_version=%d' % SCHEMA_V3)
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')

def _new_id(prefix):
    return prefix + '-' + uuid.uuid4().hex[:10]

def _transition(conn, finding_id, cmd, actor, now, check_actor=True, **updates):
    """Apply a finding lifecycle transition; records a finding_event row."""
    from .controller import Result
    now = time.time() if now is None else now
    spec = FINDING_TRANSITIONS[cmd]
    f = conn.execute('SELECT * FROM finding WHERE id=?', (finding_id,)).fetchone()
    if f is None:
        return Result(False, 'finding_not_found', 'no such finding %s' % finding_id)
    if f['status'] not in spec['from']:
        return Result(False, 'illegal_transition', 'finding %s is %s: %s not allowed' % (finding_id, f['status'], cmd))
    if check_actor and actor not in spec['who']:
        return Result(False, 'forbidden', 'only %s may %s a finding' % ('/'.join(spec['who']), cmd))
    conn.execute('BEGIN IMMEDIATE')
    try:
        for key, value in updates.items():
            if value is not None:
                conn.execute('UPDATE finding SET %s=? WHERE id=?' % key, (value, finding_id))
        conn.execute('UPDATE finding SET status=?, updated_at=? WHERE id=?', (spec['to'], now, finding_id))
        conn.execute('INSERT INTO finding_event(finding, ts, from_status, to_status, actor, candidate, code) VALUES (?,?,?,?,?,?,?)', (finding_id, now, f['status'], spec['to'], actor, f['candidate'], cmd))
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return Result(True, 'ok', 'finding %s %s -> %s' % (finding_id, f['status'], spec['to']))

def open_finding(conn, task, candidate, severity, source, ref='none', location='', repro='', phase='review', now=None, actor='reviewer'):
    from .controller import Result, _pii, PROFILE
    now = time.time() if now is None else now
    if severity not in SEVERITIES:
        return Result(False, 'invalid_field', 'severity must be one of %s' % ', '.join(SEVERITIES))
    if phase not in PHASES:
        return Result(False, 'invalid_field', 'phase must be one of %s' % ', '.join(PHASES))
    if source != 'verifier' and source != 'mutation' and (not re.fullmatch(PROFILE.reviewer_pattern, source)):
        return Result(False, 'invalid_field', 'source must be a reviewer role, verifier or mutation')
    for name, value in (('location', location), ('repro', repro), ('ref', ref)):
        if _pii(value):
            return Result(False, 'invalid_field', '%s looks like personal data' % name)
    if conn.execute('SELECT 1 FROM task WHERE id=?', (task,)).fetchone() is None:
        return Result(False, 'not_found', 'no such task')
    if conn.execute('SELECT 1 FROM candidate WHERE id=?', (candidate,)).fetchone() is None:
        return Result(False, 'candidate_not_found', 'no such candidate')
    fid = _new_id('F')
    conn.execute("INSERT INTO finding(id, task, candidate, ref, severity, source, location, repro, status, phase, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?, 'open', ?, ?, ?)", (fid, task, candidate, ref, severity, source, location, repro, phase, now, now))
    conn.execute("INSERT INTO finding_event(finding, ts, from_status, to_status, actor, candidate, code) VALUES (?,?,NULL,'open',?,?, 'open')", (fid, now, actor, candidate))
    return Result(True, 'ok', 'finding %s opened' % fid, fid)

def fix_finding(conn, finding_id, fix_candidate, now=None):
    from .controller import Result
    now = time.time() if now is None else now
    f = conn.execute('SELECT * FROM finding WHERE id=?', (finding_id,)).fetchone()
    if f is None:
        return Result(False, 'finding_not_found', 'no such finding')
    if fix_candidate == f['candidate']:
        return Result(False, 'invalid_field', 'a fix must be a NEW candidate, not the one it was found on')
    return _transition(conn, finding_id, 'fix', 'builder', now, check_actor=False, fix_candidate=fix_candidate)

def verify_finding(conn, finding_id, now=None):
    """Clear only the finding's own unchanged reproducer, or its original reviewer's re-review."""
    from .controller import Result
    from .acceptance import staleness
    conn.execute('BEGIN IMMEDIATE')
    try:
        f = conn.execute('SELECT * FROM finding WHERE id=?', (finding_id,)).fetchone()
        if not f or f['status'] != 'fixed' or (not f['fix_candidate']):
            conn.execute('ROLLBACK')
            return Result(False, 'repro_missing', 'no fixed finding')
        old = conn.execute('SELECT * FROM candidate WHERE id=?', (f['candidate'],)).fetchone()
        new = conn.execute('SELECT * FROM candidate WHERE id=?', (f['fix_candidate'],)).fetchone()
        if not old or not new or old['task'] != new['task'] or (new['task'] != f['task']):
            conn.execute('ROLLBACK')
            return Result(False, 'repro_missing', 'fix is not on the same task')
        if not f['repro']:
            review = conn.execute("SELECT 1 FROM review WHERE candidate=? AND role=? AND head_sha=? AND state='done' AND findings=0 AND ended_at>=?", (new['id'], f['source'], new['head_sha'], f['updated_at'])).fetchone()
            if not review:
                conn.execute('ROLLBACK')
                return Result(False, 'review_missing', 'same-role re-review required')
        else:
            evidence = []
            for c, outcome in ((old, 'fail'), (new, 'pass')):
                rec = conn.execute("SELECT x.* FROM receipt x JOIN verify_run r ON r.id=x.run WHERE r.purpose='finding_repro' AND r.state='done' AND r.candidate=? AND r.target_sha=? AND x.candidate=? AND x.target_sha=? AND x.policy=? AND x.outcome=? AND x.stale=0 AND x.check_id=? ORDER BY x.created_at DESC LIMIT 1", (c['id'], c['head_sha'], c['id'], c['head_sha'], c['policy'], outcome, f['repro'])).fetchone()
                if not rec:
                    conn.execute('ROLLBACK')
                    return Result(False, 'repro_missing', 'exact reproducer proof missing')
                evidence.append(rec)
            if evidence[0]['check_digest'] != evidence[1]['check_digest']:
                conn.execute('ROLLBACK')
                return Result(False, 'repro_mismatch', 'reproducer definition changed')
            for rec in evidence:
                stale, reason = staleness(conn, rec['id'])
                if stale:
                    conn.execute('ROLLBACK')
                    return Result(False, 'repro_stale', reason)
        conn.execute("UPDATE finding SET status='verified',updated_at=? WHERE id=?", (time.time() if now is None else now, finding_id))
        conn.execute("INSERT INTO finding_event(finding,ts,from_status,to_status,actor,candidate,code) VALUES (?,?,'fixed','verified','verifier',?,'verify')", (finding_id, time.time() if now is None else now, new['id']))
        conn.execute('COMMIT')
        return Result(True, 'ok', 'finding verified')
    except BaseException:
        conn.execute('ROLLBACK')
        raise

def reopen_finding(conn, finding_id, now=None):
    return _transition(conn, finding_id, 'reopen', 'verifier', now, check_actor=False)

def waive_finding(conn, finding_id, reason, by='pm', now=None):
    from .controller import Result
    now = time.time() if now is None else now
    if by != 'pm':
        return Result(False, 'forbidden', 'only the pm may waive a finding')
    if not (reason or '').strip():
        return Result(False, 'invalid_field', 'a waiver needs a reason')
    return _transition(conn, finding_id, 'waive', by, now, waiver_reason=reason, waived_by=by)

def reject_finding(conn, finding_id, reason, by='pm', now=None):
    from .controller import Result
    now = time.time() if now is None else now
    if by != 'pm':
        return Result(False, 'forbidden', 'only the pm may reject a finding')
    if not (reason or '').strip():
        return Result(False, 'invalid_field', 'a rejection needs a reason')
    return _transition(conn, finding_id, 'reject', by, now, waiver_reason=reason, waived_by=by)

def record_review(conn, candidate, role, round_, state, head_sha, coverage=(), findings=0, now=None):
    from .controller import Result
    now = time.time() if now is None else now
    if state not in ('started', 'done', 'partial'):
        return Result(False, 'invalid_field', 'review state must be started/done/partial')
    conn.execute('BEGIN IMMEDIATE')
    try:
        existing = conn.execute('SELECT * FROM review WHERE candidate=? AND role=? AND round=?', (candidate, role, round_)).fetchone()
        if existing is None:
            rid = _new_id('R')
            conn.execute('INSERT INTO review(id, candidate, head_sha, role, round, state, coverage, findings, started_at, ended_at) VALUES (?,?,?,?,?,?,?,?,?,?)', (rid, candidate, head_sha, role, round_, state, json.dumps(list(coverage)), findings, now, now if state != 'started' else None))
        elif existing['head_sha'] != head_sha:
            conn.execute('ROLLBACK')
            return Result(False, 'sha_mismatch', 'review sha %s != started sha %s' % (head_sha, existing['head_sha']))
        elif existing['state'] in ('done', 'partial'):
            conn.execute('ROLLBACK')
            return Result(False, 'already_completed', 'review already %s' % existing['state'])
        else:
            conn.execute('UPDATE review SET state=?, head_sha=?, coverage=?, findings=?, ended_at=? WHERE id=?', (state, head_sha, json.dumps(list(coverage)), findings, now, existing['id']))
            rid = existing['id']
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return Result(True, 'ok', 'review %s recorded' % rid, rid)

def list_findings(conn, task=None, status=None):
    q = 'SELECT * FROM finding'
    args = []
    where = []
    if task:
        where.append('task=?')
        args.append(task)
    if status:
        where.append('status=?')
        args.append(status)
    if where:
        q += ' WHERE ' + ' AND '.join(where)
    q += ' ORDER BY created_at'
    return conn.execute(q, args).fetchall()

def open_mutation_findings(conn, task, candidate, summary):
    """Open a test_gap finding for each SURVIVED mutant in a mutation summary.json."""
    out = []
    for mutant in summary.get('mutants', []):
        if mutant.get('result', mutant.get('status')) == 'SURVIVED':
            if conn.execute("SELECT 1 FROM finding WHERE candidate=? AND ref=? AND source='mutation'", (candidate, mutant.get('id', 'none'))).fetchone():
                continue
            r = open_finding(conn, task, candidate, 'test_gap', 'mutation', ref=mutant.get('id', 'none'), location=mutant.get('location', ''), repro=mutant.get('id', ''), phase='review')
            out.append(r)
    return out
