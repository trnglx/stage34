if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import collections
from contextlib import contextmanager
import contextvars
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
import re
import secrets
import sqlite3
import sys
import time
import urllib.parse
from pathlib import Path
TOOLS = Path(__file__).resolve().parent
if str(TOOLS) not in sys.path:
    pass
from . import activity
from .profile_selection import PROFILE
if __name__ == '__main__':
    sys.modules.setdefault('controller', sys.modules[__name__])
SCHEMA_VERSION = 5
HOOK_TIMEOUT = 2.0
HEARTBEAT_TIMEOUT = 0.25
CLI_TIMEOUT = 5.0
BOARD_TIMEOUT = 0.5
LAUNCH_TTL_S = 900
RUN_LEASE_S = 7200
UNBOUND_SEEN_EVERY_S = 60
UNAVAILABLE = ('unavailable_missing', 'unavailable_locked', 'unavailable_corrupt', 'unavailable_schema', 'unavailable_io')
TASK_RE = re.compile('T-\\d{6}-[0-9a-f]{4}')
LAUNCH_RE = re.compile('L-[0-9a-f]{12}')
LAUNCH_LINE = re.compile('^Launch:[ \\t]*(L-[0-9a-f]{12})\\b', re.M)
SLUG_RE = re.compile('[a-z0-9][a-z0-9-]{0,79}')
ROLE_RE = re.compile('[a-z0-9][a-z0-9-]{0,63}')
LANES = PROFILE.lanes
KINDS = PROFILE.kinds
STATUSES = ('open', 'reserved', 'running', 'waiting', 'submitted', 'done', 'cancelled')
TERMINAL = ('done', 'cancelled')
LIVE = ('launching', 'running', 'waiting', 'submitted')
ENDED = ('accepted', 'abandoned', 'cancelled')
MAX_GOAL, MAX_TEXT, MAX_ITEMS, MAX_FIELD = (4000, 1000, 100, 300)
REPAIR_TEST_RE = re.compile(PROFILE.repair_test_pattern)
TEST_LIKE = re.compile(PROFILE.test_path_pattern)
REMOTE_FORBIDDEN = re.compile(PROFILE.remote_forbidden_pattern)
_ = (None, ())
TRANSITIONS = {'new': {'from': (), 'to': 'open', 'attempt': _, 'refusals': ('invalid_field', 'dangling_dep', 'remote_forbidden')}, 'reserve': {'from': ('open',), 'to': 'reserved', 'attempt': ((), 'launching'), 'refusals': ('not_found', 'terminal', 'role_mismatch', 'busy', 'deps_open', 'lane_mismatch', 'nonce_missing', 'nonce_reused', 'launch_reused')}, 'record_launch': {'from': None, 'to': None, 'attempt': _, 'refusals': ()}, 'start': {'from': ('reserved',), 'to': 'running', 'attempt': (('launching',), 'running'), 'refusals': ()}, 'heartbeat': {'from': None, 'to': None, 'attempt': (('launching', 'running'), None), 'refusals': ()}, 'submit': {'from': ('reserved', 'running', 'waiting'), 'to': 'submitted', 'attempt': (('launching', 'running', 'waiting'), 'submitted'), 'refusals': ('not_found', 'attempt_not_found', 'task_mismatch', 'stale_generation', 'attempt_ended', 'credential_required')}, 'block': {'from': ('reserved', 'running'), 'to': 'waiting', 'attempt': (('launching', 'running'), 'waiting'), 'refusals': ('not_found', 'attempt_not_found', 'task_mismatch', 'stale_generation', 'attempt_ended', 'credential_required', 'illegal_transition')}, 'handback': {'from': ('reserved',), 'to': 'running', 'attempt': (('launching',), 'running'), 'refusals': ('bind_mismatch',)}, 'stop': {'from': ('reserved', 'running'), 'to': 'waiting', 'attempt': (('launching', 'running'), 'waiting'), 'refusals': ('bind_mismatch',)}, 'resume': {'from': ('waiting', 'submitted'), 'to': 'running', 'attempt': (('waiting', 'submitted'), 'running'), 'refusals': ('not_found', 'no_live_attempt', 'sdk_paused', 'illegal_transition')}, 'reopen': {'from': ('reserved', 'running', 'waiting', 'submitted', 'done', 'cancelled'), 'to': 'open', 'attempt': (LIVE, 'abandoned'), 'refusals': ('not_found',)}, 'close_done': {'from': ('submitted',), 'to': 'done', 'attempt': (('submitted',), 'accepted'), 'refusals': ('not_found', 'illegal_transition', 'terminal')}, 'cancel': {'from': ('open', 'reserved', 'running', 'waiting', 'submitted'), 'to': 'cancelled', 'attempt': (LIVE, 'cancelled'), 'refusals': ('not_found', 'terminal')}, 'expire': {'from': ('reserved', 'running'), 'to': 'open', 'attempt': (('launching', 'running'), 'abandoned'), 'refusals': ()}, 'dispatch': {'from': ('open',), 'to': 'running', 'attempt': ((), 'running'), 'refusals': ('not_found', 'terminal', 'busy', 'deps_open', 'lane_mismatch')}, 'collect': {'from': ('running',), 'to': ('submitted', 'open'), 'attempt': (('running',), ('submitted', 'abandoned')), 'refusals': ('not_found', 'attempt_not_found')}, 'set_ref': {'from': None, 'to': None, 'attempt': _, 'refusals': ('not_found', 'invalid_field')}, 'note': {'from': None, 'to': None, 'attempt': _, 'refusals': ('not_found', 'invalid_field')}, 'import': {'from': (), 'to': STATUSES, 'attempt': ((), LIVE), 'refusals': ('invalid_field', 'conflict')}}
DDL = '\nCREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);\nCREATE TABLE task(\n  id TEXT PRIMARY KEY CHECK(id GLOB \'T-[0-9][0-9][0-9][0-9][0-9][0-9]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]\'),\n  slug TEXT NOT NULL, goal TEXT NOT NULL, role TEXT NOT NULL, lane TEXT NOT NULL CHECK(lane IN (\'local\',\'cloud\',\'pc-vm\')),\n  kind TEXT NOT NULL DEFAULT \'task\' CHECK(kind IN (\'task\',\'repair\')), repair_test TEXT NOT NULL DEFAULT \'\',\n  depth INTEGER NOT NULL DEFAULT 1, parent TEXT NOT NULL DEFAULT \'pm\', budget TEXT NOT NULL DEFAULT \'\',\n  worktree TEXT NOT NULL DEFAULT \'\', ref TEXT NOT NULL DEFAULT \'\',\n  status TEXT NOT NULL CHECK(status IN (\'open\',\'reserved\',\'running\',\'waiting\',\'submitted\',\'done\',\'cancelled\')),\n  gen INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,\n  rev INTEGER NOT NULL DEFAULT 0, projection_sha TEXT NOT NULL DEFAULT \'\',\n  source TEXT NOT NULL CHECK(source IN (\'new\',\'migrated\',\'recovered\')), import_sha TEXT NOT NULL DEFAULT \'\');\nCREATE TABLE task_owns(task TEXT REFERENCES task(id), ord INTEGER, glob TEXT NOT NULL, PRIMARY KEY(task, ord));\nCREATE TABLE task_done(task TEXT REFERENCES task(id), ord INTEGER, text TEXT NOT NULL, PRIMARY KEY(task, ord));\nCREATE TABLE task_dep(task TEXT REFERENCES task(id), dep TEXT NOT NULL, PRIMARY KEY(task, dep));\nCREATE TABLE task_note(id INTEGER PRIMARY KEY, task TEXT REFERENCES task(id), ts TEXT NOT NULL, text TEXT NOT NULL);\nCREATE TABLE launch(id TEXT PRIMARY KEY, session TEXT NOT NULL, agent_type TEXT NOT NULL,\n  attempt TEXT REFERENCES attempt(id), spawned_at REAL NOT NULL, lease_until REAL NOT NULL, started_by TEXT);\nCREATE TABLE attempt(id TEXT PRIMARY KEY, nonce TEXT UNIQUE, task TEXT NOT NULL REFERENCES task(id),\n  gen INTEGER NOT NULL, role TEXT NOT NULL, lane TEXT NOT NULL, session TEXT NOT NULL DEFAULT \'\',\n  workspace TEXT NOT NULL DEFAULT \'\', agent_id TEXT, runtime_ref TEXT NOT NULL DEFAULT \'\',\n  runtime_digest TEXT NOT NULL DEFAULT \'\',\n  credential TEXT NOT NULL CHECK(credential IN (\'nonce\',\'legacy\',\'dispatch\')),\n  state TEXT NOT NULL CHECK(state IN (\'launching\',\'running\',\'waiting\',\'submitted\',\'accepted\',\'abandoned\',\'cancelled\')),\n  reason TEXT NOT NULL DEFAULT \'\', start_seen INTEGER NOT NULL DEFAULT 0,\n  created_at REAL NOT NULL, started_at REAL, lease_until REAL, ended_at REAL, UNIQUE(task, gen));\nCREATE UNIQUE INDEX attempt_one_live ON attempt(task) WHERE state IN (\'launching\',\'running\',\'waiting\',\'submitted\');\nCREATE UNIQUE INDEX attempt_agent ON attempt(agent_id) WHERE agent_id IS NOT NULL;\nCREATE INDEX launch_pending ON launch(agent_type, session) WHERE started_by IS NULL;\nCREATE TABLE run(agent_id TEXT PRIMARY KEY, agent_type TEXT NOT NULL, session TEXT NOT NULL, started_at REAL NOT NULL,\n  last_seen REAL NOT NULL, stopped_at REAL, attempt TEXT REFERENCES attempt(id), bound_by TEXT);\nCREATE TABLE "transition"(id INTEGER PRIMARY KEY, ts REAL NOT NULL, task TEXT, attempt TEXT, gen INTEGER,\n  cmd TEXT NOT NULL, from_state TEXT, to_state TEXT, ok INTEGER NOT NULL, code TEXT NOT NULL, actor TEXT NOT NULL);\n'
Result = collections.namedtuple('Result', 'ok code message task attempt gen from_state to_state bound')
Result.__new__.__defaults__ = ('', None, None, None, None, None, '')

class Unavailable(Exception):

    def __init__(self, code, detail=''):
        Exception.__init__(self, code + (': ' + detail if detail else ''))
        self.code = code
        self.detail = detail

class Refused(Exception):

    def __init__(self, code, message, task=None, attempt=None, gen=None, from_state=None):
        Exception.__init__(self, code)
        self.code, self.message, self.task, self.attempt, self.gen, self.from_state = (code, message, task, attempt, gen, from_state)

def new_nonce():
    return 'L-' + secrets.token_hex(6)

def attempt_id_for(nonce):
    return 'A-' + hashlib.sha256(nonce.encode('utf-8')).hexdigest()[:10]

def legacy_attempt_id(task, gen):
    return 'A-' + hashlib.sha256(('%s:%d' % (task, gen)).encode('utf-8')).hexdigest()[:10]

def parse_launch(text):
    """The first `Launch: L-<12 hex>` line of a prompt, or None."""
    m = LAUNCH_LINE.search(text or '')
    return m.group(1) if m else None

def iso_local(now):
    return dt.datetime.fromtimestamp(now).astimezone().isoformat(timespec='seconds')

def iso_utc(now):
    return dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(timespec='seconds')

def _epoch(iso):
    try:
        return dt.datetime.fromisoformat(iso).timestamp()
    except (TypeError, ValueError):
        return None

def db_path():
    o = os.environ.get('STAGE34_CONTROLLER_DB')
    return Path(o) if o else activity.WORK / 'controller.db'

def _classify(exc, path=None):
    msg = str(exc).lower()
    if 'locked' in msg or 'busy' in msg:
        return Unavailable('unavailable_locked', 'database is locked')
    if 'not a database' in msg or 'malformed' in msg or 'corrupt' in msg:
        return Unavailable('unavailable_corrupt', 'database file is damaged')
    if 'no such table' in msg or 'no such column' in msg:
        return Unavailable('unavailable_schema', 'schema is missing')
    if 'unable to open' in msg:
        return Unavailable('unavailable_missing' if path is not None and (not Path(path).exists()) else 'unavailable_io', 'cannot open the database')
    if isinstance(exc, sqlite3.OperationalError):
        return Unavailable('unavailable_io', type(exc).__name__)
    return Unavailable('unavailable_corrupt', type(exc).__name__)

def _connect(path, mode, timeout):
    uri = 'file:' + urllib.parse.quote(str(path)) + '?mode=' + mode
    conn = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def open_db(mode='rw', timeout=HOOK_TIMEOUT, path=None):
    """A connection to the controller DB, or Unavailable(code). Never creates the file."""
    if mode not in ('rw', 'ro'):
        raise ValueError('mode must be rw or ro')
    p = Path(path) if path else db_path()
    if not p.exists():
        raise Unavailable('unavailable_missing', 'no controller.db: run controller.py init (or import --recover)')
    conn = None
    try:
        conn = _connect(p, mode, timeout)
        if mode == 'rw':
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA foreign_keys=ON')
        v = conn.execute('PRAGMA user_version').fetchone()[0]
        if v != SCHEMA_VERSION:
            raise Unavailable('unavailable_schema', 'user_version %s, expected %d' % (v, SCHEMA_VERSION))
        conn.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
        return conn
    except Unavailable:
        if conn is not None:
            conn.close()
        raise
    except sqlite3.Error as e:
        if conn is not None:
            conn.close()
        raise _classify(e, p)

def create_schema(conn, launch_ttl_s=LAUNCH_TTL_S, run_lease_s=RUN_LEASE_S, now=None):
    now = time.time() if now is None else now
    conn.executescript(DDL)
    from . import acceptance
    conn.executescript(acceptance.V2_DDL)
    from . import finding
    conn.executescript(finding.V3_DDL)
    from . import outbox
    conn.executescript(outbox.V4_DDL)
    conn.executescript(outbox.V5_DDL)
    conn.execute('BEGIN IMMEDIATE')
    for k, v in (('schema', str(SCHEMA_VERSION)), ('created_at', iso_utc(now)), ('launch_ttl_s', repr(float(launch_ttl_s))), ('run_lease_s', repr(float(run_lease_s)))):
        conn.execute('INSERT INTO meta(key, value) VALUES (?, ?)', (k, v))
    conn.execute('PRAGMA user_version=%d' % SCHEMA_VERSION)
    conn.execute('COMMIT')

def memory_db(launch_ttl_s=LAUNCH_TTL_S, run_lease_s=RUN_LEASE_S):
    conn = sqlite3.connect(':memory:', isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    create_schema(conn, launch_ttl_s, run_lease_s)
    return conn
REVISION_LINE = re.compile('^\\*\\*Revision:\\*\\*[ \\t]*(\\d+)', re.M)

def projections_with_revision(work=None):
    """Projection files that declare **Revision:** > 0 (evidence that a DB existed)."""
    out = []
    for p in sorted(Path(work or activity.WORK).glob('*/tasks/T-*.md')):
        try:
            m = REVISION_LINE.search(p.read_text(encoding='utf-8', errors='replace'))
        except OSError:
            continue
        if m and int(m.group(1)) > 0:
            out.append(p)
    return out

def init_db(path=None, launch_ttl_s=LAUNCH_TTL_S, run_lease_s=RUN_LEASE_S, now=None):
    p = Path(path) if path else db_path()
    if p.exists():
        return Result(False, 'exists', '%s exists: init never overwrites (use doctor, or import --recover)' % p.name)
    ev = projections_with_revision(p.parent)
    if ev:
        return Result(False, 'recover_required', '%d projection(s) declare a Revision > 0, so a DB existed: run controller.py import --recover' % len(ev))
    if not all((math.isfinite(v) and v > 0 for v in (launch_ttl_s, run_lease_s))):
        return Result(False, 'invalid_field', 'TTLs must be finite and positive')
    p.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
    os.close(fd)
    os.chmod(str(p), 384)
    try:
        conn = _connect(p, 'rw', CLI_TIMEOUT)
        conn.execute('PRAGMA journal_mode=WAL')
        create_schema(conn, launch_ttl_s, run_lease_s, now)
        conn.close()
    except BaseException:
        for s in ('', '-wal', '-shm'):
            try:
                os.unlink(str(p) + s)
            except OSError:
                pass
        raise
    return Result(True, 'ok', 'created %s (launch_ttl_s=%s, run_lease_s=%s)' % (p.name, launch_ttl_s, run_lease_s))

def migrate_db(path=None, now=None):
    """Migrate a controller DB to schema v5 (candidate/policy/receipt/finding/outbox records). Idempotent."""
    p = Path(path) if path else db_path()
    if not p.exists():
        return Result(False, 'unavailable_missing', 'no controller.db: run controller.py init first')
    try:
        conn = open_db('rw', CLI_TIMEOUT, p)
    except Unavailable as e:
        if e.code != 'unavailable_schema':
            raise
        conn = _connect(p, 'rw', CLI_TIMEOUT)
        conn.execute('PRAGMA foreign_keys=ON')
    try:
        from . import acceptance
        from . import finding
        from . import outbox
        before = conn.execute('PRAGMA user_version').fetchone()[0]
        if before not in (1, 2, 3, 4, 5):
            raise Unavailable('unavailable_schema', 'unsupported migration source')
        if before < 2:
            acceptance.migrate(conn)
        if before < 3:
            finding.migrate(conn)
        outbox.migrate(conn)
        after = conn.execute('PRAGMA user_version').fetchone()[0]
        from . import verification_contract
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('verifier_digest',?)", (verification_contract.verifier_digest(TOOLS),))
        return Result(True, 'ok', 'migrated user_version %s -> %s' % (before, after))
    except Unavailable as e:
        return Result(False, e.code, e.detail)
    finally:
        conn.close()

def ttls(conn):
    rows = dict((tuple(r) for r in conn.execute("SELECT key, value FROM meta WHERE key IN ('launch_ttl_s', 'run_lease_s')")))
    try:
        values = (float(rows['launch_ttl_s']), float(rows['run_lease_s']))
        if not all((math.isfinite(v) and v > 0 for v in values)):
            raise ValueError('invalid lease')
        return values
    except (KeyError, ValueError):
        raise Unavailable('unavailable_schema', 'meta TTLs must be finite and positive')

class _Tx:
    """State of one command transaction. Only DB statements run while it is open."""

    def __init__(self, conn, cmd, actor, now):
        self.conn, self.cmd, self.actor, self.now = (conn, cmd, actor, now)
        self.events = []
        self.touched = []
        self.audit = True
        self.launch_ttl, self.run_lease = ttls(conn)

    def q(self, sql, args=()):
        return self.conn.execute(sql, args)

    def one(self, sql, args=()):
        return self.conn.execute(sql, args).fetchone()

    def touch(self, task):
        if task and task not in self.touched:
            self.touched.append(task)

    def event(self, name, **fields):
        self.events.append((name, fields))

    def task(self, tid):
        return self.one('SELECT * FROM task WHERE id=?', (tid,))

    def set_task(self, t, status, prev):
        """Move task row `t` to `status`, bump rev, queue the transition + legacy events."""
        self.q('UPDATE task SET status=?, rev=rev+1, updated_at=? WHERE id=?', (status, iso_local(self.now), t['id']))
        self.touch(t['id'])
        self.event('task', slug=t['slug'], task=t['id'], state=status, lane=t['lane'])
        return prev

def _transition_row(tx, task, attempt, gen, cmd, frm, to, ok, code, actor):
    tx.q('INSERT INTO "transition"(ts, task, attempt, gen, cmd, from_state, to_state, ok, code, actor) VALUES (?,?,?,?,?,?,?,?,?,?)', (tx.now, task, attempt, gen, cmd, frm, to, 1 if ok else 0, code, actor))
_MUTATION_GUARD = contextvars.ContextVar('controller_mutation_guard', default=None)

@contextmanager
def mutation_guard(conn, check):
    """Integration-only fence checked under each mutation's BEGIN IMMEDIATE.

    `check(now)` must verify its credential using this connection. No network,
    rendering or provider code belongs in the callback. Guard state is scoped to
    the calling context, never a process-wide active supervisor variable.
    """
    if _MUTATION_GUARD.get() is not None or not callable(check):
        raise ValueError('invalid or nested mutation guard')
    token = _MUTATION_GUARD.set((conn, check))
    try:
        yield
    finally:
        _MUTATION_GUARD.reset(token)

def _run(conn, cmd, actor, now, body):
    """BEGIN IMMEDIATE; expire_due; SAVEPOINT; body; audit row; COMMIT. Then events and renders, outside."""
    try:
        conn.execute('BEGIN IMMEDIATE')
    except sqlite3.Error as e:
        raise _classify(e)
    try:
        now = time.time() if now is None else float(now)
        guard = _MUTATION_GUARD.get()
        if guard is not None:
            if guard[0] is not conn:
                raise ValueError('mutation guard connection mismatch')
            guard[1](now)
        tx = _Tx(conn, cmd, actor, now)
        expire_due(conn, now, tx)
        mark = (len(tx.events), len(tx.touched))
        conn.execute('SAVEPOINT body')
        try:
            res = body(tx)
            conn.execute('RELEASE body')
        except Refused as r:
            conn.execute('ROLLBACK TO body')
            conn.execute('RELEASE body')
            del tx.events[mark[0]:]
            del tx.touched[mark[1]:]
            res = Result(False, r.code, r.message, r.task, r.attempt, r.gen, r.from_state, None)
            tx.audit = True
            tx.event('transition_refused', task=r.task, attempt=r.attempt, gen=r.gen, cmd=cmd, code=r.code, actor=actor)
        if tx.audit:
            _transition_row(tx, res.task, res.attempt, res.gen, cmd, res.from_state, res.to_state, res.ok, res.code, actor)
        conn.execute('COMMIT')
    except BaseException as e:
        try:
            if conn.in_transaction:
                conn.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        if isinstance(e, sqlite3.Error):
            raise _classify(e)
        raise
    _after_commit(conn, tx)
    return res
_PROJ = {}

def _projection():
    if 'mod' not in _PROJ:
        p = TOOLS / 'projection.py'
        mod = None
        if p.exists():
            spec = importlib.util.spec_from_file_location('projection', str(p))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        _PROJ['mod'] = mod
    return _PROJ['mod']

def _after_commit(conn, tx):
    for name, fields in tx.events:
        activity.log(name, **fields)
    if not tx.touched:
        return
    try:
        proj = _projection()
    except Exception as e:
        proj, err = (None, type(e).__name__)
    else:
        err = 'missing'
    for tid in tx.touched:
        try:
            if proj is None:
                raise LookupError(err)
            proj.render(conn, tid)
        except Exception as e:
            rev = None
            try:
                r = conn.execute('SELECT rev FROM task WHERE id=?', (tid,)).fetchone()
                rev = r[0] if r else None
            except sqlite3.Error:
                pass
            activity.log('projection_failed', task=tid, rev=rev, reason=str(e) if isinstance(e, LookupError) else type(e).__name__)

def _ok(tx, t, frm, to, attempt=None, gen=None, code='ok', message='', bound=''):
    """A successful result for task row `t`; queues the transition event."""
    tid = t['id'] if t is not None else None
    lane = t['lane'] if t is not None else None
    dur = None
    if t is not None:
        e = _epoch(t['updated_at'])
        dur = round(tx.now - e, 3) if e is not None else None
    if code != 'noop':
        tx.event('transition', task=tid, attempt=attempt, gen=gen, cmd=tx.cmd, actor=tx.actor, lane=lane, dur_s=dur, slug=t['slug'] if t is not None else None, **{'from': frm, 'to': to})
    return Result(True, code, message, tid, attempt, gen, frm, to, bound)

def expire_due(conn, now, tx=None):
    """Abandon launching/running attempts whose lease is over (lease_until < now); reopen their task.
    Runs inside the caller's write transaction. Returns the expired attempt ids."""
    rows = conn.execute("SELECT * FROM attempt WHERE state IN ('launching','running') AND lease_until IS NOT NULL AND lease_until < ?", (now,)).fetchall()
    out = []
    for a in rows:
        checkpoint = conn.execute('SELECT 1 FROM checkpoint WHERE attempt=? LIMIT 1', (a['id'],)).fetchone()
        preserve = a['state'] == 'running' and (checkpoint is not None or bool(a['workspace']))
        reason = 'lease_lost' if preserve else 'launch_expired' if a['state'] == 'launching' and (not a['start_seen']) else 'run_expired'
        if preserve:
            conn.execute("UPDATE attempt SET state='waiting',death='lease_lost',reason=?,lease_until=NULL WHERE id=?", (reason, a['id']))
        else:
            conn.execute("UPDATE attempt SET state='abandoned', reason=?, ended_at=? WHERE id=?", (reason, now, a['id']))
        t = conn.execute('SELECT * FROM task WHERE id=?', (a['task'],)).fetchone()
        frm = t['status'] if t is not None else None
        to = frm
        if t is not None and t['gen'] == a['gen'] and (frm in TRANSITIONS['expire']['from']):
            to = 'waiting' if preserve else 'open'
            conn.execute('UPDATE task SET status=?, rev=rev+1, updated_at=? WHERE id=?', (to, iso_local(now), t['id']))
        conn.execute('INSERT INTO "transition"(ts, task, attempt, gen, cmd, from_state, to_state, ok, code, actor) VALUES (?,?,?,?,\'expire\',?,?,1,?,\'expire\')', (now, a['task'], a['id'], a['gen'], frm, to, reason))
        if tx is not None and t is not None:
            tx.touch(t['id'])
            tx.event('transition', task=t['id'], attempt=a['id'], gen=a['gen'], cmd='expire', actor='expire', lane=t['lane'], slug=t['slug'], **{'from': frm, 'to': to})
            if to != frm:
                tx.event('task', slug=t['slug'], task=t['id'], state=to, lane=t['lane'])
        out.append(a['id'])
    return out

def _pii(*texts):
    return any((activity.PII.search(str(t or '')) for t in texts))

def _check_text(name, value, limit, required=False):
    if not isinstance(value, str):
        raise Refused('invalid_field', '%s must be text' % name)
    if required and (not value.strip()):
        raise Refused('invalid_field', '%s is required' % name)
    if len(value) > limit:
        raise Refused('invalid_field', '%s is longer than %d characters' % (name, limit))
    if '\n' in value and name not in ('goal',):
        raise Refused('invalid_field', '%s must be one line' % name)
    if _pii(value):
        raise Refused('invalid_field', '%s looks like personal data (email or CNP-like number): keys and codes only' % name)

def _validate_task_fields(f):
    _check_text('goal', f['goal'], MAX_GOAL, required=True)
    if not isinstance(f['slug'], str) or not SLUG_RE.fullmatch(f['slug']):
        raise Refused('invalid_field', 'slug must match [a-z0-9][a-z0-9-]*')
    if not isinstance(f['role'], str) or not ROLE_RE.fullmatch(f['role']):
        raise Refused('invalid_field', 'role must be a named agent type')
    for name in ('parent', 'budget', 'worktree', 'ref', 'repair_test'):
        _check_text(name, f[name], MAX_FIELD)
    if not isinstance(f['depth'], int) or isinstance(f['depth'], bool) or (not 0 <= f['depth'] <= 9):
        raise Refused('invalid_field', 'depth must be an integer 0..9')
    for name in ('owns', 'done'):
        items = f[name]
        if not isinstance(items, (list, tuple)) or not items or len(items) > MAX_ITEMS:
            raise Refused('invalid_field', 'a task needs 1..%d %s entries' % (MAX_ITEMS, name))
        for it in items:
            _check_text(name, it, MAX_FIELD if name == 'owns' else MAX_TEXT, required=True)
    policy_error = PROFILE.task_error(f)
    if policy_error:
        raise Refused('invalid_field', policy_error)
    deps = f['depends']
    if not isinstance(deps, (list, tuple)) or len(deps) > MAX_ITEMS:
        raise Refused('invalid_field', 'depends must be a list of task ids')
    for d in deps:
        if not isinstance(d, str) or not TASK_RE.fullmatch(d):
            raise Refused('invalid_field', 'depends entries must be task ids')

def _remote_violations(owns):
    return PROFILE.remote_violations(owns)

def _insert_children(tx, tid, f, notes=()):
    for i, g in enumerate(f['owns']):
        tx.q('INSERT INTO task_owns(task, ord, glob) VALUES (?,?,?)', (tid, i, g))
    for i, d in enumerate(f['done']):
        tx.q('INSERT INTO task_done(task, ord, text) VALUES (?,?,?)', (tid, i, d))
    for d in dict.fromkeys(f['depends']):
        tx.q('INSERT INTO task_dep(task, dep) VALUES (?,?)', (tid, d))
    for ts, text in notes:
        tx.q('INSERT INTO task_note(task, ts, text) VALUES (?,?,?)', (tid, ts, text))

def _open_deps(q, tid):
    """I6: a dependency is ready iff its task is done AND (ungated OR it has an accepted,
    non-invalidated candidate)."""
    rows = q('SELECT d.dep, t.status, t.policy FROM task_dep d LEFT JOIN task t ON t.id = d.dep WHERE d.task=? ORDER BY d.dep', (tid,)).fetchall()
    open_deps = []
    for r in rows:
        if r[1] != 'done':
            open_deps.append(r[0])
        elif r[2]:
            candidate = q("SELECT id FROM candidate WHERE task=? AND state='accepted' ORDER BY updated_at DESC LIMIT 1", (r[0],)).fetchone()
            owner = getattr(q, '__self__', None)
            conn = owner.conn if isinstance(owner, _Tx) else owner
            from . import acceptance
            if candidate is None or conn is None or (not acceptance.evaluate(conn, candidate[0], include_accepted=True).ok):
                open_deps.append(r[0])
    return open_deps

def _live_attempt(tx, task):
    return tx.one("SELECT * FROM attempt WHERE task=? AND state IN ('launching','running','waiting','submitted')", (task,))

def _need_task(tx, task):
    t = tx.task(task) if isinstance(task, str) else None
    if t is None:
        raise Refused('not_found', 'Task packet %s does not exist (packet.py list).' % task, task=None)
    return t

def _task_fields(slug, goal, role, owns, done, lane='local', depends=(), kind='task', repair_test='', depth=1, parent='pm', budget='', worktree='', ref=''):
    return dict(slug=slug, goal=' '.join(str(goal or '').split()) if isinstance(goal, str) else goal, role=role, owns=list(owns or []), done=list(done or []), lane=lane, depends=list(depends or []), kind=kind, repair_test=repair_test or '', depth=depth, parent=parent, budget=budget, worktree=worktree, ref=ref)

def _task_reservation_owner(tx, tid):
    feature = tx.one("SELECT value FROM meta WHERE key='task_id_reservations'")
    if feature is None:
        if tx.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_id_reservation'"):
            raise Unavailable('unavailable_schema', 'task identifier reservation metadata is missing')
        return None
    if feature['value'] != '1':
        raise Unavailable('unavailable_schema', 'unknown task identifier reservation version')
    row = tx.one('SELECT owner FROM task_id_reservation WHERE id=?', (tid,))
    return row['owner'] if row else None

def new_task_id(tx, reserved=()):
    """Allocate an identifier using the authority's scheme, without inserting a task."""
    day = dt.datetime.fromtimestamp(tx.now).strftime('%y%m%d')
    for _ in range(1024):
        tid = 'T-%s-%s' % (day, secrets.token_hex(2))
        if tid not in reserved and tx.task(tid) is None and (_task_reservation_owner(tx, tid) is None):
            return tid
    raise Refused('invalid_field', 'task identifier allocation is exhausted')

def _insert_new_task(tx, f, task_id=None, reservation_owner=None):
    _validate_task_fields(f)
    if f['lane'] in ('pc-vm', 'cloud'):
        bad = _remote_violations(f['owns'])
        if bad:
            raise Refused('remote_forbidden', 'the remote lane refuses these owned paths: %s (use the local lane)' % ', '.join(bad))
    for d in f['depends']:
        if tx.task(d) is None:
            raise Refused('dangling_dep', 'depends on %s: no such task' % d)
    tid = task_id
    if tid is None:
        tid = new_task_id(tx)
    elif not TASK_RE.fullmatch(tid) or tx.task(tid) is not None:
        raise Refused('invalid_field', 'task id %s is malformed or exists' % tid)
    owner = _task_reservation_owner(tx, tid)
    if owner != reservation_owner:
        raise Refused('invalid_field', 'task identifier is reserved by another operation')
    if reservation_owner is not None:
        tx.q('DELETE FROM task_id_reservation WHERE id=? AND owner=?', (tid, reservation_owner))
    stamp = iso_local(tx.now)
    tx.q("INSERT INTO task(id, slug, goal, role, lane, kind, repair_test, depth, parent, budget, worktree, ref, status, gen, created_at, updated_at, rev, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'open', 0, ?, ?, 1, 'new')", (tid, f['slug'], f['goal'], f['role'], f['lane'], f['kind'], f['repair_test'], f['depth'], f['parent'], f['budget'], f['worktree'], f['ref'], stamp, stamp))
    _insert_children(tx, tid, f)
    tx.touch(tid)
    tx.event('task', slug=f['slug'], task=tid, agent_type=f['role'], lane=f['lane'], state='open', depends=','.join(f['depends']))
    return _ok(tx, tx.task(tid), None, 'open', gen=0)

def new_task(conn, slug, goal, role, owns, done, lane='local', depends=(), kind='task', repair_test='', depth=1, parent='pm', budget='', worktree='', ref='', task_id=None, now=None, actor='cli'):
    f = _task_fields(slug, goal, role, owns, done, lane, depends, kind, repair_test, depth, parent, budget, worktree, ref)
    return _run(conn, 'new', actor, now, lambda tx: _insert_new_task(tx, f, task_id))

def new_tasks(conn, records, now=None, actor='cli'):
    """Atomic plan materialization; ordered dependencies must precede their consumers."""
    created = []

    def body(tx):
        if not isinstance(records, (list, tuple)) or not 1 <= len(records) <= MAX_ITEMS:
            raise Refused('invalid_field', 'batch requires 1..%d task records' % MAX_ITEMS)
        tx.audit = False
        for record in records:
            if not isinstance(record, dict):
                raise Refused('invalid_field', 'batch task must be a record')
            for name in ('owns', 'done', 'depends'):
                if name in record and (not isinstance(record[name], (list, tuple))):
                    raise Refused('invalid_field', 'batch %s must be a list' % name)
            fields = dict(record)
            task_id = fields.pop('task_id', None)
            if task_id is not None and (not isinstance(task_id, str)):
                raise Refused('invalid_field', 'batch task id must be text')
            try:
                normalized = _task_fields(**fields)
            except (TypeError, ValueError):
                raise Refused('invalid_field', 'batch task fields are invalid')
            result = _insert_new_task(tx, normalized, task_id)
            _transition_row(tx, result.task, result.attempt, result.gen, 'new', None, 'open', True, 'ok', actor)
            created.append(result.task)
        return Result(True, 'ok', '')
    result = _run(conn, 'new_batch', actor, now, body)
    return {'ok': result.ok, 'code': result.code, 'message': result.message, 'tasks': created if result.ok else []}

def _busy_message(t, launch_ttl):
    st, tid = (t['status'], t['id'])
    if st == 'reserved':
        return 'Task packet %s is reserved by a launch that has not started yet; wait, or retry after %d min (the reservation then expires).' % (tid, max(1, int(launch_ttl // 60)))
    if st == 'running':
        return 'Task packet %s is already running: continue its agent with SendMessage instead of launching a second owner, or reopen it (packet.py track %s --status open) if it died.' % (tid, tid)
    if st == 'submitted':
        return 'Task packet %s is submitted (DONE claimed, awaiting PM verification). Verify and close it (packet.py close %s --status done), or reopen it (packet.py track %s --status open).' % (tid, tid, tid)
    if st == 'waiting':
        return 'Task packet %s is waiting for the PM: resume its agent (packet.py track %s --status running) or reopen it (packet.py track %s --status open).' % (tid, tid, tid)
    return 'Task packet %s already has a live attempt.' % tid

def _launch_checks(tx, t, role, lane):
    """The refusals reserve and dispatch share, in the table's order."""
    tid = t['id']
    if t['status'] in TERMINAL:
        raise Refused('terminal', 'Task packet %s is already %s. Create a new packet, or reopen it: packet.py track %s --status open.' % (tid, t['status'], tid), task=tid, gen=t['gen'], from_state=t['status'])
    if role is not None and t['role'] != role:
        raise Refused('role_mismatch', 'Task packet %s is assigned to %s, not %s. Launch %s, or create a packet for %s (packet.py new ... --agent %s).' % (tid, t['role'], role, t['role'], role, role), task=tid, gen=t['gen'], from_state=t['status'])
    if t['status'] not in TRANSITIONS['reserve']['from'] or _live_attempt(tx, tid) is not None:
        raise Refused('busy', _busy_message(t, tx.launch_ttl), task=tid, gen=t['gen'], from_state=t['status'])
    deps = _open_deps(tx.q, tid)
    if deps:
        raise Refused('deps_open', 'Task packet %s waits on %s (not done yet).' % (tid, ', '.join(deps)), task=tid, gen=t['gen'], from_state=t['status'])
    from . import runtime_adapters
    if runtime_adapters.is_unavailable(tx.conn, t['lane']):
        raise Refused('adapter_unavailable', 'the %s runtime adapter is unavailable' % t['lane'], task=tid, gen=t['gen'], from_state=t['status'])
    if t['lane'] != lane:
        raise Refused('lane_mismatch', 'Task packet %s is a %s packet, not %s%s.' % (tid, t['lane'], lane, ' (dispatch it: packet.py dispatch %s)' % tid if t['lane'] == 'pc-vm' else ''), task=tid, gen=t['gen'], from_state=t['status'])

def reserve_in_tx(tx, task, role, nonce, launch_id=None, session='', lane='local', workspace=''):
    """Canonical reservation used by adapters already inside an authority transaction."""
    t = _need_task(tx, task)
    _launch_checks(tx, t, role, lane)
    tid = t['id']
    if not isinstance(nonce, str) or not LAUNCH_RE.fullmatch(nonce):
        raise Refused('nonce_missing', 'The spawn prompt has no `Launch: L-<12 hex>` line: generate it with packet.py prompt %s.' % tid, task=tid, gen=t['gen'], from_state=t['status'])
    if tx.one('SELECT 1 FROM attempt WHERE nonce=?', (nonce,)) is not None:
        raise Refused('nonce_reused', 'This Launch credential was already used: run packet.py prompt %s again for a fresh one.' % tid, task=tid, gen=t['gen'], from_state=t['status'])
    lid = launch_id or 'uuid-' + secrets.token_hex(8)
    if tx.one('SELECT 1 FROM launch WHERE id=?', (lid,)) is not None:
        raise Refused('launch_reused', 'Launch id %s was already recorded.' % lid, task=tid, gen=t['gen'], from_state=t['status'])
    gen = t['gen'] + 1
    aid = attempt_id_for(nonce)
    try:
        tx.q("INSERT INTO attempt(id, nonce, task, gen, role, lane, session, workspace, credential, state, created_at, lease_until) VALUES (?,?,?,?,?,?,?,?, 'nonce', 'launching', ?, ?)", (aid, nonce, tid, gen, role, lane, session or '', workspace or '', tx.now, tx.now + tx.launch_ttl))
    except sqlite3.IntegrityError:
        raise Refused('busy', _busy_message(t, tx.launch_ttl), task=tid, gen=t['gen'], from_state=t['status'])
    tx.q('INSERT INTO launch(id, session, agent_type, attempt, spawned_at, lease_until) VALUES (?,?,?,?,?,?)', (lid, session or '', role, aid, tx.now, tx.now + tx.launch_ttl))
    tx.q('UPDATE task SET gen=? WHERE id=?', (gen, tid))
    tx.set_task(t, 'reserved', t['status'])
    return _ok(tx, t, t['status'], 'reserved', attempt=aid, gen=gen)

def reserve(conn, task, role, nonce, launch_id=None, session='', lane='local', workspace='', now=None, actor='guard'):
    return _run(conn, 'reserve', actor, now, lambda tx: reserve_in_tx(tx, task, role, nonce, launch_id, session, lane, workspace))

def adapter_check(conn, name, payload_keys=None, now=None):
    """Run the runtime adapter conformance check and persist the verdict (S5-D4)."""
    from . import runtime_adapters
    status, reason = runtime_adapters.conformance(conn, name, payload_keys)
    runtime_adapters.upsert_adapter(conn, name, status, now=now)
    return Result(True, status, reason or 'adapter %s is %s' % (name, status))

def observe_unbound(conn, agent_id, agent_type, session, transcript_path='', now=None):
    """Record runtime drift without granting task binding to an incomplete start payload."""

    def body(tx):
        if agent_id:
            tx.q("INSERT INTO run(agent_id,agent_type,session,started_at,last_seen,transcript_path,bound_by) VALUES (?,?,?,?,?,?,'unbound') ON CONFLICT(agent_id) DO UPDATE SET last_seen=excluded.last_seen,attempt=NULL,bound_by='unbound'", (agent_id, agent_type, session, tx.now, tx.now, transcript_path))
        tx.event('adapter_drift', adapter='claude-local', code='missing_payload_keys')
        return Result(True, 'unbound', 'runtime payload drift: binding withheld')
    return _run(conn, 'observe_unbound', 'hook', now, body)

def record_launch(conn, launch_id, session, agent_type, now=None, actor='guard'):

    def body(tx):
        tx.audit = False
        lid = launch_id or 'uuid-' + secrets.token_hex(8)
        if tx.one('SELECT 1 FROM launch WHERE id=?', (lid,)) is not None:
            return Result(True, 'noop', 'launch already recorded')
        tx.q('INSERT INTO launch(id, session, agent_type, spawned_at, lease_until) VALUES (?,?,?,?,?)', (lid, session or '', agent_type or '', tx.now, tx.now + tx.launch_ttl))
        return Result(True, 'ok', 'launch recorded')
    return _run(conn, 'record_launch', actor, now, body)

def _bind(tx, a, agent_id, bound_by, group_size):
    """Bind attempt row `a` to agent_id; launching -> running (task reserved -> running). Returns from/to."""
    t = tx.task(a['task'])
    frm = t['status']
    to = frm
    tx.q('UPDATE attempt SET agent_id=? WHERE id=?', (agent_id, a['id']))
    if a['state'] == 'launching':
        tx.q("UPDATE attempt SET state='running', started_at=?, lease_until=? WHERE id=?", (tx.now, tx.now + tx.run_lease, a['id']))
        if frm == 'reserved' and t['gen'] == a['gen']:
            to = 'running'
            tx.set_task(t, 'running', frm)
    tx.q('UPDATE launch SET started_by=? WHERE attempt=? AND started_by IS NULL', (agent_id, a['id']))
    tx.event('bind', attempt=a['id'], agent_id=agent_id, bound_by=bound_by, group_size=group_size, task=t['id'])
    return (t, frm, to)

def start(conn, agent_id, agent_type, session, transcript_path='', now=None):

    def body(tx):
        run = tx.one('SELECT * FROM run WHERE agent_id=?', (agent_id,))
        if run is not None:
            tx.q('UPDATE run SET last_seen=?, stopped_at=NULL, transcript_path=? WHERE agent_id=?', (tx.now, transcript_path or run['transcript_path'], agent_id))
            a = tx.one('SELECT * FROM attempt WHERE id=?', (run['attempt'],)) if run['attempt'] else None
            if a is not None and a['state'] == 'running':
                tx.q('UPDATE attempt SET lease_until=? WHERE id=?', (tx.now + tx.run_lease, a['id']))
            t = tx.task(a['task']) if a is not None else None
            return Result(True, 'resume', 'known agent: lease refreshed, no new binding', t['id'] if t else None, a['id'] if a else None, a['gen'] if a else None, None, None, 'resume')
        group = tx.q('SELECT * FROM launch WHERE agent_type=? AND session=? AND started_by IS NULL AND lease_until >= ? ORDER BY spawned_at', (agent_type, session or '', tx.now)).fetchall()
        tx.q('INSERT INTO run(agent_id, agent_type, session, started_at, last_seen, transcript_path) VALUES (?,?,?,?,?,?)', (agent_id, agent_type, session or '', tx.now, tx.now, transcript_path or ''))
        if not group:
            return Result(True, 'no_launch', 'no pending launch for this agent type in this session')
        if len(group) == 1:
            la = group[0]
            a = tx.one('SELECT * FROM attempt WHERE id=?', (la['attempt'],)) if la['attempt'] else None
            if a is not None and a['state'] == 'launching' and a['start_seen']:
                tx.event('bind', agent_id=agent_id, bound_by='unbound', group_size=1)
                return Result(True, 'unbound', 'launch was in an ambiguous group: transcript proof required')
            if a is None or a['state'] != 'launching':
                tx.q('UPDATE launch SET started_by=? WHERE id=?', (agent_id, la['id']))
                return Result(True, 'no_attempt', 'the pending launch carries no live attempt')
            if tx.one('SELECT 1 FROM attempt WHERE agent_id=?', (agent_id,)) is not None:
                return Result(True, 'unbound', 'agent id already bound elsewhere')
            t, frm, to = _bind(tx, a, agent_id, 'single', 1)
            tx.q("UPDATE run SET attempt=?, bound_by='single' WHERE agent_id=?", (a['id'], agent_id))
            return _ok(tx, t, frm, to, attempt=a['id'], gen=a['gen'], code='bound', bound='single')
        for launch in group:
            tx.q('UPDATE launch SET lease_until=MAX(lease_until, ?) WHERE id=?', (tx.now + tx.run_lease, launch['id']))
        ids = [la['attempt'] for la in group if la['attempt']]
        for aid in ids:
            tx.q("UPDATE attempt SET start_seen=1, lease_until=MAX(COALESCE(lease_until, 0), ?) WHERE id=? AND state='launching'", (tx.now + tx.run_lease, aid))
        tx.event('bind', agent_id=agent_id, bound_by='unbound', group_size=len(group))
        return Result(True, 'unbound', '%d launches of this type are pending: not bound (proof binds later)' % len(group))
    return _run(conn, 'start', 'start', now, body)

def heartbeat(conn, agent_id, now=None):
    """Renew the run lease (bound) or the group's start_seen leases (unbound). Writes only when needed."""
    preflight_now = time.time() if now is None else float(now)
    try:
        run = conn.execute('SELECT * FROM run WHERE agent_id=?', (agent_id,)).fetchone()
        if run is None:
            return Result(True, 'noop', 'unknown agent')
        _, lease = ttls(conn)
        if run['attempt']:
            need = conn.execute("SELECT 1 FROM attempt WHERE id=? AND state='running' AND lease_until IS NOT NULL AND lease_until - ? < ?", (run['attempt'], preflight_now, lease / 2.0)).fetchone()
        else:
            need = preflight_now - run['last_seen'] >= UNBOUND_SEEN_EVERY_S or conn.execute("SELECT 1 FROM attempt a JOIN launch l ON l.attempt = a.id WHERE l.agent_type=? AND l.session=? AND l.started_by IS NULL AND a.state='launching' AND a.start_seen=1 AND a.lease_until - ? < ?", (run['agent_type'], run['session'], preflight_now, lease / 2.0)).fetchone()
    except sqlite3.Error as e:
        raise _classify(e)
    if not need:
        return Result(True, 'noop', 'lease has more than half left')

    def body(tx):
        tx.audit = False
        r = tx.one('SELECT * FROM run WHERE agent_id=?', (agent_id,))
        tx.q('UPDATE run SET last_seen=? WHERE agent_id=?', (tx.now, agent_id))
        if r['attempt']:
            n = tx.q("UPDATE attempt SET lease_until=? WHERE id=? AND state='running' AND lease_until - ? < ?", (tx.now + tx.run_lease, r['attempt'], tx.now, tx.run_lease / 2.0)).rowcount
        else:
            n = tx.q("UPDATE attempt SET lease_until=? WHERE state='launching' AND start_seen=1 AND lease_until - ? < ? AND id IN (SELECT attempt FROM launch WHERE agent_type=? AND session=? AND started_by IS NULL AND attempt IS NOT NULL)", (tx.now + tx.run_lease, tx.now, tx.run_lease / 2.0, r['agent_type'], r['session'])).rowcount
            tx.q('UPDATE launch SET lease_until=MAX(lease_until, ?) WHERE agent_type=? AND session=? AND started_by IS NULL AND lease_until >= ?', (tx.now + tx.run_lease, r['agent_type'], r['session'], tx.now))
        return Result(True, 'renewed' if n else 'noop', 'lease renewed' if n else 'last_seen updated')
    return _run(conn, 'heartbeat', 'lanerules', now, body)

def _credential_attempt(tx, t, nonce):
    """The attempt a submit/block names, or Refused, per the table's order (I2)."""
    tid = t['id']
    if nonce:
        a = tx.one('SELECT * FROM attempt WHERE nonce=?', (nonce,)) if LAUNCH_RE.fullmatch(nonce) else None
        if a is None:
            raise Refused('attempt_not_found', 'No attempt carries this Launch credential (check the Launch line of your prompt).', task=tid, gen=t['gen'], from_state=t['status'])
        if a['task'] != tid:
            raise Refused('task_mismatch', 'This Launch credential belongs to another task, not %s.' % tid, task=tid, attempt=a['id'], gen=a['gen'], from_state=t['status'])
        if a['gen'] != t['gen']:
            raise Refused('stale_generation', 'Attempt %s is generation %d; %s is at generation %d: this run was superseded and its result is not recorded as a submit.' % (a['id'], a['gen'], tid, t['gen']), task=tid, attempt=a['id'], gen=a['gen'], from_state=t['status'])
        if a['state'] in ENDED:
            raise Refused('attempt_ended', 'Attempt %s is %s (%s): its result is not recorded as a submit.' % (a['id'], a['state'], a['reason'] or '-'), task=tid, attempt=a['id'], gen=a['gen'], from_state=t['status'])
        return (a, 'proof')
    a = _live_attempt(tx, tid)
    if a is None or a['gen'] != t['gen']:
        raise Refused('attempt_not_found', 'Task %s has no live attempt to submit.' % tid, task=tid, gen=t['gen'], from_state=t['status'])
    if a['credential'] != 'legacy':
        raise Refused('credential_required', "Task %s's attempt needs its credential: handoff.py ... --task %s --attempt L-... (the Launch line of your prompt), or packet.py submit %s --attempt L-..." % (tid, tid, tid), task=tid, attempt=a['id'], gen=a['gen'], from_state=t['status'])
    return (a, '')

def _deliver(conn, cmd, task, nonce, now, actor):
    target = 'submitted' if cmd == 'submit' else 'waiting'

    def body(tx):
        t = _need_task(tx, task)
        a, proof = _credential_attempt(tx, t, nonce)
        frm = t['status']
        if a['state'] == target and (target == 'submitted' or a['reason'] == 'blocked'):
            return _ok(tx, t, frm, frm, attempt=a['id'], gen=a['gen'], code='noop', message='already ' + target)
        if a['state'] not in TRANSITIONS[cmd]['attempt'][0] or frm not in TRANSITIONS[cmd]['from']:
            raise Refused('illegal_transition', 'Task %s is %s (attempt %s): %s is not allowed.' % (t['id'], frm, a['state'], cmd), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        bound = ''
        if proof and a['state'] == 'launching':
            tx.q("UPDATE launch SET started_by='proof' WHERE attempt=? AND started_by IS NULL", (a['id'],))
            tx.event('bind', attempt=a['id'], bound_by='proof', task=t['id'])
            bound = 'proof'
        tx.q('UPDATE attempt SET state=?, reason=?, started_at=COALESCE(started_at, ?) WHERE id=?', (target, '' if target == 'submitted' else 'blocked', tx.now, a['id']))
        tx.set_task(t, target, frm)
        return _ok(tx, t, frm, target, attempt=a['id'], gen=a['gen'], bound=bound)
    return _run(conn, cmd, actor, now, body)

def submit(conn, task, nonce=None, now=None, actor='handoff'):
    return _deliver(conn, 'submit', task, nonce, now, actor)

def block(conn, task, nonce=None, now=None, actor='handoff'):
    return _deliver(conn, 'block', task, nonce, now, actor)

def _run_attempt(tx, cmd, agent_id, agent_type, session, transcript_nonce):
    """(run row, attempt row or None, bound_by) after binding the run from the transcript nonce when possible."""
    run = tx.one('SELECT * FROM run WHERE agent_id=?', (agent_id,))
    if run is None:
        tx.q('INSERT INTO run(agent_id, agent_type, session, started_at, last_seen) VALUES (?,?,?,?,?)', (agent_id, agent_type, session or '', tx.now, tx.now))
        run = tx.one('SELECT * FROM run WHERE agent_id=?', (agent_id,))
        created = True
    else:
        created = False
    named = None
    if transcript_nonce and LAUNCH_RE.fullmatch(transcript_nonce):
        named = tx.one('SELECT * FROM attempt WHERE nonce=?', (transcript_nonce,))
    if run['attempt']:
        a = tx.one('SELECT * FROM attempt WHERE id=?', (run['attempt'],))
        if named is not None and named['id'] != a['id']:
            raise Refused('bind_mismatch', 'This run is bound to attempt %s, but its transcript names %s.' % (a['id'], named['id']), task=a['task'], attempt=a['id'], gen=a['gen'])
        return (run, a, run['bound_by'] or '', created)
    if named is None:
        return (run, None, '', created)
    if named['role'] != agent_type or (named['session'] and named['session'] != (session or '')) or (named['agent_id'] and named['agent_id'] != agent_id):
        raise Refused('bind_mismatch', 'The transcript names attempt %s, which belongs to another launch (role, session or agent differ).' % named['id'], task=named['task'], attempt=named['id'], gen=named['gen'])
    if named['state'] in ENDED:
        return (run, named, '', created)
    _bind(tx, named, agent_id, 'transcript', 0)
    tx.q("UPDATE run SET attempt=?, bound_by='transcript' WHERE agent_id=?", (named['id'], agent_id))
    return (run, tx.one('SELECT * FROM attempt WHERE id=?', (named['id'],)), 'transcript', created)

def handback(conn, agent_id, agent_type, session, transcript_nonce=None, transcript_path='', now=None):

    def body(tx):
        if tx.one('SELECT 1 FROM run WHERE agent_id=?', (agent_id,)) is None and (not transcript_nonce):
            tx.audit = False
            return Result(True, 'no_run', 'no run recorded for this agent')
        before = tx.one('SELECT status FROM task WHERE id=(SELECT task FROM attempt WHERE nonce=?)', (transcript_nonce or '',))
        run, a, bound, _ = _run_attempt(tx, 'handback', agent_id, agent_type, session, transcript_nonce)
        tx.q('UPDATE run SET last_seen=?, transcript_path=? WHERE agent_id=?', (tx.now, transcript_path or run['transcript_path'], agent_id))
        if a is None:
            return Result(True, 'no_attempt', 'this run has no attempt')
        t = tx.task(a['task'])
        frm = before['status'] if before is not None and bound == 'transcript' else t['status']
        if a['state'] in ENDED:
            return Result(True, 'attempt_ended', 'attempt %s is %s' % (a['id'], a['state']), t['id'], a['id'], a['gen'], frm, t['status'], bound)
        code = 'verified' if a['state'] in ('submitted', 'waiting') else 'unverified'
        if bound == 'transcript':
            return _ok(tx, t, frm, t['status'], attempt=a['id'], gen=a['gen'], code=code, bound=bound)
        return Result(True, code, '', t['id'], a['id'], a['gen'], frm, t['status'], bound)
    return _run(conn, 'handback', 'handback', now, body)

def stop(conn, agent_id, agent_type, session, transcript_nonce=None, now=None):

    def body(tx):
        if tx.one('SELECT 1 FROM run WHERE agent_id=?', (agent_id,)) is None and (not transcript_nonce):
            tx.audit = False
            return Result(True, 'noop', 'no run recorded for this agent')
        run, a, bound, _ = _run_attempt(tx, 'stop', agent_id, agent_type, session, transcript_nonce)
        tx.q('UPDATE run SET stopped_at=?, last_seen=? WHERE agent_id=?', (tx.now, tx.now, agent_id))
        if a is None or a['state'] not in ('launching', 'running'):
            return Result(True, 'noop', 'nothing running', a['task'] if a else None, a['id'] if a else None, a['gen'] if a else None, None, None, bound)
        t = tx.task(a['task'])
        frm = t['status']
        tx.q("UPDATE attempt SET state='waiting', reason='stopped_no_submit' WHERE id=?", (a['id'],))
        to = frm
        if frm in TRANSITIONS['stop']['from'] and t['gen'] == a['gen']:
            to = 'waiting'
            tx.set_task(t, 'waiting', frm)
        return _ok(tx, t, frm, to, attempt=a['id'], gen=a['gen'], bound=bound)
    return _run(conn, 'stop', 'stop', now, body)

def _note_in_tx(tx, tid, note):
    if note:
        _check_text('note', ' '.join(str(note).split()), MAX_TEXT, required=True)
        tx.q('INSERT INTO task_note(task, ts, text) VALUES (?,?,?)', (tid, iso_local(tx.now), ' '.join(note.split())))
        tx.touch(tid)

def resume(conn, task, now=None, actor='cli', note=None):

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        a = _live_attempt(tx, t['id'])
        if a is None or a['gen'] != t['gen']:
            raise Refused('no_live_attempt', 'Task %s has no live attempt to resume (reopen it instead).' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        if a['resumable_after'] is not None and tx.now < a['resumable_after']:
            raise Refused('not_resumable', 'Task %s is in its death cooldown until %s.' % (t['id'], a['resumable_after']), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        cp = tx.one('SELECT contract_rev FROM checkpoint WHERE attempt=? ORDER BY seq DESC LIMIT 1', (a['id'],))
        if cp is not None and cp['contract_rev'] != t['contract_rev']:
            raise Refused('contract_changed', 'Task %s contract changed (checkpoint rev %s != %s): re-design before resuming.' % (t['id'], cp['contract_rev'], t['contract_rev']), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if a['reason'] == 'sdk_paused':
            raise Refused('sdk_paused', 'Task %s is paused by the SDK; resume it through the SDK supervisor (resume_task), not the CLI.' % t['id'], task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if frm not in TRANSITIONS['resume']['from']:
            raise Refused('illegal_transition', 'Task %s is %s: only waiting or submitted tasks resume.' % (t['id'], frm), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        tx.q("UPDATE attempt SET state='running', reason='', lease_until=? WHERE id=?", (tx.now + tx.run_lease, a['id']))
        tx.set_task(t, 'running', frm)
        _note_in_tx(tx, t['id'], note)
        return _ok(tx, t, frm, 'running', attempt=a['id'], gen=a['gen'])
    return _run(conn, 'resume', actor, now, body)

def reopen(conn, task, now=None, actor='cli', note=None):

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if frm == 'open':
            return _ok(tx, t, frm, frm, gen=t['gen'], code='noop', message='already open')
        a = _live_attempt(tx, t['id'])
        if a is not None:
            tx.q("UPDATE attempt SET state='abandoned', reason='reopened', ended_at=? WHERE id=?", (tx.now, a['id']))
        tx.set_task(t, 'open', frm)
        _note_in_tx(tx, t['id'], note)
        return _ok(tx, t, frm, 'open', attempt=a['id'] if a is not None else None, gen=t['gen'])
    return _run(conn, 'reopen', actor, now, body)

def checkpoint(conn, task, attempt, kind, cause, workspace, head_sha='', dirty_digest='', contract_rev=0, candidate='', handoff_ref='', body_sha='', now=None, actor='handoff'):
    """Record a continuation checkpoint (S5-D3). No task state change."""

    def body(tx):
        t = _need_task(tx, task)
        a = tx.one('SELECT * FROM attempt WHERE id=?', (attempt,))
        if a is None or a['task'] != t['id']:
            raise Refused('not_found', 'no such attempt %s on task %s' % (attempt, t['id']), task=t['id'], gen=t['gen'], from_state=t['status'])
        if a['gen'] != t['gen']:
            raise Refused('stale_generation', 'attempt %s is a stale generation' % attempt, task=t['id'], attempt=a['id'], gen=a['gen'], from_state=t['status'])
        if a['state'] in ('accepted', 'abandoned', 'cancelled'):
            raise Refused('attempt_ended', 'attempt %s is %s' % (attempt, a['state']), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=t['status'])
        seq = tx.one('SELECT COALESCE(MAX(seq),0)+1 FROM checkpoint WHERE attempt=?', (attempt,))[0]
        cid = 'CK-' + secrets.token_hex(5)
        tx.q('INSERT INTO checkpoint(id, task, attempt, gen, seq, kind, cause, workspace, head_sha, dirty_digest, contract_rev, candidate, handoff_ref, body_sha, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (cid, t['id'], attempt, a['gen'], seq, kind, cause, workspace, head_sha, dirty_digest, contract_rev, candidate, handoff_ref, body_sha, tx.now))
        return _ok(tx, t, t['status'], t['status'], attempt=a['id'], gen=a['gen'], code='ok', message=cid)
    return _run(conn, 'checkpoint', actor, now, body)

def classify_death(conn, attempt, death, resumable_after=None, now=None, actor='reconcile'):
    """Mark a running attempt's death and move its task to waiting (S5-D3)."""
    if death not in ('usage_limit', 'api_error', 'lease_lost', 'crash'):
        return Result(False, 'invalid_field', 'death must be usage_limit/api_error/lease_lost/crash')
    if death == 'usage_limit' and resumable_after is None:
        resumable_after = (time.time() if now is None else now) + 3600

    def body(tx):
        a = tx.one('SELECT * FROM attempt WHERE id=?', (attempt,))
        if a is None:
            raise Refused('not_found', 'no such attempt %s' % attempt)
        t = tx.task(a['task'])
        if a['state'] != 'running':
            return _ok(tx, t, t['status'], t['status'], attempt=a['id'], gen=a['gen'], code='noop', message='attempt %s is %s' % (attempt, a['state']))
        tx.q("UPDATE attempt SET state='waiting', death=?, resumable_after=?, reason=? WHERE id=?", (death, resumable_after, death, attempt))
        tx.set_task(t, 'waiting', t['status'])
        return _ok(tx, t, t['status'], 'waiting', attempt=a['id'], gen=a['gen'])
    return _run(conn, 'classify_death', actor, now, body)

def next_task(conn, task, workspace_head=None, now=None):
    """The resume/continue/reopen decision for a waiting task (S5-D3). Read-only."""
    now = time.time() if now is None else now
    t = conn.execute('SELECT * FROM task WHERE id=?', (task,)).fetchone()
    if t is None:
        return {'action': 'none', 'reason': 'no such task'}
    a = conn.execute("SELECT * FROM attempt WHERE task=? AND state IN ('launching','running','waiting','submitted') ORDER BY gen DESC LIMIT 1", (task,)).fetchone()
    if a is None:
        return {'action': 'reopen', 'reason': 'no live attempt'}
    cp = conn.execute('SELECT * FROM checkpoint WHERE attempt=? ORDER BY seq DESC LIMIT 1', (a['id'],)).fetchone()
    if cp is None:
        return {'action': 'reopen', 'reason': 'no checkpoint and no dirty workspace'}
    if a['death']:
        if a['resumable_after'] is not None and now < a['resumable_after']:
            return {'action': 'none', 'reason': 'not_resumable'}
    if cp['contract_rev'] != t['contract_rev']:
        return {'action': 'continue', 'reason': 'contract_changed'}
    if workspace_head is not None and workspace_head != cp['head_sha']:
        return {'action': 'continue', 'reason': 'workspace_moved'}
    return {'action': 'resume', 'reason': 'checkpoint head unchanged'}

def continue_task(conn, task, nonce, workspace='', now=None, actor='cli'):
    """Continue a waiting task from its checkpoint: abandon the old attempt, reserve a new gen."""

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if frm != 'waiting':
            raise Refused('illegal_transition', 'Task %s is %s: only a waiting task continues.' % (t['id'], frm), task=t['id'], gen=t['gen'], from_state=frm)
        a = _live_attempt(tx, t['id'])
        if a is None:
            raise Refused('no_live_attempt', 'Task %s has no live attempt to continue.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        cp = tx.one('SELECT * FROM checkpoint WHERE attempt=? ORDER BY seq DESC LIMIT 1', (a['id'],))
        if cp is None:
            raise Refused('no_checkpoint', 'Task %s has no checkpoint: reopen it instead.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        if cp['contract_rev'] != t['contract_rev']:
            raise Refused('contract_changed', 'Task %s contract changed (checkpoint rev %s != %s): re-design before continuing, do not continue blind.' % (t['id'], cp['contract_rev'], t['contract_rev']), task=t['id'], gen=t['gen'], from_state=frm)
        if not isinstance(nonce, str) or not LAUNCH_RE.fullmatch(nonce):
            raise Refused('nonce_missing', 'continue needs a fresh Launch nonce', task=t['id'], gen=t['gen'], from_state=frm)
        tx.q("UPDATE attempt SET state='abandoned', reason='superseded', ended_at=? WHERE id=?", (tx.now, a['id']))
        gen = t['gen'] + 1
        aid = attempt_id_for(nonce)
        tx.q("INSERT INTO attempt(id, nonce, task, gen, role, lane, session, workspace, credential, state, created_at, lease_until) VALUES (?,?,?,?,?,?,?,?, 'nonce', 'launching', ?, ?)", (aid, nonce, t['id'], gen, a['role'], a['lane'], a['session'], workspace or a['workspace'], tx.now, tx.now + tx.launch_ttl))
        tx.q('UPDATE task SET gen=? WHERE id=?', (gen, t['id']))
        tx.set_task(t, 'reserved', frm)
        return _ok(tx, t, frm, 'reserved', attempt=aid, gen=gen)
    return _run(conn, 'continue', actor, now, body)

def fallback_provider(conn, task, alternate, model, authorize=False, now=None, actor='provider_fallback'):
    """provider fallback: stop the running provider and stage an authorized replacement.

    Safe handoff, first half — the worker must acknowledge the stop before anything is released:
      * default-off: an unauthorized call refuses and leaves the old execution running;
      * validates the continuation contract (live attempt of the current generation, a live engine
        assignment, an existing checkpoint whose contract_rev matches the task, a different provider);
      * stops the old execution by queueing a cancel intent (desired=cancelled) WITHOUT abandoning or
        releasing it — the slot is released only when the worker acknowledges the cancel;
      * does NOT create the replacement attempt: once the cancel is acknowledged the task reopens
        (cancel ack -> open), and the normal dispatch_ready reserves the new generation with the
        alternate provider and carries the checkpoint into the start manifest.

    Returns ok with message JSON {cancel_command, alternate_provider, alternate_model, checkpoint,
    head_sha, candidate, handoff_ref}. Every refusal leaves the old execution running and its
    assignment allocated (safely tracked), never half-released.
    """

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if authorize is not True:
            raise Refused('fallback_disabled', 'provider fallback is default-off: authorize the alternate provider before calling fallback_provider(..., authorize=True)', task=t['id'], gen=t['gen'], from_state=frm)
        for name, value in (('alternate', alternate), ('model', model)):
            if not isinstance(value, str) or not value.strip():
                raise Refused('invalid_field', 'provider fallback needs a non-empty %s' % name, task=t['id'], gen=t['gen'], from_state=frm)
        a = _live_attempt(tx, t['id'])
        if a is None:
            raise Refused('no_live_attempt', 'Task %s has no live attempt to fall back from.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        if a['gen'] != t['gen']:
            raise Refused('stale_generation', 'attempt %s is a stale generation.' % a['id'], task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        assignment = tx.one('SELECT * FROM engine_assignment WHERE attempt=?', (a['id'],))
        if assignment is None or assignment['slot_state'] == 'released':
            raise Refused('attempt_ended', 'attempt %s has no live engine assignment to stop.' % a['id'], task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if assignment['desired'] == 'cancelled':
            raise Refused('illegal_transition', 'attempt %s is already stopping.' % a['id'], task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if assignment['provider'] == alternate:
            raise Refused('same_provider', 'fallback requires a different provider (current: %s).' % assignment['provider'], task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        node = tx.one('SELECT config, enabled FROM engine_node WHERE id=?', (assignment['node'],))
        if node is None or not node['enabled']:
            raise Refused('node_unavailable', 'fallback node is unavailable', task=t['id'])
        try:
            providers = json.loads(node['config'])['providers']
            if not isinstance(providers, dict):
                raise ValueError('invalid providers')
            runtime = providers.get(alternate)
            if runtime is not None and (not isinstance(runtime, dict) or not isinstance(runtime.get('models'), list)):
                raise ValueError('invalid runtime')
        except (ValueError, TypeError, KeyError):
            raise Refused('invalid_node_configuration', 'fallback enrollment is corrupt', task=t['id'])
        if runtime is None or model not in runtime['models']:
            raise Refused('capability_unavailable', 'alternate runtime is not enrolled', task=t['id'])
        cp = tx.one('SELECT * FROM checkpoint WHERE attempt=? ORDER BY seq DESC LIMIT 1', (a['id'],))
        if cp is None:
            raise Refused('no_checkpoint', 'Task %s has no checkpoint: a provider fallback must resume from a checkpoint; reopen instead.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        if cp['contract_rev'] != t['contract_rev']:
            raise Refused('contract_changed', 'Task %s contract changed (checkpoint rev %s != %s): re-design before falling back.' % (t['id'], cp['contract_rev'], t['contract_rev']), task=t['id'], gen=t['gen'], from_state=frm)
        if not re.fullmatch('[a-f0-9]{40}', cp['head_sha']) or cp['dirty_digest'] or cp['body_sha']:
            raise Refused('checkpoint_unrestorable', 'commit workspace state before fallback', task=t['id'])
        tx.q("UPDATE engine_assignment SET desired='cancelled' WHERE attempt=?", (a['id'],))
        cid = 'C-' + secrets.token_hex(12)
        tx.q("INSERT INTO engine_command(id, attempt, operation, payload, state, created_at, ack_sha) VALUES (?,?, 'cancel', ?, 'pending', ?, NULL)", (cid, a['id'], json.dumps({'generation': a['gen']}), tx.now))
        fid = 'PF-' + secrets.token_hex(8)
        tx.q("INSERT INTO provider_fallback(id, task, old_attempt, alternate_provider, alternate_model, checkpoint, state, created_at) VALUES (?,?,?,?,?,?, 'pending', ?)", (fid, t['id'], a['id'], alternate, model, cp['id'], tx.now))
        _note_in_tx(tx, t['id'], 'provider fallback to %s/%s: old attempt %s cancel queued' % (alternate, model, a['id']))
        return _ok(tx, t, frm, frm, attempt=a['id'], gen=t['gen'], code='ok', message=json.dumps({'cancel_command': cid, 'handoff': fid, 'alternate_provider': alternate, 'alternate_model': model, 'checkpoint': cp['id'], 'head_sha': cp['head_sha'], 'candidate': cp['candidate'], 'handoff_ref': cp['handoff_ref']}))
    return _run(conn, 'fallback_provider', actor, now, body)

def abort_fallback(conn, task, now=None, actor='cli'):
    """Explicitly discard a stopped handoff so a redesigned task can start fresh."""

    def body(tx):
        t = _need_task(tx, task)
        handoff = tx.one("SELECT * FROM provider_fallback WHERE task=? AND state='pending'", (task,))
        if handoff is None:
            return _ok(tx, t, t['status'], t['status'], code='noop', message='no pending handoff')
        old = tx.one('SELECT a.state,e.slot_state FROM attempt a JOIN engine_assignment e ON e.attempt=a.id WHERE a.id=?', (handoff['old_attempt'],))
        if old is None or old['state'] != 'cancelled' or old['slot_state'] != 'released':
            raise Refused('handoff_not_stopped', 'wait for confirmed cancellation before discarding the handoff', task=task)
        unresolved = tx.one("SELECT count(*) FROM ext_op WHERE attempt=? AND state IN ('sending','unknown','conflict')", (handoff['old_attempt'],))[0]
        if unresolved:
            raise Refused('unreconciled_external_effects', 'reconcile external writes before discarding the handoff', task=task)
        tx.q("UPDATE provider_fallback SET state='cancelled', consumed_at=? WHERE id=?", (tx.now, handoff['id']))
        _note_in_tx(tx, task, 'provider fallback aborted; next dispatch starts fresh')
        return _ok(tx, t, t['status'], t['status'], message='handoff cancelled')
    return _run(conn, 'abort_fallback', actor, now, body)

def reconcile_runs(conn, classify, quiet_s=600, now=None):
    """Death detection (S5-D3): classify quiet running attempts via `classify` and mark deaths.

    `classify(attempt_id)` returns (death, resumable_after) or None; it reads the transcript tail
    and is injected so tests run a fake. Runs at most the read, outside any write transaction.
    """
    now = time.time() if now is None else now
    rows = conn.execute("SELECT a.id FROM attempt a JOIN run r ON r.attempt=a.id WHERE a.state='running' AND r.last_seen < ?", (now - quiet_s,)).fetchall()
    for r in rows:
        verdict = classify(r['id'])
        if verdict:
            classify_death(conn, r['id'], verdict[0], verdict[1] if len(verdict) > 1 else None, now=now)

def close(conn, task, status, now=None, actor='cli', note=None):
    if status == 'cancelled':
        return cancel(conn, task, now=now, actor=actor, note=note)

    def body(tx):
        if status != 'done':
            raise Refused('invalid_field', 'close --status must be done or cancelled', task=task)
        t = _need_task(tx, task)
        frm = t['status']
        if frm in TERMINAL:
            raise Refused('terminal', 'Task %s is already %s.' % (t['id'], frm), task=t['id'], gen=t['gen'], from_state=frm)
        if t['policy']:
            raise Refused('acceptance_required', 'Task %s is gated: accept its verified candidate with controller.py accept, not close --status done.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        a = _live_attempt(tx, t['id'])
        if frm not in TRANSITIONS['close_done']['from'] or a is None or a['state'] != 'submitted':
            raise Refused('illegal_transition', "Task %s is %s: only a submitted task closes as done (wait for the agent's DONE, or reopen/cancel it)." % (t['id'], frm), task=t['id'], gen=t['gen'], from_state=frm)
        tx.q("UPDATE attempt SET state='accepted', ended_at=? WHERE id=?", (tx.now, a['id']))
        tx.set_task(t, 'done', frm)
        _note_in_tx(tx, t['id'], note)
        return _ok(tx, t, frm, 'done', attempt=a['id'], gen=a['gen'])
    return _run(conn, 'close_done', actor, now, body)

def accept(conn, task, now=None, actor='pm'):
    """Accept a gated task (Stage 3): its verified candidate satisfies the policy."""
    from . import acceptance
    return acceptance.accept(conn, task, now=now, actor=actor)

def reject(conn, task, reason='', now=None, actor='pm'):
    """Reject a gated task's open candidate back to waiting (Stage 3)."""
    from . import acceptance
    return acceptance.reject(conn, task, reason=reason, now=now, actor=actor)

def cancel(conn, task, now=None, actor='cli', note=None):

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if frm in TERMINAL:
            raise Refused('terminal', 'Task %s is already %s.' % (t['id'], frm), task=t['id'], gen=t['gen'], from_state=frm)
        live = tx.q("SELECT id FROM attempt WHERE task=? AND state IN ('launching','running','waiting','submitted')", (t['id'],)).fetchall()
        for a in live:
            tx.q("UPDATE attempt SET state='cancelled', ended_at=? WHERE id=?", (tx.now, a['id']))
        tx.set_task(t, 'cancelled', frm)
        _note_in_tx(tx, t['id'], note)
        return _ok(tx, t, frm, 'cancelled', attempt=live[0]['id'] if live else None, gen=t['gen'])
    return _run(conn, 'cancel', actor, now, body)

def dispatch_attempt(conn, task, runtime_ref='', now=None, actor='dispatch', nonce=None, dispatch_context=None):

    def body(tx):
        t = _need_task(tx, task)
        _launch_checks(tx, t, None, 'pc-vm')
        if nonce is not None and (not isinstance(nonce, str) or not LAUNCH_RE.fullmatch(nonce)):
            raise Refused('nonce_missing', 'invalid prepared dispatch nonce')
        if dispatch_context is not None:
            from . import verification_contract as vc
            if dispatch_context.get('initial_contract') != vc.contract_digest(tx.conn, task):
                raise Refused('contract_changed', 'task changed while preparing dispatch')
        _check_text('runtime_ref', runtime_ref or '', MAX_FIELD)
        gen = t['gen'] + 1
        aid = legacy_attempt_id(t['id'], gen)
        try:
            tx.q("INSERT INTO attempt(id, nonce, task, gen, role, lane, credential, state, runtime_ref, created_at, started_at, lease_until) VALUES (?, ?, ?, ?, ?, 'pc-vm', 'dispatch', 'running', ?, ?, ?, NULL)", (aid, nonce or new_nonce(), t['id'], gen, t['role'], runtime_ref or '', tx.now, tx.now))
        except sqlite3.IntegrityError:
            raise Refused('busy', _busy_message(t, tx.launch_ttl), task=t['id'], gen=t['gen'], from_state=t['status'])
        tx.q("UPDATE task SET gen=?, ref=CASE WHEN ?='' THEN ref ELSE ? END WHERE id=?", (gen, runtime_ref or '', runtime_ref or '', t['id']))
        if dispatch_context is not None:
            frozen = json.loads(json.dumps(dispatch_context, allow_nan=False))
            frozen.pop('initial_contract', None)
            base = frozen['request']['base_sha']
            if not isinstance(base, str) or not re.fullmatch('[0-9a-f]{40}', base):
                raise Refused('invalid_field', 'invalid prepared dispatch base')
            tx.q('UPDATE task SET base_ref=? WHERE id=?', (base, task))
            frozen['request']['contract_digest'] = vc.contract_digest(tx.conn, task)
            frozen['inputs']['attempt_id'] = aid
            vc.put(tx.conn, 'agent-dispatch', aid, frozen)
        tx.set_task(t, 'running', t['status'])
        return _ok(tx, t, t['status'], 'running', attempt=aid, gen=gen)
    return _run(conn, 'dispatch', actor, now, body)

def bind_dispatch(conn, task, attempt, runtime_ref, now=None, actor='dispatch'):
    """Attach an external run once, fenced to the attempt reserved before the dispatch."""

    def body(tx):
        t = _need_task(tx, task)
        a = tx.one('SELECT * FROM attempt WHERE id=?', (attempt,))
        if a is None or a['task'] != task:
            raise Refused('attempt_not_found', 'No dispatch attempt for this task', task=task)
        if a['gen'] != t['gen']:
            raise Refused('stale_generation', 'Dispatch response belongs to a superseded attempt', task=task, attempt=attempt, gen=a['gen'])
        if a['state'] != 'running' or a['credential'] != 'dispatch' or t['status'] != 'running':
            raise Refused('attempt_ended', 'Dispatch attempt is no longer running', task=task, attempt=attempt)
        _check_text('runtime_ref', runtime_ref, MAX_FIELD, required=True)
        if a['runtime_ref'] and a['runtime_ref'] != runtime_ref:
            raise Refused('conflict', 'Dispatch attempt already names another external run', task=task, attempt=attempt)
        tx.q('UPDATE attempt SET runtime_ref=? WHERE id=?', (runtime_ref, attempt))
        tx.q('UPDATE task SET ref=?, rev=rev+1, updated_at=? WHERE id=?', (runtime_ref, iso_local(tx.now), task))
        tx.touch(task)
        return _ok(tx, t, t['status'], t['status'], attempt=attempt, gen=a['gen'])
    return _run(conn, 'bind_dispatch', actor, now, body)

def collect_result(conn, task, ok, now=None, actor='dispatch', *, attempt=None):

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if not attempt:
            raise Refused('credential_required', 'Collect requires the attempt id captured before reading the run.', task=t['id'], gen=t['gen'], from_state=frm)
        a = tx.one('SELECT * FROM attempt WHERE id=?', (attempt,))
        if a is not None and a['task'] != t['id']:
            raise Refused('task_mismatch', 'The collected attempt belongs to another task.', task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if a is not None and a['gen'] != t['gen']:
            raise Refused('stale_generation', 'The collected attempt was superseded; result ignored.', task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if a is None or a['credential'] != 'dispatch' or a['state'] != 'running' or (frm != 'running'):
            raise Refused('attempt_not_found', 'Task %s has no running dispatch attempt.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        if ok:
            tx.q("UPDATE attempt SET state='submitted' WHERE id=?", (a['id'],))
            to = 'submitted'
        else:
            tx.q("UPDATE attempt SET state='abandoned', reason='run_failed', ended_at=? WHERE id=?", (tx.now, a['id']))
            to = 'open'
        tx.set_task(t, to, frm)
        return _ok(tx, t, frm, to, attempt=a['id'], gen=a['gen'])
    return _run(conn, 'collect', actor, now, body)

def set_ref(conn, task, ref, now=None, actor='cli'):

    def body(tx):
        t = _need_task(tx, task)
        _check_text('ref', ref or '', MAX_FIELD)
        tx.q('UPDATE task SET ref=?, rev=rev+1, updated_at=? WHERE id=?', (ref or '', iso_local(tx.now), t['id']))
        tx.touch(t['id'])
        return _ok(tx, t, t['status'], t['status'], gen=t['gen'])
    return _run(conn, 'set_ref', actor, now, body)

def add_note(conn, task, text, now=None, actor='cli'):

    def body(tx):
        t = _need_task(tx, task)
        _check_text('note', ' '.join(str(text or '').split()), MAX_TEXT, required=True)
        tx.q('INSERT INTO task_note(task, ts, text) VALUES (?,?,?)', (t['id'], iso_local(tx.now), ' '.join(text.split())))
        tx.q('UPDATE task SET rev=rev+1 WHERE id=?', (t['id'],))
        tx.touch(t['id'])
        return _ok(tx, t, t['status'], t['status'], gen=t['gen'])
    return _run(conn, 'note', actor, now, body)
IMPORT_ATTEMPT_STATES = {'reserved': 'launching', 'running': 'running', 'waiting': 'waiting', 'submitted': 'submitted'}

def import_tasks(conn, records, recover=False, now=None, actor='import'):
    """Write migrated (or recovered) tasks. All or nothing; never overwrites an existing row (D4)."""
    report = {'imported': 0, 'skipped': 0, 'conflicts': [], 'code': 'ok', 'message': ''}
    source = 'recovered' if recover else 'migrated'

    def one(tx, rec):
        tid = rec.get('id')
        if not isinstance(tid, str) or not TASK_RE.fullmatch(tid):
            raise Refused('invalid_field', 'record id %r is not a task id' % (tid,))
        sha = rec.get('import_sha') or ''
        row = tx.task(tid)
        if row is not None:
            if row['import_sha'] and row['import_sha'] == sha:
                report['skipped'] += 1
            else:
                report['conflicts'].append(tid)
            return
        if _task_reservation_owner(tx, tid) is not None:
            raise Refused('invalid_field', 'task identifier is reserved by another operation')
        f = dict(slug=rec.get('slug'), goal=' '.join(str(rec.get('goal') or '').split()), role=rec.get('role'), owns=list(rec.get('owns') or []), done=list(rec.get('done') or []), lane=rec.get('lane') or 'local', depends=list(rec.get('depends') or []), kind=rec.get('kind') or 'task', repair_test=rec.get('repair_test') or '', depth=rec.get('depth', 1), parent=rec.get('parent') or 'pm', budget=rec.get('budget') or '', worktree=rec.get('worktree') or '', ref=rec.get('ref') or '')
        try:
            _validate_task_fields(f)
            status = rec.get('status')
            if status not in STATUSES:
                raise Refused('invalid_field', 'status %r is unknown' % (status,))
            notes = [(str(ts), ' '.join(str(text).split())) for ts, text in rec.get('notes') or []]
            for _, text in notes:
                _check_text('note', text, MAX_TEXT * 4)
        except Refused as r:
            raise Refused(r.code, '%s: %s' % (tid, r.message))
        att = rec.get('attempt')
        gen = rec.get('gen', 0)
        rev = rec.get('rev', 0)
        if any((not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in (gen, rev))):
            raise Refused('invalid_field', '%s: generation and revision must be nonnegative integers' % tid)
        expected = IMPORT_ATTEMPT_STATES.get(status)
        if expected is not None:
            if not isinstance(att, dict) or IMPORT_ATTEMPT_STATES.get(att.get('state'), att.get('state')) != expected:
                raise Refused('invalid_field', '%s: attempt must match task state %s' % (tid, status))
        elif att is not None:
            raise Refused('invalid_field', '%s: open/terminal tasks cannot carry a live attempt' % tid)
        if att and gen < 1:
            gen = 1
        created = rec.get('created_at') or iso_local(tx.now)
        tx.q('INSERT INTO task(id, slug, goal, role, lane, kind, repair_test, depth, parent, budget, worktree, ref, status, gen, created_at, updated_at, rev, source, import_sha) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (tid, f['slug'], f['goal'], f['role'], f['lane'], f['kind'], f['repair_test'], f['depth'], f['parent'], f['budget'], f['worktree'], f['ref'], status, gen, created, iso_local(tx.now), max(rev, 1), source, sha))
        _insert_children(tx, tid, f, notes)
        aid = None
        if att:
            astate = att.get('state')
            if astate in IMPORT_ATTEMPT_STATES:
                astate = IMPORT_ATTEMPT_STATES[astate]
            if astate not in LIVE:
                raise Refused('invalid_field', '%s: attempt state %r is not live' % (tid, astate))
            lease = att.get('lease_until')
            if lease is None and astate == 'launching':
                lease = tx.now + tx.launch_ttl
            elif lease is None and astate == 'running':
                lease = tx.now + tx.run_lease
            aid = legacy_attempt_id(tid, gen)
            tx.q("INSERT INTO attempt(id, nonce, task, gen, role, lane, credential, state, reason, created_at, started_at, lease_until) VALUES (?, NULL, ?, ?, ?, ?, 'legacy', ?, ?, ?, ?, ?)", (aid, tid, gen, f['role'], f['lane'], astate, att.get('reason') or '', tx.now, tx.now if astate != 'launching' else None, lease))
        _transition_row(tx, tid, aid, gen, 'import', None, status, True, source, actor)
        tx.touch(tid)
        tx.event('transition', task=tid, attempt=aid, gen=gen, cmd='import', actor=actor, lane=f['lane'], slug=f['slug'], **{'from': None, 'to': status})
        report['imported'] += 1

    def body(tx):
        tx.audit = False
        for rec in records:
            one(tx, rec)
        if report['conflicts']:
            ids = list(report['conflicts'])
            report.update(imported=0, skipped=0)
            raise Refused('conflict', 'conflicting rows (DB kept): ' + ', '.join(ids))
        return Result(True, 'ok', '')
    try:
        res = _run(conn, 'import', actor, now, body)
    except Unavailable:
        raise
    if not res.ok:
        report.update(code=res.code, message=res.message, imported=0, skipped=0 if res.code != 'conflict' else report['skipped'])
    return report

def set_projection(conn, task, rev, sha):
    """Record what the renderer wrote (rev + sha). Not a transition; no rev bump; no events, no render."""
    try:
        conn.execute('BEGIN IMMEDIATE')
        n = conn.execute('UPDATE task SET projection_sha=? WHERE id=? AND rev=?', (sha, task, rev)).rowcount
        conn.execute('COMMIT')
        return bool(n)
    except sqlite3.Error as e:
        try:
            if conn.in_transaction:
                conn.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        raise _classify(e)
ATTEMPT_COLS = ('id', 'gen', 'role', 'lane', 'state', 'reason', 'credential', 'session', 'workspace', 'agent_id', 'runtime_ref', 'start_seen', 'created_at', 'started_at', 'lease_until', 'ended_at')

def _attempt_view(a, now):
    d = {k: a[k] for k in ATTEMPT_COLS}
    d['expired'] = bool(a['state'] in ('launching', 'running') and a['lease_until'] is not None and (a['lease_until'] < now))
    return d

def _read(fn):

    def wrap(*a, **kw):
        try:
            return fn(*a, **kw)
        except sqlite3.Error as e:
            raise _classify(e)
    wrap.__name__, wrap.__doc__ = (fn.__name__, fn.__doc__)
    return wrap

@_read
def get(conn, task, now=None):
    now = time.time() if now is None else now
    t = conn.execute('SELECT * FROM task WHERE id=?', (task,)).fetchone()
    if t is None:
        return None
    d = {k: t[k] for k in t.keys()}
    d['owns'] = [r[0] for r in conn.execute('SELECT glob FROM task_owns WHERE task=? ORDER BY ord', (task,))]
    d['done'] = [r[0] for r in conn.execute('SELECT text FROM task_done WHERE task=? ORDER BY ord', (task,))]
    d['depends'] = [r[0] for r in conn.execute('SELECT dep FROM task_dep WHERE task=? ORDER BY dep', (task,))]
    d['notes'] = [{'ts': r[0], 'text': r[1]} for r in conn.execute('SELECT ts, text FROM task_note WHERE task=? ORDER BY id', (task,))]
    rows = conn.execute('SELECT * FROM attempt WHERE task=? ORDER BY gen', (task,)).fetchall()
    d['attempts'] = [_attempt_view(a, now) for a in rows]
    live = [a for a in rows if a['state'] == 'launching' and a['gen'] == t['gen']]
    d['reserved_at'] = iso_utc(live[0]['created_at']) if live and t['status'] == 'reserved' else ''
    return d

@_read
def list_tasks(conn, slug=None, include_done=False):
    q = 'SELECT * FROM task' + (' WHERE slug=?' if slug else '') + ' ORDER BY id'
    out = []
    for t in conn.execute(q, (slug,) if slug else ()).fetchall():
        if not include_done and t['status'] in TERMINAL:
            continue
        d = {k: t[k] for k in t.keys()}
        a = conn.execute("SELECT id, gen, state, agent_id, lease_until FROM attempt WHERE task=? AND state IN ('launching','running','waiting','submitted')", (t['id'],)).fetchone()
        d['live_attempt'] = dict(zip(a.keys(), tuple(a))) if a is not None else None
        out.append(d)
    return out

@_read
def open_deps(conn, task):
    return _open_deps(conn.execute, task)

@_read
def snapshot(conn, now=None):
    now = time.time() if now is None else now
    live = []
    for a in conn.execute("SELECT a.*, t.slug FROM attempt a JOIN task t ON t.id = a.task WHERE a.state IN ('launching','running','waiting','submitted') ORDER BY a.created_at").fetchall():
        live.append({'task': a['task'], 'slug': a['slug'], 'attempt': a['id'], 'gen': a['gen'], 'role': a['role'], 'state': a['state'], 'reason': a['reason'], 'credential': a['credential'], 'bound': a['agent_id'] is not None, 'start_seen': bool(a['start_seen']), 'lease_left_s': round(a['lease_until'] - now, 3) if a['lease_until'] is not None else None})
    unbound = [{'agent_id': r['agent_id'], 'agent_type': r['agent_type'], 'started_at': r['started_at'], 'last_seen': r['last_seen']} for r in conn.execute('SELECT * FROM run WHERE attempt IS NULL AND stopped_at IS NULL ORDER BY started_at').fetchall()]
    refusals = {}
    for code, n in conn.execute('SELECT code, count(*) FROM "transition" WHERE ok=0 AND ts >= ? GROUP BY code', (now - 86400,)):
        refusals[code] = n
    counts = {s: n for s, n in conn.execute('SELECT status, count(*) FROM task GROUP BY status')}
    meta = dict((tuple(r) for r in conn.execute('SELECT key, value FROM meta')))
    return {'live_attempts': live, 'unbound_runs': unbound, 'refusals_24h': refusals, 'counts': counts, 'health': {'schema': conn.execute('PRAGMA user_version').fetchone()[0], 'last_backup_at': meta.get('last_backup_at', ''), 'created_at': meta.get('created_at', ''), 'launch_ttl_s': float(meta.get('launch_ttl_s', 0)), 'run_lease_s': float(meta.get('run_lease_s', 0))}}

def _mode(p):
    try:
        return oct(os.stat(str(p)).st_mode & 511)
    except OSError:
        return None

def doctor(path=None, now=None):
    """(exit code, lines). 0 healthy, 1 problems, 3 unavailable."""
    now = time.time() if now is None else now
    p = Path(path) if path else db_path()
    try:
        conn = open_db('ro', CLI_TIMEOUT, p)
    except Unavailable as e:
        return (3, ['controller unavailable (%s): %s' % (e.code, e.detail)])
    lines, bad = ([], [])
    try:
        qc = conn.execute('PRAGMA quick_check').fetchone()[0]
        s = snapshot(conn, now)
        tasks = conn.execute('SELECT id, slug, rev FROM task').fetchall()
    except (sqlite3.Error, Unavailable) as e:
        conn.close()
        e2 = e if isinstance(e, Unavailable) else _classify(e)
        return (3, ['controller unavailable (%s): %s' % (e2.code, e2.detail)])
    conn.close()
    lines.append('schema %d, quick_check %s' % (s['health']['schema'], qc))
    if qc != 'ok':
        bad.append('integrity')
    for suf in ('', '-wal', '-shm'):
        m = _mode(str(p) + suf)
        if m is not None and m != '0o600':
            bad.append('mode')
            lines.append('file %s%s has mode %s, expected 0o600' % (p.name, suf, m))
    lines.append('db %d bytes; launch_ttl_s %s, run_lease_s %s' % (os.path.getsize(str(p)), s['health']['launch_ttl_s'], s['health']['run_lease_s']))
    lines.append('tasks: ' + (', '.join(('%s %d' % kv for kv in sorted(s['counts'].items()))) or 'none'))
    lines.append('live attempts %d (expired, pending the next write: %d); unbound runs %d' % (len(s['live_attempts']), sum((1 for a in s['live_attempts'] if a['lease_left_s'] is not None and a['lease_left_s'] < 0)), len(s['unbound_runs'])))
    if s['refusals_24h']:
        lines.append('refusals 24h: ' + ', '.join(('%s %d' % kv for kv in sorted(s['refusals_24h'].items()))))
    lb = s['health']['last_backup_at']
    age = (now - _epoch(lb)) / 3600.0 if lb and _epoch(lb) is not None else None
    lines.append('last backup: ' + ('%s (%.1f h ago)' % (lb, age) if age is not None else 'never'))
    stale = 0
    for t in tasks:
        f = p.parent / t['slug'] / 'tasks' / (t['id'] + '.md')
        try:
            m = REVISION_LINE.search(f.read_text(encoding='utf-8', errors='replace'))
        except OSError:
            m = None
        if not m or int(m.group(1)) != t['rev']:
            stale += 1
    lines.append('stale projections %d (controller.py render --all repairs)' % stale)
    return (1 if bad else 0, lines)

def backup(dest=None, path=None, now=None):
    """(exit code, message). Online backup API, destination 0600; records meta last_backup_at."""
    now = time.time() if now is None else now
    p = Path(path) if path else db_path()
    if not p.exists():
        return (0, 'no controller.db: nothing to back up')
    d = Path(dest) if dest else Path.home() / '.stage34' / 'backups'
    created = not d.exists()
    d.mkdir(parents=True, exist_ok=True)
    if created:
        os.chmod(str(d), 448)
    stamp = dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = d / ('orch-controller-%s.db' % stamp)
    k = 1
    while out.exists():
        out = d / ('orch-controller-%s-%d.db' % (stamp, k))
        k += 1
    try:
        src = open_db('rw', CLI_TIMEOUT, p)
    except Unavailable as e:
        return (3, 'controller unavailable (%s): %s' % (e.code, e.detail))
    fd = os.open(str(out), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
    os.close(fd)
    try:
        dst = sqlite3.connect(str(out))
        src.backup(dst)
        dst.close()
        os.chmod(str(out), 384)
        src.execute('BEGIN IMMEDIATE')
        src.execute("INSERT INTO meta(key, value) VALUES ('last_backup_at', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (iso_utc(now),))
        src.execute('COMMIT')
    except sqlite3.Error as e:
        try:
            if src.in_transaction:
                src.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        src.close()
        return (3, 'backup failed (%s)' % _classify(e).code)
    src.close()
    return (0, 'backup written: %s' % out)

def _fmt_ts(v):
    return iso_utc(v) if isinstance(v, (int, float)) else v or '-'

def _show(conn, tid, now):
    g = get(conn, tid, now)
    if g is None:
        return (1, ['not_found: no task %s' % tid])
    lines = ['%s  %s  %s  %s  gen %d  rev %d  (%s)' % (g['id'], g['status'], g['lane'], g['role'], g['gen'], g['rev'], g['source']), 'feature %s; goal: %s' % (g['slug'], g['goal'][:200]), 'owns: ' + ', '.join(g['owns']), 'depends: ' + (', '.join(g['depends']) or '-'), 'attempts:']
    for a in g['attempts']:
        lines.append('  %s gen %d %s%s %s agent=%s lease=%s%s' % (a['id'], a['gen'], a['state'], '(' + a['reason'] + ')' if a['reason'] else '', a['credential'], a['agent_id'] or 'unbound', _fmt_ts(a['lease_until']), ' EXPIRED' if a['expired'] else ''))
    return (0, lines)

def _attempts(conn, live_only, now):
    q = 'SELECT * FROM attempt' + (" WHERE state IN ('launching','running','waiting','submitted')" if live_only else '') + ' ORDER BY created_at'
    lines = []
    for a in conn.execute(q).fetchall():
        v = _attempt_view(a, now)
        left = '-' if a['lease_until'] is None else '%ds' % int(a['lease_until'] - now)
        lines.append('%s %s gen %d %-9s %-8s %-24s %-8s lease_left=%s%s' % (v['id'], a['task'], v['gen'], v['state'], v['credential'], v['role'], 'bound' if v['agent_id'] else 'unbound', left, ' EXPIRED' if v['expired'] else ''))
    return lines

def sdk_transition_in_tx(tx, task, attempt, generation, operation, native_session=None):
    """Trusted transport confirmation; provider completion alone never accepts work."""
    t = _need_task(tx, task)
    a = tx.one('SELECT * FROM attempt WHERE id=? AND task=?', (attempt, task))
    if a is None or a['gen'] != generation or t['gen'] != generation:
        raise Refused('stale_generation', 'SDK command belongs to a superseded attempt', task=task)
    if a['state'] not in LIVE:
        raise Refused('attempt_ended', 'SDK attempt is no longer live', task=task)
    frm = t['status']
    if operation == 'start':
        if a['state'] != 'launching' or frm != 'reserved':
            raise Refused('illegal_transition', 'SDK attempt is not launching', task=task)
        if not isinstance(native_session, str) or not re.fullmatch('[a-zA-Z0-9_.:-]{1,128}', native_session):
            raise Refused('invalid_field', 'SDK start requires native session identity', task=task)
        tx.q("UPDATE attempt SET state='running',start_seen=1,started_at=?,runtime_ref=?,lease_until=? WHERE id=?", (tx.now, native_session, tx.now + tx.run_lease, attempt))
        tx.q('UPDATE launch SET started_by=? WHERE attempt=?', ('sdk:' + attempt, attempt))
        to = 'running'
    elif operation == 'pause':
        if a['state'] != 'running' or frm != 'running':
            raise Refused('illegal_transition', 'SDK attempt is not running', task=task)
        tx.q("UPDATE attempt SET state='waiting',reason='sdk_paused',lease_until=NULL WHERE id=?", (attempt,))
        to = 'waiting'
    elif operation in ('resume', 'reply'):
        if a['state'] != 'waiting' or frm != 'waiting':
            raise Refused('illegal_transition', 'SDK attempt is not paused', task=task)
        tx.q("UPDATE attempt SET state='running',reason='',lease_until=? WHERE id=?", (tx.now + tx.run_lease, attempt))
        to = 'running'
    elif operation == 'cancel':
        tx.q("UPDATE attempt SET state='cancelled',reason='sdk_cancelled',ended_at=?,lease_until=NULL WHERE id=?", (tx.now, attempt))
        to = 'open'
    else:
        raise Refused('invalid_field', 'unknown SDK transition', task=task)
    tx.set_task(t, to, frm)
    return _ok(tx, t, frm, to, attempt=attempt, gen=generation)

def main(argv=None):
    ap = argparse.ArgumentParser(prog='controller.py')
    sub = ap.add_subparsers(dest='cmd')
    i = sub.add_parser('init')
    i.add_argument('--launch-ttl', type=float, default=LAUNCH_TTL_S)
    i.add_argument('--run-lease', type=float, default=RUN_LEASE_S)
    sub.add_parser('doctor')
    sub.add_parser('migrate')
    abort = sub.add_parser('fallback-abort')
    abort.add_argument('task')
    ac = sub.add_parser('accept')
    ac.add_argument('task')
    rj = sub.add_parser('reject')
    rj.add_argument('task')
    rj.add_argument('--reason', required=True)
    b = sub.add_parser('backup')
    b.add_argument('--dest')
    s = sub.add_parser('show')
    s.add_argument('task')
    at = sub.add_parser('attempts')
    at.add_argument('--live', action='store_true')
    for name in ('render', 'compare', 'import'):
        sub.add_parser(name, add_help=False)
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ('render', 'compare', 'import'):
        try:
            proj = _projection()
        except Exception as e:
            print('projection.py failed to load (%s)' % type(e).__name__, file=sys.stderr)
            return 3
        if proj is None or not hasattr(proj, 'main'):
            print('projection.py is not installed: render/compare/import are unavailable', file=sys.stderr)
            return 3
        return int(proj.main(argv) or 0)
    try:
        a = ap.parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    if not a.cmd:
        ap.print_usage(sys.stderr)
        return 2
    if a.cmd == 'init':
        r = init_db(launch_ttl_s=a.launch_ttl, run_lease_s=a.run_lease)
        print(('' if r.ok else r.code + ': ') + r.message, file=sys.stdout if r.ok else sys.stderr)
        return 0 if r.ok else 1
    if a.cmd == 'doctor':
        code, lines = doctor()
        print('\n'.join(lines), file=sys.stdout if code != 3 else sys.stderr)
        return code
    if a.cmd == 'migrate':
        r = migrate_db()
        print((r.code + ': ' if not r.ok else '') + r.message, file=sys.stdout if r.ok else sys.stderr)
        return 0 if r.ok else 1
    if a.cmd in ('accept', 'reject', 'fallback-abort'):
        try:
            conn = open_db('rw', CLI_TIMEOUT)
        except Unavailable as e:
            print('controller unavailable (%s): %s' % (e.code, e.detail), file=sys.stderr)
            return 3
        try:
            r = abort_fallback(conn, a.task) if a.cmd == 'fallback-abort' else accept(conn, a.task) if a.cmd == 'accept' else reject(conn, a.task, a.reason)
            print((r.code + ': ' if not r.ok else '') + r.message, file=sys.stdout if r.ok else sys.stderr)
            return 0 if r.ok else 1
        finally:
            conn.close()
    if a.cmd == 'backup':
        code, msg = backup(a.dest)
        print(msg, file=sys.stdout if code == 0 else sys.stderr)
        return code
    try:
        conn = open_db('ro', CLI_TIMEOUT)
    except Unavailable as e:
        print('controller unavailable (%s): %s' % (e.code, e.detail), file=sys.stderr)
        return 3
    try:
        now = time.time()
        if a.cmd == 'show':
            code, lines = _show(conn, a.task, now)
            print('\n'.join(lines), file=sys.stdout if code == 0 else sys.stderr)
            return code
        print('\n'.join(_attempts(conn, a.live, now)) or 'no attempts')
        return 0
    except Unavailable as e:
        print('controller unavailable (%s): %s' % (e.code, e.detail), file=sys.stderr)
        return 3
    finally:
        conn.close()
if __name__ == '__main__':
    sys.exit(main())
