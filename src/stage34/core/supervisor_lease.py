if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import hmac
import math
import re
import secrets
import sqlite3
import time

class LeaseError(Exception):

    def __init__(self, code):
        self.code = code
        super().__init__(code)

@dataclass(frozen=True)
class Credential:
    holder: str
    provider: str
    generation: int
    secret: str = field(repr=False)

@contextmanager
def _write(conn):
    if conn.in_transaction:
        raise LeaseError('transaction_already_open')
    try:
        conn.execute('BEGIN IMMEDIATE')
        yield
        conn.execute('COMMIT')
    except BaseException as error:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        if isinstance(error, sqlite3.Error):
            code = 'unavailable_locked' if 'locked' in str(error).lower() else 'unavailable_schema'
            raise LeaseError(code) from error
        raise

def install(conn):
    """Explicit, atomic additive migration; never changes controller user_version."""
    with _write(conn):
        existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'supervisor_%'")}
        if existing:
            if existing != {'supervisor_schema', 'supervisor_lease', 'supervisor_audit'}:
                raise LeaseError('unavailable_schema')
            version = conn.execute('SELECT version FROM supervisor_schema WHERE id=1').fetchone()
            if version is None or version[0] != 1:
                raise LeaseError('unavailable_schema')
            return
        conn.execute('CREATE TABLE supervisor_schema(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)')
        conn.execute('INSERT INTO supervisor_schema VALUES(1,1)')
        conn.execute("CREATE TABLE supervisor_lease (id INTEGER PRIMARY KEY CHECK(id=1), holder TEXT NOT NULL, provider TEXT NOT NULL CHECK(provider IN ('claude','codex')), generation INTEGER NOT NULL CHECK(generation>0), credential_sha TEXT NOT NULL, lease_until REAL NOT NULL, updated_at REAL NOT NULL)")
        conn.execute('CREATE TABLE supervisor_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, operation TEXT NOT NULL, holder TEXT NOT NULL, provider TEXT NOT NULL, generation INTEGER NOT NULL)')

def _now(now):
    value = time.time() if now is None else now
    if isinstance(value, bool) or not isinstance(value, (int, float)) or (not math.isfinite(value)):
        raise LeaseError('invalid_time')
    return value

def _identity(holder, provider):
    if not isinstance(holder, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}', holder):
        raise LeaseError('invalid_holder')
    if provider not in ('claude', 'codex'):
        raise LeaseError('capability_unavailable')

def _ttl(ttl):
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or (not math.isfinite(ttl)) or (not 1 <= ttl <= 300):
        raise LeaseError('invalid_ttl')

def _row(conn):
    try:
        schema = conn.execute('SELECT version FROM supervisor_schema WHERE id=1').fetchone()
        if schema is None or schema[0] != 1:
            raise LeaseError('unavailable_schema')
        cur = conn.execute('SELECT holder,provider,generation,credential_sha,lease_until,updated_at FROM supervisor_lease WHERE id=1')
        row = cur.fetchone()
        return dict(zip([column[0] for column in cur.description], row)) if row is not None else None
    except sqlite3.Error as error:
        raise LeaseError('unavailable_schema') from error

def _hash(secret):
    if not isinstance(secret, str) or not re.fullmatch('[a-zA-Z0-9_-]{43}', secret):
        raise LeaseError('invalid_credential')
    return hashlib.sha256(secret.encode('ascii')).hexdigest()

def _clock(row, now):
    if row and (not math.isfinite(row['lease_until']) or not math.isfinite(row['updated_at'])):
        raise LeaseError('unavailable_schema')
    if row and now < row['updated_at']:
        raise LeaseError('clock_regression')

def assert_current(conn, credential, now=None):
    """Check an exclusive SQLite transaction's lease fence. Never does a preflight."""
    if not conn.in_transaction:
        raise LeaseError('transaction_required')
    if not isinstance(credential, Credential):
        raise LeaseError('invalid_credential')
    stamp = _now(now)
    row = _row(conn)
    _clock(row, stamp)
    digest = _hash(credential.secret)
    if row is None or row['generation'] != credential.generation or row['holder'] != credential.holder or (row['provider'] != credential.provider) or (not hmac.compare_digest(row['credential_sha'], digest)):
        raise LeaseError('stale_supervisor')
    if row['lease_until'] <= stamp:
        raise LeaseError('supervisor_expired')
    return row

def _audit(conn, now, operation, holder, provider, generation):
    conn.execute('INSERT INTO supervisor_audit(ts,operation,holder,provider,generation) VALUES (?,?,?,?,?)', (now, operation, holder, provider, generation))

def _assign(conn, holder, provider, generation, stamp, ttl, operation):
    secret = secrets.token_urlsafe(32)
    conn.execute('INSERT INTO supervisor_lease VALUES(1,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET holder=excluded.holder,provider=excluded.provider,generation=excluded.generation,credential_sha=excluded.credential_sha,lease_until=excluded.lease_until,updated_at=excluded.updated_at', (holder, provider, generation, _hash(secret), stamp + ttl, stamp))
    _audit(conn, stamp, operation, holder, provider, generation)
    return Credential(holder, provider, generation, secret)

def _persist_before_commit(conn, credential, ttl, stamp, now, persist):
    if persist is None:
        return
    if not callable(persist) or persist(credential) is not None:
        raise LeaseError('credential_persist_failed')
    ready = _now(now)
    if ready < stamp:
        raise LeaseError('clock_regression')
    conn.execute('UPDATE supervisor_lease SET lease_until=?,updated_at=? WHERE id=1', (ready + ttl, ready))

def acquire(conn, holder, provider, ttl=60, now=None, *, persist=None):
    _identity(holder, provider)
    _ttl(ttl)
    with _write(conn):
        stamp = _now(now)
        row = _row(conn)
        _clock(row, stamp)
        if row and row['lease_until'] > stamp:
            raise LeaseError('supervisor_busy')
        credential = _assign(conn, holder, provider, row['generation'] + 1 if row else 1, stamp, ttl, 'acquire')
        _persist_before_commit(conn, credential, ttl, stamp, now, persist)
        return credential

def renew(conn, credential, ttl=60, now=None):
    _ttl(ttl)
    with _write(conn):
        stamp = _now(now)
        row = assert_current(conn, credential, stamp)
        conn.execute('UPDATE supervisor_lease SET lease_until=?,updated_at=? WHERE id=1', (stamp + ttl, stamp))
        _audit(conn, stamp, 'renew', row['holder'], row['provider'], row['generation'])
    return credential

def handoff(conn, credential, holder, provider, ttl=60, now=None, *, persist=None):
    _identity(holder, provider)
    _ttl(ttl)
    with _write(conn):
        stamp = _now(now)
        row = assert_current(conn, credential, stamp)
        target = _assign(conn, holder, provider, row['generation'] + 1, stamp, ttl, 'handoff')
        _persist_before_commit(conn, target, ttl, stamp, now, persist)
        return target

def release(conn, credential, now=None):
    with _write(conn):
        stamp = _now(now)
        row = assert_current(conn, credential, stamp)
        conn.execute('UPDATE supervisor_lease SET lease_until=?,updated_at=? WHERE id=1', (stamp, stamp))
        _audit(conn, stamp, 'release', row['holder'], row['provider'], row['generation'])

def status(conn, now=None):
    """Public status omits the credential hash as well as the raw credential."""
    stamp = _now(now)
    row = _row(conn)
    _clock(row, stamp)
    if row is None:
        return {'active': False, 'generation': 0}
    return {key: row[key] for key in ('holder', 'provider', 'generation', 'lease_until')} | {'active': row['lease_until'] > stamp}
