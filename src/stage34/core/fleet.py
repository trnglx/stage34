if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import json
import math
import re
import sqlite3
import time
from pathlib import Path
SCHEMA_VERSION = 1
PLATFORMS = ('darwin', 'linux', 'windows')
MAX_MACHINES = 64
MAX_CONCURRENCY = 64
MAX_CAPABILITIES = 32
ID_RE = re.compile('^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$')
TRANSPORT_RE = re.compile('^[a-z][a-z0-9-]{0,63}$')
CAPABILITY_RE = re.compile('^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$')
_CREDENTIAL_PATTERNS = (re.compile('-----BEGIN [A-Z ]*PRIVATE KEY-----'), re.compile('ssh-(rsa|ed25519|ecdsa) '), re.compile('\\b(sk-ant-|ghp_|gho_|ghs_|github_pat_|AKIA[0-9A-Z]{16})'), re.compile('\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b'), re.compile('(password|passwd|secret|token|api[_-]?key)\\s*[:=]', re.I))
FLEET_DDL = '\nCREATE TABLE fleet_schema(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);\nINSERT INTO fleet_schema VALUES(1,1);\nCREATE TABLE fleet_machine(\n  id TEXT PRIMARY KEY,\n  platform TEXT NOT NULL,\n  transport TEXT NOT NULL,\n  capabilities TEXT NOT NULL,\n  concurrency INTEGER NOT NULL CHECK(concurrency>0),\n  enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),\n  enrolled_at REAL NOT NULL,\n  last_seen REAL\n);\n'
REQUIRED_MACHINE_KEYS = {'platform', 'transport', 'capabilities', 'enabled', 'concurrency'}

class FleetError(Exception):

    def __init__(self, code, message=''):
        self.code = code
        super().__init__(code if not message else code + ': ' + message)

def _now(now):
    value = time.time() if now is None else now
    if isinstance(value, bool) or not isinstance(value, (int, float)) or (not math.isfinite(value)):
        raise FleetError('invalid_time')
    return value

def _credential_problem(value):
    """A non-empty reason when `value` carries a credential-shaped string, else ''."""
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
    for pattern in _CREDENTIAL_PATTERNS:
        if pattern.search(text):
            return 'credential-shaped value'
    return ''

def validate(machines):
    """Problems for a fleet `machines` list; [] when valid. Detects duplicate ids.

    Error text references entry indexes and field names only — never the supplied values — so a
    misplaced credential is never echoed into diagnostics (R3).
    """
    problems = []
    if not isinstance(machines, list) or not machines or len(machines) > MAX_MACHINES:
        return ['machines must be a non-empty list of at most %d entries' % MAX_MACHINES]
    seen = {}
    for index, spec in enumerate(machines):
        if not isinstance(spec, dict):
            problems.append('machine at index %d must be an object' % index)
            continue
        mid = spec.get('id')
        if not isinstance(mid, str) or not ID_RE.fullmatch(mid):
            problems.append('machine at index %d has a bad id' % index)
            continue
        if mid in seen:
            problems.append('duplicate machine id (index %d repeats index %d)' % (index, seen[mid]))
        seen[mid] = index
        if set(spec) != REQUIRED_MACHINE_KEYS | {'id'}:
            problems.append('machine at index %d must have exactly id/platform/transport/capabilities/enabled/concurrency' % index)
            continue
        if spec['platform'] not in PLATFORMS:
            problems.append('machine at index %d has unknown platform' % index)
        if not isinstance(spec['transport'], str) or not TRANSPORT_RE.fullmatch(spec['transport']):
            problems.append('machine at index %d has a bad transport reference' % index)
        caps = spec['capabilities']
        if not isinstance(caps, list) or not caps or len(caps) > MAX_CAPABILITIES or any((not isinstance(c, str) or not CAPABILITY_RE.fullmatch(c) for c in caps)):
            problems.append('machine at index %d has bad capabilities' % index)
        if not isinstance(spec['enabled'], bool):
            problems.append('machine at index %d enabled must be a boolean' % index)
        concurrency = spec['concurrency']
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or (not 1 <= concurrency <= MAX_CONCURRENCY):
            problems.append('machine at index %d concurrency must be an integer 1..%d' % (index, MAX_CONCURRENCY))
        if _credential_problem(spec):
            problems.append('machine at index %d carries a credential-shaped value' % index)
    return problems

def load(path):
    """Read and validate a fleet config file; returns the validated `machines` list."""
    try:
        data = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError) as error:
        raise FleetError('invalid_json') from error
    if not isinstance(data, dict) or set(data) != {'schema_version', 'machines'} or data.get('schema_version') != SCHEMA_VERSION:
        raise FleetError('invalid_fleet_config')
    problems = validate(data['machines'])
    if problems:
        raise FleetError('invalid_fleet_config', '; '.join(problems[:3]))
    return data['machines']

def _schema(conn):
    """Check the fleet schema is installed at the expected version (no commit)."""
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'fleet_%'")}
    if names != {'fleet_schema', 'fleet_machine'}:
        raise FleetError('unavailable_schema')
    row = conn.execute('SELECT version FROM fleet_schema WHERE id=1').fetchone()
    if row is None or row[0] != SCHEMA_VERSION:
        raise FleetError('unavailable_schema')

def _begin(conn):
    if conn.in_transaction:
        raise FleetError('transaction_already_open')
    conn.execute('BEGIN IMMEDIATE')

def install(conn):
    """Additive schema; never changes controller/engine user_version or existing tables."""
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'fleet_%'")}
    if names:
        _schema(conn)
        return
    _begin(conn)
    try:
        for stmt in FLEET_DDL.split(';'):
            if stmt.strip():
                conn.execute(stmt)
        conn.execute('COMMIT')
    except BaseException:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise

def register(conn, machines, now=None):
    """Upsert the validated fleet list into `fleet_machine` (enrollment)."""
    problems = validate(machines)
    if problems:
        raise FleetError('invalid_fleet_config', '; '.join(problems[:3]))
    stamp = _now(now)
    install(conn)
    _begin(conn)
    try:
        for spec in machines:
            conn.execute('INSERT INTO fleet_machine(id, platform, transport, capabilities, concurrency, enabled, enrolled_at, last_seen) VALUES(?,?,?,?,?,?,?,NULL) ON CONFLICT(id) DO UPDATE SET platform=excluded.platform, transport=excluded.transport, capabilities=excluded.capabilities, concurrency=excluded.concurrency, enabled=excluded.enabled', (spec['id'], spec['platform'], spec['transport'], json.dumps(spec['capabilities'], sort_keys=True), spec['concurrency'], 1 if spec['enabled'] else 0, stamp))
        conn.execute('COMMIT')
    except BaseException:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise

def retire(conn, machine_id):
    """Disable a registered machine (removal from rotation; never touches engine_node)."""
    if not ID_RE.fullmatch(str(machine_id)):
        raise FleetError('invalid_machine_id')
    install(conn)
    _begin(conn)
    try:
        if conn.execute('UPDATE fleet_machine SET enabled=0 WHERE id=?', (machine_id,)).rowcount != 1:
            raise FleetError('machine_not_found')
        conn.execute('COMMIT')
    except BaseException:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise

def remove(conn, machine_id):
    """Delete a machine registration (full removal; never touches engine_node)."""
    if not ID_RE.fullmatch(str(machine_id)):
        raise FleetError('invalid_machine_id')
    install(conn)
    _begin(conn)
    try:
        if conn.execute('DELETE FROM fleet_machine WHERE id=?', (machine_id,)).rowcount != 1:
            raise FleetError('machine_not_found')
        conn.execute('COMMIT')
    except BaseException:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise

def _entry(mid, reg, node, stamp):
    registered = reg is not None
    if not registered and _credential_problem(str(mid)):
        mid = '[redacted-id]'
    platform = reg['platform'] if registered else 'unknown'
    transport = reg['transport'] if registered else 'unknown'
    capabilities = json.loads(reg['capabilities']) if registered else []
    runtime_enabled = node is None or bool(node['enabled'])
    if registered:
        enabled = bool(reg['enabled']) and runtime_enabled
    else:
        enabled = bool(node['enabled'])
    concurrency = reg['concurrency'] if registered else None
    slots = None
    node_config = {}
    if node is not None:
        try:
            node_config = json.loads(node['config'])
            slots = node_config.get('slots')
            if concurrency is None:
                concurrency = slots
        except (ValueError, TypeError, AttributeError):
            node_config = {}
    last_seen = node['last_seen'] if node is not None else None
    pressure = node['pressure'] if node is not None else None
    age_raw = stamp - last_seen if last_seen is not None else None
    age = round(age_raw, 1) if age_raw is not None else None
    disabled = not enabled
    if disabled:
        status = 'disabled'
    elif node is None or last_seen is None:
        status = 'unknown'
    else:
        ttl = node_config.get('heartbeat_ttl', 0)
        if age_raw < 0:
            status = 'unknown'
        elif age_raw > ttl:
            status = 'offline'
        elif pressure is not None and node_config.get('max_pressure') is not None and (pressure >= node_config['max_pressure']):
            status = 'overloaded'
        else:
            status = 'healthy'
    return {'id': mid, 'platform': platform, 'transport': transport, 'capabilities': capabilities, 'concurrency': concurrency, 'slots': slots, 'enabled': enabled, 'registered': registered, 'status': status, 'last_seen': last_seen, 'age_s': age, 'pressure': pressure}

def snapshot(conn, now=None):
    """Read-only fleet view merging registrations with `engine_node` runtime health."""
    stamp = _now(now)
    try:
        regs = conn.execute('SELECT * FROM fleet_machine ORDER BY id').fetchall()
    except sqlite3.OperationalError:
        regs = []
    try:
        nodes = conn.execute('SELECT id, config, enabled, last_seen, pressure FROM engine_node').fetchall()
    except sqlite3.OperationalError:
        nodes = []
    node_by_id = {n['id']: n for n in nodes}
    out = [_entry(r['id'], r, node_by_id.get(r['id']), stamp) for r in regs]
    seen = {r['id'] for r in regs}
    for node in nodes:
        if node['id'] not in seen:
            out.append(_entry(node['id'], None, node, stamp))
    return out
