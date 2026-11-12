if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import json
import time
CONTRACT_VERSION = 1
REQUIRED_KEYS = {'claude-local': ('agent_id', 'session_id', 'agent_type'), 'pc-vm': (), 'cloud': ()}
ADAPTERS = {'claude-local': {'contract_version': 1, 'status': 'conformant', 'launch': 'reserve', 'identity': 'prompt', 'binding': 'single/unbound/proof/transcript', 'liveness': 'lanerules+transcript'}, 'pc-vm': {'contract_version': 1, 'status': 'unchecked', 'launch': 'gh_dispatch_agent', 'identity': 'op+prompt', 'binding': 'dispatch', 'liveness': 'gh_run_view'}, 'cloud': {'contract_version': 1, 'status': 'unavailable', 'launch': '-', 'identity': '-', 'binding': '-', 'liveness': '-'}}
STATIC_UNAVAILABLE = frozenset((name for name, caps in ADAPTERS.items() if caps['status'] == 'unavailable'))

def status_of(conn, name):
    """The adapter's current status: the live table row, else the static default."""
    row = conn.execute('SELECT status FROM adapter WHERE name=?', (name,)).fetchone()
    if row is not None:
        return row['status']
    caps = ADAPTERS.get(name)
    return caps['status'] if caps else 'unchecked'

def is_unavailable(conn, name):
    """True when the adapter must refuse builders (static `unavailable`, or a recorded one)."""
    if name in STATIC_UNAVAILABLE:
        return True
    return status_of(conn, name) == 'unavailable'

def check_payload(name, payload_keys):
    """True when the recorded payload_keys (comma-separated) cover the adapter's required keys."""
    required = REQUIRED_KEYS.get(name, ())
    have = set((payload_keys or '').split(','))
    return set(required) <= have

def upsert_adapter(conn, name, status, runtime_version='', now=None):
    now = time.time() if now is None else now
    caps = ADAPTERS.get(name, {'contract_version': CONTRACT_VERSION})
    caps_json = json.dumps(caps, sort_keys=True)
    existing = conn.execute('SELECT 1 FROM adapter WHERE name=?', (name,)).fetchone()
    if existing is None:
        conn.execute('INSERT INTO adapter(name, contract_version, status, capabilities, checked_at, runtime_version) VALUES (?,?,?,?,?,?)', (name, CONTRACT_VERSION, status, caps_json, now, runtime_version))
    else:
        conn.execute('UPDATE adapter SET status=?, capabilities=?, checked_at=?, runtime_version=? WHERE name=?', (status, caps_json, now, runtime_version, name))

def conformance(conn, name, payload_keys=None):
    """Conformance verdict for an adapter, optionally from a start event's payload_keys.

    Returns (status, reason). `cloud` is always unavailable; a provided payload_keys set decides
    `conformant`/`degraded` for the required-key adapters; otherwise the adapter's recorded status.
    """
    if name in STATIC_UNAVAILABLE:
        return ('unavailable', 'static')
    if name == 'pc-vm':
        row = conn.execute("SELECT value FROM meta WHERE key='adapter:pc-vm:drill'").fetchone()
        try:
            evidence = json.loads(row[0]) if row else {}
            if evidence.get('workflow_op_input') is True and evidence.get('conclusion') == 'success' and (0 <= time.time() - evidence['completed_at'] <= 30 * 86400):
                return ('conformant', '')
        except (ValueError, TypeError, KeyError):
            pass
        return ('unchecked', 'no current successful drill and op-input proof')
    if payload_keys is not None:
        if check_payload(name, payload_keys):
            return ('conformant', '')
        return ('degraded', 'missing payload keys')
    return (status_of(conn, name), '')
