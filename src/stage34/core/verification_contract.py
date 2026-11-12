if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import fnmatch
import hashlib
import json
import os
import subprocess
from pathlib import Path
VERIFIER_FILES = ('verification_contract.py', 'verify.py', 'verify_exec.py', 'verify_pc_run.py')

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()

def put(conn, kind, identity, value):
    conn.execute('INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)', ('verify:%s:%s' % (kind, identity), json.dumps(value, sort_keys=True)))

def get(conn, kind, identity):
    row = conn.execute('SELECT value FROM meta WHERE key=?', ('verify:%s:%s' % (kind, identity),)).fetchone()
    return json.loads(row[0]) if row else {}

def git(tree, *args):
    p = subprocess.run(['git', '-C', str(tree), *args], capture_output=True, env={'PATH': '/usr/bin:/bin', 'GIT_OPTIONAL_LOCKS': '0'}, timeout=30)
    if p.returncode:
        raise ValueError('git operation failed: ' + args[0])
    return p.stdout

def verifier_digest(directory):
    h = hashlib.sha256()
    for name in VERIFIER_FILES:
        h.update(name.encode() + b'\x00' + (Path(directory) / name).read_bytes())
    return h.hexdigest()

def contract_digest(conn, task):
    t = conn.execute('SELECT * FROM task WHERE id=?', (task,)).fetchone()
    owns = [list(r) for r in conn.execute('SELECT ord,glob FROM task_owns WHERE task=? ORDER BY ord', (task,))]
    done = [list(r) for r in conn.execute('SELECT ord,text FROM task_done WHERE task=? ORDER BY ord', (task,))]
    return digest([t['goal'], t['role'], t['policy'], t['base_ref'], t['ac_task'], owns, done])

def ac_ref(conn, task):
    t = conn.execute('SELECT ac_task FROM task WHERE id=?', (task,)).fetchone()
    if not t or not t[0]:
        return ''
    c = conn.execute("SELECT head_sha FROM candidate WHERE task=? AND state='accepted' ORDER BY updated_at DESC LIMIT 1", (t[0],)).fetchone()
    return c[0] if c else ''

def snapshot(conn, candidate):
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    put(conn, 'candidate', candidate, {'contract': contract_digest(conn, c['task']), 'ac_ref': ac_ref(conn, c['task'])})

def matches(globs, path):
    return any((fnmatch.fnmatchcase(path, pattern) for pattern in globs))

def protected_manifest(tree, ref, patterns):
    entries = {}
    for record in git(tree, 'ls-tree', '-rz', '--full-tree', ref).split(b'\x00'):
        if not record:
            continue
        info, path = record.split(b'\t', 1)
        path = path.decode('utf-8', 'strict')
        mode, kind, blob = info.decode().split()
        if matches(patterns, path):
            if kind != 'blob' or mode not in ('100644', '100755'):
                raise ValueError('protected path is not a regular file')
            entries[path] = [mode, blob]
    return entries

def overlay_digest(tree, ref, patterns):
    return digest(protected_manifest(tree, ref, patterns))

def overlay(tree, ref, patterns):
    """Apply only trusted regular-file blobs; refuse symlink traversal in candidate parents."""
    tree = Path(tree)
    trusted = protected_manifest(tree, ref, patterns)
    current = protected_manifest(tree, 'HEAD', patterns)
    for path in sorted(set(trusted) | set(current)):
        target = tree / path
        if any((parent.is_symlink() for parent in [target, *target.parents] if parent != tree.parent)):
            raise ValueError('protected path traverses symlink')
        if path not in trusted:
            target.unlink(missing_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        mode, blob = trusted[path]
        target.write_bytes(git(tree, 'cat-file', 'blob', blob))
        target.chmod(493 if mode == '100755' else 420)
    return digest(trusted)

def tree_fingerprint(tree):
    """Tracked and untracked content, not just status letters, including intended overlay."""
    paths = git(tree, 'ls-files', '-z', '--cached', '--others', '--exclude-standard').split(b'\x00')
    h = hashlib.sha256()
    for raw in sorted(set((p for p in paths if p))):
        path = Path(tree) / raw.decode('utf-8', 'strict')
        h.update(raw + b'\x00')
        if path.is_symlink():
            h.update(b'L' + os.readlink(path).encode())
        elif path.is_file():
            h.update(b'F' + str(path.stat().st_mode & 511).encode() + path.read_bytes())
        else:
            h.update(b'D')
    return h.hexdigest()
