if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import collections
import fnmatch
import hashlib
import json
import math
import re
import secrets
import subprocess
import time
import uuid
from pathlib import Path
from .profile_selection import PROFILE
SCHEMA_V2 = 2
CHECK_KINDS = ('test', 'static', 'workflow', 'mutation')
LANES = ('local', 'pc')
RISKS = ('ordinary', 'sensitive')
SEVERITIES = ('block', 'critical', 'important', 'medium', 'minor', 'test_gap')
PLACEHOLDERS = ('{py}', '{py39}', '{junit}', '{out}')
TOP_KEYS = {'schema', 'name', 'version', 'risk', 'interpreters', 'env_allow', 'env_set', 'protected', 'checks', 'review', 'integration'}
CHECK_KEYS = {'id', 'kind', 'lane', 'interpreter', 'cwd', 'argv', 'timeout_s', 'junit', 'min_tests', 'watch', 'workflow', 'suite', 'spec_from', 'protected_suite'}
INTERP_KEYS = {'path', 'version_prefix'}
MAX_POLICY_BYTES = 65536
V2_DDL = "\nCREATE TABLE candidate(\n  id TEXT PRIMARY KEY,\n  task TEXT NOT NULL REFERENCES task(id),\n  attempt TEXT NOT NULL REFERENCES attempt(id),\n  gen INTEGER NOT NULL,\n  base_sha TEXT NOT NULL,\n  head_sha TEXT NOT NULL,\n  tree_sha TEXT NOT NULL,\n  diff_digest TEXT NOT NULL,\n  policy TEXT NOT NULL REFERENCES policy(id),\n  runtime_digest TEXT NOT NULL,\n  producer TEXT NOT NULL DEFAULT 'local' CHECK(producer IN ('local','pc-vm')),\n  claims TEXT NOT NULL DEFAULT '[]',\n  no_change_reason TEXT NOT NULL DEFAULT '',\n  state TEXT NOT NULL CHECK(state IN ('submitted','verifying','verified','failed','in_review','accepted',\n                                      'rejected','superseded','invalidated','refused')),\n  reason TEXT NOT NULL DEFAULT '',\n  created_at REAL NOT NULL,\n  updated_at REAL NOT NULL,\n  UNIQUE(task, head_sha, attempt));\nCREATE TABLE candidate_file(\n  candidate TEXT REFERENCES candidate(id),\n  path TEXT NOT NULL,\n  change TEXT NOT NULL CHECK(change IN ('A','M','D','R','T')),\n  blob_sha TEXT NOT NULL DEFAULT '',\n  PRIMARY KEY(candidate, path));\nCREATE UNIQUE INDEX candidate_one_open ON candidate(task) WHERE state IN ('submitted','verifying','verified','in_review');\nCREATE TABLE policy(\n  id TEXT PRIMARY KEY,\n  name TEXT NOT NULL,\n  version INTEGER NOT NULL,\n  body TEXT NOT NULL,\n  loaded_at REAL NOT NULL,\n  UNIQUE(name, version));\nALTER TABLE task ADD COLUMN policy TEXT NOT NULL DEFAULT '';\nALTER TABLE task ADD COLUMN base_ref TEXT NOT NULL DEFAULT '';\nALTER TABLE task ADD COLUMN ac_task TEXT NOT NULL DEFAULT '';\nCREATE TABLE verify_run(\n  id TEXT PRIMARY KEY,\n  candidate TEXT NOT NULL REFERENCES candidate(id),\n  lane TEXT NOT NULL CHECK(lane IN ('local','pc')),\n  purpose TEXT NOT NULL CHECK(purpose IN ('candidate','integration','finding_repro')),\n  target_sha TEXT NOT NULL,\n  verifier_digest TEXT NOT NULL,\n  state TEXT NOT NULL CHECK(state IN ('intent','running','done','lost','error')),\n  remote_ref TEXT NOT NULL DEFAULT '',\n  remote_branch TEXT NOT NULL DEFAULT '',\n  intent_at REAL NOT NULL,\n  started_at REAL,\n  ended_at REAL,\n  reason TEXT NOT NULL DEFAULT '');\nCREATE TABLE receipt(\n  id TEXT PRIMARY KEY,\n  run TEXT NOT NULL REFERENCES verify_run(id),\n  candidate TEXT NOT NULL REFERENCES candidate(id),\n  target_sha TEXT NOT NULL,\n  policy TEXT NOT NULL,\n  check_id TEXT NOT NULL,\n  check_digest TEXT NOT NULL,\n  lane TEXT NOT NULL,\n  interpreter TEXT NOT NULL,\n  env_digest TEXT NOT NULL,\n  argv_digest TEXT NOT NULL,\n  exit_code INTEGER,\n  duration_s REAL NOT NULL,\n  outcome TEXT NOT NULL CHECK(outcome IN ('pass','fail','error','timeout','vacuous','weakened',\n                                          'dirty_tree','sha_mismatch','overlay_mismatch')),\n  counts TEXT NOT NULL DEFAULT '{}',\n  node_ids_digest TEXT NOT NULL DEFAULT '',\n  artifact_digest TEXT NOT NULL DEFAULT '',\n  overlay_digest TEXT NOT NULL DEFAULT '',\n  runner TEXT NOT NULL,\n  stale INTEGER NOT NULL DEFAULT 0,\n  stale_reason TEXT NOT NULL DEFAULT '',\n  created_at REAL NOT NULL,\n  UNIQUE(run, check_id));\nCREATE TABLE receipt_node(\n  receipt TEXT REFERENCES receipt(id),\n  node TEXT NOT NULL,\n  status TEXT NOT NULL CHECK(status IN ('passed','failed','skipped','error')),\n  PRIMARY KEY(receipt, node));\nCREATE TABLE ac_baseline(\n  policy_check TEXT NOT NULL,\n  ac_ref TEXT NOT NULL,\n  node TEXT NOT NULL,\n  PRIMARY KEY(policy_check, ac_ref, node));\n"

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)

def migrate(conn):
    """Idempotently migrate a v1 controller DB to schema v2. Refuses any version but 1/2.

    The version is re-checked AFTER the BEGIN IMMEDIATE lock, so two concurrent migrators
    cannot both observe v1 and both try to create the v2 tables."""
    from .controller import Unavailable
    conn.execute('BEGIN IMMEDIATE')
    try:
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        if version == SCHEMA_V2:
            conn.execute('COMMIT')
            return
        if version != 1:
            raise Unavailable('unavailable_schema', 'cannot migrate user_version %s; expected 1 or %d' % (version, SCHEMA_V2))
        for stmt in V2_DDL.split(';'):
            if stmt.strip():
                conn.execute(stmt)
        conn.execute('PRAGMA user_version=%d' % SCHEMA_V2)
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')

def check_digest(check):
    import hashlib
    return hashlib.sha256(canonical(check).encode('utf-8')).hexdigest()

def policy_id_for(name, version, body):
    import hashlib
    return 'P-' + hashlib.sha256(canonical(body).encode('utf-8')).hexdigest()[:10]

def _placeholder_errors(value):
    out = []
    for token in re.findall('\\{[^}]*\\}', value):
        if token not in PLACEHOLDERS:
            out.append('unknown placeholder %s' % token)
    return out
_VALUE_OPTIONS = {'-k', '-m', '-p', '-c', '-o', '--keyword', '--junitxml', '--junit-prefix', '--ignore', '--maxfail', '--cov', '--cov-config', '--cov-report', '--cov-fail-under', '--deselect', '--tb', '--log-level', '--log-file', '--basetemp', '--rootdir', '--import-mode', '--color', '--confcutdir', '--cache-dir', '--cov-context'}
_FLAG_OPTIONS = {'-q', '-v', '-vv', '-x', '-s', '-l', '--quiet', '--verbose', '--exitfirst', '--no-header', '--no-summary', '--no-cov', '--collect-only', '--disable-warnings', '--continue-on-collection-errors', '--strict-markers', '--strict-config'}

def _k_filters(argv):
    """All -k / --keyword filter values, in argv order. Pytest honors only the last."""
    out = []
    for i, arg in enumerate(argv):
        if arg == '-k' and i + 1 < len(argv):
            out.append(argv[i + 1])
        elif arg.startswith('--keyword='):
            out.append(arg.split('=', 1)[1])
    return out

def _narrowed(argv):
    """Fail-closed: a local-lane check must be targeted. A positional target must be a
    .py path or a ::node; a bare directory target is allowed only when a non-empty -k
    filter restricts it. Option values never count as targets, unsupported options are
    refused, and an empty filter is refused. Duplicate -k/--keyword filters are rejected
    (pytest honors only the last, so any earlier one is ambiguous)."""
    filters = _k_filters(argv)
    if len(filters) > 1:
        return False
    k = filters[0] if filters else None
    if k is not None and (not k.strip()):
        return False
    targets = []
    i = 0
    while i < len(argv) and argv[i] in PLACEHOLDERS:
        i += 1
    if i < len(argv) and argv[i] == '-m':
        i += 1
    if i < len(argv) and argv[i] in ('pytest', 'python', 'python3'):
        i += 1
    while i < len(argv):
        arg = argv[i]
        if arg.startswith('--'):
            name, eq, _ = arg.partition('=')
            if name in _VALUE_OPTIONS:
                i += 1 if eq else 2
                continue
            if eq or name not in _FLAG_OPTIONS:
                return False
            i += 1
            continue
        if arg.startswith('-') and arg != '-':
            if arg in _VALUE_OPTIONS:
                i += 2
                continue
            if arg in _FLAG_OPTIONS:
                i += 1
                continue
            return False
        targets.append(arg)
        i += 1
    if not targets:
        return False
    if k is not None:
        return True
    return all((a.endswith('.py') or '::' in a for a in targets))

def validate_policy(body):
    """Return a list of error strings (empty == valid). Fail closed on unknown keys."""
    errors = []
    if not isinstance(body, dict):
        return ['policy body must be an object']
    unknown = set(body) - TOP_KEYS
    if unknown:
        errors.append('unknown top-level keys: %s' % ', '.join(sorted(unknown)))
    if body.get('schema') != 'orch-policy/1':
        errors.append('schema must be orch-policy/1')
    name = body.get('name')
    if not isinstance(name, str) or not name:
        errors.append('name must be a non-empty string')
    version = body.get('version')
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        errors.append('version must be a positive integer')
    if body.get('risk') not in RISKS:
        errors.append('risk must be one of %s' % ', '.join(RISKS))
    sensitive = body.get('risk') == 'sensitive'
    interp = body.get('interpreters')
    if not isinstance(interp, dict) or not (interp.get('py') or interp.get('py39')):
        errors.append('interpreters must define py and/or py39')
    else:
        for key, spec in interp.items():
            if not isinstance(spec, dict) or set(spec) != INTERP_KEYS:
                errors.append('interpreter %s must have path and version_prefix' % key)
                continue
            if not isinstance(spec['path'], str) or not spec['path']:
                errors.append('interpreter %s path must be a non-empty string' % key)
            if not isinstance(spec['version_prefix'], str):
                errors.append('interpreter %s version_prefix must be a string' % key)
    env_allow = body.get('env_allow')
    if not isinstance(env_allow, list) or any((not isinstance(v, str) for v in env_allow)):
        errors.append('env_allow must be a list of strings')
    env_set = body.get('env_set')
    if not isinstance(env_set, dict) or any((not isinstance(v, str) for v in env_set.values())):
        errors.append('env_set must be an object of string values')
    protected = body.get('protected')
    if not isinstance(protected, list) or any((not isinstance(v, str) for v in protected)):
        errors.append('protected must be a list of strings')
    checks = body.get('checks')
    if not isinstance(checks, list) or not checks:
        errors.append('checks must be a non-empty list')
    else:
        seen_ids = set()
        for index, check in enumerate(checks):
            where = 'checks[%d]' % index
            if not isinstance(check, dict):
                errors.append('%s must be an object' % where)
                continue
            unknown = set(check) - CHECK_KEYS
            if unknown:
                errors.append('%s has unknown keys: %s' % (where, ', '.join(sorted(unknown))))
            cid = check.get('id')
            if not isinstance(cid, str) or not cid:
                errors.append('%s.id must be a non-empty string' % where)
            elif cid in seen_ids:
                errors.append('duplicate check id %s' % cid)
            else:
                seen_ids.add(cid)
            if check.get('kind') not in CHECK_KINDS:
                errors.append('%s.kind must be one of %s' % (where, ', '.join(CHECK_KINDS)))
            if check.get('lane') not in LANES:
                errors.append('%s.lane must be local or pc' % where)
            watch = check.get('watch')
            if not isinstance(watch, list) or not watch or any((not isinstance(v, str) for v in watch)):
                errors.append('%s.watch must be a non-empty list of strings' % where)
            timeout = check.get('timeout_s')
            if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or (not math.isfinite(timeout)) or (timeout <= 0):
                errors.append('%s.timeout_s must be a finite positive number' % where)
            kind = check.get('kind')
            lane = check.get('lane')
            if kind in ('test', 'static'):
                argv = check.get('argv')
                if not isinstance(argv, list) or any((not isinstance(v, str) for v in argv)):
                    errors.append('%s.argv must be a list of strings' % where)
                else:
                    for arg in argv:
                        for e in _placeholder_errors(arg):
                            errors.append('%s.argv %r: %s' % (where, arg, e))
                    if lane == 'local' and (not _narrowed(argv)):
                        errors.append('%s names a whole test tree on the local lane (name a .py path, a ::node or -k, or use lane: pc)' % where)
                if sensitive and lane != 'pc':
                    errors.append('%s: risk sensitive requires lane pc for test/static checks' % where)
            elif kind == 'workflow':
                for key in ('workflow', 'suite'):
                    if not isinstance(check.get(key), str) or not check[key]:
                        errors.append('%s.%s must be a non-empty string' % (where, key))
            elif kind == 'mutation':
                if not isinstance(check.get('spec_from'), str) or not check['spec_from']:
                    errors.append('%s.spec_from must be a non-empty string' % where)
            if check.get('protected_suite') is not None and (not isinstance(check.get('protected_suite'), bool)):
                errors.append('%s.protected_suite must be a boolean' % where)
    review = body.get('review')
    if not isinstance(review, dict) or 'roles' not in review or 'blocking_severities' not in review:
        errors.append('review must define roles and blocking_severities')
    else:
        roles = review['roles']
        if not isinstance(roles, list) or not roles or any((not isinstance(v, str) for v in roles)):
            errors.append('review.roles must be a non-empty list of strings')
        sev = review['blocking_severities']
        if not isinstance(sev, list) or any((v not in SEVERITIES for v in sev)):
            errors.append('review.blocking_severities must be a subset of %s' % ', '.join(SEVERITIES))
    integration = body.get('integration')
    if not isinstance(integration, dict) or 'checks' not in integration or (not isinstance(integration['checks'], list)) or any((not isinstance(v, str) for v in integration['checks'])):
        errors.append('integration.checks must be a list of strings')
    return errors

def load_policy(conn, path, now=None):
    """Read, validate and register a policy file; return its id. Idempotent for an
    unchanged (name, version); a changed body under the same (name, version) is refused.
    """
    from .controller import Unavailable
    p = Path(path)
    try:
        raw = p.read_text(encoding='utf-8')
    except OSError as error:
        raise Unavailable('policy_missing', '%s: %s' % (p.name, type(error).__name__)) from error
    if len(raw.encode('utf-8')) > MAX_POLICY_BYTES:
        raise Unavailable('invalid_field', 'policy %s is too large' % p.name)
    try:
        body = json.loads(raw)
    except ValueError as error:
        raise Unavailable('invalid_field', 'policy %s is not valid JSON' % p.name) from error
    errors = validate_policy(body)
    if errors:
        raise Unavailable('invalid_field', 'policy %s: %s' % (p.name, '; '.join(errors)))
    name, version = (body['name'], int(body['version']))
    cid = policy_id_for(name, version, body)
    now = time.time() if now is None else now
    row = conn.execute('SELECT id FROM policy WHERE name=? AND version=?', (name, version)).fetchone()
    if row is not None:
        if row['id'] != cid:
            raise Unavailable('policy_changed', 'policy %s v%d changed without a version bump' % (name, version))
        return cid
    conn.execute('INSERT INTO policy(id, name, version, body, loaded_at) VALUES (?,?,?,?,?)', (cid, name, version, canonical(body), now))
    return cid
Verdict = collections.namedtuple('Verdict', 'ok missing reasons')

class GitError(Exception):
    pass

def _sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()

def _glob_match(globs, path):
    return any((fnmatch.fnmatch(path, g) for g in globs))

def _parse_name_status(data):
    out = []
    fields = data.split(b'\x00')
    i = 0
    while i < len(fields):
        status = fields[i].decode('utf-8', 'replace')
        if not status:
            i += 1
            continue
        if status.startswith('R') and i + 2 < len(fields):
            old = fields[i + 1].decode('utf-8', 'replace')
            new = fields[i + 2].decode('utf-8', 'replace')
            out.append(('R', old))
            out.append(('R', new))
            i += 3
        elif i + 1 < len(fields) and fields[i + 1]:
            out.append((status, fields[i + 1].decode('utf-8', 'replace')))
            i += 2
        else:
            i += 1
    return out

class Git:
    """Small git adapter so submit_candidate never shells out directly; tests inject a fixture."""

    def __init__(self, cwd='.'):
        self.cwd = cwd

    def _run(self, *args):
        return subprocess.run(['git', *args], cwd=self.cwd, capture_output=True, timeout=30, env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'GIT_OPTIONAL_LOCKS': '0'})

    def cat_file_e(self, sha):
        return self._run('cat-file', '-e', sha + '^{commit}').returncode == 0

    def merge_base(self, base_ref, sha):
        p = self._run('merge-base', base_ref, sha)
        out = p.stdout.decode('utf-8', 'replace').strip()
        return out if p.returncode == 0 and out else None

    def rev_parse_tree(self, sha):
        p = self._run('rev-parse', sha + '^{tree}')
        out = p.stdout.decode('utf-8', 'replace').strip()
        return out if p.returncode == 0 and out else None

    def diff_name_status(self, base, sha):
        p = self._run('diff', '--name-status', '-z', '-M', base, sha)
        if p.returncode != 0:
            raise GitError('git diff failed')
        return _parse_name_status(p.stdout)

    def diff_binary(self, base, sha):
        p = self._run('diff', '--binary', '--full-index', base, sha)
        if p.returncode != 0:
            raise GitError('git diff failed')
        return p.stdout

    def is_ancestor(self, workspace, sha):
        return self._run('-C', workspace, 'merge-base', '--is-ancestor', sha, 'HEAD').returncode == 0

def submit_candidate(conn, task, nonce, sha, git, now=None, claims=(), no_change='', fixes=()):
    """A builder submits a candidate (a sha + a controller-computed manifest), never an acceptance.

    The manifest (base/head/tree sha, diff digest, file list) is computed from git itself;
    the agent supplies only the sha. Refusals: sha_unknown, base_unresolved, sha_not_in_workspace,
    wrong_writer, protected_path_changed, no_change_unexplained, policy_missing, plus the
    Stage 2 submit refusals.
    """
    from .controller import _run, _need_task, _credential_attempt, _ok, Refused, Result, MAX_TEXT, _check_text
    now = time.time() if now is None else now

    def refuse(code, message):
        return Result(False, code, message, task, None, None, None, None, None)
    if not isinstance(sha, str) or not re.fullmatch('[a-f0-9]{40}', sha):
        return refuse('sha_unknown', 'candidate sha must be 40 hex')
    if not git.cat_file_e(sha):
        return refuse('sha_unknown', 'no such commit %s' % sha)
    t0 = conn.execute('SELECT * FROM task WHERE id=?', (task,)).fetchone()
    if t0 is None:
        return refuse('not_found', 'Task packet %s does not exist.' % task)
    if not t0['base_ref']:
        return refuse('base_unresolved', 'Task %s has no base_ref (set it first).' % task)
    base_sha = git.merge_base(t0['base_ref'], sha)
    if base_sha is None:
        return refuse('base_unresolved', 'no merge base for %s and %s' % (t0['base_ref'], sha))
    tree_sha = git.rev_parse_tree(sha)
    if tree_sha is None:
        return refuse('sha_unknown', 'no tree for %s' % sha)
    try:
        diff = git.diff_binary(base_sha, sha)
        files = git.diff_name_status(base_sha, sha)
    except GitError as error:
        return refuse('base_unresolved', str(error))
    diff_digest = _sha256_bytes(diff)
    a0 = conn.execute('SELECT * FROM attempt WHERE nonce=?', (nonce,)).fetchone() if nonce else None
    if a0 is not None and a0['workspace'] and (not git.is_ancestor(a0['workspace'], sha)):
        return refuse('sha_not_in_workspace', "sha %s is not an ancestor of the attempt's workspace" % sha)
    claims_json = json.dumps(sorted(claims or ()))

    def body(tx):
        t = _need_task(tx, task)
        a, proof = _credential_attempt(tx, t, nonce)
        frm = t['status']
        if frm not in ('reserved', 'running', 'waiting'):
            raise Refused('illegal_transition', 'Task %s is %s: candidates come from a reserved/running/waiting task.' % (t['id'], frm), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if a['state'] not in ('launching', 'running', 'waiting'):
            raise Refused('illegal_transition', 'Attempt %s is %s: nothing to submit.' % (a['id'], a['state']), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if t['policy'] == '':
            raise Refused('policy_missing', 'Task %s is ungated; submit without --sha.' % t['id'], task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        owns = [r['glob'] for r in tx.q('SELECT glob FROM task_owns WHERE task=? ORDER BY ord', (t['id'],)).fetchall()]
        for change, path in files:
            if not _glob_match(owns, path):
                raise Refused('wrong_writer', "path %s is outside this task's owns" % path, task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        pbody = json.loads(tx.one('SELECT body FROM policy WHERE id=?', (t['policy'],))['body'])
        for change, path in files:
            if _glob_match(pbody['protected'], path) and (not (t['role'] in PROFILE.verifier_roles and _glob_match(owns, path))):
                raise Refused('protected_path_changed', 'protected path %s changed' % path, task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        if sha == base_sha and (not (no_change or '').strip()):
            raise Refused('no_change_unexplained', 'head_sha == base_sha needs a --no-change reason', task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
        _check_text('no_change', no_change or '', MAX_TEXT)
        tx.q("UPDATE candidate SET state='superseded', reason='superseded', updated_at=? WHERE task=? AND state IN ('submitted','verifying','verified','in_review')", (now, t['id']))
        runtime_digest = ''
        rrow = tx.one("SELECT value FROM meta WHERE key='runtime_digest'")
        if rrow is not None:
            runtime_digest = rrow['value']
        existing = tx.one('SELECT id FROM candidate WHERE task=? AND head_sha=? AND attempt=?', (t['id'], sha, a['id']))
        if existing is not None:
            cid = existing['id']
            tx.q("UPDATE candidate SET state='submitted', reason='', base_sha=?, tree_sha=?, diff_digest=?, policy=?, runtime_digest=?, claims=?, no_change_reason=?, updated_at=? WHERE id=?", (base_sha, tree_sha, diff_digest, t['policy'], runtime_digest, claims_json, no_change or '', now, cid))
            tx.q('DELETE FROM candidate_file WHERE candidate=?', (cid,))
        else:
            cid = 'C-' + hashlib.sha256(('%s|%s|%s' % (t['id'], a['id'], sha)).encode()).hexdigest()[:10]
            tx.q("INSERT INTO candidate(id, task, attempt, gen, base_sha, head_sha, tree_sha, diff_digest, policy, runtime_digest, producer, claims, no_change_reason, state, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?, ?, ?, ?, 'submitted', ?, ?)", (cid, t['id'], a['id'], a['gen'], base_sha, sha, tree_sha, diff_digest, t['policy'], runtime_digest, 'pc-vm' if a['lane'] == 'pc-vm' else 'local', claims_json, no_change or '', now, now))
        for change, path in files:
            tx.q('INSERT INTO candidate_file(candidate, path, change) VALUES (?,?,?)', (cid, path, change))
        for fid in fixes or ():
            fx = tx.one('SELECT * FROM finding WHERE id=?', (fid,))
            if fx is None or fx['task'] != t['id']:
                raise Refused('finding_not_found', 'finding %s is not on task %s' % (fid, t['id']), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
            if fx['status'] != 'open':
                raise Refused('illegal_transition', 'finding %s is %s: a fix needs an open finding' % (fid, fx['status']), task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
            if fx['candidate'] == cid:
                raise Refused('invalid_field', 'finding %s: a fix must be a NEW candidate' % fid, task=t['id'], attempt=a['id'], gen=a['gen'], from_state=frm)
            tx.q("UPDATE finding SET status='fixed', fix_candidate=?, updated_at=? WHERE id=?", (cid, now, fid))
            tx.q('INSERT INTO finding_event(finding, ts, from_status, to_status, actor, candidate, code) VALUES (?,?,?,?,?,?,?)', (fid, now, 'open', 'fixed', 'builder', fx['candidate'], 'fix'))
        from . import verification_contract as vc
        vc.snapshot(conn, cid)
        vc.put(conn, 'ac-red', cid, {})
        tx.set_task(t, 'submitted', frm)
        tx.q("UPDATE attempt SET state='submitted' WHERE id=?", (a['id'],))
        return _ok(tx, t, frm, 'submitted', attempt=a['id'], gen=a['gen'])
    return _run(conn, 'submit_candidate', 'builder', now, body)

def record_run_intent(conn, candidate, lane, purpose, target_sha, verifier_digest, now=None):
    """Create a verify_run in 'intent' state before any push/dispatch (C9)."""
    from .controller import Refused
    now = time.time() if now is None else now
    if lane not in ('local', 'pc'):
        raise Refused('invalid_field', 'lane must be local or pc')
    if purpose not in ('candidate', 'integration', 'finding_repro'):
        raise Refused('invalid_field', 'unknown purpose %s' % purpose)
    rid = 'V-' + uuid.uuid4().hex[:10]
    conn.execute("INSERT INTO verify_run(id, candidate, lane, purpose, target_sha, verifier_digest, state, intent_at) VALUES (?,?,?,?,?,?, 'intent', ?)", (rid, candidate, lane, purpose, target_sha, verifier_digest, now))
    return rid

def record_receipt(conn, run, check_id, fields):
    """Write one check receipt (and its protected-suite nodes). Receipts are facts, never edited."""
    from .controller import Refused
    now = time.time()
    if 'candidate' not in fields or 'outcome' not in fields or 'duration_s' not in fields:
        raise Refused('invalid_field', 'receipt needs candidate, outcome and duration_s')
    rid = 'R-' + hashlib.sha256(('%s|%s' % (run, check_id)).encode()).hexdigest()[:10]
    counts = json.dumps(fields.get('counts') or {})
    conn.execute('BEGIN IMMEDIATE')
    try:
        conn.execute('INSERT INTO receipt(id, run, candidate, target_sha, policy, check_id, check_digest, lane, interpreter, env_digest, argv_digest, exit_code, duration_s, outcome, counts, node_ids_digest, artifact_digest, overlay_digest, runner, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (rid, run, fields['candidate'], fields.get('target_sha', ''), fields.get('policy', ''), check_id, fields.get('check_digest', ''), fields.get('lane', ''), fields.get('interpreter', ''), fields.get('env_digest', ''), fields.get('argv_digest', ''), fields.get('exit_code'), fields['duration_s'], fields['outcome'], counts, fields.get('node_ids_digest', ''), fields.get('artifact_digest', ''), fields.get('overlay_digest', ''), fields.get('runner', ''), now))
        for node, status in fields.get('nodes') or []:
            conn.execute('INSERT INTO receipt_node(receipt, node, status) VALUES (?,?,?)', (rid, node, status))
        from . import verification_contract as vc
        vc.put(conn, 'receipt', rid, {key: fields[key] for key in ('mutants', 'observed_sha', 'verifier_digest') if key in fields})
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return rid

def _ac_ref(conn, t):
    """The AC reference sha: the accepted candidate's head_sha of the task's QA AC task."""
    if not t['ac_task']:
        return ''
    row = conn.execute("SELECT head_sha FROM candidate WHERE task=? AND state='accepted' ORDER BY updated_at DESC LIMIT 1", (t['ac_task'],)).fetchone()
    return row['head_sha'] if row else ''

def evaluate(conn, candidate, now=None, include_accepted=False):
    """C5 acceptance rules: a pure read. Verdict(ok, missing[], reasons[])."""
    from .controller import LIVE
    now = time.time() if now is None else now
    missing, reasons = ([], [])
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    if c is None:
        return Verdict(False, ['candidate_not_found'], ['no such candidate'])
    t = conn.execute('SELECT * FROM task WHERE id=?', (c['task'],)).fetchone()
    a = conn.execute('SELECT * FROM attempt WHERE id=?', (c['attempt'],)).fetchone()
    if t is None or a is None or a['gen'] != t['gen'] or (a['gen'] != c['gen']) or (a['state'] not in LIVE and (not (include_accepted and a['state'] == 'accepted' and (c['state'] == 'accepted')))):
        missing.append('stale_generation')
        reasons.append('candidate belongs to a superseded or ended attempt')
        return Verdict(False, missing, reasons)
    owns = [r['glob'] for r in conn.execute('SELECT glob FROM task_owns WHERE task=? ORDER BY ord', (t['id'],)).fetchall()]
    cfs = conn.execute('SELECT path FROM candidate_file WHERE candidate=?', (candidate,)).fetchall()
    pbody = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c['policy'],)).fetchone()['body'])
    for row in cfs:
        if not _glob_match(owns, row['path']):
            missing.append('wrong_writer')
            reasons.append('path %s outside owns' % row['path'])
        if _glob_match(pbody['protected'], row['path']) and (not (t['role'] in PROFILE.verifier_roles and _glob_match(owns, row['path']))):
            missing.append('protected_path_changed')
            reasons.append('protected path %s changed' % row['path'])
    for check in pbody['checks']:
        cid = check['id']
        rec = conn.execute('SELECT * FROM receipt WHERE candidate=? AND check_id=? ORDER BY created_at DESC LIMIT 1', (candidate, cid)).fetchone()
        if rec is None:
            missing.append('check_missing:' + cid)
            reasons.append('no receipt for ' + cid)
            continue
        if rec['target_sha'] != c['head_sha'] or rec['policy'] != c['policy'] or c['policy'] != t['policy']:
            missing.append('check_missing:' + cid)
            reasons.append('receipt for wrong sha/policy')
            continue
        if rec['check_digest'] != check_digest(check):
            missing.append('check_missing:' + cid)
            reasons.append('receipt for a different check definition')
            continue
        run_row = conn.execute('SELECT * FROM verify_run WHERE id=?', (rec['run'],)).fetchone()
        if run_row is None or run_row['purpose'] != 'candidate' or run_row['candidate'] != candidate or (run_row['target_sha'] != c['head_sha']) or (run_row['state'] != 'done') or (run_row['lane'] != check.get('lane', 'local')):
            missing.append('check_missing:' + cid)
            reasons.append("receipt run is not a done candidate run on the check's lane")
            continue
        stale, sreason = staleness(conn, rec['id'])
        if stale:
            missing.append('check_stale:' + cid)
            reasons.append('receipt stale: ' + sreason)
            continue
        mutation_waived = False
        if check.get('kind') == 'mutation':
            from . import verification_contract as vc
            totals = json.loads(rec['counts'])
            mutants = vc.get(conn, 'receipt', rec['id']).get('mutants', [])
            survivors = [m for m in mutants if m.get('result', m.get('status')) == 'SURVIVED']
            mutation_waived = rec['outcome'] == 'fail' and totals.get('SURVIVED', 0) == len(survivors) and bool(survivors) and (not totals.get('ERROR', 0)) and (not totals.get('BASELINE_RED', 0))
            for mutant in survivors:
                if not conn.execute("SELECT 1 FROM finding WHERE task=? AND candidate=? AND ref=? AND severity='test_gap' AND status IN ('verified','waived')", (t['id'], candidate, mutant['id'])).fetchone():
                    mutation_waived = False
        qa_ac = t['role'] in PROFILE.verifier_roles and check.get('protected_suite') and (rec['outcome'] in ('pass', 'fail')) and (json.loads(rec['counts']).get('errors', 0) == 0) and (sum((json.loads(rec['counts']).get(k, 0) for k in ('passed', 'failed'))) > 0)
        if rec['outcome'] != 'pass' and (not mutation_waived) and (not qa_ac):
            missing.append('check_failed:' + cid + ':' + rec['outcome'])
            reasons.append(cid + ' ' + rec['outcome'])
            continue
        if check.get('kind') == 'test' and check.get('junit') and (not qa_ac):
            counts = json.loads(rec['counts'] or '{}')
            if counts.get('passed', 0) < (check.get('min_tests') or 1) or counts.get('failed', 0) != 0 or counts.get('errors', 0) != 0:
                missing.append('check_failed:' + cid + ':vacuous')
                reasons.append('insufficient passing tests')
                continue
        if check.get('protected_suite'):
            ac_ref = c['head_sha'] if t['role'] in PROFILE.verifier_roles else _ac_ref(conn, t)
            if t['role'] in PROFILE.verifier_roles:
                if not conn.execute("SELECT 1 FROM receipt_node WHERE receipt=? AND status IN ('passed','failed')", (rec['id'],)).fetchone():
                    missing.append('ac_missing')
                continue
            from . import verification_contract as vc
            frozen = vc.get(conn, 'ac', ac_ref)
            if not ac_ref or not rec['overlay_digest'] or frozen.get('overlay_digest') != rec['overlay_digest']:
                missing.append('ac_missing')
                reasons.append('protected check lacks accepted AC or overlay proof')
                continue
            baseline = {r['node'] for r in conn.execute('SELECT node FROM ac_baseline WHERE policy_check=? AND ac_ref=?', (cid, ac_ref)).fetchall()}
            passed = {r['node'] for r in conn.execute("SELECT node FROM receipt_node WHERE receipt=? AND status='passed'", (rec['id'],)).fetchall()}
            if not baseline or not baseline <= passed:
                missing.append('suite_weakened')
                reasons.append('protected suite does not cover the AC baseline')
                continue
    if c['head_sha'] == c['base_sha'] and (not c['no_change_reason']):
        missing.append('no_change_unexplained')
        reasons.append('no-change candidate lacks a reason')
    if t['role'] in PROFILE.verifier_roles and any((ch.get('protected_suite') for ch in pbody['checks'])):
        from . import verification_contract as vc
        validation = next((ch for ch in pbody['checks'] if ch['id'] == 'ac-validation' and (not ch.get('protected_suite'))), None)
        if validation is None:
            missing.append('ac_validation_missing')
        proof = vc.get(conn, 'ac-red', candidate)
        protected = {ch['id']: ch for ch in pbody['checks'] if ch.get('protected_suite')}
        reds = [conn.execute('SELECT * FROM receipt WHERE id=?', (rid,)).fetchone() for rid in proof.get('receipts', [])]
        valid_red = bool(reds) and len(reds) == len(protected) and (proof.get('ac_sha') == c['head_sha'])
        seen = set()
        for red in reds:
            if red is None:
                valid_red = False
                continue
            run = conn.execute('SELECT * FROM verify_run WHERE id=?', (red['run'],)).fetchone()
            check = protected.get(red['check_id'])
            stale, _ = staleness(conn, red['id'])
            if not check or red['candidate'] != candidate or red['policy'] != c['policy'] or (red['target_sha'] != c['base_sha']) or (red['outcome'] != 'fail') or (red['check_digest'] != check_digest(check)) or stale or (json.loads(red['counts']).get('failed', 0) < 1) or (not run) or (run['state'] != 'done') or (run['purpose'] != 'finding_repro') or (run['candidate'] != candidate) or (run['target_sha'] != c['base_sha']):
                valid_red = False
            seen.add(red['check_id'])
        if not valid_red or seen != set(protected):
            missing.append('ac_not_red')
            reasons.append('QA suite lacks current red proof for every protected check')
    for f in conn.execute("SELECT * FROM finding WHERE task=? AND status IN ('open','fixed')", (t['id'],)).fetchall():
        if f['severity'] in pbody['review']['blocking_severities'] or f['severity'] == 'test_gap':
            missing.append('finding_open')
            reasons.append('open finding %s (%s)' % (f['id'], f['severity']))
    for role in pbody['review']['roles']:
        done = conn.execute("SELECT 1 FROM review WHERE candidate=? AND role=? AND state='done' AND head_sha=?", (candidate, role, c['head_sha'])).fetchone()
        if done is None:
            missing.append('review_missing:' + role)
            reasons.append('no done review for role ' + role)
    return Verdict(len(missing) == 0, missing, reasons)

def accept(conn, task, now=None, actor='pm'):
    """Accept a task: re-run evaluate inside the write transaction, then done/accepted."""
    from .controller import _run, _need_task, _ok, Refused
    now = time.time() if now is None else now

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if t['policy'] == '':
            raise Refused('policy_missing', 'Task %s is ungated; use close --status done.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        if frm != 'submitted':
            raise Refused('illegal_transition', 'Task %s is %s: only a submitted task accepts.' % (t['id'], frm), task=t['id'], gen=t['gen'], from_state=frm)
        c = tx.one("SELECT * FROM candidate WHERE task=? AND (state IN ('verified','in_review') OR (state='failed' AND EXISTS (SELECT 1 FROM finding f WHERE f.candidate=candidate.id AND f.severity='test_gap' AND f.status IN ('verified','waived')))) ORDER BY updated_at DESC LIMIT 1", (t['id'],))
        if c is None:
            raise Refused('acceptance_missing', 'Task %s has no verified candidate.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        verdict = evaluate(conn, c['id'], now)
        if not verdict.ok:
            raise Refused('acceptance_missing', 'Task %s acceptance missing: %s' % (t['id'], ', '.join(verdict.missing)), task=t['id'], attempt=c['attempt'], gen=c['gen'], from_state=frm)
        if t['role'] in PROFILE.verifier_roles:
            from . import verification_contract as vc
            proof = vc.get(conn, 'ac-red', c['id'])
            red = tx.one('SELECT overlay_digest FROM receipt WHERE id=?', (proof.get('receipt', ''),))
            if red:
                vc.put(conn, 'ac', c['head_sha'], {'overlay_digest': red[0]})
            policy = json.loads(tx.one('SELECT body FROM policy WHERE id=?', (c['policy'],))['body'])
            for check in policy['checks']:
                if check.get('protected_suite'):
                    rec = tx.one("SELECT id FROM receipt WHERE candidate=? AND check_id=? AND outcome IN ('pass','fail') ORDER BY created_at DESC LIMIT 1", (c['id'], check['id']))
                    tx.q("INSERT OR IGNORE INTO ac_baseline(policy_check,ac_ref,node) SELECT ?,?,node FROM receipt_node WHERE receipt=? AND status IN ('passed','failed')", (check['id'], c['head_sha'], rec['id']))
        tx.q("UPDATE candidate SET state='accepted', updated_at=? WHERE id=?", (now, c['id']))
        tx.q("UPDATE attempt SET state='accepted', ended_at=? WHERE id=?", (now, c['attempt']))
        tx.set_task(t, 'done', frm)
        return _ok(tx, t, frm, 'done', attempt=c['attempt'], gen=c['gen'])
    return _run(conn, 'accept', actor, now, body)

def reject(conn, task, reason='', now=None, actor='pm'):
    """Reject a task's open candidate back to waiting (the builder reworks and re-submits)."""
    from .controller import _run, _need_task, _ok, Refused, MAX_TEXT, _check_text
    now = time.time() if now is None else now

    def body(tx):
        t = _need_task(tx, task)
        frm = t['status']
        if frm != 'submitted':
            raise Refused('illegal_transition', 'Task %s is %s: only a submitted task rejects.' % (t['id'], frm), task=t['id'], gen=t['gen'], from_state=frm)
        c = tx.one("SELECT * FROM candidate WHERE task=? AND state IN ('submitted','verifying','verified','in_review') ORDER BY updated_at DESC LIMIT 1", (t['id'],))
        if c is None:
            raise Refused('candidate_closed', 'Task %s has no open candidate.' % t['id'], task=t['id'], gen=t['gen'], from_state=frm)
        _check_text('reason', reason, MAX_TEXT, required=True)
        tx.q("UPDATE candidate SET state='rejected', reason=?, updated_at=? WHERE id=?", (reason, now, c['id']))
        tx.q("UPDATE attempt SET state='waiting', reason='rejected', lease_until=NULL WHERE id=?", (c['attempt'],))
        tx.set_task(t, 'waiting', frm)
        return _ok(tx, t, frm, 'waiting', attempt=c['attempt'], gen=c['gen'])
    return _run(conn, 'reject', actor, now, body)

def staleness(conn, receipt, at_sha=None):
    """C7: is this receipt stale now? Returns (stale, reason). Source/contract rules are
    computed by verify.py invalidate (which has git), then persisted via the stale flag."""
    rec = conn.execute('SELECT * FROM receipt WHERE id=?', (receipt,)).fetchone()
    if rec is None:
        return (False, '')
    if rec['stale']:
        return (True, rec['stale_reason'])
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (rec['candidate'],)).fetchone()
    if c is None:
        return (True, 'candidate')
    t = conn.execute('SELECT * FROM task WHERE id=?', (c['task'],)).fetchone()
    if t is None:
        return (True, 'task')
    if t['policy'] != rec['policy']:
        return (True, 'policy')
    from . import verification_contract as vc
    context = vc.get(conn, 'candidate', c['id'])
    if context and (context['contract'] != vc.contract_digest(conn, t['id']) or context['ac_ref'] != vc.ac_ref(conn, t['id'])):
        return (True, 'contract')
    pbody = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (t['policy'],)).fetchone()['body'])
    for check in pbody['checks']:
        if check['id'] == rec['check_id'] and check_digest(check) != rec['check_digest']:
            return (True, 'policy')
    run = conn.execute('SELECT * FROM verify_run WHERE id=?', (rec['run'],)).fetchone()
    installed = conn.execute("SELECT value FROM meta WHERE key='verifier_digest'").fetchone()
    if run is not None and installed is not None and installed['value'] and run['verifier_digest'] and (installed['value'] != run['verifier_digest']):
        return (True, 'verifier')
    return (False, '')
