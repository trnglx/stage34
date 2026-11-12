if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
PLACEHOLDERS = ('{py}', '{py39}', '{junit}', '{out}')
SHA = re.compile('^[a-f0-9]{40}$')

def _sha256_bytes(data):
    return hashlib.sha256(data if isinstance(data, bytes) else data.encode('utf-8')).hexdigest()

def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

def _substitute(arg, py, out):
    return arg.replace('{py}', py).replace('{py39}', py).replace('{junit}', str(out / 'junit.xml')).replace('{out}', str(out))

def _build_env(policy, spec, tree, out):
    env = {'PATH': '/usr/bin:/bin:' + os.path.dirname(spec['path']), 'HOME': str(out / 'home'), 'LANG': 'C', 'GIT_OPTIONAL_LOCKS': '0'}
    env.update(policy.get('env_set') or {})
    for name in policy.get('env_allow') or []:
        if name in os.environ:
            env[name] = os.environ[name]
    return env

def _run(argv, cwd, env, timeout):
    proc = subprocess.Popen(argv, cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _killpg(proc.pid)
        proc.wait(timeout=5)
        return None
    return subprocess.CompletedProcess(argv, proc.returncode, out, err)

def _killpg(pgid):
    """Terminate the whole process group with bounded escalation; never raises."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except (ProcessLookupError, PermissionError):
            return
        time.sleep(0.25)

def _git_head(tree):
    p = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=str(tree), capture_output=True, text=True, env={'PATH': '/usr/bin:/bin', 'GIT_OPTIONAL_LOCKS': '0'})
    return p.stdout.strip() if p.returncode == 0 else None

def _git_dirty(tree):
    p = subprocess.run(['git', 'status', '--porcelain'], cwd=str(tree), capture_output=True, text=True, env={'PATH': '/usr/bin:/bin', 'GIT_OPTIONAL_LOCKS': '0'})
    return p.stdout.strip() != ''

def _parse_junit(path):
    counts = {'passed': 0, 'failed': 0, 'skipped': 0, 'errors': 0}
    nodes = []
    if not path.exists():
        return (counts, nodes)
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return (counts, nodes)
    for tc in root.iter('testcase'):
        node = '%s::%s' % (tc.get('classname', ''), tc.get('name', ''))
        if tc.find('error') is not None:
            node_status, count_status = ('error', 'errors')
        elif tc.find('failure') is not None:
            node_status, count_status = ('failed', 'failed')
        elif tc.find('skipped') is not None:
            node_status, count_status = ('skipped', 'skipped')
        else:
            node_status, count_status = ('passed', 'passed')
        counts[count_status] = counts.get(count_status, 0) + 1
        nodes.append((node, node_status))
    return (counts, nodes)

def check_digest(check):
    return _sha256_bytes(json.dumps(check, sort_keys=True, separators=(',', ':')))

def _env_digest(env):
    return _sha256_bytes(json.dumps(sorted(env.items())))

def _nodes_digest(nodes):
    return _sha256_bytes(json.dumps(sorted((n[0] for n in nodes))))

def _finish(check, sha, start, outcome, reason, argv=(), env=None, counts=None, nodes=None, artifact=None):
    return {'check_id': check['id'] if check else '', 'target_sha': sha, 'check_digest': check_digest(check) if check else '', 'lane': 'local', 'interpreter': '', 'env_digest': _env_digest(env) if env else '', 'argv_digest': _sha256_bytes(json.dumps(argv, sort_keys=True)) if argv else '', 'exit_code': None, 'duration_s': round(time.time() - start, 3), 'outcome': outcome, 'reason': reason, 'counts': counts or {}, 'node_ids_digest': _nodes_digest(nodes) if nodes else '', 'artifact_digest': _sha256_file(artifact) if artifact else '', 'runner': 'mac-local', 'nodes': nodes or []}

def run_check(policy, check_id, sha, tree, ac_ref, out_dir, runner='mac-local'):
    """Execute against exact candidate code with a trusted AC overlay and scrubbed env."""
    from . import verification_contract as vc
    start = time.time()
    tree, out = (Path(tree), Path(out_dir))
    out.mkdir(parents=True, exist_ok=True)
    check = next((c for c in policy['checks'] if c['id'] == check_id), None)
    argv, env, version, overlay_hash = ([], {}, '', '')
    exit_code = None
    touched, original = (set(), {})

    def finish(outcome, reason='', counts=None, nodes=None, artifact=None, mutants=None):
        result = _finish(check, sha, start, outcome, reason, argv, env, counts, nodes, artifact)
        result.update(interpreter=version, exit_code=exit_code, overlay_digest=overlay_hash, runner=runner, observed_sha=_git_head(tree), verifier_digest=vc.verifier_digest(Path(__file__).parent))
        if mutants is not None:
            result['mutants'] = mutants
        return result
    try:
        if check is None:
            return finish('error', 'check_not_found')
        if not SHA.fullmatch(sha) or _git_head(tree) != sha:
            return finish('sha_mismatch', 'checkout HEAD is not requested sha')
        if _git_dirty(tree):
            return finish('dirty_tree', 'checkout was dirty before execution')
        spec = policy['interpreters'].get(check.get('interpreter') or 'py')
        if not spec:
            return finish('error', 'unknown interpreter')
        ipath = os.path.expanduser(spec['path'])
        if not os.path.isfile(ipath):
            return finish('error', 'interpreter missing')
        ver = _run([ipath, '--version'], tree, {'PATH': '/usr/bin:/bin'}, 10)
        if ver is None or not ver.stdout.startswith('Python ' + spec['version_prefix']):
            return finish('error', 'interpreter version mismatch')
        version = ver.stdout.strip() + ' @' + _sha256_bytes(ipath)[:10]
        if not isinstance(check.get('argv'), list) or not check['argv']:
            return finish('error', 'check has no argv')
        if ac_ref:
            original = vc.protected_manifest(tree, sha, policy.get('protected', []))
            touched = set(original) | set(vc.protected_manifest(tree, ac_ref, policy.get('protected', [])))
            overlay_hash = vc.overlay(tree, ac_ref, policy.get('protected', []))
        before = vc.tree_fingerprint(tree)
        cwd = (tree / check.get('cwd', '.')).resolve()
        if tree.resolve() not in (cwd, *cwd.parents):
            return finish('error', 'check cwd escapes tree')
        argv = [_substitute(a, ipath, out) for a in check['argv']]
        env = _build_env(policy, spec, tree, out)
        result = _run(argv, cwd, env, check.get('timeout_s', 300))
        if result is None:
            return finish('timeout')
        exit_code = result.returncode
        dirty = vc.tree_fingerprint(tree) != before or _git_head(tree) != sha
        if check.get('kind') == 'mutation':
            summary = _read_summary(out / 'summary.json')
            outcome, reason = _classify_mutation(result, summary)
            if dirty:
                outcome, reason = ('dirty_tree', 'check changed tree')
            return finish(outcome, reason, summary.get('totals', {}) if summary else {}, artifact=out / 'summary.json' if (out / 'summary.json').exists() else None, mutants=summary.get('mutants', []) if summary else [])
        junit = out / 'junit.xml'
        counts, nodes = _parse_junit(junit) if check.get('junit') else ({}, [])
        outcome, reason = _classify(check, result, counts, dirty)
        return finish(outcome, reason, counts, nodes, junit if junit.exists() else None)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return finish('error', type(error).__name__)
    finally:
        if touched:
            for path in touched:
                target = tree / path
                if any((parent.is_symlink() for parent in [target, *target.parents])):
                    continue
                if path in original:
                    mode, blob = original[path]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(vc.git(tree, 'cat-file', 'blob', blob))
                    target.chmod(493 if mode == '100755' else 420)
                else:
                    target.unlink(missing_ok=True)

def _classify(check, result, counts, dirty):
    if dirty:
        return ('dirty_tree', 'tree changed during the check')
    if result.returncode != 0:
        return ('fail', 'exit %d' % result.returncode)
    if check.get('junit'):
        if not counts or counts.get('passed', 0) < (check.get('min_tests') or 1):
            return ('vacuous', 'no test nodes collected')
        if counts.get('failed', 0) or counts.get('errors', 0):
            return ('fail', 'junit failures')
        if check.get('min_tests') and counts.get('passed', 0) < check['min_tests']:
            return ('vacuous', 'insufficient passing tests')
    return ('pass', '')

def _read_summary(path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except ValueError:
        return None

def _classify_mutation(result, summary):
    if result is None:
        return ('timeout', '')
    if result.returncode not in (0, 1) or not isinstance(summary, dict):
        return ('error', 'mutation runner failed')
    total = summary.get('totals', {})
    if not isinstance(total, dict) or not sum((total.get(k, 0) for k in ('KILLED', 'SURVIVED', 'ERROR', 'BASELINE_RED'))):
        return ('error', 'empty mutation result')
    if total.get('ERROR', 0) or total.get('BASELINE_RED', 0):
        return ('error', 'mutation baseline or runner error')
    if total.get('SURVIVED', 0):
        return ('fail', 'surviving mutants')
    return ('pass', '')

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--policy-json', required=True)
    ap.add_argument('--check', required=True)
    ap.add_argument('--sha', required=True)
    ap.add_argument('--tree', required=True)
    ap.add_argument('--ac-ref', default='')
    ap.add_argument('--out', required=True)
    args = ap.parse_args(argv)
    try:
        policy = json.loads(Path(args.policy_json).read_text())
    except (OSError, ValueError) as e:
        print(json.dumps({'outcome': 'error', 'reason': 'policy unreadable: %s' % type(e).__name__}))
        return 2
    receipt = run_check(policy, args.check, args.sha, Path(args.tree), args.ac_ref, Path(args.out))
    print(json.dumps(receipt))
    return 0 if receipt['outcome'] == 'pass' else 1
if __name__ == '__main__':
    raise SystemExit(main())
