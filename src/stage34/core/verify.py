if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
pass
from . import acceptance
from . import controller
from . import verification_contract as vc

def _cache_dir():
    base = os.environ.get('STAGE34_VERIFY_ROOT') or os.path.expanduser('~/.cache/stage34/verify')
    Path(base).mkdir(parents=True, exist_ok=True, mode=448)
    return base

def _worktree(sha, dest, repo=None):
    args = ['git'] + (['-C', str(repo)] if repo else [])
    return subprocess.run(args + ['worktree', 'add', '--detach', dest, sha], capture_output=True).returncode == 0

def _remove_worktree(dest, repo=None):
    args = ['git'] + (['-C', str(repo)] if repo else [])
    subprocess.run(args + ['worktree', 'remove', '--force', dest], capture_output=True)

def _verify_exec(policy_path, check_id, sha, tree, ac_ref, out):
    return subprocess.run([sys.executable, str(Path(__file__).parent / 'verify_exec.py'), '--policy-json', str(policy_path), '--check', check_id, '--sha', sha, '--tree', str(tree), '--ac-ref', ac_ref, '--out', str(out)], capture_output=True, text=True)

def _verifier_digest():
    return vc.verifier_digest(Path(__file__).parent)

def execute_run(conn, candidate, pbody, target_sha, checks, repo, purpose='candidate', ac_ref=''):
    """One durable intent and independent checkout. No candidate-state mutation here."""
    run = acceptance.record_run_intent(conn, candidate, 'local', purpose, target_sha, _verifier_digest())
    vc.put(conn, 'run', run, {'ac_ref': ac_ref, 'checks': list(checks), 'policy': pbody})
    conn.execute("UPDATE verify_run SET state='running', started_at=? WHERE id=?", (time.time(), run))
    dest = os.path.join(_cache_dir(), run)
    if not _worktree(target_sha, dest, repo):
        conn.execute("UPDATE verify_run SET state='error',reason='worktree',ended_at=? WHERE id=?", (time.time(), run))
        return (run, 'error')
    try:
        policy_path = Path(_cache_dir()) / (run + '-policy.json')
        policy_path.write_text(json.dumps(pbody))
        policy_path.chmod(384)
        c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
        for check_id in checks:
            check = next((ch for ch in pbody['checks'] if ch['id'] == check_id))
            out = Path(_cache_dir()) / (run + '-' + check_id)
            if check.get('kind') == 'mutation' and check.get('spec_from') == 'review':
                binding = vc.get(conn, 'mutation', candidate)
                out.mkdir(parents=True, exist_ok=True, mode=448)
                if binding.get('candidate_sha') != target_sha:
                    receipt = {'candidate': candidate, 'target_sha': target_sha, 'policy': c['policy'], 'check_digest': acceptance.check_digest(check), 'lane': 'local', 'outcome': 'error', 'duration_s': 0}
                    acceptance.record_receipt(conn, run, check_id, receipt)
                    continue
                (out / 'mutation-spec.json').write_text(json.dumps(binding['body']))
                (out / 'mutation_batch.py').write_bytes(vc.git(repo, 'show', binding['ref'] + ':scripts/quality/mutation_batch.py'))
            proc = _verify_exec(policy_path, check_id, target_sha, dest, ac_ref, out)
            try:
                receipt = json.loads(proc.stdout)
                if not isinstance(receipt, dict) or (proc.returncode and receipt.get('outcome') == 'pass'):
                    raise ValueError('bad executor result')
            except ValueError:
                receipt = {'outcome': 'error', 'duration_s': 0}
            receipt.update(candidate=candidate, target_sha=target_sha, policy=c['policy'], lane='local')
            receipt.setdefault('check_digest', acceptance.check_digest(check))
            receipt.setdefault('duration_s', 0)
            acceptance.record_receipt(conn, run, check_id, receipt)
            if receipt.get('mutants'):
                from . import finding
                finding.open_mutation_findings(conn, c['task'], candidate, {'mutants': receipt['mutants']})
        conn.execute("UPDATE verify_run SET state='done',ended_at=? WHERE id=?", (time.time(), run))
        return (run, 'done')
    finally:
        _remove_worktree(dest, repo)

def run_candidate(conn, candidate, checks=None, lane='local', now=None, repo=None):
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    if not c or c['state'] not in ('submitted', 'verified', 'failed', 'verifying') or lane != 'local':
        return 'refused'
    pbody = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c['policy'],)).fetchone()[0])
    chosen = [ch['id'] for ch in pbody['checks'] if ch['lane'] == 'local' and (not checks or ch['id'] in checks)]
    if not chosen or (checks and set(chosen) != set(checks)):
        return 'lane_forbidden'
    conn.execute("UPDATE candidate SET state='verifying' WHERE id=?", (candidate,))
    ref = vc.ac_ref(conn, c['task'])
    run, state = execute_run(conn, candidate, pbody, c['head_sha'], chosen, repo, ac_ref=ref)
    rows = conn.execute('SELECT outcome FROM receipt WHERE run=?', (run,)).fetchall()
    t = conn.execute('SELECT role FROM task WHERE id=?', (c['task'],)).fetchone()
    if t[0] in controller.PROFILE.verifier_roles:
        rows = conn.execute('SELECT check_id,outcome,counts FROM receipt WHERE run=?', (run,)).fetchall()
        valid = all((r['outcome'] == 'pass' or (r['outcome'] == 'fail' and next((ch for ch in pbody['checks'] if ch['id'] == r['check_id'])).get('protected_suite') and (json.loads(r['counts']).get('failed', 0) > 0) and (not json.loads(r['counts']).get('errors', 0))) for r in rows))
    else:
        valid = all((r[0] == 'pass' for r in rows))
    state = 'verified' if state == 'done' and rows and valid else 'failed'
    conn.execute("UPDATE candidate SET state=? WHERE id=? AND state='verifying'", (state, candidate))
    return state

def red_first(conn, candidate, repo=None):
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    t = conn.execute('SELECT * FROM task WHERE id=?', (c['task'],)).fetchone() if c else None
    if not t or t['role'] not in controller.PROFILE.verifier_roles:
        return 'refused'
    p = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c['policy'],)).fetchone()[0])
    checks = [ch['id'] for ch in p['checks'] if ch.get('protected_suite') and ch['lane'] == 'local']
    vc.put(conn, 'ac-red', candidate, {})
    if not checks:
        return 'refused'
    run, state = execute_run(conn, candidate, p, c['base_sha'], checks, repo, purpose='finding_repro', ac_ref=c['head_sha'])
    red = conn.execute("SELECT * FROM receipt WHERE run=? AND outcome='fail' ORDER BY created_at", (run,)).fetchall()
    if state == 'done' and red and all((json.loads(r['counts']).get('failed', 0) > 0 for r in red)) and (len(red) == len(checks)):
        vc.put(conn, 'ac-red', candidate, {'receipt': red[0]['id'], 'receipts': [r['id'] for r in red], 'ac_sha': c['head_sha']})
        return 'red'
    return 'ac_not_red'

def reconcile(conn, now=None):
    now = time.time() if now is None else now
    for row in conn.execute("SELECT * FROM verify_run WHERE state='running' AND lane='local'").fetchall():
        c = conn.execute('SELECT policy FROM candidate WHERE id=?', (row['candidate'],)).fetchone()
        p = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c[0],)).fetchone()[0])
        budget = sum((ch.get('timeout_s', 1800) for ch in p['checks'])) + 60
        if row['started_at'] and now - row['started_at'] > budget:
            conn.execute("UPDATE verify_run SET state='lost',ended_at=?,reason='timeout' WHERE id=?", (now, row['id']))

def invalidate(conn, receipt_ids, reason):
    for rid in receipt_ids:
        conn.execute('UPDATE receipt SET stale=1,stale_reason=? WHERE id=?', (reason, rid))
        conn.execute("UPDATE candidate SET state='invalidated',reason=? WHERE state='accepted' AND id IN (SELECT candidate FROM receipt WHERE id=?)", (reason, rid))

def invalidate_between(conn, repo, before, after):
    paths = [p.decode() for p in vc.git(repo, 'diff', '--name-only', '-z', before, after).split(b'\x00') if p]
    stale = []
    for rec in conn.execute('SELECT * FROM receipt WHERE stale=0').fetchall():
        c = conn.execute('SELECT * FROM candidate WHERE id=?', (rec['candidate'],)).fetchone()
        p = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c['policy'],)).fetchone()[0])
        check = next((ch for ch in p['checks'] if ch['id'] == rec['check_id']), None)
        files = [r[0] for r in conn.execute('SELECT path FROM candidate_file WHERE candidate=?', (c['id'],))]
        if check and any((vc.matches(check['watch'], path) or path in files for path in paths)):
            stale.append(rec['id'])
    invalidate(conn, stale, 'source')
    return len(stale)

def integrate(conn, branch, repo, lane='local'):
    """Execute accepted packages' integration policy against the exact combined tree."""
    sha = vc.git(repo, 'rev-parse', branch + '^{commit}').decode().strip()
    prev = conn.execute('SELECT sha FROM integration WHERE branch=? ORDER BY created_at DESC LIMIT 1', (branch,)).fetchone()
    identity = 'I-' + vc.digest([branch, sha])[:10]
    accepted = []
    for c in conn.execute("SELECT * FROM candidate WHERE state='accepted'").fetchall():
        if subprocess.run(['git', '-C', str(repo), 'merge-base', '--is-ancestor', c['head_sha'], sha], capture_output=True).returncode == 0:
            accepted.append(c)
    if not accepted:
        return 'refused'
    conn.execute("INSERT OR IGNORE INTO integration(id,branch,sha,prev_sha,candidates,state,created_at) VALUES (?,?,?,?,?,'pending',?)", (identity, branch, sha, prev[0] if prev else '', json.dumps([c['id'] for c in accepted]), time.time()))
    conn.execute("UPDATE integration SET state='verifying' WHERE id=?", (identity,))
    contexts, failed = ([], [])
    for c in accepted:
        p = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c['policy'],)).fetchone()[0])
        checks = [ch for ch in p['checks'] if ch['id'] in p['integration']['checks']]
        if len(checks) != len(p['integration']['checks']) or not checks or any((ch['lane'] != lane for ch in checks)):
            failed.append(c['id'])
            continue
        if lane != 'local':
            from . import verify_pc
            run = verify_pc.dispatch(conn, c['id'], [ch['id'] for ch in checks], purpose='integration', target_sha=sha)
            if not run:
                failed.append(c['id'])
                continue
            contexts.append({'candidate': c['id'], 'run': run, 'checks': [ch['id'] for ch in checks]})
            continue
        run, state = execute_run(conn, c['id'], p, sha, [ch['id'] for ch in checks], repo, purpose='integration', ac_ref=vc.ac_ref(conn, c['task']))
        contexts.append({'candidate': c['id'], 'run': run, 'checks': [ch['id'] for ch in checks]})
        rows = conn.execute('SELECT outcome FROM receipt WHERE run=?', (run,)).fetchall()
        if state != 'done' or not rows or any((r[0] != 'pass' for r in rows)):
            failed.append(c['id'])
            from . import finding
            finding.open_finding(conn, c['task'], c['id'], 'important', 'verifier', ref=identity, phase='post_accept', repro='integration')
    vc.put(conn, 'integration', identity, {'runs': contexts, 'failed': failed})
    state = 'failed' if failed else 'verifying' if lane == 'pc' else 'verified'
    conn.execute('UPDATE integration SET state=? WHERE id=?', (state, identity))
    return state

def collect_integration(conn, identity, gh=None):
    row = conn.execute('SELECT * FROM integration WHERE id=?', (identity,)).fetchone()
    context = vc.get(conn, 'integration', identity)
    if not row or not context.get('runs'):
        return 'refused'
    pending = False
    required = set(json.loads(row['candidates']))
    reported = [entry['candidate'] for entry in context['runs']]
    failed = set(context.get('failed', [])) | required - set(reported)
    if set(reported) - required or len(reported) != len(set(reported)):
        failed.add('integration_manifest_mismatch')
    from . import verify_pc
    for entry in context['runs']:
        bound_run = conn.execute('SELECT * FROM verify_run WHERE id=?', (entry.get('run'),)).fetchone()
        run_context = vc.get(conn, 'run', entry['run']) if entry.get('run') else {}
        if not bound_run or bound_run['purpose'] != 'integration' or bound_run['candidate'] != entry['candidate'] or (bound_run['target_sha'] != row['sha']) or (set(run_context.get('checks', [])) != set(entry['checks'])):
            failed.add(entry['candidate'])
            continue
        result = verify_pc.collect(conn, entry['candidate'], gh=gh, run_id=entry['run'])
        if result == 'pending':
            pending = True
        elif result != 'verified':
            failed.add(entry['candidate'])
            c = conn.execute('SELECT * FROM candidate WHERE id=?', (entry['candidate'],)).fetchone()
            from . import finding
            if not conn.execute('SELECT 1 FROM finding WHERE candidate=? AND ref=?', (c['id'], identity)).fetchone():
                finding.open_finding(conn, c['task'], c['id'], 'important', 'verifier', ref=identity, phase='post_accept', repro='integration')
    state = 'failed' if failed else 'verifying' if pending else 'verified'
    conn.execute('UPDATE integration SET state=? WHERE id=?', (state, identity))
    return state

def status(conn, candidate):
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    if not c:
        return {'code': 'candidate_not_found'}
    verdict = acceptance.evaluate(conn, candidate)
    runs = [dict(r) for r in conn.execute('SELECT id,lane,purpose,state,target_sha,remote_ref FROM verify_run WHERE candidate=? ORDER BY intent_at', (candidate,))]
    return {'candidate': candidate, 'sha': c['head_sha'], 'state': c['state'], 'missing': verdict.missing, 'runs': runs}

def doctor(conn):
    problems = []
    for row in conn.execute('SELECT body FROM policy'):
        p = json.loads(row[0])
        for key, spec in p['interpreters'].items():
            if not Path(os.path.expanduser(spec['path'])).is_file():
                problems.append('interpreter:' + key)
    return problems

def main(argv=None):
    ap = argparse.ArgumentParser(prog='verify.py')
    sub = ap.add_subparsers(dest='cmd', required=True)
    for name in ('run', 'red-first'):
        p = sub.add_parser(name)
        p.add_argument('candidate')
        p.add_argument('--repo')
        p.add_argument('--checks', nargs='*')
    p = sub.add_parser('status')
    p.add_argument('candidate')
    sub.add_parser('reconcile')
    sub.add_parser('doctor')
    p = sub.add_parser('invalidate')
    p.add_argument('--from', dest='before', required=True)
    p.add_argument('--to', dest='after', required=True)
    p.add_argument('--repo', default='.')
    p = sub.add_parser('integrate')
    p.add_argument('--branch', required=True)
    p.add_argument('--repo', default='.')
    p.add_argument('--lane', choices=('local', 'pc'), default='pc')
    p = sub.add_parser('collect-integration')
    p.add_argument('identity')
    args = ap.parse_args(argv)
    conn = controller.open_db('rw', controller.CLI_TIMEOUT)
    try:
        if args.cmd == 'run':
            result = run_candidate(conn, args.candidate, args.checks, repo=args.repo)
        elif args.cmd == 'red-first':
            result = red_first(conn, args.candidate, args.repo)
        elif args.cmd == 'integrate':
            result = integrate(conn, args.branch, args.repo, args.lane)
        elif args.cmd == 'collect-integration':
            result = collect_integration(conn, args.identity)
        elif args.cmd == 'invalidate':
            result = invalidate_between(conn, args.repo, args.before, args.after)
        elif args.cmd == 'doctor':
            result = doctor(conn)
        elif args.cmd == 'status':
            result = status(conn, args.candidate)
        else:
            reconcile(conn)
            result = 'reconciled'
        print(json.dumps(result))
        return 0 if result not in ('failed', 'refused', 'ac_not_red') else 1
    finally:
        conn.close()
if __name__ == '__main__':
    raise SystemExit(main())
