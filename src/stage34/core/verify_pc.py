if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import base64
import hashlib
import json
import math
import re
import subprocess
import tempfile
import time
from pathlib import Path
from . import acceptance
from . import controller
from . import verification_contract as vc
from . import outbox
from . import durable_transport
V_ID = re.compile('V-[a-f0-9]{10}')
SHA = re.compile('[a-f0-9]{40}')
CHECK_ID = re.compile('[a-z0-9-]{1,64}')
TRUSTED_REF = controller.PROFILE.verifier_trusted_ref
WORKFLOW = controller.PROFILE.verifier_workflow

def _gh(args, timeout=60):
    return subprocess.run(['gh', *args], capture_output=True, text=True, timeout=timeout)

def _verifier_digest():
    return vc.verifier_digest(Path(__file__).parent)

def _find_run_id(gh, verify_id):
    p = gh(['run', 'list', '--workflow', WORKFLOW, '--json', 'databaseId,displayTitle', '--limit', '100'])
    if p.returncode:
        return ''
    try:
        matches = [str(r['databaseId']) for r in json.loads(p.stdout) if r.get('displayTitle') == 'verify ' + verify_id]
        return matches[0] if len(matches) == 1 else ''
    except (ValueError, TypeError, KeyError):
        return ''

def approved_revision(gh, ref):
    p = gh(['api', 'repos/{owner}/{repo}/commits/' + ref, '--jq', '.sha'])
    sha = p.stdout.strip().strip('"') if not p.returncode else ''
    if not SHA.fullmatch(sha):
        raise ValueError('approved verifier revision unavailable')
    h = hashlib.sha256()
    for name in vc.VERIFIER_FILES:
        blob = gh(['api', 'repos/{owner}/{repo}/contents/' + controller.PROFILE.verifier_source_root + '/' + name, '-f', 'ref=' + sha, '--method', 'GET', '--jq', '.content'])
        if blob.returncode:
            raise ValueError('approved verifier source unavailable')
        h.update(name.encode() + b'\x00' + base64.b64decode(blob.stdout))
    return (sha, h.hexdigest())

def reconcile(conn, run_id=None, gh=None):
    """Mirror the outbox's reconciled dispatch; never infer a second resend authority."""
    gh = gh or _gh
    outbox.migrate(conn)
    rows = conn.execute("SELECT * FROM verify_run WHERE lane='pc' AND state IN ('intent','running')" + (' AND id=?' if run_id else ''), (run_id,) if run_id else ()).fetchall()
    for row in rows:
        if row['ext_op']:
            op = durable_transport.reconcile_operation(conn, row['ext_op'], gh=gh)
            if op and op['state'] == 'confirmed':
                conn.execute("UPDATE verify_run SET state='running',remote_ref=?,started_at=COALESCE(started_at,?) WHERE id=?", (op['remote_id'], time.time(), row['id']))
            elif op and op['state'] in ('failed', 'conflict', 'abandoned'):
                conn.execute("UPDATE verify_run SET state='error',reason=? WHERE id=?", ('outbox_' + op['state'], row['id']))

def _send_run(conn, run, pbody, gh, now):
    row = conn.execute('SELECT * FROM verify_run WHERE id=?', (run,)).fetchone()
    context = vc.get(conn, 'run', run)
    inputs = {'verify_id': run, 'sha': row['target_sha'], 'checks': ','.join(context['checks']), 'policy_json': json.dumps(pbody), 'verifier_sha': context['verifier_sha'], 'ac_ref': context['ac_ref']}
    for branch, sha in [(row['remote_branch'], row['target_sha']), (context['runtime_ref'], context['verifier_sha'])]:
        if not durable_transport.ensure_ref(conn, branch, sha, gh=gh, now=now):
            return run
    op = durable_transport.dispatch_workflow(conn, 'gh_dispatch_verify', WORKFLOW, inputs, {'verify_id': run, 'target_sha': row['target_sha'], 'verifier_sha': context['verifier_sha'], 'checks': context['checks'], 'policy_digest': context['policy_digest']}, op_token=run, gh=gh, now=now, ref=context['runtime_ref'], token_field='verify_id')
    conn.execute('UPDATE verify_run SET ext_op=? WHERE id=?', (op['id'], run))
    if op['state'] == 'confirmed':
        conn.execute("UPDATE verify_run SET state='running',remote_ref=?,started_at=COALESCE(started_at,?) WHERE id=?", (op['remote_id'], now, run))
    elif op['state'] in ('failed', 'conflict', 'abandoned'):
        conn.execute("UPDATE verify_run SET state='error',reason=? WHERE id=?", ('outbox_' + op['state'], run))
    return run

def dispatch(conn, candidate, checks=None, now=None, gh=None, ref=TRUSTED_REF, purpose='candidate', target_sha=None):
    gh = gh or _gh
    now = time.time() if now is None else now
    outbox.migrate(conn)
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    if not c or purpose not in ('candidate', 'integration', 'finding_repro'):
        return None
    if purpose == 'candidate' and c['state'] not in ('submitted', 'verifying', 'verified', 'failed'):
        return None
    target = target_sha or c['head_sha']
    pbody = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (c['policy'],)).fetchone()[0])
    chosen = [ch['id'] for ch in pbody['checks'] if ch['lane'] == 'pc' and (not checks or ch['id'] in checks)]
    if not SHA.fullmatch(target) or not chosen or any((not CHECK_ID.fullmatch(x) for x in chosen)) or (checks and set(checks) != set(chosen)):
        return None
    for old in conn.execute("SELECT * FROM verify_run WHERE candidate=? AND target_sha=? AND purpose=? AND lane='pc' AND state IN ('intent','running')", (candidate, target, purpose)).fetchall():
        if vc.get(conn, 'run', old['id']).get('checks') == chosen:
            reconcile(conn, old['id'], gh)
            return _send_run(conn, old['id'], pbody, gh, now)
    approved, executable_digest = approved_revision(gh, ref)
    conn.execute('BEGIN IMMEDIATE')
    try:
        old = conn.execute("SELECT id FROM verify_run WHERE candidate=? AND target_sha=? AND purpose=? AND lane='pc' AND state IN ('intent','running')", (candidate, target, purpose)).fetchone()
        if old:
            conn.execute('COMMIT')
            return old[0]
        run = acceptance.record_run_intent(conn, candidate, 'pc', purpose, target, executable_digest, now)
        context = {'checks': chosen, 'verifier_sha': approved, 'verifier_digest': executable_digest, 'policy_digest': vc.digest(pbody), 'policy': c['policy'], 'target_sha': target, 'ac_ref': vc.ac_ref(conn, c['task']), 'runtime_ref': 'verify-runtime/' + approved, 'contract': vc.contract_digest(conn, c['task'])}
        vc.put(conn, 'run', run, context)
        branch = 'verify/' + run
        conn.execute('UPDATE verify_run SET remote_branch=? WHERE id=?', (branch, run))
        conn.execute('COMMIT')
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    return _send_run(conn, run, pbody, gh, now)

def validate_receipts(receipts, pbody, head_sha, checks=None, require_pass=True):
    required = [ch for ch in pbody['checks'] if ch['lane'] == 'pc' and (not checks or ch['id'] in checks)]
    if not required or not isinstance(receipts, dict):
        return (False, 'missing')
    for check in required:
        r = receipts.get(check['id'])
        if not isinstance(r, dict):
            return (False, check['id'] + ':missing')
        if r.get('target_sha') != head_sha or r.get('observed_sha') != head_sha or r.get('check_digest') != acceptance.check_digest(check) or (r.get('lane') != 'pc'):
            return (False, check['id'] + ':provenance')
        if r.get('outcome') not in ('pass', 'fail', 'error', 'timeout', 'vacuous', 'dirty_tree', 'sha_mismatch', 'overlay_mismatch', 'weakened'):
            return (False, check['id'] + ':outcome')
        if not isinstance(r.get('duration_s'), (int, float)) or not math.isfinite(r['duration_s']) or r['duration_s'] < 0 or (not isinstance(r.get('interpreter'), str)) or (not r['interpreter']) or (not re.fullmatch('[a-f0-9]{64}', r.get('env_digest', ''))) or (not re.fullmatch('[a-f0-9]{64}', r.get('argv_digest', ''))):
            return (False, check['id'] + ':incomplete')
        if r.get('exit_code') != 0 and r.get('outcome') == 'pass':
            return (False, check['id'] + ':exit_code')
        if r.get('outcome') != 'pass':
            if require_pass:
                return (False, check['id'] + ':' + r['outcome'])
            continue
        if check.get('junit'):
            counts = r.get('counts', {})
            if not isinstance(counts, dict) or any((not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in counts.values())):
                return (False, check['id'] + ':counts')
            if counts.get('passed', 0) < (check.get('min_tests') or 1) or counts.get('failed', 0) or counts.get('errors', 0):
                return (False, check['id'] + ':vacuous')
    return (True, '')

def _read_receipts(dest):
    try:
        value = json.loads((Path(dest) / 'receipts.json').read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}

def collect(conn, candidate, now=None, gh=None, run_id=None):
    gh = gh or _gh
    now = time.time() if now is None else now
    run = conn.execute("SELECT * FROM verify_run WHERE candidate=? AND lane='pc'" + (' AND id=?' if run_id else '') + ' ORDER BY intent_at DESC LIMIT 1', (candidate, run_id) if run_id else (candidate,)).fetchone()
    if not run:
        return 'pending'
    if not run['remote_ref']:
        reconcile(conn, run['id'], gh)
        return 'pending'
    context = vc.get(conn, 'run', run['id'])
    status = gh(['run', 'view', run['remote_ref'], '--json', 'status,conclusion,headSha,workflowName,displayTitle,databaseId'])
    try:
        meta = json.loads(status.stdout) if not status.returncode else {}
    except ValueError:
        return 'pending'
    if meta.get('status') != 'completed':
        return 'pending'
    c = conn.execute('SELECT * FROM candidate WHERE id=?', (candidate,)).fetchone()
    if context.get('policy') != c['policy'] or (context.get('contract') and context['contract'] != vc.contract_digest(conn, c['task'])) or context.get('ac_ref') != vc.ac_ref(conn, c['task']):
        conn.execute("UPDATE verify_run SET state='error',reason='obsolete_context',ended_at=? WHERE id=?", (now, run['id']))
        return 'failed'
    pbody = json.loads(conn.execute('SELECT body FROM policy WHERE id=?', (context['policy'],)).fetchone()[0])
    if vc.digest(pbody) != context.get('policy_digest'):
        conn.execute("UPDATE verify_run SET state='error',reason='obsolete_policy',ended_at=? WHERE id=?", (now, run['id']))
        return 'failed'
    with tempfile.TemporaryDirectory(prefix='verify-collect-') as dest:
        got = gh(['run', 'download', run['remote_ref'], '-n', 'receipts-' + run['id'], '-D', dest])
        bundle = _read_receipts(dest) if not got.returncode else {}
    reason = ''
    if not context or meta.get('headSha') != context.get('verifier_sha') or meta.get('displayTitle') != 'verify ' + run['id'] or (str(meta.get('databaseId')) != run['remote_ref']) or (meta.get('workflowName') != controller.PROFILE.verifier_workflow_name):
        reason = 'workflow_provenance'
    expected = {'verify_id': run['id'], 'run_id': run['remote_ref'], 'verifier_sha': context.get('verifier_sha'), 'verifier_digest': context.get('verifier_digest'), 'target_sha': run['target_sha'], 'policy_digest': context.get('policy_digest')}
    if any((str(bundle.get(k)) != str(v) for k, v in expected.items())):
        reason = 'bundle_provenance'
    receipts = bundle.get('receipts', {})
    if isinstance(receipts, dict) and any((not isinstance(r, dict) or r.get('verifier_digest') != context.get('verifier_digest') for r in receipts.values())):
        reason = 'receipt_provenance'
    envelope_ok = not reason
    workflow_ok = meta.get('conclusion') == 'success'
    protocol_ok, protocol_reason = validate_receipts(receipts, pbody, run['target_sha'], context.get('checks'), require_pass=False)
    checks_ok, check_reason = validate_receipts(receipts, pbody, run['target_sha'], context.get('checks'))
    eligible_source = envelope_ok and workflow_ok and protocol_ok
    ok = eligible_source and checks_ok
    if not workflow_ok:
        reason = reason or 'workflow_failed'
    reason = reason or protocol_reason or check_reason
    if eligible_source:
        for check_id in context.get('checks', []):
            r = receipts.get(check_id)
            if not isinstance(r, dict) or r.get('target_sha') != run['target_sha'] or r.get('lane') != 'pc':
                continue
            if conn.execute('SELECT 1 FROM receipt WHERE run=? AND check_id=?', (run['id'], check_id)).fetchone():
                continue
            r = dict(r)
            r.update(candidate=candidate, policy=context['policy'])
            acceptance.record_receipt(conn, run['id'], check_id, r)
    conn.execute('UPDATE verify_run SET state=?,ended_at=?,reason=? WHERE id=?', ('done' if eligible_source else 'error', now, reason, run['id']))
    if run['purpose'] == 'candidate':
        conn.execute("UPDATE candidate SET state=? WHERE id=? AND state NOT IN ('accepted','invalidated','superseded','rejected')", ('verified' if ok else 'failed', candidate))
    return 'verified' if ok else 'failed'

def main(argv=None):
    ap = argparse.ArgumentParser(prog='verify_pc.py')
    sub = ap.add_subparsers(dest='cmd', required=True)
    for name in ('dispatch', 'collect'):
        p = sub.add_parser(name)
        p.add_argument('candidate')
    sub.add_parser('reconcile')
    args = ap.parse_args(argv)
    conn = controller.open_db('rw', controller.CLI_TIMEOUT)
    try:
        if args.cmd == 'dispatch':
            result = dispatch(conn, args.candidate)
        elif args.cmd == 'collect':
            result = collect(conn, args.candidate)
        else:
            reconcile(conn)
            result = 'reconciled'
        print(result or 'refused')
        return 0 if result not in (None, 'failed') else 1
    finally:
        conn.close()
if __name__ == '__main__':
    raise SystemExit(main())
