if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import datetime
import hashlib
import json
import os
import tempfile
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
from . import outbox

def _fault(conn, point):
    """Fault injection only for an explicitly selected temporary test database."""
    if os.environ.get('STAGE34_FAULT') != point or not os.environ.get('STAGE34_CONTROLLER_DB'):
        return
    selected = Path(os.environ['STAGE34_CONTROLLER_DB']).resolve()
    temporary = Path(tempfile.gettempdir()).resolve()
    if temporary not in selected.parents and Path('/private/tmp') not in selected.parents:
        return
    actual = conn.execute('PRAGMA database_list').fetchone()[2]
    if actual and Path(actual).resolve() == selected:
        os._exit(137)

def _gh(args, timeout=60):
    return subprocess.run(['gh', *args], capture_output=True, text=True, timeout=timeout)

def _row(conn, op_id):
    r = conn.execute('SELECT * FROM ext_op WHERE id=?', (op_id,)).fetchone()
    return dict(r) if r else None

def _runs(gh, workflow, token, since):
    created = datetime.datetime.fromtimestamp(max(0, since - 60), datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    p = gh(['api', '--paginate', '--slurp', 'repos/{owner}/{repo}/actions/workflows/' + workflow + '/runs?event=workflow_dispatch&per_page=100&created=>=' + created])
    if p.returncode:
        return ('error', [])
    try:
        pages = json.loads(p.stdout)
        if not isinstance(pages, list) or not pages:
            raise ValueError('not paginated')
        found = {}
        pattern = re.compile('(?<![A-Za-z0-9-])' + re.escape(token) + '(?![A-Za-z0-9-])')
        for page in pages:
            if not isinstance(page, dict) or not isinstance(page.get('workflow_runs'), list):
                raise ValueError('incomplete page')
            for run in page['workflow_runs']:
                if pattern.search(str(run.get('display_title', ''))):
                    found[str(run['id'])] = run
        return ('found' if len(found) == 1 else 'ambiguous' if found else 'absent', list(found.values()))
    except (ValueError, KeyError, TypeError):
        return ('error', [])

def _response_id(stdout):
    try:
        value = json.loads(stdout)
        if isinstance(value, dict):
            identity = value.get('id') or value.get('workflow_run_id')
            if isinstance(identity, int) and identity > 0:
                return str(identity)
    except (ValueError, TypeError):
        pass
    hit = re.search('https://[^\\s]+/actions/runs/(\\d+)\\b', stdout or '')
    return hit.group(1) if hit else ''

def _probe_operation(gh, op, request):
    """Kind-specific read-only probes. Never use missing/error output as absence."""
    if request.get('_workflow'):
        outcome, runs = _runs(gh, request['_workflow'], op['id'], op['sent_at'] or op['created_at'])
        return (outcome, str(runs[0]['id']) if len(runs) == 1 else '', len(runs))
    if op['kind'] in ('gh_push_branch', 'gh_push_verify'):
        p = gh(['api', 'repos/{owner}/{repo}/git/ref/heads/' + op['target'], '--jq', '.object.sha'])
        if p.returncode == 0:
            return ('found' if p.stdout.strip().strip('"') == request.get('sha') else 'ambiguous', op['target'], 1)
        return ('absent' if re.search('\\bHTTP 404\\b', p.stderr or '') else 'error', '', 0)
    if op['kind'] == 'gh_pr_create':
        branch = urllib.parse.quote(op['target'], safe='')
        p = gh(['api', '--paginate', '--slurp', 'repos/{owner}/{repo}/pulls?state=all&per_page=100&head={owner}:' + branch])
        if p.returncode:
            return ('error', '', 0)
        pages = json.loads(p.stdout)
        if not isinstance(pages, list) or not pages or any((not isinstance(page, list) for page in pages)):
            return ('error', '', 0)
        marker = '<!-- stage34-op:' + op['id'] + ' -->'
        matches = {str(row['number']) for page in pages for row in page if marker in str(row.get('body') or '')}
        return ('found' if len(matches) == 1 else 'ambiguous' if matches else 'absent', next(iter(matches)) if len(matches) == 1 else '', len(matches))
    if op['kind'] == 'gh_pr_merge':
        p = gh(['pr', 'view', op['target'], '--json', 'state,mergeCommit,headRefOid'])
        data = json.loads(p.stdout) if not p.returncode else {}
        if data.get('state') == 'MERGED':
            return ('found' if data.get('headRefOid') == request.get('head_sha') else 'ambiguous', op['target'], 1)
        if data.get('state') == 'OPEN' and data.get('headRefOid') == request.get('head_sha'):
            return ('absent', '', 0)
        return ('ambiguous' if data else 'error', '', 0)
    if op['kind'] == 'gh_run_cancel':
        p = gh(['run', 'view', op['target'], '--json', 'status,conclusion'])
        data = json.loads(p.stdout) if not p.returncode else {}
        if data.get('status') == 'completed' and data.get('conclusion') == 'cancelled':
            return ('found', op['target'], 1)
        return ('absent' if data.get('status') else 'error', '', 0)
    return ('error', '', 0)

def reconcile_operation(conn, op_id, gh=None, now=None):
    gh = gh or _gh
    now = time.time() if now is None else now
    op = _row(conn, op_id)
    if not op or op['state'] not in ('intent', 'sending', 'unknown', 'reconciled_absent'):
        return op
    request = json.loads(op['request_digest'])
    if op['state'] == 'intent' or (op['state'] == 'sending' and now - (op['sent_at'] or now) <= outbox.SENDING_TTL_S):
        return op
    try:
        outcome, remote, count = _probe_operation(gh, op, request)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError):
        outcome, remote, count = ('error', '', 0)
    if outcome == 'absent' and now < (op['reconcile_after'] or 0):
        return op
    outbox.reconcile(conn, op_id, outcome, method='github-all-pages', remote_id=remote, observed=str(count), now=now, epoch=op['sends'])
    return _row(conn, op_id)

def dispatch_workflow(conn, kind, workflow, inputs, request, *, task=None, attempt=None, gen=None, op_token=None, gh=None, now=None, ref='main', token_field='op_token', before_send=None):
    """Return the op row; only `confirmed` means dispatch was acknowledged/reconciled.

    Repeated calls never allocate another authority. Retrying an uncertain call runs only a
    read probe until the outbox has two current-send absence proofs.
    """
    gh = gh or _gh
    now = time.time() if now is None else now
    snapshot = dict(request, _workflow=workflow, _ref=ref, _inputs_digest=hashlib.sha256(json.dumps(inputs, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
    prior = _row(conn, op_token) if op_token else None
    if prior:
        previous = json.loads(prior['request_digest'])
        compatible = previous == snapshot or (previous.get('legacy_import') and kind == 'gh_dispatch_verify' and (previous.get('verify_id') == op_token) and (previous.get('target_sha') == request.get('target_sha')))
        if prior['kind'] != kind or prior['target'] != workflow or (not compatible):
            raise ValueError('operation_contract_changed')
        op_id = op_token
    else:
        intent = outbox.intent(conn, kind, workflow, snapshot, task=task, attempt=attempt, gen=gen, op_token=op_token, now=now)
        if not intent.ok:
            raise ValueError(intent.code)
        op_id = intent.task
    _fault(conn, 'after_intent')
    op = reconcile_operation(conn, op_id, gh=gh, now=now)
    if op['state'] not in ('intent', 'reconciled_absent'):
        return op
    release = before_send(op['id']) if before_send else None
    claim = outbox.mark_sending(conn, op['id'], now=now)
    if not claim.ok:
        if release:
            release()
        return _row(conn, op['id'])
    fields = dict(inputs)
    if token_field == 'op':
        fields['op'] = json.dumps({'token': op['id'], 'base_sha': request['base_sha'], 'gen': gen, 'budget_usd': request.get('budget_usd', 30)}, separators=(',', ':'))
    elif token_field:
        fields[token_field] = op['id']
    _fault(conn, 'before_send')
    args = ['workflow', 'run', workflow, '--ref', ref]
    for key, value in sorted(fields.items()):
        args.extend(['-f', key + '=' + str(value)])
    try:
        proc = gh(args)
        _fault(conn, 'after_remote')
        remote = _response_id(proc.stdout) if proc.returncode == 0 else ''
        if remote:
            outbox.settle(conn, op['id'], 'confirmed', remote_id=remote, now=now, epoch=claim.gen)
            return _row(conn, op['id'])
        outbox.settle(conn, op['id'], 'unknown', reason='dispatch_ack_unknown', now=now, epoch=claim.gen)
    except (OSError, subprocess.SubprocessError):
        outbox.settle(conn, op['id'], 'unknown', reason='dispatch_transport_unknown', now=now, epoch=claim.gen)
    finally:
        if release:
            release()
    return reconcile_operation(conn, op['id'], gh=gh, now=now)

def ensure_ref(conn, branch, sha, *, gh=None, now=None, parent_op=None):
    """Idempotent create-only branch publication with read-after-ambiguous-write verification."""
    gh = gh or _gh
    now = time.time() if now is None else now
    if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9/_-]{0,199}', branch) or '..' in branch or (not re.fullmatch('[0-9a-f]{40}', sha)):
        raise ValueError('invalid reference')
    r = outbox.intent(conn, 'gh_push_verify', branch, {'sha': sha}, now=now)
    if not r.ok:
        raise ValueError(r.code)
    op = _row(conn, r.task)
    if parent_op:
        conn.execute('UPDATE ext_op SET parent_op=? WHERE id=? AND parent_op IS NULL', (parent_op, op['id']))
    if op['state'] in ('failed', 'conflict', 'abandoned'):
        return False
    if op['state'] == 'confirmed':
        try:
            current = gh(['api', 'repos/{owner}/{repo}/git/ref/heads/' + branch, '--jq', '.object.sha'])
        except (OSError, subprocess.SubprocessError):
            return False
        if current.returncode == 0 and current.stdout.strip().strip('"') == sha:
            return True
        if current.returncode == 0 or re.search('\\bHTTP 404\\b', current.stderr or ''):
            conn.execute("UPDATE ext_op SET state='conflict',reason='reference_changed' WHERE id=? AND state='confirmed' AND sends=?", (op['id'], op['sends']))
        return False
    if op['state'] == 'intent':
        claim = outbox.mark_sending(conn, op['id'], now=now)
        if not claim.ok:
            return False
        try:
            p = gh(['api', 'repos/{owner}/{repo}/git/refs', '--method', 'POST', '-f', 'ref=refs/heads/' + branch, '-f', 'sha=' + sha])
            if p.returncode == 0:
                outbox.settle(conn, op['id'], 'confirmed', remote_id=branch, now=now, epoch=claim.gen)
                return True
        except (OSError, subprocess.SubprocessError):
            pass
        outbox.settle(conn, op['id'], 'unknown', reason='reference_ack_unknown', now=now, epoch=claim.gen)
        op = _row(conn, op['id'])
    try:
        p = gh(['api', 'repos/{owner}/{repo}/git/ref/heads/' + branch, '--jq', '.object.sha'])
        if p.returncode == 0:
            actual = p.stdout.strip().strip('"')
            result = 'found' if actual == sha else 'ambiguous'
            outbox.reconcile(conn, op['id'], result, remote_id=branch, now=now, epoch=op['sends'])
            return result == 'found'
    except (OSError, subprocess.SubprocessError):
        pass
    return False

def cancel_run(conn, run_id, gh=None, now=None):
    gh = gh or _gh
    now = time.time() if now is None else now
    if not re.fullmatch('[1-9][0-9]{0,19}', str(run_id)):
        raise ValueError('invalid run id')
    r = outbox.intent(conn, 'gh_run_cancel', str(run_id), {'run_id': str(run_id)}, now=now)
    if not r.ok:
        raise ValueError(r.code)
    op = reconcile_operation(conn, r.task, gh=gh, now=now)
    if op['state'] == 'confirmed':
        return op
    if op['state'] in ('intent', 'reconciled_absent'):
        claim = outbox.mark_sending(conn, op['id'], now=now)
        if not claim.ok:
            return _row(conn, op['id'])
        try:
            proc = gh(['run', 'cancel', str(run_id)])
            outcome = 'confirmed' if proc.returncode == 0 else 'unknown'
        except (OSError, subprocess.SubprocessError):
            outcome = 'unknown'
        outbox.settle(conn, op['id'], outcome, remote_id=str(run_id), now=now, epoch=claim.gen)
    op = _row(conn, op['id'])
    if op['state'] == 'unknown':
        try:
            p = gh(['run', 'view', str(run_id), '--json', 'status,conclusion'])
            status = json.loads(p.stdout) if not p.returncode else {}
            if status.get('status') == 'completed' and status.get('conclusion') == 'cancelled':
                outbox.reconcile(conn, op['id'], 'found', remote_id=str(run_id), now=now, epoch=op['sends'])
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
    return _row(conn, op['id'])

def record_drill(conn, op_id, gh=None, now=None):
    """Record only an observed successful no-AI drill plus immutable workflow contract proof."""
    import base64
    gh = gh or _gh
    now = time.time() if now is None else now
    op = _row(conn, op_id)
    if not op or op['kind'] != 'gh_dispatch_drill' or op['state'] != 'confirmed':
        return False
    try:
        p = gh(['run', 'view', op['remote_id'], '--json', 'databaseId,status,conclusion,workflowName,displayTitle,headSha'])
        data = json.loads(p.stdout) if not p.returncode else {}
        if str(data.get('databaseId')) != op['remote_id'] or data.get('status') != 'completed' or data.get('conclusion') != 'success' or (data.get('workflowName') != 'Orchestrator drill (no AI)') or (data.get('displayTitle') != 'drill ' + op_id) or (not re.fullmatch('[0-9a-f]{40}', data.get('headSha', ''))):
            return False
        blob = gh(['api', 'repos/{owner}/{repo}/contents/.github/workflows/agent-task.yml', '--method', 'GET', '-f', 'ref=' + data['headSha'], '--jq', '.content'])
        source = base64.b64decode(blob.stdout, validate=False).decode() if not blob.returncode else ''
        if not re.search('(?m)^      op:\\s*$', source):
            return False
        evidence = {'completed_at': now, 'conclusion': 'success', 'workflow_op_input': True, 'run_id': op['remote_id'], 'workflow_sha': data['headSha']}
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('adapter:pc-vm:drill',?)", (json.dumps(evidence),))
        return True
    except (OSError, subprocess.SubprocessError, ValueError, TypeError):
        return False

def main(argv=None):
    import argparse
    from . import controller
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest='cmd', required=True)
    d = sub.add_parser('dispatch')
    d.add_argument('--kind', choices=('gh_dispatch_suite', 'gh_dispatch_drill'), required=True)
    d.add_argument('--workflow', required=True)
    d.add_argument('--ref', default='main')
    d.add_argument('--input', action='append', default=[])
    r = sub.add_parser('reconcile')
    r.add_argument('operation')
    c = sub.add_parser('cancel')
    c.add_argument('run')
    proof = sub.add_parser('drill-proof')
    proof.add_argument('operation')
    args = ap.parse_args(argv)
    conn = controller.open_db('rw', controller.CLI_TIMEOUT)
    try:
        outbox.migrate(conn)
        if args.cmd == 'dispatch':
            allowed = {'gh_dispatch_suite': ('test-backend.yml', 'test-frontend.yml'), 'gh_dispatch_drill': ('orchestrator-drill.yml',)}
            if args.workflow not in allowed[args.kind]:
                ap.error('workflow is not allowed for this kind')
            inputs = {}
            for item in args.input:
                key, sep, value = item.partition('=')
                if not sep or not re.fullmatch('[a-z_]{1,40}', key):
                    ap.error('input must be key=value')
                inputs[key] = value
            op = dispatch_workflow(conn, args.kind, args.workflow, inputs, {'inputs': inputs}, ref=args.ref)
        elif args.cmd == 'cancel':
            op = cancel_run(conn, args.run)
        elif args.cmd == 'drill-proof':
            ok = record_drill(conn, args.operation)
            print('conformant' if ok else 'unproven')
            return 0 if ok else 2
        else:
            op = reconcile_operation(conn, args.operation)
        if op and op['state'] == 'confirmed':
            print(op['remote_id'])
            return 0
        print('external operation remains ' + (op['state'] if op else 'missing'))
        return 2
    finally:
        conn.close()
if __name__ == '__main__':
    raise SystemExit(main())
