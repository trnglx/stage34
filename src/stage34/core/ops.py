if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import re
import subprocess
import hashlib
import sys
import time
from pathlib import Path
pass
from . import controller
from . import outbox
from . import durable_transport

def _gh(args, timeout=60):
    return subprocess.run(['gh', *args], capture_output=True, text=True, timeout=timeout)

def _git(args, timeout=60):
    return subprocess.run(['git', *args], capture_output=True, text=True, timeout=timeout)

def _settle_push(conn, op, proc, epoch=None, remote_id='', now=None):
    """Settle a `git push --force-with-lease`: success -> confirmed; stale lease -> failed (proven
    non-execution); anything else -> unknown for reconcile."""
    if proc.returncode == 0:
        return outbox.settle(conn, op, 'confirmed', remote_id=remote_id, now=now, epoch=epoch)
    err = (proc.stderr or '') + '\n' + (proc.stdout or '')
    if 'stale info' in err:
        return outbox.settle(conn, op, 'failed', reason=err.strip()[:300], now=now, epoch=epoch)
    return outbox.settle(conn, op, 'unknown', reason='git push exit %d' % proc.returncode, now=now, epoch=epoch)

def _settle_from(conn, op, proc, epoch=None, remote_id='', remote_url='', now=None):
    """A 2xx-ish success -> confirmed; a proven 4xx non-execution -> failed; else unknown.

    S5-R5: exit 1/4 is NOT enough to call a write failed — a 5xx, timeout, reset, or lost reply is
    ambiguous (the remote may have executed it). Only the parsed transport `HTTP 4xx` status proves
    non-execution and may settle `failed`; incidental digits in URLs/PR numbers are never treated as
    a status. Everything else stays `unknown` for reconcile.
    """
    if proc.returncode == 0:
        return outbox.settle(conn, op, 'confirmed', remote_id=remote_id, remote_url=remote_url, now=now, epoch=epoch)
    err = (proc.stderr or '') + '\n' + (proc.stdout or '')
    m = re.search('HTTP\\s+(\\d{3})', err)
    if m and m.group(1).startswith('4'):
        return outbox.settle(conn, op, 'failed', reason=err.strip()[:300], now=now, epoch=epoch)
    return outbox.settle(conn, op, 'unknown', reason='gh exit %d' % proc.returncode, now=now, epoch=epoch)

def push(conn, branch, sha, old_sha=None, gh=None, git=None, now=None):
    """Push `branch` at `sha` (kind gh_push_branch). Refuses main; deduped by the outbox.

    S5-R10 re-review: with `old_sha` the update is a fast-forward-only expected-old lease — the
    expected head must be an ancestor of the target (no history rewinds), and the remote then
    rejects the leased push unless the ref is exactly at `old_sha`. A non-fast-forward request is
    refused and settled `failed`; a stale-lease rejection also settles `failed` (proven
    non-execution). Without `old_sha` the call creates a new reference.
    """
    gh = gh or _gh
    git = git or _git
    now = time.time() if now is None else now
    if branch == 'main':
        return controller.Result(False, 'forbidden', 'refusing to push main')
    request = {'sha': sha, 'old_sha': old_sha} if old_sha else {'sha': sha}
    r = outbox.intent(conn, 'gh_push_branch', branch, request)
    if not r.ok:
        return r
    if r.code == 'existing':
        previous = durable_transport.reconcile_operation(conn, r.task, gh=gh, now=now)
        if previous['state'] not in ('intent', 'reconciled_absent'):
            return r
    op = r.task
    mr = outbox.mark_sending(conn, op, now)
    if not mr.ok:
        return mr
    if old_sha:
        ancestry = git(['merge-base', '--is-ancestor', old_sha, sha])
        if ancestry.returncode != 0:
            outbox.settle(conn, op, 'failed', reason='refusing non-fast-forward push: %s is not an ancestor of %s' % (old_sha, sha), epoch=mr.gen, now=now)
            return controller.Result(False, 'non_fast_forward', 'refusing to rewind %s: %s is not an ancestor of %s' % (branch, old_sha, sha), op)
        p = git(['push', 'origin', '%s:refs/heads/%s' % (sha, branch), '--force-with-lease=%s:%s' % (branch, old_sha)])
        _settle_push(conn, op, p, epoch=mr.gen, remote_id=branch, now=now)
    else:
        p = gh(['api', 'repos/{owner}/{repo}/git/refs', '--method', 'POST', '-f', 'ref=refs/heads/' + branch, '-f', 'sha=' + sha])
        _settle_from(conn, op, p, epoch=mr.gen, remote_id=branch, now=now)
    return controller.Result(True, 'ok', 'push op %s' % op, op)

def pr_create(conn, branch, title, body, base='main', gh=None, now=None):
    """Open a PR for `branch` (kind gh_pr_create), with an idempotency marker in the body.

    S5-R7: the marker is minted from the durable op id returned by `intent`, so reconciliation by
    operation id can find the created PR after a lost acknowledgement.
    """
    gh = gh or _gh
    now = time.time() if now is None else now
    request = {'branch': branch, 'title': title, 'base': base, 'body_digest': hashlib.sha256((body or '').encode()).hexdigest()}
    r = outbox.intent(conn, 'gh_pr_create', branch, request)
    if not r.ok:
        return r
    if r.code == 'existing':
        previous = durable_transport.reconcile_operation(conn, r.task, gh=gh, now=now)
        if previous['state'] not in ('intent', 'reconciled_absent'):
            return r
    op = r.task
    body = (body or '') + '\n\n<!-- stage34-op:%s -->' % op
    mr = outbox.mark_sending(conn, op, now)
    if not mr.ok:
        return mr
    p = gh(['pr', 'create', '--base', base, '--head', branch, '--title', title, '--body', body])
    remote_id = p.stdout.strip().split('/')[-1] if p.returncode == 0 else ''
    _settle_from(conn, op, p, epoch=mr.gen, remote_id=remote_id, now=now)
    return controller.Result(True, 'ok', 'pr-create op %s' % op, op)

def pr_merge(conn, pr, head_sha, approval_id, gh=None, now=None):
    """Merge a PR at its exact head (kind gh_pr_merge): needs an owner approval bound to it."""
    gh = gh or _gh
    now = time.time() if now is None else now
    r = outbox.intent(conn, 'gh_pr_merge', pr, {'head_sha': head_sha}, approval_id=approval_id)
    if not r.ok:
        return r
    if r.code == 'existing':
        previous = durable_transport.reconcile_operation(conn, r.task, gh=gh, now=now)
        if previous['state'] not in ('intent', 'reconciled_absent'):
            return r
    op = r.task
    mr = outbox.mark_sending(conn, op, now)
    if not mr.ok:
        return mr
    p = gh(['pr', 'merge', pr, '--match-head-commit', head_sha])
    _settle_from(conn, op, p, epoch=mr.gen, remote_id=pr, now=now)
    return controller.Result(True, 'ok', 'pr-merge op %s' % op, op)

def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog='ops.py')
    sub = ap.add_subparsers(dest='cmd', required=True)
    pu = sub.add_parser('push')
    pu.add_argument('branch')
    pu.add_argument('sha')
    pu.add_argument('--old-sha', default='', help='update an existing branch at this expected head')
    pc = sub.add_parser('pr-create')
    pc.add_argument('branch')
    pc.add_argument('--title', required=True)
    pc.add_argument('--body', default='')
    pm = sub.add_parser('pr-merge')
    pm.add_argument('pr')
    pm.add_argument('--head-sha', required=True)
    pm.add_argument('--approval', required=True)
    args = ap.parse_args(argv)
    conn = controller.open_db('rw', controller.CLI_TIMEOUT)
    try:
        if args.cmd == 'push':
            r = push(conn, args.branch, args.sha, old_sha=args.old_sha or None)
        elif args.cmd == 'pr-create':
            r = pr_create(conn, args.branch, args.title, args.body)
        else:
            r = pr_merge(conn, args.pr, args.head_sha, args.approval)
        print(r.message)
        return 0 if r.ok else 1
    finally:
        conn.close()
if __name__ == '__main__':
    raise SystemExit(main())
