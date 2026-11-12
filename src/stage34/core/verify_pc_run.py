if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from . import verification_contract as vc

def run_checks(policy, checks, sha, tree, out_dir, tools_dir, executable=None, ac_ref='', verify_id='', verifier_sha='', run_id='', resolved_python=None, cli_python=None):
    if not re.fullmatch('[a-f0-9]{40}', sha) or not re.fullmatch('V-[a-f0-9]{10}', verify_id):
        raise ValueError('invalid dispatch identity')
    actual = vc.git(tools_dir, 'rev-parse', 'HEAD').decode().strip()
    if actual != verifier_sha:
        raise ValueError('executed verifier revision differs from approved revision')
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True, mode=448)
    original = json.loads(Path(policy).read_text())
    selected = [ch for ch in original['checks'] if ch['id'] in checks and ch['lane'] == 'pc']
    if set(checks) != {ch['id'] for ch in selected} or not checks or any((not re.fullmatch('[a-z0-9-]{1,64}', cid) for cid in checks)):
        raise ValueError('invalid requested checks')
    resolved = json.loads(json.dumps(original))
    if resolved_python:
        for spec in resolved['interpreters'].values():
            if spec['path'] == '/usr/bin/python3' and spec['version_prefix'].startswith('3.14.'):
                spec['path'] = str(Path(resolved_python).absolute())
    if cli_python:
        resolved.setdefault('env_set', {})['STAGE34_SYS_PY'] = cli_python
    path = out / 'resolved-policy.json'
    path.write_text(json.dumps(resolved))
    path.chmod(384)
    receipts = {}
    for cid in checks:
        c_out = out / cid
        c_out.mkdir(parents=True, exist_ok=True, mode=448)
        check = next((ch for ch in selected if ch['id'] == cid))
        try:
            result = subprocess.run([executable or sys.executable, str(Path(tools_dir) / 'verify_exec.py'), '--policy-json', str(path), '--check', cid, '--sha', sha, '--tree', str(tree), '--ac-ref', ac_ref, '--out', str(c_out)], capture_output=True, text=True, timeout=check['timeout_s'] + 45)
            receipt = json.loads(result.stdout)
            if not isinstance(receipt, dict) or (result.returncode and receipt.get('outcome') == 'pass'):
                raise ValueError('invalid receipt')
        except (ValueError, subprocess.SubprocessError):
            receipt = {'outcome': 'error', 'reason': 'executor_failed', 'duration_s': 0, 'target_sha': sha}
        receipt['lane'] = 'pc'
        receipts[cid] = receipt
    bundle = {'verify_id': verify_id, 'run_id': str(run_id), 'verifier_sha': actual, 'verifier_digest': vc.verifier_digest(tools_dir), 'target_sha': sha, 'observed_sha': vc.git(tree, 'rev-parse', 'HEAD').decode().strip(), 'tree_sha': vc.git(tree, 'rev-parse', 'HEAD^{tree}').decode().strip(), 'policy_digest': vc.digest(original), 'receipts': receipts}
    target = out / 'receipts.json'
    target.write_text(json.dumps(bundle))
    target.chmod(384)
    return receipts

def main(argv=None):
    ap = argparse.ArgumentParser()
    for key in ('policy', 'checks', 'sha', 'tree', 'out', 'tools', 'verify-id', 'verifier-sha', 'run-id'):
        ap.add_argument('--' + key, required=True)
    ap.add_argument('--ac-ref', default='')
    ap.add_argument('--python')
    ap.add_argument('--cli-python')
    args = ap.parse_args(argv)
    run_checks(args.policy, args.checks.split(','), args.sha, args.tree, args.out, args.tools, ac_ref=args.ac_ref, verify_id=args.verify_id, verifier_sha=args.verifier_sha, run_id=args.run_id, resolved_python=args.python, cli_python=args.cli_python)
    return 0
if __name__ == '__main__':
    raise SystemExit(main())
