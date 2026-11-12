"""Reproducible offline fixture: mock builder, real controller and independent verifier.

Only creates a new disposable repository. Never runs scripts from an owner's repo.
The fixture reviewer is deterministic test evidence, not a paid/model/human review.
"""

import json
import os
from pathlib import Path
import subprocess
import sys


def run(destination):
    root = Path(destination).absolute()
    if root.exists() or root.is_symlink():
        raise ValueError("demo destination must not exist")
    root.mkdir(mode=0o700, parents=True)
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(root),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_AUTHOR_NAME": "Stage34 fixture",
        "GIT_AUTHOR_EMAIL": "fixture@invalid.example",
        "GIT_COMMITTER_NAME": "Stage34 fixture",
        "GIT_COMMITTER_EMAIL": "fixture@invalid.example",
    }

    def git(*args):
        result = subprocess.run(  # noqa: S603 - explicit Git argv in a newly created fixture only
            [
                "git",
                "-c",
                "core.hooksPath=" + os.devnull,
                "-c",
                "commit.gpgsign=false",
                "-c",
                "init.templateDir=",
                "-C",
                str(root),
                *args,
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        return result.stdout.strip()

    git("init", "-q", "-b", "main")
    (root / "src").mkdir()
    (root / "checks").mkdir()
    (root / ".gitignore").write_text(".stage34/\n")
    (root / "src/answer.py").write_text("answer = 0\n")
    (root / "checks/check_answer.py").write_text(
        'from pathlib import Path\nassert Path("src/answer.py").read_text() == "answer = 42\\n"\n'
    )
    git("add", ".")
    git("commit", "-q", "-m", "fixture base")
    base = git("rev-parse", "HEAD")
    from .cli import authority

    _, state, c = authority(root)
    created = c.init_db(path=state / "work/controller.db")
    if not created.ok:
        raise ValueError("fixture authority initialization refused")
    from .core import acceptance, finding, verify

    conn = c.open_db(path=state / "work/controller.db")
    try:
        policy = {
            "schema": "orch-policy/1",
            "name": "offline-fixture",
            "version": 1,
            "risk": "ordinary",
            "interpreters": {
                "py": {
                    "path": sys.executable,
                    "version_prefix": "%d.%d." % sys.version_info[:2],
                }
            },
            "env_allow": [],
            "env_set": {},
            "protected": ["checks/**"],
            "checks": [
                {
                    "id": "answer",
                    "kind": "static",
                    "lane": "local",
                    "argv": ["{py}", "checks/check_answer.py"],
                    "cwd": ".",
                    "timeout_s": 10,
                    "junit": False,
                    "watch": ["src/**"],
                }
            ],
            "review": {
                "roles": ["fixture-reviewer"],
                "blocking_severities": ["block", "critical", "important"],
            },
            "integration": {"checks": []},
        }
        policy_path = state / "fixture-policy.json"
        policy_path.write_text(json.dumps(policy))
        policy_path.chmod(0o600)
        pid = acceptance.load_policy(conn, policy_path)
        result = c.new_task(
            conn,
            "fixture",
            "deliver the bounded answer fixture",
            "builder",
            ["src/**"],
            ["independent check passes"],
        )
        if not result.ok:
            raise ValueError("fixture task refused")
        tid = result.task
        conn.execute("UPDATE task SET policy=?,base_ref=? WHERE id=?", (pid, base, tid))
        nonce = c.new_nonce()
        reserved = c.reserve(conn, tid, "builder", nonce, workspace=str(root))
        if not reserved.ok:
            raise ValueError("fixture reservation refused")
        # Deterministic mock coding adapter. There is no SDK/account/model connection.
        (root / "src/answer.py").write_text("answer = 42\n")
        git("add", "src/answer.py")
        git("commit", "-q", "-m", "mock builder candidate")
        head = git("rev-parse", "HEAD")
        submitted = acceptance.submit_candidate(
            conn, tid, nonce, head, acceptance.Git(cwd=root)
        )
        if not submitted.ok:
            raise ValueError("fixture submission refused: " + submitted.code)
        candidate = conn.execute(
            "SELECT id FROM candidate WHERE task=?", (tid,)
        ).fetchone()[0]
        before = acceptance.evaluate(conn, candidate)
        if before.ok:
            raise ValueError("fixture accepted without verification")
        verified = verify.run_candidate(conn, candidate, repo=str(root))
        if verified != "verified":
            raise ValueError("independent fixture verification failed")
        # The fixture review checks only the bounded fixture contract; no model claims.
        if git("show", head + ":src/answer.py") != "answer = 42":
            raise ValueError("fixture reviewer refused")
        finding.record_review(conn, candidate, "fixture-reviewer", 1, "done", head)
        accepted = acceptance.accept(conn, tid)
        if not accepted.ok:
            raise ValueError("fixture acceptance refused: " + accepted.code)
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        return {
            "product": "Stage34",
            "provider": "mock",
            "paid_requests": 0,
            "task": tid,
            "status": accepted.to_state,
            "candidate": candidate,
            "head_sha": head,
            "verification": "independent-checkout",
            "review": "deterministic-fixture",
            "integrity": integrity,
        }
    finally:
        conn.close()
