"""Shipped workflow metadata must pass the actual exported receipt collector."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class RemoteVerifier(unittest.TestCase):
    def test_collection_binds_actual_shipped_workflow_name(self):
        package = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(package / "src"))
        workflow = package / ".github/workflows/candidate-verify.yml"
        producer_name = next(
            line.split(":", 1)[1].strip()
            for line in workflow.read_text().splitlines()
            if line.startswith("name:")
        )
        with tempfile.TemporaryDirectory(prefix="stage34-collect-") as directory:
            os.environ["STAGE34_WORK_DIR"] = directory
            from stage34.core import (
                controller as c,
                acceptance as a,
                verification_contract as vc,
                verify_pc,
            )

            body = {
                "schema": "orch-policy/1",
                "name": "remote-fixture",
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
                "protected": [],
                "checks": [
                    {
                        "id": "pc-check",
                        "kind": "static",
                        "lane": "pc",
                        "argv": ["{py}", "check.py"],
                        "timeout_s": 10,
                        "junit": False,
                        "watch": ["src/**"],
                    }
                ],
                "review": {
                    "roles": ["fixture-reviewer"],
                    "blocking_severities": ["block"],
                },
                "integration": {"checks": []},
            }
            policy = Path(directory) / "policy.json"
            policy.write_text(json.dumps(body))
            for name, expected in [
                (producer_name, "verified"),
                ("unapproved-workflow", "failed"),
            ]:
                conn = c.memory_db()
                try:
                    pid = a.load_policy(conn, policy)
                    tid = c.new_task(
                        conn,
                        "fixture",
                        "bounded receipt fixture",
                        "builder",
                        ["src/**"],
                        ["check passes"],
                    ).task
                    conn.execute(
                        "UPDATE task SET policy=?,status='submitted' WHERE id=?",
                        (pid, tid),
                    )
                    nonce = c.new_nonce()
                    # Reserve before submission through the existing authority, not a parallel engine.
                    conn.execute("UPDATE task SET status='open' WHERE id=?", (tid,))
                    self.assertTrue(c.reserve(conn, tid, "builder", nonce).ok)
                    conn.execute(
                        "UPDATE task SET status='submitted' WHERE id=?", (tid,)
                    )
                    conn.execute(
                        "UPDATE attempt SET state='submitted' WHERE task=?", (tid,)
                    )
                    aid = conn.execute(
                        "SELECT id FROM attempt WHERE task=?", (tid,)
                    ).fetchone()[0]
                    cid = "C-" + "b" * 10
                    head = "b" * 40
                    approved = "a" * 40
                    verifier = "c" * 64
                    conn.execute(
                        "INSERT INTO candidate(id,task,attempt,gen,base_sha,head_sha,tree_sha,diff_digest,policy,runtime_digest,state,created_at,updated_at) VALUES(?,?,?,1,'',?,'','',?,'','submitted',1,1)",
                        (cid, tid, aid, head, pid),
                    )
                    run = a.record_run_intent(
                        conn, cid, "pc", "candidate", head, verifier
                    )
                    conn.execute(
                        "UPDATE verify_run SET state='running',remote_ref='12345' WHERE id=?",
                        (run,),
                    )
                    vc.put(
                        conn,
                        "run",
                        run,
                        {
                            "policy": pid,
                            "policy_digest": vc.digest(body),
                            "contract": vc.contract_digest(conn, tid),
                            "ac_ref": "",
                            "checks": ["pc-check"],
                            "verifier_sha": approved,
                            "verifier_digest": verifier,
                        },
                    )
                    receipt = {
                        "outcome": "pass",
                        "target_sha": head,
                        "observed_sha": head,
                        "check_digest": a.check_digest(body["checks"][0]),
                        "duration_s": 1.0,
                        "exit_code": 0,
                        "counts": {},
                        "runner": "offline-fixture",
                        "interpreter": "Python fixture",
                        "env_digest": "e" * 64,
                        "argv_digest": "d" * 64,
                        "lane": "pc",
                        "verifier_digest": verifier,
                    }
                    bundle = {
                        "verify_id": run,
                        "run_id": "12345",
                        "verifier_sha": approved,
                        "verifier_digest": verifier,
                        "target_sha": head,
                        "policy_digest": vc.digest(body),
                        "receipts": {"pc-check": receipt},
                    }

                    def gh(args, timeout=60):
                        if args[:2] == ["run", "view"]:
                            data = {
                                "status": "completed",
                                "conclusion": "success",
                                "headSha": approved,
                                "workflowName": name,
                                "displayTitle": "verify " + run,
                                "databaseId": 12345,
                            }
                            return subprocess.CompletedProcess(
                                args, 0, json.dumps(data), ""
                            )
                        self.assertEqual(args[:2], ["run", "download"])
                        (Path(args[args.index("-D") + 1]) / "receipts.json").write_text(
                            json.dumps(bundle)
                        )
                        return subprocess.CompletedProcess(args, 0, "", "")

                    self.assertEqual(verify_pc.collect(conn, cid, gh=gh), expected)
                    self.assertEqual(
                        conn.execute(
                            "SELECT count(*) FROM receipt WHERE run=?", (run,)
                        ).fetchone()[0],
                        1 if expected == "verified" else 0,
                    )
                finally:
                    conn.close()


if __name__ == "__main__":
    unittest.main()
