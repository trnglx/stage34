"""Public acceptance: unrelated repos, no accounts, no provider SDK installation."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class Quickstart(unittest.TestCase):
    def test_two_independent_repositories_and_read_only_surfaces(self):
        package = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="stage34-public-") as directory:
            env = {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "HOME": directory,
                "PYTHONPATH": str(package / "src"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            for name in ("example-one", "example-two"):
                repo = Path(directory) / name

                def run(*args, data=None):
                    return subprocess.run(  # noqa: S603 - fixed local CLI, no shell/provider execution
                        [sys.executable, "-m", "stage34", *args],
                        env=env,
                        input=data,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )

                demo = run("demo", str(repo))
                self.assertEqual(demo.returncode, 0, demo.stderr)
                receipt = json.loads(demo.stdout)
                self.assertEqual(receipt["status"], "done")
                self.assertEqual(receipt["paid_requests"], 0)
                self.assertEqual(receipt["verification"], "independent-checkout")
                status = run("--repository", str(repo), "status")
                self.assertEqual(status.returncode, 0, status.stderr)
                self.assertEqual(len(json.loads(status.stdout)["tasks"]), 1)
                self.assertNotEqual(run("demo", str(repo)).returncode, 0)
                dashboard = run("--repository", str(repo), "dashboard")
                self.assertEqual(dashboard.returncode, 0, dashboard.stderr)
                self.assertIn("Stage34", dashboard.stdout)
                self.assertIn("done", dashboard.stdout)
                requests = [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "stage34_status", "arguments": {}},
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {"name": "run_command", "arguments": {"cmd": "exit"}},
                    },
                ]
                mcp = run(
                    "--repository",
                    str(repo),
                    "mcp",
                    data="".join(json.dumps(r) + "\n" for r in requests),
                )
                replies = [json.loads(line) for line in mcp.stdout.splitlines()]
                self.assertIn("result", replies[0])
                self.assertEqual(replies[1]["error"]["code"], -32601)
                self.assertEqual((repo / ".stage34").stat().st_mode & 0o777, 0o700)

    def test_doctor_failure_codes_and_mcp_bad_params(self):
        package = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="stage34-errors-") as directory:
            env = {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "HOME": directory,
                "PYTHONPATH": str(package / "src"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            repo = Path(directory) / "fixture"

            def run(*args, data=None):
                return subprocess.run(  # noqa: S603 - fixed offline fixture CLI
                    [sys.executable, "-m", "stage34", *args],
                    env=env,
                    input=data,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )

            self.assertEqual(run("demo", str(repo)).returncode, 0)
            db = repo / ".stage34/work/controller.db"
            db.chmod(0o644)
            with self.subTest("doctor unhealthy exit"):
                self.assertEqual(run("--repository", str(repo), "doctor").returncode, 1)
            db.chmod(0o600)
            requests = [
                {"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": value}
                for i, value in enumerate((None, []), 1)
            ]
            requests.append({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
            result = run(
                "--repository",
                str(repo),
                "mcp",
                data="".join(json.dumps(r) + "\n" for r in requests),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            replies = [json.loads(line) for line in result.stdout.splitlines()]
            self.assertEqual(len(replies), 3)
            self.assertEqual(
                [r["error"]["code"] for r in replies[:2]], [-32602, -32602]
            )
            self.assertIn("result", replies[2])
            db.unlink()
            self.assertEqual(run("--repository", str(repo), "doctor").returncode, 3)


if __name__ == "__main__":
    unittest.main()
