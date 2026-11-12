"""Stage34 local administration. Provider execution is never enabled by these commands."""

import argparse
import html
import json
import os
from pathlib import Path
import stat
import subprocess
import sys


def authority(repository):
    root = Path(repository).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("repository must be a directory")
    result = (
        subprocess.run(  # noqa: S603 - explicit Git argv, selected repository, no shell
            ["git", "-C", str(root), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=10,
            env={
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
            },
        )
    )
    if result.returncode or Path(result.stdout.strip()).resolve() != root:
        raise ValueError("select a Git repository root")
    state = root / ".stage34"
    if state.is_symlink():
        raise ValueError("state directory must not be a symlink")
    state.mkdir(mode=0o700, exist_ok=True)
    info = state.stat()
    if stat.S_IMODE(info.st_mode) & 0o077 or info.st_uid != os.getuid():
        raise ValueError("state directory must be owned by you with mode 0700")
    work = state / "work"
    work.mkdir(mode=0o700, exist_ok=True)
    if work.is_symlink() or stat.S_IMODE(work.stat().st_mode) & 0o077:
        raise ValueError("unsafe work directory")
    os.environ["STAGE34_WORK_DIR"] = str(work)
    os.environ["STAGE34_CONTROLLER_DB"] = str(work / "controller.db")
    os.environ["STAGE34_VERIFY_ROOT"] = str(state / "verify")
    # Core modules are imported only after the explicit repository's state is selected.
    from .core import controller

    return root, state, controller


def snapshot(repository):
    root, state, c = authority(repository)
    db = state / "work/controller.db"
    if not db.exists():
        raise ValueError("run stage34 init first")
    conn = c.open_db("ro", path=db)
    try:
        tasks = [
            dict(id=r["id"], status=r["status"], role=r["role"], generation=r["gen"])
            for r in conn.execute("SELECT id,status,role,gen FROM task ORDER BY id")
        ]
        return {"schema_version": 1, "product": "Stage34", "tasks": tasks}
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(prog="stage34")
    parser.add_argument("--repository", default=".")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    commands.add_parser("status")
    commands.add_parser("doctor")
    commands.add_parser("dashboard")
    demo = commands.add_parser(
        "demo",
        help="create a NEW unrelated fixture repository; no model or credentials",
    )
    demo.add_argument("destination", type=Path)
    commands.add_parser("mcp", help="read-only stdio MCP status server")
    connection = commands.add_parser(
        "connection-add",
        help="store an OpenAI connection using a masked terminal prompt and OS keychain",
    )
    connection.add_argument("--reference", required=True)
    connection.add_argument("--model", action="append", required=True)
    connection.add_argument("--repository-ref", action="append", required=True)
    connection.add_argument(
        "--directory", type=Path, default=Path.home() / ".config/stage34/connections"
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "connection-add":
            if not sys.stdin.isatty():
                raise ValueError("masked terminal prompt required")
            import getpass
            from .core.provider_connections import SecureConnections
            from .core.inference import InferenceError

            try:
                SecureConnections(args.directory).enroll(
                    args.reference,
                    args.model,
                    args.repository_ref,
                    getpass.getpass("OpenAI API key (OS keychain only): "),
                )
            except InferenceError as error:
                print(json.dumps({"ok": False, "code": error.code}))
                return 1
            print(
                json.dumps(
                    {
                        "ok": True,
                        "connection_ref": args.reference,
                        "paid_execution_enabled": False,
                    }
                )
            )
            return 0
        if args.command == "demo":
            from .demo import run

            print(json.dumps(run(args.destination), sort_keys=True))
            return 0
        if args.command == "mcp":
            from .mcp import serve

            serve(args.repository)
            return 0
        if args.command in ("status", "dashboard"):
            data = snapshot(args.repository)
            if args.command == "dashboard":
                # Report to stdout; never starts a server or opens a public endpoint.
                rows = "".join(
                    "<tr>"
                    + "".join(
                        "<td>" + html.escape(str(row[k])) + "</td>"
                        for k in ("id", "status", "role", "generation")
                    )
                    + "</tr>"
                    for row in data["tasks"]
                )
                print(
                    '<!doctype html><html lang="en"><meta charset="utf-8"><title>Stage34</title><h1>Stage34</h1><table><caption>Controller task status</caption><thead><tr><th>Task</th><th>Status</th><th>Role</th><th>Generation</th></tr></thead><tbody>'
                    + rows
                    + "</tbody></table></html>"
                )
            else:
                print(json.dumps(data, sort_keys=True))
            return 0
        root, state, c = authority(args.repository)
        if args.command == "init":
            result = c.init_db(path=state / "work/controller.db")
            print(json.dumps({"ok": result.ok, "code": result.code}))
            return 0 if result.ok else 1
        code, lines = c.doctor(path=state / "work/controller.db")
        print(
            json.dumps({"ok": code == 0, "code": code, "checks": lines}, sort_keys=True)
        )
        return code
    except (OSError, ValueError, subprocess.SubprocessError):
        print("Stage34: invalid repository or unavailable local state", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
