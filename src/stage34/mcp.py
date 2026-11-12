"""Read-only MCP stdio endpoint. No task writes, code execution or secret retrieval."""

import json
import sys


def serve(repository):
    from .cli import snapshot

    for line in sys.stdin:
        if len(line) > 65536:
            return
        request = None
        try:
            request = json.loads(line)
            if not isinstance(request, dict) or request.get("jsonrpc") != "2.0":
                return
            if "id" not in request:
                continue
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("params must be an object")
            method = request.get("method")
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "Stage34", "version": "0.1.0rc1"},
                }
            elif method == "tools/list":
                result = {
                    "tools": [
                        {
                            "name": "stage34_status",
                            "description": "Read controller task status",
                            "inputSchema": {
                                "type": "object",
                                "properties": {},
                                "additionalProperties": False,
                            },
                            "annotations": {"readOnlyHint": True},
                        }
                    ]
                }
            elif (
                method == "tools/call"
                and request.get("params", {}).get("name") == "stage34_status"
                and request["params"].get("arguments", {}) == {}
            ):
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(snapshot(repository), sort_keys=True),
                        }
                    ]
                }
            else:
                print(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "error": {
                                "code": -32601,
                                "message": "unsupported read-only method",
                            },
                        }
                    ),
                    flush=True,
                )
                continue
            print(
                json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}),
                flush=True,
            )
        except (ValueError, TypeError, OSError):
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request.get("id") if isinstance(request, dict) else None,
                        "error": {
                            "code": -32602,
                            "message": "invalid request or unavailable state",
                        },
                    }
                ),
                flush=True,
            )
