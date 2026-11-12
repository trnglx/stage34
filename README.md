# Stage34

Stage34 coordinates native coding agents through one durable controller, fenced attempts,
bounded workers, independent candidate verification, and explicit approvals for external effects.

This is a **private release candidate** under Apache-2.0. Publication and account-backed
provider acceptance are separate release gates. No paid execution is enabled by installation.

## Offline quickstart

Requires Python 3.9+, Git and an existing setuptools build environment:

```sh
python3 -m pip install --no-deps --no-build-isolation .
stage34 demo /tmp/stage34-example
stage34 --repository /tmp/stage34-example status
stage34 --repository /tmp/stage34-example dashboard > /tmp/stage34-dashboard.html
```

The demo creates a new unrelated repository and uses a deterministic mock builder and fixture
reviewer. It uses the real controller, candidate manifests, independent Git checkout verifier,
receipts and acceptance engine. It needs no provider credentials, GitHub/Jira login or network.
Existing destination directories are refused. It does not demonstrate model quality gains.

For an existing repository, run `stage34 --repository /path/to/repo init`. Keep `.stage34/`
out of version control. Status, dashboard and `mcp` expose task metadata only. MCP uses stdio
and offers only the read-only `stage34_status` tool; the dashboard is static HTML on stdout.

## Native providers

`runtime/` contains the native SDK bridge, durable worker service and isolated job runtime.
Node 24 or 26 is required; dependency versions are locked. Install with `npm ci --ignore-scripts`
and build with `npm run build` in that directory. Provider SDKs have their own licenses and
service terms; Stage34's license grants no API credits or rights to provider services.

For an approved connection, install the `connections` extra and run `stage34 connection-add
--reference connection:my-provider --model gpt-5.4 --repository-ref my-repo` in a terminal.
The key is entered through a masked prompt and stored only in an approved OS keychain.
Only scoped metadata is written to the private connection directory. No key argument,
plaintext backend or piped prompt is accepted. This does not enable paid execution.

Constrained Codex jobs require the controller-side inference gateway, a network-disabled
worker and its advertised enforcement capability. Legacy unconstrained SDK transport must
refuse requested tool/turn/cost restrictions. Stream notifications and conditional approval
hooks are not authorization boundaries. See `SECURITY.md` and `docs/architecture.md`.

## Support and release state

Controller: macOS/Linux. Worker: Linux Docker, including Docker Desktop on macOS/Windows.
Native Windows controller support is not claimed. Native/provider verification is distinct
from the no-token demo. See `docs/release.md` for outstanding evidence and publication gates.
