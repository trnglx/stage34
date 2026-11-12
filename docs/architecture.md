# Architecture and extension boundary

One SQLite controller owns tasks, generations, leases, attempts, external-effect intents,
candidates and receipts. Native workers acknowledge durable commands; provider notifications
do not grant acceptance or release uncertain capacity. Fallback stops and reconciles the old
attempt before dispatching a new generation with its bound checkpoint and constraints.

For constrained Codex execution, native model requests travel through a loopback relay,
private Unix socket and worker queue to the trusted controller. Only the controller resolves
provider credentials. It filters tool declarations, buffers the complete response, validates
the terminal tool calls and usage, stages the canonical response privately, commits its
ledger decision and releases delivery. Raw upstream deltas never reach the CLI. Unknown
usage/effects block new inference. Turn counts persist across SDK objects and resume.

The fixed Stage34 MCP namespace exposes only allowed Read/Grep/Glob/Edit/Write operations.
Its filesystem server rejects parent traversal, symlinks, hard links and writes to control
directories. Bash is a separate broad command capability; never grant it to emulate Read.
Project-local Codex config is untrusted. Conditional approvals and notification streams do
not provide enforcement; the gateway validates tool calls before the native binary sees them.

Only the ticket-verified managed-container entrypoint delegates the native CLI sandbox to
Docker on hosts where nested bubblewrap is unavailable. Readonly root, the requested workspace
mount mode, non-root UID, dropped capabilities, no-new-privileges and network isolation remain
enforced by the container policy. Task fields cannot request delegation. Standalone runtimes
retain the native sandbox. See [OpenAI container guidance](https://learn.chatgpt.com/docs/agent-approvals-security).

Upstream references: [Codex MCP configuration](https://learn.chatgpt.com/docs/extend/mcp) and
[configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).
Keep provider versions pinned and rerun native denial/positive controls before upgrading.

`stage34.interfaces` declares version 1 contracts for ProjectProfile, PolicyProvider,
CodingRuntime, Transport, Verifier, ArtifactPublisher, ApprovalProvider and ReviewStrategy.
These are trusted integration interfaces, not an arbitrary Python plugin loader. Profiles
provide lane/kind/role/path policy data; they cannot turn off fencing or acceptance. Review
all adapter wiring against the actual controller boundary before enabling it in production.

The optional GitHub verifier adapter obtains its source root, workflow and trusted ref from
the profile. The generic profile uses `src/stage34/core` and the shipped `candidate-verify.yml`.
The workflow admits an exact immutable verifier SHA and exports the same bound receipt format.
It requires separately configured GitHub access; the offline quickstart never uses this adapter.

Private integrations supply a separate versioned profile. This source candidate supplies
only a generic profile; it contains no private machine enrollment, transcripts or history.
