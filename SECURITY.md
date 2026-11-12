# Security

Do not put API keys in chat, command arguments, repository files, fleet configuration,
transcripts, manifests or logs. Provider connections must use an approved OS credential store
and metadata references. Plaintext keyring backends are refused. Missing/revoked connections,
unknown pricing or unknown spend must refuse execution. Paid execution defaults off.

The controller, connection resolver and worker supervisor are trusted. Model jobs are
untrusted: network disabled, bounded filesystem access, fresh attempt identities and fenced
commands. Container control goes through the restrictive broker, never a model-visible raw
Docker socket. Do not give untrusted users controller state or supervisor credentials.

Codex model responses are buffered, tool-checked and reconstructed before delivery. One
request per attempt can be outstanding; uncertain effects block retries. Budget enforcement
is an API-equivalent threshold with possible one-response overshoot, not a zero-overshoot
billing promise. A response exceeding the threshold is withheld before tool execution.

Keep `.stage34/` private (0700), backups encrypted/restricted, and provider gateways on the
trusted controller. The MCP server is read-only. The dashboard contains task identifiers and
roles and should be shared only with authorized operators.

Until a public reporting channel is approved, report candidate vulnerabilities privately to
the repository owner. Do not post credentials, proprietary source or exploit data publicly.
