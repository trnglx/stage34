/**
 * Codex app-server approval enforcement (offline prototype).
 *
 * The pinned `@openai/codex-sdk` only wraps `codex exec` (approvalPolicy is the SDK's own mode,
 * not an external pre-execution hook), so it cannot authorize tools before execution. The Codex
 * CLI `app-server` mode emits `item/commandExecution/requestApproval` and
 * `item/fileChange/requestApproval` requests BEFORE the CLI acts — but only when the CLI's
 * approval policy requires them (approval depends on configuration; see
 * docs/OC985-codex-app-server.md). This module is the policy + loop that answers those requests.
 * The worker's own mandatory restrictions (no MCP servers, no network) are enforced regardless of
 * the CLI's approval configuration.
 *
 * Protocol-agnostic: the JSONL wire format of `codex app-server` is parsed at the transport
 * boundary (not here), so the enforcement contract — a denied operation never executes — is fully
 * testable offline with an injected fake server. The fake proves the *policy* contract, not that
 * the real binary emits every request (interception coverage remains unverified until tested).
 */

export type ApprovalKind = 'command_execution' | 'file_change' | 'mcp_tool_call' | 'web_search';

export interface ApprovalRequest {
  request_id: string;
  kind: ApprovalKind;
  command?: string;          // command_execution: the shell command line
  paths?: readonly string[]; // file_change: affected paths
  tool?: string;             // mcp_tool_call: MCP tool name
}

export interface ApprovalDecision {
  request_id: string;
  approve: boolean;
  reason?: string;
}

/** Kinds the worker never enables: no MCP servers are configured and network is disabled. These
 *  are denied regardless of the allowlist — even under an unconstrained (`null`) allowlist. */
const ALWAYS_DENIED: ReadonlySet<ApprovalKind> = new Set(['mcp_tool_call', 'web_search']);

/** Map each *allowable* approval kind to the Claude allowlist vocabulary it requires. */
const REQUIRED_TOOLS: Partial<Record<ApprovalKind, readonly string[]>> = {
  command_execution: ['Bash'],
  file_change: ['Edit', 'Write'],
};

export class CodexApprovalPolicy {
  /**
   * `allowed` is the Claude allowlist vocabulary (Bash/Edit/Write/Read/...). `null` means "no
   * policy" (unconstrained) and approves every *allowable* kind; an explicit (possibly empty) set
   * enforces the allowlist — an empty set denies every command/file change (read-only). Kinds the
   * worker cannot support (MCP, web search) are always denied, before the allowlist is consulted.
   */
  constructor(private readonly allowed: ReadonlySet<string> | null) {}

  decide(request: ApprovalRequest): ApprovalDecision {
    // Mandatory worker restrictions come first: the worker configures no MCP servers and network
    // is disabled, so mcp_tool_call/web_search are always denied — even with no allowlist.
    if (ALWAYS_DENIED.has(request.kind)) {
      return {request_id: request.request_id, approve: false, reason: 'not_supported_by_worker'};
    }
    if (this.allowed === null) return {request_id: request.request_id, approve: true};
    const required = REQUIRED_TOOLS[request.kind] ?? [];
    const approve = required.some(tool => this.allowed!.has(tool));
    return approve
      ? {request_id: request.request_id, approve: true}
      : {request_id: request.request_id, approve: false, reason: 'tool_not_allowed_by_worker_policy'};
  }
}

/**
 * Consume approval requests from `requests`, answer each via `answer`, stopping on abort. A pending
 * read on a silent source is raced against the abort signal so the loop cannot remain blocked
 * forever. On exit the iterator's `return()` runs as transport cleanup, raced against a bounded
 * deadline so a source whose `return()` queues behind a stalled read cannot hang shutdown. The
 * transport must be signal-aware: it should reject/resolve its pending read when the signal fires,
 * which unblocks the read and lets `return()` complete (see the stalled-generator regression test).
 * In production `requests`/`answer` are the `codex app-server` JSONL stdio; tests inject a fake so
 * the "deny -> no effect" contract is proven without a real CLI or model.
 */
export async function runApprovalLoop(
  requests: AsyncIterable<ApprovalRequest>,
  policy: CodexApprovalPolicy,
  answer: (decision: ApprovalDecision, signal: AbortSignal) => void | Promise<void>,
  signal: AbortSignal,
  cleanupTimeoutMs = 1000,
): Promise<number> {
  const ABORTED = Symbol('aborted');
  let answered = 0;
  const iterator = requests[Symbol.asyncIterator]();
  let resolveAbort: ((value: typeof ABORTED) => void) | undefined;
  const aborted = new Promise<typeof ABORTED>(resolve => { resolveAbort = resolve; });
  const onAbort = () => { if (resolveAbort) resolveAbort(ABORTED); };
  if (signal.aborted) onAbort();
  else signal.addEventListener('abort', onAbort, {once: true});
  try {
    while (!signal.aborted) {
      let result: IteratorResult<ApprovalRequest, unknown> | typeof ABORTED;
      try {
        // Race the pending read against abort so a silent server cannot block the loop forever.
        result = await Promise.race([iterator.next(), aborted]);
      } catch (error) {
        // A signal-aware transport rejects its pending read when aborted; that is a clean stop.
        if (signal.aborted) break;
        throw error;
      }
      if (result === ABORTED || signal.aborted) break;
      if (result.done) break;
      // Writes can backpressure just like reads. The transport must use this signal to cancel
      // its pending write; do not let a silent/broken sink hold shutdown indefinitely.
      const sent = await Promise.race([Promise.resolve(answer(policy.decide(result.value), signal)).then(() => true), aborted]);
      if (sent === ABORTED || signal.aborted) break;
      answered += 1;
    }
  } finally {
    signal.removeEventListener('abort', onAbort);
    // Transport cleanup: release the source (in production, close the child stdio stream). Race it
    // against a bounded deadline so a source that cannot finish cleanup cannot hang the loop.
    if (typeof iterator.return === 'function') {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const deadline = new Promise<void>(resolve => { timer = setTimeout(resolve, cleanupTimeoutMs); });
      try {
        await Promise.race([Promise.resolve(iterator.return()), deadline]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    }
  }
  return answered;
}
