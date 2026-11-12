import type {Codex, Thread, ThreadEvent, ThreadOptions, CodexOptions} from '@openai/codex-sdk';
import type {query, Query, SDKMessage} from '@anthropic-ai/claude-agent-sdk';
import {isAbsolute, resolve, relative, dirname} from 'node:path';
import {realpathSync} from 'node:fs';
import {claudeEvent, codexEvent, type MappedEvent, type Provider} from './events.js';

export class RuntimeError extends Error {
  constructor(readonly code: string) { super(code); }
}
export interface AttemptContext {
  task_id: string;
  attempt_id: string;
  generation: number;
  node_id: string;
  policy_sha256: string;
  workspace: string;
  model: string;
  sandbox: 'read-only' | 'workspace-write';
  timeout_ms: number;
  allowed_tools?: readonly string[];
  max_turns?: number;
  max_budget_usd?: number;
}
export interface RuntimeBinding {
  sdk_version: string;
  sdk_sha256: string;
  cli_path: string;
  cli_version: string;
  cli_sha256: string;
}
export interface WorkerEvent extends MappedEvent {
  schema_version: 1;
  task_id: string;
  attempt_id: string;
  generation: number;
  node_id: string;
  provider: Provider;
  sdk_version: string;
  sdk_sha256: string;
  cli_version: string;
  cli_sha256: string;
  policy_sha256: string;
}
// The worker's mandatory publisher redacts and durably spools private payloads,
// allocates replay-stable event IDs, and returns only canonical metadata.
export type Publish = (event: WorkerEvent, privatePayload: unknown) => Promise<void>;
type ClaudeQuery = AsyncIterable<SDKMessage> & Pick<Query, 'close'>;
type ClaudeFactory = (args: Parameters<typeof query>[0]) => ClaudeQuery;
type CodexThread = Pick<Thread, 'id' | 'runStreamed'>;
interface CodexClient {
  startThread(options: ThreadOptions): CodexThread;
  resumeThread(id: string, options: ThreadOptions): CodexThread;
}
type CodexFactory = (options: CodexOptions) => CodexClient;

function validate(context: AttemptContext, binding: RuntimeBinding): void {
  for (const id of [context.task_id, context.attempt_id, context.node_id]) {
    if (typeof id !== 'string' || !/^[a-zA-Z0-9_.:-]{1,128}$/.test(id)) throw new RuntimeError('invalid_identity');
  }
  for (const sha of [context.policy_sha256, binding.sdk_sha256, binding.cli_sha256]) {
    if (typeof sha !== 'string' || !/^[a-f0-9]{64}$/.test(sha)) throw new RuntimeError('invalid_provenance');
  }
  if (!Number.isSafeInteger(context.generation) || context.generation < 1 || !isAbsolute(context.workspace) ||
      !isAbsolute(binding.cli_path) || !/^[a-zA-Z0-9_.-]{1,128}$/.test(context.model)) {
    throw new RuntimeError('invalid_context');
  }
  if (!Number.isSafeInteger(context.timeout_ms) || context.timeout_ms < 1 || context.timeout_ms > 3_600_000 ||
      !['read-only', 'workspace-write'].includes(context.sandbox)) throw new RuntimeError('invalid_policy');
  if (context.max_turns !== undefined && (!Number.isSafeInteger(context.max_turns) || context.max_turns < 1)) {
    throw new RuntimeError('invalid_budget');
  }
  if (context.max_budget_usd !== undefined && (!Number.isFinite(context.max_budget_usd) || context.max_budget_usd <= 0)) {
    throw new RuntimeError('invalid_budget');
  }
  if (context.allowed_tools !== undefined && (!Array.isArray(context.allowed_tools) ||
      context.allowed_tools.some(tool => typeof tool !== 'string'))) throw new RuntimeError('invalid_policy');
}

function snapshot(context: AttemptContext): AttemptContext {
  return Object.freeze({...context, allowed_tools: context.allowed_tools === undefined ? undefined :
    Object.freeze([...context.allowed_tools])});
}

export class RuntimeHandle {
  private claimed = false;
  private settled = false;
  private nativeSession: string | undefined;
  private state = 'starting';
  private readonly timer: ReturnType<typeof setTimeout>;
  private timeout = false;
  readonly abort = new AbortController();

  constructor(readonly provider: Provider, readonly context: AttemptContext, readonly binding: RuntimeBinding,
    private readonly open: (signal: AbortSignal) => Promise<AsyncIterable<SDKMessage | ThreadEvent>>,
    private readonly publish: Publish, session?: string, private readonly policyError?: () => string | undefined) {
    this.nativeSession = session;
    this.context = snapshot(context);
    this.binding = Object.freeze({...binding});
    this.timer = setTimeout(() => { this.timeout = true; this.cancel(); }, context.timeout_ms);
    this.timer.unref();
  }

  cancel(): void {
    if (!this.settled) {
      this.state = 'cancel_requested';
      this.abort.abort();
    }
  }

  inspect() {
    return {provider: this.provider, task_id: this.context.task_id, attempt_id: this.context.attempt_id,
      generation: this.context.generation, session_id: this.nativeSession ?? null,
      requested_model: this.context.model, state: this.state, settled: this.settled,
      sdk_version: this.binding.sdk_version, sdk_sha256: this.binding.sdk_sha256,
      cli_version: this.binding.cli_version, cli_sha256: this.binding.cli_sha256};
  }

  async events(): Promise<void> {
    if (this.claimed) throw new RuntimeError('stream_already_consumed');
    this.claimed = true;
    let terminal: MappedEvent['status'] | undefined;
    let publicationFailed = false;
    const emit = async (mapped: MappedEvent, raw: unknown) => {
      if (mapped.session_id) {
        if (!/^[a-zA-Z0-9_.:-]{1,128}$/.test(mapped.session_id)) throw new RuntimeError('invalid_session');
        if (this.nativeSession && this.nativeSession !== mapped.session_id) throw new RuntimeError('session_conflict');
        this.nativeSession = mapped.session_id;
      }
      if (mapped.status === 'completed' && !this.nativeSession) throw new RuntimeError('missing_native_session');
      if (['completed', 'failed', 'cancelled'].includes(mapped.status)) terminal = mapped.status;
      else if (terminal) mapped = {...mapped, status: 'informational'};
      this.state = terminal ?? mapped.status;
      const event: WorkerEvent = {...mapped, schema_version: 1, task_id: this.context.task_id,
        attempt_id: this.context.attempt_id, generation: this.context.generation, node_id: this.context.node_id,
        provider: this.provider, sdk_version: this.binding.sdk_version, sdk_sha256: this.binding.sdk_sha256,
        cli_version: this.binding.cli_version, cli_sha256: this.binding.cli_sha256,
        policy_sha256: this.context.policy_sha256, session_id: this.nativeSession};
      try { await this.publish(event, raw); }
      catch { publicationFailed = true; throw new RuntimeError('event_storage_failed'); }
    };
    try {
      if (this.abort.signal.aborted) throw new RuntimeError('cancelled');
      const stream = await this.open(this.abort.signal);
      for await (const raw of stream) {
        let mapped = this.provider === 'claude' ? claudeEvent(raw as SDKMessage) : codexEvent(raw as ThreadEvent);
        const blocked=this.policyError?.();
        if(blocked&&mapped.status==='failed')mapped={...mapped,code:blocked};
        await emit(mapped, raw);
      }
      if (this.abort.signal.aborted) throw new RuntimeError('cancelled');
      if (!terminal) throw new RuntimeError('stream_ended_without_result');
    } catch (error) {
      const code = this.timeout ? 'runtime_timeout' : this.abort.signal.aborted ? 'cancelled' :
        this.policyError?.() ?? (error instanceof RuntimeError ? error.code : 'provider_error');
      this.abort.abort();
      if (publicationFailed) { this.state = 'failed'; throw new RuntimeError('event_storage_failed'); }
      this.state = code === 'cancelled' ? 'cancelled' : 'failed';
      await emit({status: code === 'cancelled' ? 'cancelled' : 'failed', native_type: 'bridge.error', code}, {code});
    } finally {
      this.settled = true;
      clearTimeout(this.timer);
    }
  }
}

export interface CodexEnforcement {baseUrl:string;readonly errorCode?:string;toolServer:string;python:string;externalSandbox?:true;}
export class CodexRuntime {
  get capabilities() {return Object.freeze({start: true, events: true, resume: true, cancel: true,
    reply_between_turns: true, reply_during_turn: false, tool_allowlist: !!this.enforcement,
    cost_budget: !!this.enforcement, turn_limit: !!this.enforcement, native_cost_budget: false,
    structured_error_codes: !!this.enforcement});}
  constructor(private readonly binding: RuntimeBinding, private readonly environment: Record<string, string>,
    private readonly publish: Publish, private readonly factory?: CodexFactory,
    private readonly enforcement?:CodexEnforcement) {
    this.binding = Object.freeze({...binding});
    this.environment = Object.freeze({...environment});
  }

  async start(context: AttemptContext, prompt: string, session?: string): Promise<RuntimeHandle> {
    validate(context, this.binding);
    context = snapshot(context);
    if (!this.enforcement && (context.allowed_tools !== undefined || context.max_turns !== undefined || context.max_budget_usd !== undefined)) {
      throw new RuntimeError('capability_unavailable');
    }
    if(this.enforcement && (context.allowed_tools===undefined || context.allowed_tools.some(t=>!['Read','Grep','Glob','Edit','Write','Bash'].includes(t))))throw new RuntimeError('capability_unavailable');
    const clientOptions: CodexOptions = {codexPathOverride: this.binding.cli_path, env: this.environment,
      config: {shell_environment_policy: {inherit: 'none'}}};
    if(this.enforcement) {
      const e=this.enforcement;
      if(!/^http:\/\/127\.0\.0\.1:[0-9]+\/v1$/.test(e.baseUrl)||!isAbsolute(e.toolServer)||!isAbsolute(e.python))throw new RuntimeError('invalid_policy');
      clientOptions.env={...this.environment,STAGE34_INFERENCE:'local-relay'};
      for(const key of ['CODEX_API_KEY','OPENAI_API_KEY','ANTHROPIC_API_KEY','CLAUDE_CODE_OAUTH_TOKEN'])delete clientOptions.env[key];
      clientOptions.config={shell_environment_policy:{inherit:'none'},model_provider:'stage34',
        model_providers:{stage34:{name:'Stage34 policy gateway',base_url:e.baseUrl,env_key:'STAGE34_INFERENCE',
          wire_api:'responses',requires_openai_auth:false,supports_websockets:false,request_max_retries:0,stream_max_retries:0}},
        features:{code_mode:false,shell_snapshot:false,shell_tool:context.allowed_tools!.includes('Bash'),multi_agent:false},
        projects:{[context.workspace]:{trust_level:'untrusted'}},
        mcp_servers:{stage34:{command:e.python,args:[e.toolServer,'--workspace',context.workspace,'--tools',JSON.stringify(context.allowed_tools)],
          enabled:true,required:true,env_vars:[],enabled_tools:context.allowed_tools!.filter(t=>t!=='Bash'),
          default_tools_approval_mode:'prompt',tools:Object.fromEntries(context.allowed_tools!.filter(t=>t!=='Bash').map(t=>[t,{approval_mode:'approve'}]))}}};
    }
    const client = this.factory ? this.factory(clientOptions) :
      new (await import('@openai/codex-sdk')).Codex(clientOptions);
    const options: ThreadOptions = {model: context.model, workingDirectory: context.workspace,
      // Only the ticket-verified managed-container entrypoint supplies externalSandbox.
      // Its Docker policy enforces readonly root, per-job workspace mode, network/cap/UID
      // isolation. Standalone/legacy callers retain the native sandbox unconditionally.
      sandboxMode: this.enforcement?.externalSandbox===true?'danger-full-access':context.sandbox, approvalPolicy: 'never', networkAccessEnabled: false,
      webSearchMode: 'disabled', skipGitRepoCheck: false, additionalDirectories: []};
    const thread = session ? client.resumeThread(session, options) : client.startThread(options);
    return new RuntimeHandle('codex', context, this.binding, async signal =>
      (await thread.runStreamed(prompt, {signal})).events, this.publish, session, this.enforcement?()=>{
        const code=this.enforcement!.errorCode;
        return code===undefined?undefined:['permission_denied','error_max_turns','error_max_budget_usd','authentication_failed','rate_limited'].includes(code)?code:'provider_error';
      }:undefined);
  }

  resume(context: AttemptContext, session: string, prompt: string) { return this.start(context, prompt, session); }
  reply(handle: RuntimeHandle, reply: string) {
    const view = handle.inspect();
    if (!view.settled || !view.session_id) throw new RuntimeError('capability_unavailable');
    return this.resume(handle.context, view.session_id, reply);
  }
}

function withinWorkspace(workspace: string, name: string, input: Record<string, unknown>): boolean {
  if (name === 'Bash') return true; // Native shell sandbox is mandatory below.
  const supplied = ['Read', 'Edit', 'Write'].includes(name) ? input.file_path : input.path ?? workspace;
  if (typeof supplied !== 'string' || supplied.includes('\0')) return false;
  const target = resolve(workspace, supplied);
  const inside = (root: string, path: string) => {
    const rel = relative(root, path);
    return rel === '' || (!isAbsolute(rel) && rel !== '..' && !rel.startsWith('../'));
  };
  if (!inside(workspace, target)) return false;
  try {
    const root = realpathSync(workspace);
    let ancestor = target;
    for (;;) {
      try { return inside(root, realpathSync(ancestor)); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') return false;
        const parent = dirname(ancestor);
        if (parent === ancestor) return false;
        ancestor = parent;
      }
    }
  } catch { return false; }
}

export class ClaudeRuntime {
  readonly capabilities = Object.freeze({start: true, events: true, resume: true, cancel: true,
    reply_between_turns: true, reply_during_turn: false, tool_allowlist: true, native_cost_budget: true,
    structured_error_codes: true});
  constructor(private readonly binding: RuntimeBinding, private readonly environment: Record<string, string>,
    private readonly publish: Publish, private readonly factory?: ClaudeFactory) {
    this.binding = Object.freeze({...binding});
    this.environment = Object.freeze({...environment});
  }

  async start(context: AttemptContext, prompt: string, session?: string): Promise<RuntimeHandle> {
    validate(context, this.binding);
    context = snapshot(context);
    const allowed = context.allowed_tools ?? [];
    if (allowed.some(tool => !['Read', 'Glob', 'Grep', 'Edit', 'Write', 'Bash'].includes(tool))) {
      throw new RuntimeError('capability_unavailable');
    }
    const tools = context.sandbox === 'read-only' ? allowed.filter(tool => ['Read', 'Glob', 'Grep'].includes(tool)) : [...allowed];
    const factory = this.factory ?? (await import('@anthropic-ai/claude-agent-sdk')).query;
    return new RuntimeHandle('claude', context, this.binding, async signal => {
      const abortController = new AbortController();
      const abort = () => abortController.abort();
      signal.addEventListener('abort', abort, {once: true});
      if (signal.aborted) abort();
      const stream = factory({prompt, options: {cwd: context.workspace, model: context.model, resume: session,
        env: this.environment, pathToClaudeCodeExecutable: this.binding.cli_path,
        tools, allowedTools: [], disallowedTools: ['Task', 'Agent'], settingSources: [],
        permissionMode: 'default',
        settings: {permissions: {blockReadsOutsideWorkingDirectories: true,
          blockWritesOutsideWorkingDirectories: true, workingDirectoriesAsGiven: [context.workspace],
          disableBypassPermissionsMode: 'disable', additionalDirectories: []}},
        abortController, maxTurns: context.max_turns, maxBudgetUsd: context.max_budget_usd,
        sandbox: {enabled: true, allowUnsandboxedCommands: false, autoAllowBashIfSandboxed: false,
          network: {allowedDomains: []}},
        canUseTool: async (name, input) => tools.includes(name) && input.dangerouslyDisableSandbox !== true &&
          input.run_in_background !== true && withinWorkspace(context.workspace, name, input) ? {behavior: 'allow', updatedInput: input} :
          {behavior: 'deny', message: 'tool_not_allowed_by_worker_policy'},
      }});
      return {async *[Symbol.asyncIterator]() {
        try { yield* stream; }
        finally { signal.removeEventListener('abort', abort); stream.close(); }
      }};
    }, this.publish, session);
  }

  resume(context: AttemptContext, session: string, prompt: string) { return this.start(context, prompt, session); }
  reply(handle: RuntimeHandle, reply: string) {
    const view = handle.inspect();
    if (!view.settled || !view.session_id) throw new RuntimeError('capability_unavailable');
    return this.resume(handle.context, view.session_id, reply);
  }
}
