/** Trusted local composition. Transport/authority must validate the attempt before calling start. */
import {ClaudeRuntime, CodexRuntime, type AttemptContext, type RuntimeHandle} from './runtime.js';
import {verifyRuntime, type ApprovedRuntime} from './provenance.js';
import {EventSpool} from './spool.js';
import {InferenceRelay} from './inference-relay.js';

export class LocalWorker {
  private handle?: RuntimeHandle;
  private busy = false;
  private closed = false;
  private cancellationRequested = false;
  private constructor(private readonly runtime: ClaudeRuntime | CodexRuntime, readonly spool: EventSpool,private readonly relay?:InferenceRelay) {}

  static async create(approved: ApprovedRuntime, environment: Record<string, string>, spoolRoot: string,gateway?:{socketPath:string;toolServer:string;python:string;externalSandbox?:true}): Promise<LocalWorker> {
    if(gateway&&approved.provider!=='codex')throw new Error('capability_unavailable');
    const binding = await verifyRuntime(approved);
    const spool = new EventSpool(spoolRoot);
    const relay=gateway?new InferenceRelay():undefined;
    try {
      if(relay)await relay.listen(gateway!.socketPath);
      const runtime = approved.provider === 'codex' ? new CodexRuntime(binding, environment, spool.publish,undefined,relay?{get baseUrl(){return relay.baseUrl;},get errorCode(){return relay.errorCode;},toolServer:gateway!.toolServer,python:gateway!.python,...(gateway!.externalSandbox===true?{externalSandbox:true as const}:{})}:undefined) :
        new ClaudeRuntime(binding, environment, spool.publish);
      return new LocalWorker(runtime, spool,relay);
    } catch(error) {spool.close();await relay?.close();throw error;}
  }

  async start(context: AttemptContext, prompt: string, session?: string): Promise<void> {
    if (this.closed) throw new Error('worker_closed');
    if (this.busy) throw new Error('worker_busy');
    this.busy = true;
    this.handle = undefined;
    this.cancellationRequested = false;
    try {
      this.handle = await this.runtime.start(context, prompt, session);
      if (this.cancellationRequested) this.handle.cancel();
      await this.handle.events();
    } finally { this.busy = false; }
  }

  cancel(): void {
    if (this.busy) this.cancellationRequested = true;
    this.handle?.cancel();
  }
  inspect() { return this.handle?.inspect() ?? null; }
  close(): Promise<void> {
    if (this.busy) throw new Error('worker_busy');
    if (!this.closed) { this.spool.close(); this.closed = true;return this.relay?.close()??Promise.resolve(); }
    return Promise.resolve();
  }
}
