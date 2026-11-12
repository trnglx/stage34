import test from 'node:test';
import assert from 'node:assert/strict';
import {CodexApprovalPolicy, runApprovalLoop} from '../dist/approval.js';

// A fake app-server: it emits approval requests and only performs the (workspace) effect after an
// explicit approval. This models the Codex CLI pre-execution contract: a denied request is never
// executed, so no effect can exist.
async function simulate(items, allowed) {
  const performed = [];
  const policy = new CodexApprovalPolicy(allowed === undefined ? null : new Set(allowed));
  const requests = (async function* () { for (const item of items) yield item; })();
  await runApprovalLoop(requests, policy, decision => {
    if (decision.approve) performed.push(decision.request_id);
  }, new AbortController().signal);
  return performed;
}

test('denied command and file change produce no effects under a read-only allowlist', async () => {
  const performed = await simulate([
    {request_id: 'r1', kind: 'command_execution', command: 'rm -rf /'},
    {request_id: 'r2', kind: 'file_change', paths: ['/workspace/secret.txt']},
  ], ['Read']);
  assert.deepEqual(performed, []);
});

test('allowed command and file change execute; MCP and web search never execute', async () => {
  const performed = await simulate([
    {request_id: 'r1', kind: 'command_execution', command: 'ls'},
    {request_id: 'r2', kind: 'file_change', paths: ['/workspace/new.txt']},
    {request_id: 'r3', kind: 'mcp_tool_call', tool: 'some-tool'},
    {request_id: 'r4', kind: 'web_search'},
  ], ['Bash', 'Edit']);
  assert.deepEqual(performed, ['r1', 'r2']);
});

test('absent allowlist approves allowable kinds; MCP and web search stay denied', async () => {
  const performed = await simulate([
    {request_id: 'r1', kind: 'command_execution', command: 'ls'},
    {request_id: 'r2', kind: 'file_change', paths: ['/workspace/new.txt']},
    {request_id: 'r3', kind: 'mcp_tool_call', tool: 'some-tool'},
    {request_id: 'r4', kind: 'web_search'},
  ], undefined);
  assert.deepEqual(performed, ['r1', 'r2']);
});

test('empty allowlist is distinct from absent policy: read-only denies every effect', async () => {
  const performed = await simulate([
    {request_id: 'r1', kind: 'command_execution', command: 'ls'},
    {request_id: 'r2', kind: 'file_change', paths: ['/workspace/new.txt']},
  ], []);
  assert.deepEqual(performed, []);
});

test('abort interrupts a stalled loop awaiting a silent server and runs transport cleanup', async () => {
  const controller = new AbortController();
  let cleaned = false;
  const requests = {
    [Symbol.asyncIterator]() {
      return {
        // A silent server: `next()` never resolves, so the loop blocks on the pending read.
        next: () => new Promise(() => {}),
        return: () => { cleaned = true; return Promise.resolve({done: true}); },
      };
    },
  };
  const answered = [];
  const loop = runApprovalLoop(requests, new CodexApprovalPolicy(null),
    decision => answered.push(decision), controller.signal);
  await new Promise(resolve => setTimeout(resolve, 10)); // let it block on the pending read
  controller.abort();
  const count = await loop;
  assert.equal(count, 0);
  assert.deepEqual(answered, []);
  assert.equal(cleaned, true);
});

test('abort unblocks a real stalled async generator and confirms its finally-cleanup', async () => {
  const controller = new AbortController();
  let cleaned = false;
  // A real (signal-aware) async generator stalled on unresolved input: when the signal fires its
  // pending read rejects, its `finally` runs, and its `return()` completes — the production stdio
  // reader must do the same so shutdown is bounded and cleanup is confirmed.
  async function* stalled(signal) {
    try {
      yield await new Promise((resolve, reject) => {
        if (signal.aborted) return reject(new DOMException('aborted', 'AbortError'));
        signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), {once: true});
      });
    } finally {
      cleaned = true;
    }
  }
  const loop = runApprovalLoop(stalled(controller.signal), new CodexApprovalPolicy(null),
    () => { throw new Error('answer must not run'); }, controller.signal);
  await new Promise(resolve => setTimeout(resolve, 10)); // let it block on the pending read
  controller.abort();
  const count = await loop;
  assert.equal(count, 0);
  assert.equal(cleaned, true);
});


test('abort interrupts a backpressured approval write and closes its source', async () => {
  const ac=new AbortController();let cleaned=false;
  async function* source(){try{yield {request_id:'r1',kind:'command_execution'};}finally{cleaned=true;}}
  const loop=runApprovalLoop(source(),new CodexApprovalPolicy(null),(_decision,signal)=>{
    assert.equal(signal,ac.signal);return new Promise(()=>{});
  },ac.signal,20);
  await new Promise(resolve=>setTimeout(resolve,10));ac.abort();
  assert.equal(await Promise.race([loop,new Promise((_,reject)=>setTimeout(()=>reject(new Error('shutdown hung')),200))]),0);
  assert.equal(cleaned,true);
});
