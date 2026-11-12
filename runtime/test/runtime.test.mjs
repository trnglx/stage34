import test from 'node:test';
import assert from 'node:assert/strict';
import {CodexRuntime, ClaudeRuntime} from '../dist/runtime.js';

const context = () => ({task_id: 'task-1', attempt_id: 'attempt-1', generation: 1, node_id: 'worker-1',
  policy_sha256: 'a'.repeat(64), workspace: '/isolated/worktree', model: 'example-model',
  sandbox: 'workspace-write', timeout_ms: 10_000});
const binding = provider => ({sdk_version: provider === 'claude' ? '0.3.319' : '0.167.0',
  sdk_sha256: 'b'.repeat(64), cli_version: provider === 'claude' ? '2.1.319' : '0.167.0',
  cli_sha256: 'c'.repeat(64), cli_path: '/immutable/bin/' + provider});
const usage = {input_tokens: 1, output_tokens: 2, cached_input_tokens: 0,
  cache_write_input_tokens: 0, reasoning_output_tokens: 0};
const codexFactory = () => ({startThread: () => ({id: null, runStreamed: async () => ({events: (async function* () {
  yield {type: 'thread.started', thread_id: 'native-session'};
  yield {type: 'turn.completed', usage};
})()})})});

test('Codex captures identity, safe native options and per-attempt provenance', async () => {
  const events = [];
  let options;
  let nativeConfig;
  const client = codexFactory();
  const runtime = new CodexRuntime(binding('codex'), {PATH: '/bin'}, async event => events.push(event), config => {
    nativeConfig = config;
    return {...client, startThread: given => { options = given; return client.startThread(given); }};
  });
  const job = context();
  const handle = await runtime.start(job, 'bounded task');
  job.generation = 999;
  await handle.events();
  assert.equal(events[0].generation, 1);
  assert.equal(handle.inspect().session_id, 'native-session');
  assert.equal(handle.inspect().state, 'completed');
  assert.equal(options.sandboxMode, 'workspace-write');
  assert.equal(options.networkAccessEnabled, false);
  assert.equal(options.skipGitRepoCheck, false);
  assert.equal(nativeConfig.config.shell_environment_policy.inherit, 'none');
});

test('Cancellation before consumption never starts a native query', async () => {
  let started = 0;
  const events = [];
  const runtime = new ClaudeRuntime(binding('claude'), {}, async event => events.push(event), () => {
    started++;
    return {async *[Symbol.asyncIterator]() {}, close() {}};
  });
  const handle = await runtime.start(context(), 'bounded task');
  handle.cancel();
  await handle.events();
  assert.equal(started, 0);
  assert.equal(handle.inspect().state, 'cancelled');
  assert.equal(events.at(-1).status, 'cancelled');
});

test('Claude permission policy cannot be widened by caller mutation', async () => {
  const job = {...context(), allowed_tools: ['Read']};
  let observed;
  const runtime = new ClaudeRuntime(binding('claude'), {}, async () => {}, args => {
    observed = args.options;
    return {async *[Symbol.asyncIterator]() {
      yield {type: 'system', subtype: 'init', session_id: 'native-session', model: 'example-model'};
      yield {type: 'result', subtype: 'success', is_error: false, session_id: 'native-session', modelUsage: {}};
      yield {type: 'system', subtype: 'informational', session_id: 'native-session'};
    }, close() {}};
  });
  const handle = await runtime.start(job, 'bounded task');
  job.allowed_tools.push('WebFetch');
  await handle.events();
  assert.deepEqual(observed.tools, ['Read']);
  assert.equal(observed.sandbox.allowUnsandboxedCommands, false);
  assert.equal(observed.permissionMode, 'default');
  assert.equal(handle.inspect().state, 'completed');
});

test('Unsupported Codex limits refuse before native startup', async () => {
  let started = false;
  const runtime = new CodexRuntime(binding('codex'), {}, async () => {}, () => {started = true; return codexFactory();});
  await assert.rejects(runtime.start({...context(), max_budget_usd: 1}, 'bounded task'), /capability_unavailable/);
  assert.equal(started, false);
});

test('only trusted container composition may delegate the native sandbox',async()=>{
 for(const managed of [false,true]){
  let options;
  const client=codexFactory();
  const runtime=new CodexRuntime(binding('codex'),{PATH:'/bin'},async()=>{},()=>({
   ...client,startThread:given=>{options=given;return client.startThread(given);}}),
   {baseUrl:'http://127.0.0.1:12345/v1',toolServer:'/immutable/file_tools.py',python:'/usr/bin/python3',...(managed?{externalSandbox:true}:{})});
  const handle=await runtime.start({...context(),allowed_tools:['Read'],externalSandbox:true},'fixture');
  await handle.events();
  assert.equal(options.sandboxMode,managed?'danger-full-access':'workspace-write');
  assert.equal(options.networkAccessEnabled,false);
 }
});

test('A failed private publisher aborts the running native operation', async () => {
  let signal;
  const runtime = new CodexRuntime(binding('codex'), {}, async () => {throw new Error('storage unavailable');}, () => ({
    startThread: () => ({id: null, runStreamed: async (_prompt, options) => {
      signal = options.signal;
      return {events: (async function* () {yield {type: 'thread.started', thread_id: 'native-session'};})()};
    }}),
  }));
  const handle = await runtime.start(context(), 'bounded task');
  await assert.rejects(handle.events());
  assert.equal(signal.aborted, true);
});

test('Claude file tools are confined to the supplied working directory',async()=>{
 let observed;
 const runtime=new ClaudeRuntime(binding('claude'),{},async()=>{},args=>{
  observed=args.options;
  return {async *[Symbol.asyncIterator](){yield {type:'result',subtype:'success',is_error:false,
    session_id:'native-session',modelUsage:{}};},close(){}};
 });
 const handle=await runtime.start({...context(),allowed_tools:['Read','Write']},'bounded');
 await handle.events();
 assert.equal(observed.settings.permissions.blockReadsOutsideWorkingDirectories,true);
 assert.equal(observed.settings.permissions.blockWritesOutsideWorkingDirectories,true);
 assert.deepEqual(observed.settings.permissions.workingDirectoriesAsGiven,[context().workspace]);
 assert.equal((await observed.canUseTool('Write',{file_path:'/outside/worktree/file',content:'x'})).behavior,'deny');
});

test('Claude refuses symlink escapes and permits ordinary workspace file paths',async t=>{
 const {mkdtempSync,symlinkSync,rmSync,mkdirSync}=await import('node:fs');
 const {tmpdir}=await import('node:os'); const {join}=await import('node:path');
 const root=mkdtempSync(join(tmpdir(),'runtime-paths-')); t.after(()=>rmSync(root,{recursive:true,force:true}));
 const workspace=join(root,'work'); const outside=join(root,'private'); mkdirSync(workspace);mkdirSync(outside);
 symlinkSync(outside,join(workspace,'redirect'));
 let options;
 const runtime=new ClaudeRuntime(binding('claude'),{},async()=>{},args=>{
  options=args.options;
  return {async *[Symbol.asyncIterator](){yield {type:'result',subtype:'success',is_error:false,
    session_id:'native-session',modelUsage:{}};},close(){}};
 });
 await (await runtime.start({...context(),workspace,allowed_tools:['Read','Write']},'task')).events();
 assert.equal((await options.canUseTool('Write',{file_path:join(workspace,'new.txt')})).behavior,'allow');
 assert.equal((await options.canUseTool('Write',{file_path:join(workspace,'redirect','new.txt')})).behavior,'deny');
 assert.equal((await options.canUseTool('Read',{file_path:join(outside,'secret')})).behavior,'deny');
 assert.equal((await options.canUseTool('Write',{})).behavior,'deny');
});
