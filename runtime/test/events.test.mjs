import test from 'node:test';
import assert from 'node:assert/strict';
import {claudeEvent, codexEvent} from '../dist/events.js';

test('Codex error prose stays out of canonical metadata', () => {
  const raw = 'private provider failure text';
  const result = codexEvent({type: 'turn.failed', error: {message: raw}});
  assert.equal(result.status, 'failed');
  assert.equal(result.code, 'provider_error');
  assert.equal(JSON.stringify(result).includes(raw), false);
});

test('Claude success subtype with is_error remains a failed run', () => {
  const result = claudeEvent({type: 'result', subtype: 'success', is_error: true,
    session_id: 'native-session', result: 'DONE', modelUsage: {}});
  assert.equal(result.status, 'failed');
  assert.equal(JSON.stringify(result).includes('DONE'), false);
});

test('Claude model usage retains its native cumulative accounting', () => {
  const result = claudeEvent({type: 'result', subtype: 'success', is_error: false,
    session_id: 'native-session', modelUsage: {
      first: {inputTokens: 11, outputTokens: 7, cacheReadInputTokens: 3, cacheCreationInputTokens: 2},
      second: {inputTokens: 5, outputTokens: 1, cacheReadInputTokens: 0, cacheCreationInputTokens: 0},
    }});
  assert.deepEqual(result.usage, {input_tokens: 16, output_tokens: 8,
    cached_input_tokens: 3, cache_write_input_tokens: 2, accounting: 'native_cumulative'});
  assert.equal(result.status, 'completed');
  assert.equal(Object.hasOwn(result, 'accepted'), false);
});

test('Codex turn usage is not mislabeled cumulative', () => {
  const result = codexEvent({type: 'turn.completed', usage: {input_tokens: 12,
    output_tokens: 4, cached_input_tokens: 3, cache_write_input_tokens: 0,
    reasoning_output_tokens: 1}});
  assert.equal(result.usage.accounting, 'turn');
  assert.equal(result.usage.input_tokens, 12);
});

test('Claude structured auth, permission and rate failures remain distinct', () => {
  assert.equal(claudeEvent({type: 'auth_status', session_id: 'session', error: 'private'}).status,
    'authentication_failed');
  assert.equal(claudeEvent({type: 'system', subtype: 'permission_denied', session_id: 'session'}).status,
    'permission_denied');
  assert.equal(claudeEvent({type: 'rate_limit_event', session_id: 'session',
    rate_limit_info: {status: 'rejected'}}).status, 'rate_limited');
});
