import type { ThreadEvent } from '@openai/codex-sdk';
import type { SDKMessage } from '@anthropic-ai/claude-agent-sdk';

export type Provider = 'claude' | 'codex';
export type RuntimeStatus = 'starting' | 'running' | 'completed' | 'failed' |
  'permission_denied' | 'authentication_failed' | 'rate_limited' | 'cancelled' | 'informational';
export interface Usage {
  input_tokens: number;
  output_tokens: number;
  cached_input_tokens: number;
  cache_write_input_tokens: number;
  accounting: 'turn' | 'native_cumulative';
}
export interface MappedEvent {
  status: RuntimeStatus;
  native_type: string;
  session_id?: string;
  model?: string;
  code?: string;
  usage?: Usage;
}

// Completion is a runtime claim. Neither mapping grants candidate acceptance.
export function codexEvent(event: ThreadEvent): MappedEvent {
  switch (event.type) {
    case 'thread.started':
      return {status: 'starting', native_type: event.type, session_id: event.thread_id};
    case 'turn.completed':
      return {status: 'completed', native_type: event.type, usage: {
        input_tokens: event.usage.input_tokens,
        output_tokens: event.usage.output_tokens,
        cached_input_tokens: event.usage.cached_input_tokens,
        cache_write_input_tokens: event.usage.cache_write_input_tokens,
        accounting: 'turn',
      }};
    case 'turn.failed':
    case 'error':
      // The official SDK exposes prose here, not a stable typed error code.
      // Keep the native payload private; never infer authorization from prose.
      return {status: 'failed', native_type: event.type, code: 'provider_error'};
    default:
      return {status: 'running', native_type: event.type};
  }
}

export function claudeEvent(event: SDKMessage): MappedEvent {
  const base = {native_type: event.type, session_id: event.session_id};
  if (event.type === 'system' && event.subtype === 'init') {
    return {...base, status: 'starting', model: event.model};
  }
  if (event.type === 'system' && event.subtype === 'permission_denied') {
    return {...base, status: 'permission_denied', code: 'permission_denied'};
  }
  if (event.type === 'auth_status' && event.error) {
    return {...base, status: 'authentication_failed', code: 'authentication_failed'};
  }
  if (event.type === 'rate_limit_event' && event.rate_limit_info.status === 'rejected') {
    return {...base, status: 'rate_limited', code: 'rate_limited'};
  }
  if (event.type === 'result') {
    const totals = Object.values(event.modelUsage);
    const sum = (key: 'inputTokens' | 'outputTokens' | 'cacheReadInputTokens' | 'cacheCreationInputTokens') =>
      totals.reduce((total, model) => total + model[key], 0);
    const usage: Usage = {
      input_tokens: sum('inputTokens'), output_tokens: sum('outputTokens'),
      cached_input_tokens: sum('cacheReadInputTokens'),
      cache_write_input_tokens: sum('cacheCreationInputTokens'),
      accounting: 'native_cumulative',
    };
    const success = event.subtype === 'success' && !event.is_error;
    return {...base, status: success ? 'completed' : 'failed', usage,
      ...(success ? {} : {code: event.subtype === 'success' ? 'provider_error' : event.subtype})};
  }
  return {...base, status: 'running'};
}
