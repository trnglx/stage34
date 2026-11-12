if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from datetime import date
import hashlib
import json
import re
import sqlite3
from pathlib import PurePosixPath
from . import supervisor_lease as leases
MAX_BODY = 256 * 1024
IDENTITY = re.compile('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}')
MODEL = re.compile('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}')
TOOL_REQUIREMENTS = {'exec_command': ('Bash',), 'shell_command': ('Bash',), 'shell': ('Bash',), 'write_stdin': ('Bash',), 'apply_patch': ('Edit', 'Write'), 'mcp__stage34__Read': ('Read',), 'mcp__stage34__Grep': ('Grep',), 'mcp__stage34__Glob': ('Glob',), 'mcp__stage34__Edit': ('Edit',), 'mcp__stage34__Write': ('Write',)}

class InferenceError(Exception):

    def __init__(self, code):
        self.code = code
        super().__init__(code)

def canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)
    except (ValueError, TypeError):
        raise InferenceError('invalid_inference_payload')

def digest(value):
    return hashlib.sha256(canonical(value).encode('ascii')).hexdigest()

def strict_json(raw):
    if not isinstance(raw, (str, bytes)) or len(raw) > MAX_BODY:
        raise InferenceError('invalid_inference_payload')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise InferenceError('invalid_inference_payload')
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: (_ for _ in ()).throw(InferenceError('invalid_inference_payload')))
    except (ValueError, UnicodeError, RecursionError):
        raise InferenceError('invalid_inference_payload')

def nanodollars(value):
    if isinstance(value, bool):
        raise InferenceError('invalid_budget')
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0:
            raise InferenceError('invalid_budget')
        return int((number * 1000000000).to_integral_value(rounding=ROUND_CEILING))
    except (InvalidOperation, ValueError, TypeError):
        raise InferenceError('invalid_budget')

@dataclass(frozen=True)
class Price:
    model: str
    snapshots: tuple
    input_usd_per_million: str
    cached_input_usd_per_million: str
    output_usd_per_million: str
    source: str
    checked: str
    long_context_threshold: int = 272000
    long_input_multiplier: str = '2'
    long_output_multiplier: str = '1.5'

    def __post_init__(self):
        if not MODEL.fullmatch(self.model) or not isinstance(self.snapshots, tuple) or any((not MODEL.fullmatch(m) for m in self.snapshots)):
            raise InferenceError('invalid_pricing')
        if not self.source.startswith('https://developers.openai.com/api/docs/') or not re.fullmatch('\\d{4}-\\d{2}-\\d{2}', self.checked):
            raise InferenceError('invalid_pricing')
        for value in (self.input_usd_per_million, self.cached_input_usd_per_million, self.output_usd_per_million, self.long_input_multiplier, self.long_output_multiplier):
            nanodollars(value)
        if type(self.long_context_threshold) is not int or self.long_context_threshold < 1:
            raise InferenceError('invalid_pricing')

    @property
    def fingerprint(self):
        return digest(self.__dict__)

    def assert_fresh(self, today=None):
        try:
            age = ((today or date.today()) - date.fromisoformat(self.checked)).days
        except (ValueError, TypeError):
            raise InferenceError('invalid_pricing')
        if not 0 <= age <= 30:
            raise InferenceError('pricing_stale')

    def cost(self, usage):
        if not isinstance(usage, dict):
            raise InferenceError('usage_unavailable')
        incoming, outgoing = (usage.get('input_tokens'), usage.get('output_tokens'))
        details = usage.get('input_tokens_details', {})
        if not isinstance(details, dict):
            raise InferenceError('usage_unavailable')
        cached = details.get('cached_tokens', 0)
        if any((type(n) is not int or not 0 <= n <= 2 ** 53 - 1 for n in (incoming, outgoing, cached))) or cached > incoming:
            raise InferenceError('usage_unavailable')
        if details.get('cache_write_tokens', 0) or usage.get('cache_write_input_tokens', 0):
            raise InferenceError('pricing_unavailable')
        rate_in = Decimal(self.input_usd_per_million)
        rate_cache = Decimal(self.cached_input_usd_per_million)
        rate_out = Decimal(self.output_usd_per_million)
        if incoming > self.long_context_threshold:
            rate_in *= Decimal(self.long_input_multiplier)
            rate_cache *= Decimal(self.long_input_multiplier)
            rate_out *= Decimal(self.long_output_multiplier)
        cost = ((incoming - cached) * rate_in + cached * rate_cache + outgoing * rate_out) * 1000
        return int(cost.to_integral_value(rounding=ROUND_CEILING))

@dataclass(frozen=True)
class InferencePolicy:
    model: str
    allowed_tools: tuple
    max_turns: int
    max_budget_usd: str
    connection_ref: str
    price: Price
    workspace: str = '/workspace'

    def __post_init__(self):
        if not MODEL.fullmatch(self.model) or self.model not in (self.price.model,) + self.price.snapshots:
            raise InferenceError('pricing_unavailable')
        if not isinstance(self.allowed_tools, tuple) or len(set(self.allowed_tools)) != len(self.allowed_tools) or any((t not in ('Read', 'Grep', 'Glob', 'Edit', 'Write', 'Bash') for t in self.allowed_tools)):
            raise InferenceError('capability_unavailable')
        if type(self.max_turns) is not int or not 1 <= self.max_turns <= 10000:
            raise InferenceError('invalid_budget')
        nanodollars(self.max_budget_usd)
        if not re.fullmatch('connection:[a-zA-Z0-9_.-]{1,64}', self.connection_ref):
            raise InferenceError('needs_connection')
        if not isinstance(self.workspace, str) or not self.workspace.startswith('/') or '..' in PurePosixPath(self.workspace).parts:
            raise InferenceError('invalid_workspace')

    @property
    def fingerprint(self):
        return digest(dict(model=self.model, tools=self.allowed_tools, max_turns=self.max_turns, budget=self.max_budget_usd, connection_ref=self.connection_ref, price=self.price.fingerprint, workspace=self.workspace))

    def allows(self, name):
        return isinstance(name, str) and any((tool in self.allowed_tools for tool in TOOL_REQUIREMENTS.get(name, ())))

    def path(self, path):
        if not isinstance(path, str) or not path or '\x00' in path or ('\\' in path):
            raise InferenceError('permission_denied')
        candidate = PurePosixPath(path)
        if '..' in candidate.parts:
            raise InferenceError('permission_denied')
        if candidate.is_absolute() and (not (str(candidate) == self.workspace or str(candidate).startswith(self.workspace.rstrip('/') + '/'))):
            raise InferenceError('permission_denied')

def authorize_request(policy, raw):
    body = strict_json(raw) if isinstance(raw, (str, bytes)) else strict_json(canonical(raw))
    if not isinstance(body, dict) or body.get('model') != policy.model or (not isinstance(body.get('input'), (str, list))):
        raise InferenceError('invalid_inference_payload')
    if body.get('service_tier', 'default') not in ('default', 'auto'):
        raise InferenceError('pricing_unavailable')
    tools = body.get('tools', [])
    if not isinstance(tools, list) or len(tools) > 256:
        raise InferenceError('invalid_inference_payload')
    permitted = []
    for tool in tools:
        if not isinstance(tool, dict):
            raise InferenceError('invalid_inference_payload')
        if tool.get('type') in ('function', 'custom') and policy.allows(tool.get('name')):
            permitted.append(tool)
        elif tool.get('type') == 'namespace' and tool.get('name') == 'mcp__stage34':
            nested = tool.get('tools')
            if not isinstance(nested, list) or len(nested) > 6:
                raise InferenceError('invalid_inference_payload')
            approved = [entry for entry in nested if isinstance(entry, dict) and entry.get('type') == 'function' and isinstance(entry.get('name'), str) and policy.allows('mcp__stage34__' + entry['name'])]
            if approved:
                permitted.append(dict(tool, tools=approved))
    body.update(tools=permitted, stream=True, store=False, parallel_tool_calls=False, service_tier='default')
    return body

def validate_response(policy, raw):
    """Consume a whole response first; reconstruct safe SSE from the terminal output ONLY.

    Never forward raw upstream deltas: contradictory earlier tool events could otherwise execute
    an unvalidated call even if the terminal output looked safe. The native CLI accepts output-
    item-done + response-completed records, as verified by the actual pinned binary fixture.
    """
    if not isinstance(raw, (str, bytes)) or len(raw) > MAX_BODY:
        raise InferenceError('invalid_model_response')
    if isinstance(raw, bytes):
        try:
            raw = raw.decode('utf-8')
        except UnicodeError:
            raise InferenceError('invalid_model_response')
    completed = []
    if raw.lstrip().startswith('{'):
        completed = [strict_json(raw)]
    else:
        for block in re.split('\\r?\\n\\r?\\n', raw):
            data = '\n'.join((line[5:].lstrip() for line in block.splitlines() if line.startswith('data:')))
            if not data or data == '[DONE]':
                continue
            event = strict_json(data)
            if not isinstance(event, dict):
                raise InferenceError('invalid_model_response')
            if event.get('type') == 'response.completed':
                completed.append(event.get('response'))
    if len(completed) != 1 or not isinstance(completed[0], dict):
        raise InferenceError('invalid_model_response')
    response = completed[0]
    if response.get('status') != 'completed' or response.get('model') not in (policy.price.model,) + policy.price.snapshots or (not isinstance(response.get('output'), list)):
        raise InferenceError('invalid_model_response')
    if response.get('service_tier', 'default') != 'default':
        raise InferenceError('pricing_unavailable')
    cost = policy.price.cost(response.get('usage'))
    output = response['output']
    if len(output) > 256:
        raise InferenceError('invalid_model_response')
    for item in output:
        if not isinstance(item, dict):
            raise InferenceError('invalid_model_response')
        kind = item.get('type')
        if kind in ('message', 'reasoning'):
            continue
        namespace = item.get('namespace')
        if kind not in ('function_call', 'custom_tool_call') or namespace not in (None, 'functions', 'mcp__stage34'):
            raise InferenceError('permission_denied')
        name = item.get('name')
        if namespace == 'mcp__stage34':
            if kind != 'function_call' or not isinstance(name, str):
                raise InferenceError('permission_denied')
            name = 'mcp__stage34__' + name
        if not policy.allows(name):
            raise InferenceError('permission_denied')
        if name == 'apply_patch':
            if kind == 'custom_tool_call':
                patch = item.get('input')
            else:
                arguments = strict_json(item.get('arguments', ''))
                if not isinstance(arguments, dict):
                    raise InferenceError('invalid_model_response')
                patch = arguments.get('patch')
            if not isinstance(patch, str):
                raise InferenceError('invalid_model_response')
            for path in re.findall('^\\*\\*\\* (?:Add File|Update File|Delete File|Move to): (.+)$', patch, re.MULTILINE):
                policy.path(path)
        elif name.startswith('mcp__stage34__'):
            arguments = strict_json(item.get('arguments', ''))
            if not isinstance(arguments, dict):
                raise InferenceError('invalid_model_response')
            policy.path(arguments.get('file_path', arguments.get('path', '.')))
    clean = {key: response[key] for key in ('id', 'object', 'status', 'model', 'output', 'usage') if key in response}
    events = [{'type': 'response.created', 'response': dict(clean, status='in_progress', output=[])}]
    events += [{'type': 'response.output_item.done', 'output_index': i, 'item': item} for i, item in enumerate(output)]
    events.append({'type': 'response.completed', 'response': clean})
    stream = ''.join(('event: ' + event['type'] + '\ndata: ' + canonical(event) + '\n\n' for event in events))
    return (stream, cost)
DDL = "\nCREATE TABLE inference_schema(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);\nINSERT INTO inference_schema VALUES(1,1);\nCREATE TABLE inference_attempt(\n attempt TEXT PRIMARY KEY REFERENCES attempt(id), task TEXT NOT NULL, gen INTEGER NOT NULL,\n node TEXT NOT NULL, policy_sha TEXT NOT NULL, max_turns INTEGER NOT NULL, budget_nano INTEGER NOT NULL,\n turns INTEGER NOT NULL DEFAULT 0, spent_nano INTEGER NOT NULL DEFAULT 0,\n blocked TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL);\nCREATE TABLE inference_call(\n id TEXT PRIMARY KEY, attempt TEXT NOT NULL REFERENCES inference_attempt(attempt), body_sha TEXT NOT NULL,\n state TEXT NOT NULL CHECK(state IN ('admitted','completed','denied','unknown')),\n cost_nano INTEGER, response_sha TEXT, code TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL);\nCREATE UNIQUE INDEX inference_one_pending ON inference_call(attempt) WHERE state IN ('admitted','unknown');\n"

def install(conn):
    with leases._write(conn):
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'inference_%'")}
        if names:
            if names != {'inference_schema', 'inference_attempt', 'inference_call'} or conn.execute('SELECT version FROM inference_schema WHERE id=1').fetchone()[0] != 1:
                raise InferenceError('unavailable_schema')
            return
        for stmt in DDL.split(';'):
            if stmt.strip():
                conn.execute(stmt)

class InferenceLedger:

    def __init__(self, conn, supervisor):
        self.conn, self.supervisor = (conn, supervisor)

    @contextmanager
    def current(self, attempt):
        with leases._write(self.conn):
            now = leases._now(None)
            leases.assert_current(self.conn, self.supervisor, now)
            row = self.conn.execute('SELECT a.*,t.gen AS task_gen,e.node,e.desired,e.slot_state FROM attempt a JOIN task t ON t.id=a.task JOIN engine_assignment e ON e.attempt=a.id WHERE a.id=?', (attempt,)).fetchone()
            if row is None or row['gen'] != row['task_gen'] or row['state'] not in ('launching', 'running') or (row['desired'] != 'running') or (row['slot_state'] == 'released') or (row['lease_until'] is None) or (row['lease_until'] <= now):
                raise InferenceError('stale_generation')
            yield (row, now)

    def register(self, attempt, policy):
        with self.current(attempt) as (row, now):
            previous = self.conn.execute('SELECT * FROM inference_attempt WHERE attempt=?', (attempt,)).fetchone()
            if previous is not None:
                if previous['policy_sha'] != policy.fingerprint:
                    raise InferenceError('policy_changed')
                return
            self.conn.execute('INSERT INTO inference_attempt(attempt,task,gen,node,policy_sha,max_turns,budget_nano,created_at) VALUES(?,?,?,?,?,?,?,?)', (attempt, row['task'], row['gen'], row['node'], policy.fingerprint, policy.max_turns, nanodollars(policy.max_budget_usd), now))

    def admit(self, attempt, call, request, policy):
        if not isinstance(call, str) or not IDENTITY.fullmatch(call):
            raise InferenceError('invalid_inference_identity')
        body = authorize_request(policy, request)
        with self.current(attempt) as (row, now):
            grant = self.conn.execute('SELECT * FROM inference_attempt WHERE attempt=?', (attempt,)).fetchone()
            if grant is None or grant['policy_sha'] != policy.fingerprint:
                raise InferenceError('policy_changed')
            existing = self.conn.execute('SELECT * FROM inference_call WHERE id=?', (call,)).fetchone()
            if existing is not None:
                if existing['attempt'] != attempt or existing['body_sha'] != digest(request):
                    raise InferenceError('idempotency_conflict')
                raise InferenceError('inference_already_admitted')
            if grant['blocked']:
                raise InferenceError(grant['blocked'])
            if self.conn.execute("SELECT 1 FROM inference_call WHERE attempt=? AND state IN ('admitted','unknown')", (attempt,)).fetchone():
                raise InferenceError('inference_effect_unknown')
            if grant['turns'] >= grant['max_turns']:
                raise InferenceError('error_max_turns')
            if grant['spent_nano'] >= grant['budget_nano']:
                raise InferenceError('error_max_budget_usd')
            self.conn.execute("INSERT INTO inference_call(id,attempt,body_sha,state,created_at) VALUES(?,?,?,'admitted',?)", (call, attempt, digest(request), now))
            self.conn.execute('UPDATE inference_attempt SET turns=turns+1 WHERE attempt=?', (attempt,))
        return body

    def complete(self, attempt, call, raw, policy, *, recover_staged=False):
        code, stream, cost = ('', '', None)
        try:
            stream, cost = validate_response(policy, raw)
        except InferenceError as error:
            code = error.code
            try:
                body = strict_json(raw) if isinstance(raw, str) and raw.lstrip().startswith('{') else None
                if body is not None:
                    cost = policy.price.cost(body.get('usage'))
            except InferenceError:
                pass
        with self.current(attempt) as (row, now):
            grant = self.conn.execute('SELECT * FROM inference_attempt WHERE attempt=?', (attempt,)).fetchone()
            req = self.conn.execute('SELECT * FROM inference_call WHERE id=? AND attempt=?', (call, attempt)).fetchone()
            recoverable = recover_staged and req is not None and (req['state'] == 'unknown') and (req['code'] == 'inference_effect_unknown') and (req['response_sha'] is None)
            if grant is None or req is None or (req['state'] != 'admitted' and (not recoverable)) or (grant['policy_sha'] != policy.fingerprint):
                raise InferenceError('inference_already_settled')
            if cost is None:
                code = code or 'usage_unavailable'
                state = 'unknown'
            else:
                total = grant['spent_nano'] + cost
                if total > grant['budget_nano']:
                    code = 'error_max_budget_usd'
                self.conn.execute('UPDATE inference_attempt SET spent_nano=? WHERE attempt=?', (total, attempt))
                state = 'denied' if code else 'completed'
            self.conn.execute('UPDATE inference_call SET state=?,cost_nano=?,response_sha=?,code=? WHERE id=?', (state, cost, hashlib.sha256(raw.encode() if isinstance(raw, str) else raw).hexdigest(), code, call))
            if code:
                self.conn.execute('UPDATE inference_attempt SET blocked=? WHERE attempt=?', (code, attempt))
            elif recoverable and grant['blocked'] == 'inference_effect_unknown':
                self.conn.execute("UPDATE inference_attempt SET blocked='' WHERE attempt=?", (attempt,))
        if code:
            raise InferenceError(code)
        return stream

    def uncertain(self, attempt, call):
        with self.current(attempt):
            self.conn.execute("UPDATE inference_call SET state='unknown',code='inference_effect_unknown' WHERE id=? AND attempt=? AND state='admitted'", (call, attempt))
            self.conn.execute("UPDATE inference_attempt SET blocked='inference_effect_unknown' WHERE attempt=?", (attempt,))

class InferenceDispatcher:
    """Two-phase worker cache + authority ledger; a restart never repeats paid inference."""

    def __init__(self, ledger, attempt, policy, upstream, stage, release):
        self.ledger, self.attempt, self.policy = (ledger, attempt, policy)
        self.upstream, self.stage, self.release = (upstream, stage, release)

    def run(self, call):
        import base64
        if not isinstance(call, dict) or not IDENTITY.fullmatch(call.get('id', '')):
            raise InferenceError('invalid_inference_identity')
        try:
            body_raw = base64.b64decode(call['body_b64'], validate=True)
            if len(body_raw) > MAX_BODY or hashlib.sha256(body_raw).hexdigest() != call['body_sha256']:
                raise ValueError('body mismatch')
            body = strict_json(body_raw)
        except (ValueError, TypeError, KeyError):
            raise InferenceError('invalid_inference_payload')
        self.ledger.register(self.attempt, self.policy)
        with self.ledger.current(self.attempt):
            previous = self.ledger.conn.execute('SELECT * FROM inference_call WHERE id=?', (call['id'],)).fetchone()
            if previous is not None and (previous['attempt'] != self.attempt or previous['body_sha'] != digest(body)):
                raise InferenceError('idempotency_conflict')
            previous = dict(previous) if previous is not None else None
        code = ''
        response_sha = call.get('response_sha256')
        try:
            if previous is None:
                authorized = self.ledger.admit(self.attempt, call['id'], body, self.policy)
                raw = self.upstream(self.policy, authorized)
                clean, _ = validate_response(self.policy, raw)
                staged = self.stage(call['id'], clean)
                response_sha = staged['response_sha256']
                if hashlib.sha256(clean.encode('utf-8')).hexdigest() != response_sha:
                    raise InferenceError('invalid_inference_response')
                self.ledger.complete(self.attempt, call['id'], clean, self.policy)
            elif previous['state'] in ('admitted', 'unknown') and call['state'] == 'staged':
                raw = base64.b64decode(call['response_b64'], validate=True)
                if hashlib.sha256(raw).hexdigest() != response_sha:
                    raise InferenceError('invalid_inference_response')
                self.ledger.complete(self.attempt, call['id'], raw, self.policy, recover_staged=True)
            elif previous['state'] == 'completed':
                if call['state'] != 'staged' or previous['response_sha'] != response_sha:
                    raise InferenceError('inference_delivery_unknown')
            else:
                raise InferenceError(previous['code'] or 'inference_effect_unknown')
        except InferenceError as error:
            code = error.code
            with self.ledger.current(self.attempt):
                state = self.ledger.conn.execute('SELECT state FROM inference_call WHERE id=? AND attempt=?', (call['id'], self.attempt)).fetchone()
            if state is not None and state['state'] == 'admitted':
                self.ledger.uncertain(self.attempt, call['id'])
            if response_sha is None:
                message = canonical({'error': {'code': code, 'message': 'Stage34 policy refused the request'}})
                response_sha = self.stage(call['id'], message)['response_sha256']
        with self.ledger.current(self.attempt):
            grant = self.ledger.conn.execute('SELECT policy_sha FROM inference_attempt WHERE attempt=?', (self.attempt,)).fetchone()
            if grant is None or grant['policy_sha'] != self.policy.fingerprint:
                raise InferenceError('policy_changed')
            if not code:
                settled = self.ledger.conn.execute('SELECT state,response_sha FROM inference_call WHERE id=? AND attempt=?', (call['id'], self.attempt)).fetchone()
                if settled is None or settled['state'] != 'completed' or settled['response_sha'] != response_sha:
                    raise InferenceError('inference_delivery_unknown')
        self.release(call['id'], response_sha, code)
        return code or 'completed'
