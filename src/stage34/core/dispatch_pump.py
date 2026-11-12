if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import json
import base64
from decimal import Decimal
from . import inference
from . import provider_connections
import math
import re
import secrets
from . import control_api as api
from . import engine
from . import supervisor_lease as leases
from . import worker_events
from .ssh_transport import TransportError

class PumpError(Exception):
    pass

def _node_runtime(raw, provider, model):
    """Translate corrupt stored enrollment at its read boundary, never hide lease/SQL errors."""
    try:
        runtime = json.loads(raw)['providers'][provider]
        if not isinstance(runtime, dict) or set(runtime) != {'models', 'sdk_version', 'sdk_sha256', 'cli_version', 'cli_sha256'}:
            raise ValueError('invalid_runtime_shape')
        if not isinstance(runtime['models'], list) or model not in runtime['models']:
            raise ValueError('invalid_model')
        if any((not isinstance(runtime[key], str) or not re.fullmatch('[a-f0-9]{64}', runtime[key]) for key in ('sdk_sha256', 'cli_sha256'))):
            raise ValueError('invalid_digest')
        if any((not engine._identity(runtime[key]) for key in ('sdk_version', 'cli_version'))):
            raise ValueError('invalid_version')
        return runtime
    except (ValueError, KeyError, TypeError) as error:
        raise PumpError('invalid_node_configuration') from error

def install(conn):
    worker_events.install(conn)
    inference.install(conn)
    with leases._write(conn):
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dispatch_%'")}
        expected = {'dispatch_schema', 'dispatch_job', 'dispatch_request'}
        if names:
            if names != expected or conn.execute('SELECT version FROM dispatch_schema WHERE id=1').fetchone()[0] != 1:
                raise PumpError('unavailable_schema')
            return
        conn.execute('CREATE TABLE dispatch_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL)')
        conn.execute('INSERT INTO dispatch_schema VALUES(1,1)')
        conn.execute('CREATE TABLE dispatch_job(attempt TEXT PRIMARY KEY REFERENCES engine_assignment(attempt), manifest TEXT NOT NULL,binding TEXT NOT NULL,capability TEXT,deadline_at REAL NOT NULL,journal_id TEXT NOT NULL)')
        conn.execute('CREATE TABLE dispatch_request(command TEXT PRIMARY KEY REFERENCES engine_command(id), request TEXT NOT NULL)')

class DispatchPump:

    def __init__(self, authority, conn, credential, transports, policies, *, lease_ttl=None, inference_upstream=None):
        self.c, self.conn, self.credential = (authority, conn, credential)
        if lease_ttl is not None:
            leases._ttl(lease_ttl)
        self.lease_ttl = lease_ttl
        self.checkpoint_workers = {}
        self.codex_constraint_workers = {}
        self.inference_upstream = inference_upstream
        self.transports, self.policies = (dict(transports), json.loads(api._canonical(policies)))
        if set(self.transports) != set(self.policies):
            raise PumpError('invalid_pump_configuration')
        for node, policy in self.policies.items():
            if not engine._identity(node) or not isinstance(policy, dict) or (not {'journal_id', 'sandbox', 'timeout_ms'} <= set(policy)) or set(policy) - {'journal_id', 'sandbox', 'timeout_ms', 'providers', 'fallbacks', 'inference'}:
                raise PumpError('invalid_pump_configuration')
            if not isinstance(policy['journal_id'], str) or not re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', policy['journal_id']):
                raise PumpError('invalid_worker_journal')
            if policy['sandbox'] not in ('read-only', 'workspace-write') or type(policy['timeout_ms']) is not int or (not 1 <= policy['timeout_ms'] <= 3600000):
                raise PumpError('invalid_pump_configuration')
            providers = policy.get('providers', {})
            if not isinstance(providers, dict) or set(providers) - {'claude', 'codex'}:
                raise PumpError('invalid_provider_policy')
            for constraints in providers.values():
                if not isinstance(constraints, dict) or set(constraints) - {'allowed_tools', 'max_turns', 'max_budget_usd'}:
                    raise PumpError('invalid_provider_policy')
                if 'allowed_tools' in constraints and (not isinstance(constraints['allowed_tools'], list) or len(constraints['allowed_tools']) > 64 or any((not isinstance(tool, str) or not re.fullmatch('[a-zA-Z][a-zA-Z0-9_.*:-]{0,127}', tool) for tool in constraints['allowed_tools']))):
                    raise PumpError('invalid_provider_policy')
                if 'max_turns' in constraints and (type(constraints['max_turns']) is not int or constraints['max_turns'] < 1):
                    raise PumpError('invalid_provider_policy')
                budget = constraints.get('max_budget_usd', 1)
                if isinstance(budget, bool) or not isinstance(budget, (float, int)) or (not math.isfinite(budget)) or (budget <= 0):
                    raise PumpError('invalid_provider_policy')
            fallbacks = policy.get('fallbacks', {})
            if not isinstance(fallbacks, dict):
                raise PumpError('invalid_fallback_policy')
            for task, rule in fallbacks.items():
                if not engine._identity(task) or not isinstance(rule, dict) or set(rule) != {'provider', 'model', 'connection_ref', 'max_handoffs', 'max_total_budget_usd'}:
                    raise PumpError('invalid_fallback_policy')
                if rule['provider'] not in ('claude', 'codex') or not engine._identity(rule['model']) or (not isinstance(rule['connection_ref'], str)) or (not re.fullmatch('connection:[a-zA-Z0-9_.-]{1,64}', rule['connection_ref'])):
                    raise PumpError('invalid_fallback_policy')
                cap = rule['max_total_budget_usd']
                count = rule['max_handoffs']
                constraints = providers.get(rule['provider'], {})
                budget = constraints.get('max_budget_usd')
                if type(count) is not int or not 1 <= count <= 8 or isinstance(cap, bool) or (not isinstance(cap, (int, float))) or (not math.isfinite(cap)) or (cap <= 0) or (budget is None) or (budget * count > cap):
                    raise PumpError('invalid_fallback_budget')
                if rule['provider'] == 'codex' and 'inference' not in policy or 'allowed_tools' not in constraints:
                    raise PumpError('capability_unavailable')
            gateway = policy.get('inference')
            if gateway is not None:
                required = {'allow_paid', 'connection_ref', 'repository_ref', 'connections_dir', 'max_turns', 'max_budget_usd', 'pricing'}
                if not isinstance(gateway, dict) or set(gateway) != required or type(gateway['allow_paid']) is not bool or (not isinstance(gateway['repository_ref'], str)) or (not engine._identity(gateway['repository_ref'])) or (not isinstance(gateway['connections_dir'], str)) or (not gateway['connections_dir'].startswith('/')) or (not isinstance(gateway['pricing'], dict)):
                    raise PumpError('invalid_inference_policy')
                self._inference_policy(node, dict(model=gateway['pricing'].get('model'), allowed_tools=['Read'], max_turns=gateway['max_turns'], max_budget_usd=gateway['max_budget_usd']))

    def _inference_policy(self, node, manifest):
        config = self.policies[node]['inference']
        try:
            price = dict(config['pricing'])
            price['snapshots'] = tuple(price['snapshots'])
            price = inference.Price(**price)
            return inference.InferencePolicy(manifest['model'], tuple(manifest.get('allowed_tools', ['Read', 'Grep', 'Glob', 'Edit', 'Write', 'Bash'])), min(manifest.get('max_turns', config['max_turns']), config['max_turns']), str(min(Decimal(str(manifest.get('max_budget_usd', config['max_budget_usd']))), Decimal(str(config['max_budget_usd'])))), config['connection_ref'], price)
        except (KeyError, TypeError, ValueError, ArithmeticError, inference.InferenceError):
            raise PumpError('invalid_inference_policy')

    def _assert(self):
        if self.conn.in_transaction:
            raise PumpError('transport_inside_transaction')
        if self.lease_ttl is not None:
            leases.renew(self.conn, self.credential, ttl=self.lease_ttl)
            return
        with leases._write(self.conn):
            leases.assert_current(self.conn, self.credential, leases._now(None))

    def _request(self, node, operation, request_id, arguments):
        self._assert()
        arguments = dict(arguments)
        arguments.setdefault('expected_journal_id', self.policies[node]['journal_id'])
        if arguments['expected_journal_id'] != self.policies[node]['journal_id']:
            raise PumpError('worker_identity_changed')
        answer = self.transports[node].request(operation, request_id, arguments)
        if not isinstance(answer, dict) or set(answer) != {'schema_version', 'request_id', 'ok', 'code', 'data'} or type(answer['schema_version']) is not int or (answer['schema_version'] != 1) or (answer['request_id'] != request_id) or (type(answer['ok']) is not bool):
            raise PumpError('invalid_worker_response')
        self._assert()
        return answer

    def _health(self, node):
        answer = self._request(node, 'health', 'H-' + secrets.token_hex(12), {})
        data = answer['data']
        if not answer['ok'] or not isinstance(data, dict) or data.get('node_id') != node or (data.get('journal_id') != self.policies[node]['journal_id']) or (type(data.get('revoked')) is not bool):
            raise PumpError('worker_identity_unavailable')
        self.checkpoint_workers[node] = data.get('checkpoint_restore') is True
        self.codex_constraint_workers[node] = data.get('codex_constraints') is True
        engine.touch_node(self.conn, self.credential, node)
        return data['revoked']

    def _revoke(self, node):

        def body(tx):
            tx.q('UPDATE engine_node SET enabled=0 WHERE id=?', (node,))
            rows = tx.q("SELECT a.id,a.task,a.gen FROM attempt a JOIN engine_assignment e ON e.attempt=a.id WHERE e.node=? AND e.slot_state!='released' AND e.desired!='cancelled'", (node,)).fetchall()
            for row in rows:
                engine.Engine(self.c, self.conn).cancel_attempt(tx, dict(task_id=row['task'], attempt_id=row['id'], generation=row['gen']))
            return self.c.Result(True, 'ok', '')
        with self.c.mutation_guard(self.conn, lambda stamp: leases.assert_current(self.conn, self.credential, stamp)):
            result = self.c._run(self.conn, 'sdk_dispatch_revoked', 'transport', None, body)
        if not result.ok:
            raise PumpError(result.code)

    def _retire_stale(self):

        def body(tx):
            rows = tx.q("SELECT a.id,a.task,a.gen,e.desired FROM attempt a JOIN engine_assignment e ON e.attempt=a.id JOIN task t ON t.id=a.task LEFT JOIN dispatch_job d ON d.attempt=a.id WHERE e.slot_state!='released' AND e.desired!='cancelled' AND (a.gen!=t.gen OR a.state NOT IN ('launching','running','waiting','submitted') OR t.status IN ('done','cancelled') OR d.deadline_at<=?)", (tx.now,)).fetchall()
            for row in rows:
                engine.Engine(self.c, self.conn).cancel_attempt(tx, dict(task_id=row['task'], attempt_id=row['id'], generation=row['gen']))
            return self.c.Result(True, 'ok', '')
        with self.c.mutation_guard(self.conn, lambda stamp: leases.assert_current(self.conn, self.credential, stamp)):
            result = self.c._run(self.conn, 'sdk_dispatch_tick', 'transport', None, body)
        if not result.ok:
            raise PumpError(result.code)

    def _prepare(self, command):
        with leases._write(self.conn):
            leases.assert_current(self.conn, self.credential, leases._now(None))
            row = self.conn.execute('SELECT c.*,e.node,e.provider,e.model,e.base_sha,e.policy_sha,e.desired,a.task,a.gen,a.workspace,n.config FROM engine_command c JOIN engine_assignment e ON e.attempt=c.attempt JOIN attempt a ON a.id=e.attempt JOIN engine_node n ON n.id=e.node WHERE c.id=?', (command,)).fetchone()
            if row is None or row['state'] not in ('pending', 'unknown'):
                return None
            node = row['node']
            policy = self.policies[node]
            stored = self.conn.execute('SELECT * FROM dispatch_job WHERE attempt=?', (row['attempt'],)).fetchone()
            if stored is None:
                constraints = policy.get('providers', {}).get(row['provider'], {})
                if row['operation'] != 'cancel' and (row['provider'] == 'codex' and constraints and ('inference' not in policy) or (row['provider'] == 'claude' and 'allowed_tools' not in constraints)):
                    raise PumpError('capability_unavailable')
                handoff = self.conn.execute("SELECT d.manifest FROM provider_fallback f LEFT JOIN dispatch_job d ON d.attempt=f.old_attempt WHERE f.new_attempt=? AND f.state='consumed'", (row['attempt'],)).fetchone()
                if row['operation'] != 'cancel' and handoff is not None:
                    if handoff['manifest'] is None:
                        raise PumpError('fallback_policy_unavailable')
                    previous = json.loads(handoff['manifest'])
                    if 'allowed_tools' in previous and ('allowed_tools' not in constraints or not set(constraints['allowed_tools']) <= set(previous['allowed_tools'])):
                        raise PumpError('fallback_policy_weakened')
                    for key in ('max_turns', 'max_budget_usd'):
                        if key in previous and (key not in constraints or constraints[key] > previous[key]):
                            raise PumpError('fallback_policy_weakened')
                    if previous['sandbox'] == 'read-only' and policy['sandbox'] != 'read-only':
                        raise PumpError('fallback_policy_weakened')
                    if policy['timeout_ms'] > previous['timeout_ms']:
                        raise PumpError('fallback_policy_weakened')
                runtime = _node_runtime(row['config'], row['provider'], row['model'])
                task = self.c.get(self.conn, row['task'])
                contract = {key: task[key] for key in ('id', 'goal', 'role', 'owns', 'done', 'depends')}
                manifest = dict(task_id=row['task'], attempt_id=row['attempt'], generation=row['gen'], node_id=node, provider=row['provider'], model=row['model'], base_sha=row['base_sha'], policy_sha256=row['policy_sha'], workspace=row['workspace'], sandbox=policy['sandbox'], timeout_ms=policy['timeout_ms'], prompt=api._canonical({'task_contract': contract}), **constraints)
                start = self.conn.execute("SELECT payload FROM engine_command WHERE attempt=? AND operation='start' ORDER BY sequence LIMIT 1", (row['attempt'],)).fetchone()
                if start is not None:
                    checkpoint = json.loads(start['payload']).get('checkpoint')
                    if isinstance(checkpoint, dict):
                        manifest['checkpoint'] = checkpoint
                if row['provider'] == 'codex' and 'inference' in policy:
                    effective = self._inference_policy(node, manifest)
                    manifest.update(inference={'schema_version': 1}, allowed_tools=list(effective.allowed_tools), max_turns=effective.max_turns, max_budget_usd=float(effective.max_budget_usd))
                if len(manifest['prompt'].encode('utf8')) > 256 * 1024:
                    raise PumpError('task_contract_too_large')
                binding = {key: manifest[key] for key in ('task_id', 'attempt_id', 'generation', 'node_id', 'provider', 'policy_sha256')}
                binding.update({key: runtime[key] for key in ('sdk_version', 'sdk_sha256', 'cli_version', 'cli_sha256')})
                self.conn.execute('INSERT INTO dispatch_job VALUES(?,?,?,NULL,?,?)', (row['attempt'], api._canonical(manifest), api._canonical(binding), leases._now(None) + policy['timeout_ms'] / 1000, policy['journal_id']))
            else:
                if stored['journal_id'] != policy['journal_id']:
                    raise PumpError('worker_identity_changed')
                manifest = json.loads(stored['manifest'])
            if row['operation'] != 'cancel' and 'checkpoint' in manifest and (not self.checkpoint_workers.get(node, False)):
                raise PumpError('checkpoint_restore_unavailable')
            if row['operation'] != 'cancel' and manifest.get('inference'):
                if not self.codex_constraint_workers.get(node, False):
                    raise PumpError('capability_unavailable')
                if 'inference' not in policy:
                    raise PumpError('inference_policy_missing')
                if not policy['inference']['allow_paid'] and self.inference_upstream is None:
                    raise PumpError('paid_execution_disabled')
            prior = self.conn.execute('SELECT request FROM dispatch_request WHERE command=?', (command,)).fetchone()
            if prior is None:
                operation = row['operation']
                arguments = {'attempt_id': row['attempt']}
                if operation == 'start':
                    arguments = {'operation': 'start', 'job': manifest}
                elif operation == 'cancel':
                    arguments['job'] = manifest
                else:
                    arguments['operation'] = operation
                    if operation == 'reply':
                        arguments['reply'] = json.loads(row['payload'])['reply']
                arguments['expected_journal_id'] = policy['journal_id']
                request = dict(schema_version=1, request_id=command, operation='cancel' if operation == 'cancel' else 'dispatch', arguments=arguments)
                self.conn.execute('INSERT INTO dispatch_request VALUES(?,?)', (command, api._canonical(request)))
            else:
                request = json.loads(prior['request'])
        return (dict(row), request)

    def _grant(self, attempt):
        row = self.conn.execute('SELECT * FROM dispatch_job WHERE attempt=?', (attempt,)).fetchone()
        if row['capability'] is not None:
            return row['capability']

        def persist(token):
            self.conn.execute('UPDATE dispatch_job SET capability=? WHERE attempt=?', (token, attempt))
        return worker_events.grant(self.conn, self.credential, json.loads(row['binding']), persist=persist)

    def _command(self, command):
        prepared = self._prepare(command)
        if prepared is None:
            return
        row, request = prepared
        node = row['node']
        if row['operation'] != 'cancel':
            self._grant(row['attempt'])
        answer = None
        if row['state'] == 'unknown':
            answer = self._request(node, 'reconnect', 'R-' + command, dict(attempt_id=row['attempt'], command_id=command))
            if not answer['ok'] and answer['code'] in ('command_not_found', 'attempt_not_found'):
                answer = None
        if answer is None:
            answer = self._request(node, request['operation'], command, request['arguments'])
        data = answer['data']
        applied = answer['ok'] and isinstance(data, dict) and (data.get('state') == 'applied') and (data.get('attempt_id') == row['attempt'])
        if row['operation'] in ('pause', 'cancel') and (not isinstance(data, dict) or data.get('stopped') is not True):
            applied = False
        session = data.get('session_id') if isinstance(data, dict) else None
        if row['operation'] == 'start' and (not engine._identity(session)):
            applied = False
        engine.acknowledge_command(self.c, self.conn, self.credential, command, outcome='applied' if applied else 'unknown', native_session=session if applied else None)

    def _events(self, node, attempt):
        row = self.conn.execute('SELECT capability,journal_id FROM dispatch_job WHERE attempt=?', (attempt,)).fetchone()
        if row is None or row['capability'] is None:
            return 0
        if row['journal_id'] != self.policies[node]['journal_id']:
            raise PumpError('worker_identity_changed')
        answer = self._request(node, 'subscribe_events', 'E-' + secrets.token_hex(12), dict(attempt_id=attempt, limit=100))
        if not answer['ok'] or not isinstance(answer['data'], dict) or (not isinstance(answer['data'].get('events'), list)) or (len(answer['data']['events']) > 100):
            raise PumpError('invalid_worker_events')
        acks = [worker_events.ingest(self.conn, row['capability'], event) for event in answer['data']['events']]
        if acks:
            self._request(node, 'subscribe_events', 'E-' + secrets.token_hex(12), dict(attempt_id=attempt, limit=1, acknowledgements=acks))
        return len(acks)

    def _renew(self, node, attempt):
        binding = self.conn.execute('SELECT journal_id FROM dispatch_job WHERE attempt=?', (attempt,)).fetchone()
        if binding and binding['journal_id'] != self.policies[node]['journal_id']:
            raise PumpError('worker_identity_changed')
        row = self.conn.execute("SELECT c.id,e.session FROM engine_command c JOIN engine_assignment e ON e.attempt=c.attempt JOIN attempt a ON a.id=c.attempt WHERE c.attempt=? AND c.state='acknowledged' AND a.state='running' AND e.desired='running' ORDER BY c.sequence DESC LIMIT 1", (attempt,)).fetchone()
        if row is None:
            return
        answer = self._request(node, 'reconnect', 'P-' + secrets.token_hex(12), dict(attempt_id=attempt, command_id=row['id']))
        data = answer['data']
        if not answer['ok'] or not isinstance(data, dict) or data.get('state') != 'applied' or (data.get('attempt_id') != attempt) or (data.get('session_id') != row['session']):
            return

        def body(tx):
            current = tx.one('SELECT a.*,t.gen AS current_gen,d.deadline_at,e.desired FROM attempt a JOIN task t ON t.id=a.task JOIN dispatch_job d ON d.attempt=a.id JOIN engine_assignment e ON e.attempt=a.id WHERE a.id=?', (attempt,))
            if current and current['state'] == 'running' and (current['gen'] == current['current_gen']) and (current['desired'] == 'running') and (current['deadline_at'] > tx.now):
                until = min(current['deadline_at'], tx.now + tx.run_lease)
                tx.q('UPDATE attempt SET lease_until=? WHERE id=?', (until, attempt))
                tx.q('UPDATE launch SET lease_until=? WHERE attempt=?', (until, attempt))
            return self.c.Result(True, 'ok', '')
        with self.c.mutation_guard(self.conn, lambda stamp: leases.assert_current(self.conn, self.credential, stamp)):
            result = self.c._run(self.conn, 'sdk_dispatch_heartbeat', 'transport', None, body)
        if not result.ok:
            raise PumpError(result.code)

    def fallbacks(self, node):
        """Explicit per-task private policy only; events are triggers, never authority."""
        rules = self.policies[node].get('fallbacks', {})
        for task, rule in rules.items():
            self._assert()
            handoff = self.conn.execute("SELECT * FROM provider_fallback WHERE task=? AND state='pending'", (task,)).fetchone()
            if handoff is not None:
                if handoff['alternate_provider'] != rule['provider'] or handoff['alternate_model'] != rule['model']:
                    raise PumpError('handoff_binding_mismatch')
                stopped = self.conn.execute('SELECT a.state,e.slot_state FROM attempt a JOIN engine_assignment e ON e.attempt=a.id WHERE a.id=?', (handoff['old_attempt'],)).fetchone()
                if stopped is None or stopped['state'] != 'cancelled' or stopped['slot_state'] != 'released':
                    continue

                def dispatch(tx):
                    engine.Engine(self.c, self.conn).dispatch_ready(tx, dict(task_id=task, node_id=node, provider=rule['provider'], model=rule['model']))
                    return self.c.Result(True, 'ok', '')
                with self.c.mutation_guard(self.conn, lambda stamp: leases.assert_current(self.conn, self.credential, stamp)):
                    result = self.c._run(self.conn, 'sdk_provider_fallback_dispatch', 'transport', None, dispatch)
                if not result.ok:
                    raise PumpError(result.code)
                continue
            row = self.conn.execute("SELECT a.id,e.provider,d.manifest FROM attempt a JOIN task t ON t.id=a.task JOIN engine_assignment e ON e.attempt=a.id JOIN dispatch_job d ON d.attempt=a.id WHERE a.task=? AND a.gen=t.gen AND a.state IN ('running','waiting') AND e.node=? AND e.slot_state!='released' AND e.desired!='cancelled'", (task, node)).fetchone()
            if row is None or row['provider'] == rule['provider']:
                continue
            event = self.conn.execute('SELECT payload FROM worker_event WHERE attempt=? ORDER BY sequence DESC LIMIT 1', (row['id'],)).fetchone()
            if event is None:
                continue
            payload = json.loads(event['payload'])
            if payload.get('status') not in ('rate_limited', 'authentication_failed') and (not (payload.get('status') == 'failed' and payload.get('code') == 'provider_error')):
                continue
            used = self.conn.execute('SELECT count(*) FROM provider_fallback WHERE task=?', (task,)).fetchone()[0]
            if used >= rule['max_handoffs']:
                continue
            old = json.loads(row['manifest'])
            new = self.policies[node]['providers'][rule['provider']]
            if not set(new['allowed_tools']) <= set(old.get('allowed_tools', new['allowed_tools'])):
                raise PumpError('fallback_policy_weakened')
            for field in ('max_turns', 'max_budget_usd'):
                if field in old and (field not in new or new[field] > old[field]):
                    raise PumpError('fallback_policy_weakened')
            with self.c.mutation_guard(self.conn, lambda stamp: leases.assert_current(self.conn, self.credential, stamp)):
                result = self.c.fallback_provider(self.conn, task, rule['provider'], rule['model'], authorize=True)
            if not result.ok:
                raise PumpError(result.code)

    def _inference(self, node, attempt):
        assignment = self.conn.execute('SELECT desired,slot_state FROM engine_assignment WHERE attempt=?', (attempt,)).fetchone()
        if assignment is None or assignment['desired'] != 'running' or assignment['slot_state'] == 'released':
            return
        job = self.conn.execute('SELECT manifest FROM dispatch_job WHERE attempt=?', (attempt,)).fetchone()
        if job is None:
            return
        manifest = json.loads(job['manifest'])
        if not manifest.get('inference'):
            return
        config = self.policies[node].get('inference')
        if config is None:
            raise PumpError('inference_policy_missing')
        policy = self._inference_policy(node, manifest)
        answer = self._request(node, 'inference', 'IP-' + secrets.token_hex(12), dict(attempt_id=attempt, action='poll'))
        if not answer['ok'] or not isinstance(answer['data'], dict) or (not isinstance(answer['data'].get('calls'), list)) or (len(answer['data']['calls']) > 1):
            raise PumpError('invalid_inference_response')
        upstream = self.inference_upstream or provider_connections.OpenAIUpstream(provider_connections.SecureConnections(config['connections_dir']), config['repository_ref'], config['allow_paid'])

        def stage(call, body):
            response = self._request(node, 'inference', 'IS-' + secrets.token_hex(12), dict(attempt_id=attempt, action='stage', call_id=call, response_b64=base64.b64encode(body.encode('utf8')).decode('ascii'), status=200))
            if not response['ok'] or not isinstance(response['data'], dict):
                raise inference.InferenceError('inference_effect_unknown')
            return response['data']

        def release(call, sha, code):
            response = self._request(node, 'inference', 'IR-' + secrets.token_hex(12), dict(attempt_id=attempt, action='release', call_id=call, response_sha256=sha, code=code))
            if not response['ok']:
                raise inference.InferenceError('inference_delivery_unknown')
        for call in answer['data']['calls']:
            inference.InferenceDispatcher(inference.InferenceLedger(self.conn, self.credential), attempt, policy, upstream, stage, release).run(call)

    def cycle(self):
        self._assert()
        self._retire_stale()
        result = {'commands': 0, 'events': 0, 'blocked': []}
        for node in self.transports:
            try:
                if self._health(node):
                    self._revoke(node)
                rows = self.conn.execute("SELECT c.id,c.attempt FROM engine_command c JOIN engine_assignment e ON e.attempt=c.attempt WHERE e.node=? AND c.state IN ('pending','unknown') ORDER BY CASE WHEN c.operation='cancel' THEN 0 ELSE 1 END,c.sequence LIMIT 100", (node,)).fetchall()
                seen = set()
                for row in rows:
                    if row['attempt'] in seen:
                        continue
                    seen.add(row['attempt'])
                    self._command(row['id'])
                    result['commands'] += 1
                for row in self.conn.execute("SELECT attempt FROM engine_assignment WHERE node=? AND slot_state!='released' AND desired!='cancelled'", (node,)).fetchall():
                    self._renew(node, row['attempt'])
                    self._inference(node, row['attempt'])
                    result['events'] += self._events(node, row['attempt'])
                self.fallbacks(node)
            except (PumpError, api.APIError, worker_events.EventError, TransportError, inference.InferenceError) as error:
                result['blocked'].append({'node_id': node, 'code': str(error) if isinstance(error, PumpError) else error.code})
        return result
