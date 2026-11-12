if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import hashlib
import json
import re
import secrets
from . import supervisor_lease as leases
VERSION = 1
OPERATIONS = ('create_plan', 'submit_plan', 'task_status', 'dispatch_ready', 'reply_to_question', 'pause', 'resume_task', 'cancel_attempt', 'inspect_evidence', 'request_review', 'request_owner_approval')
READS = ('task_status', 'inspect_evidence')
MAX_REQUEST_BYTES = 1024 * 1024

class APIError(Exception):

    def __init__(self, code):
        self.code = code
        super().__init__(code)

def install(conn):
    leases.install(conn)
    with leases._write(conn):
        existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'control_%'")}
        marker = conn.execute("SELECT value FROM meta WHERE key='task_id_reservations'").fetchone()
        reservations = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_id_reservation'").fetchone()
        if marker:
            guard = conn.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='task_id_reservation_guard'").fetchone()
            if marker[0] != '1' or not reservations or (not guard):
                raise APIError('unavailable_schema')
        elif reservations or existing:
            raise APIError('unavailable_schema')
        else:
            conn.execute('CREATE TABLE task_id_reservation(id TEXT PRIMARY KEY, owner TEXT NOT NULL, created_at REAL NOT NULL)')
            conn.execute("CREATE TRIGGER task_id_reservation_guard BEFORE INSERT ON task WHEN EXISTS(SELECT 1 FROM task_id_reservation WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'identifier_reserved'); END")
            conn.execute("INSERT INTO meta(key,value) VALUES('task_id_reservations','1')")
        expected = {'control_schema', 'control_plan', 'control_plan_task', 'control_command'}
        if existing:
            if existing != expected:
                raise APIError('unavailable_schema')
            row = conn.execute('SELECT version FROM control_schema WHERE id=1').fetchone()
            if row is None or row[0] != VERSION:
                raise APIError('unavailable_schema')
            return
        conn.execute('CREATE TABLE control_schema(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)')
        conn.execute('INSERT INTO control_schema VALUES(1,?)', (VERSION,))
        conn.execute("CREATE TABLE control_plan(id TEXT PRIMARY KEY, slug TEXT NOT NULL, policy_sha TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('draft','submitted')), created_at REAL NOT NULL)")
        conn.execute('CREATE TABLE control_plan_task(task_id TEXT PRIMARY KEY, plan TEXT NOT NULL REFERENCES control_plan(id), ordinal INTEGER NOT NULL, record TEXT NOT NULL)')
        conn.execute('CREATE TABLE control_command(id TEXT PRIMARY KEY, operation TEXT NOT NULL, request_sha TEXT NOT NULL, response TEXT NOT NULL, created_at REAL NOT NULL)')

def _canonical(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise APIError('invalid_request') from error
    if len(raw.encode('ascii')) > MAX_REQUEST_BYTES:
        raise APIError('request_too_large')
    return raw

def _response(ok, code='ok', data=None):
    return {'schema_version': VERSION, 'ok': ok, 'code': code, 'data': data}

class ControlAPI:

    def __init__(self, authority, conn, credential=None, handlers=None):
        self.authority, self.conn, self.credential = (authority, conn, credential)
        self.handlers = dict(handlers or {})
        if any((name not in OPERATIONS or name in ('create_plan', 'submit_plan', 'task_status') or (not callable(handler)) for name, handler in self.handlers.items())):
            raise APIError('invalid_handler')

    def capabilities(self):
        return {'schema_version': VERSION, 'operations': [{'name': name, 'read_only': name in READS, 'available': name in ('create_plan', 'submit_plan', 'task_status') or name in self.handlers} for name in OPERATIONS]}

    def invoke(self, operation, arguments, request_id=None, schema_version=VERSION):
        try:
            if type(schema_version) is not int or schema_version != VERSION:
                raise APIError('unsupported_api_version')
            if operation not in OPERATIONS or not isinstance(arguments, dict):
                raise APIError('invalid_request')
            encoded = _canonical({'operation': operation, 'arguments': arguments})
            if operation in READS:
                if operation == 'task_status':
                    if set(arguments) != {'task_id'} or not isinstance(arguments['task_id'], str):
                        raise APIError('invalid_request')
                    task = self.authority.get(self.conn, arguments['task_id'])
                    return _response(task is not None, 'ok' if task else 'not_found', task)
                if operation not in self.handlers:
                    raise APIError('capability_unavailable')
                return _response(True, data=self.handlers[operation](None, arguments))
            if not isinstance(request_id, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', request_id):
                raise APIError('invalid_request_id')
            request_sha = hashlib.sha256(encoded.encode('ascii')).hexdigest()
            answer = {}

            def body(tx):
                schema = tx.one('SELECT version FROM control_schema WHERE id=1')
                if schema is None or schema['version'] != VERSION:
                    raise APIError('unavailable_schema')
                prior = tx.one('SELECT request_sha,response FROM control_command WHERE id=?', (request_id,))
                if prior:
                    if prior['request_sha'] != request_sha:
                        raise APIError('idempotency_conflict')
                    answer.update(json.loads(prior['response']))
                    tx.audit = False
                    return self.authority.Result(True, 'noop', '')
                if operation == 'create_plan':
                    data = self._create_plan(tx, arguments)
                elif operation == 'submit_plan':
                    data = self._submit_plan(tx, arguments)
                elif operation in self.handlers:
                    data = self.handlers[operation](tx, arguments)
                else:
                    raise APIError('capability_unavailable')
                answer.update(_response(True, data=data))
                tx.q('INSERT INTO control_command VALUES(?,?,?,?,?)', (request_id, operation, request_sha, _canonical(answer), tx.now))
                return self.authority.Result(True, 'ok', '')
            with self.authority.mutation_guard(self.conn, lambda now: leases.assert_current(self.conn, self.credential, now)):
                result = self.authority._run(self.conn, 'api_' + operation, 'supervisor', None, body)
            return answer if result.ok else _response(False, result.code)
        except (APIError, leases.LeaseError, self.authority.Unavailable) as error:
            return _response(False, error.code)

    def _create_plan(self, tx, arguments):
        if set(arguments) != {'slug', 'tasks'}:
            raise APIError('invalid_request')
        items = arguments['tasks']
        if not isinstance(items, list) or not 1 <= len(items) <= self.authority.MAX_ITEMS:
            raise APIError('invalid_plan')
        keyed, ids, aliases, used = ({}, {}, {}, set())
        plan_id = 'P-' + secrets.token_hex(12)
        for item in items:
            if not isinstance(item, dict):
                raise APIError('invalid_plan')
            fields = dict(item)
            key = fields.pop('key', None)
            if not isinstance(key, str) or not re.fullmatch('[a-z][a-z0-9_-]{0,63}', key) or key in keyed:
                raise APIError('invalid_plan')
            pending = fields.pop('depends_on', [])
            if not isinstance(pending, list) or any((not isinstance(dep, str) for dep in pending)):
                raise APIError('invalid_plan')
            for name in ('owns', 'done', 'depends'):
                if name in fields and (not isinstance(fields[name], list)):
                    raise APIError('invalid_plan')
            try:
                fields = self.authority._task_fields(slug=arguments['slug'], **fields)
            except (TypeError, ValueError):
                raise APIError('invalid_plan')
            self.authority._validate_task_fields(fields)
            if fields['lane'] in self.authority.PROFILE.remote_lanes and self.authority._remote_violations(fields['owns']):
                raise APIError('remote_forbidden')
            for _ in range(1024):
                task_id = self.authority.new_task_id(tx, used)
                if tx.one('SELECT 1 FROM control_plan_task WHERE task_id=?', (task_id,)) is None:
                    break
            else:
                raise APIError('identifier_exhausted')
            used.add(task_id)
            tx.q('INSERT INTO task_id_reservation VALUES(?,?,?)', (task_id, plan_id, tx.now))
            keyed[key], ids[key], aliases[key] = (fields, task_id, pending)
        ordered, unresolved = ([], set(keyed))
        while unresolved:
            ready = [key for key in keyed if key in unresolved and all((dep in ordered for dep in aliases[key]))]
            if not ready:
                raise APIError('invalid_dag')
            for key in ready:
                unresolved.remove(key)
                ordered.append(key)
        tx.q("INSERT INTO control_plan VALUES(?,?,?,'draft',?)", (plan_id, arguments['slug'], self.authority.PROFILE.digest, tx.now))
        for ordinal, key in enumerate(ordered):
            fields = keyed[key]
            fields['depends'] += [ids[dep] for dep in aliases[key]]
            tx.q('INSERT INTO control_plan_task VALUES(?,?,?,?)', (ids[key], plan_id, ordinal, _canonical(fields)))
        return {'plan_id': plan_id, 'state': 'draft', 'tasks': ids}

    def _submit_plan(self, tx, arguments):
        if set(arguments) != {'plan_id'} or not isinstance(arguments['plan_id'], str):
            raise APIError('invalid_request')
        plan = tx.one('SELECT * FROM control_plan WHERE id=?', (arguments['plan_id'],))
        if plan is None:
            raise APIError('not_found')
        if plan['policy_sha'] != self.authority.PROFILE.digest:
            raise APIError('policy_drift')
        records = tx.q('SELECT task_id,record FROM control_plan_task WHERE plan=? ORDER BY ordinal', (plan['id'],)).fetchall()
        if plan['state'] == 'draft':
            for record in records:
                result = self.authority._insert_new_task(tx, json.loads(record['record']), record['task_id'], reservation_owner=plan['id'])
                self.authority._transition_row(tx, result.task, None, result.gen, 'new', None, 'open', True, 'ok', 'supervisor')
            tx.q("UPDATE control_plan SET state='submitted' WHERE id=?", (plan['id'],))
        return {'plan_id': plan['id'], 'state': 'submitted', 'tasks': [record['task_id'] for record in records]}
