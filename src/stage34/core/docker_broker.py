if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import fcntl
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import socketserver
import stat
import subprocess
import sys
import tempfile
LIMIT = 1024 * 1024
INSPECT_FORMAT = '{"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}}}'
ID = '[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}'
SHA = '[a-f0-9]{64}'

class BrokerError(Exception):
    pass

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)

def boot_identity():
    value = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if not re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', value):
        raise BrokerError('invalid_linux_boot_identity')
    return value

def policy(value):
    keys = {'node_id', 'policy_sha256', 'job_image_id', 'volume', 'cpus', 'memory_mb', 'pids_limit', 'max_runtime_ms', 'network', 'slots'}
    if not isinstance(value, dict) or set(value) != keys:
        raise BrokerError('invalid_broker_policy')
    for key, pattern in [('node_id', ID), ('policy_sha256', SHA), ('job_image_id', 'sha256:' + SHA), ('volume', '[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}'), ('network', '[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}')]:
        if not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]):
            raise BrokerError('invalid_broker_policy')
    if value['network'] in ('host', 'bridge', 'default'):
        raise BrokerError('invalid_broker_policy')
    for key, low, high in [('memory_mb', 256, 32768), ('pids_limit', 16, 512), ('max_runtime_ms', 1, 3600000), ('slots', 1, 32)]:
        if type(value[key]) is not int or not low <= value[key] <= high:
            raise BrokerError('invalid_broker_policy')
    if type(value['cpus']) not in (int, float) or not math.isfinite(value['cpus']) or (not 0.1 <= value['cpus'] <= 8):
        raise BrokerError('invalid_broker_policy')
    return json.loads(canonical(value))

def private_directory(path):
    path = Path(path)
    path.mkdir(mode=448, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 448:
        raise BrokerError('unsafe_broker_state')
    return path

def atomic(path, value):
    fd, temporary = tempfile.mkstemp(prefix='.registry-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

class Broker:

    def __init__(self, approved, state='/broker', shared='/state', execute=None, docker_host='unix:///var/run/docker.sock'):
        if not isinstance(docker_host, str) or not re.fullmatch('unix:///[a-zA-Z0-9_./-]+', docker_host):
            raise BrokerError('invalid_docker_host')
        self.docker_host = docker_host
        self.policy = policy(approved)
        self.root = private_directory(state)
        self.shared = Path(shared)
        self.registry_path = self.root / 'registry.json'
        self.execute = execute or self.docker
        self.stopping = False
        self.pending = False
        self.boot = boot_identity()
        self.rows = {}
        if self.registry_path.exists():
            fd = os.open(self.registry_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or (stat.S_IMODE(info.st_mode) != 384) or (info.st_size > 16 * LIMIT):
                    raise BrokerError('unsafe_broker_registry')
                saved = json.loads(os.read(fd, 16 * LIMIT + 1))
            finally:
                os.close(fd)
            if set(saved) != {'policy', 'rows', 'pending', 'boot'} or saved['policy'] != self.policy or (not isinstance(saved['rows'], dict)) or (type(saved['pending']) is not bool) or (not isinstance(saved['boot'], str)):
                raise BrokerError('broker_policy_changed')
            self.rows = saved['rows']
            self.pending = saved['pending']
            if self.pending:
                self.boot = saved['boot']
        self.by_id = {row['cid']: name for name, row in self.rows.items() if row.get('cid')}

    def docker(self, args):
        result = subprocess.run(['/usr/local/bin/docker', '--host', self.docker_host, *args], capture_output=True, text=True, timeout=20, env={'PATH': '/usr/bin:/bin', 'DOCKER_CONFIG': '/nonexistent', 'LANG': 'C'})
        if result.returncode or len(result.stdout.encode()) > LIMIT:
            raise BrokerError('docker_effect_unknown')
        return result.stdout.strip()

    def save(self):
        atomic(self.registry_path, {'policy': self.policy, 'rows': self.rows, 'pending': self.pending, 'boot': self.boot})

    def safe_path(self, relative, *, file=False):
        if not isinstance(relative, str) or not relative or relative.startswith('/') or any((p in ('', '.', '..') for p in relative.split('/'))):
            raise BrokerError('unsafe_job_path')
        current = self.shared
        for index, part in enumerate(relative.split('/')):
            current = current / part
            info = current.lstat()
            last = index == len(relative.split('/')) - 1
            if stat.S_ISLNK(info.st_mode) or (not (file and last) and (not stat.S_ISDIR(info.st_mode))):
                raise BrokerError('unsafe_job_path')
            if file and last and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
                raise BrokerError('unsafe_job_path')
        return current

    def snapshot(self, relative):
        source = self.safe_path(relative, file=True)
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63:
                raise BrokerError('unsafe_credentials')
            data = os.read(fd, 16385)
            if len(data) > 16384:
                raise BrokerError('unsafe_credentials')
        finally:
            os.close(fd)
        try:
            lines = data.decode('utf8').splitlines()
        except UnicodeError as error:
            raise BrokerError('unsafe_credentials') from error
        if len(lines) != 1 or '=' not in lines[0] or '\r' in data.decode() or ('\x00' in data.decode()):
            raise BrokerError('unsafe_credentials')
        key, value = lines[0].split('=', 1)
        if key not in ('CODEX_API_KEY', 'OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'CLAUDE_CODE_OAUTH_TOKEN') or not value or len(value) > 8192 or any((c.isspace() for c in value)):
            raise BrokerError('unsafe_credentials')
        return data

    def validate_create(self, args):
        p = self.policy
        fixed = ['container', 'create', '--name']
        if args[:3] != fixed or len(args) < 40 or (not re.fullmatch('orch-[a-f0-9]{32}', args[3])):
            raise BrokerError('create_denied')
        name = args[3]
        expected = fixed + [name, '--pull=never', '--restart=no', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges', '--init', '--no-healthcheck', '--log-driver=none', '--user', '1000:1000', '--cpus', format(p['cpus'], '.15g'), '--memory', str(p['memory_mb']) + 'm', '--memory-swap', str(p['memory_mb']) + 'm', '--pids-limit', str(p['pids_limit']), '--network', p['network'], '--workdir', '/workspace', '--entrypoint', '/usr/bin/timeout', '--tmpfs', '/tmp:rw,nosuid,nodev,size=256m']
        if args[:len(expected)] != expected:
            raise BrokerError('create_denied')
        pos = len(expected)
        labels = {}
        for key in ('orch.node', 'orch.attempt', 'orch.generation', 'orch.policy', 'orch.command', 'orch.command_sha'):
            if args[pos:pos + 1] != ['--label'] or pos + 1 >= len(args) or (not args[pos + 1].startswith(key + '=')):
                raise BrokerError('create_denied')
            labels[key] = args[pos + 1][len(key) + 1:]
            pos += 2
        if labels['orch.node'] != p['node_id'] or labels['orch.policy'] != p['policy_sha256'] or (not re.fullmatch(ID, labels['orch.attempt'])) or (not re.fullmatch(ID, labels['orch.command'])) or (not re.fullmatch(SHA, labels['orch.command_sha'])) or (not re.fullmatch('[1-9][0-9]{0,14}', labels['orch.generation'])):
            raise BrokerError('create_denied')
        key = hashlib.sha256(json.dumps([p['node_id'], labels['orch.attempt'], int(labels['orch.generation'])], separators=(',', ':')).encode()).hexdigest()[:32]
        if name != 'orch-' + key:
            raise BrokerError('create_denied')
        base = 'containers/jobs/' + key
        credential = None
        if args[pos:pos + 1] == ['--env-file']:
            allowed = '/state/' + base + '/credentials/' + labels['orch.command'] + '.' + labels['orch.command_sha'] + '.env'
            if args[pos:pos + 2] != ['--env-file', allowed]:
                raise BrokerError('create_denied')
            credential = self.snapshot(allowed[len('/state/'):])
            pos += 2
        mounts = []
        while args[pos:pos + 1] == ['--mount']:
            if pos + 1 >= len(args):
                raise BrokerError('create_denied')
            mounts.append(args[pos + 1])
            pos += 2
        if len(mounts) != 5:
            raise BrokerError('create_denied')
        entry = re.fullmatch('type=volume,src=' + re.escape(p['volume']) + ',dst=/run/orch-entry,volume-subpath=(host/boots/B-[a-zA-Z0-9]{6}/entry),volume-nocopy,readonly', mounts[0])
        if not entry:
            raise BrokerError('create_denied')
        self.safe_path(entry[1])
        expected_mounts = [mounts[0]]
        for directory, target, readonly in [('workspace', '/workspace', None), ('home', '/home/worker', False), ('control', '/run/orch-control', True), ('output', '/run/orch-output', False)]:
            self.safe_path(base + '/' + directory)
            mount = 'type=volume,src=' + p['volume'] + ',dst=' + target + ',volume-subpath=' + base + '/' + directory + ',volume-nocopy'
            if readonly is True:
                mount += ',readonly'
            if readonly is None and mounts[len(expected_mounts)] == mount + ',readonly':
                mount += ',readonly'
            expected_mounts.append(mount)
        if mounts != expected_mounts:
            raise BrokerError('create_denied')
        tail = args[pos:]
        if len(tail) != 8 or tail[:3] != [p['job_image_id'], '--signal=TERM', '--kill-after=5s'] or tail[4:] != ['/usr/local/bin/node', '/opt/orch/dist/job-child.js', '--command-id', labels['orch.command']]:
            raise BrokerError('create_denied')
        seconds = tail[3]
        if not re.fullmatch('[0-9]+(?:\\.[0-9]{1,3})?s', seconds):
            raise BrokerError('create_denied')
        milliseconds = float(seconds[:-1]) * 1000
        if not 1 <= milliseconds <= p['max_runtime_ms']:
            raise BrokerError('create_denied')
        return (name, labels, credential)

    def owned(self, name):
        if name not in self.rows:
            raise BrokerError('foreign_container')
        record = self.rows[name]
        ids = self.execute(['container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'name=^/' + name + '$']).split()
        if not ids:
            return None
        if len(ids) != 1 or not re.fullmatch(SHA, ids[0]):
            raise BrokerError('foreign_container')
        raw = self.execute(['container', 'inspect', '--format', INSPECT_FORMAT, ids[0]])
        row = json.loads(raw)
        labels = row.get('labels') or {}
        if row.get('id') != ids[0] or row.get('image') != self.policy['job_image_id'] or type(row.get('running')) is not bool or any((labels.get(k) != v for k, v in record['labels'].items())) or (not hmac.compare_digest(str(labels.get('orch.broker_ticket', '')), record['ticket'])):
            raise BrokerError('foreign_container')
        row['labels'] = dict(record['labels'])
        return row

    def handle(self, args):
        if self.stopping or not isinstance(args, list) or len(args) > 100 or any((not isinstance(a, str) or len(a) > 4096 or '\x00' in a for a in args)):
            raise BrokerError('request_denied')
        if args[:2] == ['container', 'create']:
            if self.pending:
                raise BrokerError('broker_effect_uncertain')
            name, labels, credential = self.validate_create(args)
            fingerprint = hashlib.sha256(canonical(args).encode()).hexdigest()
            record = self.rows.get(name)
            if record:
                if record['fingerprint'] != fingerprint:
                    raise BrokerError('create_conflict')
                row = self.owned(name)
                if row:
                    return row['id']
            else:
                if len(self.rows) >= 4096:
                    raise BrokerError('broker_capacity')
                record = {'labels': labels, 'fingerprint': fingerprint, 'ticket': secrets.token_hex(32)}
                self.rows[name] = record
                self.save()
            forwarded = list(args)
            credential_path = None
            try:
                if credential is not None:
                    fd, credential_path = tempfile.mkstemp(prefix='.credential-', dir=self.root)
                    with os.fdopen(fd, 'wb') as stream:
                        stream.write(credential)
                    index = forwarded.index('--env-file')
                    forwarded[index + 1] = credential_path
                index = forwarded.index(self.policy['job_image_id'])
                forwarded[index:index] = ['--label', 'orch.broker_ticket=' + record['ticket']]
                self.pending = True
                self.save()
                cid = self.execute(forwarded)
                row = self.owned(name)
                if not row or row['id'] != cid:
                    raise BrokerError('docker_effect_unknown')
                record['cid'] = cid
                self.by_id[cid] = name
                self.pending = False
                self.save()
                return cid
            finally:
                if credential_path:
                    os.unlink(credential_path)
        if args[:6] == ['container', 'ls', '--all', '--quiet', '--no-trunc', '--filter'] and len(args) == 7:
            match = re.fullmatch('name=\\^/(orch-[a-f0-9]{32})\\$', args[6])
            if not match:
                raise BrokerError('request_denied')
            if match[1] not in self.rows:
                return ''
            row = self.owned(match[1])
            return row['id'] if row else ''
        if args[:4] == ['container', 'inspect', '--format', INSPECT_FORMAT] and len(args) == 5 and (args[4] in self.rows):
            row = self.owned(args[4])
            if row is None:
                raise BrokerError('docker_effect_unknown')
            return canonical(row)
        operation = args[1] if len(args) > 1 and args[0] == 'container' else ''
        if operation in ('start', 'rm', 'stop'):
            if self.pending:
                raise BrokerError('broker_effect_uncertain')
            prefix = ['container', operation] + (['--signal=SIGTERM', '--timeout=5'] if operation == 'stop' else [])
            if len(args) != len(prefix) + 1 or args[:-1] != prefix or (not re.fullmatch(SHA, args[-1])):
                raise BrokerError('request_denied')
            name = self.by_id.get(args[-1])
            if name is not None:
                row = self.owned(name)
                if row and row['id'] == args[-1]:
                    if operation == 'start' and (not row['running']):
                        running_ids = self.execute(['container', 'ls', '--quiet', '--no-trunc', '--filter', 'label=orch.node=' + self.policy['node_id']]).split()
                        if any((not re.fullmatch(SHA, cid) for cid in running_ids)):
                            raise BrokerError('docker_effect_unknown')
                        running = len(running_ids)
                        if running >= self.policy['slots']:
                            raise BrokerError('broker_capacity')
                    self.pending = True
                    self.save()
                    output = self.execute(prefix + [row['id']])
                    if operation == 'rm':
                        del self.rows[name]
                        del self.by_id[row['id']]
                    self.pending = False
                    self.save()
                    return output
            raise BrokerError('foreign_container')
        raise BrokerError('request_denied')

def receive(stream):
    raw = stream.readline(LIMIT + 1)
    if not raw.endswith(b'\n') or len(raw) > LIMIT:
        raise BrokerError('request_too_large')
    return json.loads(raw)

def serve(approved, docker_host='unix:///var/run/docker.sock'):
    broker = Broker(approved, docker_host=docker_host)
    owner = os.open(broker.root / 'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 384)
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    entry = private_directory(broker.root / 'entry')
    path = entry / 'docker.sock'
    if path.exists():
        if not stat.S_ISSOCK(path.lstat().st_mode):
            raise BrokerError('unsafe_broker_socket')
        path.unlink()

    class Handler(socketserver.StreamRequestHandler):

        def handle(self):
            self.connection.settimeout(5)
            try:
                request = receive(self.rfile)
                if not isinstance(request, dict) or set(request) != {'version', 'args'} or type(request['version']) is not int or (request['version'] != 1):
                    raise BrokerError('request_denied')
                output = broker.handle(request['args'])
                answer = {'ok': True, 'output': output}
            except (BrokerError, OSError, ValueError, subprocess.SubprocessError):
                answer = {'ok': False, 'output': 'broker_request_refused_or_unknown'}
            try:
                self.wfile.write((canonical(answer) + '\n').encode())
            except OSError:
                pass
    with socketserver.UnixStreamServer(str(path), Handler) as server:
        os.chmod(path, 384)
        server.timeout = 0.2
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: setattr(broker, 'stopping', True))
        while not broker.stopping:
            server.handle_request()
    if broker.pending:
        raise BrokerError('broker_effect_uncertain')

def drain_check(approved, *, state='/broker', shared='/state', docker_host='unix:///var/run/docker.sock'):
    broker = Broker(approved, state, shared, docker_host=docker_host)
    owner = os.open(broker.root / 'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 384)
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = boot_identity()
        if broker.pending and broker.boot == current:
            raise BrokerError('broker_effect_uncertain')
        broker.pending = False
        broker.boot = current
        broker.save()
    finally:
        os.close(owner)
    print(canonical({'drained': True}))

def client(args):
    if len(args) < 3 or args[:2] != ['--host', 'unix:///run/orch-broker/docker.sock']:
        raise BrokerError('invalid_broker_endpoint')
    payload = (canonical({'version': 1, 'args': args[2:]}) + '\n').encode()
    if len(payload) > LIMIT:
        raise BrokerError('request_too_large')
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(25)
        connection.connect('/run/orch-broker/docker.sock')
        connection.sendall(payload)
        with connection.makefile('rb') as stream:
            response = receive(stream)
    if not isinstance(response, dict) or set(response) != {'ok', 'output'} or response['ok'] is not True or (not isinstance(response['output'], str)):
        raise BrokerError('broker_request_refused_or_unknown')
    print(response['output'])

def main():
    try:
        if sys.argv[1:2] in (['--serve'], ['--drain-check']):
            parser = argparse.ArgumentParser()
            parser.add_argument('--serve', action='store_true')
            parser.add_argument('--drain-check', action='store_true')
            parser.add_argument('--policy', required=True)
            parser.add_argument('--docker-host', default='unix:///var/run/docker.sock')
            parsed = parser.parse_args()
            approved = policy(json.loads(parsed.policy))
            if parsed.drain_check:
                drain_check(approved, docker_host=parsed.docker_host)
            else:
                serve(approved, docker_host=parsed.docker_host)
        else:
            client(sys.argv[1:])
        return 0
    except (BrokerError, OSError, ValueError, subprocess.SubprocessError):
        print('broker_request_refused_or_unknown', file=sys.stderr)
        return 1
if __name__ == '__main__':
    raise SystemExit(main())
