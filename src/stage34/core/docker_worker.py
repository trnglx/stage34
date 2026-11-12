if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import threading
import time
import sys
from .docker_broker import policy as broker_policy, BrokerError
OWNER_ROOT = Path('/tmp').resolve()

def emit_status(value, stream=None):
    """A disconnected foreground reader must never terminate stopping/protection."""
    try:
        print(json.dumps(value), file=sys.stdout if stream is None else stream, flush=True)
    except (OSError, ValueError):
        pass

class DockerWorkerError(Exception):
    pass

def configuration(path):
    path = Path(path)
    parent = path.parent.lstat()
    if not path.is_absolute() or not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 63:
        raise DockerWorkerError('unsafe_supervisor_configuration')
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63:
            raise DockerWorkerError('unsafe_supervisor_configuration')
        raw = os.read(fd, 16385)
        if len(raw) > 16384:
            raise DockerWorkerError('invalid_supervisor_configuration')
        value = json.loads(raw)
    finally:
        os.close(fd)
    required = {'schema_version', 'executable', 'endpoint', 'node_id', 'policy_sha256', 'worker_image_id', 'job_image_id', 'volume', 'socket_gid', 'broker_volume', 'job_limits'}
    if not isinstance(value, dict) or set(value) != required or type(value['schema_version']) is not int or (value['schema_version'] != 2):
        raise DockerWorkerError('invalid_supervisor_configuration')
    for key, pattern in [('executable', '/[a-zA-Z0-9_./-]+'), ('endpoint', 'unix:///[a-zA-Z0-9_./-]+'), ('node_id', '[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}'), ('volume', '[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}'), ('broker_volume', '[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}'), ('policy_sha256', '[a-f0-9]{64}'), ('worker_image_id', 'sha256:[a-f0-9]{64}'), ('job_image_id', 'sha256:[a-f0-9]{64}')]:
        if not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]):
            raise DockerWorkerError('invalid_supervisor_configuration')
    if type(value['socket_gid']) is not int or not 0 <= value['socket_gid'] <= 65535:
        raise DockerWorkerError('invalid_supervisor_configuration')
    if value['volume'] == value['broker_volume'] or not isinstance(value['job_limits'], dict) or set(value['job_limits']) != {'cpus', 'memory_mb', 'pids_limit', 'max_runtime_ms', 'network', 'slots'}:
        raise DockerWorkerError('invalid_supervisor_configuration')
    try:
        broker_policy({**value['job_limits'], **{k: value[k] for k in ('node_id', 'policy_sha256', 'job_image_id', 'volume')}})
    except BrokerError as error:
        raise DockerWorkerError('invalid_supervisor_configuration') from error
    return value

class DockerWorker:

    def __init__(self, config):
        self.config = config
        self.stop = threading.Event()
        self.container = None
        self.wait = time.sleep
        self.name = 'orch-worker-' + hashlib.sha256(config['node_id'].encode()).hexdigest()[:32]
        self.broker = None
        self.broker_name = self.name.replace('orch-worker-', 'orch-broker-', 1)

    def broker_policy(self):
        c = self.config
        return broker_policy({**c['job_limits'], **{k: c[k] for k in ('node_id', 'policy_sha256', 'job_image_id', 'volume')}})

    def _broker_identity(self, row):
        c = self.config
        labels = row.get('labels') or {}
        digest = hashlib.sha256(json.dumps(self.broker_policy(), sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if row.get('id') != self.broker or row.get('image') != c['worker_image_id'] or row.get('name') != '/' + self.broker_name or (labels.get('orch.broker_node') != c['node_id']) or (labels.get('orch.broker_policy') != digest) or (labels.get('orch.supervisor_uid') != str(os.getuid())):
            raise DockerWorkerError('broker_identity_conflict')

    def _stop_broker(self):
        if not self.broker:
            return
        ids = self.cli('container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'id=' + self.broker).split()
        if not ids:
            return
        if ids != [self.broker]:
            raise DockerWorkerError('broker_identity_conflict')
        row = self.inspect(self.broker)
        self._broker_identity(row)
        if row.get('running') is True:
            self.cli('container', 'stop', '--signal=SIGTERM', '--timeout=-1', self.broker)
        if self.inspect(self.broker).get('running') is not False:
            raise DockerWorkerError('broker_stop_unknown')
        exitcode = self.cli('container', 'inspect', '--format', '{{.State.ExitCode}}', self.broker)
        if exitcode != '0':
            c = self.config
            encoded = json.dumps(self.broker_policy(), sort_keys=True, separators=(',', ':'))
            drained = self.cli('run', '--rm', '--pull=never', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges', '--user', '1000:1000', '--network', 'none', '--pids-limit', '32', '--memory', '64m', '--mount', 'type=volume,src=' + c['broker_volume'] + ',dst=/broker,volume-nocopy', '--entrypoint', '/usr/bin/python3', c['worker_image_id'], '/opt/orch/docker_broker.py', '--drain-check', '--policy', encoded)
            if json.loads(drained) != {'drained': True}:
                raise DockerWorkerError('broker_effect_uncertain')

    def start_broker(self):
        c = self.config
        approved = self.broker_policy()
        self.cli('volume', 'inspect', c['broker_volume'])
        encoded = json.dumps(approved, sort_keys=True, separators=(',', ':'))
        socket_path = c['endpoint'][len('unix://'):]
        self.broker = self.cli('container', 'create', '--name', self.broker_name, '--pull=never', '--restart=no', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges', '--user', '1000:1000', '--group-add', str(c['socket_gid']), '--network', 'none', '--pids-limit', '64', '--memory', '256m', '--cpus', '0.5', '--tmpfs', '/tmp:rw,nosuid,nodev,size=32m', '--log-driver=none', '--label', 'orch.broker_node=' + c['node_id'], '--label', 'orch.broker_policy=' + hashlib.sha256(encoded.encode()).hexdigest(), '--label', 'orch.supervisor_uid=' + str(os.getuid()), '--mount', 'type=volume,src=' + c['volume'] + ',dst=/state,volume-nocopy', '--mount', 'type=volume,src=' + c['broker_volume'] + ',dst=/broker,volume-nocopy', '--mount', 'type=bind,src=' + socket_path + ',dst=' + socket_path, '--entrypoint', '/usr/bin/python3', c['worker_image_id'], '/opt/orch/docker_broker.py', '--serve', '--policy', encoded, '--docker-host', 'unix://' + socket_path)
        if not re.fullmatch('[a-f0-9]{64}', self.broker):
            raise DockerWorkerError('invalid_container_identity')
        self.cli('container', 'start', self.broker)
        ready = False
        for _ in range(50):
            if self.stop.is_set():
                raise DockerWorkerError('supervisor_stopped')
            try:
                status = self.cli('container', 'exec', self.broker, '/usr/bin/python3', '-c', "import socket; s=socket.socket(socket.AF_UNIX);s.settimeout(1);s.connect('/broker/entry/docker.sock');s.close();print('ready')")
                if status == 'ready':
                    ready = True
                    break
            except (DockerWorkerError, OSError, subprocess.SubprocessError):
                pass
            self.wait(0.1)
        if not ready:
            raise DockerWorkerError('broker_unavailable')
        self._verify_broker_daemon(socket_path)

    def _verify_broker_daemon(self, socket_path):
        """The broker must talk to the same daemon the watchdog supervises: a rootless or
        alternate Unix endpoint that mounted a different socket would otherwise let the
        broker dispatch jobs outside the watchdog's daemon."""
        supervisor = self.cli('info', '--format', '{{.ID}}')
        if not re.fullmatch('[a-zA-Z0-9:-]{1,128}', supervisor):
            raise DockerWorkerError('invalid_daemon_identity')
        broker = self.cli('container', 'exec', self.broker, '/usr/local/bin/docker', '--host', 'unix://' + socket_path, 'info', '--format', '{{.ID}}')
        if not re.fullmatch('[a-zA-Z0-9:-]{1,128}', broker):
            raise DockerWorkerError('invalid_daemon_identity')
        if supervisor != broker:
            raise DockerWorkerError('broker_daemon_mismatch')

    def cli(self, *args):
        c = self.config
        result = subprocess.run([c['executable'], '--host', c['endpoint'], *args], capture_output=True, text=True, timeout=30, env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'DOCKER_CONFIG': '/nonexistent-orchestrator-config'})
        if result.returncode or len(result.stdout) > 1024 * 1024:
            raise DockerWorkerError('docker_effect_unknown')
        return result.stdout.strip()

    def inspect(self, container):
        if not re.fullmatch('[a-f0-9]{64}', container):
            raise DockerWorkerError('invalid_container_identity')
        format = '{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}}}'
        return json.loads(self.cli('container', 'inspect', '--format', format, container))

    def stop_jobs(self):
        """No job-controlled files and no Config.Env are read in this emergency path."""
        c = self.config
        ids = self.cli('container', 'ls', '--quiet', '--no-trunc', '--filter', 'label=orch.node=' + c['node_id']).split()
        stopped = 0
        failures = []
        for container in ids:
            try:
                row = self.inspect(container)
                labels = row.get('labels') or {}
                attempt = labels.get('orch.attempt')
                generation = labels.get('orch.generation', '')
                if not isinstance(attempt, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', attempt) or (not isinstance(generation, str)) or (not re.fullmatch('[1-9][0-9]{0,14}', generation)):
                    raise DockerWorkerError('job_identity_conflict')
                key = hashlib.sha256(json.dumps([c['node_id'], attempt, int(generation)], separators=(',', ':')).encode()).hexdigest()[:32]
                if row.get('id') != container or row.get('name') != '/orch-' + key or row.get('image') != c['job_image_id'] or (labels.get('orch.node') != c['node_id']) or (labels.get('orch.policy') != c['policy_sha256']):
                    raise DockerWorkerError('job_identity_conflict')
                if row.get('running') is True:
                    self.cli('container', 'stop', '--signal=SIGTERM', '--timeout=5', container)
                if self.inspect(container).get('running') is not False:
                    raise DockerWorkerError('job_stop_unknown')
                stopped += 1
            except (DockerWorkerError, OSError, ValueError, subprocess.SubprocessError) as error:
                failures.append(str(error) if isinstance(error, DockerWorkerError) else 'job_stop_unknown')
        if failures:
            raise DockerWorkerError(failures[0])
        return stopped

    def _worker_identity(self, row):
        c = self.config
        labels = row.get('labels') or {}
        if row.get('id') != self.container or row.get('image') != c['worker_image_id'] or row.get('name') != '/' + self.name or (labels.get('orch.worker_node') != c['node_id']) or (labels.get('orch.worker_policy') != c['policy_sha256']) or (labels.get('orch.supervisor_uid') != str(os.getuid())):
            raise DockerWorkerError('worker_identity_conflict')

    def _stop_worker(self):
        if not self.container:
            return
        ids = self.cli('container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'id=' + self.container).split()
        if not ids:
            return
        if ids != [self.container]:
            raise DockerWorkerError('worker_identity_conflict')
        row = self.inspect(self.container)
        self._worker_identity(row)
        if row.get('running') is True:
            self.cli('container', 'stop', '--signal=SIGTERM', '--timeout=25', self.container)
        if self.inspect(self.container).get('running') is not False:
            raise DockerWorkerError('worker_stop_unknown')

    def cleanup(self):
        """Retain the owner lock and retry unknown stopping instead of abandoning jobs."""
        backoff = 1
        while True:
            unknown = False
            stops = [self._stop_worker, self._stop_broker]
            if self.container is not None:
                stops.append(self.stop_jobs)
            for stop in stops:
                try:
                    stop()
                except (DockerWorkerError, OSError, ValueError, subprocess.SubprocessError):
                    unknown = True
            try:
                if unknown:
                    raise DockerWorkerError('stop_unknown')
                if self.container:
                    ids = self.cli('container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'id=' + self.container).split()
                    if ids:
                        self.cli('container', 'rm', self.container)
                if self.broker:
                    ids = self.cli('container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'id=' + self.broker).split()
                    if ids:
                        self.cli('container', 'rm', self.broker)
                self.container = None
                self.broker = None
                return
            except (DockerWorkerError, OSError, ValueError, subprocess.SubprocessError):
                emit_status({'schema_version': 1, 'node_id': self.config['node_id'], 'status': 'stop_unknown'})
                self.wait(backoff)
                backoff = min(10, backoff * 2)

    def run(self):
        c = self.config
        brokers = self.cli('container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'name=^/' + self.broker_name + '$').split()
        if len(brokers) > 1:
            raise DockerWorkerError('broker_identity_conflict')
        if brokers:
            self.broker = brokers[0]
            self._broker_identity(self.inspect(self.broker))
        prior = self.cli('container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'name=^/' + self.name + '$').split()
        if len(prior) > 1:
            raise DockerWorkerError('worker_identity_conflict')
        if prior:
            self.container = prior[0]
            self._worker_identity(self.inspect(self.container))
        if prior or brokers:
            self.cleanup()
        self.cli('volume', 'inspect', c['volume'])
        args = ['container', 'create', '--name', self.name, '--pull=never', '--restart=no', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges', '--user', '1000:1000', '--network', 'none', '--pids-limit', '128', '--memory', '1024m', '--cpus', '1', '--tmpfs', '/tmp:rw,nosuid,nodev,size=128m', '--log-driver=none', '--label', 'orch.worker_node=' + c['node_id'], '--label', 'orch.worker_policy=' + c['policy_sha256'], '--label', 'orch.supervisor_uid=' + str(os.getuid()), '--mount', 'type=volume,src=' + c['volume'] + ',dst=/state,volume-nocopy', '--mount', 'type=volume,src=' + c['broker_volume'] + ',dst=/run/orch-broker,volume-subpath=entry,volume-nocopy,readonly', c['worker_image_id'], '--config', '/state/config/worker.json']
        try:
            self.start_broker()
            self.container = self.cli(*args)
            if not re.fullmatch('[a-f0-9]{64}', self.container):
                raise DockerWorkerError('invalid_container_identity')
            self.stop_jobs()
            self.cli('container', 'start', self.container)
            emit_status({'schema_version': 1, 'node_id': c['node_id'], 'container_id': self.container, 'status': 'starting'})
            while not self.stop.wait(1):
                state = self.inspect(self.container)
                self._worker_identity(state)
                if state.get('running') is not True:
                    raise DockerWorkerError('worker_exited')
                broker = self.inspect(self.broker)
                self._broker_identity(broker)
                if broker.get('running') is not True:
                    raise DockerWorkerError('broker_exited')
        finally:
            self.cleanup()

def acquire_owner(worker):
    """Config-directory-independent lock, keyed by the actual daemon ID and node."""
    daemon = worker.cli('info', '--format', '{{.ID}}')
    if not re.fullmatch('[a-zA-Z0-9:-]{1,128}', daemon):
        raise DockerWorkerError('invalid_daemon_identity')
    root = OWNER_ROOT / ('orch-worker-owner-' + str(os.getuid()))
    root.mkdir(mode=448, exist_ok=True)
    parent = root.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 63:
        raise DockerWorkerError('unsafe_supervisor_lock')
    key = hashlib.sha256((daemon + '\x00' + worker.config['node_id']).encode()).hexdigest()
    fd = os.open(str(root / (key + '.lock')), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 384)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63:
            raise DockerWorkerError('unsafe_supervisor_lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    descriptor = None
    try:
        config = configuration(args.config)
        worker = DockerWorker(config)
        descriptor = acquire_owner(worker)
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: worker.stop.set())
        worker.run()
        return 0
    except (OSError, ValueError, DockerWorkerError, subprocess.SubprocessError):
        emit_status({'status': 'docker_worker_failed'}, stream=sys.stderr)
        return 1
    finally:
        if descriptor is not None:
            os.close(descriptor)
if __name__ == '__main__':
    raise SystemExit(main())
