if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

from dataclasses import dataclass
import json
import os
from pathlib import PurePosixPath
import re
import selectors
import shlex
import subprocess
import time
from typing import Optional
MAX_BYTES = 1024 * 1024
OPERATIONS = ('register_node', 'dispatch', 'subscribe_events', 'reconnect', 'cancel', 'health', 'revoke')

class TransportError(Exception):

    def __init__(self, reason):
        self.code, self.reason = ('transport_unknown', reason)
        super().__init__(self.code + ':' + reason)

@dataclass(frozen=True)
class SSHNode:
    host_alias: str
    python: str
    rpc_path: str
    rpc_sha256: str
    socket_path: Optional[str] = None
    timeout_s: float = 30
    endpoint_path: Optional[str] = None
    node_id: Optional[str] = None

    def __post_init__(self):
        if not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}', self.host_alias):
            raise ValueError('invalid_ssh_alias')
        if (self.socket_path is None) == (self.endpoint_path is None):
            raise ValueError('invalid_remote_endpoint')
        if self.endpoint_path is not None and (not isinstance(self.node_id, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', self.node_id)):
            raise ValueError('invalid_remote_node')
        for path in (self.python, self.rpc_path, self.socket_path if self.socket_path is not None else self.endpoint_path):
            if not isinstance(path, str) or not re.fullmatch('/[a-zA-Z0-9_./-]+', path) or '..' in PurePosixPath(path).parts:
                raise ValueError('invalid_remote_path')
        if not isinstance(self.rpc_sha256, str) or not re.fullmatch('[a-f0-9]{64}', self.rpc_sha256):
            raise ValueError('invalid_worker_digest')
        if isinstance(self.timeout_s, bool) or not isinstance(self.timeout_s, (int, float)) or (not 0 < self.timeout_s <= 60):
            raise ValueError('invalid_transport_timeout')
BOOTSTRAP = "import hashlib,sys\np,d,s,*node=sys.argv[1:]\nb=open(p,'rb').read()\nif hashlib.sha256(b).hexdigest()!=d: raise SystemExit(73)\nsys.argv=[p,'--endpoint',s,'--node-id',node[0]] if node else [p,'--socket',s]\nexec(compile(b,p,'exec'),{'__name__':'__main__','__file__':p})\n"

def exchange(argv, payload, timeout_s, *, environment=None):
    """Write/read concurrently with strict byte/time bounds; effects may be unknown."""
    deadline = time.monotonic() + timeout_s
    output = bytearray()
    errors = bytearray()
    with subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment, close_fds=True) as child:
        selector = selectors.DefaultSelector()
        sent = 0
        try:
            for stream, events in ((child.stdin, selectors.EVENT_WRITE), (child.stdout, selectors.EVENT_READ), (child.stderr, selectors.EVENT_READ)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, events)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportError('timeout')
                for key, _ in selector.select(min(remaining, 0.25)):
                    stream = key.fileobj
                    if stream is child.stdin:
                        try:
                            count = os.write(stream.fileno(), payload[sent:sent + 16384])
                            sent += count
                        except BrokenPipeError:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        if sent == len(payload):
                            selector.unregister(stream)
                            stream.close()
                    else:
                        part = os.read(stream.fileno(), 16384)
                        if not part:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        target = output if stream is child.stdout else errors
                        target.extend(part)
                        if len(output) + len(errors) > MAX_BYTES:
                            raise TransportError('response_too_large')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransportError('timeout')
            try:
                status = child.wait(timeout=remaining)
            except subprocess.TimeoutExpired as error:
                raise TransportError('timeout') from error
            if status != 0 or sent != len(payload):
                raise TransportError('connection_or_remote_failure')
            return bytes(output)
        finally:
            selector.close()
            if child.poll() is None:
                child.kill()
            child.wait()

class SSHTransport:

    def __init__(self, node, ssh_executable='/usr/bin/ssh'):
        if not isinstance(node, SSHNode) or not os.path.isabs(ssh_executable):
            raise ValueError('invalid_transport_config')
        self.node, self.ssh_executable = (node, ssh_executable)

    def request(self, operation, request_id, arguments):
        if operation not in OPERATIONS or not isinstance(request_id, str) or (not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', request_id)):
            raise ValueError('invalid_transport_request')
        if not isinstance(arguments, dict):
            raise ValueError('invalid_transport_request')
        payload = json.dumps(dict(schema_version=1, operation=operation, request_id=request_id, arguments=arguments), separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(payload) > MAX_BYTES:
            raise ValueError('request_too_large')
        node = self.node
        destination = [node.socket_path] if node.socket_path is not None else [node.endpoint_path, node.node_id]
        remote = shlex.join([node.python, '-I', '-B', '-c', BOOTSTRAP, node.rpc_path, node.rpc_sha256, *destination])
        argv = [self.ssh_executable, '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ForwardAgent=no', '-o', 'ClearAllForwardings=yes', '-o', 'ConnectTimeout=5', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', '--', node.host_alias, remote]
        environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C'}
        for key in ('HOME', 'SSH_AUTH_SOCK'):
            if key in os.environ:
                environment[key] = os.environ[key]
        try:
            raw = exchange(argv, payload, node.timeout_s, environment=environment)
            response = json.loads(raw)
        except (OSError, ValueError, RecursionError) as error:
            raise TransportError('invalid_response_or_connection') from error
        if not isinstance(response, dict) or set(response) != {'schema_version', 'request_id', 'ok', 'code', 'data'} or type(response['schema_version']) is not int or (response['schema_version'] != 1) or (response['request_id'] != request_id) or (type(response['ok']) is not bool) or (not isinstance(response['code'], str)):
            raise TransportError('invalid_response')
        return response
