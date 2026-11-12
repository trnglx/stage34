if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import json
import re
from .ssh_transport import SSHNode, TransportError, exchange, BOOTSTRAP, OPERATIONS, MAX_BYTES

class DockerTransport:

    def __init__(self, *, executable, endpoint, container_id, image_id, node_id, rpc_sha256, rpc_path='/opt/orch/worker_rpc.py', endpoint_path='/state/host/endpoint.json', timeout_s=30):
        self.node = SSHNode('docker-worker', '/usr/bin/python3', rpc_path, rpc_sha256, endpoint_path=endpoint_path, node_id=node_id, timeout_s=timeout_s)
        if not isinstance(executable, str) or not re.fullmatch('/[a-zA-Z0-9_./-]+', executable) or (not isinstance(endpoint, str)) or (not re.fullmatch('unix:///[a-zA-Z0-9_./-]+', endpoint)):
            raise ValueError('invalid_docker_transport')
        if not re.fullmatch('[a-f0-9]{64}', container_id) or not re.fullmatch('sha256:[a-f0-9]{64}', image_id):
            raise ValueError('invalid_docker_transport')
        self.executable, self.endpoint, self.container_id, self.image_id = (executable, endpoint, container_id, image_id)

    def request(self, operation, request_id, arguments):
        if operation not in OPERATIONS or not isinstance(request_id, str) or (not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', request_id)) or (not isinstance(arguments, dict)):
            raise ValueError('invalid_transport_request')
        payload = json.dumps(dict(schema_version=1, operation=operation, request_id=request_id, arguments=arguments), separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(payload) > MAX_BYTES:
            raise ValueError('request_too_large')
        base = [self.executable, '--host', self.endpoint]
        environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'DOCKER_CONFIG': '/nonexistent-orchestrator-config'}
        try:
            format = '{"image":{{json .Image}},"running":{{json .State.Running}},"user":{{json .Config.User}},"labels":{{json .Config.Labels}}}'
            info = json.loads(exchange(base + ['container', 'inspect', '--format', format, self.container_id], b'', self.node.timeout_s, environment=environment))
            if info.get('image') != self.image_id or info.get('running') is not True or info.get('user') != '1000:1000' or (info.get('labels', {}).get('orch.worker_node') != self.node.node_id):
                raise TransportError('worker_identity_mismatch')
            argv = base + ['container', 'exec', '--interactive', '--user', '1000:1000', self.container_id, self.node.python, '-I', '-B', '-c', BOOTSTRAP, self.node.rpc_path, self.node.rpc_sha256, self.node.endpoint_path, self.node.node_id]
            answer = json.loads(exchange(argv, payload, self.node.timeout_s, environment=environment))
        except (OSError, ValueError, RecursionError, AttributeError) as error:
            raise TransportError('invalid_response_or_connection') from error
        if not isinstance(answer, dict) or set(answer) != {'schema_version', 'request_id', 'ok', 'code', 'data'} or type(answer['schema_version']) is not int or (answer['schema_version'] != 1) or (answer['request_id'] != request_id) or (type(answer['ok']) is not bool) or (not isinstance(answer['code'], str)):
            raise TransportError('invalid_response')
        return answer
