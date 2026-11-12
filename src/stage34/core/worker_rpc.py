if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import json
import os
import re
from pathlib import Path
import socket
import stat
import struct
import sys
import time
MAX_BYTES = 1024 * 1024

def discover_endpoint(path, node_id):
    """Read the private host descriptor; never accept an arbitrary socket from it."""
    if not path.is_absolute() or path.name != 'endpoint.json' or (not isinstance(node_id, str)) or (not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}', node_id)):
        raise ValueError('invalid_worker_endpoint')
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) & 63 or (info.st_size > 4096):
            raise ValueError('unsafe_worker_endpoint')
        raw = os.read(fd, 4097)
        if len(raw) > 4096:
            raise ValueError('unsafe_worker_endpoint')
        value = json.loads(raw)
    finally:
        os.close(fd)
    if not isinstance(value, dict) or set(value) != {'schema_version', 'node_id', 'boot_id', 'worker_socket'} or type(value['schema_version']) is not int or (value['schema_version'] != 1) or (value['node_id'] != node_id) or (not isinstance(value['boot_id'], str)) or (not re.fullmatch('B-[A-Za-z0-9]{6}', value['boot_id'])):
        raise ValueError('invalid_worker_endpoint')
    root = path.parent
    boot = root / 'boots' / value['boot_id']
    socket_path = boot / 'management' / 'worker.sock'
    if value['worker_socket'] != str(socket_path):
        raise ValueError('invalid_worker_endpoint')
    for directory in (root, root / 'boots', boot, boot / 'management'):
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63:
            raise ValueError('unsafe_worker_endpoint')
    return socket_path

def receive_exact(stream, length, deadline):
    data = bytearray()
    while len(data) < length:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('rpc_deadline')
        stream.settimeout(remaining)
        chunk = stream.recv(length - len(data))
        if not chunk:
            raise ValueError('truncated_frame')
        data.extend(chunk)
    return bytes(data)

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument('--socket', type=Path)
    destination.add_argument('--endpoint', type=Path)
    parser.add_argument('--node-id')
    args = parser.parse_args(argv)
    request_id = None
    sent = False
    try:
        raw = sys.stdin.buffer.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise ValueError('request_too_large')
        request = json.loads(raw)
        if not isinstance(request, dict) or type(request.get('request_id')) is not str:
            raise ValueError('invalid_request')
        request_id = request['request_id']
        path = discover_endpoint(args.endpoint, args.node_id) if args.endpoint is not None else args.socket
        info = path.lstat()
        parent = path.parent.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63 or (not stat.S_ISDIR(parent.st_mode)) or (parent.st_uid != os.getuid()) or stat.S_IMODE(parent.st_mode) & 63:
            raise ValueError('unsafe_worker_socket')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            deadline = time.monotonic() + 25
            client.settimeout(25)
            client.connect(str(path))
            sent = True
            client.sendall(struct.pack('!I', len(raw)) + raw)
            length = struct.unpack('!I', receive_exact(client, 4, deadline))[0]
            if not 1 <= length <= MAX_BYTES:
                raise ValueError('invalid_response_size')
            response = json.loads(receive_exact(client, length, deadline))
        if not isinstance(response, dict) or response.get('request_id') != request_id:
            raise ValueError('invalid_response')
    except (OSError, ValueError, RecursionError):
        if sent:
            return 74
        response = dict(schema_version=1, request_id=request_id, ok=False, code='worker_rpc_unavailable', data=None)
    print(json.dumps(response, separators=(',', ':'), allow_nan=False))
    return 0
if __name__ == '__main__':
    sys.exit(main())
