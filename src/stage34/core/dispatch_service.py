if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import stat
import threading
import sys
TOOLS = Path(__file__).resolve().parent
pass
from . import capability_cache
from . import dispatch_pump
from . import docker_transport
from . import engine
from . import ssh_transport
from . import supervisor_lease as leases
from . import control_api
from . import worker_events

def configuration(path):
    path = Path(path)
    parent = path.parent.lstat()
    if not path.is_absolute() or not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 63:
        raise ValueError('unsafe_dispatch_configuration')
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63:
            raise ValueError('unsafe_dispatch_configuration')
        raw = os.read(fd, 65537)
        if len(raw) > 65536:
            raise ValueError('invalid_dispatch_configuration')
        value = json.loads(raw)
    finally:
        os.close(fd)
    if not isinstance(value, dict) or set(value) != {'schema_version', 'nodes'} or type(value['schema_version']) is not int or (value['schema_version'] != 1) or (not isinstance(value['nodes'], dict)) or (not 1 <= len(value['nodes']) <= 32):
        raise ValueError('invalid_dispatch_configuration')
    return value

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True, type=Path)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--credential-file', required=True, type=Path)
    parser.add_argument('--init', action='store_true')
    parser.add_argument('--enroll', action='store_true')
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--interval', type=int, default=5)
    args = parser.parse_args()
    stop = threading.Event()
    if not 1 <= args.interval <= 60:
        parser.error('interval must be between 1 and 60 seconds')
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    os.environ['STAGE34_WORK_DIR'] = str(args.db.resolve().parent)
    from . import controller
    try:
        settings = configuration(args.config)
        transports = {}
        policies = {}
        for node, record in settings['nodes'].items():
            if not isinstance(record, dict) or set(record) != {'transport', 'policy', 'enrollment'}:
                raise ValueError('invalid_node_configuration')
            transport = dict(record['transport'])
            kind = transport.pop('kind')
            if kind == 'ssh':
                transports[node] = ssh_transport.SSHTransport(ssh_transport.SSHNode(**transport))
            elif kind == 'docker':
                transports[node] = docker_transport.DockerTransport(**transport)
            else:
                raise ValueError('unsupported_transport')
            policies[node] = record['policy']
        with closing(controller.open_db('rw', path=args.db, timeout=controller.CLI_TIMEOUT)) as conn:
            credential = capability_cache.load(args.credential_file)
            with leases._write(conn):
                leases.assert_current(conn, credential, leases._now(None))
            if args.init:
                engine.install(conn)
                dispatch_pump.install(conn)
            if args.enroll:
                for node, record in settings['nodes'].items():
                    engine.configure_node(conn, credential, node, record['enrollment'])
            runner = dispatch_pump.DispatchPump(controller, conn, credential, transports, policies, lease_ttl=300 if args.watch else None)
            while not stop.is_set():
                if args.watch:
                    leases.renew(conn, credential, ttl=300)
                print(json.dumps(runner.cycle()), flush=True)
                if not args.watch or stop.wait(args.interval):
                    break
        return 0
    except (OSError, ValueError, KeyError, TypeError, controller.Unavailable, leases.LeaseError, dispatch_pump.PumpError, control_api.APIError, worker_events.EventError):
        print(json.dumps({'ok': False, 'code': 'dispatch_service_failed'}))
        return 1
if __name__ == '__main__':
    raise SystemExit(main())
