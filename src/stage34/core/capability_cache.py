if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

from dataclasses import asdict
import json
import os
from pathlib import Path
import stat
import tempfile
from . import supervisor_lease as leases

def load(path):
    path = Path(path)
    descriptor = None
    try:
        descriptor = os.open(str(path), os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 63 or info.st_uid != os.getuid() or (info.st_nlink != 1):
            raise leases.LeaseError('credential_permissions')
        with os.fdopen(descriptor, 'rb') as stream:
            descriptor = None
            raw = stream.read(4097)
        if len(raw) > 4096:
            raise leases.LeaseError('credential_unavailable')
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) != {'schema_version', 'holder', 'provider', 'generation', 'secret'}:
            raise leases.LeaseError('credential_unavailable')
        version = data.pop('schema_version')
        if type(version) is not int or version != 1 or type(data['generation']) is not int or (data['generation'] <= 0):
            raise leases.LeaseError('credential_unavailable')
        credential = leases.Credential(**data)
        leases._identity(credential.holder, credential.provider)
        leases._hash(credential.secret)
        return credential
    except (OSError, ValueError, TypeError, UnicodeError) as error:
        raise leases.LeaseError('credential_unavailable') from error
    finally:
        if descriptor is not None:
            os.close(descriptor)

def save(path, credential):
    path = Path(path)
    temporary = None
    try:
        path.parent.mkdir(mode=448, parents=True, exist_ok=True)
        parent = path.parent.stat()
        if parent.st_mode & 63 or parent.st_uid != os.getuid():
            raise leases.LeaseError('credential_permissions')
        if path.exists() or path.is_symlink():
            prior = load(path)
            if (prior.holder, prior.provider) != (credential.holder, credential.provider):
                raise leases.LeaseError('credential_file_conflict')
        data = {'schema_version': 1, **asdict(credential)}
        with tempfile.NamedTemporaryFile(mode='w', dir=str(path.parent), prefix='.capability-', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(data, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(str(temporary), str(path))
        temporary = None
        directory = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as error:
        raise leases.LeaseError('credential_persist_failed') from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
