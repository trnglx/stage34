if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import http.client
import json
import os
from pathlib import Path
import re
import ssl
import stat
from .inference import InferenceError, MAX_BODY, canonical, strict_json, MODEL, IDENTITY
SECURE_BACKENDS = {'keyring.backends.macOS.Keyring', 'keyring.backends.Windows.WinVaultKeyring', 'keyring.backends.SecretService.Keyring', 'keyring.backends.kwallet.DBusKeyring'}

class SecureConnections:

    def __init__(self, directory, backend=None):
        self.directory = Path(directory)
        self.backend = backend

    def store(self):
        if self.backend is not None:
            return self.backend
        try:
            import keyring
            backend = keyring.get_keyring()
            name = type(backend).__module__ + '.' + type(backend).__name__
            if name not in SECURE_BACKENDS:
                raise InferenceError('secure_store_unavailable')
            return backend
        except ImportError:
            raise InferenceError('secure_store_unavailable')
        except InferenceError:
            raise
        except Exception:
            raise InferenceError('secure_store_unavailable')

    def metadata(self, reference):
        if not isinstance(reference, str) or not re.fullmatch('connection:[a-zA-Z0-9_.-]{1,64}', reference):
            raise InferenceError('needs_connection')
        try:
            root_fd = os.open(str(self.directory), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                root = os.fstat(root_fd)
                if root.st_uid != os.getuid() or stat.S_IMODE(root.st_mode) & 63:
                    raise InferenceError('unsafe_connection_store')
                fd = os.open(reference.split(':', 1)[1] + '.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root_fd)
                try:
                    info = os.fstat(fd)
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 63 or (info.st_size > 8192):
                        raise InferenceError('unsafe_connection_store')
                    data = strict_json(os.read(fd, 8193))
                finally:
                    os.close(fd)
            finally:
                os.close(root_fd)
        except (OSError, ValueError):
            raise InferenceError('needs_connection')
        if not isinstance(data, dict) or set(data) != {'schema_version', 'provider', 'endpoint', 'models', 'repository_refs', 'enabled'} or type(data['schema_version']) is not int or (data['schema_version'] != 1) or (data['provider'] != 'openai') or (data['endpoint'] != 'https://api.openai.com/v1') or (type(data['enabled']) is not bool) or (not isinstance(data['models'], list)) or (not isinstance(data['repository_refs'], list)):
            raise InferenceError('invalid_connection_metadata')
        if not data['models'] or not data['repository_refs'] or any((not isinstance(v, str) or not MODEL.fullmatch(v) for v in data['models'])) or any((not isinstance(v, str) or not IDENTITY.fullmatch(v) for v in data['repository_refs'])):
            raise InferenceError('invalid_connection_metadata')
        if not data['enabled']:
            raise InferenceError('connection_revoked')
        return data

    def resolve(self, policy, repository_ref):
        metadata = self.metadata(policy.connection_ref)
        if policy.model not in metadata['models'] or repository_ref not in metadata['repository_refs']:
            raise InferenceError('provider_not_authorized')
        try:
            secret = self.store().get_password('stage34', policy.connection_ref)
        except InferenceError:
            raise
        except Exception:
            raise InferenceError('needs_connection')
        if not isinstance(secret, str) or not secret or len(secret) > 8192 or any((c.isspace() for c in secret)):
            raise InferenceError('needs_connection')
        return secret

    def enroll(self, reference, models, repository_refs, secret):
        """Explicit interactive enrollment; only a reference/scope is written to disk.

        Never overwrite an existing metadata entry or keychain value. An interrupted
        enrollment leaves disabled metadata; it cannot authorize provider execution.
        """
        if not isinstance(reference, str) or not re.fullmatch('connection:[a-zA-Z0-9_.-]{1,64}', reference):
            raise InferenceError('invalid_connection_metadata')
        if not isinstance(models, list) or not models or any((not isinstance(m, str) or not MODEL.fullmatch(m) for m in models)) or (not isinstance(repository_refs, list)) or (not repository_refs) or any((not isinstance(r, str) or not IDENTITY.fullmatch(r) for r in repository_refs)):
            raise InferenceError('invalid_connection_metadata')
        if not isinstance(secret, str) or not secret or len(secret) > 8192 or any((c.isspace() for c in secret)):
            raise InferenceError('needs_connection')
        backend = self.store()
        self.directory.mkdir(parents=True, exist_ok=True, mode=448)
        root_fd = os.open(str(self.directory), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            root = os.fstat(root_fd)
            if root.st_uid != os.getuid() or stat.S_IMODE(root.st_mode) & 63:
                raise InferenceError('unsafe_connection_store')
            name = reference.split(':', 1)[1] + '.json'
            try:
                fd = os.open(name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 384, dir_fd=root_fd)
            except FileExistsError:
                raise InferenceError('connection_exists')
            try:
                metadata = dict(schema_version=1, provider='openai', endpoint='https://api.openai.com/v1', models=models, repository_refs=repository_refs, enabled=False)
                os.write(fd, canonical(metadata).encode())
                os.fsync(fd)
                os.fsync(root_fd)
                if backend.get_password('stage34', reference):
                    raise InferenceError('connection_exists')
                backend.set_password('stage34', reference, secret)
                metadata['enabled'] = True
                data = canonical(metadata).encode()
                os.lseek(fd, 0, os.SEEK_SET)
                os.ftruncate(fd, 0)
                os.write(fd, data)
                os.fsync(fd)
            except InferenceError:
                raise
            except Exception:
                raise InferenceError('secure_store_unavailable')
            finally:
                os.close(fd)
        finally:
            os.close(root_fd)
            secret = None

class OpenAIUpstream:
    """No redirects, ambient proxy/auth inheritance or endpoint selection from model input."""

    def __init__(self, connections, repository_ref, allow_paid=False):
        self.connections, self.repository_ref, self.allow_paid = (connections, repository_ref, allow_paid)

    def __call__(self, policy, body):
        if self.allow_paid is not True:
            raise InferenceError('paid_execution_disabled')
        policy.price.assert_fresh()
        secret = self.connections.resolve(policy, self.repository_ref)
        connection = http.client.HTTPSConnection('api.openai.com', timeout=30, context=ssl.create_default_context())
        try:
            connection.request('POST', '/v1/responses', body=canonical(body).encode('utf-8'), headers={'Authorization': 'Bearer ' + secret, 'Content-Type': 'application/json', 'Accept': 'text/event-stream'})
            response = connection.getresponse()
            raw = response.read(MAX_BODY + 1)
            if len(raw) > MAX_BODY:
                raise InferenceError('model_response_too_large')
            if response.status in (401, 403):
                raise InferenceError('authentication_failed')
            if response.status == 429:
                raise InferenceError('rate_limited')
            if response.status != 200:
                raise InferenceError('provider_error')
            return raw
        except InferenceError:
            raise
        except (OSError, http.client.HTTPException):
            raise InferenceError('inference_effect_unknown')
        finally:
            connection.close()
            secret = None
