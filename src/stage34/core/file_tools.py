if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import fnmatch
import json
import os
from pathlib import PurePosixPath
import secrets
import stat
import sys
LIMIT = 128 * 1024
FILE_LIMIT = 2 * 1024 * 1024
NAMES = ('Read', 'Grep', 'Glob', 'Edit', 'Write')

class ToolError(Exception):
    pass

class Stage34Tools:

    def __init__(self, workspace, allowed):
        self.workspace = os.path.abspath(workspace)
        if not os.path.isabs(workspace) or not isinstance(allowed, list) or any((x not in NAMES + ('Bash',) for x in allowed)):
            raise ToolError('invalid_policy')
        self.root = os.open(self.workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.allowed = frozenset(allowed) & frozenset(NAMES)

    def close(self):
        os.close(self.root)

    def parts(self, path, write=False):
        if not isinstance(path, str) or '\x00' in path or '\\' in path:
            raise ToolError('permission_denied')
        if path.startswith('/'):
            if path == self.workspace:
                path = '.'
            elif path.startswith(self.workspace.rstrip('/') + '/'):
                path = path[len(self.workspace.rstrip('/')) + 1:]
            else:
                raise ToolError('permission_denied')
        parts = PurePosixPath(path).parts
        if '..' in parts or len(parts) > 32:
            raise ToolError('permission_denied')
        if write and any((p in ('.git', '.codex', '.stage34') for p in parts)):
            raise ToolError('permission_denied')
        return [p for p in parts if p not in ('', '.')]

    def parent(self, path, write=False):
        parts = self.parts(path, write)
        if not parts:
            raise ToolError('invalid_file')
        fd = os.dup(self.root)
        try:
            for name in parts[:-1]:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            return (fd, parts[-1])
        except BaseException:
            os.close(fd)
            raise

    def read(self, path):
        parent, name = self.parent(path)
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > FILE_LIMIT:
                    raise ToolError('unsupported_file')
                raw = bytearray()
                while len(raw) <= FILE_LIMIT:
                    part = os.read(fd, min(65536, FILE_LIMIT + 1 - len(raw)))
                    if not part:
                        break
                    raw.extend(part)
                if len(raw) > FILE_LIMIT:
                    raise ToolError('unsupported_file')
                return bytes(raw).decode('utf-8')
            finally:
                os.close(fd)
        finally:
            os.close(parent)

    def write(self, path, content):
        if not isinstance(content, str) or len(content.encode('utf-8')) > FILE_LIMIT:
            raise ToolError('unsupported_file')
        parent, name = self.parent(path, write=True)
        temporary = '.stage34-write-' + secrets.token_hex(8)
        try:
            try:
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ToolError('permission_denied')
                mode = stat.S_IMODE(info.st_mode) & 511
            except FileNotFoundError:
                mode = 384
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=parent)
            try:
                raw = content.encode('utf-8')
                while raw:
                    written = os.write(fd, raw)
                    if written <= 0:
                        raise ToolError('write_failed')
                    raw = raw[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass
            os.close(parent)

    def files(self, path):
        parts = self.parts(path)
        fd = os.dup(self.root)
        try:
            for name in parts:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            pending = [(fd, '/'.join(parts), 0)]
            fd = None
            seen = 0
            while pending:
                directory, prefix, depth = pending.pop()
                try:
                    for name in sorted(os.listdir(directory)):
                        seen += 1
                        if seen > 10000:
                            raise ToolError('scan_limit')
                        if name in ('.git', '.codex', '.stage34'):
                            continue
                        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        relative = prefix + '/' + name if prefix else name
                        if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                            yield relative
                        elif stat.S_ISDIR(info.st_mode) and depth < 16:
                            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                            pending.append((child, relative, depth + 1))
                finally:
                    os.close(directory)
        finally:
            if fd is not None:
                os.close(fd)
            for entry in locals().get('pending', []):
                os.close(entry[0])

    def call(self, name, arguments):
        if name not in self.allowed:
            raise ToolError('permission_denied')
        if not isinstance(arguments, dict):
            raise ToolError('invalid_arguments')
        required = {'Read': {'file_path'}, 'Write': {'file_path', 'content'}, 'Edit': {'file_path', 'old_string', 'new_string'}, 'Glob': {'pattern'}, 'Grep': {'pattern'}}[name]
        optional = {'Read': {'offset', 'limit'}, 'Edit': {'replace_all'}, 'Write': set(), 'Grep': {'path'}, 'Glob': {'path'}}[name]
        if not required <= set(arguments) or set(arguments) - required - optional:
            raise ToolError('invalid_arguments')
        if name == 'Read':
            text = self.read(arguments['file_path'])
            offset, limit = (arguments.get('offset', 1), arguments.get('limit', 2000))
            if type(offset) is not int or offset < 1 or type(limit) is not int or (not 1 <= limit <= 2000):
                raise ToolError('invalid_arguments')
            return ''.join(text.splitlines(keepends=True)[offset - 1:offset - 1 + limit])[:LIMIT]
        if name == 'Write':
            self.write(arguments['file_path'], arguments['content'])
            return 'written'
        if name == 'Edit':
            text = self.read(arguments['file_path'])
            old, new = (arguments['old_string'], arguments['new_string'])
            replace_all = arguments.get('replace_all', False)
            if not isinstance(old, str) or not old or (not isinstance(new, str)) or (type(replace_all) is not bool) or (text.count(old) == 0) or (not replace_all and text.count(old) != 1):
                raise ToolError('ambiguous_edit')
            self.write(arguments['file_path'], text.replace(old, new, -1 if replace_all else 1))
            return 'edited'
        pattern = arguments['pattern']
        if not isinstance(pattern, str) or not pattern or len(pattern) > 256:
            raise ToolError('invalid_arguments')
        result = []
        for path in self.files(arguments.get('path', '.')):
            if name == 'Glob':
                if fnmatch.fnmatchcase(path, pattern) or (pattern.startswith('**/') and fnmatch.fnmatchcase(path, pattern[3:])):
                    result.append(path)
            else:
                try:
                    text = self.read(path)
                except (OSError, UnicodeError, ToolError):
                    continue
                for line, content in enumerate(text.splitlines(), 1):
                    if pattern in content:
                        result.append('%s:%d:%s' % (path, line, content[:1000]))
            if len(result) >= 200 or sum(map(len, result)) >= LIMIT:
                break
        return '\n'.join(result)[:LIMIT]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace', required=True)
    parser.add_argument('--tools', required=True)
    args = parser.parse_args()
    tools = Stage34Tools(args.workspace, json.loads(args.tools))
    try:
        for raw in sys.stdin:
            if len(raw) > 512 * 1024:
                return 1
            try:
                request = json.loads(raw)
                rid = request.get('id')
                method = request.get('method')
                if rid is None:
                    continue
                if method == 'initialize':
                    result = dict(protocolVersion='2024-11-05', capabilities={'tools': {}}, serverInfo={'name': 'Stage34', 'version': '0.1.0'})
                elif method == 'tools/list':
                    schemas = {'Read': {'file_path': {'type': 'string'}, 'offset': {'type': 'integer'}, 'limit': {'type': 'integer'}}, 'Write': {'file_path': {'type': 'string'}, 'content': {'type': 'string'}}, 'Edit': {'file_path': {'type': 'string'}, 'old_string': {'type': 'string'}, 'new_string': {'type': 'string'}, 'replace_all': {'type': 'boolean'}}, 'Glob': {'pattern': {'type': 'string'}, 'path': {'type': 'string'}}, 'Grep': {'pattern': {'type': 'string', 'description': 'Literal text, not a regular expression'}, 'path': {'type': 'string'}}}
                    required = {'Read': ['file_path'], 'Write': ['file_path', 'content'], 'Edit': ['file_path', 'old_string', 'new_string'], 'Glob': ['pattern'], 'Grep': ['pattern']}
                    result = {'tools': [{'name': name, 'description': 'Workspace-scoped ' + name, 'inputSchema': {'type': 'object', 'properties': schemas[name], 'required': required[name], 'additionalProperties': False}} for name in sorted(tools.allowed)]}
                elif method == 'tools/call':
                    params = request.get('params', {})
                    try:
                        output = tools.call(params.get('name'), params.get('arguments', {}))
                        result = {'content': [{'type': 'text', 'text': output}]}
                    except (OSError, ValueError, UnicodeError, ToolError):
                        result = {'content': [{'type': 'text', 'text': 'Stage34 tool refused this operation'}], 'isError': True}
                else:
                    print(json.dumps({'jsonrpc': '2.0', 'id': rid, 'error': {'code': -32601, 'message': 'Unsupported method'}}), flush=True)
                    continue
                print(json.dumps({'jsonrpc': '2.0', 'id': rid, 'result': result}), flush=True)
            except (ValueError, TypeError, AttributeError):
                return 1
    finally:
        tools.close()
    return 0
if __name__ == '__main__':
    sys.exit(main())
