if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

from dataclasses import asdict, dataclass
import hashlib
import json
import re
from typing import Optional, Tuple

@dataclass(frozen=True)
class ProjectProfile:
    name: str
    revision: str
    schema_version: int = 1
    lanes: Tuple[str, ...] = ('local',)
    local_lane: str = 'local'
    local_role_prefixes: Tuple[str, ...] = ('',)
    kinds: Tuple[str, ...] = ('task',)
    remote_lanes: Tuple[str, ...] = ()
    remote_forbidden_pattern: str = '(?!)'
    repair_kind: Optional[str] = None
    repair_lane: Optional[str] = None
    repair_test_pattern: str = '(?!)'
    repair_test_description: str = 'project test node id'
    verifier_roles: Tuple[str, ...] = ('qa',)
    reviewer_pattern: str = '[a-z][a-z0-9-]*'
    verifier_source_root: str = 'src/stage34/core'
    verifier_workflow: str = 'candidate-verify.yml'
    verifier_workflow_name: str = 'Stage34 candidate verification'
    verifier_trusted_ref: str = 'main'
    test_path_pattern: str = '(^|/)(tests?/|test_[^/]*\\.py$|[^/]*_test\\.py$|conftest\\.py$|[^/]*\\.(test|spec)\\.tsx?$)'

    def __post_init__(self):
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError('unsupported project profile schema')
        for name in (self.name, self.revision):
            if not isinstance(name, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}', name):
                raise ValueError('invalid profile name or revision')
        for field in (self.lanes, self.kinds, self.remote_lanes, self.local_role_prefixes, self.verifier_roles):
            if not isinstance(field, tuple) or any((not isinstance(v, str) for v in field)):
                raise ValueError('profile collections must be immutable tuples of strings')
            if len(field) != len(set(field)):
                raise ValueError('duplicate profile entry')
        if not self.lanes or not self.kinds or self.local_lane not in self.lanes:
            raise ValueError('profile requires lanes, kinds and a local lane')
        if not self.local_role_prefixes or any((lane not in self.lanes for lane in self.remote_lanes)):
            raise ValueError('invalid role prefixes or remote lanes')
        if self.local_lane in self.remote_lanes:
            raise ValueError('local lane cannot be remote')
        if self.repair_kind is not None:
            if self.repair_kind not in self.kinds or self.repair_lane not in self.lanes:
                raise ValueError('repair policy requires a declared kind and lane')
        elif self.repair_lane is not None:
            raise ValueError('repair lane requires a repair kind')
        if not self.verifier_roles or any((not re.fullmatch('[a-z][a-z0-9-]*', role) for role in self.verifier_roles)):
            raise ValueError('invalid verifier roles')
        if not isinstance(self.verifier_source_root, str) or not re.fullmatch('[a-zA-Z0-9_./-]{1,200}', self.verifier_source_root) or self.verifier_source_root.startswith('/') or any((part in ('', '..', '.') for part in self.verifier_source_root.split('/'))):
            raise ValueError('invalid verifier source root')
        if not isinstance(self.verifier_workflow, str) or not re.fullmatch('[a-zA-Z0-9_-]+\\.ya?ml', self.verifier_workflow) or (not isinstance(self.verifier_trusted_ref, str)) or (not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}', self.verifier_trusted_ref)):
            raise ValueError('invalid verifier workflow or trusted ref')
        if not isinstance(self.verifier_workflow_name, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9 ()_.-]{0,99}', self.verifier_workflow_name):
            raise ValueError('invalid verifier workflow name')
        for pattern in (self.remote_forbidden_pattern, self.repair_test_pattern, self.test_path_pattern, self.reviewer_pattern):
            try:
                re.compile(pattern)
            except (re.error, TypeError) as error:
                raise ValueError('invalid project policy pattern') from error

    @property
    def digest(self):
        """Stable identity of every policy value, not only its display revision."""
        canonical = json.dumps(asdict(self), sort_keys=True, separators=(',', ':'), ensure_ascii=True)
        return hashlib.sha256(canonical.encode('ascii')).hexdigest()

    def task_error(self, fields):
        """Project constraints only; authority validates task shape and lifecycle."""
        if fields['lane'] not in self.lanes:
            return 'lane must be one of %s' % ', '.join(self.lanes)
        if fields['lane'] == self.local_lane and (not fields['role'].startswith(self.local_role_prefixes)):
            return 'local packets are for %s roles' % ', '.join((p + '*' for p in self.local_role_prefixes))
        if fields['kind'] not in self.kinds:
            return 'kind must be one of %s' % ', '.join(self.kinds)
        if self.repair_kind is not None and fields['kind'] == self.repair_kind:
            if fields['lane'] != self.repair_lane:
                return '%s tasks run on the %s lane' % (self.repair_kind, self.repair_lane)
            node = fields['repair_test'] or ''
            if not re.fullmatch(self.repair_test_pattern, node) or '..' in node:
                return 'repair_test must be a %s' % self.repair_test_description
            if any((re.search(self.test_path_pattern, g.replace('**', 'x').replace('*', 'x')) for g in fields['owns'])):
                return 'a %s task may not own test files' % self.repair_kind
        elif fields['repair_test']:
            return 'repair_test needs kind %s' % (self.repair_kind or 'with a repair policy')
        return None

    def remote_violations(self, owns):
        return [g for g in owns if re.match(self.remote_forbidden_pattern, g.replace('**', 'x').replace('*', 'x'))]
