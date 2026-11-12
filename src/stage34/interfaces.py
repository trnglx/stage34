"""Version 1 extension contracts; extensions cannot bypass controller acceptance/fencing."""

from typing import Any, Dict, Mapping, Protocol, Sequence

INTERFACE_VERSION = 1


class ProjectProfile(Protocol):
    schema_version: int
    digest: str

    def task_error(self, fields: Mapping[str, Any]) -> Any: ...
    def remote_violations(self, owns: Sequence[str]) -> Sequence[str]: ...


class PolicyProvider(Protocol):
    def load(self, reference: str) -> Mapping[str, Any]: ...


class CodingRuntime(Protocol):
    def capabilities(self) -> Mapping[str, bool]: ...
    def start(self, manifest: Mapping[str, Any]) -> str: ...
    def cancel(self, attempt: str, generation: int) -> None: ...


class Transport(Protocol):
    def request(
        self, node: str, operation: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...


class Verifier(Protocol):
    def verify(
        self, candidate: str, policy: str, target_sha: str
    ) -> Mapping[str, Any]: ...


class ArtifactPublisher(Protocol):
    def publish(self, artifact_digest: str, approval: str) -> Mapping[str, Any]: ...


class ApprovalProvider(Protocol):
    def authorize(self, action: str, target: str, bound_sha: str) -> str: ...


class ReviewStrategy(Protocol):
    def reviewers(self, candidate: str, policy: str) -> Sequence[str]: ...
    def assess(self, receipts: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: ...
