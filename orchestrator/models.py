from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class FailurePolicy(Enum):
    FAIL_FAST = "fail_fast"
    CONTINUE = "continue"


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ResourceDriver(Enum):
    DOCKER_CONTAINER = "docker_container"
    FILE_SHARE = "file_share"


@dataclass(frozen=True)
class ResourceWeight:
    """Relative resource cost of a job expressed in abstract slot units.

    Slot counts are not tied to physical resources — they are relative weights
    declared per-job so the scheduler can prevent resource overcommit without
    needing to introspect process memory at runtime.
    """

    cpu_slots: int = 1
    memory_slots: int = 1


@dataclass(frozen=True)
class VolumeMount:
    """A bind mount from the host into the container.

    host_path:      Absolute path on the host machine.
    container_path: Absolute path inside the container.
    read_only:      If True the mount is read-only inside the container.
    """

    host_path: str
    container_path: str
    read_only: bool = False


@dataclass(frozen=True)
class ArtifactSpec:
    """Describes a set of files to collect as artifacts after a build."""

    source_glob: str
    destination_subdir: str


@dataclass
class ResourceSpec:
    """Pipeline-wide shared resource configuration.

    Two drivers are supported:

    ``docker_container`` — a managed container started before jobs run and torn
    down afterwards.  Requires ``image``; supports ``aliases``, ``command``,
    ``volumes``, ``env_vars``, and ``artifacts``.

    ``file_share`` — a host directory bind-mounted read-only into every job that
    declares it in ``resources``.  Requires ``host_path`` and
    ``container_path``; all other Docker-specific fields must be absent.
    """

    id: str
    kind: str = "generic"
    driver: ResourceDriver = ResourceDriver.DOCKER_CONTAINER
    image: str | None = None
    host_path: str | None = None
    container_path: str | None = None
    aliases: list[str] = field(default_factory=list)
    command: list[str] | None = None
    artifacts: list[ArtifactSpec] = field(default_factory=list)
    volumes: list[VolumeMount] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)


@dataclass
class JobSpec:
    """Complete specification for a single build job."""

    id: str
    image: str
    depends_on: frozenset[str]
    resource_weight: ResourceWeight
    artifacts: list[ArtifactSpec]
    command: list[str] | None = None
    timeout_seconds: int | None = None
    volumes: list[VolumeMount] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)
    resources: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class JobResult:
    """Outcome of executing a single build job."""

    job_id: str
    success: bool
    exit_code: int
    duration_seconds: float
    log_path: Path


@dataclass
class BuildPlan:
    """Complete orchestration plan derived from a configuration file."""

    jobs: list[JobSpec]
    failure_policy: FailurePolicy
    max_parallel: int
    total_cpu_slots: int
    total_memory_slots: int
    job_timeout_seconds: int | None = 3600
    resource_network: str | None = None
    resources: list[ResourceSpec] = field(default_factory=list)


@dataclass(frozen=True)
class OrchestratorResult:
    """Final result returned to the Azure DevOps pipeline."""

    success: bool
    job_results: tuple[JobResult, ...]
