from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, override

import yaml

from orchestrator.exceptions import ConfigurationError
from orchestrator.path_safety import require_safe_path_component
from orchestrator.models import (
    ArtifactSpec,
    BuildPlan,
    FailurePolicy,
    JobSpec,
    ResourceDriver,
    ResourceSpec,
    ResourceWeight,
    VolumeMount,
)


@dataclass
class RawArtifactConfig:
    """Schema for a single artifact entry in the YAML configuration."""

    source_glob: str
    destination_subdir: str = ""


@dataclass
class RawVolumeConfig:
    """Schema for a single volume mount entry in the YAML configuration."""

    host_path: str
    container_path: str
    read_only: bool = False


@dataclass
class RawJobConfig:
    """Schema for a single job entry in the YAML configuration."""

    id: str
    image: str
    depends_on: list[str] = field(default_factory=list)
    command: list[str] | None = None
    timeout_seconds: int | None = None
    cpu_slots: int = 1
    memory_slots: int = 1
    artifacts: list[RawArtifactConfig] = field(default_factory=list)
    volumes: list[RawVolumeConfig] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)
    resources: list[str] = field(default_factory=list)


@dataclass
class RawResourceConfig:
    """Schema for a pipeline-wide shared resource."""

    id: str
    kind: str = "generic"
    driver: str = ResourceDriver.DOCKER_CONTAINER.value
    image: str | None = None
    host_path: str | None = None
    container_path: str | None = None
    aliases: list[str] = field(default_factory=list)
    command: list[str] | None = None
    artifacts: list[RawArtifactConfig] = field(default_factory=list)
    volumes: list[RawVolumeConfig] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)


@dataclass
class RawPipelineConfig:
    """Schema for the top-level YAML configuration file."""

    jobs: list[RawJobConfig]
    failure_policy: str = "fail_fast"
    max_parallel: int = 4
    total_cpu_slots: int = 8
    total_memory_slots: int = 8
    job_timeout_seconds: int | None = 3600
    resource_network: str | None = None
    resources: list[RawResourceConfig] = field(default_factory=list)


class IConfigLoader(ABC):
    """Loads and validates a build configuration file into a BuildPlan."""

    @abstractmethod
    def load(self, path: Path) -> BuildPlan:
        """Parse the configuration file at path and return a validated BuildPlan.

        Raises:
            ConfigurationError: On schema violations or invalid dependency references.
        """
        ...


_VALID_FAILURE_POLICIES = {p.value for p in FailurePolicy}


class YamlConfigLoader(IConfigLoader):
    """Parses a YAML configuration file into a validated BuildPlan.

    Expected YAML structure::

        failure_policy: fail_fast    # or "continue"
        max_parallel: 4
        total_cpu_slots: 8
        total_memory_slots: 8
        jobs:
          - id: compile
            image: registry/builder:latest
            command: ["--project", "services/app"]
            depends_on: []
            cpu_slots: 2
            memory_slots: 2
            artifacts:
              - source_glob: "**/*.dll"
                destination_subdir: binaries
            env_vars:
              BUILD_CONFIG: Release
    """

    @override
    def load(self, path: Path) -> BuildPlan:
        data = self._read_yaml(path)
        raw = self._parse_pipeline(data)
        self._validate(raw)
        return self._to_build_plan(raw)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _read_yaml(path: Path) -> dict[str, Any]:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigurationError(f"Cannot read config file '{path}': {exc}") from exc

        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"Invalid YAML in '{path}': {exc}") from exc

        if not isinstance(data, dict):
            raise ConfigurationError(
                f"Config file '{path}' must be a YAML mapping, got {type(data).__name__}"
            )
        return data

    @staticmethod
    def _parse_pipeline(data: dict[str, Any]) -> RawPipelineConfig:
        if "jobs" not in data:
            raise ConfigurationError("Config is missing required key 'jobs'")

        raw_jobs = data["jobs"]
        if not isinstance(raw_jobs, list):
            raise ConfigurationError("'jobs' must be a list")

        jobs: list[RawJobConfig] = []
        for i, entry in enumerate(raw_jobs):
            if not isinstance(entry, dict):
                raise ConfigurationError(f"jobs[{i}]: expected a mapping, got {type(entry).__name__}")
            jobs.append(_parse_job(entry, index=i))

        return RawPipelineConfig(
            jobs=jobs,
            failure_policy=str(data.get("failure_policy", "fail_fast")),
            max_parallel=_expect_int(data, "max_parallel", default=4),
            total_cpu_slots=_expect_int(data, "total_cpu_slots", default=8),
            total_memory_slots=_expect_int(data, "total_memory_slots", default=8),
            job_timeout_seconds=_expect_optional_int(
                data, "job_timeout_seconds", default=3600
            ),
            resource_network=_parse_optional_string(
                data,
                "resource_network",
                legacy_key="network",
            ),
            resources=_parse_resources(
                data.get("resources", data.get("services", [])),
            ),
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate(raw: RawPipelineConfig) -> None:
        if raw.failure_policy not in _VALID_FAILURE_POLICIES:
            raise ConfigurationError(
                f"Invalid failure_policy '{raw.failure_policy}', "
                f"must be one of {sorted(_VALID_FAILURE_POLICIES)}"
            )

        if raw.max_parallel < 1:
            raise ConfigurationError("max_parallel must be >= 1")
        if raw.total_cpu_slots < 1:
            raise ConfigurationError("total_cpu_slots must be >= 1")
        if raw.total_memory_slots < 1:
            raise ConfigurationError("total_memory_slots must be >= 1")
        if raw.job_timeout_seconds is not None and raw.job_timeout_seconds < 1:
            raise ConfigurationError("job_timeout_seconds must be >= 1")
        if raw.resource_network is not None and not raw.resource_network:
            raise ConfigurationError("resource_network must not be empty")

        seen_ids: set[str] = set()
        for job in raw.jobs:
            require_safe_path_component(job.id, owner_label="Job", field_name="id")
            if not job.image:
                raise ConfigurationError(f"Job '{job.id}': image must not be empty")
            if job.id in seen_ids:
                raise ConfigurationError(f"Duplicate job id '{job.id}'")
            seen_ids.add(job.id)

        all_ids = seen_ids
        for job in raw.jobs:
            unknown = set(job.depends_on) - all_ids
            if unknown:
                raise ConfigurationError(
                    f"Job '{job.id}' depends on unknown job(s): {sorted(unknown)}"
                )

        for job in raw.jobs:
            if job.cpu_slots < 1:
                raise ConfigurationError(f"Job '{job.id}': cpu_slots must be >= 1")
            if job.memory_slots < 1:
                raise ConfigurationError(f"Job '{job.id}': memory_slots must be >= 1")
            if job.timeout_seconds is not None and job.timeout_seconds < 1:
                raise ConfigurationError(f"Job '{job.id}': timeout_seconds must be >= 1")
            for index, artifact in enumerate(job.artifacts):
                _validate_artifact_destination_subdir(
                    artifact.destination_subdir,
                    owner_label=f"Job '{job.id}'",
                    artifact_index=index,
                )

        seen_resource_ids: set[str] = set()
        for resource in raw.resources:
            require_safe_path_component(
                resource.id, owner_label="Resource", field_name="id"
            )
            if resource.id in seen_resource_ids:
                raise ConfigurationError(f"Duplicate resource id '{resource.id}'")
            seen_resource_ids.add(resource.id)
            if resource.driver not in {d.value for d in ResourceDriver}:
                raise ConfigurationError(
                    f"Resource '{resource.id}': invalid driver '{resource.driver}'"
                )
            if not resource.kind:
                raise ConfigurationError(f"Resource '{resource.id}': kind must not be empty")
            for alias in resource.aliases:
                if not alias:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': aliases must not be empty"
                    )
            if resource.driver == ResourceDriver.DOCKER_CONTAINER.value:
                if not resource.image:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': image must not be empty for managed docker_container resources"
                    )
                if resource.host_path is not None:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': host_path is not supported for docker_container resources"
                    )
                if resource.container_path is not None:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': container_path is not supported for docker_container resources"
                    )
            if resource.driver == ResourceDriver.FILE_SHARE.value:
                if not resource.host_path:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': host_path must not be empty for file_share resources"
                    )
                if not resource.container_path:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': container_path must not be empty for file_share resources"
                    )
                if not resource.container_path.startswith("/"):
                    raise ConfigurationError(
                        f"Resource '{resource.id}': container_path must be an absolute path"
                    )
                if resource.image:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': image is not supported for file_share resources"
                    )
                if resource.command is not None:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': command is not supported for file_share resources"
                    )
                if resource.aliases:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': aliases are not supported for file_share resources"
                    )
                if resource.artifacts:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': artifacts are not supported for file_share resources"
                    )
                if resource.env_vars:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': env_vars are not supported for file_share resources"
                    )
                if resource.volumes:
                    raise ConfigurationError(
                        f"Resource '{resource.id}': volumes are not supported for file_share resources"
                    )
            for index, artifact in enumerate(resource.artifacts):
                _validate_artifact_destination_subdir(
                    artifact.destination_subdir,
                    owner_label=f"Resource '{resource.id}'",
                    artifact_index=index,
                )

        file_share_ids = {
            r.id
            for r in raw.resources
            if r.driver == ResourceDriver.FILE_SHARE.value
        }
        all_resource_ids = {r.id for r in raw.resources}
        for job in raw.jobs:
            seen_job_resources: set[str] = set()
            for resource_id in job.resources:
                if resource_id not in all_resource_ids:
                    raise ConfigurationError(
                        f"Job '{job.id}': resources references unknown resource '{resource_id}'"
                    )
                if resource_id not in file_share_ids:
                    raise ConfigurationError(
                        f"Job '{job.id}': resources can only reference file_share resources, "
                        f"'{resource_id}' is not a file_share"
                    )
                if resource_id in seen_job_resources:
                    raise ConfigurationError(
                        f"Job '{job.id}': duplicate resource reference '{resource_id}'"
                    )
                seen_job_resources.add(resource_id)

    # ------------------------------------------------------------------
    # Conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _to_build_plan(raw: RawPipelineConfig) -> BuildPlan:
        jobs = [
            JobSpec(
                id=rj.id,
                image=rj.image,
                depends_on=frozenset(rj.depends_on),
                resource_weight=ResourceWeight(
                    cpu_slots=rj.cpu_slots,
                    memory_slots=rj.memory_slots,
                ),
                artifacts=[
                    ArtifactSpec(
                        source_glob=a.source_glob,
                        destination_subdir=a.destination_subdir,
                    )
                    for a in rj.artifacts
                ],
                command=rj.command,
                timeout_seconds=rj.timeout_seconds,
                volumes=[
                    VolumeMount(
                        host_path=v.host_path,
                        container_path=v.container_path,
                        read_only=v.read_only,
                    )
                    for v in rj.volumes
                ],
                env_vars=rj.env_vars,
                resources=rj.resources,
            )
            for rj in raw.jobs
        ]
        return BuildPlan(
            jobs=jobs,
            failure_policy=FailurePolicy(raw.failure_policy),
            max_parallel=raw.max_parallel,
            total_cpu_slots=raw.total_cpu_slots,
            total_memory_slots=raw.total_memory_slots,
            job_timeout_seconds=raw.job_timeout_seconds,
            resource_network=raw.resource_network,
            resources=[
                ResourceSpec(
                    id=resource.id,
                    kind=resource.kind,
                    driver=ResourceDriver(resource.driver),
                    image=resource.image,
                    host_path=resource.host_path,
                    container_path=resource.container_path,
                    aliases=resource.aliases if resource.aliases else [resource.id],
                    command=resource.command,
                    artifacts=[
                        ArtifactSpec(
                            source_glob=artifact.source_glob,
                            destination_subdir=artifact.destination_subdir,
                        )
                        for artifact in resource.artifacts
                    ],
                    volumes=[
                        VolumeMount(
                            host_path=v.host_path,
                            container_path=v.container_path,
                            read_only=v.read_only,
                        )
                        for v in resource.volumes
                    ],
                    env_vars=resource.env_vars,
                )
                for resource in raw.resources
            ],
        )


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _parse_job(entry: dict[str, Any], index: int) -> RawJobConfig:
    prefix = f"jobs[{index}]"

    for key in ("id", "image"):
        if key not in entry:
            raise ConfigurationError(f"{prefix}: missing required key '{key}'")
        if not isinstance(entry[key], str):
            raise ConfigurationError(f"{prefix}.{key}: expected a string")

    depends_on = entry.get("depends_on", [])
    if not isinstance(depends_on, list) or not all(isinstance(d, str) for d in depends_on):
        raise ConfigurationError(f"{prefix}.depends_on: expected a list of strings")

    command = entry.get("command")
    if command is not None:
        if not isinstance(command, list) or not all(isinstance(c, str) for c in command):
            raise ConfigurationError(f"{prefix}.command: expected a list of strings or null")

    timeout_seconds = _expect_optional_int(
        entry, "timeout_seconds", default=None, prefix=prefix
    )

    volumes = _parse_volumes(entry.get("volumes", []), prefix=f"{prefix}.volumes")

    env_vars = entry.get("env_vars", {})
    if not isinstance(env_vars, dict):
        raise ConfigurationError(f"{prefix}.env_vars: expected a mapping")
    env_vars = {str(k): str(v) for k, v in env_vars.items()}

    resources = entry.get("resources", [])
    if not isinstance(resources, list) or not all(isinstance(r, str) for r in resources):
        raise ConfigurationError(f"{prefix}.resources: expected a list of strings")

    return RawJobConfig(
        id=entry["id"],
        image=entry["image"],
        depends_on=depends_on,
        command=command,
        timeout_seconds=timeout_seconds,
        cpu_slots=_expect_int(entry, "cpu_slots", default=1, prefix=prefix),
        memory_slots=_expect_int(entry, "memory_slots", default=1, prefix=prefix),
        artifacts=_parse_artifacts(entry.get("artifacts", []), prefix=f"{prefix}.artifacts"),
        volumes=volumes,
        env_vars=env_vars,
        resources=resources,
    )


def _expect_int(
    data: dict[str, Any],
    key: str,
    *,
    default: int,
    prefix: str = "",
) -> int:
    value = data.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        loc = f"{prefix}.{key}" if prefix else key
        raise ConfigurationError(f"{loc}: expected an integer, got {type(value).__name__}")
    return value


def _expect_optional_int(
    data: dict[str, Any],
    key: str,
    *,
    default: int | None,
    prefix: str = "",
) -> int | None:
    value = data.get(key, default)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        loc = f"{prefix}.{key}" if prefix else key
        raise ConfigurationError(f"{loc}: expected an integer or null, got {type(value).__name__}")
    return value


def _parse_optional_string(
    data: dict[str, Any],
    key: str,
    *,
    legacy_key: str | None = None,
) -> str | None:
    value = data.get(key)
    if value is None and legacy_key is not None:
        value = data.get(legacy_key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"{key}: expected a string")
    return value


def _parse_volumes(raw_volumes: Any, prefix: str) -> list[RawVolumeConfig]:
    if not isinstance(raw_volumes, list):
        raise ConfigurationError(f"{prefix}: expected a list")

    volumes: list[RawVolumeConfig] = []
    for j, vol in enumerate(raw_volumes):
        vol_prefix = f"{prefix}[{j}]"
        if not isinstance(vol, dict):
            raise ConfigurationError(f"{vol_prefix}: expected a mapping")
        for req_key in ("host_path", "container_path"):
            if req_key not in vol:
                raise ConfigurationError(f"{vol_prefix}: missing '{req_key}'")
        read_only = vol.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ConfigurationError(f"{vol_prefix}.read_only: expected a boolean")
        volumes.append(
            RawVolumeConfig(
                host_path=str(vol["host_path"]),
                container_path=str(vol["container_path"]),
                read_only=read_only,
            )
        )
    return volumes


def _parse_resources(raw_resources: Any) -> list[RawResourceConfig]:
    if not isinstance(raw_resources, list):
        raise ConfigurationError("resources: expected a list")

    resources: list[RawResourceConfig] = []
    for i, raw_resource in enumerate(raw_resources):
        prefix = f"resources[{i}]"
        if not isinstance(raw_resource, dict):
            raise ConfigurationError(f"{prefix}: expected a mapping")
        if "id" not in raw_resource:
            raise ConfigurationError(f"{prefix}: missing required key 'id'")
        resource_id = raw_resource["id"]
        if not isinstance(resource_id, str):
            raise ConfigurationError(f"{prefix}.id: expected a string")
        kind = raw_resource.get("kind", "generic")
        if not isinstance(kind, str):
            raise ConfigurationError(f"{prefix}.kind: expected a string")
        driver = raw_resource.get("driver", ResourceDriver.DOCKER_CONTAINER.value)
        if not isinstance(driver, str):
            raise ConfigurationError(f"{prefix}.driver: expected a string")
        image = raw_resource.get("image")
        if image is not None and not isinstance(image, str):
            raise ConfigurationError(f"{prefix}.image: expected a string or null")
        host_path = raw_resource.get("host_path")
        if host_path is not None and not isinstance(host_path, str):
            raise ConfigurationError(f"{prefix}.host_path: expected a string or null")
        container_path = raw_resource.get("container_path")
        if container_path is not None and not isinstance(container_path, str):
            raise ConfigurationError(f"{prefix}.container_path: expected a string or null")
        aliases = raw_resource.get("aliases", [])
        if not isinstance(aliases, list) or not all(isinstance(a, str) for a in aliases):
            raise ConfigurationError(f"{prefix}.aliases: expected a list of strings")

        command = raw_resource.get("command")
        if command is not None:
            if not isinstance(command, list) or not all(isinstance(c, str) for c in command):
                raise ConfigurationError(f"{prefix}.command: expected a list of strings or null")

        env_vars = raw_resource.get("env_vars", {})
        if not isinstance(env_vars, dict):
            raise ConfigurationError(f"{prefix}.env_vars: expected a mapping")

        resources.append(
            RawResourceConfig(
                id=resource_id,
                kind=kind,
                driver=driver,
                image=image,
                host_path=host_path,
                container_path=container_path,
                aliases=aliases,
                command=command,
                artifacts=_parse_artifacts(
                    raw_resource.get("artifacts", []),
                    prefix=f"{prefix}.artifacts",
                ),
                volumes=_parse_volumes(
                    raw_resource.get("volumes", []),
                    prefix=f"{prefix}.volumes",
                ),
                env_vars={str(k): str(v) for k, v in env_vars.items()},
            )
        )
    return resources


def _validate_artifact_destination_subdir(
    destination_subdir: str,
    *,
    owner_label: str,
    artifact_index: int,
) -> None:
    if not destination_subdir:
        return

    path = Path(destination_subdir)
    if (
        destination_subdir.startswith(("/", "\\"))
        or path.is_absolute()
        or path.anchor
        or any(part == ".." for part in path.parts)
    ):
        raise ConfigurationError(
            f"{owner_label}: artifacts[{artifact_index}].destination_subdir "
            f"must be a relative path without '..' segments"
        )


def _parse_artifacts(raw_artifacts: Any, prefix: str) -> list[RawArtifactConfig]:
    if not isinstance(raw_artifacts, list):
        raise ConfigurationError(f"{prefix}: expected a list")

    artifacts: list[RawArtifactConfig] = []
    for index, art in enumerate(raw_artifacts):
        if not isinstance(art, dict):
            raise ConfigurationError(f"{prefix}[{index}]: expected a mapping")
        if "source_glob" not in art:
            raise ConfigurationError(f"{prefix}[{index}]: missing 'source_glob'")
        artifacts.append(
            RawArtifactConfig(
                source_glob=str(art["source_glob"]),
                destination_subdir=str(art.get("destination_subdir", "")),
            )
        )
    return artifacts
