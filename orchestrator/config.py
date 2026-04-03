from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, override

import yaml

from orchestrator.exceptions import ConfigurationError
from orchestrator.models import (
    ArtifactSpec,
    BuildPlan,
    FailurePolicy,
    JobSpec,
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
    cpu_slots: int = 1
    memory_slots: int = 1
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

        seen_ids: set[str] = set()
        for job in raw.jobs:
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
                volumes=[
                    VolumeMount(
                        host_path=v.host_path,
                        container_path=v.container_path,
                        read_only=v.read_only,
                    )
                    for v in rj.volumes
                ],
                env_vars=rj.env_vars,
            )
            for rj in raw.jobs
        ]
        return BuildPlan(
            jobs=jobs,
            failure_policy=FailurePolicy(raw.failure_policy),
            max_parallel=raw.max_parallel,
            total_cpu_slots=raw.total_cpu_slots,
            total_memory_slots=raw.total_memory_slots,
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

    artifacts: list[RawArtifactConfig] = []
    for j, art in enumerate(entry.get("artifacts", [])):
        if not isinstance(art, dict):
            raise ConfigurationError(f"{prefix}.artifacts[{j}]: expected a mapping")
        if "source_glob" not in art:
            raise ConfigurationError(f"{prefix}.artifacts[{j}]: missing 'source_glob'")
        artifacts.append(
            RawArtifactConfig(
                source_glob=str(art["source_glob"]),
                destination_subdir=str(art.get("destination_subdir", "")),
            )
        )

    volumes: list[RawVolumeConfig] = []
    for j, vol in enumerate(entry.get("volumes", [])):
        if not isinstance(vol, dict):
            raise ConfigurationError(f"{prefix}.volumes[{j}]: expected a mapping")
        for req_key in ("host_path", "container_path"):
            if req_key not in vol:
                raise ConfigurationError(
                    f"{prefix}.volumes[{j}]: missing '{req_key}'"
                )
        read_only = vol.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ConfigurationError(
                f"{prefix}.volumes[{j}].read_only: expected a boolean"
            )
        volumes.append(
            RawVolumeConfig(
                host_path=str(vol["host_path"]),
                container_path=str(vol["container_path"]),
                read_only=read_only,
            )
        )

    env_vars = entry.get("env_vars", {})
    if not isinstance(env_vars, dict):
        raise ConfigurationError(f"{prefix}.env_vars: expected a mapping")
    env_vars = {str(k): str(v) for k, v in env_vars.items()}

    return RawJobConfig(
        id=entry["id"],
        image=entry["image"],
        depends_on=depends_on,
        command=command,
        cpu_slots=_expect_int(entry, "cpu_slots", default=1, prefix=prefix),
        memory_slots=_expect_int(entry, "memory_slots", default=1, prefix=prefix),
        artifacts=artifacts,
        volumes=volumes,
        env_vars=env_vars,
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
