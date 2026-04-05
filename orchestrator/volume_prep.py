"""Volume mount helpers for the orchestrator.

These functions compute the system-managed volume mounts for jobs and
resources. They are pure computations (plus directory creation side-effects)
and do not mutate any BuildPlan or JobSpec objects.

Container path conventions:
    /src     — read-only source tree (jobs only)
    /output  — writable artifact output directory
"""
from __future__ import annotations

from pathlib import Path

from orchestrator.models import ContainerOS, JobSpec, ResourceDriver, ResourceSpec, VolumeMount
from orchestrator.path_safety import require_safe_path_component

LINUX_CONTAINER_SOURCE_PATH = "/src"
LINUX_CONTAINER_OUTPUT_PATH = "/output"
LINUX_CONTAINER_INPUT_PREFIX = "/input"
WINDOWS_CONTAINER_SOURCE_PATH = r"C:\src"
WINDOWS_CONTAINER_OUTPUT_PATH = r"C:\output"
WINDOWS_CONTAINER_INPUT_PREFIX = r"C:\input"
RESOURCE_OUTPUT_DIRNAME = "resources"

# Backwards-compatible aliases for existing Linux-oriented callers/tests.
CONTAINER_SOURCE_PATH = LINUX_CONTAINER_SOURCE_PATH
CONTAINER_OUTPUT_PATH = LINUX_CONTAINER_OUTPUT_PATH
CONTAINER_INPUT_PREFIX = LINUX_CONTAINER_INPUT_PREFIX


def _container_source_path(job: JobSpec) -> str:
    return WINDOWS_CONTAINER_SOURCE_PATH if job.container_os == ContainerOS.WINDOWS else LINUX_CONTAINER_SOURCE_PATH


def _container_output_path(job: JobSpec) -> str:
    return WINDOWS_CONTAINER_OUTPUT_PATH if job.container_os == ContainerOS.WINDOWS else LINUX_CONTAINER_OUTPUT_PATH


def _container_input_prefix(job: JobSpec) -> str:
    return WINDOWS_CONTAINER_INPUT_PREFIX if job.container_os == ContainerOS.WINDOWS else LINUX_CONTAINER_INPUT_PREFIX


def compute_job_volumes(
    job: JobSpec,
    source_dir: Path,
    container_output_root: Path,
    file_shares: dict[str, ResourceSpec],
) -> list[VolumeMount]:
    """Return the system-managed volume mounts for a job.

    Creates required host directories as a side-effect.
    Does not modify the job or the plan.

    Args:
        job: The job spec (user-declared volumes are ignored here).
        source_dir: Host path of the read-only source tree.
        container_output_root: Base path for per-job writable output dirs.
        file_shares: Mapping of resource id → ResourceSpec for FILE_SHARE resources.

    Returns:
        List of system-managed VolumeMounts: source, output, file shares, input_from.
    """
    safe_id = require_safe_path_component(job.id, owner_label="Job", field_name="id")
    job_output_dir = container_output_root / safe_id
    job_output_dir.mkdir(parents=True, exist_ok=True)

    vols: list[VolumeMount] = [
        VolumeMount(
            host_path=str(source_dir),
            container_path=_container_source_path(job),
            read_only=True,
        ),
        VolumeMount(
            host_path=str(job_output_dir),
            container_path=_container_output_path(job),
            read_only=False,
        ),
    ]

    for resource_id in job.resources:
        share = file_shares[resource_id]
        vols.append(
            VolumeMount(
                host_path=share.host_path,
                container_path=share.container_path,
                read_only=True,
            )
        )

    for source_job_id in job.input_from:
        safe_source_id = require_safe_path_component(
            source_job_id, owner_label="Job", field_name="input_from"
        )
        source_output_dir = container_output_root / safe_source_id
        source_output_dir.mkdir(parents=True, exist_ok=True)
        vols.append(
            VolumeMount(
                host_path=str(source_output_dir),
                container_path=(
                    f"{_container_input_prefix(job)}\\{safe_source_id}"
                    if job.container_os == ContainerOS.WINDOWS
                    else f"{_container_input_prefix(job)}/{safe_source_id}"
                ),
                read_only=True,
            )
        )

    return vols


def compute_resource_output_volume(
    resource: ResourceSpec,
    container_output_root: Path,
) -> VolumeMount:
    """Return the system-managed output VolumeMount for a managed resource.

    Creates the host output directory as a side-effect.
    """
    resource_id = require_safe_path_component(
        resource.id, owner_label="Resource", field_name="id"
    )
    output_dir = container_output_root / RESOURCE_OUTPUT_DIRNAME / resource_id
    output_dir.mkdir(parents=True, exist_ok=True)
    return VolumeMount(
        host_path=str(output_dir),
        container_path=LINUX_CONTAINER_OUTPUT_PATH,
        read_only=False,
    )
