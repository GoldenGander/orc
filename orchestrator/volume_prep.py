"""Injects system-managed volume mounts into a BuildPlan.

Source and artifact-output mounts are pipeline-level concerns. Jobs get
the standard source + output mounts, while managed resources get a managed output
mount so they can write optional first-class artifacts to a consistent
host location.

Container path conventions:
    /src     — read-only source tree (jobs only)
    /output  — writable artifact output directory
"""
from __future__ import annotations

from pathlib import Path

from orchestrator.models import BuildPlan, ResourceDriver, VolumeMount
from orchestrator.path_safety import require_safe_path_component

CONTAINER_SOURCE_PATH = "/src"
CONTAINER_OUTPUT_PATH = "/output"
CONTAINER_INPUT_PREFIX = "/input"
RESOURCE_OUTPUT_DIRNAME = "resources"


def prepare_volumes(
    plan: BuildPlan,
    source_dir: Path,
    container_output_root: Path,
) -> None:
    """Append managed source/output mounts to jobs and output mounts to resources.

    For each job the function:
    1. Creates ``container_output_root / <job_id>`` on the host.
    2. Appends a read-only bind mount  source_dir → /src.
    3. Appends a read-write bind mount  container_output_root/<job_id> → /output.

    For each managed resource the function:
    1. Creates ``container_output_root / resources / <resource_id>`` on the host.
    2. Appends a read-write bind mount of that directory → /output.

    Args:
        plan: The BuildPlan whose jobs will be mutated in place.
        source_dir: Absolute host path to the checked-out source tree.
        container_output_root: Absolute host path under which per-job
            and per-resource output directories are created.
    """
    file_shares = {
        resource.id: resource
        for resource in plan.resources
        if resource.driver == ResourceDriver.FILE_SHARE
    }

    for job in plan.jobs:
        job_output_dir = container_output_root / require_safe_path_component(
            job.id, owner_label="Job", field_name="id"
        )
        job_output_dir.mkdir(parents=True, exist_ok=True)

        job.volumes.append(
            VolumeMount(
                host_path=str(source_dir),
                container_path=CONTAINER_SOURCE_PATH,
                read_only=True,
            )
        )
        job.volumes.append(
            VolumeMount(
                host_path=str(job_output_dir),
                container_path=CONTAINER_OUTPUT_PATH,
                read_only=False,
            )
        )

        for resource_id in job.resources:
            share = file_shares[resource_id]
            job.volumes.append(
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
            job.volumes.append(
                VolumeMount(
                    host_path=str(source_output_dir),
                    container_path=f"{CONTAINER_INPUT_PREFIX}/{safe_source_id}",
                    read_only=True,
                )
            )

    for resource in plan.resources:
        if resource.driver != ResourceDriver.DOCKER_CONTAINER:
            continue
        resource_output_dir = container_output_root / RESOURCE_OUTPUT_DIRNAME / require_safe_path_component(
            resource.id, owner_label="Resource", field_name="id"
        )
        resource_output_dir.mkdir(parents=True, exist_ok=True)
        resource.volumes.append(
            VolumeMount(
                host_path=str(resource_output_dir),
                container_path=CONTAINER_OUTPUT_PATH,
                read_only=False,
            )
        )
