"""Injects system-managed volume mounts into a BuildPlan.

Source and artifact-output mounts are pipeline-level concerns — individual
jobs should not need to declare them.  This module adds them to every job
before the plan is handed to the engine.

Container path conventions:
    /src     — read-only source tree (agent checkout)
    /output  — writable artifact output directory (per-job on host)
"""
from __future__ import annotations

from pathlib import Path

from orchestrator.models import BuildPlan, VolumeMount

CONTAINER_SOURCE_PATH = "/src"
CONTAINER_OUTPUT_PATH = "/output"


def prepare_volumes(
    plan: BuildPlan,
    source_dir: Path,
    container_output_root: Path,
) -> None:
    """Append source and output volume mounts to every job in the plan.

    For each job the function:
    1. Creates ``container_output_root / <job_id>`` on the host.
    2. Appends a read-only bind mount  source_dir → /src.
    3. Appends a read-write bind mount  container_output_root/<job_id> → /output.

    Args:
        plan: The BuildPlan whose jobs will be mutated in place.
        source_dir: Absolute host path to the checked-out source tree.
        container_output_root: Absolute host path under which per-job
            output directories are created.
    """
    for job in plan.jobs:
        job_output_dir = container_output_root / job.id
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
