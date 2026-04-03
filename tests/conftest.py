"""Shared pytest fixtures for the build orchestrator test suite."""
from __future__ import annotations

import pytest
from pathlib import Path

from orchestrator.models import (
    ArtifactSpec,
    BuildPlan,
    FailurePolicy,
    JobResult,
    JobSpec,
    ResourceWeight,
)
from orchestrator.scheduler import ResourceScheduler


@pytest.fixture
def default_resource_weight() -> ResourceWeight:
    return ResourceWeight(cpu_slots=1, memory_slots=1)


@pytest.fixture
def heavy_resource_weight() -> ResourceWeight:
    """Simulates a high-memory job."""
    return ResourceWeight(cpu_slots=2, memory_slots=4)


@pytest.fixture
def simple_job(default_resource_weight: ResourceWeight) -> JobSpec:
    return JobSpec(
        id="job_a",
        image="my-registry/builder-a:latest",
        depends_on=frozenset(),
        resource_weight=default_resource_weight,
        artifacts=[ArtifactSpec(source_glob="**/output/*", destination_subdir="out")],
        command=["--project", "services/project_a"],
    )


@pytest.fixture
def dependent_job(default_resource_weight: ResourceWeight) -> JobSpec:
    return JobSpec(
        id="job_b",
        image="my-registry/builder-b:latest",
        depends_on=frozenset({"job_a"}),
        resource_weight=default_resource_weight,
        artifacts=[ArtifactSpec(source_glob="**/output/*", destination_subdir="out")],
        command=["--project", "services/project_b"],
    )


@pytest.fixture
def minimal_plan(simple_job: JobSpec, dependent_job: JobSpec) -> BuildPlan:
    return BuildPlan(
        jobs=[simple_job, dependent_job],
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=2,
        total_cpu_slots=4,
        total_memory_slots=8,
    )


@pytest.fixture
def success_result(simple_job: JobSpec, tmp_path: Path) -> JobResult:
    return JobResult(
        job_id=simple_job.id,
        success=True,
        exit_code=0,
        duration_seconds=1.5,
        log_path=tmp_path / "job_a.log",
    )


@pytest.fixture
def failure_result(simple_job: JobSpec, tmp_path: Path) -> JobResult:
    return JobResult(
        job_id=simple_job.id,
        success=False,
        exit_code=1,
        duration_seconds=0.3,
        log_path=tmp_path / "job_a.log",
    )


@pytest.fixture
def resource_scheduler(minimal_plan: BuildPlan) -> ResourceScheduler:
    """Instantiate a scheduler with the minimal_plan."""
    return ResourceScheduler(minimal_plan)
