"""Tests for the SchedulerABC interface."""
from __future__ import annotations

import pytest

from orchestrator.scheduler import SchedulerABC
from orchestrator.models import BuildPlan, JobSpec, ResourceWeight


def test_scheduler_creation(resource_scheduler: SchedulerABC, minimal_plan: BuildPlan):
    """Test that a scheduler is created with full plan context."""
    assert resource_scheduler.plan == minimal_plan
    assert resource_scheduler.plan.max_parallel == 2
    assert resource_scheduler.plan.total_cpu_slots == 4
    assert resource_scheduler.plan.total_memory_slots == 8


def test_ready_jobs_initial_state(resource_scheduler: SchedulerABC):
    """Test ready_jobs returns dependency-ready jobs with available resources."""
    ready = resource_scheduler.ready_jobs(completed=set(), failed=set())
    # Only job_a has no dependencies
    assert ready == ["job_a"]


def test_ready_jobs_after_completion(resource_scheduler: SchedulerABC):
    """Test ready_jobs returns dependent jobs after their dependencies complete."""
    ready = resource_scheduler.ready_jobs(completed={"job_a"}, failed=set())
    # Now job_b is ready (all dependencies satisfied)
    assert "job_b" in ready


def test_ready_jobs_excludes_failed_dependents(resource_scheduler: SchedulerABC):
    """Test ready_jobs excludes jobs whose dependencies have failed."""
    ready = resource_scheduler.ready_jobs(completed=set(), failed={"job_a"})
    # job_b depends on job_a which failed
    assert "job_b" not in ready


def test_ready_jobs_with_resource_constraint(resource_scheduler: SchedulerABC):
    """Test ready_jobs respects resource limits when determining dispatch eligibility."""
    # Acquire resources to fill concurrency limit (max_parallel = 2)
    job_a = resource_scheduler.get_job("job_a")
    job_b = resource_scheduler.get_job("job_b")
    resource_scheduler.acquire(job_a)
    resource_scheduler.acquire(job_b)

    # job_a is dependency-ready but can't dispatch due to concurrency limit being full
    ready = resource_scheduler.ready_jobs(completed=set(), failed=set())
    assert "job_a" not in ready


def test_can_dispatch_empty_scheduler(
    resource_scheduler: SchedulerABC, simple_job: JobSpec
):
    """Test that jobs can dispatch when no resources are allocated."""
    assert resource_scheduler.can_dispatch(simple_job)


def test_can_dispatch_cpu_limit(resource_scheduler: SchedulerABC):
    """Test that dispatch fails when CPU slots are exhausted."""
    # Acquire all CPU slots with 4 simple jobs (1 CPU each)
    job_a = resource_scheduler.get_job("job_a")
    for i in range(4):
        job = (
            job_a
            if i == 0
            else JobSpec(
                id=f"job_cpu_{i}",
                image="img:latest",
                depends_on=frozenset(),
                resource_weight=job_a.resource_weight,
                artifacts=[],
                command=[],
            )
        )
        resource_scheduler.acquire(job)

    # Next job should fail due to CPU limit
    another_job = JobSpec(
        id="job_cpu_fail",
        image="img:latest",
        depends_on=frozenset(),
        resource_weight=job_a.resource_weight,
        artifacts=[],
        command=[],
    )
    assert not resource_scheduler.can_dispatch(another_job)


def test_can_dispatch_memory_limit(
    resource_scheduler: SchedulerABC,
    heavy_resource_weight: ResourceWeight,
):
    """Test that dispatch fails when memory slots are exhausted."""
    job_a = resource_scheduler.get_job("job_a")
    # Acquire most memory with 2 heavy jobs (4 memory each = 8 total)
    for i in range(2):
        job = JobSpec(
            id=f"job_mem_{i}",
            image="img:latest",
            depends_on=frozenset(),
            resource_weight=heavy_resource_weight,
            artifacts=[],
            command=[],
        )
        resource_scheduler.acquire(job)

    # Next job should fail due to memory limit
    another_job = JobSpec(
        id="job_mem_fail",
        image="img:latest",
        depends_on=frozenset(),
        resource_weight=job_a.resource_weight,
        artifacts=[],
        command=[],
    )
    assert not resource_scheduler.can_dispatch(another_job)


def test_can_dispatch_concurrency_limit(resource_scheduler: SchedulerABC):
    """Test that dispatch fails when max_parallel is reached."""
    job_a = resource_scheduler.get_job("job_a")
    # Acquire 2 slots (max_parallel = 2)
    for i in range(2):
        job = (
            job_a
            if i == 0
            else JobSpec(
                id=f"job_parallel_{i}",
                image="img:latest",
                depends_on=frozenset(),
                resource_weight=job_a.resource_weight,
                artifacts=[],
                command=[],
            )
        )
        resource_scheduler.acquire(job)

    # Next job should fail due to concurrency limit
    another_job = JobSpec(
        id="job_parallel_fail",
        image="img:latest",
        depends_on=frozenset(),
        resource_weight=job_a.resource_weight,
        artifacts=[],
        command=[],
    )
    assert not resource_scheduler.can_dispatch(another_job)


def test_acquire_reserves_resources(resource_scheduler: SchedulerABC):
    """Test that acquire() reserves CPU and memory slots."""
    job_a = resource_scheduler.get_job("job_a")
    assert resource_scheduler.allocated_cpu_slots == 0
    assert resource_scheduler.allocated_memory_slots == 0

    resource_scheduler.acquire(job_a)

    assert resource_scheduler.allocated_cpu_slots == 1
    assert resource_scheduler.allocated_memory_slots == 1
    assert job_a.id in resource_scheduler.running_jobs


def test_release_frees_resources(resource_scheduler: SchedulerABC):
    """Test that release() frees reserved resources."""
    job_a = resource_scheduler.get_job("job_a")
    resource_scheduler.acquire(job_a)
    resource_scheduler.release(job_a)

    assert resource_scheduler.allocated_cpu_slots == 0
    assert resource_scheduler.allocated_memory_slots == 0
    assert job_a.id not in resource_scheduler.running_jobs


def test_acquire_release_cycle(
    resource_scheduler: SchedulerABC,
    heavy_resource_weight: ResourceWeight,
):
    """Test acquire/release cycle allows job to dispatch again after release."""
    # Create a heavy job that exhausts most resources
    heavy_job = JobSpec(
        id="heavy",
        image="img:latest",
        depends_on=frozenset(),
        resource_weight=heavy_resource_weight,  # 2 CPU, 4 memory
        artifacts=[],
        command=[],
    )

    assert resource_scheduler.can_dispatch(heavy_job)

    resource_scheduler.acquire(heavy_job)
    job_a = resource_scheduler.get_job("job_a")
    # After acquiring heavy job (2 CPU, 4 memory), simple job (1 CPU, 1 memory) still fits
    assert resource_scheduler.can_dispatch(job_a)

    resource_scheduler.release(heavy_job)
    # After release, we're back to empty
    assert resource_scheduler.can_dispatch(heavy_job)


def test_release_nonexistent_job(resource_scheduler: SchedulerABC):
    """Test that releasing a non-acquired job is safe (idempotent)."""
    job = JobSpec(
        id="never_acquired",
        image="img:latest",
        depends_on=frozenset(),
        resource_weight=ResourceWeight(),
        artifacts=[],
        command=[],
    )
    # Should not raise an error
    resource_scheduler.release(job)
    assert resource_scheduler.allocated_cpu_slots == 0
    assert resource_scheduler.allocated_memory_slots == 0
