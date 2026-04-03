"""Tests for the DependencyGraph class."""
from __future__ import annotations

import pytest

from orchestrator.exceptions import CyclicDependencyError
from orchestrator.graph import DependencyGraph
from orchestrator.models import BuildPlan, JobSpec, ArtifactSpec, FailurePolicy, ResourceWeight


def test_graph_creation_from_plan(minimal_plan: BuildPlan):
    """Test that a DependencyGraph can be created from a BuildPlan."""
    graph = DependencyGraph(minimal_plan)
    assert graph is not None


def test_validate_acyclic_graph(minimal_plan: BuildPlan):
    """Test validation passes for a valid DAG."""
    graph = DependencyGraph(minimal_plan)
    graph.validate()  # Should not raise


def test_topological_order_simple(minimal_plan: BuildPlan):
    """Test topological ordering of jobs with dependencies."""
    graph = DependencyGraph(minimal_plan)
    order = graph.topological_order()

    assert len(order) == 2
    assert order[0] == "job_a"  # job_a has no dependencies
    assert order[1] == "job_b"  # job_b depends on job_a


def test_ready_jobs_initial_state(minimal_plan: BuildPlan):
    """Test ready_jobs returns only jobs with no dependencies initially."""
    graph = DependencyGraph(minimal_plan)
    ready = graph.ready_jobs(completed=set(), failed=set())

    assert ready == ["job_a"]  # Only job_a has no dependencies


def test_ready_jobs_after_completion(minimal_plan: BuildPlan):
    """Test ready_jobs returns dependent jobs after their dependencies complete."""
    graph = DependencyGraph(minimal_plan)
    ready = graph.ready_jobs(completed={"job_a"}, failed=set())

    assert "job_b" in ready


def test_ready_jobs_excludes_failed_dependents(minimal_plan: BuildPlan):
    """Test ready_jobs excludes jobs whose dependencies have failed."""
    graph = DependencyGraph(minimal_plan)
    ready = graph.ready_jobs(completed=set(), failed={"job_a"})

    assert "job_b" not in ready


def test_detect_cyclic_dependency(default_resource_weight: ResourceWeight):
    """Test that cyclic dependencies are detected."""
    job_a = JobSpec(
        id="job_a",
        image="img:latest",
        depends_on=frozenset({"job_b"}),
        resource_weight=default_resource_weight,
        artifacts=[],
        command=[],
    )
    job_b = JobSpec(
        id="job_b",
        image="img:latest",
        depends_on=frozenset({"job_a"}),
        resource_weight=default_resource_weight,
        artifacts=[],
        command=[],
    )

    cyclic_plan = BuildPlan(
        jobs=[job_a, job_b],
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=2,
        total_cpu_slots=4,
        total_memory_slots=8,
    )

    graph = DependencyGraph(cyclic_plan)
    with pytest.raises(CyclicDependencyError):
        graph.validate()


def test_topological_order_single_job(simple_job: JobSpec):
    """Test topological ordering with a single job."""
    plan = BuildPlan(
        jobs=[simple_job],
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=1,
        total_cpu_slots=4,
        total_memory_slots=8,
    )

    graph = DependencyGraph(plan)
    order = graph.topological_order()

    assert order == ["job_a"]
