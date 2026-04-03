from __future__ import annotations

from abc import ABC, abstractmethod

from orchestrator.models import BuildPlan, JobSpec
from orchestrator.graph import DependencyGraph


class SchedulerABC(ABC):
    """Controls resource-aware job dispatch using full build context.

    The scheduler receives the complete BuildPlan and uses the dependency graph
    to make informed scheduling decisions. It determines which jobs are ready to
    run (dependency + resource constraints) and brackets execution with
    acquire()/release() to track resource allocation.
    """

    plan: BuildPlan
    """The BuildPlan this scheduler manages."""

    @property
    @abstractmethod
    def running_jobs(self) -> set[str]:
        """Return IDs of currently running jobs."""
        ...

    @property
    @abstractmethod
    def allocated_cpu_slots(self) -> int:
        """Return total CPU slots currently allocated to running jobs."""
        ...

    @property
    @abstractmethod
    def allocated_memory_slots(self) -> int:
        """Return total memory slots currently allocated to running jobs."""
        ...

    def get_job(self, job_id: str) -> JobSpec:
        """Return the JobSpec for the given job ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            The JobSpec for the job.

        Raises:
            KeyError: If no job with that ID exists.
        """
        for job in self.plan.jobs:
            if job.id == job_id:
                return job
        raise KeyError(f"Job {job_id} not found in plan")

    @abstractmethod
    def ready_jobs(
        self, completed: set[str], failed: set[str]
    ) -> list[str]:
        """Return IDs of jobs that are ready to dispatch.

        Jobs must satisfy both:
        1. Dependency constraints (all dependencies completed)
        2. Resource constraints (can fit within current slot budgets)

        Jobs whose dependencies include any failed job are excluded.

        Args:
            completed: IDs of jobs that have finished successfully.
            failed: IDs of jobs that have finished with an error or been skipped.

        Returns:
            List of job IDs ready to dispatch, considering both dependencies
            and available resources.
        """
        ...

    @abstractmethod
    def can_dispatch(self, job: JobSpec) -> bool:
        """Return True if the job can start without exceeding resource limits.

        Checks both concurrency (max_parallel) and slot budgets (cpu_slots,
        memory_slots) against currently running jobs.
        """
        ...

    @abstractmethod
    def acquire(self, job: JobSpec) -> None:
        """Reserve resources for the given job.

        Called immediately before the job is submitted to the executor.
        Must be paired with a release() call regardless of job outcome.
        """
        ...

    @abstractmethod
    def release(self, job: JobSpec) -> None:
        """Release resources held by the given job.

        Called after the job's Future resolves (success or failure).
        """
        ...


class ResourceScheduler(SchedulerABC):
    """Scheduler that combines dependency awareness with resource constraints.

    Analyzes the full BuildPlan and uses DependencyGraph to make efficient
    scheduling decisions based on job dependencies and available resources.
    """

    def __init__(self, plan: BuildPlan) -> None:
        """Initialize the scheduler with a complete build plan.

        Args:
            plan: The complete BuildPlan containing all jobs and constraints.
        """
        self.plan = plan
        self.graph = DependencyGraph(plan)
        self._job_map = {job.id: job for job in plan.jobs}

        self._running_jobs_set: set[str] = set()
        self._allocated_cpu = 0
        self._allocated_mem = 0

    @property
    def running_jobs(self) -> set[str]:
        """Return IDs of currently running jobs."""
        return self._running_jobs_set.copy()

    @property
    def allocated_cpu_slots(self) -> int:
        """Return total CPU slots currently allocated to running jobs."""
        return self._allocated_cpu

    @property
    def allocated_memory_slots(self) -> int:
        """Return total memory slots currently allocated to running jobs."""
        return self._allocated_mem

    def ready_jobs(
        self, completed: set[str], failed: set[str]
    ) -> list[str]:
        """Return jobs ready to dispatch, considering dependencies and resources."""
        # Get jobs that satisfy dependency constraints
        dependency_ready = self.graph.ready_jobs(completed, failed)

        # Filter to only those that have available resources
        resource_ready = []
        for job_id in dependency_ready:
            job = self._job_map[job_id]
            if self.can_dispatch(job):
                resource_ready.append(job_id)

        return resource_ready

    def can_dispatch(self, job: JobSpec) -> bool:
        """Check if job can be dispatched without exceeding resource limits."""
        # Check concurrency limit
        if len(self._running_jobs_set) >= self.plan.max_parallel:
            return False

        # Check CPU slots
        if (
            self._allocated_cpu + job.resource_weight.cpu_slots
            > self.plan.total_cpu_slots
        ):
            return False

        # Check memory slots
        if (
            self._allocated_mem + job.resource_weight.memory_slots
            > self.plan.total_memory_slots
        ):
            return False

        return True

    def acquire(self, job: JobSpec) -> None:
        """Reserve resources for the job."""
        self._running_jobs_set.add(job.id)
        self._allocated_cpu += job.resource_weight.cpu_slots
        self._allocated_mem += job.resource_weight.memory_slots

    def release(self, job: JobSpec) -> None:
        """Release resources held by the job.

        Safe to call even if the job was not acquired (idempotent).
        """
        if job.id in self._running_jobs_set:
            self._running_jobs_set.discard(job.id)
            self._allocated_cpu -= job.resource_weight.cpu_slots
            self._allocated_mem -= job.resource_weight.memory_slots
