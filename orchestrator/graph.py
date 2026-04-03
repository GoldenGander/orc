from __future__ import annotations

from orchestrator.exceptions import CyclicDependencyError
from orchestrator.models import BuildPlan, JobSpec


class DependencyGraph:
    """Tracks job dependencies and determines dispatch order.

    Built from a BuildPlan. Callers use ready_jobs() in a loop, advancing
    the set of completed job IDs as futures resolve.
    """

    def __init__(self, plan: BuildPlan) -> None:
        self.plan = plan
        self._job_map = {job.id: job for job in plan.jobs}
        # Build reverse dependency map: job_id → set of job IDs that depend on it
        self._dependents = {job.id: set() for job in plan.jobs}
        for job in plan.jobs:
            for dep in job.depends_on:
                self._dependents[dep].add(job.id)

    def validate(self) -> None:
        """Assert the graph is a DAG.

        Raises:
            CyclicDependencyError: If any cycle is detected.
        """
        visited = set()
        rec_stack = set()

        def has_cycle(job_id: str) -> bool:
            """DFS to detect cycles."""
            visited.add(job_id)
            rec_stack.add(job_id)

            for dep in self._job_map[job_id].depends_on:
                if dep not in visited:
                    if has_cycle(dep):
                        return True
                elif dep in rec_stack:
                    return True

            rec_stack.remove(job_id)
            return False

        for job_id in self._job_map:
            if job_id not in visited:
                if has_cycle(job_id):
                    raise CyclicDependencyError(f"Cycle detected in job dependencies")

    def topological_order(self) -> list[str]:
        """Return all job IDs in a valid serial execution order.

        Raises:
            CyclicDependencyError: If the graph is not a DAG.
        """
        # Kahn's algorithm: process nodes with in-degree 0
        in_degree = {job.id: len(job.depends_on) for job in self.plan.jobs}
        queue = [job_id for job_id, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            job_id = queue.pop(0)
            result.append(job_id)

            # Reduce in-degree of all dependent jobs
            for dependent in self._dependents[job_id]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # If not all jobs were processed, there's a cycle
        if len(result) != len(self._job_map):
            raise CyclicDependencyError(f"Cycle detected in job dependencies")

        return result

    def ready_jobs(self, completed: set[str], failed: set[str]) -> list[str]:
        """Return IDs of jobs whose dependencies are all in completed.

        Jobs whose dependencies include any failed job are excluded so the
        scheduler never dispatches work that cannot succeed.

        Args:
            completed: IDs of jobs that have finished successfully.
            failed: IDs of jobs that have finished with an error or been skipped.
        """
        ready = []
        for job in self.plan.jobs:
            # Skip if any dependency failed
            if job.depends_on & failed:
                continue
            # Include if all dependencies are completed (or job has no dependencies)
            if job.depends_on <= completed:
                ready.append(job.id)
        return ready
