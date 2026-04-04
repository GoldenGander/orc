from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future

from orchestrator.models import BuildPlan, JobResult, JobSpec


class ExecutorABC(ABC):
    """Submits jobs for concurrent execution.

    Abstracts the execution backend so the engine dispatch loop can stay
    backend-agnostic even when the concrete runtime is container-based.
    """

    @abstractmethod
    def submit(self, job: JobSpec) -> Future[JobResult]:
        """Start a job and return a Future for its result.

        The concrete executor is responsible for interpreting the job spec,
        applying the backend-specific runtime contract, and resolving the
        Future with a JobResult once execution completes.

        Args:
            job: The job to execute.

        Returns:
            A Future that resolves to a JobResult when the container exits.
        """
        ...

    def start(self, plan: BuildPlan) -> None:
        """Optional lifecycle hook run once before job submission begins."""

    def stop(self) -> None:
        """Optional lifecycle hook run once after orchestration is finished."""

    @abstractmethod
    def shutdown(self, wait: bool = True) -> None:
        """Shut down the executor.

        Args:
            wait: If True, block until all submitted jobs have completed.
                If False, cancel pending work and return immediately.
        """
        ...
