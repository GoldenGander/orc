from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future

from orchestrator.models import BuildPlan, JobResult, JobSpec


class ExecutorABC(ABC):
    """Submits jobs for concurrent execution inside Docker containers.

    Abstracts the concurrency backend so the engine dispatch loop is
    backend-agnostic. Each submitted job maps directly to one container run:
    `docker run <image> <command>` (or the image default if command is None).
    """

    @abstractmethod
    def submit(self, job: JobSpec) -> Future[JobResult]:
        """Spawn a container for the job and return a Future for its result.

        The executor pulls the image declared in job.image, passes job.command
        as the container command (or uses the image entrypoint if None), injects
        job.env_vars, and resolves the Future with a JobResult once the
        container exits.

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
