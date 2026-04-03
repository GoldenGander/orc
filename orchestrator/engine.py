from __future__ import annotations

import logging
from concurrent.futures import Future, wait, FIRST_COMPLETED
from pathlib import Path

from orchestrator.graph import DependencyGraph
from orchestrator.artifact_store import ArtifactStoreABC
from orchestrator.executor import ExecutorABC
from orchestrator.logger import JobLoggerABC
from orchestrator.scheduler import SchedulerABC
from orchestrator.models import (
    BuildPlan,
    FailurePolicy,
    JobResult,
    OrchestratorResult,
)
from orchestrator.pipeline import IPipelineReporter

logger = logging.getLogger(__name__)


class Engine:
    """Coordinates the full build orchestration lifecycle.

    Composes all subsystems via constructor injection so each dependency can
    be replaced with a test double without monkey-patching.

    Dispatch loop (run):
        1. Build and validate the dependency graph (detect cycles).
        2. Enter the scheduling loop:
           - Ask the scheduler for ready jobs (dependencies + resources).
           - Submit dispatchable jobs through the executor; acquire slots.
           - As Futures resolve, release slots, collect artifacts,
             apply the failure policy, and advance the graph.
        3. Call artifact_store.finalize() once all jobs have settled.
        4. Report the final result and return OrchestratorResult.
    """

    def __init__(
        self,
        scheduler: SchedulerABC,
        executor: ExecutorABC,
        artifact_store: ArtifactStoreABC,
        job_logger: JobLoggerABC,
        reporter: IPipelineReporter,
        output_root: Path,
    ) -> None:
        self._scheduler = scheduler
        self._executor = executor
        self._artifact_store = artifact_store
        self._job_logger = job_logger
        self._reporter = reporter
        self._output_root = output_root

    def run(self, plan: BuildPlan) -> OrchestratorResult:
        """Execute the full build plan and return the final result."""
        graph = DependencyGraph(plan)
        graph.validate()

        job_map = {job.id: job for job in plan.jobs}
        all_ids = set(job_map)

        completed: set[str] = set()
        failed: set[str] = set()
        results: list[JobResult] = []
        in_flight: dict[Future[JobResult], str] = {}
        cancelling = False

        try:
            self._executor.start(plan)
            while (completed | failed) != all_ids:
                # ---- submit phase ----
                if not cancelling:
                    ready = self._scheduler.ready_jobs(completed, failed)
                    for job_id in ready:
                        if job_id in completed or job_id in failed:
                            continue
                        if job_id in self._scheduler.running_jobs:
                            continue
                        job = job_map[job_id]
                        if not self._scheduler.can_dispatch(job):
                            continue
                        self._scheduler.acquire(job)
                        self._reporter.report_job_started(job_id)
                        logger.info("Submitting job %s", job_id)
                        future = self._executor.submit(job)
                        in_flight[future] = job_id

                # ---- stall detection ----
                # No futures running and not all settled → remaining jobs are
                # unreachable (blocked by failed dependencies or cancelled).
                if not in_flight:
                    for job_id in all_ids - completed - failed:
                        logger.info("Skipping unreachable job %s", job_id)
                        results.append(
                            JobResult(
                                job_id=job_id,
                                success=False,
                                exit_code=-1,
                                duration_seconds=0.0,
                                log_path=self._job_logger.get_log_path(job_id),
                            )
                        )
                        failed.add(job_id)
                    break

                # ---- wait phase ----
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)

                for future in done:
                    job_id = in_flight.pop(future)
                    job = job_map[job_id]
                    result = future.result()
                    results.append(result)
                    self._scheduler.release(job)

                    if result.success:
                        completed.add(job_id)
                        self._artifact_store.collect(job, result)
                        self._reporter.report_job_completed(job_id, True)
                        logger.info("Job %s succeeded (%.1fs)", job_id, result.duration_seconds)
                    else:
                        failed.add(job_id)
                        self._reporter.report_job_completed(job_id, False)
                        logger.warning(
                            "Job %s failed (exit %d, %.1fs)",
                            job_id,
                            result.exit_code,
                            result.duration_seconds,
                        )
                        if plan.failure_policy == FailurePolicy.FAIL_FAST:
                            cancelling = True
        finally:
            self._executor.stop()

        # ---- finalize ----
        self._artifact_store.finalize(self._output_root)

        orchestrator_result = OrchestratorResult(
            success=len(failed) == 0,
            job_results=tuple(results),
        )
        self._reporter.report_result(orchestrator_result)
        return orchestrator_result
