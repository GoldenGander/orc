from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, override

from orchestrator.models import OrchestratorResult, ResourceSpec
from orchestrator.pipeline import IPipelineReporter
from orchestrator.server.event_bus import EventBus

if TYPE_CHECKING:
    from orchestrator.models import BuildPlan
    from orchestrator.server.metrics import HostMetricsSampler


class EventBusReporter(IPipelineReporter):
    """Pushes pipeline lifecycle events to an EventBus for long-poll streaming.

    The main bus receives only high-level events:
        job_started, job_completed, resource_status, host_metrics,
        pipeline_complete, build_summary.

    Log lines (log_line) go to per-job buses accessed via bus.job_bus(job_id)
    and are served on the /jobs/<id>/events endpoint, keeping the main stream
    clean for callers that only need pipeline-level progress.

    Pass *plan* to include per-job slot declarations in the build summary.
    Pass *sampler* to include host peak/average CPU/memory in the summary.
    """

    def __init__(
        self,
        bus: EventBus,
        plan: "BuildPlan | None" = None,
        sampler: "HostMetricsSampler | None" = None,
    ) -> None:
        self._bus = bus
        self._plan = plan
        self._sampler = sampler
        self._start_time = time.monotonic()

    @override
    def report_job_started(self, job_id: str) -> None:
        # Eagerly create the per-job bus so clients can start polling
        # /jobs/<id>/events before the first log line arrives.
        self._bus.job_bus(job_id)
        self._bus.push({"type": "job_started", "job_id": job_id, "ts": _now_iso()})

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        self._bus.push(
            {"type": "job_completed", "job_id": job_id, "success": success, "ts": _now_iso()}
        )

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        failed_count = sum(1 for r in result.job_results if not r.success)
        # Backward-compatible summary fields on pipeline_complete.
        self._bus.push(
            {
                "type": "pipeline_complete",
                "success": result.success,
                "total_jobs": len(result.job_results),
                "failed_jobs": failed_count,
                "ts": _now_iso(),
            }
        )
        self._bus.push(self._build_summary(result))
        self._bus.close()

    @override
    def report_resource_status(self, resources: list[ResourceSpec]) -> None:
        self._bus.push(
            {
                "type": "resource_status",
                "resources": [
                    {"id": r.id, "kind": r.kind, "status": "running"}
                    for r in resources
                ],
                "ts": _now_iso(),
            }
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_summary(self, result: OrchestratorResult) -> dict:
        """Construct the audit-friendly build_summary event payload."""
        job_spec_map = (
            {job.id: job for job in self._plan.jobs} if self._plan is not None else {}
        )

        succeeded = failed = skipped = 0
        job_rows: list[dict] = []
        for r in result.job_results:
            if r.exit_code == -1 and not r.success:
                status = "skipped"
                skipped += 1
            elif r.success:
                status = "success"
                succeeded += 1
            else:
                status = "failed"
                failed += 1

            spec = job_spec_map.get(r.job_id)
            row: dict = {
                "id": r.job_id,
                "status": status,
                "exit_code": r.exit_code,
                "duration_seconds": round(r.duration_seconds, 2),
            }
            if spec is not None:
                row["cpu_slots"] = spec.resource_weight.cpu_slots
                row["memory_slots"] = spec.resource_weight.memory_slots
            job_rows.append(row)

        total_duration = round(time.monotonic() - self._start_time, 2)

        summary: dict = {
            "type": "build_summary",
            "success": result.success,
            "total_duration_seconds": total_duration,
            "totals": {
                "jobs": len(result.job_results),
                "succeeded": succeeded,
                "failed": failed,
                "skipped": skipped,
            },
            "jobs": job_rows,
            "ts": _now_iso(),
        }

        if self._sampler is not None:
            summary["host"] = {
                "peak_cpu_percent": self._sampler.peak_cpu_percent(),
                "peak_memory_percent": self._sampler.peak_memory_percent(),
                "avg_cpu_percent": self._sampler.avg_cpu_percent(),
            }

        return summary


class CompositeReporter(IPipelineReporter):
    """Fans out reporter calls to multiple delegates."""

    def __init__(self, *reporters: IPipelineReporter) -> None:
        self._reporters = reporters

    @override
    def report_job_started(self, job_id: str) -> None:
        for r in self._reporters:
            r.report_job_started(job_id)

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        for r in self._reporters:
            r.report_job_completed(job_id, success)

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        for r in self._reporters:
            r.report_result(result)

    @override
    def report_resource_status(self, resources: list[ResourceSpec]) -> None:
        for r in self._reporters:
            r.report_resource_status(resources)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
