from __future__ import annotations

from datetime import datetime, timezone
from typing import override

from orchestrator.models import OrchestratorResult
from orchestrator.pipeline import IPipelineReporter
from orchestrator.server.event_bus import EventBus


class EventBusReporter(IPipelineReporter):
    """Pushes pipeline lifecycle events to an EventBus for long-poll streaming."""

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    @override
    def report_job_started(self, job_id: str) -> None:
        self._bus.push({"type": "job_started", "job_id": job_id, "ts": _now_iso()})

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        self._bus.push(
            {"type": "job_completed", "job_id": job_id, "success": success, "ts": _now_iso()}
        )

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        failed_count = sum(1 for r in result.job_results if not r.success)
        self._bus.push(
            {
                "type": "pipeline_complete",
                "success": result.success,
                "total_jobs": len(result.job_results),
                "failed_jobs": failed_count,
                "ts": _now_iso(),
            }
        )
        self._bus.close()


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
