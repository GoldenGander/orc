from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from orchestrator.models import (
    BuildPlan,
    FailurePolicy,
    JobResult,
    JobSpec,
    OrchestratorResult,
    ResourceWeight,
)
from orchestrator.server.event_bus import EventBus
from orchestrator.server.reporter import CompositeReporter, EventBusReporter


def _simple_result(*jobs: JobResult) -> OrchestratorResult:
    success = all(j.success for j in jobs)
    return OrchestratorResult(success=success, job_results=tuple(jobs))


def _job_result(job_id: str, success: bool, exit_code: int = 0, duration: float = 1.0) -> JobResult:
    return JobResult(
        job_id=job_id,
        success=success,
        exit_code=exit_code,
        duration_seconds=duration,
        log_path=Path(__file__),
    )


# ---------------------------------------------------------------------------
# EventBusReporter — high-level events
# ---------------------------------------------------------------------------


def test_report_job_started_pushes_correct_event() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    reporter.report_job_started("compile")

    assert len(bus._buffer) == 1
    event = bus._buffer[0]
    assert event["type"] == "job_started"
    assert event["job_id"] == "compile"
    assert "ts" in event


def test_report_job_started_creates_per_job_bus() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    reporter.report_job_started("compile")

    assert bus.get_job_bus("compile") is not None


def test_report_job_completed_success() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    reporter.report_job_completed("compile", success=True)

    event = bus._buffer[0]
    assert event["type"] == "job_completed"
    assert event["job_id"] == "compile"
    assert event["success"] is True


def test_report_job_completed_failure() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    reporter.report_job_completed("compile", success=False)

    assert bus._buffer[0]["success"] is False


def test_report_result_pushes_pipeline_complete() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    result = _simple_result(_job_result("a", True))
    reporter.report_result(result)

    event = bus._buffer[0]
    assert event["type"] == "pipeline_complete"
    assert event["success"] is True
    assert event["total_jobs"] == 1
    assert event["failed_jobs"] == 0


def test_report_result_pushes_build_summary() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    result = _simple_result(_job_result("a", True))
    reporter.report_result(result)

    summary = bus._buffer[1]
    assert summary["type"] == "build_summary"
    assert summary["success"] is True
    assert summary["totals"]["jobs"] == 1
    assert summary["totals"]["succeeded"] == 1
    assert summary["totals"]["failed"] == 0
    assert summary["totals"]["skipped"] == 0
    assert len(summary["jobs"]) == 1
    assert summary["jobs"][0]["id"] == "a"
    assert summary["jobs"][0]["status"] == "success"
    assert "total_duration_seconds" in summary
    assert "ts" in summary


def test_report_result_closes_bus() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    result = OrchestratorResult(success=True, job_results=())
    assert bus.is_done is False
    reporter.report_result(result)
    assert bus.is_done is True


def test_report_result_counts_failed_jobs() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    result = OrchestratorResult(
        success=False,
        job_results=(
            _job_result("a", True, 0),
            _job_result("b", False, 1),
            _job_result("c", False, -1),
        ),
    )
    reporter.report_result(result)
    event = bus._buffer[0]
    assert event["failed_jobs"] == 2
    assert event["total_jobs"] == 3


def test_build_summary_classifies_skipped() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    result = OrchestratorResult(
        success=False,
        job_results=(
            _job_result("a", True, 0),
            _job_result("b", False, 1),
            _job_result("c", False, -1),  # skipped
        ),
    )
    reporter.report_result(result)
    summary = bus._buffer[1]
    assert summary["totals"]["succeeded"] == 1
    assert summary["totals"]["failed"] == 1
    assert summary["totals"]["skipped"] == 1


def test_build_summary_includes_slot_info_when_plan_provided() -> None:
    bus = EventBus()

    def _spec(job_id: str, cpu: int, mem: int) -> JobSpec:
        return JobSpec(
            id=job_id,
            image="img",
            depends_on=frozenset(),
            resource_weight=ResourceWeight(cpu_slots=cpu, memory_slots=mem),
            artifacts=[],
        )

    plan = BuildPlan(
        jobs=[_spec("compile", 2, 4)],
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=2,
        total_cpu_slots=4,
        total_memory_slots=8,
    )
    reporter = EventBusReporter(bus, plan=plan)
    reporter.report_result(_simple_result(_job_result("compile", True)))

    summary = bus._buffer[1]
    job_row = summary["jobs"][0]
    assert job_row["cpu_slots"] == 2
    assert job_row["memory_slots"] == 4


def test_build_summary_omits_host_when_no_sampler() -> None:
    bus = EventBus()
    reporter = EventBusReporter(bus)
    reporter.report_result(_simple_result(_job_result("a", True)))
    summary = bus._buffer[1]
    assert "host" not in summary


def test_build_summary_includes_host_when_sampler_provided() -> None:
    bus = EventBus()
    sampler = MagicMock()
    sampler.peak_cpu_percent.return_value = 75.0
    sampler.peak_memory_percent.return_value = 50.0
    sampler.avg_cpu_percent.return_value = 40.0
    reporter = EventBusReporter(bus, sampler=sampler)
    reporter.report_result(_simple_result(_job_result("a", True)))

    summary = bus._buffer[1]
    assert "host" in summary
    assert summary["host"]["peak_cpu_percent"] == 75.0
    assert summary["host"]["peak_memory_percent"] == 50.0
    assert summary["host"]["avg_cpu_percent"] == 40.0


# ---------------------------------------------------------------------------
# CompositeReporter
# ---------------------------------------------------------------------------


def _mock_reporter():
    m = MagicMock()
    m.report_job_started = MagicMock()
    m.report_job_completed = MagicMock()
    m.report_result = MagicMock()
    return m


def test_composite_fans_out_job_started() -> None:
    a, b = _mock_reporter(), _mock_reporter()
    reporter = CompositeReporter(a, b)
    reporter.report_job_started("build")
    a.report_job_started.assert_called_once_with("build")
    b.report_job_started.assert_called_once_with("build")


def test_composite_fans_out_job_completed() -> None:
    a, b = _mock_reporter(), _mock_reporter()
    reporter = CompositeReporter(a, b)
    reporter.report_job_completed("build", True)
    a.report_job_completed.assert_called_once_with("build", True)
    b.report_job_completed.assert_called_once_with("build", True)


def test_composite_fans_out_report_result() -> None:
    a, b = _mock_reporter(), _mock_reporter()
    reporter = CompositeReporter(a, b)
    result = OrchestratorResult(success=True, job_results=())
    reporter.report_result(result)
    a.report_result.assert_called_once_with(result)
    b.report_result.assert_called_once_with(result)
