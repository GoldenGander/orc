from __future__ import annotations

from unittest.mock import MagicMock

from orchestrator.models import JobResult, OrchestratorResult
from orchestrator.server.event_bus import EventBus
from orchestrator.server.reporter import CompositeReporter, EventBusReporter


# ---------------------------------------------------------------------------
# EventBusReporter
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
    result = OrchestratorResult(
        success=True,
        job_results=(
            JobResult(
                job_id="a",
                success=True,
                exit_code=0,
                duration_seconds=1.0,
                log_path=__file__,  # type: ignore[arg-type]
            ),
        ),
    )
    reporter.report_result(result)

    event = bus._buffer[0]
    assert event["type"] == "pipeline_complete"
    assert event["success"] is True
    assert event["total_jobs"] == 1
    assert event["failed_jobs"] == 0


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
            JobResult("a", True, 0, 1.0, __file__),  # type: ignore[arg-type]
            JobResult("b", False, 1, 2.0, __file__),  # type: ignore[arg-type]
            JobResult("c", False, -1, 0.0, __file__),  # type: ignore[arg-type]
        ),
    )
    reporter.report_result(result)
    event = bus._buffer[0]
    assert event["failed_jobs"] == 2
    assert event["total_jobs"] == 3


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
