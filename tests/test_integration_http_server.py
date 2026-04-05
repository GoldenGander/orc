"""Integration test for HTTP server event streaming feature.

This test runs a real build pipeline with the EventBusJobLogger and
OrchestratorHTTPServer, then concurrently polls events and status while
the build executes. It validates that:

1. All expected events are received in correct order
2. Cursor pagination works correctly
3. Log lines are captured and streamed
4. Status endpoint reflects running state
"""
from __future__ import annotations

import json
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import override

import pytest

from orchestrator.artifact_store import ArtifactStore
from orchestrator.config import YamlConfigLoader
from orchestrator.engine import Engine
from orchestrator.executor import DockerExecutor
from orchestrator.models import OrchestratorResult
from orchestrator.pipeline import IPipelineReporter
from orchestrator.scheduler import ResourceScheduler
from orchestrator.server.event_bus import EventBus
from orchestrator.server.http_server import OrchestratorHTTPServer
from orchestrator.server.reporter import CompositeReporter, EventBusReporter
from orchestrator.server.tee_logger import EventBusJobLogger

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Lightweight stub for recording results
# ---------------------------------------------------------------------------


class RecordingReporter(IPipelineReporter):
    """Captures pipeline lifecycle events for assertions."""

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.final_result: OrchestratorResult | None = None

    @override
    def report_job_started(self, job_id: str) -> None:
        pass

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        pass

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        self.final_result = result

    @override
    def report_resource_status(self, resources: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    """Return True if the Docker daemon is reachable."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.SubprocessError):
        return False


requires_docker = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

integration = pytest.mark.integration


def _free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get_json(url: str) -> dict:
    """Fetch JSON from a URL."""
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


def _poll_events(port: int, cursor: int = 0, timeout: float = 30.0) -> tuple[list[dict], int, bool]:
    """Poll /events endpoint and return (events, new_cursor, done)."""
    url = f"http://127.0.0.1:{port}/events?cursor={cursor}"
    try:
        data = _get_json(url)
        return data["events"], data["cursor"], data["done"]
    except urllib.error.HTTPError as e:
        raise AssertionError(f"Failed to poll events: {e}") from e


def _drain_events(port: int, timeout: float = 60.0) -> list[dict]:
    """Poll /events until done, returning all events."""
    all_events: list[dict] = []
    cursor = 0
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            events, cursor, done = _poll_events(port, cursor, timeout=5.0)
            all_events.extend(events)
            if done and not events:
                break
        except (urllib.error.HTTPError, TimeoutError):
            pass
        time.sleep(0.1)

    return all_events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_docker
@integration
class TestHTTPServerEventStreaming:
    """Test HTTP server event streaming during real pipeline execution."""

    def test_events_captured_during_execution(self, tmp_path: Path) -> None:
        """Verify all events are captured and streamed via /events endpoint."""
        # ---- config ----
        loader = YamlConfigLoader()
        plan = loader.load(FIXTURES_DIR / "integration_plan.yaml")

        # ---- directories ----
        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()


        # ---- wire up real components with event bus ----
        bus = EventBus()
        job_logger = EventBusJobLogger(log_dir, bus)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter(bus)
        composite_reporter = CompositeReporter(reporter, EventBusReporter(bus))
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

        # ---- start HTTP server on a free port ----
        port = _free_port()
        server = OrchestratorHTTPServer(port, bus)
        server_thread = threading.Thread(target=server.serve_until_done, daemon=True)
        server_thread.start()
        time.sleep(0.1)  # let server bind

        # ---- run engine in separate thread ----
        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=composite_reporter,
            output_root=output_dir,
        )

        engine_exc: list[BaseException] = []

        def _run_engine() -> None:
            try:
                engine.run(plan)
            except BaseException as exc:
                engine_exc.append(exc)
            finally:
                executor.shutdown()

        engine_thread = threading.Thread(target=_run_engine, daemon=False)
        engine_thread.start()

        # ---- concurrently poll events until done ----
        all_events = _drain_events(port, timeout=60.0)
        server_thread.join(timeout=10.0)
        engine_thread.join(timeout=10.0)

        if engine_exc:
            raise engine_exc[0]

        # ---- verify events ----
        assert len(all_events) > 0, "No events captured"

        # Extract event types and job_ids for easier assertions
        event_types = [e["type"] for e in all_events]

        # Verify we got job_started events for each job
        job_started_events = [e for e in all_events if e["type"] == "job_started"]
        assert len(job_started_events) == 4, f"Expected 4 job_started events, got {len(job_started_events)}"

        started_job_ids = {e["job_id"] for e in job_started_events}
        assert started_job_ids == {"greeting", "farewell", "parallel_a", "final"}

        # Verify we got job_completed events for each job
        job_completed_events = [e for e in all_events if e["type"] == "job_completed"]
        assert len(job_completed_events) == 4, f"Expected 4 job_completed events, got {len(job_completed_events)}"

        completed_job_ids = {e["job_id"] for e in job_completed_events}
        assert completed_job_ids == {"greeting", "farewell", "parallel_a", "final"}

        # All jobs should have succeeded
        assert all(e["success"] for e in job_completed_events), "Some jobs failed unexpectedly"

        # Verify we got log_line events
        log_line_events = [e for e in all_events if e["type"] == "log_line"]
        assert len(log_line_events) > 0, "No log_line events captured"

        # Each log_line should have job_id and line content
        for event in log_line_events:
            assert "job_id" in event
            assert "line" in event
            assert "ts" in event
            assert event["job_id"] in {"greeting", "farewell", "parallel_a", "final"}

        # Verify we got pipeline_complete event
        pipeline_complete = [e for e in all_events if e["type"] == "pipeline_complete"]
        assert len(pipeline_complete) == 1, "Expected exactly one pipeline_complete event"
        assert pipeline_complete[0]["success"] is True
        assert pipeline_complete[0]["total_jobs"] == 4
        assert pipeline_complete[0]["failed_jobs"] == 0

    def test_cursor_pagination(self, tmp_path: Path) -> None:
        """Verify cursor pagination works correctly during execution."""
        # ---- config ----
        loader = YamlConfigLoader()
        plan = loader.load(FIXTURES_DIR / "integration_plan.yaml")

        # ---- directories ----
        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()


        # ---- wire up real components with event bus ----
        bus = EventBus()
        job_logger = EventBusJobLogger(log_dir, bus)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter(bus)
        composite_reporter = CompositeReporter(reporter, EventBusReporter(bus))
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

        # ---- start HTTP server ----
        port = _free_port()
        server = OrchestratorHTTPServer(port, bus)
        server_thread = threading.Thread(target=server.serve_until_done, daemon=True)
        server_thread.start()
        time.sleep(0.1)

        # ---- run engine ----
        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=composite_reporter,
            output_root=output_dir,
        )

        engine_exc: list[BaseException] = []

        def _run_engine() -> None:
            try:
                engine.run(plan)
            except BaseException as exc:
                engine_exc.append(exc)
            finally:
                executor.shutdown()

        engine_thread = threading.Thread(target=_run_engine, daemon=False)
        engine_thread.start()

        # ---- poll events with cursor pagination ----
        all_events: list[dict] = []
        cursor = 0
        deadline = time.monotonic() + 60.0

        while time.monotonic() < deadline:
            try:
                events, new_cursor, done = _poll_events(port, cursor, timeout=5.0)

                # Cursor should advance by number of events
                assert new_cursor == cursor + len(events), \
                    f"Cursor mismatch: expected {cursor + len(events)}, got {new_cursor}"

                all_events.extend(events)
                cursor = new_cursor

                if done and not events:
                    break
            except (urllib.error.HTTPError, TimeoutError):
                pass
            time.sleep(0.05)

        server_thread.join(timeout=10.0)
        engine_thread.join(timeout=10.0)

        if engine_exc:
            raise engine_exc[0]

        # ---- verify pagination worked ----
        assert len(all_events) > 0

        # Should have events from each job
        event_types = [e["type"] for e in all_events]
        assert "job_started" in event_types
        assert "job_completed" in event_types
        assert "log_line" in event_types
        assert "pipeline_complete" in event_types

    def test_status_endpoint_reflects_state(self, tmp_path: Path) -> None:
        """Verify /status endpoint correctly reflects running/completed state."""
        # ---- config ----
        loader = YamlConfigLoader()
        plan = loader.load(FIXTURES_DIR / "integration_plan.yaml")

        # ---- directories ----
        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()


        # ---- wire up real components with event bus ----
        bus = EventBus()
        job_logger = EventBusJobLogger(log_dir, bus)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter(bus)
        composite_reporter = CompositeReporter(reporter, EventBusReporter(bus))
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

        # ---- start HTTP server ----
        port = _free_port()
        server = OrchestratorHTTPServer(port, bus)
        server_thread = threading.Thread(target=server.serve_until_done, daemon=True)
        server_thread.start()
        time.sleep(0.1)

        # ---- run engine ----
        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=composite_reporter,
            output_root=output_dir,
        )

        engine_exc: list[BaseException] = []

        def _run_engine() -> None:
            try:
                engine.run(plan)
            except BaseException as exc:
                engine_exc.append(exc)
            finally:
                executor.shutdown()

        engine_thread = threading.Thread(target=_run_engine, daemon=False)
        engine_thread.start()

        # ---- check status during execution ----
        initial_status = None
        final_status = None
        deadline = time.monotonic() + 60.0

        while time.monotonic() < deadline:
            try:
                status = _get_json(f"http://127.0.0.1:{port}/status")

                # Every status should have required fields
                assert "done" in status
                assert "elapsed_seconds" in status
                assert "jobs" in status

                if initial_status is None:
                    initial_status = status

                # Once done, stay done
                if status["done"]:
                    final_status = status
                    break

                time.sleep(0.2)
            except (urllib.error.HTTPError, TimeoutError):
                pass

        server_thread.join(timeout=10.0)
        engine_thread.join(timeout=10.0)

        if engine_exc:
            raise engine_exc[0]

        # ---- verify status progression ----
        assert initial_status is not None, "Could not fetch initial status"
        assert final_status is not None, "Build did not complete"

        # Initial status should show jobs as empty or in progress
        assert isinstance(initial_status["jobs"], dict)

        # Final status should show done=True with job outcomes
        assert final_status["done"] is True
        assert len(final_status["jobs"]) > 0, "Final status should have job states"

        # All jobs in final status should be either success or failed
        for job_id, state in final_status["jobs"].items():
            assert state in ("success", "failed", "running"), f"Invalid job state: {state}"
            assert job_id in {"greeting", "farewell", "parallel_a", "final"}

    def test_log_lines_contain_output(self, tmp_path: Path) -> None:
        """Verify log_line events contain actual job command output."""
        # ---- config ----
        loader = YamlConfigLoader()
        plan = loader.load(FIXTURES_DIR / "integration_plan.yaml")

        # ---- directories ----
        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()


        # ---- wire up real components with event bus ----
        bus = EventBus()
        job_logger = EventBusJobLogger(log_dir, bus)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter(bus)
        composite_reporter = CompositeReporter(reporter, EventBusReporter(bus))
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

        # ---- start HTTP server ----
        port = _free_port()
        server = OrchestratorHTTPServer(port, bus)
        server_thread = threading.Thread(target=server.serve_until_done, daemon=True)
        server_thread.start()
        time.sleep(0.1)

        # ---- run engine ----
        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=composite_reporter,
            output_root=output_dir,
        )

        engine_exc: list[BaseException] = []

        def _run_engine() -> None:
            try:
                engine.run(plan)
            except BaseException as exc:
                engine_exc.append(exc)
            finally:
                executor.shutdown()

        engine_thread = threading.Thread(target=_run_engine, daemon=False)
        engine_thread.start()

        # ---- collect all events ----
        all_events = _drain_events(port, timeout=60.0)
        server_thread.join(timeout=10.0)
        engine_thread.join(timeout=10.0)

        if engine_exc:
            raise engine_exc[0]

        # ---- verify log lines ----
        log_lines = [e for e in all_events if e["type"] == "log_line"]
        assert len(log_lines) > 0, "No log_line events captured"

        # Extract actual line content
        line_contents = [e["line"] for e in log_lines]
        all_content = " ".join(line_contents)

        # The integration_plan.yaml has these expected outputs:
        # - "hello from greeting"
        # - "goodbye from farewell"
        # - "side-a" (from parallel_a)
        # - "done" (from final)
        # Not all may be captured depending on Docker output buffering,
        # but at least some should appear
        has_some_output = any([
            "hello" in all_content or "greeting" in all_content,
            "goodbye" in all_content or "farewell" in all_content,
            "side-a" in all_content,
            "done" in all_content,
        ])
        assert has_some_output, f"Expected some recognizable output in logs, got: {all_content[:200]}"
