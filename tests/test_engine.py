"""Tests for the Engine orchestration loop."""
from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import IO, override

import pytest

from orchestrator.artifact_store import ArtifactStoreABC
from orchestrator.engine import Engine
from orchestrator.exceptions import CyclicDependencyError
from orchestrator.executor import ExecutorABC
from orchestrator.logger import JobLoggerABC
from orchestrator.models import (
    ArtifactSpec,
    BuildPlan,
    FailurePolicy,
    JobResult,
    JobSpec,
    OrchestratorResult,
    ResourceWeight,
)
from orchestrator.pipeline import IPipelineReporter
from orchestrator.scheduler import ResourceScheduler


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeExecutor(ExecutorABC):
    """In-process executor that resolves futures via a thread pool.

    Supply a ``results`` dict mapping job_id → JobResult to control outcomes.
    Jobs not in the dict succeed by default.
    """

    def __init__(self, results: dict[str, JobResult] | None = None, delay: float = 0.0) -> None:
        self._results = results or {}
        self._delay = delay
        self._pool = ThreadPoolExecutor(max_workers=4)
        self.submitted: list[str] = []
        self.started = False
        self.stopped = False

    @override
    def start(self, plan: BuildPlan) -> None:
        self.started = True

    @override
    def submit(self, job: JobSpec) -> Future[JobResult]:
        self.submitted.append(job.id)
        return self._pool.submit(self._run, job)

    def _run(self, job: JobSpec) -> JobResult:
        if self._delay:
            time.sleep(self._delay)
        if job.id in self._results:
            return self._results[job.id]
        return JobResult(
            job_id=job.id,
            success=True,
            exit_code=0,
            duration_seconds=0.01,
            log_path=Path(f"/tmp/{job.id}.log"),
        )

    @override
    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    @override
    def stop(self) -> None:
        self.stopped = True


class FakeArtifactStore(ArtifactStoreABC):
    """Records collect/finalize calls without touching the filesystem."""

    def __init__(self) -> None:
        self.collected: list[tuple[str, str]] = []  # (job_id, success)
        self.finalized_to: Path | None = None

    @override
    def collect(self, job: JobSpec, result: JobResult) -> None:
        self.collected.append((job.id, "success" if result.success else "failure"))

    @override
    def finalize(self, output_root: Path) -> None:
        self.finalized_to = output_root


class FakeLogger(JobLoggerABC):
    @override
    def get_log_path(self, job_id: str) -> Path:
        return Path(f"/tmp/{job_id}.log")

    @override
    def open_stream(self, job_id: str) -> IO[str]:
        raise NotImplementedError

    @override
    def close_stream(self, job_id: str) -> None:
        pass


class FakeReporter(IPipelineReporter):
    def __init__(self) -> None:
        self.started: list[str] = []
        self.completed: list[tuple[str, bool]] = []
        self.final_result: OrchestratorResult | None = None

    @override
    def report_job_started(self, job_id: str) -> None:
        self.started.append(job_id)

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        self.completed.append((job_id, success))

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        self.final_result = result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _job(
    jid: str,
    depends_on: frozenset[str] | None = None,
    cpu: int = 1,
    mem: int = 1,
) -> JobSpec:
    return JobSpec(
        id=jid,
        image=f"registry/{jid}:latest",
        depends_on=depends_on or frozenset(),
        resource_weight=ResourceWeight(cpu_slots=cpu, memory_slots=mem),
        artifacts=[ArtifactSpec(source_glob="*.bin", destination_subdir=jid)],
    )


def _plan(
    jobs: list[JobSpec],
    policy: FailurePolicy = FailurePolicy.FAIL_FAST,
    max_parallel: int = 4,
    cpu: int = 8,
    mem: int = 8,
) -> BuildPlan:
    return BuildPlan(
        jobs=jobs,
        failure_policy=policy,
        max_parallel=max_parallel,
        total_cpu_slots=cpu,
        total_memory_slots=mem,
    )


def _fail_result(job_id: str, exit_code: int = 1) -> JobResult:
    return JobResult(
        job_id=job_id,
        success=False,
        exit_code=exit_code,
        duration_seconds=0.01,
        log_path=Path(f"/tmp/{job_id}.log"),
    )


def _build_engine(
    plan: BuildPlan,
    executor: FakeExecutor | None = None,
    artifact_store: FakeArtifactStore | None = None,
    reporter: FakeReporter | None = None,
    output_root: Path | None = None,
) -> tuple[Engine, FakeExecutor, FakeArtifactStore, FakeReporter]:
    ex = executor or FakeExecutor()
    ar = artifact_store or FakeArtifactStore()
    rp = reporter or FakeReporter()
    engine = Engine(
        scheduler=ResourceScheduler(plan),
        executor=ex,
        artifact_store=ar,
        job_logger=FakeLogger(),
        reporter=rp,
        output_root=output_root or Path("/output"),
    )
    return engine, ex, ar, rp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHappyPath:
    """All jobs succeed with no failures."""

    def test_single_job_succeeds(self) -> None:
        plan = _plan([_job("a")])
        engine, ex, ar, rp = _build_engine(plan)

        result = engine.run(plan)

        assert result.success is True
        assert len(result.job_results) == 1
        assert result.job_results[0].job_id == "a"
        assert ex.started is True
        assert ex.stopped is True

    def test_linear_chain(self) -> None:
        """a → b → c executed in dependency order."""
        a = _job("a")
        b = _job("b", depends_on=frozenset({"a"}))
        c = _job("c", depends_on=frozenset({"b"}))
        plan = _plan([a, b, c])
        engine, ex, ar, rp = _build_engine(plan)

        result = engine.run(plan)

        assert result.success is True
        assert len(result.job_results) == 3
        # All three should have been submitted
        assert set(ex.submitted) == {"a", "b", "c"}

    def test_independent_jobs_all_submitted(self) -> None:
        """Independent jobs can run in parallel."""
        plan = _plan([_job("a"), _job("b"), _job("c")])
        engine, ex, ar, rp = _build_engine(plan)

        result = engine.run(plan)

        assert result.success is True
        assert set(ex.submitted) == {"a", "b", "c"}

    def test_diamond_dependency(self) -> None:
        """a → (b, c) → d diamond pattern."""
        a = _job("a")
        b = _job("b", depends_on=frozenset({"a"}))
        c = _job("c", depends_on=frozenset({"a"}))
        d = _job("d", depends_on=frozenset({"b", "c"}))
        plan = _plan([a, b, c, d])
        engine, ex, ar, rp = _build_engine(plan)

        result = engine.run(plan)

        assert result.success is True
        assert len(result.job_results) == 4
        submitted_order = ex.submitted
        assert submitted_order.index("a") < submitted_order.index("b")
        assert submitted_order.index("a") < submitted_order.index("c")
        assert submitted_order.index("b") < submitted_order.index("d")
        assert submitted_order.index("c") < submitted_order.index("d")


class TestArtifactCollection:
    def test_artifacts_collected_for_successful_jobs(self) -> None:
        plan = _plan([_job("a"), _job("b")])
        engine, ex, ar, rp = _build_engine(plan)

        _ = engine.run(plan)

        collected_ids = [jid for jid, _ in ar.collected]
        assert set(collected_ids) == {"a", "b"}

    def test_artifacts_not_collected_for_failed_jobs(self) -> None:
        plan = _plan([_job("a")], policy=FailurePolicy.CONTINUE)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, _, ar, _ = _build_engine(plan, executor=executor)

        _ = engine.run(plan)

        assert ar.collected == []

    def test_finalize_called_with_output_root(self) -> None:
        plan = _plan([_job("a")])
        out = Path("/my/output")
        engine, _, ar, _ = _build_engine(plan, output_root=out)

        _ = engine.run(plan)

        assert ar.finalized_to == out

    def test_finalize_called_even_on_failure(self) -> None:
        plan = _plan([_job("a")], policy=FailurePolicy.CONTINUE)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, _, ar, _ = _build_engine(plan, executor=executor)

        _ = engine.run(plan)

        assert ar.finalized_to is not None


class TestReporting:
    def test_reporter_notified_of_job_lifecycle(self) -> None:
        plan = _plan([_job("a")])
        engine, _, _, rp = _build_engine(plan)

        _ = engine.run(plan)

        assert "a" in rp.started
        assert ("a", True) in rp.completed

    def test_reporter_receives_final_result(self) -> None:
        plan = _plan([_job("a")])
        engine, _, _, rp = _build_engine(plan)

        _ = engine.run(plan)

        assert rp.final_result is not None
        assert rp.final_result.success is True

    def test_failed_job_reported(self) -> None:
        plan = _plan([_job("a")], policy=FailurePolicy.CONTINUE)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, _, _, rp = _build_engine(plan, executor=executor)

        _ = engine.run(plan)

        assert ("a", False) in rp.completed
        assert rp.final_result is not None
        assert rp.final_result.success is False


class TestFailFast:
    def test_fail_fast_skips_dependent_jobs(self) -> None:
        """When a fails under fail-fast, b (depends on a) is skipped."""
        a = _job("a")
        b = _job("b", depends_on=frozenset({"a"}))
        plan = _plan([a, b], policy=FailurePolicy.FAIL_FAST)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, ex, _, rp = _build_engine(plan, executor=executor)

        result = engine.run(plan)

        assert result.success is False
        assert len(result.job_results) == 2
        result_map = {r.job_id: r for r in result.job_results}
        assert result_map["a"].success is False
        assert result_map["b"].success is False
        # b should never have been submitted to the executor
        assert "b" not in ex.submitted

    def test_fail_fast_does_not_cancel_already_running(self) -> None:
        """Already-running jobs finish even under fail-fast."""
        a = _job("a")
        b = _job("b")  # independent, can run in parallel
        c = _job("c", depends_on=frozenset({"a", "b"}))
        plan = _plan([a, b, c], policy=FailurePolicy.FAIL_FAST)
        # a fails, b succeeds (both independent so both submitted)
        executor = FakeExecutor(
            results={"a": _fail_result("a")},
            delay=0.05,
        )
        engine, ex, ar, _ = _build_engine(plan, executor=executor)

        result = engine.run(plan)

        assert result.success is False
        # Both a and b should have been submitted (independent)
        assert "a" in ex.submitted
        assert "b" in ex.submitted
        # c should be skipped (depends on failed a)
        assert "c" not in ex.submitted

    def test_fail_fast_returns_single_failure(self) -> None:
        plan = _plan([_job("a")], policy=FailurePolicy.FAIL_FAST)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, _, _, _ = _build_engine(plan, executor=executor)

        result = engine.run(plan)

        assert result.success is False


class TestContinuePolicy:
    def test_continue_runs_independent_jobs_after_failure(self) -> None:
        """Under continue policy, independent jobs still run after a failure."""
        a = _job("a")
        b = _job("b")  # independent of a
        plan = _plan([a, b], policy=FailurePolicy.CONTINUE)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, ex, _, _ = _build_engine(plan, executor=executor)

        result = engine.run(plan)

        assert result.success is False
        assert set(ex.submitted) == {"a", "b"}
        result_map = {r.job_id: r for r in result.job_results}
        assert result_map["a"].success is False
        assert result_map["b"].success is True

    def test_continue_skips_jobs_blocked_by_failure(self) -> None:
        """Even under continue, jobs whose deps failed are skipped."""
        a = _job("a")
        b = _job("b", depends_on=frozenset({"a"}))
        c = _job("c")  # independent
        plan = _plan([a, b, c], policy=FailurePolicy.CONTINUE)
        executor = FakeExecutor(results={"a": _fail_result("a")})
        engine, ex, _, _ = _build_engine(plan, executor=executor)

        result = engine.run(plan)

        assert result.success is False
        assert "b" not in ex.submitted
        assert "c" in ex.submitted
        result_map = {r.job_id: r for r in result.job_results}
        assert result_map["b"].success is False  # skipped
        assert result_map["c"].success is True


class TestResourceConstraints:
    def test_heavy_job_blocks_until_slots_free(self) -> None:
        """A job requiring more slots than available must wait."""
        # total budget: 2 cpu slots
        a = _job("a", cpu=2)
        b = _job("b", cpu=2, depends_on=frozenset({"a"}))
        plan = _plan([a, b], cpu=2, mem=8)
        engine, ex, _, _ = _build_engine(plan)

        result = engine.run(plan)

        assert result.success is True
        assert ex.submitted == ["a", "b"]


class TestEmptyPlan:
    def test_empty_plan_succeeds(self) -> None:
        plan = _plan([])
        engine, _, ar, rp = _build_engine(plan)

        result = engine.run(plan)

        assert result.success is True
        assert result.job_results == ()
        assert ar.finalized_to is not None
        assert rp.final_result is not None


class TestCyclicDependency:
    def test_cycle_raises(self) -> None:
        a = _job("a", depends_on=frozenset({"b"}))
        b = _job("b", depends_on=frozenset({"a"}))
        plan = _plan([a, b])
        engine, _, _, _ = _build_engine(plan)

        with pytest.raises(CyclicDependencyError):
            _  = engine.run(plan)
