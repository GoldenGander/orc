from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import main as main_module
from orchestrator.models import BuildPlan, FailurePolicy, JobResult, OrchestratorResult


class _FakeExecutor:
    def __init__(self) -> None:
        self.shutdown_called = False

    def shutdown(self) -> None:
        self.shutdown_called = True


class _FakeEngine:
    def __init__(self, **kwargs) -> None:
        self.executor = kwargs["executor"]

    def run(self, plan: BuildPlan) -> None:
        raise RuntimeError("boom")


def _base_args(tmp_path: Path, **overrides) -> SimpleNamespace:
    defaults = dict(
        config_path=tmp_path / "plan.yaml",
        output_dir=tmp_path / "output",
        source_dir=tmp_path / "source",
        dry_run=False,
        port=None,
        keep_logs=False,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _empty_plan() -> BuildPlan:
    return BuildPlan(
        jobs=[],
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=1,
        total_cpu_slots=1,
        total_memory_slots=1,
    )


def test_executor_shuts_down_when_engine_raises(tmp_path) -> None:
    plan = _empty_plan()
    args = _base_args(tmp_path)
    executors: list[_FakeExecutor] = []

    def _make_executor(*args, **kwargs):
        executor = _FakeExecutor()
        executors.append(executor)
        return executor

    with (
        patch.object(main_module, "_parse_args", return_value=args),
        patch.object(main_module.YamlConfigLoader, "load", return_value=plan),
        patch.object(main_module, "DockerExecutor", side_effect=_make_executor),
        patch.object(main_module, "Engine", side_effect=lambda **kwargs: _FakeEngine(**kwargs)),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            main_module.main()

    assert executors
    assert executors[0].shutdown_called is True


def _make_fake_logger_class(write_log: bool = True):
    """Return a FileJobLogger replacement that optionally creates a log file."""

    class _FakeFileJobLogger:
        def __init__(self, log_dir: Path) -> None:
            if write_log:
                log_dir.mkdir(parents=True, exist_ok=True)
                (log_dir / "job1.log").write_text("job output\n")

    return _FakeFileJobLogger


def _engine_returning(result: OrchestratorResult):
    class _E:
        def __init__(self, **kwargs) -> None:
            pass

        def run(self, plan: BuildPlan) -> OrchestratorResult:
            return result

    return lambda **kwargs: _E(**kwargs)


def test_logs_copied_to_output_dir_on_failure(tmp_path) -> None:
    plan = _empty_plan()
    output_dir = tmp_path / "output"
    args = _base_args(tmp_path, output_dir=output_dir, keep_logs=False)

    failed_result = OrchestratorResult(
        success=False,
        job_results=(
            JobResult(job_id="job1", success=False, exit_code=1, duration_seconds=1.0, log_path=Path("job1.log")),
        ),
    )
    fake_executor = SimpleNamespace(shutdown=lambda: None)

    with (
        patch.object(main_module, "_parse_args", return_value=args),
        patch.object(main_module.YamlConfigLoader, "load", return_value=plan),
        patch.object(main_module, "DockerExecutor", return_value=fake_executor),
        patch.object(main_module, "FileJobLogger", side_effect=_make_fake_logger_class()),
        patch.object(main_module, "Engine", side_effect=_engine_returning(failed_result)),
    ):
        with pytest.raises(SystemExit):
            main_module.main()

    assert (output_dir / "logs" / "job1.log").exists()


def test_logs_copied_to_output_dir_when_keep_logs_set(tmp_path) -> None:
    plan = _empty_plan()
    output_dir = tmp_path / "output"
    args = _base_args(tmp_path, output_dir=output_dir, keep_logs=True)

    success_result = OrchestratorResult(
        success=True,
        job_results=(
            JobResult(job_id="job1", success=True, exit_code=0, duration_seconds=1.0, log_path=Path("job1.log")),
        ),
    )
    fake_executor = SimpleNamespace(shutdown=lambda: None)

    with (
        patch.object(main_module, "_parse_args", return_value=args),
        patch.object(main_module.YamlConfigLoader, "load", return_value=plan),
        patch.object(main_module, "DockerExecutor", return_value=fake_executor),
        patch.object(main_module, "FileJobLogger", side_effect=_make_fake_logger_class()),
        patch.object(main_module, "Engine", side_effect=_engine_returning(success_result)),
    ):
        main_module.main()

    assert (output_dir / "logs" / "job1.log").exists()


def test_logs_not_copied_on_success_without_keep_logs(tmp_path) -> None:
    plan = _empty_plan()
    output_dir = tmp_path / "output"
    args = _base_args(tmp_path, output_dir=output_dir, keep_logs=False)

    success_result = OrchestratorResult(
        success=True,
        job_results=(
            JobResult(job_id="job1", success=True, exit_code=0, duration_seconds=1.0, log_path=Path("job1.log")),
        ),
    )
    fake_executor = SimpleNamespace(shutdown=lambda: None)

    with (
        patch.object(main_module, "_parse_args", return_value=args),
        patch.object(main_module.YamlConfigLoader, "load", return_value=plan),
        patch.object(main_module, "DockerExecutor", return_value=fake_executor),
        patch.object(main_module, "FileJobLogger", side_effect=_make_fake_logger_class()),
        patch.object(main_module, "Engine", side_effect=_engine_returning(success_result)),
    ):
        main_module.main()

    assert not (output_dir / "logs").exists()
