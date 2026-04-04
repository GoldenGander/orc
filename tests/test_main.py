from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

import main as main_module
from orchestrator.models import BuildPlan, FailurePolicy


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


def test_executor_shuts_down_when_engine_raises(tmp_path) -> None:
    plan = BuildPlan(
        jobs=[],
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=1,
        total_cpu_slots=1,
        total_memory_slots=1,
    )
    args = SimpleNamespace(
        config_path=tmp_path / "plan.yaml",
        output_dir=tmp_path / "output",
        source_dir=tmp_path / "source",
        dry_run=False,
    )
    executors: list[_FakeExecutor] = []

    def _make_executor(*args, **kwargs):
        executor = _FakeExecutor()
        executors.append(executor)
        return executor

    with (
        patch.object(main_module, "_parse_args", return_value=args),
        patch.object(main_module.YamlConfigLoader, "load", return_value=plan),
        patch.object(main_module, "prepare_volumes"),
        patch.object(main_module, "DockerExecutor", side_effect=_make_executor),
        patch.object(main_module, "Engine", side_effect=lambda **kwargs: _FakeEngine(**kwargs)),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            main_module.main()

    assert executors
    assert executors[0].shutdown_called is True
