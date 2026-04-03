"""Tests for ExecutorABC contract.

These tests verify the interface contract — any conforming ExecutorABC
implementation must satisfy them. The Docker subprocess layer is patched
so tests run without a Docker daemon.
"""
from __future__ import annotations

import subprocess
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import patch

import pytest

from orchestrator.executor import DockerExecutor, ExecutorABC
from orchestrator.exceptions import ConfigurationError
from orchestrator.logger import FileJobLogger
from orchestrator.models import (
    ArtifactSpec,
    BuildPlan,
    FailurePolicy,
    JobResult,
    JobSpec,
    ResourceWeight,
    ServiceSpec,
    VolumeMount,
)


@pytest.fixture
def job_logger(tmp_path: Path) -> FileJobLogger:
    return FileJobLogger(tmp_path / "logs")


@pytest.fixture
def executor(job_logger: FileJobLogger) -> DockerExecutor:
    ex = DockerExecutor(logger=job_logger, max_workers=2)
    yield ex
    ex.shutdown(wait=True)


@pytest.fixture
def job_a() -> JobSpec:
    return JobSpec(
        id="job_a",
        image="registry/builder:latest",
        depends_on=frozenset(),
        resource_weight=ResourceWeight(cpu_slots=1, memory_slots=1),
        artifacts=[ArtifactSpec(source_glob="**/out/*", destination_subdir="out")],
        command=["--build", "project_a"],
        env_vars={"CI": "true", "BUILD_NUM": "42"},
    )


@pytest.fixture
def job_no_command() -> JobSpec:
    return JobSpec(
        id="job_no_cmd",
        image="registry/default-entry:latest",
        depends_on=frozenset(),
        resource_weight=ResourceWeight(),
        artifacts=[],
    )


def _fake_run_success(cmd, **kwargs):
    stdout = kwargs.get("stdout")
    if stdout and hasattr(stdout, "write"):
        stdout.write("build output\n")
    return subprocess.CompletedProcess(cmd, returncode=0)


def _fake_run_failure(cmd, **kwargs):
    stdout = kwargs.get("stdout")
    if stdout and hasattr(stdout, "write"):
        stdout.write("error: compilation failed\n")
    return subprocess.CompletedProcess(cmd, returncode=1)


def _fake_run_crash(cmd, **kwargs):
    raise OSError("docker not found")


# --- Interface contract tests ---


class TestSubmitReturnsContract:
    """submit() must return a Future[JobResult]."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_returns_future(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        future = executor.submit(job_a)
        assert isinstance(future, Future)

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_future_resolves_to_job_result(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        assert isinstance(result, JobResult)


class TestSuccessfulJob:
    """A container that exits 0 must produce a successful JobResult."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_success_flag(self, _mock_run, executor: ExecutorABC, job_a: JobSpec):
        result = executor.submit(job_a).result(timeout=5)
        assert result.success is True

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_exit_code_zero(self, _mock_run, executor: ExecutorABC, job_a: JobSpec):
        result = executor.submit(job_a).result(timeout=5)
        assert result.exit_code == 0

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_job_id_matches(self, _mock_run, executor: ExecutorABC, job_a: JobSpec):
        result = executor.submit(job_a).result(timeout=5)
        assert result.job_id == "job_a"

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_duration_is_positive(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        assert result.duration_seconds >= 0

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_log_path_exists(self, _mock_run, executor: ExecutorABC, job_a: JobSpec):
        result = executor.submit(job_a).result(timeout=5)
        assert result.log_path.exists()

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_log_contains_output(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        assert "build output" in result.log_path.read_text()


class TestFailedJob:
    """A container that exits non-zero must produce a failed JobResult."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_failure)
    def test_failure_flag(self, _mock_run, executor: ExecutorABC, job_a: JobSpec):
        result = executor.submit(job_a).result(timeout=5)
        assert result.success is False

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_failure)
    def test_exit_code_nonzero(self, _mock_run, executor: ExecutorABC, job_a: JobSpec):
        result = executor.submit(job_a).result(timeout=5)
        assert result.exit_code != 0

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_failure)
    def test_log_contains_error_output(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        assert "compilation failed" in result.log_path.read_text()


class TestExecutorError:
    """If the container runtime itself fails, the result must still be a
    valid JobResult indicating failure — never an unhandled exception."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_crash)
    def test_crash_returns_failed_result(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        assert result.success is False

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_crash)
    def test_crash_exit_code(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        assert result.exit_code != 0

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_crash)
    def test_crash_log_records_error(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        result = executor.submit(job_a).result(timeout=5)
        log_text = result.log_path.read_text()
        assert "docker not found" in log_text


class TestNoCommand:
    """When command is None, the executor must omit extra args so Docker
    uses the image's default entrypoint."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_no_command_succeeds(
        self, mock_run, executor: ExecutorABC, job_no_command: JobSpec
    ):
        result = executor.submit(job_no_command).result(timeout=5)
        assert result.success is True
        # The actual docker command should end with just the image name
        invoked_cmd = mock_run.call_args[0][0]
        assert invoked_cmd[-1] == job_no_command.image


class TestEnvVars:
    """Environment variables declared on the job must be passed to the
    container via -e flags."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_env_vars_passed(
        self, mock_run, executor: ExecutorABC, job_a: JobSpec
    ):
        executor.submit(job_a).result(timeout=5)
        invoked_cmd = mock_run.call_args[0][0]
        assert "-e" in invoked_cmd
        assert "CI=true" in invoked_cmd
        assert "BUILD_NUM=42" in invoked_cmd


class TestConcurrentSubmission:
    """Multiple jobs submitted concurrently must all resolve independently."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_parallel_jobs_all_resolve(
        self, _mock_run, executor: ExecutorABC, job_a: JobSpec, job_no_command: JobSpec
    ):
        futures = [executor.submit(job_a), executor.submit(job_no_command)]
        results = [f.result(timeout=5) for f in futures]
        assert len(results) == 2
        job_ids = {r.job_id for r in results}
        assert job_ids == {"job_a", "job_no_cmd"}


class TestShutdown:
    """shutdown() must not raise and must allow pending work to complete
    when wait=True."""

    @patch("orchestrator.executor.docker_executor.subprocess.run", side_effect=_fake_run_success)
    def test_shutdown_after_submit(
        self, _mock_run, job_logger: FileJobLogger, job_a: JobSpec
    ):
        ex = DockerExecutor(logger=job_logger, max_workers=1)
        future = ex.submit(job_a)
        ex.shutdown(wait=True)
        assert future.done()
        assert future.result().success is True


class TestPipelineLifecycle:
    def _plan(
        self,
        *,
        network: str | None = None,
        services: list[ServiceSpec] | None = None,
    ) -> BuildPlan:
        return BuildPlan(
            jobs=[],
            failure_policy=FailurePolicy.FAIL_FAST,
            max_parallel=2,
            total_cpu_slots=2,
            total_memory_slots=2,
            network=network,
            services=services or [],
        )

    def test_start_adds_network_to_job_runs(
        self, executor: DockerExecutor, job_no_command: JobSpec
    ) -> None:
        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            return_value=subprocess.CompletedProcess(["docker"], 0),
        ):
            executor.start(self._plan(network="build-net"))

        cmd = executor._build_docker_command(job_no_command)
        assert "--network" in cmd
        net_index = cmd.index("--network")
        assert cmd[net_index + 1] == "build-net"

    def test_start_services_without_network_raises(
        self, executor: DockerExecutor
    ) -> None:
        plan = self._plan(
            services=[ServiceSpec(
                id="redis",
                image="redis:7-alpine",
                aliases=["redis"],
                command=None,
                volumes=[],
            )]
        )
        with pytest.raises(ConfigurationError, match="services require network"):
            executor.start(plan)

    def test_start_creates_network_and_starts_service_containers(
        self, executor: DockerExecutor
    ) -> None:
        redis = ServiceSpec(
            id="redis",
            image="redis:7-alpine",
            aliases=["redis"],
            command=["redis-server", "--appendonly", "yes"],
            volumes=[
                VolumeMount(
                    host_path="/host/cache",
                    container_path="/data",
                    read_only=False,
                )
            ],
            env_vars={"REDIS_PASSWORD": "secret"},
        )
        queue = ServiceSpec(
            id="queue",
            image="memcached:1.6",
            aliases=["cache-queue"],
        )
        plan = self._plan(network="build-net", services=[redis, queue])

        calls: list[list[str]] = []

        def _fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:4] == ["docker", "network", "inspect", "build-net"]:
                return subprocess.CompletedProcess(cmd, 1)
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ):
            executor.start(plan)

        assert ["docker", "network", "inspect", "build-net"] in calls
        assert ["docker", "network", "create", "build-net"] in calls
        run_cmds = [cmd for cmd in calls if cmd[:3] == ["docker", "run", "-d"]]
        assert len(run_cmds) == 2
        redis_cmd = next(cmd for cmd in run_cmds if "redis:7-alpine" in cmd)
        assert "--network" in redis_cmd
        assert "build-net" in redis_cmd
        assert "--network-alias" in redis_cmd
        assert "redis" in redis_cmd
        assert "-v" in redis_cmd
        assert "/host/cache:/data" in redis_cmd

    def test_stop_stops_services_and_removes_created_network(
        self, executor: DockerExecutor
    ) -> None:
        services = [
            ServiceSpec(id="redis", image="redis:7-alpine", aliases=["redis"]),
            ServiceSpec(id="queue", image="memcached:1.6", aliases=["cache-queue"]),
        ]
        plan = self._plan(network="build-net", services=services)

        calls: list[list[str]] = []

        def _fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:4] == ["docker", "network", "inspect", "build-net"]:
                return subprocess.CompletedProcess(cmd, 1)
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ):
            executor.start(plan)
            executor.stop()

        stop_calls = [cmd for cmd in calls if cmd[:2] == ["docker", "stop"]]
        assert len(stop_calls) == 2
        for call in stop_calls:
            assert len(call) == 3
            assert call[2].startswith("orch_service_")
        assert ["docker", "network", "rm", "build-net"] in calls
