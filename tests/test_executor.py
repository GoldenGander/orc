"""Tests for ExecutorABC contract.

These tests verify the interface contract — any conforming ExecutorABC
implementation must satisfy them. The Docker subprocess layer is patched
so tests run without a Docker daemon.
"""
from __future__ import annotations

import json
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
    ResourceDriver,
    ResourceSpec,
    ResourceWeight,
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


class TestJobTimeouts:
    """The executor must bound hung jobs and stop their containers."""

    @patch("orchestrator.executor.docker_executor.uuid.uuid4")
    @patch("orchestrator.executor.docker_executor.subprocess.run")
    def test_timeout_returns_failed_result_and_stops_container(
        self,
        mock_run,
        mock_uuid,
        executor: ExecutorABC,
        job_logger: FileJobLogger,
    ) -> None:
        mock_uuid.return_value = type("U", (), {"hex": "deadbeefcafebabe"})()

        def _side_effect(cmd, **kwargs):
            if cmd[:2] == ["docker", "stop"]:
                return subprocess.CompletedProcess(cmd, 0)
            raise subprocess.TimeoutExpired(cmd, timeout=kwargs.get("timeout"))

        mock_run.side_effect = _side_effect

        timed_job = JobSpec(
            id="job_timeout",
            image="registry/timed:latest",
            depends_on=frozenset(),
            resource_weight=ResourceWeight(),
            artifacts=[],
            timeout_seconds=5,
        )

        result = executor.submit(timed_job).result(timeout=5)

        assert result.success is False
        assert result.exit_code == -124
        assert result.log_path.exists()
        assert "timed out after 5 seconds" in result.log_path.read_text()
        assert mock_run.call_args_list[0].args[0][:3] == ["docker", "run", "--rm"]
        assert mock_run.call_args_list[0].kwargs["timeout"] == 5
        assert mock_run.call_args_list[1].args[0][:2] == ["docker", "stop"]
        assert mock_run.call_args_list[1].args[0][2] == "orch_job_deadbeefcafe"


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
        resource_network: str | None = None,
        resources: list[ResourceSpec] | None = None,
    ) -> BuildPlan:
        return BuildPlan(
            jobs=[],
            failure_policy=FailurePolicy.FAIL_FAST,
            max_parallel=2,
            total_cpu_slots=2,
            total_memory_slots=2,
            resource_network=resource_network,
            resources=resources or [],
        )

    def test_start_adds_network_to_job_runs(
        self, executor: DockerExecutor, job_no_command: JobSpec
    ) -> None:
        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            return_value=subprocess.CompletedProcess(["docker"], 0),
        ):
            executor.start(self._plan(resource_network="build-net"))

        cmd = executor._build_docker_command(job_no_command, "orch_job_test")
        assert "--network" in cmd
        net_index = cmd.index("--network")
        assert cmd[net_index + 1] == "build-net"
        assert "--name" in cmd
        name_index = cmd.index("--name")
        assert cmd[name_index + 1] == "orch_job_test"

    def test_start_managed_resources_without_network_autocreates_network(
        self, executor: DockerExecutor
    ) -> None:
        plan = self._plan(
            resources=[
                ResourceSpec(
                    id="redis",
                    kind="cache",
                    driver=ResourceDriver.DOCKER_CONTAINER,
                    image="redis:7-alpine",
                    aliases=["redis"],
                )
            ]
        )
        calls: list[list[str]] = []

        def _fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:3] == ["docker", "network", "inspect"]:
                return subprocess.CompletedProcess(cmd, 1)
            if cmd[:2] == ["docker", "inspect"]:
                payload = [{"State": {"Status": "running"}}]
                return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload))
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ), patch("orchestrator.executor.docker_executor.time.sleep", return_value=None):
            executor.start(plan)
        assert executor._network is not None
        assert executor._network_created is True
        assert any(cmd[:3] == ["docker", "run", "-d"] for cmd in calls)

    def test_start_creates_network_and_starts_resource_containers(
        self, executor: DockerExecutor
    ) -> None:
        redis = ResourceSpec(
            id="redis",
            kind="cache",
            driver=ResourceDriver.DOCKER_CONTAINER,
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
        queue = ResourceSpec(
            id="queue",
            kind="cache",
            driver=ResourceDriver.DOCKER_CONTAINER,
            image="memcached:1.6",
            aliases=["cache-queue"],
        )
        plan = self._plan(resource_network="build-net", resources=[redis, queue])

        calls: list[list[str]] = []
        inspect_calls = 0

        def _fake_run(cmd, **kwargs):
            nonlocal inspect_calls
            calls.append(cmd)
            if cmd[:4] == ["docker", "network", "inspect", "build-net"]:
                return subprocess.CompletedProcess(cmd, 1)
            if cmd[:2] == ["docker", "inspect"]:
                inspect_calls += 1
                payload = [{"State": {"Status": "running"}}]
                return subprocess.CompletedProcess(
                    cmd, 0, stdout=json.dumps(payload)
                )
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ), patch("orchestrator.executor.docker_executor.time.sleep", return_value=None):
            executor.start(plan)

        assert ["docker", "network", "inspect", "build-net"] in calls
        assert ["docker", "network", "create", "build-net"] in calls
        run_cmds = [cmd for cmd in calls if cmd[:3] == ["docker", "run", "-d"]]
        assert len(run_cmds) == 2
        inspect_cmds = [cmd for cmd in calls if cmd[:2] == ["docker", "inspect"]]
        assert len(inspect_cmds) == 4
        assert inspect_calls == 4
        redis_cmd = next(cmd for cmd in run_cmds if "redis:7-alpine" in cmd)
        assert "--network" in redis_cmd
        assert "build-net" in redis_cmd
        assert "--network-alias" in redis_cmd
        assert "redis" in redis_cmd
        assert "-v" in redis_cmd
        assert "/host/cache:/data" in redis_cmd

    def test_start_waits_for_healthcheck_before_dispatching_jobs(
        self, executor: DockerExecutor
    ) -> None:
        resource = ResourceSpec(
            id="redis",
            kind="cache",
            driver=ResourceDriver.DOCKER_CONTAINER,
            image="redis:7-alpine",
            aliases=["redis"],
        )
        plan = self._plan(resource_network="build-net", resources=[resource])

        calls: list[list[str]] = []
        inspect_statuses = iter(["starting", "healthy"])

        def _fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:4] == ["docker", "network", "inspect", "build-net"]:
                return subprocess.CompletedProcess(cmd, 1)
            if cmd[:2] == ["docker", "inspect"]:
                health_status = next(inspect_statuses)
                payload = [
                    {
                        "State": {
                            "Status": "running",
                            "Health": {"Status": health_status},
                        }
                    }
                ]
                return subprocess.CompletedProcess(
                    cmd, 0, stdout=json.dumps(payload)
                )
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ), patch("orchestrator.executor.docker_executor.time.sleep", return_value=None):
            executor.start(plan)

        inspect_cmds = [cmd for cmd in calls if cmd[:2] == ["docker", "inspect"]]
        assert len(inspect_cmds) == 2
        run_index = calls.index(next(cmd for cmd in calls if cmd[:3] == ["docker", "run", "-d"]))
        inspect_index = calls.index(inspect_cmds[0])
        assert run_index < inspect_index

    def test_stop_stops_resources_and_removes_created_network(
        self, executor: DockerExecutor
    ) -> None:
        resources = [
            ResourceSpec(
                id="redis",
                kind="cache",
                driver=ResourceDriver.DOCKER_CONTAINER,
                image="redis:7-alpine",
                aliases=["redis"],
            ),
            ResourceSpec(
                id="queue",
                kind="cache",
                driver=ResourceDriver.DOCKER_CONTAINER,
                image="memcached:1.6",
                aliases=["cache-queue"],
            ),
        ]
        plan = self._plan(resource_network="build-net", resources=resources)

        calls: list[list[str]] = []

        def _fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:4] == ["docker", "network", "inspect", "build-net"]:
                return subprocess.CompletedProcess(cmd, 1)
            if cmd[:2] == ["docker", "inspect"]:
                payload = [{"State": {"Status": "running"}}]
                return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload))
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ), patch("orchestrator.executor.docker_executor.time.sleep", return_value=None):
            executor.start(plan)
            executor.stop()

        stop_calls = [cmd for cmd in calls if cmd[:2] == ["docker", "stop"]]
        assert len(stop_calls) == 2
        for call in stop_calls:
            assert len(call) == 3
            assert call[2].startswith("orch_resource_")
        assert ["docker", "network", "rm", "build-net"] in calls

    def test_start_file_share_resource_issues_no_docker_commands(
        self, executor: DockerExecutor
    ) -> None:
        plan = self._plan(
            resources=[
                ResourceSpec(
                    id="boost",
                    kind="library",
                    driver=ResourceDriver.FILE_SHARE,
                    host_path="/mnt/boost",
                    container_path="/opt/boost",
                    aliases=[],
                )
            ]
        )
        calls: list[list[str]] = []

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=lambda cmd, **kwargs: calls.append(cmd),
        ):
            executor.start(plan)

        assert calls == [], "file_share resources must not trigger any docker commands"

    def test_start_file_share_alongside_docker_container_resource(
        self, executor: DockerExecutor
    ) -> None:
        """File share and docker_container resources coexist: only the container triggers docker commands."""
        plan = self._plan(
            resource_network="build-net",
            resources=[
                ResourceSpec(
                    id="redis",
                    kind="cache",
                    driver=ResourceDriver.DOCKER_CONTAINER,
                    image="redis:7-alpine",
                    aliases=["redis"],
                ),
                ResourceSpec(
                    id="boost",
                    kind="library",
                    driver=ResourceDriver.FILE_SHARE,
                    host_path="/mnt/boost",
                    container_path="/opt/boost",
                    aliases=[],
                ),
            ],
        )
        calls: list[list[str]] = []

        def _fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:3] == ["docker", "network", "inspect"]:
                return subprocess.CompletedProcess(cmd, 0)
            if cmd[:2] == ["docker", "inspect"]:
                payload = [{"State": {"Status": "running"}}]
                return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload))
            return subprocess.CompletedProcess(cmd, 0)

        with patch(
            "orchestrator.executor.docker_executor.subprocess.run",
            side_effect=_fake_run,
        ), patch("orchestrator.executor.docker_executor.time.sleep", return_value=None):
            executor.start(plan)

        docker_run_calls = [cmd for cmd in calls if cmd[:3] == ["docker", "run", "-d"]]
        assert len(docker_run_calls) == 1, "only the redis container should be started"
        assert any("redis:7-alpine" in str(cmd) for cmd in docker_run_calls)

