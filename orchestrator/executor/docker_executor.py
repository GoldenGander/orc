from __future__ import annotations

import io
import json
import logging
import subprocess
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import override

from orchestrator.exceptions import ConfigurationError
from orchestrator.executor.executor import ExecutorABC
from orchestrator.logger.logger import JobLoggerABC
from orchestrator.models import (
    BuildPlan,
    JobResult,
    JobSpec,
    ResourceDriver,
    ResourceSpec,
)
from orchestrator.volume_prep import compute_job_volumes, compute_resource_output_volume

logger = logging.getLogger(__name__)


class DockerExecutor(ExecutorABC):
    """Executes build jobs as Docker containers via the Docker CLI.

    Each call to submit() runs ``docker run`` in a worker thread, streaming
    container output to the job's log file. The Future resolves with a
    JobResult once the container exits.
    """

    def __init__(
        self,
        logger: JobLoggerABC,
        source_dir: Path,
        container_output_root: Path,
        max_workers: int = 4,
    ) -> None:
        self._logger = logger
        self._source_dir = source_dir
        self._container_output_root = container_output_root
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._network: str | None = None
        self._default_job_timeout_seconds: int | None = 3600
        self._network_created = False
        self._service_container_names: list[str] = []
        self._service_ready_poll_seconds = 0.5
        self._service_ready_timeout_seconds = 30.0
        self._service_startup_stability_seconds = 1.0
        self._file_shares: dict[str, ResourceSpec] = {}

    @override
    def start(self, plan: BuildPlan) -> None:
        self._network = plan.resource_network
        self._default_job_timeout_seconds = plan.job_timeout_seconds
        self._file_shares = {
            r.id: r for r in plan.resources if r.driver == ResourceDriver.FILE_SHARE
        }
        managed_container_resources: list[ResourceSpec] = []
        for resource in plan.resources:
            if resource.driver == ResourceDriver.DOCKER_CONTAINER:
                managed_container_resources.append(resource)
            elif resource.driver == ResourceDriver.FILE_SHARE:
                logger.info(
                    "File share '%s' at host path '%s' → container path '%s'",
                    resource.id,
                    resource.host_path,
                    resource.container_path,
                )
            else:
                raise ConfigurationError(
                    f"Resource '{resource.id}': DockerExecutor does not support "
                    f"driver='{resource.driver.value}'"
                )
        if managed_container_resources and self._network is None:
            self._network = f"orch_resources_{uuid.uuid4().hex[:10]}"
        if self._network is not None:
            self._ensure_network(self._network)
        for resource in managed_container_resources:
            container_name = self._start_resource(resource)
            self._wait_for_resource_ready(container_name, resource.id)

    @override
    def stop(self) -> None:
        for container_name in reversed(self._service_container_names):
            subprocess.run(
                ["docker", "stop", container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        self._service_container_names = []

        if self._network_created and self._network is not None:
            subprocess.run(
                ["docker", "network", "rm", self._network],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            self._network_created = False

    @override
    def submit(self, job: JobSpec) -> Future[JobResult]:
        return self._pool.submit(self._run_container, job)

    @override
    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    def _build_docker_command(
        self, job: JobSpec, container_name: str | None = None
    ) -> list[str]:
        cmd: list[str] = ["docker", "run", "--rm"]
        if container_name is None:
            container_name = self._make_job_container_name()
        cmd.extend(["--name", container_name])
        if self._network is not None:
            cmd.extend(["--network", self._network])
        system_vols = compute_job_volumes(
            job, self._source_dir, self._container_output_root, self._file_shares
        )
        for vol in list(job.volumes) + system_vols:
            mount = f"{vol.host_path}:{vol.container_path}"
            if vol.read_only:
                mount += ":ro"
            cmd.extend(["-v", mount])
        for key, value in job.env_vars.items():
            cmd.extend(["-e", f"{key}={value}"])
        cmd.append(job.image)
        if job.command is not None:
            cmd.extend(job.command)
        return cmd

    def _job_timeout_seconds(self, job: JobSpec) -> int | None:
        if job.timeout_seconds is not None:
            return job.timeout_seconds
        return self._default_job_timeout_seconds

    @staticmethod
    def _make_job_container_name() -> str:
        return f"orch_job_{uuid.uuid4().hex[:12]}"

    def _run_container(self, job: JobSpec) -> JobResult:
        log_path = self._logger.get_log_path(job.id)
        stream = self._logger.open_stream(job.id)
        container_name = self._make_job_container_name()
        timeout_seconds = self._job_timeout_seconds(job)
        start = time.monotonic()
        try:
            # Streams with no OS file descriptor (e.g. TeeStream) cannot be
            # passed directly to subprocess — use PIPE and write output after.
            try:
                stream.fileno()
                has_fd = True
            except io.UnsupportedOperation:
                has_fd = False

            cmd = self._build_docker_command(job, container_name)
            if has_fd:
                result = subprocess.run(
                    cmd,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=timeout_seconds,
                )
            else:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=timeout_seconds,
                )
                if result.stdout:
                    stream.write(result.stdout)

            duration = time.monotonic() - start
            return JobResult(
                job_id=job.id,
                success=result.returncode == 0,
                exit_code=result.returncode,
                duration_seconds=duration,
                log_path=log_path,
            )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - start
            stream.write(
                f"\n--- job timed out after {timeout_seconds} seconds ---\n"
            )
            stream.flush()
            try:
                subprocess.run(
                    ["docker", "stop", container_name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception as exc:
                stream.write(f"\n--- timeout cleanup failed ---\n{exc}\n")
            return JobResult(
                job_id=job.id,
                success=False,
                exit_code=-124,
                duration_seconds=duration,
                log_path=log_path,
            )
        except Exception as exc:
            duration = time.monotonic() - start
            stream.write(f"\n--- executor error ---\n{exc}\n")
            return JobResult(
                job_id=job.id,
                success=False,
                exit_code=-1,
                duration_seconds=duration,
                log_path=log_path,
            )
        finally:
            self._logger.close_stream(job.id)

    def _ensure_network(self, network_name: str) -> None:
        inspected = subprocess.run(
            ["docker", "network", "inspect", network_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if inspected.returncode == 0:
            return

        subprocess.run(
            ["docker", "network", "create", network_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        self._network_created = True
        logger.info("Created docker network '%s'", network_name)

    def _start_resource(self, resource: ResourceSpec) -> str:
        if self._network is None:
            raise ConfigurationError("managed docker_container resources require a network")

        if resource.image is None:
            raise ConfigurationError(
                f"Resource '{resource.id}' must declare image for managed docker_container driver"
            )

        container_name = f"orch_resource_{resource.id}_{uuid.uuid4().hex[:10]}"
        cmd: list[str] = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "--network",
            self._network,
        ]
        for alias in resource.aliases:
            cmd.extend(["--network-alias", alias])
        output_vol = compute_resource_output_volume(resource, self._container_output_root)
        for vol in list(resource.volumes) + [output_vol]:
            mount = f"{vol.host_path}:{vol.container_path}"
            if vol.read_only:
                mount += ":ro"
            cmd.extend(["-v", mount])
        for key, value in resource.env_vars.items():
            cmd.extend(["-e", f"{key}={value}"])
        cmd.append(resource.image)
        if resource.command is not None:
            cmd.extend(resource.command)

        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        self._service_container_names.append(container_name)
        logger.info(
            "Started resource container '%s' (resource=%s, image=%s)",
            container_name,
            resource.id,
            resource.image,
        )
        return container_name

    def _wait_for_resource_ready(self, container_name: str, resource_id: str) -> None:
        """Wait for a managed resource container to be ready before dispatching jobs."""

        max_attempts = max(
            1, int(self._service_ready_timeout_seconds / self._service_ready_poll_seconds)
        )
        stable_attempts = max(
            1,
            int(self._service_startup_stability_seconds / self._service_ready_poll_seconds),
        )
        running_streak = 0
        last_state = "unknown"

        for attempt in range(max_attempts):
            state = self._inspect_container_state(container_name)
            health = state.get("Health")
            if isinstance(health, dict):
                health_status = str(health.get("Status", "unknown"))
                last_state = f"health={health_status}"
                if health_status == "healthy":
                    return
                if health_status == "unhealthy":
                    raise RuntimeError(
                        f"Resource '{resource_id}' container '{container_name}' reported unhealthy"
                    )
            else:
                status = str(state.get("Status", "unknown"))
                last_state = status
                if status == "running":
                    running_streak += 1
                    if running_streak >= stable_attempts:
                        return
                elif status in {"exited", "dead"}:
                    raise RuntimeError(
                        f"Resource '{resource_id}' container '{container_name}' exited during startup"
                    )
                else:
                    running_streak = 0

            if attempt < max_attempts - 1:
                time.sleep(self._service_ready_poll_seconds)

        raise RuntimeError(
            f"Resource '{resource_id}' container '{container_name}' did not become ready "
            f"within {self._service_ready_timeout_seconds:.1f}s (last state: {last_state})"
        )

    def _inspect_container_state(self, container_name: str) -> dict[str, object]:
        try:
            result = subprocess.run(
                ["docker", "inspect", container_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Failed to inspect service container '{container_name}': {exc}"
            ) from exc

        try:
            payload = json.loads(result.stdout)
            state = payload[0]["State"]
        except (IndexError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Failed to parse Docker inspect output for container '{container_name}'"
            ) from exc

        if not isinstance(state, dict):
            raise RuntimeError(
                f"Failed to parse Docker inspect output for container '{container_name}'"
            )
        return state
