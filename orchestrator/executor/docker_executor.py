from __future__ import annotations

import logging
import subprocess
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import override

from orchestrator.exceptions import ConfigurationError
from orchestrator.executor.executor import ExecutorABC
from orchestrator.logger.logger import JobLoggerABC
from orchestrator.models import BuildPlan, JobResult, JobSpec, ServiceSpec

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
        max_workers: int = 4,
    ) -> None:
        self._logger = logger
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._network: str | None = None
        self._network_created = False
        self._service_container_names: list[str] = []

    @override
    def start(self, plan: BuildPlan) -> None:
        self._network = plan.network
        if self._network is not None:
            self._ensure_network(self._network)
        for service in plan.services:
            self._start_service(service)

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

    def _build_docker_command(self, job: JobSpec) -> list[str]:
        cmd: list[str] = ["docker", "run", "--rm"]
        if self._network is not None:
            cmd.extend(["--network", self._network])
        for vol in job.volumes:
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

    def _run_container(self, job: JobSpec) -> JobResult:
        log_path = self._logger.get_log_path(job.id)
        stream = self._logger.open_stream(job.id)
        start = time.monotonic()
        try:
            result = subprocess.run(
                self._build_docker_command(job),
                stdout=stream,
                stderr=subprocess.STDOUT,
                text=True,
            )
            duration = time.monotonic() - start
            return JobResult(
                job_id=job.id,
                success=result.returncode == 0,
                exit_code=result.returncode,
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

    def _start_service(self, service: ServiceSpec) -> None:
        if self._network is None:
            raise ConfigurationError("services require network")

        container_name = f"orch_service_{service.id}_{uuid.uuid4().hex[:10]}"
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
        for alias in service.aliases:
            cmd.extend(["--network-alias", alias])
        for vol in service.volumes:
            mount = f"{vol.host_path}:{vol.container_path}"
            if vol.read_only:
                mount += ":ro"
            cmd.extend(["-v", mount])
        for key, value in service.env_vars.items():
            cmd.extend(["-e", f"{key}={value}"])
        cmd.append(service.image)
        if service.command is not None:
            cmd.extend(service.command)

        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        self._service_container_names.append(container_name)
        logger.info(
            "Started service container '%s' (service=%s, image=%s)",
            container_name,
            service.id,
            service.image,
        )
