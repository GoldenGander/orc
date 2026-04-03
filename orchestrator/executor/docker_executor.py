from __future__ import annotations

import subprocess
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import override

from orchestrator.executor.executor import ExecutorABC
from orchestrator.logger.logger import JobLoggerABC
from orchestrator.models import JobResult, JobSpec


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

    @override
    def submit(self, job: JobSpec) -> Future[JobResult]:
        return self._pool.submit(self._run_container, job)

    @override
    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    def _build_docker_command(self, job: JobSpec) -> list[str]:
        cmd: list[str] = ["docker", "run", "--rm"]
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
