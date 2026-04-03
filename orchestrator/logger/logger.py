from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, override


class JobLoggerABC(ABC):
    """Provides per-job isolated log streams.

    Each job gets its own log file so output is attributable to a specific
    project and build step without interleaving. The engine opens the stream
    before submitting a job and closes it after the Future resolves.
    """

    @abstractmethod
    def get_log_path(self, job_id: str) -> Path:
        """Return the path where the log file for job_id will be written."""
        ...

    @abstractmethod
    def open_stream(self, job_id: str) -> IO[str]:
        """Open and return a writable text stream for the job's log.

        A stream must not be opened for the same job_id twice without first
        calling close_stream. The caller (engine) is responsible for pairing
        every open_stream with a close_stream.
        """
        ...

    @abstractmethod
    def close_stream(self, job_id: str) -> None:
        """Flush and close the log stream for the given job."""
        ...


class FileJobLogger(JobLoggerABC):
    """Concrete logger that writes job logs to individual files.

    Creates a log directory and maintains one text file per job. Streams are
    buffered in memory and flushed on close.
    """

    def __init__(self, log_dir: Path) -> None:
        """Initialize the logger with a log directory.

        Args:
            log_dir: Directory where log files will be created.
        """
        self.log_dir: Path = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._open_streams: dict[str, IO[str]] = {}

    @override
    def get_log_path(self, job_id: str) -> Path:
        """Return the path where the log file for job_id will be written."""
        return self.log_dir / f"{job_id}.log"

    @override
    def open_stream(self, job_id: str) -> IO[str]:
        """Open and return a writable text stream for the job's log."""
        if job_id in self._open_streams:
            raise RuntimeError(
                f"Log stream for job {job_id} is already open. "
                + "Close it before opening again."
            )

        log_path = self.get_log_path(job_id)
        stream = open(log_path, "w", encoding="utf-8")
        self._open_streams[job_id] = stream
        return stream

    @override
    def close_stream(self, job_id: str) -> None:
        """Flush and close the log stream for the given job."""
        if job_id not in self._open_streams:
            return  # Idempotent: closing a non-open stream is safe

        stream = self._open_streams.pop(job_id)
        stream.flush()
        stream.close()
