from __future__ import annotations

import io
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, override

from orchestrator.logger.logger import JobLoggerABC
from orchestrator.server.event_bus import EventBus


class TeeStream:
    """Writable text stream that tees complete lines to a callback.

    subprocess.run() delivers output in arbitrary-sized chunks.  TeeStream
    buffers incomplete lines and fires the callback only when a newline is
    encountered, so the callback always receives whole lines.

    fileno() raises UnsupportedOperation intentionally: this forces
    subprocess.run() to route writes through the Python write() method
    rather than writing directly to the underlying file descriptor, which
    would bypass our tee logic.
    """

    def __init__(self, underlying: IO[str], on_line: Callable[[str], None]) -> None:
        self._underlying = underlying
        self._on_line = on_line
        self._buf = ""

    def write(self, s: str) -> int:
        self._underlying.write(s)
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            self._on_line(line)
        return len(s)

    def flush(self) -> None:
        self._underlying.flush()

    def close(self) -> None:
        if self._buf:
            self._on_line(self._buf)
            self._buf = ""
        self._underlying.flush()
        self._underlying.close()

    def fileno(self) -> int:
        raise io.UnsupportedOperation("fileno")

    # Make duck-typing checks happy (subprocess checks for these attributes)
    @property
    def mode(self) -> str:
        return "w"

    @property
    def name(self) -> str:
        return getattr(self._underlying, "name", "<tee>")


class EventBusJobLogger(JobLoggerABC):
    """JobLogger that tees Docker log output to an EventBus as log_line SSE events.

    Opens log files directly (does not delegate to FileJobLogger) to avoid a
    double-close: TeeStream.close() already closes the underlying file handle,
    so a second close from FileJobLogger would raise ValueError.
    """

    def __init__(self, log_dir: Path, bus: EventBus) -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._bus = bus
        self._open_tees: dict[str, TeeStream] = {}

    @override
    def get_log_path(self, job_id: str) -> Path:
        return self._log_dir / f"{job_id}.log"

    @override
    def open_stream(self, job_id: str) -> IO[str]:
        if job_id in self._open_tees:
            raise RuntimeError(
                f"Log stream for job {job_id!r} is already open. "
                "Close it before opening again."
            )
        path = self.get_log_path(job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_stream = open(path, "w", encoding="utf-8")

        def _emit(line: str) -> None:
            self._bus.push(
                {
                    "type": "log_line",
                    "job_id": job_id,
                    "line": line,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            )

        tee = TeeStream(file_stream, _emit)
        self._open_tees[job_id] = tee
        return tee  # type: ignore[return-value]

    @override
    def close_stream(self, job_id: str) -> None:
        tee = self._open_tees.pop(job_id, None)
        if tee is not None:
            tee.close()
