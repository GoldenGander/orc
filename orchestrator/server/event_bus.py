from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone


class EventBus:
    """Thread-safe event buffer for build orchestration progress.

    Producers call push() with a JSON-serialisable dict.  Consumers call
    events_since(cursor, timeout) to retrieve all events after a given
    cursor index, blocking for up to *timeout* seconds if none are available
    yet.  This supports a simple long-polling HTTP API without requiring any
    special client-side protocol (plain curl suffices).

    close() marks the bus as done; events_since() returns immediately once
    the buffer is drained past the caller's cursor.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._buffer: list[dict] = []        # raw payload dicts, in order
        self._done = False
        self._job_states: dict[str, str] = {}  # job_id -> "running"|"success"|"failed"
        self._start_time = time.monotonic()

    # ------------------------------------------------------------------
    # Producer side
    # ------------------------------------------------------------------

    def push(self, payload: dict[str, object]) -> None:
        """Append a JSON payload to the buffer and notify waiting consumers."""
        with self._condition:
            event_type = payload.get("type", "")
            job_id = payload.get("job_id")
            if isinstance(job_id, str):
                if event_type == "job_started":
                    self._job_states[job_id] = "running"
                elif event_type == "job_completed":
                    self._job_states[job_id] = "success" if payload.get("success") else "failed"
            self._buffer.append(payload)
            self._condition.notify_all()

    def close(self) -> None:
        """Mark the bus as done; unblocks all waiting events_since() callers."""
        with self._condition:
            self._done = True
            self._condition.notify_all()

    # ------------------------------------------------------------------
    # Consumer side
    # ------------------------------------------------------------------

    def events_since(
        self,
        cursor: int,
        timeout: float = 30.0,
    ) -> tuple[list[dict], int, bool]:
        """Return events after *cursor*, blocking up to *timeout* seconds.

        Returns (events, new_cursor, done):
        - events: list of payload dicts with index >= cursor
        - new_cursor: cursor to pass on the next call (cursor + len(events))
        - done: True once the engine has finished and events are exhausted

        Callers poll with increasing cursors until done is True and events
        is empty.
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                if cursor < len(self._buffer):
                    batch = self._buffer[cursor:]
                    new_cursor = cursor + len(batch)
                    done = self._done and new_cursor >= len(self._buffer)
                    return batch, new_cursor, done
                if self._done:
                    return [], cursor, True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return [], cursor, False
                self._condition.wait(timeout=remaining)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    @property
    def is_done(self) -> bool:
        with self._lock:
            return self._done

    def snapshot(self) -> dict[str, object]:
        """Return a JSON-serialisable status snapshot for GET /status."""
        with self._lock:
            return {
                "done": self._done,
                "elapsed_seconds": round(time.monotonic() - self._start_time, 1),
                "jobs": dict(self._job_states),
            }
