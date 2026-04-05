from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from orchestrator.server.event_bus import EventBus

try:
    import psutil as _psutil

    _PSUTIL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _psutil = None  # type: ignore[assignment]
    _PSUTIL_AVAILABLE = False


class HostMetricsSampler:
    """Background thread that pushes host CPU/memory samples to the EventBus.

    Samples are pushed as ``host_metrics`` events on the pipeline-level bus
    so callers can track system load over the course of a build without
    having to poll an external monitoring system.

    Peak and average figures are accumulated in memory and can be read by
    the reporter when it emits the final build summary.

    If *psutil* is not installed this class is a no-op: start()/stop() are
    safe to call but no events are emitted and all metric accessors return 0.
    """

    def __init__(self, bus: EventBus, interval_seconds: float = 5.0) -> None:
        self._bus = bus
        self._interval = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._peak_cpu: float = 0.0
        self._peak_memory: float = 0.0
        self._cpu_samples: list[float] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if not _PSUTIL_AVAILABLE:
            return
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="host-metrics"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1.0)

    # ------------------------------------------------------------------
    # Accessors (safe to call before/after start)
    # ------------------------------------------------------------------

    def peak_cpu_percent(self) -> float:
        with self._lock:
            return self._peak_cpu

    def peak_memory_percent(self) -> float:
        with self._lock:
            return self._peak_memory

    def avg_cpu_percent(self) -> float:
        with self._lock:
            if not self._cpu_samples:
                return 0.0
            return round(sum(self._cpu_samples) / len(self._cpu_samples), 1)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        # Prime the first measurement — psutil needs a prior baseline.
        _psutil.cpu_percent(interval=None)
        while not self._stop.wait(timeout=self._interval):
            cpu = _psutil.cpu_percent(interval=None)
            mem = _psutil.virtual_memory().percent
            with self._lock:
                self._cpu_samples.append(cpu)
                self._peak_cpu = max(self._peak_cpu, cpu)
                self._peak_memory = max(self._peak_memory, mem)
            self._bus.push(
                {
                    "type": "host_metrics",
                    "cpu_percent": cpu,
                    "memory_percent": mem,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            )
