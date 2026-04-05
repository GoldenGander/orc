from __future__ import annotations

import threading
import time

from orchestrator.server.event_bus import EventBus


# ---------------------------------------------------------------------------
# push / buffer
# ---------------------------------------------------------------------------


def test_push_appends_payload() -> None:
    bus = EventBus()
    bus.push({"type": "job_started", "job_id": "build"})
    assert len(bus._buffer) == 1
    assert bus._buffer[0]["type"] == "job_started"


def test_push_three_events_grows_buffer() -> None:
    bus = EventBus()
    for i in range(3):
        bus.push({"type": "ping", "n": i})
    assert len(bus._buffer) == 3


def test_push_updates_job_states_running() -> None:
    bus = EventBus()
    bus.push({"type": "job_started", "job_id": "compile"})
    assert bus._job_states["compile"] == "running"


def test_push_updates_job_states_success() -> None:
    bus = EventBus()
    bus.push({"type": "job_completed", "job_id": "compile", "success": True})
    assert bus._job_states["compile"] == "success"


def test_push_updates_job_states_failed() -> None:
    bus = EventBus()
    bus.push({"type": "job_completed", "job_id": "compile", "success": False})
    assert bus._job_states["compile"] == "failed"


# ---------------------------------------------------------------------------
# is_done / close
# ---------------------------------------------------------------------------


def test_is_done_false_before_close() -> None:
    bus = EventBus()
    assert bus.is_done is False


def test_is_done_true_after_close() -> None:
    bus = EventBus()
    bus.close()
    assert bus.is_done is True


# ---------------------------------------------------------------------------
# events_since — history retrieval
# ---------------------------------------------------------------------------


def test_events_since_returns_buffered_events() -> None:
    bus = EventBus()
    bus.push({"type": "a"})
    bus.push({"type": "b"})
    bus.close()

    events, cursor, done = bus.events_since(0)
    assert len(events) == 2
    assert events[0]["type"] == "a"
    assert events[1]["type"] == "b"
    assert cursor == 2
    assert done is True


def test_events_since_cursor_advances() -> None:
    bus = EventBus()
    bus.push({"type": "a"})
    bus.push({"type": "b"})
    bus.push({"type": "c"})
    bus.close()

    _, cursor, _ = bus.events_since(0)
    assert cursor == 3

    events, cursor2, done = bus.events_since(cursor)
    assert events == []
    assert cursor2 == 3
    assert done is True


def test_events_since_partial_cursor() -> None:
    bus = EventBus()
    bus.push({"type": "a"})
    bus.push({"type": "b"})
    bus.push({"type": "c"})
    bus.close()

    events, cursor, _ = bus.events_since(1)
    assert len(events) == 2
    assert events[0]["type"] == "b"
    assert cursor == 3


def test_events_since_empty_closed_bus_returns_done() -> None:
    bus = EventBus()
    bus.close()

    events, cursor, done = bus.events_since(0)
    assert events == []
    assert cursor == 0
    assert done is True


# ---------------------------------------------------------------------------
# events_since — live blocking
# ---------------------------------------------------------------------------


def test_events_since_blocks_until_push() -> None:
    bus = EventBus()
    result: list[tuple] = []

    def _consumer() -> None:
        result.append(bus.events_since(0, timeout=5.0))

    t = threading.Thread(target=_consumer)
    t.start()
    time.sleep(0.05)
    bus.push({"type": "ping"})
    bus.close()
    t.join(timeout=3.0)

    assert len(result) == 1
    events, cursor, done = result[0]
    assert len(events) == 1
    assert events[0]["type"] == "ping"


def test_events_since_returns_empty_on_timeout_when_nothing_pushed() -> None:
    bus = EventBus()
    events, cursor, done = bus.events_since(0, timeout=0.05)
    assert events == []
    assert cursor == 0
    assert done is False


def test_close_unblocks_events_since() -> None:
    bus = EventBus()
    done_flag = threading.Event()

    def _consumer() -> None:
        bus.events_since(0, timeout=10.0)
        done_flag.set()

    t = threading.Thread(target=_consumer)
    t.start()
    time.sleep(0.05)
    bus.close()
    assert done_flag.wait(timeout=2.0), "events_since() did not return after close()"
    t.join()


# ---------------------------------------------------------------------------
# snapshot
# ---------------------------------------------------------------------------


def test_snapshot_shape() -> None:
    bus = EventBus()
    snap = bus.snapshot()
    assert "done" in snap
    assert "elapsed_seconds" in snap
    assert "jobs" in snap


def test_snapshot_reflects_job_states() -> None:
    bus = EventBus()
    bus.push({"type": "job_started", "job_id": "a"})
    bus.push({"type": "job_completed", "job_id": "a", "success": True})
    bus.push({"type": "job_started", "job_id": "b"})

    snap = bus.snapshot()
    assert snap["jobs"]["a"] == "success"
    assert snap["jobs"]["b"] == "running"
    assert snap["done"] is False


def test_snapshot_done_after_close() -> None:
    bus = EventBus()
    bus.close()
    assert bus.snapshot()["done"] is True


# ---------------------------------------------------------------------------
# Per-job bus registry
# ---------------------------------------------------------------------------


def test_job_bus_creates_new_bus() -> None:
    bus = EventBus()
    job_bus = bus.job_bus("compile")
    assert isinstance(job_bus, EventBus)


def test_job_bus_is_idempotent() -> None:
    bus = EventBus()
    b1 = bus.job_bus("compile")
    b2 = bus.job_bus("compile")
    assert b1 is b2


def test_get_job_bus_returns_none_before_creation() -> None:
    bus = EventBus()
    assert bus.get_job_bus("compile") is None


def test_get_job_bus_returns_bus_after_creation() -> None:
    bus = EventBus()
    bus.job_bus("compile")
    assert bus.get_job_bus("compile") is not None


def test_job_ids_empty_initially() -> None:
    bus = EventBus()
    assert bus.job_ids() == []


def test_job_ids_reflects_registered_jobs() -> None:
    bus = EventBus()
    bus.job_bus("a")
    bus.job_bus("b")
    assert sorted(bus.job_ids()) == ["a", "b"]


def test_per_job_bus_is_independent_from_main_bus() -> None:
    bus = EventBus()
    job_bus = bus.job_bus("compile")
    job_bus.push({"type": "log_line", "line": "hello"})

    # log_line should NOT appear on the main bus
    assert len(bus._buffer) == 0
    # but IS in the job bus
    assert len(job_bus._buffer) == 1
