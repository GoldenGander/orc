from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest

from orchestrator.server.event_bus import EventBus
from orchestrator.server.http_server import OrchestratorHTTPServer


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_server(bus: EventBus) -> tuple[OrchestratorHTTPServer, int, threading.Thread]:
    port = _free_port()
    server = OrchestratorHTTPServer(port, bus)
    t = threading.Thread(target=server.serve_until_done, daemon=True)
    t.start()
    time.sleep(0.05)  # let server bind and enter loop
    return server, port, t


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------


def test_status_returns_200_json() -> None:
    bus = EventBus()
    _, port, t = _start_server(bus)

    try:
        data = _get_json(f"http://127.0.0.1:{port}/status")
        assert "done" in data
        assert "elapsed_seconds" in data
        assert "jobs" in data
    finally:
        bus.close()
        t.join(timeout=5.0)


# ---------------------------------------------------------------------------
# /events — basic long-poll
# ---------------------------------------------------------------------------


def test_events_returns_buffered_events() -> None:
    bus = EventBus()
    bus.push({"type": "job_started", "job_id": "a"})
    bus.push({"type": "job_completed", "job_id": "a", "success": True})
    bus.close()

    _, port, t = _start_server(bus)
    t.join(timeout=5.0)  # server shuts down once done

    # Query after server is down — server closed cleanly, so test from the bus
    events, cursor, done = bus.events_since(0)
    assert len(events) == 2
    assert done is True


def test_events_endpoint_returns_json() -> None:
    bus = EventBus()
    bus.push({"type": "ping"})
    bus.close()

    _, port, t = _start_server(bus)

    data = _get_json(f"http://127.0.0.1:{port}/events?cursor=0")
    assert "events" in data
    assert "cursor" in data
    assert "done" in data
    assert data["events"][0]["type"] == "ping"

    t.join(timeout=5.0)


def test_events_cursor_advances() -> None:
    bus = EventBus()
    bus.push({"type": "a"})
    bus.push({"type": "b"})
    bus.push({"type": "c"})
    bus.close()

    _, port, t = _start_server(bus)

    first = _get_json(f"http://127.0.0.1:{port}/events?cursor=0")
    assert first["cursor"] == 3
    assert len(first["events"]) == 3

    second = _get_json(f"http://127.0.0.1:{port}/events?cursor=3")
    assert second["events"] == []
    assert second["done"] is True

    t.join(timeout=5.0)


def test_events_default_cursor_is_zero() -> None:
    bus = EventBus()
    bus.push({"type": "x"})
    bus.close()

    _, port, t = _start_server(bus)

    data = _get_json(f"http://127.0.0.1:{port}/events")
    assert len(data["events"]) == 1

    t.join(timeout=5.0)


def test_events_invalid_cursor_returns_400() -> None:
    bus = EventBus()
    _, port, t = _start_server(bus)

    try:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/events?cursor=notanint")
        assert exc_info.value.code == 400
    finally:
        bus.close()
        t.join(timeout=5.0)


# ---------------------------------------------------------------------------
# /unknown → 404
# ---------------------------------------------------------------------------


def test_unknown_path_returns_404() -> None:
    bus = EventBus()
    _, port, t = _start_server(bus)

    try:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/unknown")
        assert exc_info.value.code == 404
    finally:
        bus.close()
        t.join(timeout=5.0)


# ---------------------------------------------------------------------------
# /status accessible while /events is blocked (ThreadingHTTPServer)
# ---------------------------------------------------------------------------


def test_status_accessible_while_events_is_long_polling() -> None:
    bus = EventBus()
    _, port, t = _start_server(bus)

    status_result: list[int] = []
    events_done = threading.Event()

    def _long_poll() -> None:
        # Will block until bus has events or times out
        urllib.request.urlopen(f"http://127.0.0.1:{port}/events?cursor=0")
        events_done.set()

    poll_t = threading.Thread(target=_long_poll)
    poll_t.start()
    time.sleep(0.1)  # let /events start blocking

    # /status should respond immediately
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/status") as resp:
        status_result.append(resp.status)

    bus.push({"type": "ping"})
    bus.close()
    poll_t.join(timeout=5.0)
    t.join(timeout=5.0)

    assert status_result == [200]
