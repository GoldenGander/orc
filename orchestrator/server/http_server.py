from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from orchestrator.server.event_bus import EventBus

# How long the server blocks waiting for new events on each long-poll request.
_LONG_POLL_TIMEOUT_SECONDS = 30.0


class _OrchestratorHandler(BaseHTTPRequestHandler):
    """HTTP request handler.

    Endpoints:
        GET /events?cursor=N
            Long-poll endpoint.  Returns all events with index >= N as
            newline-delimited JSON, blocking up to 30 s if none are ready.
            Response body (one JSON object per line):

                {"events": [...], "cursor": <int>, "done": <bool>}

            Callers increment cursor by the number of events received and
            repeat until done is true and events is empty.  Works with plain
            curl — no special client protocol needed.

        GET /status
            JSON snapshot: {"done": bool, "elapsed_seconds": float,
                            "jobs": {job_id: "running"|"success"|"failed"}}
    """

    server: "OrchestratorHTTPServer"  # type: ignore[assignment]

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Suppress default stderr request logging."""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/events":
            self._handle_events(parsed.query)
        elif parsed.path == "/status":
            self._handle_status()
        else:
            self.send_error(404, "Not found")

    def _handle_events(self, query: str) -> None:
        params = parse_qs(query)
        try:
            cursor = int(params.get("cursor", ["0"])[0])
        except ValueError:
            self.send_error(400, "cursor must be an integer")
            return

        bus: EventBus = self.server.event_bus
        events, new_cursor, done = bus.events_since(cursor, timeout=_LONG_POLL_TIMEOUT_SECONDS)

        body = json.dumps({"events": events, "cursor": new_cursor, "done": done}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_status(self) -> None:
        bus: EventBus = self.server.event_bus
        body = json.dumps(bus.snapshot()).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class OrchestratorHTTPServer:
    """Local HTTP server that serves build events to pipeline agents.

    Binds to 127.0.0.1 only.  Uses ThreadingHTTPServer so /status requests
    are served concurrently with long-polling /events connections.

    Call serve_until_done() in the main thread; it blocks until the EventBus
    is closed (engine finished), then shuts down gracefully.
    """

    def __init__(self, port: int, bus: EventBus) -> None:
        self.event_bus = bus
        self._httpd = ThreadingHTTPServer(("127.0.0.1", port), _OrchestratorHandler)
        self._httpd.event_bus = bus  # type: ignore[attr-defined]
        # Short timeout so the is_done poll loop doesn't block indefinitely
        # after the engine finishes.  Long-poll timeout is managed inside
        # EventBus.events_since(), not here.
        self._httpd.timeout = 1.0

    def serve_until_done(self) -> None:
        """Block serving requests until the engine finishes, then shut down."""
        while not self.event_bus.is_done:
            self._httpd.handle_request()
        # Brief drain: allow any in-flight requests to complete.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            self._httpd.handle_request()
        self._httpd.server_close()
