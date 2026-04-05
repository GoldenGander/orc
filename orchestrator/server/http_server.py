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

    Pipeline-level endpoints (high-level events only, no log lines):

        GET /events?cursor=N
            Long-poll endpoint.  Returns all pipeline-level events with index
            >= N as a single JSON object, blocking up to 30 s if none are
            ready.  Pipeline events include: job_started, job_completed,
            resource_status, host_metrics, pipeline_complete, build_summary.
            Response body:
                {"events": [...], "cursor": <int>, "done": <bool>}

        GET /status
            JSON snapshot of the full pipeline state:
            {"done": bool, "elapsed_seconds": float,
             "jobs": {job_id: "running"|"success"|"failed"}}

        GET /jobs
            List all job IDs that have been registered on the pipeline:
            {"jobs": [<job_id>, ...], "pipeline_done": <bool>}

    Per-job endpoints (log lines for a single job):

        GET /jobs/<job_id>/events?cursor=N
            Long-poll endpoint for a single job's log stream.  Returns
            log_line events for that job only.  Clients should start polling
            after receiving a job_started event on /events.
            {"events": [...], "cursor": <int>, "done": <bool>}

        GET /jobs/<job_id>/status
            Snapshot of a single job's current state:
            {"job_id": <str>, "status": "running"|"success"|"failed"|"unknown",
             "done": <bool>, "log_event_count": <int>}

    Cursor-based pagination: callers increment cursor by len(events) and repeat
    until done is true and events is empty.  Works with plain curl.
    """

    server: "OrchestratorHTTPServer"  # type: ignore[assignment]

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Suppress default stderr request logging."""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/events":
            self._handle_events(parsed.query)
        elif path == "/status":
            self._handle_status()
        elif path == "/jobs":
            self._handle_jobs_list()
        elif path.startswith("/jobs/"):
            self._route_job_path(path, parsed.query)
        else:
            self.send_error(404, "Not found")

    # ------------------------------------------------------------------
    # Pipeline-level handlers
    # ------------------------------------------------------------------

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
        self._write_json(body)

    def _handle_status(self) -> None:
        bus: EventBus = self.server.event_bus
        body = json.dumps(bus.snapshot()).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_jobs_list(self) -> None:
        bus: EventBus = self.server.event_bus
        body = json.dumps(
            {"jobs": bus.job_ids(), "pipeline_done": bus.is_done}
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------
    # Per-job routing
    # ------------------------------------------------------------------

    def _route_job_path(self, path: str, query: str) -> None:
        """Dispatch /jobs/<job_id>/events and /jobs/<job_id>/status."""
        # Strip the leading "/jobs/" prefix then split on the first "/"
        # to separate job_id from the sub-resource.
        tail = path[len("/jobs/"):]
        if "/" not in tail:
            self.send_error(404, "Not found")
            return
        job_id, sub = tail.split("/", 1)
        if not job_id:
            self.send_error(404, "Not found")
            return
        if sub == "events":
            self._handle_job_events(job_id, query)
        elif sub == "status":
            self._handle_job_status(job_id)
        else:
            self.send_error(404, "Not found")

    def _handle_job_events(self, job_id: str, query: str) -> None:
        params = parse_qs(query)
        try:
            cursor = int(params.get("cursor", ["0"])[0])
        except ValueError:
            self.send_error(400, "cursor must be an integer")
            return

        bus: EventBus = self.server.event_bus
        job_bus = bus.get_job_bus(job_id)
        if job_bus is None:
            # Job not started yet or unknown — return empty, not done.
            body = json.dumps({"events": [], "cursor": 0, "done": False}).encode("utf-8")
        else:
            events, new_cursor, done = job_bus.events_since(
                cursor, timeout=_LONG_POLL_TIMEOUT_SECONDS
            )
            body = json.dumps({"events": events, "cursor": new_cursor, "done": done}).encode(
                "utf-8"
            )
        self._write_json(body)

    def _handle_job_status(self, job_id: str) -> None:
        bus: EventBus = self.server.event_bus
        pipeline_jobs: dict = bus.snapshot().get("jobs", {})  # type: ignore[assignment]
        job_status = pipeline_jobs.get(job_id)

        job_bus = bus.get_job_bus(job_id)
        log_event_count = 0
        if job_bus is not None:
            events, _, _ = job_bus.events_since(0, timeout=0.0)
            log_event_count = len(events)

        if job_status is None:
            payload = {
                "job_id": job_id,
                "status": "unknown",
                "done": False,
                "log_event_count": log_event_count,
            }
        else:
            payload = {
                "job_id": job_id,
                "status": job_status,
                "done": job_status in ("success", "failed"),
                "log_event_count": log_event_count,
            }

        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------
    # Helper
    # ------------------------------------------------------------------

    def _write_json(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
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
