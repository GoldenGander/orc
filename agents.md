# agents.md

## TL;DR
- Purpose: run config-driven, dependency-aware build jobs in Docker for Azure DevOps.
- Entry point: `main.py`.
- Core package: `orchestrator/`.
- Fast local loop: `uv sync` then `uv run pytest -m "not integration"`.

## Environment
- Python: `>=3.12` in `pyproject.toml`
- Runtime dependency: Docker daemon available for real execution/integration tests.
- Package manager: `uv` is the expected path.

## Run It
- Dry-run config validation:
  - `uv run python main.py <plan.yaml> --source-dir <src> --dry-run`
- Full run:
  - `uv run python main.py <plan.yaml> --source-dir <src> --output-dir <artifacts_dir>`
- With real-time event streaming (optional):
  - `uv run python main.py <plan.yaml> --source-dir <src> --output-dir <artifacts_dir> --port 8080`
  - HTTP server binds to `127.0.0.1:<port>` and streams events via `/events` and `/status` endpoints.
- Exit code:
  - `0` on success, `1` if any job failed/skipped.

## Code Map
- `main.py`
  - Parses CLI args (including optional `--port` for event streaming).
  - Loads YAML plan via `YamlConfigLoader`.
  - Injects system volumes (`/src`, `/output`) with `prepare_volumes`.
  - In server mode (`--port`): wires `EventBus`, `EventBusJobLogger`, `OrchestratorHTTPServer`, `EventBusReporter`.
  - In standard mode: wires `Engine + ResourceScheduler + DockerExecutor + ArtifactStore + FileJobLogger`.
- `orchestrator/config.py`
  - YAML schema parsing and validation.
  - Enforces IDs, dependency references, integer slot fields, failure policy values.
- `orchestrator/models.py`
  - Data model (`BuildPlan`, `JobSpec`, `JobResult`, etc.).
- `orchestrator/graph.py`
  - DAG validation + ready-set logic.
- `orchestrator/scheduler/scheduler.py`
  - Resource-aware dispatch (`max_parallel`, cpu slots, memory slots).
- `orchestrator/executor/docker_executor.py`
  - Runs `docker run --rm` in thread pool, streams logs, returns `JobResult`.
- `orchestrator/engine.py`
  - Main orchestration loop: submit, wait, collect artifacts, apply failure policy, finalize.
- `orchestrator/volume_prep.py`
  - Mutates jobs to append system mounts.
- `orchestrator/artifact_store/artifact_store.py`
  - Collects job outputs from per-job host dirs and finalizes into output root.
- `orchestrator/logger/logger.py`
  - Per-job log files.
- `orchestrator/server/event_bus.py`
  - Thread-safe event buffer for pipeline lifecycle events.
  - Supports long-polling with cursor-based pagination.
  - `job_bus(job_id)` / `get_job_bus(job_id)`: per-job bus registry for log streams.
- `orchestrator/server/http_server.py`
  - HTTP server with pipeline-level and per-job endpoints.
  - Binds to `127.0.0.1` only; runs in separate thread.
- `orchestrator/server/reporter.py`
  - `EventBusReporter(bus, plan, sampler)`: pushes high-level lifecycle events + `build_summary`.
  - `CompositeReporter`: fans out to multiple reporters.
- `orchestrator/server/tee_logger.py`
  - `TeeStream`: wraps file handle to tee complete lines to callback.
  - `EventBusJobLogger`: streams Docker output lines to per-job EventBus (not the main pipeline bus).
- `orchestrator/server/metrics.py`
  - `HostMetricsSampler`: background thread sampling host CPU/memory via psutil.
  - Pushes `host_metrics` events to the pipeline bus; accumulates peak/avg for `build_summary`.
- `tests/`
  - Unit coverage for each subsystem + Docker-backed integration tests.
  - `tests/test_integration_http_server.py`: end-to-end server event streaming test.

## Config Contract (YAML)
Top-level fields:
- `failure_policy`: `fail_fast` | `continue`
- `max_parallel`: int >= 1
- `total_cpu_slots`: int >= 1
- `total_memory_slots`: int >= 1
- `jobs`: list

Per-job fields:
- required: `id`, `image`
- optional: `depends_on`, `command`, `cpu_slots`, `memory_slots`, `artifacts`, `volumes`, `env_vars`

Notes:
- `command: null` means use image default entrypoint/cmd.
- `depends_on` references are validated against known IDs.
- Cycle detection happens in engine startup via `DependencyGraph.validate()`.

## Runtime Semantics
- Scheduler only dispatches jobs that are:
  - dependency-ready, and
  - within current slot + concurrency budgets.
- `fail_fast`:
  - stops submitting new work after first failure,
  - already-running jobs still finish,
  - unreachable jobs become failed/skipped (`exit_code=-1`).
- `continue`:
  - keeps running independent jobs,
  - jobs blocked by failed deps are skipped (`exit_code=-1`).
- Artifacts:
  - collected only for successful jobs,
  - missing match for a declared artifact glob raises `ArtifactError`.
- Event Streaming (when `--port` is set):
  - `EventBus` buffers pipeline-level events; each job has its own per-job bus for log lines.
  - `OrchestratorHTTPServer` serves `/events` (pipeline stream), `/status`, `/jobs`, `/jobs/<id>/events`, `/jobs/<id>/status`.
  - Pipeline stream excludes log lines — callers that only need high-level tracking never see container noise.
  - `HostMetricsSampler` samples host CPU/memory every 5 s and emits `host_metrics` events on the pipeline stream.
  - `build_summary` event is emitted at pipeline end with per-job durations, slot usage, and host peak/avg metrics.
  - Events include ISO 8601 timestamps and are streamed to clients in order.

## Event Streaming HTTP API (Optional)

When run with `--port`, the orchestrator exposes an HTTP server on `127.0.0.1:<port>`.

### Pipeline-level endpoints

Callers that want a high-level view of the entire pipeline (e.g. a single ADO step
tracking overall progress) use these endpoints.  Log lines are intentionally excluded
so the stream stays clean and small.

#### GET /status
JSON snapshot of pipeline state:
```json
{
  "done": false,
  "elapsed_seconds": 12.5,
  "jobs": {"compile": "running", "test": "success", "lint": "failed"}
}
```

#### GET /events?cursor=0
Long-poll for pipeline-level events.  Returns a single JSON object:
```json
{
  "events": [
    {"type": "job_started",   "job_id": "compile", "ts": "..."},
    {"type": "job_completed", "job_id": "compile", "success": true, "ts": "..."},
    {"type": "host_metrics",  "cpu_percent": 42.1, "memory_percent": 61.0, "ts": "..."},
    {"type": "pipeline_complete", "success": true, "total_jobs": 3, "failed_jobs": 0, "ts": "..."},
    {"type": "build_summary", ... }
  ],
  "cursor": 5,
  "done": true
}
```

Pipeline event types:
- `job_started`: job begins execution
- `job_completed`: job finishes (`success` bool)
- `resource_status`: managed resource container heartbeat
- `host_metrics`: CPU/memory snapshot from the build host (emitted every 5 s)
- `pipeline_complete`: pipeline finished (high-level totals)
- `build_summary`: full DevOps audit payload (see below)

#### GET /jobs
List all job IDs that have been registered:
```json
{"jobs": ["compile", "test", "lint"], "pipeline_done": false}
```

### Per-job endpoints

Callers that want a dedicated ADO step per build job, or that need raw Docker output,
use these endpoints.  Each job's log stream is independent of the pipeline stream.

#### GET /jobs/\<job_id\>/events?cursor=0
Long-poll for a single job's log lines.  Start polling after receiving `job_started`
on `/events`.  Returns `done: true` once the container exits.
```json
{
  "events": [
    {"type": "log_line", "job_id": "compile", "line": "Compiling main.cpp", "ts": "..."}
  ],
  "cursor": 1,
  "done": false
}
```

#### GET /jobs/\<job_id\>/status
Snapshot of a single job:
```json
{"job_id": "compile", "status": "running", "done": false, "log_event_count": 47}
```

`status` values: `"running"` | `"success"` | `"failed"` | `"unknown"` (job not yet started).

### build_summary event (audit payload)

Emitted once at the end of every successful or failed pipeline run, after
`pipeline_complete`.  Designed for DevOps audit consumers.

```json
{
  "type": "build_summary",
  "success": true,
  "total_duration_seconds": 87.4,
  "totals": {"jobs": 3, "succeeded": 3, "failed": 0, "skipped": 0},
  "jobs": [
    {
      "id": "compile",
      "status": "success",
      "exit_code": 0,
      "duration_seconds": 34.1,
      "cpu_slots": 2,
      "memory_slots": 2
    }
  ],
  "host": {
    "peak_cpu_percent": 82.3,
    "peak_memory_percent": 58.1,
    "avg_cpu_percent": 47.6
  },
  "ts": "2026-04-05T10:31:00.000000Z"
}
```

`host` is present only when `psutil` is installed (it is a declared dependency).
`cpu_slots` / `memory_slots` are present per job.
`status` per job: `"success"` | `"failed"` | `"skipped"` (exit_code -1, blocked by failed dep).

See README.md "Accessing Step Output" section for full schema and client examples.

## Volumes and Paths
System mounts added to every job:
- `<source_dir>` -> `/src` (read-only)
- `<tmp>/outputs/<job_id>` -> `/output` (read-write)

Implications:
- Jobs should read source from `/src`.
- Jobs should write files intended for collection to `/output`.

## Testing
- All tests: `uv run pytest`
- Unit only: `uv run pytest -m "not integration"`
- Integration only (Docker required): `uv run pytest -m integration`

High-value tests:
- `tests/test_engine.py`: orchestration/failure-policy behavior.
- `tests/test_config.py`: schema and validation errors.
- `tests/test_integration.py`: end-to-end Docker execution and artifact flow.
- `tests/test_integration_http_server.py`: event streaming during real pipeline execution.
- `tests/test_event_bus.py`: EventBus thread-safety and cursor pagination.
- `tests/test_http_server.py`: HTTP endpoint correctness and concurrent requests.
- `tests/test_sse_reporter.py`: event payload shape and CompositeReporter fanning.
- `tests/test_tee_stream.py`: line-splitting and partial line handling.

## Common Extension Points
- New execution backend:
  - implement `ExecutorABC`, wire it in `main.py`.
- New reporter (GitHub Actions, Slack, etc.):
  - implement `IPipelineReporter`, inject into `Engine`.
  - For server mode: add a new event type to `EventBus.push()` and document in event schema.
- New HTTP client (web UI, monitoring dashboard, etc.):
  - For overall pipeline tracking: consume `/events` long-poll and `/status`.
  - For per-job log tailing: poll `/jobs/<id>/events` after receiving `job_started`.
  - For job enumeration: `GET /jobs` to discover all registered job IDs.
- Different artifact policy:
  - implement `ArtifactStoreABC`.
- Scheduling tweaks:
  - extend/replace `SchedulerABC` implementation.
- Custom job logging/output handling:
  - implement `JobLoggerABC` (or inherit `EventBusJobLogger` to customize event emission).

## Known Gotchas
- `prepare_volumes()` mutates `BuildPlan` in place.
- `ArtifactStore.collect()` is strict: empty glob match fails job flow.
- Engine creates temp dirs for logs/staging/output mounts; only finalized artifacts are copied to `--output-dir`.
- HTTP server mode (`--port`) runs the Engine in a separate thread; ensure clients poll `/status` or `/events` to observe progress.
- `EventBus` buffers all events in memory (append-only, never dropped). Log lines now live only in per-job buses, so the pipeline bus stays small. Per-job buses can grow large on verbose builds — monitor memory on long runs.
- `HostMetricsSampler` requires `psutil` (declared dependency). If psutil is somehow unavailable, no `host_metrics` events are emitted and `build_summary.host` is absent.
- `TeeStream.close()` closes the underlying file handle; do not call close() twice or delegate to another closer.

## First 30 Minutes Checklist
1. `uv sync`
2. `uv run pytest -m "not integration"`
3. Open `main.py` then `orchestrator/engine.py`.
4. Run a dry-run with a tiny YAML plan.
5. Run one Docker integration test if daemon is available.
