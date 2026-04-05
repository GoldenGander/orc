# Build Orchestrator

A Python-based build orchestration system designed for Azure DevOps pipelines. It executes containerized build jobs with dependency management, resource scheduling, and artifact collection.

## Features

- **Docker-based execution**: Each job runs in an isolated container with configurable images
- **Dependency management**: Express job dependencies as a directed acyclic graph; the scheduler respects them
- **Resource scheduling**: Abstract CPU and memory slots prevent resource exhaustion during parallel execution
- **Artifact collection**: Collect build outputs from container filesystems using glob patterns
- **Volume mounting**: Mount source code read-only into containers; each job gets its own writable output directory
- **Failure policies**: Configure behavior on job failure (fail fast or continue)
- **YAML configuration**: Simple, declarative config format for build plans

## Quick Start

### Prerequisites

- Python 3.12+
- Docker (for running builds)
- `uv` package manager (recommended)

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd build_orch

# Install dependencies using uv
uv sync

# Or with pip (requires venv setup)
pip install -e .
```

### Running a Build

```bash
# Create a build configuration (see Configuration section below)
# Then run:
uv run python main.py config.yaml --source-dir /path/to/source --output-dir artifacts

# Or dry-run to validate config without executing:
uv run python main.py config.yaml --source-dir /path/to/source --dry-run

# For real-time monitoring via HTTP API, start a server on a local port:
uv run python main.py config.yaml --source-dir /path/to/source --output-dir artifacts --port 8080
```

## Configuration

Build plans are defined in YAML. The orchestrator loads the config, verifies dependencies are acyclic, schedules jobs respecting resource constraints, and executes them in Docker.

### YAML Format

```yaml
# Top-level orchestration settings
failure_policy: fail_fast          # "fail_fast" stops on first failure; "continue" runs all
max_parallel: 4                    # Max jobs running simultaneously
total_cpu_slots: 8                 # Total CPU slots available (distributed by job)
total_memory_slots: 8              # Total memory slots available (distributed by job)
resource_network: build-cache-net  # Optional shared network for managed container resources
resources:                         # Optional pipeline-wide shared resources
  - id: redis
    kind: cache
    lifetime: managed
    driver: docker_container
    image: redis:7-alpine
    aliases: [redis]               # Hostnames visible to jobs on resource_network
    command: ["redis-server", "--appendonly", "yes"]
    env_vars:
      REDIS_PASSWORD: "secret"
    volumes:
      - host_path: "/mnt/pipeline-cache/redis"
        container_path: "/data"

jobs:
  - id: my_job                     # Unique job identifier
    image: ubuntu:22.04            # Docker image to run
    command: ["bash", "-c", "echo hello"]  # Command to execute in container
    cpu_slots: 1                   # CPU slots required by this job
    memory_slots: 1                # Memory slots required by this job
    depends_on: []                 # List of job IDs this job depends on
    artifacts:                     # Files to collect from container output
      - source_glob: "*.txt"       # Glob pattern (relative to /output)
        destination_subdir: logs   # Subdirectory in final output
    volumes:                       # Optional: user-declared volume mounts
      - host_path: "/data/cache"
        container_path: "/root/.cache"
        read_only: true
```

### Volume Mounts

The orchestrator automatically injects two system-managed volumes into every job:

| Container Path | Host Path | Mode | Purpose |
|---|---|---|---|
| `/src` | Source directory (from `--source-dir`) | Read-only | Access checked-out source code |
| `/output` | Per-job output directory | Read-write | Write artifacts to be collected |

Jobs can additionally declare user-managed volumes in the `volumes` array.

If managed container `resources` are configured, each resource container gets
its own configured volumes. Resource volumes are never mounted into build jobs.

### Resources + Network

Managed `resources` are pipeline-wide shared dependencies. For the
`docker_container` driver, they start before job dispatch, join
`resource_network` (or an auto-created network), and are stopped after
orchestration completes.

- Jobs connect to managed container resources over `resource_network`.
- Use `aliases` (or resource `id` by default) as DNS hostnames from jobs.
- Use resource `volumes` to persist data on the host machine.
- Use `driver: external` plus `endpoint` for shared infrastructure the
  orchestrator references but does not manage.

Example Redis usage from a job command:

```yaml
resource_network: build-cache-net
resources:
  - id: redis
    kind: cache
    lifetime: managed
    driver: docker_container
    image: redis:7-alpine
    aliases: [redis]
    command: ["redis-server", "--appendonly", "yes"]
    volumes:
      - host_path: "/mnt/pipeline-cache/redis"
        container_path: "/data"

jobs:
  - id: build
    image: alpine:latest
    command: ["sh", "-c", "apk add --no-cache redis && redis-cli -h redis PING"]
    artifacts: []
```

### Example: Multi-Stage Build

```yaml
failure_policy: fail_fast
max_parallel: 2
total_cpu_slots: 4
total_memory_slots: 4

jobs:
  # Stage 1: Compile
  - id: compile
    image: gcc:12
    command: ["bash", "-c", "cd /src && gcc -o /output/app main.c"]
    cpu_slots: 2
    memory_slots: 2
    depends_on: []
    artifacts:
      - source_glob: "app"
        destination_subdir: bin

  # Stage 2: Test (runs after compile)
  - id: test
    image: gcc:12
    command: ["bash", "-c", "/output/app --test"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: ["compile"]
    artifacts:
      - source_glob: "test-results.xml"
        destination_subdir: reports
```

### Example: Parallel Jobs with Dependencies

```yaml
failure_policy: fail_fast
max_parallel: 3
total_cpu_slots: 6
total_memory_slots: 6

jobs:
  # These run in parallel (no dependencies)
  - id: unit_tests
    image: python:3.11
    command: ["bash", "-c", "cd /src && python -m pytest"]
    cpu_slots: 2
    memory_slots: 2
    depends_on: []
    artifacts:
      - source_glob: "test-results.xml"
        destination_subdir: reports

  - id: lint
    image: python:3.11
    command: ["bash", "-c", "cd /src && pylint src/"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: []
    artifacts:
      - source_glob: "*.json"
        destination_subdir: reports

  # This runs after both unit_tests and lint succeed
  - id: package
    image: python:3.11
    command: ["bash", "-c", "cd /src && python setup.py sdist"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: ["unit_tests", "lint"]
    artifacts:
      - source_glob: "dist/*.tar.gz"
        destination_subdir: packages
```

## Accessing Step Output

When running with the `--port` option, an HTTP server provides real-time access to job execution details. The server binds to `127.0.0.1` and exposes two endpoints:

### Status Snapshot

Get a quick overview of the build status:

```bash
curl http://127.0.0.1:8080/status
```

Response:
```json
{
  "done": false,
  "elapsed_seconds": 12.5,
  "jobs": {
    "compile": "running",
    "test": "success",
    "lint": "failed"
  }
}
```

### Real-Time Events (Long-Polling)

Stream job events as they occur using long-polling. Start with `cursor=0` and increment it by the number of events received:

```bash
# Get initial events
curl 'http://127.0.0.1:8080/events?cursor=0'

# Response (one JSON object per line):
# {"type": "job_started", "job_id": "compile", "timestamp": "2026-04-05T10:30:45Z"}
# {"type": "job_output", "job_id": "compile", "output": "Compiling..."}
# {"type": "job_completed", "job_id": "compile", "success": true, "timestamp": "2026-04-05T10:30:52Z"}
# {"events": [...], "cursor": 3, "done": false}

# Get next batch of events (cursor advanced by 3)
curl 'http://127.0.0.1:8080/events?cursor=3'
```

Each response contains:
- `events`: Array of job lifecycle events
- `cursor`: Next cursor position to request
- `done`: True when the build is complete and no more events are pending

Repeat requests with the updated cursor until `done` is true and `events` is empty.

**Example: Python Client**

```python
import requests
import json

cursor = 0
while True:
    response = requests.get(f"http://127.0.0.1:8080/events?cursor={cursor}")
    data = response.json()
    
    for event in data["events"]:
        print(f"{event['type']}: {event.get('job_id', 'N/A')}")
    
    cursor = data["cursor"]
    if data["done"] and not data["events"]:
        break
```

### Event Schema

The `/events` endpoint returns a stream of JSON events representing pipeline and job lifecycle changes. Each event includes a `type` field identifying its purpose, and additional fields specific to that event type.

#### event_type: `job_started`

Emitted when a job begins execution.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Always `"job_started"` |
| `job_id` | string | Yes | Unique identifier of the job that started |
| `ts` | string | Yes | ISO 8601 timestamp (UTC) when the job started |

**Example:**
```json
{
  "type": "job_started",
  "job_id": "compile",
  "ts": "2026-04-05T10:30:45.123456Z"
}
```

#### event_type: `job_completed`

Emitted when a job finishes execution (regardless of success or failure).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Always `"job_completed"` |
| `job_id` | string | Yes | Unique identifier of the job that completed |
| `success` | boolean | Yes | `true` if the job succeeded, `false` if it failed |
| `ts` | string | Yes | ISO 8601 timestamp (UTC) when the job completed |

**Example:**
```json
{
  "type": "job_completed",
  "job_id": "compile",
  "success": true,
  "ts": "2026-04-05T10:30:52.654321Z"
}
```

#### event_type: `log_line`

Emitted for each complete line of output from a running job.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Always `"log_line"` |
| `job_id` | string | Yes | Unique identifier of the job producing the output |
| `line` | string | Yes | A single line of job output (newline character removed) |
| `ts` | string | Yes | ISO 8601 timestamp (UTC) when the line was emitted |

**Example:**
```json
{
  "type": "log_line",
  "job_id": "compile",
  "line": "Compiling main.c...",
  "ts": "2026-04-05T10:30:47.234567Z"
}
```

#### event_type: `pipeline_complete`

Emitted once when the entire pipeline finishes execution.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Always `"pipeline_complete"` |
| `success` | boolean | Yes | `true` if all jobs succeeded, `false` if any job failed |
| `total_jobs` | integer | Yes | Total number of jobs in the pipeline |
| `failed_jobs` | integer | Yes | Number of jobs that failed (0 if `success` is `true`) |
| `ts` | string | Yes | ISO 8601 timestamp (UTC) when the pipeline completed |

**Example:**
```json
{
  "type": "pipeline_complete",
  "success": true,
  "total_jobs": 3,
  "failed_jobs": 0,
  "ts": "2026-04-05T10:31:05.987654Z"
}
```

## Architecture

### Components

- **YamlConfigLoader**: Parses YAML into a BuildPlan object; validates all required fields and types
- **ResourceScheduler**: Ensures jobs don't exceed CPU/memory slot limits; respects dependencies
- **DockerExecutor**: Manages Docker container lifecycle; handles volume mounts and logging
- **Engine**: Orchestrates job execution; coordinates scheduler, executor, and artifact collection
- **ArtifactStore**: Collects and stages artifacts from container output to final destination
- **FileJobLogger**: Records per-job execution logs for debugging

### Execution Flow

1. **Load config** → Parse YAML and validate against schema
2. **Inject volumes** → Add source and output mounts to every job
3. **Schedule** → Topologically sort jobs respecting dependencies and resource limits
4. **Execute** → Run jobs in Docker, respecting scheduling constraints
5. **Collect artifacts** → Glob pattern match on `/output`, stage to final directory
6. **Report** → Emit pipeline status (success/failure per job)

## Testing

Run the test suite with:

```bash
# All tests
uv run pytest

# Only unit tests (skip integration tests)
uv run pytest -m "not integration"

# Only integration tests (requires Docker)
uv run pytest -m integration

# Specific test file
uv run pytest tests/test_config.py
```

## Development

The codebase is organized with test-driven development practices:

- `orchestrator/` — Main package modules
- `tests/` — Pytest test files (including integration tests)
- `tests/fixtures/` — YAML test configs and test data
- `main.py` — Entry point for Azure DevOps pipeline invocation

## Azure DevOps Integration

This tool is designed to be invoked as a pipeline step. An annotated example is also available at [`azure-pipelines.example.yml`](azure-pipelines.example.yml) in the repository root.

### Basic Pipeline

The minimal setup: validate the config, run the build, then publish artifacts.

```yaml
# azure-pipelines.yml
trigger:
  - main

pool:
  vmImage: ubuntu-latest

steps:
  - checkout: self
    displayName: Checkout source

  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.12'
    displayName: Use Python 3.12

  - script: |
      pip install uv
      uv sync --frozen
    displayName: Install orchestrator

  - script: |
      uv run python main.py build-config.yaml \
        --source-dir $(Build.SourcesDirectory) \
        --dry-run
    displayName: Validate build config

  - script: |
      uv run python main.py build-config.yaml \
        --source-dir $(Build.SourcesDirectory) \
        --output-dir $(Build.ArtifactStagingDirectory) \
        --keep-logs
    displayName: Run orchestrated build

  - task: PublishBuildArtifacts@1
    condition: always()
    inputs:
      pathToPublish: $(Build.ArtifactStagingDirectory)
      artifactName: build-outputs
    displayName: Publish artifacts
```

### With Real-Time HTTP Monitoring

Pass `--port` to start the HTTP event server and poll job status from another step or external system while the build runs.

```yaml
  - script: |
      uv run python main.py build-config.yaml \
        --source-dir $(Build.SourcesDirectory) \
        --output-dir $(Build.ArtifactStagingDirectory) \
        --keep-logs \
        --port 9000
    displayName: Run orchestrated build (with monitoring)

  # From another terminal / script during the build:
  # curl http://127.0.0.1:9000/status
  # curl 'http://127.0.0.1:9000/events?cursor=0'
```

### CLI Reference

| Flag | Default | Description |
|---|---|---|
| `config` | (required) | Path to the YAML build plan |
| `--source-dir` | (required) | Checked-out source tree; mounted read-only at `/src` in every job |
| `--output-dir` | `artifacts` | Directory where artifacts and logs are written |
| `--dry-run` | off | Validate config without executing any containers |
| `--port PORT` | off | Start the HTTP event server on `127.0.0.1:PORT` |
| `--keep-logs` | off | Copy job logs to `--output-dir/logs/` on success (always copied on failure) |

## License

This repository does not currently include a license file.
