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

- Python 3.11+
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

This tool is designed to be invoked as a pipeline step:

```yaml
trigger:
  - main

jobs:
  - job: BuildJob
    pool:
      vmImage: 'ubuntu-latest'
    steps:
      - checkout: self
      - task: UsePythonVersion@0
        inputs:
          versionSpec: '3.11'
      - script: |
          pip install uv
          uv run python main.py build-config.yaml --source-dir $(Build.SourcesDirectory) --output-dir $(Build.ArtifactStagingDirectory)
        displayName: 'Run Orchestrated Build'
```

## License

See LICENSE file for details.
