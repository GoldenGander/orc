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
- Exit code:
  - `0` on success, `1` if any job failed/skipped.

## Code Map
- `main.py`
  - Parses CLI args.
  - Loads YAML plan via `YamlConfigLoader`.
  - Injects system volumes (`/src`, `/output`) with `prepare_volumes`.
  - Wires `Engine + ResourceScheduler + DockerExecutor + ArtifactStore + FileJobLogger`.
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
- `tests/`
  - Unit coverage for each subsystem + Docker-backed integration tests.

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

## Common Extension Points
- New execution backend:
  - implement `ExecutorABC`, wire it in `main.py`.
- New reporter (GitHub Actions, etc.):
  - implement `IPipelineReporter`, inject into `Engine`.
- Different artifact policy:
  - implement `ArtifactStoreABC`.
- Scheduling tweaks:
  - extend/replace `SchedulerABC` implementation.

## Known Gotchas
- README Python version is outdated versus `pyproject.toml`.
- `prepare_volumes()` mutates `BuildPlan` in place.
- `ArtifactStore.collect()` is strict: empty glob match fails job flow.
- Engine creates temp dirs for logs/staging/output mounts; only finalized artifacts are copied to `--output-dir`.

## First 30 Minutes Checklist
1. `uv sync`
2. `uv run pytest -m "not integration"`
3. Open `main.py` then `orchestrator/engine.py`.
4. Run a dry-run with a tiny YAML plan.
5. Run one Docker integration test if daemon is available.
