# agents-mini.md

- Purpose: config-driven Docker build orchestrator for Azure DevOps.
- Entry point: `main.py` (`build-orch` console script).
- Core flow: `YamlConfigLoader` -> `prepare_volumes` -> `Engine(ResourceScheduler, DockerExecutor, ArtifactStore, FileJobLogger)`.

## Run
- Validate only: `uv run python main.py <plan.yaml> --source-dir <src> --dry-run`
- Execute: `uv run python main.py <plan.yaml> --source-dir <src> --output-dir <artifacts>`

## Key semantics
- DAG deps enforced (`DependencyGraph.validate`).
- Dispatch constrained by `max_parallel`, `total_cpu_slots`, `total_memory_slots`.
- `fail_fast`: stop submitting new jobs after first failure.
- `continue`: keep running independent jobs; blocked dependents are skipped.
- Skipped/unreachable jobs reported failed with `exit_code=-1`.

## Paths inside containers
- `/src` = mounted source dir (read-only).
- `/output` = per-job writable output dir; artifact globs resolve from here.

## Tests
- Unit: `uv run pytest -m "not integration"`
- Integration (Docker): `uv run pytest -m integration`

## Gotchas
- Python version mismatch in docs: README says 3.11+, `pyproject.toml` requires >=3.12.
- `prepare_volumes()` mutates plan in place.
- Missing artifact glob matches raise `ArtifactError`.
