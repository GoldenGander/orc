"""End-to-end integration test: YAML config → Engine → real Docker execution.

Requires Docker to be available on the host. Tests are marked with
``pytest.mark.integration`` so they can be selected or excluded via:

    pytest -m integration          # run only integration tests
    pytest -m "not integration"    # skip them
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import override
from uuid import uuid4

import pytest

from orchestrator.artifact_store import ArtifactStore
from orchestrator.config import YamlConfigLoader
from orchestrator.engine import Engine
from orchestrator.executor import DockerExecutor
from orchestrator.logger import FileJobLogger
from orchestrator.models import OrchestratorResult
from orchestrator.pipeline import IPipelineReporter
from orchestrator.scheduler import ResourceScheduler
from orchestrator.volume_prep import prepare_volumes

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Lightweight stubs for components that don't need real implementations
# ---------------------------------------------------------------------------


class RecordingReporter(IPipelineReporter):
    """Captures pipeline lifecycle events for assertions."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.completed: list[tuple[str, bool]] = []
        self.final_result: OrchestratorResult | None = None

    @override
    def report_job_started(self, job_id: str) -> None:
        self.started.append(job_id)

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        self.completed.append((job_id, success))

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        self.final_result = result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _docker_available() -> bool:
    """Return True if the Docker daemon is reachable."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.SubprocessError):
        return False


requires_docker = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

integration = pytest.mark.integration


def _build_test_image(context_dir: Path, tag: str) -> None:
    subprocess.run(
        ["docker", "build", "-t", tag, str(context_dir)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _remove_test_image(tag: str) -> None:
    subprocess.run(
        ["docker", "rmi", "-f", tag],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _render_fixture_template(template_path: Path, destination: Path, **values: str) -> Path:
    destination.write_text(template_path.read_text(encoding="utf-8").format(**values), encoding="utf-8")
    return destination


def _run_plan(
    plan_path: Path,
    tmp_path: Path,
    *,
    source_fixture_dir: Path | None = None,
) -> tuple[OrchestratorResult, RecordingReporter, Path]:
    loader = YamlConfigLoader()
    plan = loader.load(plan_path)

    log_dir = tmp_path / "logs"
    output_dir = tmp_path / "output"
    source_dir = tmp_path / "source"
    container_output_root = tmp_path / "container_outputs"
    staging_dir = tmp_path / "staging"
    source_dir.mkdir()
    if source_fixture_dir is not None:
        for path in source_fixture_dir.iterdir():
            if path.is_file():
                shutil.copy2(path, source_dir / path.name)

    prepare_volumes(plan, source_dir, container_output_root)

    job_logger = FileJobLogger(log_dir)
    artifact_store = ArtifactStore(staging_dir, container_output_root)
    reporter = RecordingReporter()
    scheduler = ResourceScheduler(plan)
    executor = DockerExecutor(logger=job_logger, max_workers=plan.max_parallel)

    try:
        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=reporter,
            output_root=output_dir,
        )
        result = engine.run(plan)
    finally:
        executor.shutdown()

    return result, reporter, output_dir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_docker
@integration
class TestFullPipelineFromYaml:
    """Load a YAML config and run the full engine with real Docker containers."""

    def test_happy_path(self, tmp_path: Path) -> None:
        """All four jobs (greeting → farewell, parallel_a → final) succeed."""
        # ---- config ----
        loader = YamlConfigLoader()
        plan = loader.load(FIXTURES_DIR / "integration_plan.yaml")

        assert len(plan.jobs) == 4

        # ---- directories ----
        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()

        # ---- inject system volumes ----
        prepare_volumes(plan, source_dir, container_output_root)

        # ---- wire up real components ----
        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(logger=job_logger, max_workers=plan.max_parallel)

        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=reporter,
            output_root=output_dir,
        )

        # ---- run ----
        result = engine.run(plan)

        # ---- assert overall success ----
        assert result.success is True
        assert len(result.job_results) == 4

        # ---- every job reported as started and completed ----
        assert set(reporter.started) == {"greeting", "farewell", "parallel_a", "final"}
        completed_ids = {jid for jid, _ in reporter.completed}
        assert completed_ids == {"greeting", "farewell", "parallel_a", "final"}
        assert all(ok for _, ok in reporter.completed)

        # ---- reporter got the final result ----
        assert reporter.final_result is not None
        assert reporter.final_result.success is True

        # ---- per-job log files exist ----
        for job_id in ("greeting", "farewell", "parallel_a", "final"):
            log_file = log_dir / f"{job_id}.log"
            assert log_file.exists(), f"Missing log for {job_id}"
            assert log_file.stat().st_size > 0

        # ---- dependency ordering respected ----
        result_map = {r.job_id: r for r in result.job_results}
        for r in result_map.values():
            assert r.success is True
            assert r.exit_code == 0

        executor.shutdown()

    def test_artifacts_collected_through_volume(self, tmp_path: Path) -> None:
        """Containers write to /output, artifacts appear in the final output dir."""
        yaml_path = tmp_path / "artifact_plan.yaml"
        yaml_path.write_text(
            """\
failure_policy: fail_fast
max_parallel: 2
total_cpu_slots: 4
total_memory_slots: 4

jobs:
  - id: producer
    image: alpine:latest
    command: ["sh", "-c", "echo artifact-content > /output/result.txt"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: []
    artifacts:
      - source_glob: "*.txt"
        destination_subdir: results
""",
            encoding="utf-8",
        )

        loader = YamlConfigLoader()
        plan = loader.load(yaml_path)

        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()

        prepare_volumes(plan, source_dir, container_output_root)

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(logger=job_logger, max_workers=plan.max_parallel)

        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=reporter,
            output_root=output_dir,
        )

        result = engine.run(plan)

        assert result.success is True

        # Artifact was staged and finalized to the output directory
        artifact_file = output_dir / "results" / "result.txt"
        assert artifact_file.exists()
        assert artifact_file.read_text().strip() == "artifact-content"

        executor.shutdown()

    def test_source_mounted_read_only(self, tmp_path: Path) -> None:
        """Container can read files from /src (the mounted source dir)."""
        # Write a file into the source dir that the container will read
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "version.txt").write_text("1.2.3")

        yaml_path = tmp_path / "src_plan.yaml"
        yaml_path.write_text(
            """\
failure_policy: fail_fast
max_parallel: 1
total_cpu_slots: 4
total_memory_slots: 4

jobs:
  - id: reader
    image: alpine:latest
    command: ["sh", "-c", "cp /src/version.txt /output/version.txt"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: []
    artifacts:
      - source_glob: "version.txt"
        destination_subdir: meta
""",
            encoding="utf-8",
        )

        loader = YamlConfigLoader()
        plan = loader.load(yaml_path)

        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"

        prepare_volumes(plan, source_dir, container_output_root)

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(logger=job_logger, max_workers=1)

        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=reporter,
            output_root=output_dir,
        )

        result = engine.run(plan)

        assert result.success is True

        # The container read from /src and wrote to /output, artifacts collected
        copied = output_dir / "meta" / "version.txt"
        assert copied.exists()
        assert copied.read_text().strip() == "1.2.3"

        executor.shutdown()

    def test_sccache_redis_backend_restores_from_snapshot(
        self,
        tmp_path: Path,
    ) -> None:
        """The same plan uses a stable Redis host volume so the second run hits the cache on the same host."""
        image_tag = f"build-orch-sccache-test:{uuid4().hex[:12]}"
        _build_test_image(FIXTURES_DIR / "sccache_consumer", image_tag)

        try:
            fixture_dir = FIXTURES_DIR / "sccache_redis"
            redis_host_path = (tmp_path / "redis-cache").resolve()
            redis_host_path.mkdir()

            seed_tmp = tmp_path / "seed"
            seed_tmp.mkdir()
            seed_plan = seed_tmp / "plan.yaml"
            _render_fixture_template(
                fixture_dir / "plan.yaml.tmpl",
                seed_plan,
                network=f"redis-seed-{uuid4().hex[:10]}",
                image_tag=image_tag,
                redis_host_path=redis_host_path.as_posix(),
            )

            seed_result, seed_reporter, seed_output = _run_plan(
                seed_plan,
                seed_tmp,
                source_fixture_dir=fixture_dir,
            )
            assert seed_result.success is True
            assert seed_reporter.final_result is not None
            assert seed_reporter.final_result.success is True

            restored_tmp = tmp_path / "restored"
            restored_tmp.mkdir()
            restore_plan = restored_tmp / "plan.yaml"
            _render_fixture_template(
                fixture_dir / "plan.yaml.tmpl",
                restore_plan,
                network=f"redis-restore-{uuid4().hex[:10]}",
                image_tag=image_tag,
                redis_host_path=redis_host_path.as_posix(),
            )

            restore_result, restore_reporter, restore_output = _run_plan(
                restore_plan,
                restored_tmp,
                source_fixture_dir=fixture_dir,
            )

            assert restore_result.success is True
            assert restore_reporter.final_result is not None
            assert restore_reporter.final_result.success is True

            seed_stats = json.loads((seed_output / "combined" / "sccache-stats.json").read_text())["stats"]
            restore_stats = json.loads((restore_output / "combined" / "sccache-stats.json").read_text())["stats"]

            dump_file = redis_host_path / "dump.rdb"
            assert dump_file.exists()
            assert dump_file.stat().st_size > 0
            assert seed_stats["compile_requests"] >= 1
            assert seed_stats["cache_writes"] >= 1
            assert seed_stats["cache_misses"]["counts"]["C/C++"] >= 1
            assert seed_stats["cache_hits"]["counts"].get("C/C++", 0) == 0
            assert restore_stats["compile_requests"] >= 1
            assert restore_stats["cache_hits"]["counts"]["C/C++"] >= 1
            assert restore_stats["cache_misses"]["counts"].get("C/C++", 0) == 0
        finally:
            _remove_test_image(image_tag)

    def test_failing_job_propagates(self, tmp_path: Path) -> None:
        """A container that exits non-zero causes the orchestration to fail."""
        fail_yaml = tmp_path / "fail_plan.yaml"
        fail_yaml.write_text(
            """\
failure_policy: fail_fast
max_parallel: 2
total_cpu_slots: 4
total_memory_slots: 4

jobs:
  - id: will_fail
    image: alpine:latest
    command: ["sh", "-c", "exit 42"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: []
    artifacts: []

  - id: blocked
    image: alpine:latest
    command: ["echo", "should not run"]
    cpu_slots: 1
    memory_slots: 1
    depends_on: ["will_fail"]
    artifacts: []
""",
            encoding="utf-8",
        )

        loader = YamlConfigLoader()
        plan = loader.load(fail_yaml)

        log_dir = tmp_path / "logs"
        output_dir = tmp_path / "output"
        source_dir = tmp_path / "source"
        container_output_root = tmp_path / "container_outputs"
        staging_dir = tmp_path / "staging"
        source_dir.mkdir()

        prepare_volumes(plan, source_dir, container_output_root)

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(logger=job_logger, max_workers=plan.max_parallel)

        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=reporter,
            output_root=output_dir,
        )

        result = engine.run(plan)

        assert result.success is False

        result_map = {r.job_id: r for r in result.job_results}
        assert result_map["will_fail"].success is False
        assert result_map["will_fail"].exit_code == 42
        assert result_map["blocked"].success is False

        executor.shutdown()
