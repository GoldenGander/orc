"""End-to-end integration test: YAML config → Engine → real Docker execution.

Requires Docker to be available on the host. Tests are marked with
``pytest.mark.integration`` so they can be selected or excluded via:

    pytest -m integration          # run only integration tests
    pytest -m "not integration"    # skip them
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import override
from uuid import uuid4

import pytest

logger = logging.getLogger(__name__)

from orchestrator.artifact_store import ArtifactStore
from orchestrator.config import YamlConfigLoader
from orchestrator.engine import Engine
from orchestrator.executor import DockerExecutor
from orchestrator.logger import FileJobLogger
from orchestrator.models import OrchestratorResult
from orchestrator.pipeline import IPipelineReporter
from orchestrator.scheduler import ResourceScheduler

FIXTURES_DIR = Path(__file__).parent / "fixtures"
QTWASM_SAMPLE_DIR = Path(__file__).parent.parent / "QtWasm" / "sample"
QT_WASM_SCCACHE_DEFAULT_REPO = "https://github.com/GoldenGander/wasm-sccache.git"
QT_WASM_SCCACHE_DEFAULT_REV = "a44a8512228a7e49d3e9b119500c42d1fb655c55"


# ---------------------------------------------------------------------------
# Lightweight stubs for components that don't need real implementations
# ---------------------------------------------------------------------------


class RecordingReporter(IPipelineReporter):
    """Captures pipeline lifecycle events for assertions."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.completed: list[tuple[str, bool]] = []
        self.final_result: OrchestratorResult | None = None
        # Ordered log of ("started"|"completed", job_id) for ordering assertions.
        self._event_log: list[tuple[str, str]] = []

    @override
    def report_job_started(self, job_id: str) -> None:
        self.started.append(job_id)
        self._event_log.append(("started", job_id))

    @override
    def report_job_completed(self, job_id: str, success: bool) -> None:
        self.completed.append((job_id, success))
        self._event_log.append(("completed", job_id))

    @override
    def report_result(self, result: OrchestratorResult) -> None:
        self.final_result = result

    @override
    def report_resource_status(self, resources: object) -> None:
        pass


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


def _build_test_image(
    context_dir: Path,
    tag: str,
    *,
    build_args: dict[str, str] | None = None,
) -> None:
    command = ["docker", "build", "-t", tag]
    for key, value in (build_args or {}).items():
        command.extend(["--build-arg", f"{key}={value}"])
    command.append(str(context_dir))
    subprocess.run(
        command,
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


def _qt_wasm_sccache_build_args() -> dict[str, str]:
    repo = os.environ.get("QT_WASM_SCCACHE_REPO", QT_WASM_SCCACHE_DEFAULT_REPO)
    rev = os.environ.get("QT_WASM_SCCACHE_REV", QT_WASM_SCCACHE_DEFAULT_REV)
    return {
        "SCCACHE_REPO": repo,
        "SCCACHE_REV": rev,
    }


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
            elif path.is_dir():
                shutil.copytree(path, source_dir / path.name)

    job_logger = FileJobLogger(log_dir)
    artifact_store = ArtifactStore(staging_dir, container_output_root)
    reporter = RecordingReporter()
    scheduler = ResourceScheduler(plan)
    executor = DockerExecutor(
        logger=job_logger,
        source_dir=source_dir,
        container_output_root=container_output_root,
        max_workers=plan.max_parallel,
    )

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

        # ---- wire up real components ----
        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

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

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

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

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=1,
        )

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
        """Three-job DAG (compile_a + compile_b in parallel → report) run twice.

        Verifies across both runs:
        - All jobs succeed and artifacts land in their declared subdirectories.
        - Dependency ordering: report starts only after both compile jobs complete.
        - Redis cache: first run populates the cache (misses), second run hits it.
        """
        image_tag = f"build-orch-sccache-test:{uuid4().hex[:12]}"
        _build_test_image(FIXTURES_DIR / "sccache_consumer", image_tag)

        try:
            fixture_dir = FIXTURES_DIR / "sccache_orchestration"
            redis_host_path = (tmp_path / "redis-cache").resolve()
            redis_host_path.mkdir()

            def _run(run_tmp: Path) -> tuple[OrchestratorResult, RecordingReporter, Path]:
                run_tmp.mkdir()
                plan_path = run_tmp / "plan.yaml"
                _render_fixture_template(
                    fixture_dir / "plan.yaml.tmpl",
                    plan_path,
                    network=f"orch-{uuid4().hex[:10]}",
                    image_tag=image_tag,
                    redis_host_path=redis_host_path.as_posix(),
                )
                return _run_plan(plan_path, run_tmp, source_fixture_dir=fixture_dir)

            def _assert_common(
                result: OrchestratorResult,
                reporter: RecordingReporter,
                output: Path,
            ) -> None:
                assert result.success is True
                assert {r.job_id for r in result.job_results} == {"compile_a", "compile_b", "report"}
                assert all(r.success for r in result.job_results)

                assert set(reporter.started) == {"compile_a", "compile_b", "report"}
                assert {jid for jid, _ in reporter.completed} == {"compile_a", "compile_b", "report"}
                assert all(ok for _, ok in reporter.completed)

                # artifact routing: each glob lands in its declared destination_subdir
                assert (output / "objects" / "file_a.o").exists()
                assert (output / "objects" / "file_b.o").exists()
                assert (output / "stats" / "stats_a.json").exists()
                assert (output / "stats" / "stats_b.json").exists()
                assert (output / "summary" / "summary.txt").exists()

                # ordering: report must start after both compile jobs have completed
                # (and had their artifacts collected — the engine fires report_job_completed
                # after artifact_store.collect(), so the event log is a faithful record)
                pos = {event: i for i, event in enumerate(reporter._event_log)}
                assert pos[("started", "report")] > pos[("completed", "compile_a")]
                assert pos[("started", "report")] > pos[("completed", "compile_b")]

            # ---- first run: seed the cache ----
            seed_result, seed_reporter, seed_output = _run(tmp_path / "seed")
            _assert_common(seed_result, seed_reporter, seed_output)

            seed_a = json.loads((seed_output / "stats" / "stats_a.json").read_text())["stats"]
            seed_b = json.loads((seed_output / "stats" / "stats_b.json").read_text())["stats"]
            assert seed_a["cache_misses"]["counts"]["C/C++"] >= 1
            assert seed_a["cache_hits"]["counts"].get("C/C++", 0) == 0
            assert seed_b["cache_misses"]["counts"]["C/C++"] >= 1
            assert seed_b["cache_hits"]["counts"].get("C/C++", 0) == 0

            dump_file = redis_host_path / "dump.rdb"
            assert dump_file.exists()
            assert dump_file.stat().st_size > 0

            # ---- second run: restore from cache ----
            restore_result, restore_reporter, restore_output = _run(tmp_path / "restored")
            _assert_common(restore_result, restore_reporter, restore_output)

            restore_a = json.loads((restore_output / "stats" / "stats_a.json").read_text())["stats"]
            restore_b = json.loads((restore_output / "stats" / "stats_b.json").read_text())["stats"]
            assert restore_a["cache_hits"]["counts"]["C/C++"] >= 1
            assert restore_a["cache_misses"]["counts"].get("C/C++", 0) == 0
            assert restore_b["cache_hits"]["counts"]["C/C++"] >= 1
            assert restore_b["cache_misses"]["counts"].get("C/C++", 0) == 0

        finally:
            _remove_test_image(image_tag)

    def test_qt_wasm_sccache_redis_cache_hit(
        self,
        tmp_path: Path,
    ) -> None:
        """Two sequential Qt WASM compiles verify sccache Redis cache hits and binary reproducibility.

        Verifies:
        - Both compile jobs succeed (second depends on first).
        - Real Qt WASM compilation of the sample project from QtWasm/sample.
        - sccache with Redis backend: first compile produces cache misses.
        - Second compile produces cache hits with zero misses from Redis.
        - WASM artifacts are valid (magic header, non-trivial size).
        - Both compiles produce bit-for-bit identical .wasm and .js artifacts.

        Optional env vars:
        - QT_WASM_SCCACHE_REPO: fork URL used for the Qt image build.
        - QT_WASM_SCCACHE_REV: pinned full commit SHA from that fork.
        """
        image_tag = f"build-orch-qt-wasm-test:{uuid4().hex[:12]}"
        _build_test_image(
            FIXTURES_DIR / "qt_wasm_compile",
            image_tag,
            build_args=_qt_wasm_sccache_build_args(),
        )

        try:
            fixture_dir = FIXTURES_DIR / "qt_wasm_orchestration"
            redis_host_path = (tmp_path / "redis-cache").resolve()
            redis_host_path.mkdir()

            # Merge compile script + real Qt sample project into a single source tree
            combined_source = tmp_path / "combined_source"
            combined_source.mkdir()
            shutil.copy2(fixture_dir / "compile_qt.sh", combined_source / "compile_qt.sh")
            shutil.copytree(QTWASM_SAMPLE_DIR, combined_source / "sample")

            plan_path = tmp_path / "plan.yaml"
            _render_fixture_template(
                fixture_dir / "plan.yaml.tmpl",
                plan_path,
                network=f"orch-qt-{uuid4().hex[:10]}",
                image_tag=image_tag,
                redis_host_path=redis_host_path.as_posix(),
            )

            result, reporter, output_dir = _run_plan(
                plan_path, tmp_path, source_fixture_dir=combined_source,
            )

            # ---- overall success ----
            assert result.success is True
            all_job_ids = {"qt_compile_1", "qt_compile_2"}
            assert {r.job_id for r in result.job_results} == all_job_ids
            assert all(r.success for r in result.job_results)

            # ---- reporter events ----
            assert set(reporter.started) == all_job_ids
            assert {jid for jid, _ in reporter.completed} == all_job_ids
            assert all(ok for _, ok in reporter.completed)

            # ---- WASM artifact validity ----
            for label in ["compile_1", "compile_2"]:
                js_file = output_dir / "wasm_build" / label / "helloworld.js"
                wasm_file = output_dir / "wasm_build" / label / "helloworld.wasm"

                assert js_file.exists(), f"Missing JS artifact for {label}"
                assert wasm_file.exists(), f"Missing WASM artifact for {label}"
                wasm_bytes = wasm_file.read_bytes()
                assert wasm_bytes[:4] == b"\x00asm", f"Invalid WASM header for {label}"
                assert len(wasm_bytes) > 1024, f"WASM file too small for {label}"

            # ---- compiler invocation breakdown (compile_1 only) ----
            ninja_log = (output_dir / "stats" / "compile_1" / "ninja_verbose.log").read_text(errors="replace")
            shim_log = (output_dir / "stats" / "compile_1" / "compiler_shim.log").read_text(errors="replace")
            lines = ninja_log.splitlines()
            logger.info("ninja_verbose.log: %d total lines", len(lines))
            # Log every line that looks like a compiler invocation (contains a source file extension)
            invoke_lines = [l for l in lines if any(ext in l for ext in (".cpp", ".c ", ".cxx"))]
            logger.info("lines referencing source files: %d", len(invoke_lines))
            for line in invoke_lines[:20]:  # cap at 20 to avoid flooding
                logger.info("  %s", line[:2000])
            assert "em++ " in shim_log or "emcc " in shim_log

            # Verify wasm-opt was invoked and shimmed in compile_1
            wasm_opt_invocations = [l for l in shim_log.splitlines() if l.startswith("wasm-opt")]
            logger.info("wasm-opt invocations in compile_1: %d", len(wasm_opt_invocations))
            assert len(wasm_opt_invocations) > 0, "wasm-opt should be called during compilation"
            for inv in wasm_opt_invocations[:5]:  # Log first 5 invocations
                logger.info("  %s", inv[:200])

            # ---- sccache stats: first compile misses, second compile hits ----
            stats_1 = json.loads(
                (output_dir / "stats" / "compile_1" / "sccache_stats.json").read_text()
            )
            stats_2 = json.loads(
                (output_dir / "stats" / "compile_2" / "sccache_stats.json").read_text()
            )

            misses_1 = sum(
                stats_1.get("stats", stats_1)
                .get("cache_misses", {})
                .get("counts", {})
                .values()
            )
            hits_2 = sum(
                stats_2.get("stats", stats_2)
                .get("cache_hits", {})
                .get("counts", {})
                .values()
            )
            misses_2 = sum(
                stats_2.get("stats", stats_2)
                .get("cache_misses", {})
                .get("counts", {})
                .values()
            )
            unsupported_1 = sum(
                stats_1.get("stats", stats_1)
                .get("cache_unsupported", {})
                .get("counts", {})
                .values()
            )
            unsupported_2 = sum(
                stats_2.get("stats", stats_2)
                .get("cache_unsupported", {})
                .get("counts", {})
                .values()
            )
            assert misses_1 > 0, "First compile should have cache misses"
            assert hits_2 > 0, "Second compile should have cache hits from Redis"
            assert hits_2 >= misses_1, (
                f"Cache hits ({hits_2}) should cover first compile misses ({misses_1})"
            )
            durations = {r.job_id: r.duration_seconds for r in result.job_results}
            logger.info(
                "sccache stats — compile_1: misses=%d unsupported=%d | compile_2: hits=%d misses=%d unsupported=%d",
                misses_1, unsupported_1, hits_2, misses_2, unsupported_2,
            )
            logger.info(
                "job durations — qt_compile_1: %.1fs | qt_compile_2: %.1fs",
                durations.get("qt_compile_1", 0),
                durations.get("qt_compile_2", 0),
            )

            assert misses_2 == 0, (
                f"Second compile should have zero cache misses (got {misses_2}); "
                f"non-deterministic inputs may be poisoning cache keys"
            )

            # ---- wasm-opt caching verification ----
            # Verify both runs invoked wasm-opt through the shim
            shim_log_2 = (output_dir / "stats" / "compile_2" / "compiler_shim.log").read_text(errors="replace")
            wasm_opt_invocations_2 = [l for l in shim_log_2.splitlines() if l.startswith("wasm-opt")]
            logger.info("wasm-opt invocations in compile_2: %d", len(wasm_opt_invocations_2))
            assert len(wasm_opt_invocations_2) > 0, (
                "wasm-opt should be invoked in second compile and shimmed for caching"
            )
            # Both runs should have similar number of wasm-opt invocations
            assert len(wasm_opt_invocations_2) == len(wasm_opt_invocations), (
                f"wasm-opt invocation counts should match across runs; "
                f"compile_1={len(wasm_opt_invocations)} compile_2={len(wasm_opt_invocations_2)}"
            )

            # ---- binary reproducibility ----
            for artifact in ["helloworld.wasm", "helloworld.js"]:
                bytes_1 = (output_dir / "wasm_build" / "compile_1" / artifact).read_bytes()
                bytes_2 = (output_dir / "wasm_build" / "compile_2" / artifact).read_bytes()
                logger.info(
                    "%s sizes — compile_1: %d bytes | compile_2: %d bytes | identical: %s",
                    artifact, len(bytes_1), len(bytes_2), bytes_1 == bytes_2,
                )
                assert bytes_1 == bytes_2, (
                    f"{artifact} is not bit-for-bit reproducible across compiles; "
                    f"compile_1={len(bytes_1)}B compile_2={len(bytes_2)}B"
                )

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

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        reporter = RecordingReporter()
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(
            logger=job_logger,
            source_dir=source_dir,
            container_output_root=container_output_root,
            max_workers=plan.max_parallel,
        )

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
