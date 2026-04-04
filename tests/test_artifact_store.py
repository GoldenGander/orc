"""Tests for the ArtifactStore implementation."""
from __future__ import annotations

import pytest
from pathlib import Path

from orchestrator.artifact_store import ArtifactStore
from orchestrator.exceptions import ArtifactError, ConfigurationError
from orchestrator.models import (
    ArtifactSpec,
    JobResult,
    JobSpec,
    ResourceDriver,
    ResourceLifetime,
    ResourceSpec,
    ResourceWeight,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(
    job_id: str = "job_a",
    artifacts: list[ArtifactSpec] | None = None,
) -> JobSpec:
    return JobSpec(
        id=job_id,
        image="registry/image:latest",
        depends_on=frozenset(),
        resource_weight=ResourceWeight(),
        artifacts=artifacts or [ArtifactSpec(source_glob="*.bin", destination_subdir="out")],
    )


def _make_result(job_id: str = "job_a", tmp_path: Path | None = None) -> JobResult:
    return JobResult(
        job_id=job_id,
        success=True,
        exit_code=0,
        duration_seconds=1.0,
        log_path=(tmp_path or Path("/tmp")) / f"{job_id}.log",
    )


def _make_resource(
    resource_id: str = "redis",
    artifacts: list[ArtifactSpec] | None = None,
) -> ResourceSpec:
    return ResourceSpec(
        id=resource_id,
        kind="cache",
        lifetime=ResourceLifetime.MANAGED,
        driver=ResourceDriver.DOCKER_CONTAINER,
        image="redis:7-alpine",
        artifacts=artifacts or [ArtifactSpec(source_glob="*.txt", destination_subdir="resources")],
    )


def _seed_files(base: Path, job_id: str, filenames: list[str]) -> Path:
    """Create fake output files under base/job_id and return the job dir."""
    job_dir = base / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        (job_dir / name).write_bytes(name.encode())
    return job_dir


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------

class TestCollect:
    def test_collect_copies_matching_files_to_staging(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        _seed_files(output_root, "job_a", ["result.bin", "extra.bin"])

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect(_make_job(), _make_result())

        assert (staging / "out" / "result.bin").read_bytes() == b"result.bin"
        assert (staging / "out" / "extra.bin").read_bytes() == b"extra.bin"

    def test_collect_ignores_non_matching_files(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        _seed_files(output_root, "job_a", ["result.bin", "readme.txt"])

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect(_make_job(), _make_result())

        assert (staging / "out" / "result.bin").exists()
        assert not (staging / "out" / "readme.txt").exists()

    def test_collect_recursive_glob(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        job_dir = output_root / "job_a" / "nested" / "deep"
        job_dir.mkdir(parents=True)
        (job_dir / "data.bin").write_bytes(b"nested")

        job = _make_job(artifacts=[
            ArtifactSpec(source_glob="**/*.bin", destination_subdir="out"),
        ])
        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect(job, _make_result())

        assert (staging / "out" / "nested" / "deep" / "data.bin").read_bytes() == b"nested"

    def test_collect_raises_on_target_collision(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"

        (output_root / "job_a" / "dir").mkdir(parents=True)
        (output_root / "job_a" / "dir" / "report.bin").write_bytes(b"one")
        (output_root / "job_a" / "report.bin").write_bytes(b"two")

        job = _make_job(artifacts=[
            ArtifactSpec(source_glob="report.bin", destination_subdir="dir"),
            ArtifactSpec(source_glob="dir/*.bin", destination_subdir=""),
        ])
        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        with pytest.raises(ArtifactError, match="collision"):
            store.collect(job, _make_result())

        assert not staging.exists() or not any(staging.rglob("*"))

    def test_collect_resource_preserves_recursive_paths(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        resource_dir = output_root / "resources" / "redis" / "nested" / "logs"
        resource_dir.mkdir(parents=True, exist_ok=True)
        (resource_dir / "dump.txt").write_text("ready", encoding="utf-8")

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect_resource(
            _make_resource(
                artifacts=[ArtifactSpec(source_glob="**/*.txt", destination_subdir="resources")]
            )
        )

        assert (staging / "resources" / "nested" / "logs" / "dump.txt").read_text(encoding="utf-8") == "ready"

    def test_collect_multiple_artifact_specs(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        _seed_files(output_root, "job_a", ["lib.dll", "report.xml"])

        job = _make_job(artifacts=[
            ArtifactSpec(source_glob="*.dll", destination_subdir="libs"),
            ArtifactSpec(source_glob="*.xml", destination_subdir="reports"),
        ])
        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect(job, _make_result())

        assert (staging / "libs" / "lib.dll").exists()
        assert (staging / "reports" / "report.xml").exists()

    def test_collect_raises_when_output_dir_missing(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        # Don't create the job output directory

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        with pytest.raises(ArtifactError, match="does not exist"):
            store.collect(_make_job(), _make_result())

    def test_collect_raises_when_glob_matches_nothing(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        _seed_files(output_root, "job_a", ["readme.txt"])  # no .bin files

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        with pytest.raises(ArtifactError, match="matched no files"):
            store.collect(_make_job(), _make_result())

    def test_collect_multiple_jobs_to_same_staging(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        _seed_files(output_root, "job_a", ["a.bin"])
        _seed_files(output_root, "job_b", ["b.bin"])

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect(
            _make_job("job_a", [ArtifactSpec("*.bin", "out")]),
            _make_result("job_a"),
        )
        store.collect(
            _make_job("job_b", [ArtifactSpec("*.bin", "out")]),
            _make_result("job_b"),
        )

        assert (staging / "out" / "a.bin").exists()
        assert (staging / "out" / "b.bin").exists()

    def test_collect_skips_directories_in_glob(self, tmp_path: Path) -> None:
        """Glob should only copy files, not directories that happen to match."""
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        job_dir = output_root / "job_a"
        job_dir.mkdir(parents=True)
        # Create a directory whose name matches the glob
        (job_dir / "tricky.bin").mkdir()
        # And a real file
        (job_dir / "real.bin").write_bytes(b"data")

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect(_make_job(), _make_result())

        assert (staging / "out" / "real.bin").exists()
        assert not (staging / "out" / "tricky.bin").exists()

    def test_collect_resource_copies_matching_files_to_staging(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        resource_dir = output_root / "resources" / "redis"
        resource_dir.mkdir(parents=True, exist_ok=True)
        (resource_dir / "dump.txt").write_text("ready", encoding="utf-8")

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)
        store.collect_resource(_make_resource())

        assert (staging / "resources" / "dump.txt").read_text(encoding="utf-8") == "ready"

    def test_collect_resource_raises_when_glob_matches_nothing(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        resource_dir = output_root / "resources" / "redis"
        resource_dir.mkdir(parents=True, exist_ok=True)

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        with pytest.raises(ArtifactError, match="resource 'redis'"):
            store.collect_resource(_make_resource())

    @pytest.mark.parametrize("job_id", ["../escape", "a/b", "job name", "con"])
    def test_collect_rejects_unsafe_job_ids(self, tmp_path: Path, job_id: str) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        with pytest.raises(ConfigurationError, match="single path component|Windows reserved name"):
            store.collect(_make_job(job_id), _make_result(job_id))

    @pytest.mark.parametrize("resource_id", ["../escape", "a/b", "resource name", "aux"])
    def test_collect_rejects_unsafe_resource_ids(self, tmp_path: Path, resource_id: str) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        with pytest.raises(ConfigurationError, match="single path component|Windows reserved name"):
            store.collect_resource(_make_resource(resource_id))


# ---------------------------------------------------------------------------
# finalize
# ---------------------------------------------------------------------------

class TestFinalize:
    def test_finalize_copies_staging_to_output(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        (staging / "out").mkdir(parents=True)
        (staging / "out" / "artifact.bin").write_bytes(b"payload")

        store = ArtifactStore(staging_dir=staging, container_output_root=tmp_path)
        store.finalize(output)

        assert (output / "out" / "artifact.bin").read_bytes() == b"payload"

    def test_finalize_creates_output_dir(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output = tmp_path / "deep" / "nested" / "output"
        (staging / "data").mkdir(parents=True)
        (staging / "data" / "f.txt").write_bytes(b"x")

        store = ArtifactStore(staging_dir=staging, container_output_root=tmp_path)
        store.finalize(output)

        assert (output / "data" / "f.txt").exists()

    def test_finalize_noop_when_staging_empty(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"  # does not exist
        output = tmp_path / "output"

        store = ArtifactStore(staging_dir=staging, container_output_root=tmp_path)
        store.finalize(output)

        # output dir should not be created when there's nothing to stage
        assert not output.exists()

    def test_finalize_merges_into_existing_output(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output = tmp_path / "output"

        # Pre-existing file in output
        (output / "existing").mkdir(parents=True)
        (output / "existing" / "old.txt").write_bytes(b"old")

        # New file in staging
        (staging / "new_dir").mkdir(parents=True)
        (staging / "new_dir" / "new.txt").write_bytes(b"new")

        store = ArtifactStore(staging_dir=staging, container_output_root=tmp_path)
        store.finalize(output)

        assert (output / "existing" / "old.txt").read_bytes() == b"old"
        assert (output / "new_dir" / "new.txt").read_bytes() == b"new"


# ---------------------------------------------------------------------------
# End-to-end: collect then finalize
# ---------------------------------------------------------------------------

class TestEndToEnd:
    def test_full_collect_and_finalize_flow(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        output_root = tmp_path / "container_output"
        final_output = tmp_path / "publish"

        _seed_files(output_root, "compile", ["app.exe"])
        _seed_files(output_root, "test", ["results.xml"])

        store = ArtifactStore(staging_dir=staging, container_output_root=output_root)

        store.collect(
            _make_job("compile", [ArtifactSpec("*.exe", "binaries")]),
            _make_result("compile"),
        )
        store.collect(
            _make_job("test", [ArtifactSpec("*.xml", "test-results")]),
            _make_result("test"),
        )

        store.finalize(final_output)

        assert (final_output / "binaries" / "app.exe").read_bytes() == b"app.exe"
        assert (final_output / "test-results" / "results.xml").read_bytes() == b"results.xml"
