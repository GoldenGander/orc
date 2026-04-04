"""Tests for the volume preparation layer."""
from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.exceptions import ConfigurationError
from orchestrator.models import (
    ArtifactSpec,
    BuildPlan,
    FailurePolicy,
    JobSpec,
    ResourceDriver,
    ResourceLifetime,
    ResourceSpec,
    ResourceWeight,
    VolumeMount,
)
from orchestrator.volume_prep import (
    CONTAINER_OUTPUT_PATH,
    CONTAINER_SOURCE_PATH,
    RESOURCE_OUTPUT_DIRNAME,
    prepare_volumes,
)


def _job(jid: str, volumes: list[VolumeMount] | None = None) -> JobSpec:
    return JobSpec(
        id=jid,
        image=f"registry/{jid}:latest",
        depends_on=frozenset(),
        resource_weight=ResourceWeight(),
        artifacts=[ArtifactSpec(source_glob="*.bin", destination_subdir=jid)],
        volumes=list(volumes or []),
    )


def _resource(
    resource_id: str,
    volumes: list[VolumeMount] | None = None,
) -> ResourceSpec:
    return ResourceSpec(
        id=resource_id,
        kind="cache",
        lifetime=ResourceLifetime.MANAGED,
        driver=ResourceDriver.DOCKER_CONTAINER,
        image=f"registry/{resource_id}:latest",
        artifacts=[ArtifactSpec(source_glob="*.txt", destination_subdir=resource_id)],
        volumes=list(volumes or []),
    )


def _plan(jobs: list[JobSpec], resources: list[ResourceSpec] | None = None) -> BuildPlan:
    return BuildPlan(
        jobs=jobs,
        failure_policy=FailurePolicy.FAIL_FAST,
        max_parallel=4,
        total_cpu_slots=8,
        total_memory_slots=8,
        resources=resources or [],
    )


class TestPrepareVolumes:
    def test_appends_source_mount_read_only(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([_job("a")])

        prepare_volumes(plan, source, tmp_path / "out")

        source_vols = [v for v in plan.jobs[0].volumes if v.container_path == CONTAINER_SOURCE_PATH]
        assert len(source_vols) == 1
        assert source_vols[0].host_path == str(source)
        assert source_vols[0].read_only is True

    def test_appends_output_mount_read_write(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([_job("a")])

        prepare_volumes(plan, source, tmp_path / "out")

        output_vols = [v for v in plan.jobs[0].volumes if v.container_path == CONTAINER_OUTPUT_PATH]
        assert len(output_vols) == 1
        assert output_vols[0].host_path == str(tmp_path / "out" / "a")
        assert output_vols[0].read_only is False

    def test_creates_per_job_output_directories(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"
        plan = _plan([_job("x"), _job("y")])

        prepare_volumes(plan, source, out_root)

        assert (out_root / "x").is_dir()
        assert (out_root / "y").is_dir()

    def test_preserves_user_declared_volumes(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        user_vol = VolumeMount(host_path="/cache", container_path="/root/.cache")
        plan = _plan([_job("a", volumes=[user_vol])])

        prepare_volumes(plan, source, tmp_path / "out")

        assert plan.jobs[0].volumes[0] is user_vol
        assert len(plan.jobs[0].volumes) == 3  # user + source + output

    def test_each_job_gets_own_output_dir(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([_job("a"), _job("b")])

        prepare_volumes(plan, source, tmp_path / "out")

        out_a = [v for v in plan.jobs[0].volumes if v.container_path == CONTAINER_OUTPUT_PATH][0]
        out_b = [v for v in plan.jobs[1].volumes if v.container_path == CONTAINER_OUTPUT_PATH][0]
        assert out_a.host_path != out_b.host_path
        assert out_a.host_path.endswith("/a") or out_a.host_path.endswith("\\a")
        assert out_b.host_path.endswith("/b") or out_b.host_path.endswith("\\b")

    def test_all_jobs_share_same_source_mount(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([_job("a"), _job("b")])

        prepare_volumes(plan, source, tmp_path / "out")

        src_a = [v for v in plan.jobs[0].volumes if v.container_path == CONTAINER_SOURCE_PATH][0]
        src_b = [v for v in plan.jobs[1].volumes if v.container_path == CONTAINER_SOURCE_PATH][0]
        assert src_a.host_path == src_b.host_path == str(source)

    def test_empty_plan_is_noop(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([])

        prepare_volumes(plan, source, tmp_path / "out")

        assert plan.jobs == []

    def test_appends_output_mount_to_managed_resources(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([], resources=[_resource("redis")])

        prepare_volumes(plan, source, tmp_path / "out")

        output_vols = [v for v in plan.resources[0].volumes if v.container_path == CONTAINER_OUTPUT_PATH]
        assert len(output_vols) == 1
        assert output_vols[0].host_path == str(tmp_path / "out" / RESOURCE_OUTPUT_DIRNAME / "redis")
        assert output_vols[0].read_only is False

    def test_creates_per_resource_output_directories(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"
        plan = _plan([], resources=[_resource("redis"), _resource("queue")])

        prepare_volumes(plan, source, out_root)

        assert (out_root / RESOURCE_OUTPUT_DIRNAME / "redis").is_dir()
        assert (out_root / RESOURCE_OUTPUT_DIRNAME / "queue").is_dir()

    @pytest.mark.parametrize("job_id", ["../escape", "a/b", "job name", "con"])
    def test_rejects_unsafe_job_ids(self, tmp_path: Path, job_id: str) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([_job(job_id)])

        with pytest.raises(ConfigurationError, match="single path component|Windows reserved name"):
            prepare_volumes(plan, source, tmp_path / "out")

    @pytest.mark.parametrize("resource_id", ["../escape", "a/b", "resource name", "aux"])
    def test_rejects_unsafe_resource_ids(self, tmp_path: Path, resource_id: str) -> None:
        source = tmp_path / "src"
        source.mkdir()
        plan = _plan([], resources=[_resource(resource_id)])

        with pytest.raises(ConfigurationError, match="single path component|Windows reserved name"):
            prepare_volumes(plan, source, tmp_path / "out")
