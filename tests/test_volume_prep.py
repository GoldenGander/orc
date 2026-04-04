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
    ResourceSpec,
    ResourceWeight,
    VolumeMount,
)
from orchestrator.volume_prep import (
    CONTAINER_INPUT_PREFIX,
    CONTAINER_OUTPUT_PATH,
    CONTAINER_SOURCE_PATH,
    RESOURCE_OUTPUT_DIRNAME,
    prepare_volumes,
)


def _job(
    jid: str,
    volumes: list[VolumeMount] | None = None,
    input_from: frozenset[str] | None = None,
    depends_on: frozenset[str] | None = None,
) -> JobSpec:
    return JobSpec(
        id=jid,
        image=f"registry/{jid}:latest",
        depends_on=depends_on or frozenset(),
        resource_weight=ResourceWeight(),
        artifacts=[ArtifactSpec(source_glob="*.bin", destination_subdir=jid)],
        input_from=input_from or frozenset(),
        volumes=list(volumes or []),
    )


def _resource(
    resource_id: str,
    volumes: list[VolumeMount] | None = None,
) -> ResourceSpec:
    return ResourceSpec(
        id=resource_id,
        kind="cache",
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

    def test_file_share_not_given_output_mount(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        share = ResourceSpec(
            id="boost",
            driver=ResourceDriver.FILE_SHARE,
            host_path="/mnt/boost",
            container_path="/opt/boost",
            aliases=[],
        )
        plan = _plan([], resources=[share])

        prepare_volumes(plan, source, tmp_path / "out")

        output_vols = [v for v in share.volumes if v.container_path == CONTAINER_OUTPUT_PATH]
        assert output_vols == []

    def test_file_share_injected_as_read_only_mount_into_declaring_job(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        share = ResourceSpec(
            id="boost",
            driver=ResourceDriver.FILE_SHARE,
            host_path="/mnt/boost",
            container_path="/opt/boost",
            aliases=[],
        )
        job = _job("compile")
        job.resources = ["boost"]
        plan = _plan([job], resources=[share])

        prepare_volumes(plan, source, tmp_path / "out")

        share_vols = [v for v in job.volumes if v.container_path == "/opt/boost"]
        assert len(share_vols) == 1
        assert share_vols[0].host_path == "/mnt/boost"
        assert share_vols[0].read_only is True

    def test_job_without_resource_declaration_gets_no_share_mount(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        share = ResourceSpec(
            id="boost",
            driver=ResourceDriver.FILE_SHARE,
            host_path="/mnt/boost",
            container_path="/opt/boost",
            aliases=[],
        )
        job = _job("compile")
        plan = _plan([job], resources=[share])

        prepare_volumes(plan, source, tmp_path / "out")

        share_vols = [v for v in job.volumes if v.container_path == "/opt/boost"]
        assert share_vols == []

    def test_multiple_file_shares_injected_for_single_job(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        boost = ResourceSpec(
            id="boost",
            driver=ResourceDriver.FILE_SHARE,
            host_path="/mnt/boost",
            container_path="/opt/boost",
            aliases=[],
        )
        tools = ResourceSpec(
            id="tools",
            driver=ResourceDriver.FILE_SHARE,
            host_path="/mnt/tools",
            container_path="/opt/tools",
            aliases=[],
        )
        job = _job("compile")
        job.resources = ["boost", "tools"]
        plan = _plan([job], resources=[boost, tools])

        prepare_volumes(plan, source, tmp_path / "out")

        mounted = {v.container_path: v for v in job.volumes}
        assert "/opt/boost" in mounted
        assert mounted["/opt/boost"].host_path == "/mnt/boost"
        assert mounted["/opt/boost"].read_only is True
        assert "/opt/tools" in mounted
        assert mounted["/opt/tools"].host_path == "/mnt/tools"
        assert mounted["/opt/tools"].read_only is True


class TestInputFromVolumes:
    def test_input_from_mount_is_read_only_at_input_prefix(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        compile_job = _job("compile")
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))
        plan = _plan([compile_job, opt_job])

        prepare_volumes(plan, source, tmp_path / "out")

        input_vols = [v for v in opt_job.volumes if v.container_path.startswith(CONTAINER_INPUT_PREFIX)]
        assert len(input_vols) == 1
        assert input_vols[0].container_path == f"{CONTAINER_INPUT_PREFIX}/compile"
        assert input_vols[0].host_path == str(tmp_path / "out" / "compile")
        assert input_vols[0].read_only is True

    def test_input_from_host_path_matches_source_job_output_dir(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"
        compile_job = _job("compile")
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))
        plan = _plan([compile_job, opt_job])

        prepare_volumes(plan, source, out_root)

        compile_output_vol = next(
            v for v in compile_job.volumes if v.container_path == CONTAINER_OUTPUT_PATH
        )
        input_vol = next(
            v for v in opt_job.volumes if v.container_path == f"{CONTAINER_INPUT_PREFIX}/compile"
        )
        assert input_vol.host_path == compile_output_vol.host_path

    def test_input_from_dir_created_even_when_source_listed_after_consumer(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"
        # opt listed before compile in jobs list — source dir must still be pre-created
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))
        compile_job = _job("compile")
        plan = _plan([opt_job, compile_job])

        prepare_volumes(plan, source, out_root)

        assert (out_root / "compile").is_dir()

    def test_multiple_input_from_mount_to_separate_paths(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        job_a = _job("job-a")
        job_b = _job("job-b")
        consumer = _job(
            "consumer",
            input_from=frozenset({"job-a", "job-b"}),
            depends_on=frozenset({"job-a", "job-b"}),
        )
        plan = _plan([job_a, job_b, consumer])

        prepare_volumes(plan, source, tmp_path / "out")

        input_vols = {
            v.container_path: v
            for v in consumer.volumes
            if v.container_path.startswith(CONTAINER_INPUT_PREFIX)
        }
        assert f"{CONTAINER_INPUT_PREFIX}/job-a" in input_vols
        assert f"{CONTAINER_INPUT_PREFIX}/job-b" in input_vols
        assert input_vols[f"{CONTAINER_INPUT_PREFIX}/job-a"].read_only is True
        assert input_vols[f"{CONTAINER_INPUT_PREFIX}/job-b"].read_only is True

    def test_job_without_input_from_gets_no_input_mounts(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        job = _job("standalone")
        plan = _plan([job])

        prepare_volumes(plan, source, tmp_path / "out")

        input_vols = [v for v in job.volumes if v.container_path.startswith(CONTAINER_INPUT_PREFIX)]
        assert input_vols == []

    def test_input_from_does_not_affect_source_jobs_volumes(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        compile_job = _job("compile")
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))
        plan = _plan([compile_job, opt_job])

        prepare_volumes(plan, source, tmp_path / "out")

        # compile only gets source + output, no input mounts
        input_vols = [
            v for v in compile_job.volumes if v.container_path.startswith(CONTAINER_INPUT_PREFIX)
        ]
        assert input_vols == []
        assert len(compile_job.volumes) == 2  # source + output
