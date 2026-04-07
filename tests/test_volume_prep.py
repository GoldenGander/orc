"""Tests for the volume computation helpers."""
from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.exceptions import ConfigurationError
from orchestrator.models import (
    ArtifactSpec,
    ContainerOS,
    JobSpec,
    ResourceDriver,
    ResourceSpec,
    ResourceWeight,
    VolumeMount,
)
from orchestrator.volume_prep import (
    LINUX_CONTAINER_INPUT_PREFIX,
    LINUX_CONTAINER_OUTPUT_PATH,
    LINUX_CONTAINER_SOURCE_PATH,
    RESOURCE_OUTPUT_DIRNAME,
    compute_job_volumes,
    compute_resource_output_volume,
)


def _job(
    jid: str,
    volumes: list[VolumeMount] | None = None,
    input_from: frozenset[str] | None = None,
    depends_on: frozenset[str] | None = None,
    container_os: ContainerOS = ContainerOS.LINUX,
) -> JobSpec:
    return JobSpec(
        id=jid,
        image=f"registry/{jid}:latest",
        depends_on=depends_on or frozenset(),
        resource_weight=ResourceWeight(),
        artifacts=[ArtifactSpec(source_glob="*.bin", destination_subdir=jid)],
        input_from=input_from or frozenset(),
        volumes=list(volumes or []),
        container_os=container_os,
    )


def _file_share(
    resource_id: str,
    host_path: str = "/mnt/share",
    container_path: str = "/opt/share",
) -> ResourceSpec:
    return ResourceSpec(
        id=resource_id,
        kind="cache",
        driver=ResourceDriver.FILE_SHARE,
        host_path=host_path,
        container_path=container_path,
        aliases=[],
    )


def _managed_resource(resource_id: str) -> ResourceSpec:
    return ResourceSpec(
        id=resource_id,
        kind="cache",
        driver=ResourceDriver.DOCKER_CONTAINER,
        image=f"registry/{resource_id}:latest",
        artifacts=[ArtifactSpec(source_glob="*.txt", destination_subdir=resource_id)],
    )


class TestComputeJobVolumes:
    def test_source_mount_is_read_only(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        job = _job("a")

        vols = compute_job_volumes(job, source, tmp_path / "out", {})

        source_vols = [v for v in vols if v.container_path == LINUX_CONTAINER_SOURCE_PATH]
        assert len(source_vols) == 1
        assert source_vols[0].host_path == str(source)
        assert source_vols[0].read_only is True

    def test_output_mount_is_read_write(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        job = _job("a")

        vols = compute_job_volumes(job, source, tmp_path / "out", {})

        output_vols = [v for v in vols if v.container_path == LINUX_CONTAINER_OUTPUT_PATH]
        assert len(output_vols) == 1
        assert output_vols[0].host_path == str(tmp_path / "out" / "a")
        assert output_vols[0].read_only is False

    def test_windows_job_uses_windows_mount_paths(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        job = _job("a", container_os=ContainerOS.WINDOWS)

        vols = compute_job_volumes(job, source, tmp_path / "out", {})

        mounted = {v.container_path: v for v in vols}
        assert r"C:\src" in mounted
        assert mounted[r"C:\src"].host_path == str(source)
        assert mounted[r"C:\src"].read_only is True
        assert r"C:\output" in mounted
        assert mounted[r"C:\output"].host_path == str(tmp_path / "out" / "a")
        assert mounted[r"C:\output"].read_only is False

    def test_creates_per_job_output_directory(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"

        compute_job_volumes(_job("x"), source, out_root, {})
        compute_job_volumes(_job("y"), source, out_root, {})

        assert (out_root / "x").is_dir()
        assert (out_root / "y").is_dir()

    def test_each_job_gets_own_output_dir(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()

        vols_a = compute_job_volumes(_job("a"), source, tmp_path / "out", {})
        vols_b = compute_job_volumes(_job("b"), source, tmp_path / "out", {})

        out_a = next(v for v in vols_a if v.container_path == LINUX_CONTAINER_OUTPUT_PATH)
        out_b = next(v for v in vols_b if v.container_path == LINUX_CONTAINER_OUTPUT_PATH)
        assert out_a.host_path != out_b.host_path
        assert out_a.host_path.endswith("/a") or out_a.host_path.endswith("\\a")
        assert out_b.host_path.endswith("/b") or out_b.host_path.endswith("\\b")

    def test_all_jobs_share_same_source_mount(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()

        vols_a = compute_job_volumes(_job("a"), source, tmp_path / "out", {})
        vols_b = compute_job_volumes(_job("b"), source, tmp_path / "out", {})

        src_a = next(v for v in vols_a if v.container_path == LINUX_CONTAINER_SOURCE_PATH)
        src_b = next(v for v in vols_b if v.container_path == LINUX_CONTAINER_SOURCE_PATH)
        assert src_a.host_path == src_b.host_path == str(source)

    def test_does_not_include_user_declared_volumes(self, tmp_path: Path) -> None:
        """compute_job_volumes returns only system volumes; user volumes live on job.volumes."""
        source = tmp_path / "src"
        source.mkdir()
        user_vol = VolumeMount(host_path="/cache", container_path="/root/.cache")
        job = _job("a", volumes=[user_vol])

        vols = compute_job_volumes(job, source, tmp_path / "out", {})

        assert user_vol not in vols
        assert len(vols) == 2  # source + output only

    def test_file_share_injected_for_declaring_job(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        share = _file_share("boost", host_path="/mnt/boost", container_path="/opt/boost")
        job = _job("compile")
        job.resources = ["boost"]

        vols = compute_job_volumes(job, source, tmp_path / "out", {"boost": share})

        share_vols = [v for v in vols if v.container_path == "/opt/boost"]
        assert len(share_vols) == 1
        assert share_vols[0].host_path == "/mnt/boost"
        assert share_vols[0].read_only is True

    def test_job_without_resource_declaration_gets_no_share_mount(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        share = _file_share("boost", host_path="/mnt/boost", container_path="/opt/boost")
        job = _job("compile")  # no job.resources = ["boost"]

        vols = compute_job_volumes(job, source, tmp_path / "out", {"boost": share})

        share_vols = [v for v in vols if v.container_path == "/opt/boost"]
        assert share_vols == []

    def test_multiple_file_shares_injected(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        boost = _file_share("boost", host_path="/mnt/boost", container_path="/opt/boost")
        tools = _file_share("tools", host_path="/mnt/tools", container_path="/opt/tools")
        job = _job("compile")
        job.resources = ["boost", "tools"]

        vols = compute_job_volumes(
            job, source, tmp_path / "out", {"boost": boost, "tools": tools}
        )

        mounted = {v.container_path: v for v in vols}
        assert "/opt/boost" in mounted
        assert mounted["/opt/boost"].host_path == "/mnt/boost"
        assert mounted["/opt/boost"].read_only is True
        assert "/opt/tools" in mounted
        assert mounted["/opt/tools"].host_path == "/mnt/tools"
        assert mounted["/opt/tools"].read_only is True

    @pytest.mark.parametrize("job_id", ["../escape", "a/b", "job name", "con"])
    def test_rejects_unsafe_job_ids(self, tmp_path: Path, job_id: str) -> None:
        source = tmp_path / "src"
        source.mkdir()

        with pytest.raises(ConfigurationError, match="single path component|Windows reserved name"):
            compute_job_volumes(_job(job_id), source, tmp_path / "out", {})


class TestInputFromVolumes:
    def test_input_from_mount_is_read_only_at_input_prefix(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))

        vols = compute_job_volumes(opt_job, source, tmp_path / "out", {})

        input_vols = [v for v in vols if v.container_path.startswith(LINUX_CONTAINER_INPUT_PREFIX)]
        assert len(input_vols) == 1
        assert input_vols[0].container_path == f"{LINUX_CONTAINER_INPUT_PREFIX}/compile"
        assert input_vols[0].host_path == str(tmp_path / "out" / "compile")
        assert input_vols[0].read_only is True

    def test_input_from_host_path_matches_source_job_output_dir(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"
        compile_job = _job("compile")
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))

        compile_vols = compute_job_volumes(compile_job, source, out_root, {})
        opt_vols = compute_job_volumes(opt_job, source, out_root, {})

        compile_output = next(v for v in compile_vols if v.container_path == LINUX_CONTAINER_OUTPUT_PATH)
        input_vol = next(
            v for v in opt_vols if v.container_path == f"{LINUX_CONTAINER_INPUT_PREFIX}/compile"
        )
        assert input_vol.host_path == compile_output.host_path

    def test_input_from_dir_created_even_when_consumer_computed_first(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        out_root = tmp_path / "out"
        # opt computed before compile — source dir must still be pre-created
        opt_job = _job("opt", input_from=frozenset({"compile"}), depends_on=frozenset({"compile"}))

        compute_job_volumes(opt_job, source, out_root, {})

        assert (out_root / "compile").is_dir()

    def test_multiple_input_from_mount_to_separate_paths(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()
        consumer = _job(
            "consumer",
            input_from=frozenset({"job-a", "job-b"}),
            depends_on=frozenset({"job-a", "job-b"}),
        )

        vols = compute_job_volumes(consumer, source, tmp_path / "out", {})

        input_vols = {
            v.container_path: v
            for v in vols
            if v.container_path.startswith(LINUX_CONTAINER_INPUT_PREFIX)
        }
        assert f"{LINUX_CONTAINER_INPUT_PREFIX}/job-a" in input_vols
        assert f"{LINUX_CONTAINER_INPUT_PREFIX}/job-b" in input_vols
        assert input_vols[f"{LINUX_CONTAINER_INPUT_PREFIX}/job-a"].read_only is True
        assert input_vols[f"{LINUX_CONTAINER_INPUT_PREFIX}/job-b"].read_only is True

    def test_job_without_input_from_gets_no_input_mounts(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        source.mkdir()

        vols = compute_job_volumes(_job("standalone"), source, tmp_path / "out", {})

        assert not any(v.container_path.startswith(LINUX_CONTAINER_INPUT_PREFIX) for v in vols)


class TestComputeResourceOutputVolume:
    def test_output_mount_is_read_write(self, tmp_path: Path) -> None:
        resource = _managed_resource("redis")

        vol = compute_resource_output_volume(resource, tmp_path / "out", ContainerOS.LINUX)

        assert vol.container_path == LINUX_CONTAINER_OUTPUT_PATH
        assert vol.host_path == str(tmp_path / "out" / RESOURCE_OUTPUT_DIRNAME / "redis")
        assert vol.read_only is False

    def test_creates_resource_output_directory(self, tmp_path: Path) -> None:
        out_root = tmp_path / "out"

        compute_resource_output_volume(_managed_resource("redis"), out_root, ContainerOS.LINUX)
        compute_resource_output_volume(_managed_resource("queue"), out_root, ContainerOS.LINUX)

        assert (out_root / RESOURCE_OUTPUT_DIRNAME / "redis").is_dir()
        assert (out_root / RESOURCE_OUTPUT_DIRNAME / "queue").is_dir()

    def test_file_share_not_given_output_volume(self, tmp_path: Path) -> None:
        """FILE_SHARE resources are not passed to compute_resource_output_volume."""
        # This is enforced by the caller (DockerExecutor only calls it for DOCKER_CONTAINER).
        # If called anyway it should still work (no functional restriction in the helper).
        share = ResourceSpec(
            id="boost",
            driver=ResourceDriver.FILE_SHARE,
            host_path="/mnt/boost",
            container_path="/opt/boost",
            aliases=[],
        )
        vol = compute_resource_output_volume(share, tmp_path / "out", ContainerOS.LINUX)

        assert vol.container_path == LINUX_CONTAINER_OUTPUT_PATH

    @pytest.mark.parametrize("resource_id", ["../escape", "a/b", "resource name", "aux"])
    def test_rejects_unsafe_resource_ids(self, tmp_path: Path, resource_id: str) -> None:
        resource = ResourceSpec(
            id=resource_id,
            driver=ResourceDriver.DOCKER_CONTAINER,
            image="some:image",
            aliases=[],
        )
        with pytest.raises(ConfigurationError, match="single path component|Windows reserved name"):
            compute_resource_output_volume(resource, tmp_path / "out", ContainerOS.LINUX)
