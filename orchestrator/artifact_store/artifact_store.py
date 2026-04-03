from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from orchestrator.exceptions import ArtifactError
from orchestrator.models import JobResult, JobSpec


class ArtifactStoreABC(ABC):
    """Collects and assembles build artifacts from completed jobs.

    collect() is called per-job on success; finalize() is called once after
    all jobs complete to assemble the unified output directory that the Azure
    DevOps publish step picks up.
    """

    @abstractmethod
    def collect(self, job: JobSpec, result: JobResult) -> None:
        """Stage artifacts from a completed job.

        Implementations resolve the source globs declared in job.artifacts
        and copy the matched files into a staging area, preserving the
        destination_subdir structure from each ArtifactSpec.

        Raises:
            ArtifactError: If a required glob matches no files or a copy fails.
        """
        ...

    @abstractmethod
    def finalize(self, output_root: Path) -> None:
        """Move all staged artifacts into the unified output directory.

        Called once after all jobs complete, regardless of failure policy.
        output_root is the directory the Azure DevOps publish step targets.

        Raises:
            ArtifactError: If the output directory cannot be written.
        """
        ...


class ArtifactStore(ArtifactStoreABC):
    """Concrete artifact store that stages files on the local filesystem.

    The store resolves each job's artifact globs against a per-job output
    directory under ``container_output_root/<job_id>``.  Matched files are
    copied into ``staging_dir/<destination_subdir>`` during collect(), then
    the entire staging tree is copied to the pipeline's output directory
    during finalize().

    Args:
        staging_dir: Temporary directory for accumulating artifacts.
        container_output_root: Base path where each container writes output.
            Each job's artifacts are resolved under
            ``container_output_root/<job_id>``.
    """

    def __init__(self, staging_dir: Path, container_output_root: Path) -> None:
        self._staging_dir = staging_dir
        self._container_output_root = container_output_root

    @property
    def staging_dir(self) -> Path:
        return self._staging_dir

    @property
    def container_output_root(self) -> Path:
        return self._container_output_root

    def collect(self, job: JobSpec, result: JobResult) -> None:
        job_output = self._container_output_root / job.id

        if not job_output.is_dir():
            raise ArtifactError(
                f"Container output directory does not exist for job '{job.id}': "
                f"{job_output}"
            )

        for spec in job.artifacts:
            dest = self._staging_dir / spec.destination_subdir
            dest.mkdir(parents=True, exist_ok=True)

            matches = sorted(p for p in job_output.glob(spec.source_glob) if p.is_file())

            if not matches:
                raise ArtifactError(
                    f"Artifact glob '{spec.source_glob}' matched no files "
                    f"for job '{job.id}' under {job_output}"
                )

            for match in matches:
                try:
                    shutil.copy2(match, dest / match.name)
                except OSError as exc:
                    raise ArtifactError(
                        f"Failed to stage artifact '{match}' for job '{job.id}': {exc}"
                    ) from exc

    def finalize(self, output_root: Path) -> None:
        if not self._staging_dir.exists():
            return

        try:
            output_root.mkdir(parents=True, exist_ok=True)
            shutil.copytree(self._staging_dir, output_root, dirs_exist_ok=True)
        except OSError as exc:
            raise ArtifactError(
                f"Failed to finalize artifacts to '{output_root}': {exc}"
            ) from exc
