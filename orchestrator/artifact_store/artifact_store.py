from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from orchestrator.exceptions import ArtifactError
from orchestrator.models import ArtifactSpec, JobResult, JobSpec, ResourceSpec
from orchestrator.path_safety import require_safe_path_component


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
    def collect_resource(self, resource: ResourceSpec) -> None:
        """Stage artifacts from a managed resource output directory.

        Resources may write optional artifacts into their managed output
        directory on the host. Implementations resolve the declared globs
        and copy matched files into the staging area.

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
    copied into ``staging_dir/<destination_subdir>/<relative path within the
    container output root>`` during collect(), then the entire staging tree is
    copied to the pipeline's output directory during finalize().

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
        job_id = require_safe_path_component(job.id, owner_label="Job", field_name="id")
        self._collect_specs(
            owner_kind="job",
            owner_id=job_id,
            output_dir=self._container_output_root / job_id,
            specs=job.artifacts,
        )

    def collect_resource(self, resource: ResourceSpec) -> None:
        resource_id = require_safe_path_component(
            resource.id, owner_label="Resource", field_name="id"
        )
        self._collect_specs(
            owner_kind="resource",
            owner_id=resource_id,
            output_dir=self._container_output_root / "resources" / resource_id,
            specs=resource.artifacts,
        )

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

    def _collect_specs(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        output_dir: Path,
        specs: list[ArtifactSpec],
    ) -> None:
        if not output_dir.is_dir():
            raise ArtifactError(
                f"Container output directory does not exist for {owner_kind} '{owner_id}': "
                f"{output_dir}"
            )

        planned_copies: list[tuple[Path, Path]] = []
        planned_targets: set[Path] = set()

        for spec in specs:
            dest = self._staging_dir / spec.destination_subdir
            matches = sorted(p for p in output_dir.glob(spec.source_glob) if p.is_file())

            if not matches:
                raise ArtifactError(
                    f"Artifact glob '{spec.source_glob}' matched no files "
                    f"for {owner_kind} '{owner_id}' under {output_dir}"
                )

            for match in matches:
                relative_match = match.relative_to(output_dir)
                target = dest / relative_match
                if target in planned_targets:
                    raise ArtifactError(
                        "Artifact filename collision while staging "
                        f"{owner_kind} '{owner_id}': '{match}' would overwrite '{target}'"
                    )
                if target.exists():
                    raise ArtifactError(
                        "Artifact filename collision while staging "
                        f"{owner_kind} '{owner_id}': '{match}' would overwrite existing '{target}'"
                    )
                planned_targets.add(target)
                planned_copies.append((match, target))

        for match, target in planned_copies:
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(match, target)
            except OSError as exc:
                raise ArtifactError(
                    f"Failed to stage artifact '{match}' for {owner_kind} '{owner_id}': {exc}"
                ) from exc
