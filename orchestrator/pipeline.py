from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from orchestrator.models import OrchestratorResult


@dataclass
class AzureCliArgs:
    """Parsed command-line arguments supplied by the Azure DevOps pipeline step."""

    config_path: Path
    output_dir: Path
    source_dir: Path
    dry_run: bool = False
    port: int | None = None


class IPipelineReporter(ABC):
    """Reports orchestration lifecycle events back to the Azure DevOps pipeline.

    Implementations emit ADO logging commands (##vso[...]) so the pipeline
    UI reflects per-job and overall status without parsing log files.
    """

    @abstractmethod
    def report_job_started(self, job_id: str) -> None:
        """Signal that the given job has started execution."""
        ...

    @abstractmethod
    def report_job_completed(self, job_id: str, success: bool) -> None:
        """Signal that the given job has finished, indicating pass or fail."""
        ...

    @abstractmethod
    def report_result(self, result: OrchestratorResult) -> None:
        """Emit the final orchestration outcome and set the pipeline task status."""
        ...
