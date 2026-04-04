from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from pathlib import Path

from orchestrator.artifact_store import ArtifactStore
from orchestrator.config import YamlConfigLoader
from orchestrator.engine import Engine
from orchestrator.executor import DockerExecutor
from orchestrator.logger import FileJobLogger
from orchestrator.pipeline import AzureCliArgs
from orchestrator.scheduler import ResourceScheduler
from orchestrator.volume_prep import prepare_volumes

logger = logging.getLogger(__name__)


def _parse_args() -> AzureCliArgs:
    parser = argparse.ArgumentParser(
        description="Build orchestrator for Azure DevOps pipelines."
    )
    parser.add_argument("config", type=Path, help="Path to the build configuration file.")
    parser.add_argument("--source-dir", type=Path, required=True, help="Path to the checked-out source tree.")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"), help="Artifact output directory.")
    parser.add_argument("--dry-run", action="store_true", help="Validate config without executing builds.")
    args = parser.parse_args()
    return AzureCliArgs(
        config_path=args.config,
        output_dir=args.output_dir,
        source_dir=args.source_dir,
        dry_run=args.dry_run,
    )


class _StdoutReporter:
    """Minimal reporter that prints lifecycle events to stdout."""

    def report_job_started(self, job_id: str) -> None:
        logger.info("##vso[task.logdetail] Job started: %s", job_id)

    def report_job_completed(self, job_id: str, success: bool) -> None:
        status = "succeeded" if success else "FAILED"
        logger.info("##vso[task.logdetail] Job %s: %s", status, job_id)

    def report_result(self, result: object) -> None:
        pass


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args()

    loader = YamlConfigLoader()
    plan = loader.load(args.config_path)

    if args.dry_run:
        logger.info("Dry run — config is valid (%d jobs)", len(plan.jobs))
        return

    with tempfile.TemporaryDirectory(prefix="orch_") as tmpdir:
        tmp = Path(tmpdir)
        log_dir = tmp / "logs"
        container_output_root = tmp / "outputs"
        staging_dir = tmp / "staging"

        prepare_volumes(plan, args.source_dir, container_output_root)

        job_logger = FileJobLogger(log_dir)
        artifact_store = ArtifactStore(staging_dir, container_output_root)
        scheduler = ResourceScheduler(plan)
        executor = DockerExecutor(logger=job_logger, max_workers=plan.max_parallel)
        reporter = _StdoutReporter()

        engine = Engine(
            scheduler=scheduler,
            executor=executor,
            artifact_store=artifact_store,
            job_logger=job_logger,
            reporter=reporter,
            output_root=args.output_dir,
        )

        try:
            result = engine.run(plan)
        finally:
            executor.shutdown()

    if not result.success:
        failed = [r.job_id for r in result.job_results if not r.success]
        logger.error("Build failed. Failed jobs: %s", failed)
        sys.exit(1)

    logger.info("Build succeeded (%d jobs)", len(result.job_results))


if __name__ == "__main__":
    main()
