from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import threading
from pathlib import Path

from orchestrator.artifact_store import ArtifactStore
from orchestrator.config import YamlConfigLoader
from orchestrator.engine import Engine
from orchestrator.executor import DockerExecutor
from orchestrator.logger import FileJobLogger
from orchestrator.models import BuildPlan, OrchestratorResult
from orchestrator.pipeline import AzureCliArgs
from orchestrator.scheduler import ResourceScheduler

logger = logging.getLogger(__name__)


def _parse_args() -> AzureCliArgs:
    parser = argparse.ArgumentParser(
        description="Build orchestrator for Azure DevOps pipelines."
    )
    parser.add_argument("config", type=Path, help="Path to the build configuration file.")
    parser.add_argument("--source-dir", type=Path, required=True, help="Path to the checked-out source tree.")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"), help="Artifact output directory.")
    parser.add_argument("--dry-run", action="store_true", help="Validate config without executing builds.")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Start HTTP SSE server on this local port (ADO async mode).",
    )
    args = parser.parse_args()
    return AzureCliArgs(
        config_path=args.config,
        output_dir=args.output_dir,
        source_dir=args.source_dir,
        dry_run=args.dry_run,
        port=args.port,
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

    def report_resource_status(self, resources: object) -> None:
        pass


def _print_strategy_summary(plan: BuildPlan, port: int | None) -> None:
    """Print a human-readable build plan summary to stdout.

    This is always flushed immediately so Azure DevOps captures it in the
    step log even when the pipeline moves on to other tasks.
    """
    sep = "=" * 44
    lines = [
        sep,
        "  Build Orchestrator: Strategy Summary",
        sep,
        f"  Failure policy : {plan.failure_policy.value}",
        f"  Max parallel   : {plan.max_parallel}",
        f"  CPU slots      : {plan.total_cpu_slots} / Memory slots: {plan.total_memory_slots}",
        f"  Jobs ({len(plan.jobs)}):",
    ]
    for i, job in enumerate(plan.jobs, start=1):
        if job.depends_on:
            dep_str = "depends_on=[" + ", ".join(sorted(job.depends_on)) + "]"
        else:
            dep_str = "(no deps)"
        lines.append(f"    [{i}] {job.id:<22} {dep_str}")
    if plan.resources:
        resource_ids = ", ".join(r.id for r in plan.resources)
        lines.append(f"  Resources: {resource_ids}")
    if port is not None:
        lines.append(f"  Server listening on http://localhost:{port}/stream")
    lines.append(sep)
    print("\n".join(lines), flush=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args()

    loader = YamlConfigLoader()
    plan = loader.load(args.config_path)

    if args.dry_run:
        logger.info("Dry run — config is valid (%d jobs)", len(plan.jobs))
        return

    _print_strategy_summary(plan, args.port)

    with tempfile.TemporaryDirectory(prefix="orch_") as tmpdir:
        tmp = Path(tmpdir)
        log_dir = tmp / "logs"
        container_output_root = tmp / "outputs"
        staging_dir = tmp / "staging"

        artifact_store = ArtifactStore(staging_dir, container_output_root)
        scheduler = ResourceScheduler(plan)

        if args.port is not None:
            from orchestrator.server.event_bus import EventBus
            from orchestrator.server.http_server import OrchestratorHTTPServer
            from orchestrator.server.metrics import HostMetricsSampler
            from orchestrator.server.reporter import CompositeReporter, EventBusReporter
            from orchestrator.server.tee_logger import EventBusJobLogger

            bus = EventBus()
            sampler = HostMetricsSampler(bus)
            job_logger = EventBusJobLogger(log_dir, bus)
            executor = DockerExecutor(
                logger=job_logger,
                source_dir=args.source_dir,
                container_output_root=container_output_root,
                max_workers=plan.max_parallel,
            )
            reporter = CompositeReporter(
                _StdoutReporter(),
                EventBusReporter(bus, plan=plan, sampler=sampler),
            )

            engine = Engine(
                scheduler=scheduler,
                executor=executor,
                artifact_store=artifact_store,
                job_logger=job_logger,
                reporter=reporter,
                output_root=args.output_dir,
            )

            engine_result: list[OrchestratorResult] = []
            engine_exc: list[BaseException] = []

            def _run_engine() -> None:
                try:
                    sampler.start()
                    engine_result.append(engine.run(plan))
                except BaseException as exc:  # noqa: BLE001
                    engine_exc.append(exc)
                    bus.close()
                finally:
                    sampler.stop()
                    executor.shutdown()

            engine_thread = threading.Thread(target=_run_engine, daemon=False, name="engine")
            engine_thread.start()
            OrchestratorHTTPServer(args.port, bus).serve_until_done()
            engine_thread.join()

            if engine_exc:
                raise engine_exc[0]
            result = engine_result[0]

        else:
            job_logger = FileJobLogger(log_dir)
            executor = DockerExecutor(
                logger=job_logger,
                source_dir=args.source_dir,
                container_output_root=container_output_root,
                max_workers=plan.max_parallel,
            )
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
