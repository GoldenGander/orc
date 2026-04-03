from __future__ import annotations

import argparse
import sys
from pathlib import Path

from orchestrator.pipeline import AzureCliArgs


def _parse_args() -> AzureCliArgs:
    parser = argparse.ArgumentParser(
        description="Build orchestrator for Azure DevOps pipelines."
    )
    parser.add_argument("config", type=Path, help="Path to the build configuration file.")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"), help="Artifact output directory.")
    parser.add_argument("--dry-run", action="store_true", help="Validate config without executing builds.")
    args = parser.parse_args()
    return AzureCliArgs(config_path=args.config, output_dir=args.output_dir, dry_run=args.dry_run)


def main() -> None:
    ...


if __name__ == "__main__":
    main()
