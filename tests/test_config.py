"""Tests for the YAML configuration loader."""
from __future__ import annotations

import pytest
from pathlib import Path
from textwrap import dedent

from orchestrator.config import YamlConfigLoader
from orchestrator.exceptions import ConfigurationError
from orchestrator.models import FailurePolicy


@pytest.fixture
def loader() -> YamlConfigLoader:
    return YamlConfigLoader()


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "build.yaml"
    p.write_text(dedent(content), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestValidConfig:
    def test_minimal_single_job(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: compile
                image: registry/builder:latest
        """)
        plan = loader.load(path)

        assert len(plan.jobs) == 1
        assert plan.jobs[0].id == "compile"
        assert plan.jobs[0].image == "registry/builder:latest"

    def test_defaults_applied(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
        """)
        plan = loader.load(path)

        assert plan.failure_policy == FailurePolicy.FAIL_FAST
        assert plan.max_parallel == 4
        assert plan.total_cpu_slots == 8
        assert plan.total_memory_slots == 8
        job = plan.jobs[0]
        assert job.resource_weight.cpu_slots == 1
        assert job.resource_weight.memory_slots == 1
        assert job.depends_on == frozenset()
        assert job.command is None
        assert job.artifacts == []
        assert job.env_vars == {}

    def test_full_config(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            failure_policy: continue
            max_parallel: 2
            total_cpu_slots: 4
            total_memory_slots: 16
            jobs:
              - id: build
                image: registry/builder:latest
                command: ["--release"]
                cpu_slots: 2
                memory_slots: 4
                artifacts:
                  - source_glob: "**/*.dll"
                    destination_subdir: libs
                  - source_glob: "*.pdb"
                    destination_subdir: symbols
                env_vars:
                  CONFIG: Release
                  VERBOSE: "true"
              - id: test
                image: registry/tester:latest
                depends_on: [build]
                cpu_slots: 1
                memory_slots: 2
                artifacts:
                  - source_glob: "*.xml"
                    destination_subdir: test-results
        """)
        plan = loader.load(path)

        assert plan.failure_policy == FailurePolicy.CONTINUE
        assert plan.max_parallel == 2
        assert plan.total_cpu_slots == 4
        assert plan.total_memory_slots == 16

        build = plan.jobs[0]
        assert build.id == "build"
        assert build.command == ["--release"]
        assert build.resource_weight.cpu_slots == 2
        assert build.resource_weight.memory_slots == 4
        assert len(build.artifacts) == 2
        assert build.artifacts[0].source_glob == "**/*.dll"
        assert build.artifacts[0].destination_subdir == "libs"
        assert build.env_vars == {"CONFIG": "Release", "VERBOSE": "true"}

        test = plan.jobs[1]
        assert test.depends_on == frozenset({"build"})
        assert test.resource_weight.cpu_slots == 1
        assert test.resource_weight.memory_slots == 2

    def test_empty_jobs_list(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs: []
        """)
        plan = loader.load(path)
        assert plan.jobs == []

    def test_artifact_destination_subdir_defaults_to_empty(
        self, loader: YamlConfigLoader, tmp_path: Path
    ) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
                artifacts:
                  - source_glob: "*.bin"
        """)
        plan = loader.load(path)
        assert plan.jobs[0].artifacts[0].destination_subdir == ""


# ---------------------------------------------------------------------------
# File errors
# ---------------------------------------------------------------------------


class TestFileErrors:
    def test_missing_file(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError, match="Cannot read"):
            loader.load(tmp_path / "nonexistent.yaml")

    def test_invalid_yaml_syntax(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
              bad indent
        """)
        with pytest.raises(ConfigurationError, match="Invalid YAML"):
            loader.load(path)

    def test_yaml_is_not_a_mapping(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, "- item1\n- item2\n")
        with pytest.raises(ConfigurationError, match="must be a YAML mapping"):
            loader.load(path)


# ---------------------------------------------------------------------------
# Schema violations
# ---------------------------------------------------------------------------


class TestSchemaViolations:
    def test_missing_jobs_key(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, "max_parallel: 4\n")
        with pytest.raises(ConfigurationError, match="missing required key 'jobs'"):
            loader.load(path)

    def test_jobs_not_a_list(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, "jobs: not_a_list\n")
        with pytest.raises(ConfigurationError, match="'jobs' must be a list"):
            loader.load(path)

    def test_job_missing_id(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - image: img:1
        """)
        with pytest.raises(ConfigurationError, match="missing required key 'id'"):
            loader.load(path)

    def test_job_missing_image(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
        """)
        with pytest.raises(ConfigurationError, match="missing required key 'image'"):
            loader.load(path)

    def test_job_entry_not_a_mapping(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - "just a string"
        """)
        with pytest.raises(ConfigurationError, match="expected a mapping"):
            loader.load(path)

    def test_depends_on_not_a_list(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
                depends_on: "not_a_list"
        """)
        with pytest.raises(ConfigurationError, match="depends_on.*list of strings"):
            loader.load(path)

    def test_command_not_a_list(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
                command: "single_string"
        """)
        with pytest.raises(ConfigurationError, match="command.*list of strings"):
            loader.load(path)

    def test_max_parallel_not_int(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            max_parallel: "four"
            jobs:
              - id: a
                image: img:1
        """)
        with pytest.raises(ConfigurationError, match="max_parallel.*integer"):
            loader.load(path)

    def test_artifact_missing_source_glob(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
                artifacts:
                  - destination_subdir: out
        """)
        with pytest.raises(ConfigurationError, match="missing 'source_glob'"):
            loader.load(path)


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


class TestValidationErrors:
    def test_invalid_failure_policy(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            failure_policy: explode
            jobs:
              - id: a
                image: img:1
        """)
        with pytest.raises(ConfigurationError, match="Invalid failure_policy"):
            loader.load(path)

    def test_duplicate_job_id(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
              - id: a
                image: img:2
        """)
        with pytest.raises(ConfigurationError, match="Duplicate job id 'a'"):
            loader.load(path)

    def test_unknown_dependency(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
                depends_on: [nonexistent]
        """)
        with pytest.raises(ConfigurationError, match="unknown job"):
            loader.load(path)

    def test_max_parallel_zero(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            max_parallel: 0
            jobs:
              - id: a
                image: img:1
        """)
        with pytest.raises(ConfigurationError, match="max_parallel must be >= 1"):
            loader.load(path)

    def test_cpu_slots_zero(self, loader: YamlConfigLoader, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, """\
            jobs:
              - id: a
                image: img:1
                cpu_slots: 0
        """)
        with pytest.raises(ConfigurationError, match="cpu_slots must be >= 1"):
            loader.load(path)
