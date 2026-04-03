class OrchestratorError(Exception):
    """Base exception for all build orchestrator errors."""


class CyclicDependencyError(OrchestratorError):
    """Raised when the job dependency graph contains a cycle."""


class EnvironmentValidationError(OrchestratorError):
    """Raised when a builder's required toolchain is missing or misconfigured."""


class ConfigurationError(OrchestratorError):
    """Raised when the build configuration file is invalid or malformed."""


class WorkspaceError(OrchestratorError):
    """Raised when a job workspace cannot be created or cleaned up."""


class ArtifactError(OrchestratorError):
    """Raised when artifact collection or finalization fails."""
