from __future__ import annotations

import re
from pathlib import Path

from orchestrator.exceptions import ConfigurationError

_SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9_-])?$")
_WINDOWS_RESERVED_BASENAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    *{f"com{i}" for i in range(1, 10)},
    *{f"lpt{i}" for i in range(1, 10)},
}


def require_safe_path_component(value: str, *, owner_label: str, field_name: str) -> str:
    """Return a validated host-filesystem path component.

    The orchestrator uses job and service IDs as directory names on the host.
    We keep that contract intentionally narrow: a single path component made
    from simple identifier characters, with no separators, traversal, or
    Windows-reserved base names.
    """

    if not isinstance(value, str):
        raise ConfigurationError(f"{owner_label} {field_name} must be a string")
    if not value:
        raise ConfigurationError(f"{owner_label} {field_name} must not be empty")
    if not _SAFE_COMPONENT_RE.fullmatch(value):
        raise ConfigurationError(
            f"{owner_label} {field_name} must be a single path component using "
            f"letters, numbers, dots, underscores, or hyphens"
        )

    path = Path(value)
    if path.is_absolute() or path.anchor or len(path.parts) != 1:
        raise ConfigurationError(
            f"{owner_label} {field_name} must be a single path component using "
            f"letters, numbers, dots, underscores, or hyphens"
        )

    basename = value.split(".", 1)[0].casefold()
    if basename in _WINDOWS_RESERVED_BASENAMES:
        raise ConfigurationError(
            f"{owner_label} {field_name} must not use a Windows reserved name"
        )

    return value
