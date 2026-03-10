"""Inline fix data models and config patch utilities.

Core types for the inline fix system:
- ConfigPatch: A concrete change to a YAML config file
- FixInput: Structured user decision after agent interpretation
- FixResult: What a fix handler produced

And the utility to apply patches to config files on disk.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ConfigPatch:
    """A concrete change to a config YAML file.

    Args:
        config_path: Relative path within config root, e.g. "phases/typing.yaml".
        operation: One of "set", "append", "remove", "merge".
        key_path: Nested key path, e.g. ["date_patterns"] or ["overrides", "amount"].
        value: The value to set/append/merge. Ignored for "remove".
        reason: Human-readable explanation of why this change is applied.
    """

    config_path: str
    operation: str  # "set", "append", "remove", "merge"
    key_path: list[str]
    value: Any = None
    reason: str = ""


@dataclass
class FixInput:
    """Structured user decision after agent interpretation.

    Produced by the DocumentAgent in config mode, consumed by phase fix handlers.

    Args:
        action_name: The entropy action being addressed, e.g. "transform_exclude_outliers".
        parameters: Structured parameters from user interaction.
        interpretation: Agent's interpretation of user answers.
        affected_columns: Which columns this fix applies to, e.g. ["orders.amount"].
        entropy_evidence: Evidence from the detector that triggered this fix.
    """

    action_name: str
    parameters: dict[str, Any] = field(default_factory=dict)
    interpretation: str = ""
    affected_columns: list[str] = field(default_factory=list)
    entropy_evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class FixResult:
    """Result of applying a fix handler.

    Returned by phase fix handlers after writing config changes.

    Args:
        config_patches: The YAML changes that were applied.
        requires_rerun: Name of the phase that needs to be re-run.
        summary: Human-readable summary of what was done.
    """

    config_patches: list[ConfigPatch] = field(default_factory=list)
    requires_rerun: str = ""
    summary: str = ""


def apply_config_patch(config_root: Path, patch: ConfigPatch) -> None:
    """Apply a ConfigPatch to a YAML file on disk.

    Reads the YAML file, applies the operation at the specified key path,
    and writes it back. Creates the file with an empty dict if it doesn't exist.

    Args:
        config_root: Absolute path to the config root directory.
        patch: The patch to apply.

    Raises:
        ValueError: If the operation is unknown or the key path is invalid
            for the operation.
        FileNotFoundError: If config_root does not exist.
    """
    if not config_root.is_dir():
        raise FileNotFoundError(f"Config root not found: {config_root}")

    file_path = config_root / patch.config_path
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        with open(file_path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    if not patch.key_path:
        raise ValueError("key_path must not be empty")

    _apply_operation(data, patch.key_path, patch.operation, patch.value)

    with open(file_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def _apply_operation(
    data: dict[str, Any],
    key_path: list[str],
    operation: str,
    value: Any,
) -> None:
    """Apply an operation at a nested key path within a dict.

    Navigates to the parent of the target key, creating intermediate dicts
    as needed, then applies the operation.

    Args:
        data: The mutable dict to modify in place.
        key_path: Non-empty list of keys to navigate.
        operation: One of "set", "append", "remove", "merge".
        value: The value for the operation.

    Raises:
        ValueError: If the operation is unknown or invalid for the target.
    """
    # Navigate to parent, creating intermediate dicts as needed
    current = data
    for key in key_path[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]

    target_key = key_path[-1]

    if operation == "set":
        current[target_key] = value

    elif operation == "append":
        existing = current.get(target_key)
        if existing is None:
            current[target_key] = [value]
        elif isinstance(existing, list):
            current[target_key].append(value)
        else:
            raise ValueError(
                f"Cannot append to non-list at '{'.'.join(key_path)}': "
                f"got {type(existing).__name__}"
            )

    elif operation == "remove":
        if target_key in current:
            del current[target_key]
        # Silently no-op if key doesn't exist — idempotent

    elif operation == "merge":
        if not isinstance(value, dict):
            raise ValueError(
                f"Cannot merge non-dict value at '{'.'.join(key_path)}': "
                f"got {type(value).__name__}"
            )
        existing = current.get(target_key)
        if existing is None:
            current[target_key] = value
        elif isinstance(existing, dict):
            existing.update(value)
        else:
            raise ValueError(
                f"Cannot merge into non-dict at '{'.'.join(key_path)}': "
                f"got {type(existing).__name__}"
            )

    else:
        raise ValueError(f"Unknown operation: {operation!r} (expected set/append/remove/merge)")
