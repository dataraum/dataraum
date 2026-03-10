"""Inline fix data models and config patch utilities.

Core types for the inline fix system:
- FixInput: Structured user decision after agent interpretation

And the utility to apply config changes to YAML files on disk.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class FixInput:
    """Structured user decision after agent interpretation.

    Produced by the DocumentAgent in config mode, consumed by the bridge
    function to build FixDocuments.

    Args:
        action_name: The entropy action being addressed, e.g. "accept_finding".
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


def apply_config_yaml(
    config_root: Path,
    config_path: str,
    operation: str,
    key_path: list[str],
    value: Any = None,
) -> None:
    """Apply a config change to a YAML file on disk.

    Reads the YAML file, applies the operation at the specified key path,
    and writes it back. Creates the file with an empty dict if it doesn't exist.

    Args:
        config_root: Absolute path to the config root directory.
        config_path: Relative path within config root, e.g. "phases/typing.yaml".
        operation: One of "set", "append", "remove", "merge".
        key_path: Nested key path, e.g. ["date_patterns"] or ["overrides", "amount"].
        value: The value to set/append/merge. Ignored for "remove".

    Raises:
        ValueError: If the operation is unknown or the key path is invalid
            for the operation.
        FileNotFoundError: If config_root does not exist.
    """
    if not config_root.is_dir():
        raise FileNotFoundError(f"Config root not found: {config_root}")

    file_path = config_root / config_path
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        with open(file_path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    if not key_path:
        raise ValueError("key_path must not be empty")

    _apply_operation(data, key_path, operation, value)

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
