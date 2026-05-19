"""Config patch utilities for the teach/fix infrastructure.

Provides ``apply_config_yaml`` for writing structured changes to YAML
config files on disk.  Used by the teach tool and ConfigInterpreter.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


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
        # Empty key_path: replace entire file content (only "set" supported)
        if operation != "set":
            raise ValueError("key_path must not be empty (except for operation='set')")
        if not isinstance(value, dict):
            raise ValueError("Root-level set requires a dict value")
        data = value
    else:
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
                f"Cannot merge non-dict value at '{'.'.join(key_path)}': got {type(value).__name__}"
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
