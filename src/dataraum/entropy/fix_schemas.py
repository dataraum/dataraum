"""YAML-driven fix schema loader.

Loads fix schemas from config/entropy/fixes.yaml and returns FixSchema
instances. This is the single source of truth for fix schemas — detectors
no longer define them in Python.

Usage:
    from dataraum.entropy.fix_schemas import get_fix_schema, get_schemas_for_detector

    schema = get_fix_schema("document_accepted_null_ratio", dimension_path="value.nulls.null_ratio")
    schemas = get_schemas_for_detector("type_fidelity")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField

logger = get_logger(__name__)

FIXES_CONFIG = "entropy/fixes.yaml"

# Module-level cache
_schemas_cache: dict[str, list[FixSchema]] | None = None
_triage_cache: dict[str, str] | None = None
_dimension_map_cache: dict[str, str] | None = None  # detector_id -> dimension_path


def _load_raw(config_path: Path | None = None) -> dict[str, Any]:
    """Load raw YAML from fixes config file."""
    if config_path is None:
        config_path = get_config_file(FIXES_CONFIG)
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def _parse_field(raw: dict[str, Any]) -> FixSchemaField:
    """Parse a single field definition from YAML."""
    return FixSchemaField(
        type=raw.get("type", "string"),
        required=raw.get("required", True),
        description=raw.get("description", ""),
        default=raw.get("default"),
        examples=raw.get("examples"),
        enum_values=raw.get("enum_values"),
    )


def _parse_schema(
    action_name: str,
    raw: dict[str, Any],
    detector_id: str,
    dimension_path: str,
) -> FixSchema:
    """Parse a single action definition into a FixSchema."""
    fields: dict[str, FixSchemaField] = {}
    for field_name, field_raw in (raw.get("fields") or {}).items():
        fields[field_name] = _parse_field(field_raw)

    return FixSchema(
        action=action_name,
        target=raw.get("target", "config"),
        description=raw.get("description", ""),
        fields=fields,
        config_path=raw.get("config_path"),
        key_path=raw.get("key_path"),
        operation=raw.get("operation"),
        model=raw.get("model"),
        requires_rerun=raw.get("requires_rerun"),
        guidance=raw.get("guidance", "").strip(),
        key_template=raw.get("key_template"),
        routing=raw.get("routing"),
        gate=raw.get("gate"),
        dimension_path=dimension_path,
    )


def _ensure_loaded(config_path: Path | None = None) -> None:
    """Load and cache schemas if not already loaded."""
    global _schemas_cache, _triage_cache, _dimension_map_cache
    if _schemas_cache is not None:
        return

    raw = _load_raw(config_path)
    schemas_raw = raw.get("schemas", {})

    _schemas_cache = {}
    _triage_cache = {}
    _dimension_map_cache = {}

    for detector_id, detector_raw in schemas_raw.items():
        dimension_path = detector_raw.get("dimension_path", "")
        _dimension_map_cache[detector_id] = dimension_path
        _triage_cache[detector_id] = (detector_raw.get("triage_guidance") or "").strip()

        detector_schemas: list[FixSchema] = []
        for action_name, action_raw in (detector_raw.get("actions") or {}).items():
            schema = _parse_schema(action_name, action_raw, detector_id, dimension_path)
            detector_schemas.append(schema)

        _schemas_cache[detector_id] = detector_schemas


def clear_fix_schema_cache() -> None:
    """Invalidate the cached fix schemas so the next call reloads from disk."""
    global _schemas_cache, _triage_cache, _dimension_map_cache
    _schemas_cache = None
    _triage_cache = None
    _dimension_map_cache = None


def get_fix_schema(
    action_name: str,
    dimension_path: str | None = None,
    config_path: Path | None = None,
) -> FixSchema | None:
    """Find a FixSchema by action name, optionally scoped by dimension.

    Each action name is now unique (e.g. document_accepted_null_ratio).
    dimension_path can still be used to scope the search to a specific detector.

    Args:
        action_name: The action to look up.
        dimension_path: If provided, only consider detectors whose
            dimension_path matches.
        config_path: Optional override for testing.

    Returns:
        The matching FixSchema, or None if not found.
    """
    _ensure_loaded(config_path)
    assert _schemas_cache is not None
    assert _dimension_map_cache is not None

    for detector_id, schemas in _schemas_cache.items():
        if dimension_path and _dimension_map_cache.get(detector_id) != dimension_path:
            continue
        for schema in schemas:
            if schema.action == action_name:
                return schema
    return None


def get_schemas_for_detector(
    detector_id: str,
    config_path: Path | None = None,
) -> list[FixSchema]:
    """Get all fix schemas for a given detector.

    Args:
        detector_id: Detector identifier (e.g. "type_fidelity").
        config_path: Optional override for testing.

    Returns:
        List of FixSchema instances (empty if detector has none).
    """
    _ensure_loaded(config_path)
    assert _schemas_cache is not None
    return _schemas_cache.get(detector_id, [])


def get_triage_guidance(
    detector_id: str,
    config_path: Path | None = None,
) -> str:
    """Get triage guidance for a detector.

    Args:
        detector_id: Detector identifier.
        config_path: Optional override for testing.

    Returns:
        Triage guidance string (empty if not defined).
    """
    _ensure_loaded(config_path)
    assert _triage_cache is not None
    return _triage_cache.get(detector_id, "")


def get_detector_id_for_dimension(
    dimension_path: str,
    config_path: Path | None = None,
) -> str | None:
    """Resolve a dimension_path to its detector_id.

    Args:
        dimension_path: Full dimension path (e.g. "structural.relations.relationship_quality").
        config_path: Optional override for testing.

    Returns:
        Detector ID (e.g. "relationship_entropy"), or None if not found.
    """
    _ensure_loaded(config_path)
    assert _dimension_map_cache is not None
    for detector_id, dim_path in _dimension_map_cache.items():
        if dim_path == dimension_path:
            return detector_id
    return None


def get_all_schemas(
    config_path: Path | None = None,
) -> dict[str, list[FixSchema]]:
    """Get all fix schemas grouped by detector_id.

    Args:
        config_path: Optional override for testing.

    Returns:
        Dict mapping detector_id to list of FixSchema instances.
    """
    _ensure_loaded(config_path)
    assert _schemas_cache is not None
    return dict(_schemas_cache)
