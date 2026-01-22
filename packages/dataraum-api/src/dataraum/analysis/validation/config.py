"""Configuration loader for validation specs.

Loads validation specifications from YAML files in config/validations/.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from dataraum.analysis.validation.models import (
    ValidationSeverity,
    ValidationSpec,
)
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

# Default path to validation configs (config/ is in packages/api/)
# Path: src/dataraum/analysis/validation/config.py -> 5 parents -> packages/api/config/validations
CONFIG_DIR = Path(__file__).parent.parent.parent.parent.parent / "config" / "validations"


def get_validations_config_path() -> Path:
    """Get the path to validations config directory.

    Returns:
        Path to config/validations/
    """
    return CONFIG_DIR


@lru_cache(maxsize=1)
def load_all_validation_specs() -> dict[str, ValidationSpec]:
    """Load all validation specs from config directory.

    Returns:
        Dict mapping validation_id to ValidationSpec
    """
    specs: dict[str, ValidationSpec] = {}
    config_path = get_validations_config_path()

    if not config_path.exists():
        logger.warning(f"Validations config directory not found: {config_path}")
        return specs

    # Load all YAML files recursively
    for yaml_file in config_path.rglob("*.yaml"):
        try:
            spec = load_validation_spec(yaml_file)
            if spec:
                specs[spec.validation_id] = spec
                logger.debug(f"Loaded validation spec: {spec.validation_id}")
        except Exception as e:
            logger.error(f"Failed to load validation spec {yaml_file}: {e}")

    logger.info(f"Loaded {len(specs)} validation specs")
    return specs


def load_validation_spec(file_path: Path) -> ValidationSpec | None:
    """Load a single validation spec from a YAML file.

    Args:
        file_path: Path to YAML file

    Returns:
        ValidationSpec or None if invalid
    """
    try:
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data or not isinstance(data, dict):
            return None

        # Parse severity
        severity_str = data.get("severity", "error").lower()
        try:
            severity = ValidationSeverity(severity_str)
        except ValueError:
            severity = ValidationSeverity.ERROR

        spec = ValidationSpec(
            validation_id=data.get("validation_id", file_path.stem),
            name=data.get("name", file_path.stem),
            description=data.get("description", ""),
            category=data.get("category", "general"),
            severity=severity,
            check_type=data.get("check_type", "custom"),
            parameters=data.get("parameters", {}),
            sql_hints=data.get("sql_hints"),
            expected_outcome=data.get("expected_outcome"),
            tags=data.get("tags", []),
            version=data.get("version", "1.0"),
            source="config",
        )

        return spec

    except Exception as e:
        logger.error(f"Error parsing validation spec {file_path}: {e}")
        return None


def get_validation_specs_by_category(category: str) -> list[ValidationSpec]:
    """Get all validation specs for a specific category.

    Args:
        category: Category name (e.g., 'financial', 'data_quality')

    Returns:
        List of ValidationSpecs matching the category
    """
    all_specs = load_all_validation_specs()
    return [spec for spec in all_specs.values() if spec.category == category]


def get_validation_specs_by_tags(tags: list[str]) -> list[ValidationSpec]:
    """Get all validation specs that have any of the specified tags.

    Args:
        tags: List of tags to filter by

    Returns:
        List of ValidationSpecs that have at least one matching tag
    """
    all_specs = load_all_validation_specs()
    tag_set = set(tags)
    return [spec for spec in all_specs.values() if set(spec.tags) & tag_set]


def get_validation_spec(validation_id: str) -> ValidationSpec | None:
    """Get a specific validation spec by ID.

    Args:
        validation_id: ID of the validation spec

    Returns:
        ValidationSpec or None if not found
    """
    all_specs = load_all_validation_specs()
    return all_specs.get(validation_id)


def format_validation_specs_for_context(category: str | None = None) -> str:
    """Format validation specs as context for LLM prompts.

    Args:
        category: Optional category filter

    Returns:
        Formatted string describing available validations
    """
    if category:
        specs = get_validation_specs_by_category(category)
    else:
        specs = list(load_all_validation_specs().values())

    if not specs:
        return "No validation specs available."

    lines = ["## Available Validation Checks\n"]

    for spec in sorted(specs, key=lambda s: s.validation_id):
        lines.append(f"### {spec.name} ({spec.validation_id})")
        lines.append(f"Category: {spec.category}")
        lines.append(f"Severity: {spec.severity.value}")
        lines.append(f"Description: {spec.description}")

        if spec.sql_hints:
            lines.append(f"SQL hints: {spec.sql_hints}")

        if spec.expected_outcome:
            lines.append(f"Expected outcome: {spec.expected_outcome}")

        lines.append("")

    return "\n".join(lines)


def clear_cache() -> None:
    """Clear the cached validation specs."""
    load_all_validation_specs.cache_clear()


__all__ = [
    "get_validations_config_path",
    "load_all_validation_specs",
    "load_validation_spec",
    "get_validation_specs_by_category",
    "get_validation_specs_by_tags",
    "get_validation_spec",
    "format_validation_specs_for_context",
    "clear_cache",
]
