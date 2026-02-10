"""Configuration loader for validation specs.

Loads validation specifications from YAML files in config/verticals/finance/validations/.
Uses core config loader for path resolution and YAML parsing.
"""

from __future__ import annotations

from dataraum.analysis.validation.models import (
    ValidationSpec,
)
from dataraum.core.config import get_config_dir
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

# Relative path from config root to validation specs directory
VALIDATIONS_DIR = "verticals/finance/validations"


def load_all_validation_specs() -> dict[str, ValidationSpec]:
    """Load all validation specs from config directory.

    Returns:
        Dict mapping validation_id to ValidationSpec

    Raises:
        FileNotFoundError: If the validations config directory does not exist.
    """
    import yaml

    specs: dict[str, ValidationSpec] = {}
    config_path = get_config_dir(VALIDATIONS_DIR)

    for yaml_file in config_path.rglob("*.yaml"):
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if not data or not isinstance(data, dict):
            logger.warning("validation_spec_empty", file=str(yaml_file))
            continue

        spec = ValidationSpec.model_validate(data)
        specs[spec.validation_id] = spec
        logger.debug("validation_spec_loaded", validation_id=spec.validation_id)

    logger.info("validation_specs_loaded", count=len(specs))
    return specs


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


__all__ = [
    "load_all_validation_specs",
    "get_validation_specs_by_category",
    "get_validation_specs_by_tags",
    "get_validation_spec",
    "format_validation_specs_for_context",
]
