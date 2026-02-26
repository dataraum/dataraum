"""Configuration loader for validation specs.

Loads validation specifications from YAML files in config/verticals/<vertical>/validations/.
Uses core config loader for path resolution and YAML parsing.
"""

from __future__ import annotations

from dataraum.analysis.validation.models import (
    ValidationSpec,
)
from dataraum.core.logging import get_logger

logger = get_logger(__name__)


def load_all_validation_specs(vertical: str) -> dict[str, ValidationSpec]:
    """Load all validation specs from config directory.

    Args:
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Dict mapping validation_id to ValidationSpec

    Raises:
        FileNotFoundError: If the vertical does not exist.
    """
    import yaml

    from dataraum.core.vertical import VerticalConfig

    specs: dict[str, ValidationSpec] = {}
    config_path = VerticalConfig(vertical).validations_dir

    if not config_path.is_dir():
        logger.warning("validations_dir_missing", vertical=vertical, path=str(config_path))
        return specs

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


def get_validation_specs_by_category(category: str, vertical: str) -> list[ValidationSpec]:
    """Get all validation specs for a specific category.

    Args:
        category: Category name (e.g., 'financial', 'data_quality')
        vertical: Vertical name (e.g. 'finance')

    Returns:
        List of ValidationSpecs matching the category
    """
    all_specs = load_all_validation_specs(vertical)
    return [spec for spec in all_specs.values() if spec.category == category]


def get_validation_specs_by_tags(tags: list[str], vertical: str) -> list[ValidationSpec]:
    """Get all validation specs that have any of the specified tags.

    Args:
        tags: List of tags to filter by
        vertical: Vertical name (e.g. 'finance')

    Returns:
        List of ValidationSpecs that have at least one matching tag
    """
    all_specs = load_all_validation_specs(vertical)
    tag_set = set(tags)
    return [spec for spec in all_specs.values() if set(spec.tags) & tag_set]


def get_validation_specs_for_cycles(
    cycle_types: list[str], vertical: str
) -> list[ValidationSpec]:
    """Get validation specs relevant to detected cycle types.

    Returns specs that either:
    - Have relevant_cycles overlapping with cycle_types, or
    - Have empty relevant_cycles (universal applicability)

    Args:
        cycle_types: Detected cycle canonical types (e.g. ['journal_entry_cycle'])
        vertical: Vertical name (e.g. 'finance')

    Returns:
        List of matching ValidationSpecs
    """
    all_specs = load_all_validation_specs(vertical)
    cycle_set = set(cycle_types)
    return [
        spec
        for spec in all_specs.values()
        if not spec.relevant_cycles or set(spec.relevant_cycles) & cycle_set
    ]


def get_validation_spec(validation_id: str, vertical: str) -> ValidationSpec | None:
    """Get a specific validation spec by ID.

    Args:
        validation_id: ID of the validation spec
        vertical: Vertical name (e.g. 'finance')

    Returns:
        ValidationSpec or None if not found
    """
    all_specs = load_all_validation_specs(vertical)
    return all_specs.get(validation_id)


__all__ = [
    "load_all_validation_specs",
    "get_validation_specs_by_category",
    "get_validation_specs_by_tags",
    "get_validation_specs_for_cycles",
    "get_validation_spec",
]
