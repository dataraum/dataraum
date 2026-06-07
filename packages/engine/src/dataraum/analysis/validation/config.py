"""Configuration loader for validation specs.

Loads validation specifications from YAML files in
config/verticals/<vertical>/validations/, layered with workspace
``validation`` overlay rows (DAT-438) — the OntologyLoader dual-path
pattern: the production path is overlay-aware (teach rows merge over the
shipped vertical; a *framed* vertical with no on-disk directory resolves
overlay-only), while an explicit ``verticals_dir`` (tests / fixtures)
reads raw YAML and bypasses the overlay.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dataraum.analysis.validation.models import (
    ValidationSpec,
)
from dataraum.core.logging import get_logger

logger = get_logger(__name__)


def _read_spec_dir(validations_dir: Path) -> list[dict[str, Any]]:
    """Parse every per-id YAML file in a validations directory.

    Per-file reads are raw (``yaml.safe_load``) — the overlay applies at the
    *collection* level (``verticals/<v>/validations``), not per file.
    """
    import yaml

    entries: list[dict[str, Any]] = []
    for yaml_file in sorted(validations_dir.rglob("*.yaml")):
        with open(yaml_file) as f:
            data = yaml.safe_load(f)
        if not data or not isinstance(data, dict):
            logger.warning("validation_spec_empty", file=str(yaml_file))
            continue
        entries.append(data)
    return entries


def load_all_validation_specs(
    vertical: str, verticals_dir: Path | None = None
) -> dict[str, ValidationSpec]:
    """Load a vertical's validation specs, layered with overlay teach rows.

    Production path (``verticals_dir`` is ``None``): read the shipped
    vertical's ``validations/`` directory (empty base when the vertical is
    framed — declared via the cockpit, no on-disk directory), then merge
    active ``validation`` overlay rows via
    :func:`dataraum.core.overlay.apply_overlay` (upsert by
    ``validation_id``). An unknown vertical resolves to an EMPTY dict, never
    raises — "no declared validations" is a loud, explicit outcome at the
    phase tier, not a loader crash.

    Test path (explicit ``verticals_dir``): read
    ``<verticals_dir>/<vertical>/validations`` raw, bypassing the overlay —
    deterministic for unit tests (mirrors ``OntologyLoader``).

    Args:
        vertical: Vertical name (e.g. ``'finance'``).
        verticals_dir: Root verticals directory override (tests only).

    Returns:
        Dict mapping validation_id to ValidationSpec.
    """
    if verticals_dir is not None:
        spec_dir = verticals_dir / vertical / "validations"
        entries = _read_spec_dir(spec_dir) if spec_dir.is_dir() else []
    else:
        from dataraum.core.config import get_config_dir
        from dataraum.core.overlay import apply_overlay

        try:
            spec_dir = get_config_dir(f"verticals/{vertical}/validations")
        except FileNotFoundError:
            # Framed vertical (no on-disk directory) or a vertical without
            # shipped validations — the overlay rows ARE the declared set.
            spec_dir = None
        base_entries = _read_spec_dir(spec_dir) if spec_dir is not None else []
        merged = apply_overlay(f"verticals/{vertical}/validations", {"validations": base_entries})
        entries = merged.get("validations") or []

    specs: dict[str, ValidationSpec] = {}
    for data in entries:
        spec = ValidationSpec.model_validate(data)
        specs[spec.validation_id] = spec
        logger.debug("validation_spec_loaded", validation_id=spec.validation_id)

    logger.debug("validation_specs_loaded", vertical=vertical, count=len(specs))
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


def get_validation_specs_for_cycles(cycle_types: list[str], vertical: str) -> list[ValidationSpec]:
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
