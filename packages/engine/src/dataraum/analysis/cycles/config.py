"""Configuration loader for business cycle detection.

Loads the cycle vocabulary from ``config/verticals/<vertical>/cycles.yaml``,
layered with workspace ``cycle`` overlay teach rows (DAT-455) — the same
OntologyLoader dual-path pattern validation uses: the production path is
overlay-aware (teach rows upsert over the shipped vertical's ``cycle_types``;
a *framed* vertical with no on-disk file resolves overlay-only), while an
explicit ``verticals_dir`` (tests / fixtures) reads raw YAML and bypasses the
overlay.

The vocabulary IS the declared set for the cycle lifecycle family: each
``cycle_types`` key (canonical name + stages/aliases/completion indicators) is
declared as one ``cycle`` lifecycle artifact, then bound + executed against the
workspace. The engine induces nothing — declares come from the vertical now;
user declares arrive via frame-2 teach rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dataraum.core.logging import get_logger
from dataraum.core.vertical_loader import Family, VerticalLoader

logger = get_logger(__name__)

# The honest state a family cycle carries when the served evidence does not decide
# its direction — NEVER coerced to a declared direction (DAT-856). A statically-known
# sentinel (unlike the per-vertical direction labels), so it is the one direction
# value the DB and a reader can name without loading the declaration.
UNDETERMINED_DIRECTION = "undetermined"


@dataclass(frozen=True)
class CycleIdentity:
    """The persisted identity axes resolved from the judge's cycle output (DAT-856).

    ``canonical_type`` is the artifact identity (the ``(canonical_type, run_id)``
    key). ``family`` / ``direction`` are the direction axis: both ``None`` for a
    non-family cycle, both set for a family cycle (a decided label, or
    ``UNDETERMINED_DIRECTION``).
    """

    canonical_type: str | None
    is_known_type: bool
    family: str | None
    direction: str | None


def get_cycles_config(vertical: str, verticals_dir: Path | None = None) -> dict[str, Any]:
    """Load a vertical's cycles config, layered with ``cycle`` overlay rows.

    Thin wrapper over :class:`~dataraum.core.vertical_loader.VerticalLoader`
    (DAT-481): the shipped ``cycles.yaml`` (empty base when the vertical is
    framed — declared via the cockpit, no on-disk file) ⊕ active ``cycle``
    overlay rows (upsert by cycle name into ``cycle_types``). An unknown vertical
    resolves to an EMPTY dict, never raises — "no declared cycles" is a loud,
    explicit outcome at the phase tier. An explicit ``verticals_dir`` reads raw
    YAML and bypasses the overlay (tests).

    Returns:
        The cycles config dict (``{"cycle_types": {...}, "analysis_hints": ...}``),
        or an empty dict when neither file nor overlay declares anything.
    """
    return VerticalLoader(vertical, verticals_dir).collection(Family.CYCLES)


def get_cycle_types(vertical: str, verticals_dir: Path | None = None) -> dict[str, Any]:
    """Get cycle type definitions (overlay-aware).

    Args:
        vertical: Vertical name (e.g. 'finance').
        verticals_dir: Root verticals directory override (tests only).

    Returns:
        Dictionary of cycle_type_name -> cycle definition.
    """
    config = get_cycles_config(vertical, verticals_dir)
    result: dict[str, Any] = config.get("cycle_types") or {}
    return result


def map_to_canonical_type(cycle_type: str, vertical: str) -> tuple[str | None, bool]:
    """Map an LLM-returned cycle_type to a canonical vocabulary type.

    Handles aliases (e.g., "ar_cycle" -> "accounts_receivable") and
    case-insensitive matching. For unknown types, preserves the LLM's
    type as the canonical type so it can still participate in health
    scoring with universal validations.

    Args:
        cycle_type: The cycle type string from LLM output
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Tuple of (canonical_type, is_known_type):
        - canonical_type: The vocabulary key if matched, or the normalized
          LLM type if not. None only if cycle_type is empty.
        - is_known_type: True if the type matches vocabulary
    """
    if not cycle_type:
        return None, False

    cycle_types = get_cycle_types(vertical)
    cycle_type_lower = cycle_type.lower().strip()

    # Direct match (case-insensitive)
    for canonical in cycle_types:
        if cycle_type_lower == canonical.lower():
            return canonical, True

    # Check aliases
    for canonical, config in cycle_types.items():
        aliases = config.get("aliases", [])
        for alias in aliases:
            if cycle_type_lower == alias.lower():
                return canonical, True

    # No vocabulary match — preserve the LLM's type as canonical so the cycle
    # can still be health-scored using universal validations.
    return cycle_type_lower, False


def resolve_cycle_identity(
    *,
    cycle_type: str,
    family: str,
    direction: str,
    cycle_families: dict[str, dict[str, str]],
    vertical: str,
) -> CycleIdentity:
    """Resolve the judge's ``(cycle_type, family, direction)`` to the persisted identity.

    The two-layer resolution for the direction axis (DAT-856). For a cycle the judge
    places in a DECLARED family (``cycle_families`` = ``{family: {label: member}}``, the
    loaded declaration), the family is the identity axis and ``direction`` the honest
    sub-axis:

    * a decided direction (a label the family declares) resolves to its member cycle
      type — ``canonical_type`` = that member (mapped through
      :func:`map_to_canonical_type`, so a member keeps its vocabulary ``is_known_type``
      and the validation↔cycle health linkage that keys on it);
    * ``undetermined`` keeps the FAMILY as ``canonical_type`` — the detected-but-
      undirected state, distinguishable from a missed cycle and from a directed one,
      NEVER coerced to a direction.

    Off-vocabulary references degrade LOUDLY rather than silently: a direction label the
    family does NOT declare degrades to ``undetermined`` (the family detection is kept —
    recall preserved — but the axis is left honestly open, never guessed); a family the
    vertical does NOT declare falls to the non-family path (the judge named a family that
    is not real). A non-family cycle (empty ``family``) keeps today's behavior:
    ``cycle_type`` mapped via :func:`map_to_canonical_type`, ``family`` / ``direction``
    ``None``.

    This is the resolution counterpart of the DB co-occurrence CHECK — it is the ONLY
    producer of the ``(family, direction)`` pair, so both are always set together or
    both ``None``.
    """
    fam = family.strip()
    # Case-insensitive family match, mirroring map_to_canonical_type's cycle_type
    # matching AND the direction lower-casing below — an LLM that varies the family's
    # casing ("Settlement") must NOT lose the whole axis (a strictly worse silent
    # recall loss than an off-vocab direction). The DECLARED name is the persisted
    # identity, so casing never fragments a family's rows across runs.
    families_ci = {name.lower(): (name, dirs) for name, dirs in cycle_families.items()}
    if fam and fam.lower() in families_ci:
        declared_family, directions = families_ci[fam.lower()]
        dir_norm = direction.strip().lower()
        if dir_norm == UNDETERMINED_DIRECTION:
            return CycleIdentity(
                canonical_type=declared_family,
                is_known_type=True,
                family=declared_family,
                direction=UNDETERMINED_DIRECTION,
            )
        if dir_norm in directions:
            canonical, is_known = map_to_canonical_type(directions[dir_norm], vertical)
            return CycleIdentity(
                canonical_type=canonical,
                is_known_type=is_known,
                family=declared_family,
                direction=dir_norm,
            )
        # A direction the family does not declare: keep the family detection but leave
        # the axis honestly undetermined rather than guess a member (DAT-856 — recall
        # over a coerced label). Loud so the prompt-contract miss is visible.
        logger.warning(
            "cycle_direction_off_vocab",
            family=declared_family,
            direction=direction,
            declared=sorted(directions),
        )
        return CycleIdentity(
            canonical_type=declared_family,
            is_known_type=True,
            family=declared_family,
            direction=UNDETERMINED_DIRECTION,
        )
    if fam:
        # The judge named a family the vertical does not declare — fall to the
        # non-family path (resolve by cycle_type), loudly.
        logger.warning("cycle_family_not_declared", family=fam)
    canonical, is_known = map_to_canonical_type(cycle_type, vertical)
    return CycleIdentity(
        canonical_type=canonical, is_known_type=is_known, family=None, direction=None
    )


def format_cycle_vocabulary_for_context(*, vertical: str) -> str:
    """Format cycle vocabulary as readable context for the LLM.

    Args:
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Formatted string suitable for LLM context
    """
    lines = []
    config = get_cycles_config(vertical)

    if not config:
        return ""

    # Cycle types
    cycle_types = config.get("cycle_types", {})
    if cycle_types:
        lines.append("## KNOWN BUSINESS CYCLE TYPES")
        lines.append("")

        for cycle_name, cycle_def in cycle_types.items():
            business_value = cycle_def.get("business_value", "medium")
            description = cycle_def.get("description", "")
            aliases = cycle_def.get("aliases", [])

            lines.append(f"### {cycle_name} (value: {business_value})")
            lines.append(f"Description: {description}")
            if aliases:
                lines.append(f"Also known as: {', '.join(aliases)}")

            # Stages
            stages = cycle_def.get("typical_stages", [])
            if stages:
                lines.append("Typical stages:")
                for stage in stages:
                    stage_name = stage.get("name", "")
                    indicators = stage.get("indicators", [])
                    lines.append(
                        f"  {stage['order']}. {stage_name} - indicators: {', '.join(indicators)}"
                    )

            # Completion indicators
            completion = cycle_def.get("completion_indicators", [])
            if completion:
                lines.append(f"Completion indicators: {', '.join(completion)}")

            # Downstream cycles
            feeds_into = cycle_def.get("feeds_into", [])
            if feeds_into:
                lines.append(f"Feeds into: {', '.join(feeds_into)}")

            lines.append("")

    # Analysis hints
    hints = config.get("analysis_hints", {})
    if hints:
        lines.append("## ANALYSIS GUIDANCE")

        strong = hints.get("strong_indicators", [])
        if strong:
            lines.append("Strong indicators of cycles:")
            for hint in strong:
                lines.append(f"  - {hint}")

        health = hints.get("health_factors", [])
        if health:
            lines.append("Healthy cycle indicators:")
            for hint in health:
                lines.append(f"  - {hint}")

        warnings = hints.get("warning_signs", [])
        if warnings:
            lines.append("Warning signs:")
            for hint in warnings:
                lines.append(f"  - {hint}")

    return "\n".join(lines)
