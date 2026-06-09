"""Vertical resolution — classify a vertical name (DAT-480).

A vertical name resolves to one of four kinds:

* ``shipped`` — a curated builtin with an on-disk
  ``config/verticals/<name>/ontology.yaml`` (e.g. ``finance``).
* ``framed`` — declared at runtime through the cockpit ``frame`` stage: no
  on-disk directory (the config tree is read-only), but the workspace's
  overlay carries vertical-scoped rows (``concept`` / ``validation`` /
  ``cycle`` / ``metric``) for the name.
* ``placeholder`` — the no-vertical default ``_adhoc`` (and any leading-
  underscore name): always valid, never an error, the fallback when the user
  declared no domain. Mirrors the cockpit ``DEFAULT_VERTICAL``. ``_adhoc``
  ships an on-disk dir but is classified by the underscore convention, never
  as a domain vertical.
* ``unknown`` — a name matching none of the above: a typo (``finanace``) or a
  vertical that was never framed. Both previously resolved to ``{}`` / ``None``
  at every executable-knowledge loader and every phase turned that into a
  benign ``no_declared_*`` "success". The engine now fails LOUD on it at run
  entry instead (see :func:`require_known_vertical`).

This module replaces the dead ``VerticalConfig`` path-resolver: the engine
reads vertical config through ``load_yaml_config`` (overlay-aware), never by
constructing per-file paths, so a path-resolver had no remaining caller.
"""

from __future__ import annotations

from enum import StrEnum

from dataraum.core.config import get_config_dir
from dataraum.core.overlay import get_overlay_rows

# Overlay row types that DECLARE a *vertical's* model (concepts + the
# validation/cycle/metric families, DAT-371/438/455/456) — the presence of any
# one for a name is what makes it a framed vertical. ``concept_property`` is
# deliberately excluded: it patches a field on an existing concept, so it can
# never establish a vertical on its own (it always rides alongside a ``concept``
# row for the same vertical).
_VERTICAL_SCOPED_TYPES: frozenset[str] = frozenset({"concept", "validation", "cycle", "metric"})


class VerticalKind(StrEnum):
    """Classification of a vertical name (DAT-480)."""

    SHIPPED = "shipped"
    FRAMED = "framed"
    PLACEHOLDER = "placeholder"
    UNKNOWN = "unknown"


def _is_placeholder(name: str | None) -> bool:
    """``_adhoc`` and any leading-underscore name are the no-vertical default.

    A missing name (``None`` / empty) is the same "no domain declared" state —
    phases coalesce it to ``_adhoc`` — so it classifies as a placeholder too.
    """
    return not name or name.startswith("_")


def _shipped_verticals() -> set[str]:
    """On-disk builtin verticals (a ``<name>/ontology.yaml`` exists).

    Excludes leading-underscore placeholders (``_adhoc`` ships a dir but is the
    no-vertical default, not a domain vertical). Returns ``set()`` if the
    verticals root is absent.
    """
    try:
        root = get_config_dir("verticals")
    except FileNotFoundError:
        return set()
    return {
        p.parent.name for p in root.glob("*/ontology.yaml") if not p.parent.name.startswith("_")
    }


def _framed_verticals() -> set[str]:
    """Vertical names that exist purely as overlay rows (declared via frame).

    Distinct ``payload.vertical`` over the workspace's active vertical-scoped
    overlay rows. Workspace scope is implicit in the resolver (its rows already
    come from the ``ws_<id>`` schema); multi-workspace defers to DAT-357.
    """
    names: set[str] = set()
    for row in get_overlay_rows():
        if row.type in _VERTICAL_SCOPED_TYPES:
            vertical = row.payload.get("vertical")
            if vertical:
                names.add(vertical)
    return names


def resolve_vertical(name: str | None) -> VerticalKind:
    """Classify a vertical name into shipped / framed / placeholder / unknown.

    Existence is INFERRED, no registry: placeholder by the underscore
    convention, shipped from the on-disk verticals tree, framed from the
    workspace's overlay rows. Placeholder is checked FIRST — ``_adhoc`` ships an
    on-disk dir but must never read as a domain (``shipped``) vertical.
    """
    if _is_placeholder(name):
        return VerticalKind.PLACEHOLDER
    if name in _shipped_verticals():
        return VerticalKind.SHIPPED
    if name in _framed_verticals():
        return VerticalKind.FRAMED
    return VerticalKind.UNKNOWN


def available_verticals() -> list[str]:
    """Sorted names a user can legitimately select — shipped ∪ framed.

    Never includes the ``_adhoc`` placeholder; for the "available verticals"
    half of a born-loud unknown-vertical error.
    """
    return sorted(_shipped_verticals() | _framed_verticals())


def require_known_vertical(name: str | None) -> VerticalKind:
    """Resolve ``name``, raising a born-loud error if it is ``unknown``.

    Run-entry guard (DAT-480): the engine grounds, validates, and computes
    against a vertical's declared model. An unknown name would silently resolve
    to zero concepts/specs and every phase would emit a benign ``no_declared_*``
    — the failure mode this closes. Raise here, naming the verticals that DO
    exist (shipped + framed; never the ``_adhoc`` placeholder).

    Returns the resolved :class:`VerticalKind` for the known cases (shipped /
    framed / placeholder), so a caller can branch without re-resolving.

    Raises ``RuntimeError`` (not ``ValueError``) to match the sibling fail-loud
    raises at the operating_model resolve seam — and because the failure is
    permanent (a typo doesn't fix itself on retry), keeping it the same
    deterministic-failure shape Temporal's retry policy already handles there.
    """
    kind = resolve_vertical(name)
    if kind is VerticalKind.UNKNOWN:
        available = available_verticals()
        raise RuntimeError(
            f"Unknown vertical {name!r}. Available verticals: "
            f"{', '.join(available) if available else '(none — frame one first)'}."
        )
    return kind


__all__ = [
    "VerticalKind",
    "available_verticals",
    "require_known_vertical",
    "resolve_vertical",
]
