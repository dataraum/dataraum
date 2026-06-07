"""Layered config overlays — per-type appliers (DAT-343).

The engine's config loaders (``load_yaml_config`` / ``load_phase_config``)
call :func:`apply_overlay` after reading the base YAML; this module
merges per-workspace teach edits stored in the
``ws_<id>.config_overlay`` table over the base dict.

Each teach **type** binds to one target file and one merge function — no
generic dotted-path DSL, no per-row routing decisions on the cockpit
side. The cockpit just inserts ``{type, payload}``; the engine knows
which file the type targets and how its payload merges.

How the loader gets rows
------------------------
``load_yaml_config`` doesn't depend on Postgres; the worker bootstrap
registers a *resolver* (a callable returning active overlay rows for the
current workspace) via :func:`set_overlay_resolver`. The resolver hits
Postgres; tests register fakes. With no resolver registered (CLI / tests
that never bootstrap a workspace), :func:`apply_overlay` short-circuits
and returns the base unchanged — so existing call sites are inert until
the worker boots.

Registered teach types
----------------------
* ``type_pattern`` — ``phases/typing.yaml`` ``overrides.patterns.<name>``
* ``null_value`` — ``null_values.yaml`` under its declared category list
* ``concept`` — ``verticals/<vertical>/ontology.yaml``, upsert-replace
  by ``name`` into ``concepts:``; routed via :func:`apply_overlay`'s
  vertical-path detection. Used both by user teach and by the engine's
  cold-start ``_adhoc`` induction (DAT-371) which writes one row per
  induced concept instead of a YAML file.
* ``concept_property`` — ``verticals/<vertical>/ontology.yaml``,
  patching a field on a named concept entry; routed via
  :func:`apply_overlay`'s vertical-path detection. Concept rows are
  applied first (define / replace), then concept_property rows patch on
  top.
* ``validation`` — the logical collection ``verticals/<vertical>/validations``
  (a *directory* of per-id YAML files, merged to a ``validations:`` list by
  ``load_all_validation_specs`` before the overlay applies); upsert-replace
  by ``validation_id``, routed via :func:`apply_overlay`'s vertical-path
  detection (DAT-438). This is the declared-validation teach surface
  frame-2 (DAT-441) writes into.

The 4 still-deferred types (``cycle``, ``metric``, ``relationship``,
``explanation``) have no applier — the cockpit may still write their
rows, but the layered read is a no-op until later slices wire their
consumers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

from dataraum.core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Resolver — module-level pointer set by the worker bootstrap (and by tests).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OverlayRow:
    """One un-superseded ``config_overlay`` row, as the loader sees it.

    Resolvers must return rows ordered by ``created_at ASC`` so that
    appliers' last-write-wins semantics apply naturally.
    """

    type: str
    payload: dict[str, Any]


_overlay_resolver: Callable[[], list[OverlayRow]] | None = None


def set_overlay_resolver(resolver: Callable[[], list[OverlayRow]] | None) -> None:
    """Register the callable the loaders use to fetch active overlay rows.

    Called once per process by the worker bootstrap with a Postgres-backed
    resolver; tests register fakes that return fixed lists. Pass ``None``
    to clear (also via :func:`reset_overlay_resolver_for_tests`).
    """
    global _overlay_resolver  # noqa: PLW0603
    _overlay_resolver = resolver


def reset_overlay_resolver_for_tests() -> None:
    """Drop the registered resolver. Tests only."""
    global _overlay_resolver  # noqa: PLW0603
    _overlay_resolver = None


# ---------------------------------------------------------------------------
# Per-type appliers.
# ---------------------------------------------------------------------------


def _apply_type_pattern(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Merge ``type_pattern`` rows into ``phases/typing.yaml``.

    Payload shape: ``{name, pattern, inferred_type?, semantic_type?,
    detected_unit?, case_sensitive?, standardization_expr?, ...}`` — same
    shape as the ``overrides.patterns.<name>`` dict in typing.yaml. Rows
    are keyed by ``name``; the last row for a given name wins (rows are
    pre-sorted ASC by ``created_at``).
    """
    overrides = dict(base.get("overrides") or {})
    patterns = dict(overrides.get("patterns") or {})
    for row in rows:
        payload = dict(row.payload)
        name = payload.pop("name", None)
        if not name:
            continue
        patterns[name] = payload
    overrides["patterns"] = patterns
    out = dict(base)
    out["overrides"] = overrides
    return out


def _apply_null_value(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Merge ``null_value`` rows into ``null_values.yaml`` lists.

    Payload shape: ``{category, value, description?}`` where ``category``
    is one of ``standard_nulls`` / ``spreadsheet_nulls`` /
    ``placeholder_nulls`` / ``missing_indicators``. Appends to the list,
    de-duplicating by ``value`` within the category (a duplicate is a
    no-op, not an error — teach idempotency).
    """
    out = dict(base)
    # Per-category seen-set keeps the dedup O(rows + base) rather than the
    # naive O(rows * base) inner scan — practical teach sizes are tiny but
    # the base lists already carry the engine's stock null tokens.
    seen_by_category: dict[str, set[Any]] = {}
    for row in rows:
        category = row.payload.get("category")
        value = row.payload.get("value")
        if not category or value is None:
            continue
        if category not in seen_by_category:
            existing = out.get(category) or []
            seen_by_category[category] = {e.get("value") for e in existing}
            # Copy the list once so the base dict isn't aliased to the same
            # list we then mutate (callers may pass shared base dicts).
            out[category] = list(existing)
        seen = seen_by_category[category]
        if value in seen:
            continue
        seen.add(value)
        item = {k: v for k, v in row.payload.items() if k != "category"}
        out[category].append(item)
    return out


def _apply_concept(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Upsert-replace concept rows into a vertical ontology's ``concepts:`` list.

    Payload shape mirrors :class:`OntologyConcept`:
    ``{vertical, name, description?, indicators?, exclude_patterns?,
    temporal_behavior?, typical_role?, typical_values?, unit_from_concept?,
    is_unit_dimension?}``. ``vertical`` is matched by the caller (this
    applier only sees rows already filtered to the loading vertical).

    Merge semantics: one row = one concept. Same ``name`` replaces — the
    last row for a given concept name wins (rows are pre-sorted ASC by
    ``created_at``). Used by user teach AND by ``_adhoc`` cold-start
    induction (DAT-371) — induction writes N concept rows instead of a
    YAML file, and the layered read materializes them as if they were in
    the base file.
    """
    out = dict(base)
    concepts = [dict(c) for c in (out.get("concepts") or [])]
    by_name = {c.get("name"): i for i, c in enumerate(concepts) if c.get("name")}
    for row in rows:
        payload = {k: v for k, v in row.payload.items() if k != "vertical"}
        name = payload.get("name")
        if not name:
            continue
        if name in by_name:
            concepts[by_name[name]] = payload
        else:
            by_name[name] = len(concepts)
            concepts.append(payload)
    out["concepts"] = concepts
    return out


def _apply_concept_property(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Patch a property on a named concept in a vertical ontology.

    Payload shape: ``{vertical, concept, property, value}``. The
    ``vertical`` field is matched by the caller (this applier only sees
    rows already filtered to the loading vertical). Missing concept = row
    ignored (defensive — a teach against a stale ontology shouldn't
    crash the loader).
    """
    out = dict(base)
    concepts = [dict(c) for c in (out.get("concepts") or [])]
    by_name = {c.get("name"): c for c in concepts if c.get("name")}
    for row in rows:
        concept_name = row.payload.get("concept")
        prop = row.payload.get("property")
        if not concept_name or not prop:
            continue
        target = by_name.get(concept_name)
        if target is None:
            continue
        target[prop] = row.payload.get("value")
    out["concepts"] = concepts
    return out


def _apply_validation(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Upsert-replace validation rows into a vertical's ``validations:`` list.

    Payload shape mirrors :class:`ValidationSpec`:
    ``{vertical, validation_id, name, description, category, severity,
    check_type, parameters?, sql_hints?, expected_outcome?, tags?,
    relevant_cycles?, version?}``. ``vertical`` is matched by the caller
    (this applier only sees rows already filtered to the loading vertical).

    Merge semantics mirror ``concept``: one row = one whole spec. Same
    ``validation_id`` replaces — the last row for a given id wins (rows are
    pre-sorted ASC by ``created_at``). A framed vertical resolves
    overlay-only: an empty base list plus rows IS the declared set.
    """
    out = dict(base)
    specs = [dict(s) for s in (out.get("validations") or [])]
    by_id = {s.get("validation_id"): i for i, s in enumerate(specs) if s.get("validation_id")}
    for row in rows:
        payload = {k: v for k, v in row.payload.items() if k != "vertical"}
        validation_id = payload.get("validation_id")
        if not validation_id:
            continue
        if validation_id in by_id:
            specs[by_id[validation_id]] = payload
        else:
            by_id[validation_id] = len(specs)
            specs.append(payload)
    out["validations"] = specs
    return out


# ---------------------------------------------------------------------------
# Registry + dispatcher.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ApplierSpec:
    target_path: str
    apply: Callable[[dict[str, Any], list[OverlayRow]], dict[str, Any]]


# Fixed-path appliers: teach type → (target file, merge fn). The vertical
# ontology applier is NOT here — its target path is parameterized by the
# vertical name, so :func:`apply_overlay` routes it separately.
_REGISTRY: Final[dict[str, _ApplierSpec]] = {
    "type_pattern": _ApplierSpec(
        target_path="phases/typing.yaml",
        apply=_apply_type_pattern,
    ),
    "null_value": _ApplierSpec(
        target_path="null_values.yaml",
        apply=_apply_null_value,
    ),
}


# Vertical ontology files live at ``verticals/<vertical>/ontology.yaml``;
# the validation collection is the logical path ``verticals/<vertical>/validations``
# (a directory merged to one dict by its loader, not a single file). These
# constants keep the path-parsing in one place.
_VERTICAL_ONTOLOGY_PREFIX = "verticals/"
_VERTICAL_ONTOLOGY_SUFFIX = "/ontology.yaml"
_VERTICAL_VALIDATIONS_SUFFIX = "/validations"


def apply_overlay(relative_path: str, base: dict[str, Any]) -> dict[str, Any]:
    """Layer active overlay rows over ``base`` for the file at ``relative_path``.

    Called by :func:`dataraum.core.config.load_yaml_config` after the
    file is parsed. Returns ``base`` unchanged when no resolver is
    registered or no row targets this path.

    Dispatch:
        * ``verticals/<v>/ontology.yaml`` — apply ``concept`` rows whose
          payload ``vertical`` matches ``<v>`` (upsert-replace the list),
          then ``concept_property`` rows for the same vertical patch on
          top. The order matters: concept defines / replaces a whole
          concept entry; concept_property patches one field on the
          (possibly just-replaced) concept.
        * ``verticals/<v>/validations`` — the logical validation collection
          (DAT-438): apply ``validation`` rows whose payload ``vertical``
          matches ``<v>`` (upsert-replace by ``validation_id``).
        * everything else — look up ``relative_path`` in the registry;
          apply each matching teach type's rows.
    """
    if _overlay_resolver is None:
        # Once-per-process DEBUG breadcrumb so an operator wiring the
        # resolver can confirm whether the layered read is live. Logged
        # only when there's nothing to merge AND no resolver — overlap-free
        # signal for "the overlay is inert in this process".
        logger.debug("overlay_resolver_inert", relative_path=relative_path)
        return base
    rows = _overlay_resolver()
    if not rows:
        return base

    if relative_path.startswith(_VERTICAL_ONTOLOGY_PREFIX) and relative_path.endswith(
        _VERTICAL_ONTOLOGY_SUFFIX
    ):
        vertical = relative_path[len(_VERTICAL_ONTOLOGY_PREFIX) : -len(_VERTICAL_ONTOLOGY_SUFFIX)]
        concept_rows = [
            r for r in rows if r.type == "concept" and r.payload.get("vertical") == vertical
        ]
        property_rows = [
            r
            for r in rows
            if r.type == "concept_property" and r.payload.get("vertical") == vertical
        ]
        merged = base
        if concept_rows:
            merged = _apply_concept(merged, concept_rows)
        if property_rows:
            merged = _apply_concept_property(merged, property_rows)
        return merged

    if relative_path.startswith(_VERTICAL_ONTOLOGY_PREFIX) and relative_path.endswith(
        _VERTICAL_VALIDATIONS_SUFFIX
    ):
        vertical = relative_path[
            len(_VERTICAL_ONTOLOGY_PREFIX) : -len(_VERTICAL_VALIDATIONS_SUFFIX)
        ]
        validation_rows = [
            r for r in rows if r.type == "validation" and r.payload.get("vertical") == vertical
        ]
        return _apply_validation(base, validation_rows) if validation_rows else base

    merged = base
    for teach_type, spec in _REGISTRY.items():
        if spec.target_path != relative_path:
            continue
        matching = [r for r in rows if r.type == teach_type]
        if matching:
            merged = spec.apply(merged, matching)
    return merged
