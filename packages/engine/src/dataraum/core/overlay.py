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
* ``unit`` — ``phases/typing.yaml`` ``overrides.units."<table>.<column>"`` (DAT-428):
  the column-scoped unit teach. Shares the file with ``type_pattern`` but a disjoint
  key, so both compose. The reader (``typing_phase._apply_unit_overrides``) has always
  existed; this applier is the write half that was missing.
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
* ``rebind`` — ``verticals/<vertical>/ontology.yaml``, the COLUMN-grain
  re-grounding teach (the ``temporal_behavior`` measurement's ignorance-branch
  suggestion): appends the taught column name to the named concept's
  ``indicators`` so the next run's grounding LLM (which reads indicators via
  ``OntologyLoader.format_concepts_for_prompt``) pulls the column to that
  concept. Teach-as-witness, never an override: it steers the grounding
  input; ``SemanticAnnotation.business_concept`` stays an LLM judgment.
  Applied after ``concept`` / ``concept_property`` so it patches the final
  concept state; last rebind per column wins (a later row MOVES the column).
* ``validation`` — the logical collection ``verticals/<vertical>/validations``
  (a *directory* of per-id YAML files, merged to a ``validations:`` list by
  ``load_all_validation_specs`` before the overlay applies); upsert-replace
  by ``validation_id``, routed via :func:`apply_overlay`'s vertical-path
  detection (DAT-438). This is the declared-validation teach surface
  frame-2 (DAT-441) writes into.
* ``cycle`` — ``verticals/<vertical>/cycles.yaml``, upsert-replace by cycle
  name into the ``cycle_types`` MAPPING (not a list — cycles.yaml keys cycles
  by name); routed via :func:`apply_overlay`'s vertical-path detection
  (DAT-455). This is the declared-cycle teach surface frame-2 writes into.
* ``metric`` — the logical collection ``verticals/<vertical>/metrics`` (a
  *directory* of per-``graph_id`` transformation-graph YAML files, merged to a
  ``metrics:`` list by ``graphs.config.get_metrics_config`` before the overlay
  applies); upsert-replace by ``graph_id``, routed via :func:`apply_overlay`'s
  vertical-path detection (DAT-456). This is the declared-metric teach surface
  frame-2 writes into. Mirrors ``validation`` (a list keyed by an id), not
  ``cycle`` (a mapping keyed by name).

Two further types have NO layered-read applier here but ARE consumed by direct
``config_overlay`` table reads: ``relationship`` (DAT-409 —
``analysis.relationships.utils.load_confirmed_relationship_pairs``, read by the
``join_path_determinism`` detector) and ``expected_dependency``
(``entropy.detectors.loaders.load_documented_dependencies``, read by
``dimensional_entropy``). ``explanation`` was removed (DAT-343 registry cut:
no typed tool, no applier — the cockpit's validateTeach rejects it).
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


def get_overlay_rows() -> list[OverlayRow]:
    """Return the workspace's active overlay rows, or ``[]`` if no resolver.

    The single read path for code that needs to *enumerate* overlay rows
    (vertical resolution, DAT-480) rather than merge them onto a base via
    :func:`apply_overlay`. Inert (``[]``) when no resolver is registered —
    CLI / tests that never bootstrap a workspace — mirroring
    :func:`apply_overlay`'s short-circuit.
    """
    if _overlay_resolver is None:
        return []
    return _overlay_resolver()


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


def _apply_unit(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Merge ``unit`` rows into ``phases/typing.yaml`` ``overrides.units``.

    Payload shape: ``{table, column, unit}``. Keyed by ``"{table}.{column}"`` —
    the ``col_ref`` ``typing_phase._apply_unit_overrides`` reads to patch the best
    ``TypeCandidate``'s ``detected_unit`` (and force ``unit_confidence`` → 1.0). The
    last row for a given ``table.column`` wins (rows are pre-sorted ASC by
    ``created_at``). This is the column-scoped unit teach (DAT-428): the WRITE half
    the reader was always missing, so a taught unit lands on an already-typed numeric
    column without having to win a type pattern. Shares ``phases/typing.yaml`` with
    ``type_pattern`` but touches a disjoint key (``overrides.units`` vs
    ``overrides.patterns``), so the two compose under the dispatcher.
    """
    overrides = dict(base.get("overrides") or {})
    units = dict(overrides.get("units") or {})
    for row in rows:
        table = row.payload.get("table")
        column = row.payload.get("column")
        unit = row.payload.get("unit")
        if not table or not column or not unit:
            continue
        units[f"{table}.{column}"] = {"unit": unit}
    overrides["units"] = units
    out = dict(base)
    out["overrides"] = overrides
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


def _apply_rebind(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Re-ground a column: append its name to the target concept's ``indicators``.

    Payload shape: ``{vertical, concept, column, table?}`` — ``vertical`` is
    matched by the caller (this applier only sees rows already filtered to the
    loading vertical); ``concept`` names the rebind target; ``column`` is the
    column name to pull. ``table`` is advisory context only — indicators are a
    vertical-wide name vocabulary, so the merge key is the column name.

    This is the column-grain teach closing the ``temporal_behavior``
    measurement's ignorance branch (ADR-0009 pack piece 6): the appended
    indicator reaches the next run's grounding prompt
    (``OntologyLoader.format_concepts_for_prompt``), so the LLM re-grounds the
    column and the concept's declared behaviour re-enters the pool as the
    ontology-prior witness. The teach steers a witness INPUT — it never writes
    ``business_concept`` directly (the no-override firewall).

    Merge semantics: last rebind per column wins — a later row for the same
    column MOVES it to the new concept (the column is appended only to its
    final target among rebind rows; rows are pre-sorted ASC by ``created_at``).
    Base indicators are never removed — an indicator may be a pattern serving
    other columns, so un-pulling from the old concept is not the teach's job.
    Unknown concept = row ignored (defensive — a teach against a stale ontology
    must not crash the loader); duplicate rebinds are idempotent (dedup).
    """
    out = dict(base)
    concepts = [dict(c) for c in (out.get("concepts") or [])]
    by_name = {c.get("name"): c for c in concepts if c.get("name")}
    # Last-write-wins per column: resolve the final binding first so a
    # re-taught column lands only on its newest target.
    binding: dict[str, str] = {}
    for row in rows:
        column = row.payload.get("column")
        concept_name = row.payload.get("concept")
        if not column or not concept_name:
            continue
        binding[column] = concept_name
    for column, concept_name in binding.items():
        target = by_name.get(concept_name)
        if target is None:
            continue
        # Fresh list — the shallow concept copies still alias the base lists.
        indicators = list(target.get("indicators") or [])
        if column not in indicators:
            indicators.append(column)
        target["indicators"] = indicators
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


def _apply_cycle(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Upsert-replace cycle rows into a vertical's ``cycle_types`` mapping.

    Payload shape mirrors one ``cycles.yaml`` ``cycle_types`` entry plus its
    key: ``{vertical, name, description?, business_value?, aliases?,
    typical_stages?, participating_entities?, completion_indicators?,
    feeds_into?}``. ``vertical`` is matched by the caller (this applier only
    sees rows already filtered to the loading vertical); ``name`` is the
    ``cycle_types`` key — unlike validations (a list keyed by ``validation_id``)
    the cycle vocabulary is a MAPPING.

    Merge semantics mirror ``validation``: one row = one whole cycle entry.
    Same ``name`` replaces — the last row for a given name wins (rows are
    pre-sorted ASC by ``created_at``). A framed vertical resolves overlay-only:
    an empty base mapping plus rows IS the declared set.
    """
    out = dict(base)
    cycle_types = {k: dict(v) for k, v in (out.get("cycle_types") or {}).items()}
    for row in rows:
        payload = {k: v for k, v in row.payload.items() if k != "vertical"}
        name = payload.pop("name", None)
        if not name:
            continue
        cycle_types[name] = payload
    out["cycle_types"] = cycle_types
    return out


def _apply_metric(base: dict[str, Any], rows: list[OverlayRow]) -> dict[str, Any]:
    """Upsert-replace metric rows into a vertical's ``metrics:`` list.

    Payload shape mirrors a transformation-graph definition plus its key:
    ``{vertical, graph_id, version?, metadata, output, parameters?,
    dependencies, interpretation?}`` — the same shape
    ``graphs.loader.GraphLoader`` parses from an on-disk metric YAML.
    ``vertical`` is matched by the caller (this applier only sees rows already
    filtered to the loading vertical); ``graph_id`` is the identity.

    Merge semantics mirror ``validation``: one row = one whole graph definition.
    Same ``graph_id`` replaces — the last row for a given id wins (rows are
    pre-sorted ASC by ``created_at``). A framed vertical resolves overlay-only:
    an empty base list plus rows IS the declared set.
    """
    out = dict(base)
    metrics = [dict(m) for m in (out.get("metrics") or [])]
    by_id = {m.get("graph_id"): i for i, m in enumerate(metrics) if m.get("graph_id")}
    for row in rows:
        payload = {k: v for k, v in row.payload.items() if k != "vertical"}
        graph_id = payload.get("graph_id")
        if not graph_id:
            continue
        if graph_id in by_id:
            metrics[by_id[graph_id]] = payload
        else:
            by_id[graph_id] = len(metrics)
            metrics.append(payload)
    out["metrics"] = metrics
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
    "unit": _ApplierSpec(
        target_path="phases/typing.yaml",
        apply=_apply_unit,
    ),
}


# Vertical-scoped overlay families. A teach row's target file lives under
# ``verticals/<vertical>/<suffix>`` and is filtered by ``payload.vertical`` (the
# vertical isn't in the row type, it's in the payload). Adding a vertical family
# is ONE row here — not a new branch in :func:`apply_overlay` (DAT-481).
_VERTICAL_PREFIX = "verticals/"


@dataclass(frozen=True)
class _VerticalFamily:
    """A vertical overlay family: path suffix + ordered (teach_type, applier) pairs.

    Rows are filtered to a pair's ``teach_type`` AND the path's vertical, then
    merged in order. ``ontology.yaml`` has three pairs — ``concept`` defines /
    replaces a whole entry, then ``concept_property`` patches one field on the
    (possibly just-replaced) concept, then ``rebind`` appends column-name
    indicators onto the final concept state — so order matters; the others
    have one.
    """

    suffix: str
    appliers: tuple[tuple[str, Callable[[dict[str, Any], list[OverlayRow]], dict[str, Any]]], ...]


_VERTICAL_REGISTRY: Final[tuple[_VerticalFamily, ...]] = (
    _VerticalFamily(
        "/ontology.yaml",
        (
            ("concept", _apply_concept),
            ("concept_property", _apply_concept_property),
            ("rebind", _apply_rebind),
        ),
    ),
    _VerticalFamily("/validations", (("validation", _apply_validation),)),
    _VerticalFamily("/cycles.yaml", (("cycle", _apply_cycle),)),
    _VerticalFamily("/metrics", (("metric", _apply_metric),)),
)


def appliable_teach_types() -> frozenset[str]:
    """The teach types with a registered overlay applier — the executable vocabulary.

    Derived from the live registries so it can never drift from the appliers.
    A teach suggestion emitted by an entropy measurement must name a type in
    this set (or one of the documented direct ``config_overlay`` reads) or the
    product surface receives a suggestion it cannot execute — pinned by the
    vocabulary guard test (``tests/unit/entropy/test_teach_suggestion_vocabulary``).
    """
    vertical_types = {
        teach_type for family in _VERTICAL_REGISTRY for teach_type, _ in family.appliers
    }
    return frozenset(_REGISTRY) | vertical_types


def apply_overlay(relative_path: str, base: dict[str, Any]) -> dict[str, Any]:
    """Layer active overlay rows over ``base`` for the file at ``relative_path``.

    Called by :func:`dataraum.core.config.load_yaml_config` after the
    file is parsed. Returns ``base`` unchanged when no resolver is
    registered or no row targets this path.

    Dispatch:
        * ``verticals/<v>/ontology.yaml`` — apply ``concept`` rows whose
          payload ``vertical`` matches ``<v>`` (upsert-replace the list),
          then ``concept_property`` rows for the same vertical patch on
          top, then ``rebind`` rows append column-name indicators. The
          order matters: concept defines / replaces a whole concept entry;
          concept_property patches one field on the (possibly
          just-replaced) concept; rebind pulls a column onto the final
          concept state.
        * ``verticals/<v>/validations`` — the logical validation collection
          (DAT-438): apply ``validation`` rows whose payload ``vertical``
          matches ``<v>`` (upsert-replace by ``validation_id``).
        * ``verticals/<v>/cycles.yaml`` — the cycle vocabulary (DAT-455):
          apply ``cycle`` rows whose payload ``vertical`` matches ``<v>``
          (upsert-replace by cycle name into ``cycle_types``).
        * ``verticals/<v>/metrics`` — the logical metric collection (DAT-456):
          apply ``metric`` rows whose payload ``vertical`` matches ``<v>``
          (upsert-replace by ``graph_id`` into ``metrics``).
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

    if relative_path.startswith(_VERTICAL_PREFIX):
        for family in _VERTICAL_REGISTRY:
            if not relative_path.endswith(family.suffix):
                continue
            vertical = relative_path[len(_VERTICAL_PREFIX) : -len(family.suffix)]
            merged = base
            for teach_type, apply in family.appliers:
                matching = [
                    r
                    for r in rows
                    if r.type == teach_type and r.payload.get("vertical") == vertical
                ]
                if matching:
                    merged = apply(merged, matching)
            return merged

    merged = base
    for teach_type, spec in _REGISTRY.items():
        if spec.target_path != relative_path:
            continue
        matching = [r for r in rows if r.type == teach_type]
        if matching:
            merged = spec.apply(merged, matching)
    return merged
