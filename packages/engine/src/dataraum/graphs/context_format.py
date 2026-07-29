"""Served-context renderer (DAT-869 split of ``graphs/context.py``).

``format_served_context`` renders the assembled ``GraphExecutionContext`` into
the grounding prompt's metadata document: the concept graph and its groundings
as STRUCTURE, then the knowledge sections (value sets, drivers, validation
results, business cycles). Pure rendering — every fact it prints was resolved
by ``context_reads``; nothing here reads the database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.graphs.context_models import (
    _NON_CATEGORICAL_ROLES,
    ColumnContext,
    ConceptReconciliation,
    GraphExecutionContext,
    GroundingContext,
    SliceContext,
    TableContext,
)

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# =============================================================================
# Served-Context Formatter (DAT-734 — the graph-shaped grounding document)
# =============================================================================


def format_served_context(
    context: GraphExecutionContext,
    source_name: str = "dataset",
) -> str:
    """Render the served context for the grounding prompt (``{rich_context}``).

    Graph structure served AS STRUCTURE — the concept graph (definitions,
    part_of/disjoint/reconciles edges, groundings with their used columns), FK
    references, conformed axes, materialization — plus the typed knowledge
    sections with no graph element yet: value sets, drivers, business
    processes, validation results (conventions ride their own prompt slot).

    Args:
        context: GraphExecutionContext from build_execution_context()
        source_name: Human-readable name for the data source

    Returns:
        Formatted markdown metadata document
    """
    lines: list[str] = []

    # --- Overview ---
    lines.append(f"# Data Catalog: {source_name}")
    lines.append("")
    total_columns = sum(t.column_count for t in context.tables)
    lines.append(f"{len(context.tables)} tables, {total_columns} columns.")
    lines.append("")

    _append_reporting_calendar(lines, context)

    # --- Business Concepts (the traversal core, DAT-734) ---
    _append_concepts(lines, context)

    # --- Tables ---
    lines.append("## Tables")

    for table in context.tables:
        table_type = table.table_role.upper() if table.table_role else ""

        display_name = table.duckdb_name or table.table_name
        type_label = f" ({table_type})" if table_type else ""
        lines.append(f"\n### {display_name}{type_label}")

        # Entity + description — independent fields; a table can carry a description
        # without an entity_type (don't nest one under the other, or the description
        # is dropped whenever entity_type is absent).
        if table.entity_type or table.table_description:
            desc_parts = []
            if table.entity_type:
                desc_parts.append(f"**Entity**: {table.entity_type}")
            if table.table_description:
                desc_parts.append(table.table_description)
            lines.append(" — ".join(desc_parts))

        # Grain, rows, time column
        meta_parts = []
        if table.grain_columns:
            meta_parts.append(f"**Grain**: {', '.join(table.grain_columns)}.")
        if table.row_count:
            meta_parts.append(f"**Rows**: {table.row_count:,}.")
        # Event-time axes (DAT-565): the answer agent picks the lens per question,
        # so render each with its granularity/range and one-line note. EVENT-role
        # only (DAT-780) — an attribute date (role='attribute') is a normal column
        # in the table below, never presented here as a trend/time lens.
        for tc in table.time_columns:
            name = tc.get("column")
            if not name or tc.get("role") != "event":
                continue
            time_col = next((c for c in table.columns if c.column_name == name), None)
            label = f"by {tc['aspect']}" if tc.get("aspect") else None
            time_info = f"**Time column**: {name}" + (f" ({label})" if label else "")
            if time_col:
                time_parts = []
                if time_col.detected_granularity:
                    time_parts.append(time_col.detected_granularity)
                if time_col.min_timestamp and time_col.max_timestamp:
                    time_parts.append(f"{time_col.min_timestamp} to {time_col.max_timestamp}")
                if time_col.span_days is not None:
                    time_parts.append(f"{time_col.span_days:.0f}d span")
                # Flag a discontinuous axis: a large worst-gap warns the agent the
                # series isn't a clean continuum for period-over-period work.
                if time_col.largest_gap_days:
                    time_parts.append(f"largest gap {time_col.largest_gap_days:.0f}d")
                if time_parts:
                    time_info += f" — {', '.join(time_parts)}"
            if tc.get("note"):
                time_info += f". {tc['note']}"
            meta_parts.append(time_info.rstrip(".") + ".")
        # Recurring identities (DAT-565): would-be foreign keys / cluster keys —
        # the agent uses these for "per <entity>" grouping when writing queries.
        identity_parts = []
        for ic in table.identity_columns:
            name = ic.get("column")
            if not name:
                continue
            entry = name
            if ic.get("note"):
                entry += f" ({ic['note'].rstrip('.')})"
            identity_parts.append(entry)
        if identity_parts:
            meta_parts.append(f"**Identity columns**: {', '.join(identity_parts)}.")
        if meta_parts:
            lines.append(" ".join(meta_parts))

        # Column table. Business meaning is NOT here — its one home is the
        # COLUMN MEANINGS block (field_mappings, DAT-769). Materialization is
        # the graph-resolved stock/flow verdict (og_columns, DAT-734).
        lines.append("")
        lines.append("| Column | Type | Role | Materialization | Notes |")
        lines.append("|--------|------|------|-----------------|-------|")
        for col in table.columns:
            col_type = col.data_type or ""
            col_role = col.semantic_role or ""
            col_mat = col.materialization or ""
            col_notes = _build_column_notes(col)
            lines.append(
                f"| {col.column_name} | {col_type} | {col_role} | {col_mat} | {col_notes} |"
            )

        # Value sets (DAT-616): complete enumeration of low-card categoricals, so the
        # agent grounds metric predicates in real values rather than guessing a filter.
        value_sets = _build_value_sets(table)
        if value_sets:
            lines.append("")
            lines.append("**Value sets** (categorical columns — `value (count)`):")
            lines.extend(value_sets)

    # --- Drivers (DAT-616) ---
    _append_drivers(lines, context)

    # --- Relationships (the graph's refs edges) ---
    if not context.relationships and not context.graph_readable:
        # Unreadable graph ≠ zero relationships — state the absence (DAT-853).
        lines.append("")
        lines.append("## Relationships")
        lines.append("")
        lines.append("(not analyzed — the operating-model graph is not readable for this run)")
    if context.relationships:
        lines.append("")
        lines.append("## Relationships")
        lines.append("")
        lines.append("| From | To | Cardinality | Confidence | Confirmed |")
        lines.append("|------|----|-------------|------------|-----------|")
        for rel in context.relationships:
            warning = ""
            # DAT-616 fan-trap: joining here multiplies rows → SUMming an additive
            # measure across this join double-counts. Tell the agent to aggregate
            # before the join (or COUNT DISTINCT), not after. Reads the engine's
            # introduces_duplicates flag — measuring it is the writers' job, not this
            # renderer's: the LLM-synthesis path (DAT-628), the surrogate mint, and
            # the manual-add materialize seam (DAT-790) all measure it empirically.
            # NULL = the probe was unavailable/failed — the caution is then silently
            # absent (unmeasured), never "verified safe".
            if rel.introduces_duplicates:
                warning = " ⚠ fan-out: SUM across this join double-counts (pre-aggregate)"
            lines.append(
                f"| {rel.from_table}.{rel.from_column} | {rel.to_table}.{rel.to_column} "
                f"| {rel.cardinality or '?'} | {rel.confidence:.2f} "
                f"| {rel.confirmation_source or 'unconfirmed'}{warning} |"
            )

    # --- Conformed dimensions (og_conformed_dimension, DAT-756) ---
    if context.conformed_dimensions:
        lines.append("")
        lines.append("## Conformed Dimensions")
        lines.append("")
        lines.append(
            "Facts sharing a dimension AXIS (same dimension table + attribute) — the "
            "alignable drill-across surfaces. Comparing two facts goes through a shared "
            "axis, never a direct fact-to-fact join: compose one subquery per fact and "
            "merge them on the shared axis below. Only CONFIRMED conformance is listed; "
            "a pair absent from this list has no legal merge key, and comparing it "
            "anyway would assert an identity nobody established."
        )
        for cd in context.conformed_dimensions:
            attr = f".{cd.attribute}" if cd.attribute else ""
            # Render the JOIN COLUMNS, not the conformed_group. The group is the
            # stable identity this axis is grouped by internally, but it embeds a
            # table uuid — a reader cannot use it and a model cannot write SQL with
            # it, so putting it in the prompt spends tokens on an unusable token.
            # The two facts may spell the axis differently, so both sides are named.
            left = f"{cd.table_a}.{cd.role_a}" if cd.role_a else cd.table_a
            right = f"{cd.table_b}.{cd.role_b}" if cd.role_b else cd.table_b
            src = f" ({cd.confirmation_source})" if cd.confirmation_source else ""
            lines.append(f"- {left} ↔ {right} share {cd.dimension_table}{attr}{src}")

    # --- Enriched Views ---
    # Tracks whether any slice list actually reached the document. The curation
    # note below qualifies THAT list, so without one there is nothing to qualify
    # — and a bare "showing 12 of 53 dimensions" with no dimensions in sight
    # reads as though a section went missing.
    rendered_any_slices = False
    if context.enriched_views:
        lines.append("")
        lines.append("## Enriched Views")

        slices_by_table: dict[str, list[SliceContext]] = {}
        for s in context.available_slices:
            slices_by_table.setdefault(s.table_name, []).append(s)

        for ev in context.enriched_views:
            verified = " (grain verified)" if ev.is_grain_verified else ""
            lines.append(f"\n### {ev.view_name}{verified}")
            fact_line = f"Fact table: {ev.fact_table}."
            # derived_from bases (og_derived_from) — which dimension TABLES the
            # view already joins, so the agent knows what it need not join again.
            if ev.dimension_tables:
                fact_line += f" Joins dimensions: {', '.join(ev.dimension_tables)}."
            lines.append(fact_line)
            dims = ", ".join(ev.dimension_columns) if ev.dimension_columns else "none"
            lines.append(f"Joined columns: {dims}.")

            # DAT-621: list the slice dimension NAMES only — their value-sets are served
            # COMPLETE (or size-stated) in the per-table Value sets block, so re-rendering a
            # capped [:10] sample here was redundant duplication + a partial sample.
            # DAT-879: each name carries its measured relevance, so the agent can see
            # WHY the order is what it is instead of trusting a bare sequence.
            view_slices = slices_by_table.get(ev.fact_table, [])
            if view_slices:
                rendered_any_slices = True
                names = ", ".join(_format_slice_axis(s) for s in view_slices)
                lines.append(f"Slice dimensions: {names} — see Value sets for the values.")

    # What the slice curation left out (DAT-879/DAT-622). Rendered ONCE, next to
    # the dimensions it qualifies: an agent told "these are the dimensions" with
    # no indication that forty more exist will reason as if the list is complete.
    if rendered_any_slices and context.slice_catalog_note:
        lines.append("")
        lines.append(f"_{context.slice_catalog_note}_")

    # --- Business Processes ---
    if not context.business_cycles and not context.operating_model_analyzed:
        # No promoted operating-model run: cycles were never analyzed. Omitting
        # the section would be byte-identical to "analyzed, none detected" —
        # the LLM must be able to tell the two apart (DAT-853).
        lines.append("")
        lines.append("## Business Processes")
        lines.append("")
        lines.append("(not yet analyzed — no operating-model run for this workspace)")
    if context.business_cycles:
        lines.append("")
        lines.append("## Business Processes")
        _append_business_processes(lines, context)

    # --- Validation Results ---
    if not context.validations and not context.operating_model_analyzed:
        lines.append("")
        lines.append("## Validation Results")
        lines.append("")
        lines.append("(not yet analyzed — no operating-model run for this workspace)")
    if context.validations:
        lines.append("")
        lines.append("## Validation Results")
        lines.append("")
        # Bucket by STATUS, not the passed bool (DAT-439): error = the
        # evaluation was inconclusive and skipped = never executed — labeling
        # either as FAILED would tell the LLM the data failed a check it was
        # never actually judged by.
        passed = [v for v in context.validations if v.passed]
        failed = [v for v in context.validations if v.status == "failed"]
        unjudged = [v for v in context.validations if v.status in ("error", "skipped")]
        lines.append(f"PASSED: {len(passed)} | FAILED: {len(failed)} | UNJUDGED: {len(unjudged)}")
        if failed:
            lines.append("")
            lines.append("Failed:")
            for v in failed:
                lines.append(f"- [{v.severity.upper()}] {v.validation_id}: {v.message}")
                if v.details:
                    summary = v.details.get("summary", "")
                    if summary:
                        lines.append(f"  Details: {summary}")
        if unjudged:
            lines.append("")
            lines.append("Unjudged (inconclusive or not executed — NOT data failures):")
            for v in unjudged:
                lines.append(f"- [{v.status}] {v.validation_id}: {v.message}")

    return "\n".join(lines)


_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)  # fmt: skip


def _append_reporting_calendar(lines: list[str], context: GraphExecutionContext) -> None:
    """Render the workspace's reporting calendar (DAT-887).

    **Fact only — no instruction.** This document is shared: ``validation_sql`` and
    ``validation_induction`` render the same served context, and an imperative aimed at
    the grounding author ("do not filter the period axis") would silently become a rule
    for authors this binding does not apply to. The instruction's one home is
    ``graph_sql_generation.yaml``; what belongs HERE is the fiscal year's start and
    whether it was declared or assumed, so any consumer can caveat accordingly.

    Absence is rendered as absence (the section is omitted), never as a fabricated
    calendar year.
    """
    calendar = context.reporting_calendar
    if calendar is None:
        return
    month = _MONTHS[calendar.fiscal_year_start_month - 1]
    basis = (
        "declared by this workspace"
        if calendar.source == "declared"
        else "not declared — the calendar-year default is assumed"
    )
    lines.append("## Reporting calendar")
    lines.append("")
    lines.append(f"Fiscal year starts in {month} ({basis}).")
    lines.append(
        "A reporting year therefore closes at the start of that month; a period-labelled "
        "row carries the level at the END of its own period."
    )
    lines.append("")


def _append_concepts(lines: list[str], context: GraphExecutionContext) -> None:
    """Append the concept graph (DAT-734): definitions + edges + groundings.

    The traversal core served as structure. Definition surface (description /
    indicators / excludes — the DAT-616 value-grounding aid, incl. traps like
    ``Cost Recovery Income`` being revenue despite "cost") rides each concept;
    the graph neighbourhood (part_of / disjoint_with / reconciles_with) and the
    concept's PRIOR GROUNDINGS (relation + filter + value expression + used
    columns; failures discriminated with the reason) follow as data lines.
    """
    if not context.concepts:
        return

    lines.append("## Business Concepts")
    lines.append("")
    lines.append(
        "Vertical vocabulary with its operating-model graph. Ground each metric concept "
        "in specific column values from the **Value sets** below — match by meaning, "
        "honoring `exclude` patterns; do not improvise a substring filter. A `grounded by` "
        "entry is a PRIOR COMMITTED grounding of that concept — reuse its columns/filters "
        "for the same concept unless the served evidence says it is wrong; a concept with "
        "several groundings is measured on several relations, and `reconciles` means those "
        "computations must tie out — each entry states whether the last completed run "
        "actually checked that, and what it observed."
    )
    lines.append("")
    for concept in context.concepts:
        line = f"- **{concept.name}**"
        if concept.kind:
            line += f" ({concept.kind})"
        if concept.description:
            line += f": {concept.description}"
        lines.append(line)
        if concept.indicators:
            lines.append(f"  - indicators: {', '.join(concept.indicators)}")
        if concept.exclude_patterns:
            # exclude_patterns are column-NAME match exclusions consumed HERE (the
            # grounding prompt tells the model to honor them when matching a concept
            # to a column, and NOT to improvise a substring row filter). DAT-733
            # evaluated them as a second source for the canonical validity SCOPE and
            # rejected it: they are not row predicates, so no faithful (column_id,
            # operator, value) triple exists and fabricating one is forbidden. The
            # validity scope sources solely from a measured cycle's completion status.
            lines.append(f"  - exclude: {', '.join(concept.exclude_patterns)}")
        if concept.part_of_parents:
            part_of = ", ".join(concept.part_of_parents)
            if concept.part_of_ancestry:
                part_of += f" (→ {' → '.join(concept.part_of_ancestry)})"
            lines.append(f"  - part of: {part_of}")
        if concept.part_of_children:
            lines.append(f"  - subconcepts: {', '.join(concept.part_of_children)}")
        if concept.disjoint_with:
            lines.append(f"  - disjoint with: {', '.join(concept.disjoint_with)}")
        for rec in concept.reconciles_with:
            tol = f" (tolerance {rec.tolerance:g})" if rec.tolerance is not None else ""
            subject = (
                "reconciles: across its own groundings"
                if rec.partner == concept.name
                else f"reconciles with: {rec.partner}"
            )
            lines.append(f"  - {subject}{tol} — {_reconciliation_state(rec)}")
        healthy = [g for g in concept.groundings if not g.failed]
        failed = [g for g in concept.groundings if g.failed]
        if healthy:
            lines.append("  - grounded by:")
            for g in healthy:
                lines.append(f"    - {_format_grounding(g)}")
                if g.uses:
                    uses = ", ".join(f"{u.column_name} ({u.role})" for u in g.uses)
                    lines.append(f"      uses: {uses}")
        for g in failed:
            mode = g.failure_mode or "failed"
            reason = g.failure_reason or "(no reason recorded)"
            lines.append(f"  - failed attempt [{mode}]: {reason}")
    lines.append("")


#: Why a tie-out was not computed, phrased for a reader of the served document.
_ABSTAIN_PHRASING: dict[str, str] = {
    "no_evaluable_pair": "only one grounding exists to measure",
    "different_reporting_instants": "the groundings are bound to different reporting instants",
    "different_aggregations": "the groundings aggregate differently",
    "unresolved_grounding": "a grounding has no executable form",
    "execution_failed": "a grounding failed to execute",
    "no_value": "a grounding measured no support",
    "non_numeric_value": "a grounding returned a value that is not a quantity",
}


def _reconciliation_state(rec: ConceptReconciliation) -> str:
    """What the last promoted run observed for one assertion.

    The distinction this wording exists to hold: an assertion nobody has
    evaluated is NOT an assertion that held. Only a run that executed both sides
    can say anything about agreement, so the un-evaluated case states the
    contract and says plainly that it is unchecked.

    A delta observed with no declared tolerance is reported as a MEASUREMENT and
    nothing more. Calling it a discrepancy would grade it against a band nobody
    set, which is the judgement this whole path refuses to invent.
    """
    if rec.status is None:
        return "must tie out (not yet evaluated)"
    if rec.status == "abstained":
        reason = _ABSTAIN_PHRASING.get(rec.abstain_reason or "", "no comparable pair was found")
        return f"must tie out; not compared because {reason}"

    scope = f" (widest of {rec.evaluated_pairs} pairs)" if rec.evaluated_pairs > 1 else ""
    # A partial evaluation must never read as a whole one. Reporting "ties out
    # exactly" for a concept where three of five asserted pairs were never
    # compared states verification the run did not do — the exact class this
    # whole path exists to prevent, so the remainder rides every verdict.
    uncompared = rec.pairs - rec.evaluated_pairs
    if uncompared > 0:
        scope += f"; {uncompared} of {rec.pairs} pairs not comparable"
    relative = rec.relative_delta or 0.0
    if rec.verdict == "beyond_tolerance":
        return f"evaluated: {relative:.3g} relative divergence exceeds the tolerance{scope}"
    if rec.verdict == "within_tolerance":
        return f"evaluated: ties out within tolerance, {relative:.3g} relative{scope}"
    if not rec.observed_delta:
        return f"evaluated: the groundings tie out exactly{scope}"
    return (
        f"evaluated: observed delta {rec.observed_delta:g} ({relative:.3g} relative){scope}"
        " — no tolerance is declared, so this is a measurement, not a failure"
    )


def _format_grounding(g: GroundingContext) -> str:
    """One healthy grounding as ``statement @ relation: select_expr WHERE ...``."""
    label = f"{g.statement} @ {g.relation}" if g.statement else str(g.relation)
    rendered = f"{label}: {g.select_expr}"
    if g.where:
        rendered += " WHERE " + " AND ".join(g.where)
    return rendered


# A column whose single most-frequent value covers more than this fraction is near-constant
# — not a discriminator (e.g. a 99.6%-true `sale` boolean). Grounding a concept on it is
# silently wrong, so it's flagged, never served as a groundable value-set.
_NEAR_CONSTANT_FRAC = 0.9


def _build_value_sets(table: TableContext) -> list[str]:
    """Render the value enumeration for a table's categorical columns (DAT-621).

    The agent grounds a concept in the discriminator VALUES from here, never a guessed
    ILIKE:
    - low-card (≤ reasonable-top) + non-degenerate → the COMPLETE value-set inline (the
      assembler fetched it live);
    - high-card (> reasonable-top) → size + a frequency sample + the ``search_values``
      hint (DAT-699). The GraphAgent can now drill: it resolves the exact values by
      bounded substring search and grounds the IN-list on the results. The old
      render-nothing rule made a present-but-unenumerated concept structurally
      ungroundable — concepts present by name in a several-hundred-value column
      were unreachable and the agent emitted SELECT NULL for them;
    - degenerate (one value dominates) → flagged "near-constant", NO value-set — grounding
      on a ~constant flag (e.g. a 99%-true boolean) is silently wrong.
    Only key/measure/time roles are skipped (never partitions).
    """
    out: list[str] = []
    for col in table.columns:
        if not col.top_values:
            continue
        if col.semantic_role and col.semantic_role.lower() in _NON_CATEGORICAL_ROLES:
            continue
        served = len(col.top_values)
        dc = col.distinct_count
        # High-card / incomplete-fetch → size + sample + the drill hint; the
        # values NEVER render as an (incomplete) enumeration the agent might
        # mistake for the complete set.
        if dc is not None and dc > served:
            sample = ", ".join(
                str(tv.get("value")) for tv in col.top_values[:8] if tv.get("value") is not None
            )
            out.append(
                f"- **{col.column_name}**: {dc} distinct values — NOT enumerated; "
                f"resolve exact values with the search_values tool before filtering. "
                f"Most frequent: {sample}"
            )
            continue
        # Degenerate / near-constant → not a discriminator; flag, don't serve as groundable
        # (grounding a concept on a ~constant flag is silently wrong).
        counts = [tv.get("count") or 0 for tv in col.top_values]
        total = sum(counts)
        if total and max(counts) / total > _NEAR_CONSTANT_FRAC:
            out.append(
                f"- **{col.column_name}**: near-constant ({dc} distinct, one value ≥90%) — "
                "NOT a discriminator, do not filter on it"
            )
            continue
        rendered = ", ".join(
            f"{tv.get('value')} ({tv.get('count')})"
            for tv in col.top_values
            if tv.get("value") is not None
        )
        if not rendered:
            continue
        out.append(
            f"- **{col.column_name}** (complete, {dc if dc is not None else served} distinct): {rendered}"
        )
    return out


def _format_slice_axis(s: SliceContext) -> str:
    """Render one curated slice axis: name, cardinality, and why it ranks here.

    The relevance number is shown rather than implied by position — an agent
    that can see 0.94 next to one axis and 0.11 next to another can weigh them,
    where a bare ordered list only invites it to trust the order. Omitted when
    unmeasured (no statistical profile), because printing 0.00 there would
    assert the axis resolves nothing when in truth nothing measured it.
    """
    parts = [f"{s.column_name} ({s.value_count} values"]
    if s.relevance is not None:
        parts.append(f", relevance {s.relevance:.2f}")
    if s.interest:
        parts.append(f", {s.interest}")
    parts.append(")")
    return "".join(parts)


def _build_column_notes(col: ColumnContext) -> str:
    """Build column notes: range/sign, anchor axis, derivation, readiness, flags.

    Business meaning / unit-source prose is NOT here — the column-meanings feed
    (``field_mappings``) is its one home (DAT-769).
    """
    notes = []

    # DAT-616: measure range/sign — a negative min flags a signed measure (debit/credit),
    # where a bare SUM may not be the intended metric (a signed/net expression might be).
    if col.semantic_role == "measure" and col.numeric_min is not None:
        rng = f"Range: {col.numeric_min:g}..{col.numeric_max:g}."
        if col.numeric_min < 0:
            rng += " Signed (has negatives) — SUM nets positive and negative values."
        notes.append(rng)

    # The MEASURED storage convention (DAT-875). The range note above says a column
    # carries negatives; this says WHY, which is the part an extract needs: whether
    # the magnitude a bare SUM returns is already a natural balance. Absent when
    # undetermined — no fact beats a guess.
    if col.stored_sign == "ledger_signed":
        # Descriptive, never prescriptive. An earlier draft added "a bare SUM returns
        # a signed quantity, not a natural-balance magnitude" — false wherever the
        # reconciling population is single-family or the measure is not account-shaped
        # (the two conventions coincide there, so the label is correct but the
        # consequence is not), and it invited a sign flip on a family that need not
        # exist. State the convention and let the author reason about its own query.
        notes.append(
            "Stored sign: ledger_signed — values follow one raw ledger direction "
            "across account families, so a credit-normal account (liability, equity, "
            "revenue) carries the opposite sign to its natural balance."
        )
    elif col.stored_sign == "natural_balance":
        notes.append(
            "Stored sign: natural_balance — each account family's natural direction is "
            "already applied, so a bare SUM returns a natural-balance magnitude."
        )

    # The measure's resolved anchor event-time axis (og_columns, DAT-780) — the
    # axis it trends/accumulates by.
    if col.semantic_role == "measure" and col.anchor_time_axis:
        notes.append(f"Anchor axis: {col.anchor_time_axis}.")

    if col.is_derived and col.derived_formula:
        notes.append(f"Derived: {col.derived_formula}.")

    # Entropy readiness indicator. DAT-853 abstention: a column the detectors never
    # measured must NOT render like one measured clean — its readiness band is
    # vacuous, so it is withheld and the absence stated; a partially-measured
    # column keeps its band but says what it rests on.
    if col.entropy_scores:
        coverage = col.entropy_scores.get("coverage")
        readiness = col.entropy_scores.get("readiness", "ready")
        if coverage == "unmeasured":
            notes.append("◌ unmeasured — no quality measurements exist for this column.")
        else:
            if readiness == "blocked":
                notes.append("⛔ blocked.")
            elif readiness == "investigate":
                notes.append("⚠ investigate.")
            if coverage == "partial":
                notes.append("◌ partially measured.")

    if col.flags:
        notes.append(f"Flags: {', '.join(col.flags)}.")

    return " ".join(notes)


def _append_drivers(lines: list[str], context: GraphExecutionContext) -> None:
    """Append the per-measure driver rankings (DAT-616).

    Grounds the aggregation choice (`target_type`) and tells the agent which
    dimensions/values move each measure. `interesting_slices` carry the actual
    dimension VALUES with signed effect + support — a HINT for which values carry
    data, never the complete value-set (that's the per-column Value sets).

    The ONE read-side convention (DAT-859): gate on `status == "measured"` ONLY —
    an abstained ranking (temporal_behavior undetermined, no enriched view, too few
    candidates, no usable measure value) never surfaces as a driver, full stop.
    This must NOT also gate on content, or "measured" behavior changes: a measured
    ranking that found nothing (no ranked dims/slices/secondaries — a real "no
    significant driver" answer) still renders its heading, with an explicit
    absence line — "analyzed, nothing significant" is a visible grounding signal
    in its own right, distinct from both abstention (never analyzed for a known
    reason) and non-analysis (`context.drivers` empty altogether, DAT-853's
    absence-falls-loud principle applied here). The raw artifact stays honest
    either way — this is prompt-rendering only.
    """
    measured = [d for d in context.drivers if d.status == "measured"]
    if not measured:
        return

    lines.append("")
    lines.append("## Drivers")
    lines.append("")
    lines.append(
        "Per-measure drivers (statistical, FDR-gated on this data). `target_type` grounds the "
        "aggregation: flow→SUM across periods, stock→latest-period only, ratio→Σnum/Σden. "
        "`interesting_slices` are values that MOVE the measure — a hint, NOT the value-set."
    )
    for d in measured:
        grain_note = f", grain {d.grain}" + (f"/{d.entity}" if d.entity else "")
        lines.append(f"\n### {d.measure_label} ({d.target_type}{grain_note})")
        if not (d.ranked_dimensions or d.interesting_slices or d.secondary_dimensions):
            lines.append("- No significant driver found.")
            continue
        if d.ranked_dimensions:
            dims = ", ".join(
                f"{r.get('dimension')} ({r.get('gain'):.2f})"
                if isinstance(r.get("gain"), (int, float))
                else str(r.get("dimension"))
                for r in d.ranked_dimensions
            )
            lines.append(f"- **Top dimensions**: {dims}")
        if d.interesting_slices:
            slices = "; ".join(
                f"{s.get('dimension')}={s.get('value')} "
                f"(effect {s.get('effect'):+.2f}, support {s.get('support')})"
                if isinstance(s.get("effect"), (int, float))
                else f"{s.get('dimension')}={s.get('value')}"
                for s in d.interesting_slices
            )
            lines.append(f"- **Notable slices** (hint, not the set): {slices}")
        if d.secondary_dimensions:
            sec = ", ".join(
                f"{s.get('dimension')} ({s.get('grain')})" for s in d.secondary_dimensions
            )
            lines.append(f"- **Secondary** (other grain): {sec}")


def _append_business_processes(lines: list[str], context: GraphExecutionContext) -> None:
    """Append business processes section."""
    # Build health lookup
    health_lookup: dict[str, Any] = {}
    if context.cycle_health:
        for cs in context.cycle_health.cycle_scores:
            if cs.canonical_type:
                health_lookup[cs.canonical_type] = cs

    for cycle in context.business_cycles:
        # Determine verification status
        health_score = health_lookup.get(cycle.cycle_type)
        if health_score:
            score = health_score.composite_score
            if score is not None and score >= 0.8:
                status = "VERIFIED"
            elif score is not None and score >= 0.5:
                status = "PARTIAL"
            else:
                status = "UNVERIFIED"
            val_info = (
                f"({health_score.validations_passed}/{health_score.validations_run} validations)"
            )
        else:
            status = "UNVERIFIED"
            val_info = ""

        # A family cycle names its direction honestly (DAT-856): a decided direction
        # reads as e.g. "accounts_payable, direction outgoing"; an undirected one reads
        # as "settlement, direction undetermined" — the detected-but-undirected state
        # served as exactly that, never a guessed label. A non-family cycle is unchanged.
        type_label = cycle.cycle_type
        if cycle.direction is not None:
            type_label = f"{cycle.cycle_type}, direction {cycle.direction}"
        lines.append(f"\n### {cycle.cycle_name} ({type_label}) — {status} {val_info}")
        lines.append("")

        if cycle.description:
            lines.append(cycle.description)

        # Volume
        volume_parts = []
        if cycle.total_records is not None:
            volume_parts.append(f"{cycle.total_records:,} records")
        if cycle.completed_cycles is not None:
            volume_parts.append(f"{cycle.completed_cycles:,} completed")
        if cycle.completion_rate is not None:
            volume_parts.append(f"{cycle.completion_rate:.0%} completion rate")
        if volume_parts:
            lines.append(f"Volume: {', '.join(volume_parts)}.")

        # Evidence
        if cycle.evidence:
            # DAT-621: no silent [:3] cut — evidence is a short narrative list; serve all.
            evidence_str = "; ".join(cycle.evidence)
            lines.append(f"Evidence: {evidence_str}")

        # Stages
        if cycle.stages:
            lines.append("")
            lines.append("Stages:")
            for stage in sorted(cycle.stages, key=lambda s: s.stage_order):
                vals = ", ".join(stage.indicator_values) if stage.indicator_values else ""
                ind_col = f" {stage.indicator_column}" if stage.indicator_column else ""
                indicator = f" →{ind_col} in [{vals}]" if vals else ""
                progress = (
                    f" ({stage.completion_rate:.0%})" if stage.completion_rate is not None else ""
                )
                lines.append(f"  {stage.stage_order}. {stage.stage_name}{indicator}{progress}")

        # Completion tracking (narrative — status_column is bare since DAT-733, so
        # re-qualify with its table for a precise, readable reference).
        if cycle.status_column and cycle.completion_value:
            status_ref = (
                f"{cycle.status_table}.{cycle.status_column}"
                if cycle.status_table
                else cycle.status_column
            )
            lines.append(
                f'Completion: {status_ref} = "{cycle.completion_value}"'
                + (
                    f", {cycle.completion_rate:.0%} complete"
                    if cycle.completion_rate is not None
                    else ""
                )
                + "."
            )

        # Concept bindings (DAT-616): the lifecycle/status concepts this cycle defines
        # as an EXPLICIT, IN-list-ready concept → (column, value-set) map — the one
        # detection-confirmed value→concept binding the engine already has (≈ the cut
        # DAT-620 binding shape). The narrative above is for reading; THIS is for
        # grounding a filter. Covers lifecycle/status concepts, not P&L partitions.
        #
        # DAT-733: the status_column = completion_value binding is DELIBERATELY NOT
        # emitted here anymore. That IS the canonical validity scope, and the engine
        # now composes it deterministically by default (graphs/agent grounding path).
        # With the imperative binding present, the LLM would author the predicate on
        # every grounding, the engine's defer-on-existing-constraint bypass would
        # always fire, and the typed default would never be the actual mechanism.
        # Withholding it makes the deterministic guarantee the real path and a
        # LLM-authored status constraint a GENUINE judgment (→ a visible bypass
        # assumption). The stage bindings below are legitimate per-concept filters,
        # not the validity scope, so they stay.
        binding_lines: list[str] = []
        for stage in sorted(cycle.stages, key=lambda s: s.stage_order):
            if stage.indicator_column and stage.indicator_values:
                vals = ", ".join(f"'{v}'" for v in stage.indicator_values)
                binding_lines.append(
                    f'  - "{stage.stage_name}" = WHERE {stage.indicator_column} IN ({vals})'
                )
        if binding_lines:
            lines.append("Concept bindings (confirmed — use as the filter, do not improvise):")
            lines.extend(binding_lines)

        # Entity flows
        if cycle.entity_flows:
            for ef in cycle.entity_flows:
                lines.append(
                    f"Entity flow: {ef.entity_type} "
                    f"({ef.entity_table}.{ef.entity_column}) → {ef.fact_table}."
                )

    return


__all__ = [
    "format_served_context",
]
