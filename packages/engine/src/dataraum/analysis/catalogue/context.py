"""Composed-catalogue evidence for the catalogue_semantics prompt (DAT-823).

Builds the prompt inputs the catalogue agent authors from. Every read is
scoped and fail-closed:

- begin_session-versioned tables (``TableEntity``, ``Relationship`` via
  :func:`load_defined_relationships`, ``EnrichedView``, ``SliceDefinition``)
  are read at THIS run's ``run_id`` — never through the head-resolved
  ``current_*`` views, which still name the PRIOR promoted run mid-run;
- typed statistical profiles (value samples, measure sign/range) are written
  under ADD_SOURCE runs, so they are pinned per table via the promoted
  generation head (``head_run_id(table:{id}, generation)``) — the same pin
  the cycles context and the slicing role gate use (DAT-630/725). A table
  with no promoted head serves NOTHING (fail-closed), never an arbitrary
  coexisting run's values.

The value samples on the join chain are the load-bearing section: the
discrimination between two look-alike ledgers lives in the counterparty
values riding the CONFIRMED relationship lines, so samples are rendered
attached to the relationship they travel on (privacy-gated by name pattern,
truncated, capped). Flat all-rows samples alone are not enough — a label
column shared by several populations blurs them (DAT-853: counterparty at
37% vendors flat, 100% vendors on the payment-linked rows), so each
reference line additionally carries CHAIN-CONDITIONED evidence: the
from-side identity labels aggregated over only the rows that resolve
across the join, plus the min/max (and plain sign statement) of the
from-side measure columns over that same joined population — the flow
sign the chain itself carries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.typing.db_models import TypeCandidate
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table
from dataraum.storage.snapshot_head import GENERATION_STAGE, head_run_id

if TYPE_CHECKING:
    import duckdb

    from dataraum.llm.privacy import DataSampler

logger = get_logger(__name__)

# String truncation for a single sample value — the house cap the semantic
# agent applies (``SemanticAgent._truncate_sample``).
_SAMPLE_MAX_CHARS = 100


def _truncate(value: Any, max_length: int = _SAMPLE_MAX_CHARS) -> str:
    text = str(value)
    if len(text) > max_length:
        return text[:max_length] + "..."
    return text


def _generation_heads(session: Session, table_ids: list[str]) -> dict[str, str | None]:
    """Per-table promoted add_source generation run — the profile/annotation pin."""
    return {tid: head_run_id(session, f"table:{tid}", GENERATION_STAGE) for tid in table_ids}


def _load_profiles(
    session: Session, columns_by_table: dict[str, list[Column]], heads: dict[str, str | None]
) -> dict[str, StatisticalProfile]:
    """Typed-layer profiles pinned at each table's generation head, by column_id.

    Fail-closed: a table with no promoted head contributes no profiles (its
    columns simply render without samples / ranges). One batched query (the
    ``graphs/context.py`` pattern, not per-table round trips): fetch rows for
    any pinned run, then keep exactly the row whose ``run_id`` matches ITS
    table's pin — the over-fetch is bounded to the pinned runs.
    """
    head_by_column: dict[str, str] = {}
    for table_id, columns in columns_by_table.items():
        run_id = heads.get(table_id)
        if run_id is None:
            continue
        for column in columns:
            head_by_column[column.column_id] = run_id
    if not head_by_column:
        return {}
    rows = session.execute(
        select(StatisticalProfile).where(
            StatisticalProfile.column_id.in_(list(head_by_column)),
            StatisticalProfile.run_id.in_(sorted(set(head_by_column.values()))),
            StatisticalProfile.layer == "typed",
        )
    ).scalars()
    return {p.column_id: p for p in rows if p.run_id == head_by_column.get(p.column_id)}


def _sample_line(
    sampler: DataSampler, column_name: str, profile: StatisticalProfile | None
) -> str | None:
    """Render a column's top values as one privacy-gated line, or None.

    Sensitive names (the same ``DataSampler.is_sensitive`` gate every prompt
    sample passes) render as ``<REDACTED>`` — existence stays visible, values
    never leave. Capped at the configured ``max_sample_values``; each value
    truncated.
    """
    if profile is None or not profile.profile_data:
        return None
    top_values = profile.profile_data.get("top_values") or []
    if not top_values:
        return None
    if sampler.is_sensitive(column_name):
        return "<REDACTED>"
    parts = []
    for tv in top_values[: sampler.config.max_sample_values]:
        pct = tv.get("percentage")
        pct_str = f" ({pct:.0f}%)" if isinstance(pct, (int, float)) else ""
        parts.append(f"'{_truncate(tv.get('value', ''))}'{pct_str}")
    return ", ".join(parts)


def _load_annotation_rows(
    session: Session,
    table_ids: list[str],
    heads: dict[str, str | None],
) -> list[dict[str, Any]]:
    """Object-grain per-column annotations, pinned per table at the generation head.

    The role/term/claim serving the per-table prompt reads, but run-scoped:
    ``SemanticAnnotation`` is add_source-run-versioned, so an unscoped scan
    would pick an arbitrary coexisting run's row (the DAT-725 staleness class).
    A table with no promoted head serves no annotations — fail-closed.
    ``detected_unit`` rides along (DAT-647): the value-carried unit the typing
    phase parsed, the evidence unit_source resolution leans on.
    """
    pinned = {tid: run_id for tid, run_id in heads.items() if run_id is not None}
    rows_out: list[dict[str, Any]] = []
    column_ids: list[str] = []
    if pinned:
        # One batched query (the graphs/context.py pattern): fetch any pinned
        # run's rows, keep exactly those matching THEIR table's pin.
        rows = session.execute(
            select(
                Table.table_id,
                Table.table_name,
                Column.column_name,
                Column.column_id,
                SemanticAnnotation.run_id,
                SemanticAnnotation.semantic_role,
                SemanticAnnotation.entity_type,
                SemanticAnnotation.business_name,
                SemanticAnnotation.temporal_behavior_claim,
                SemanticAnnotation.temporal_behavior_claim_confidence,
            )
            .join(Column, SemanticAnnotation.column_id == Column.column_id)
            .join(Table, Column.table_id == Table.table_id)
            .where(
                Table.table_id.in_(list(pinned)),
                SemanticAnnotation.run_id.in_(sorted(set(pinned.values()))),
            )
            .order_by(Table.table_name, Column.column_position)
        ).all()
        for row in rows:
            if row.run_id != pinned.get(row.table_id):
                continue
            rows_out.append(
                {
                    "table_name": row.table_name,
                    "column_name": row.column_name,
                    "column_id": row.column_id,
                    "semantic_role": row.semantic_role,
                    "entity_type": row.entity_type,
                    "business_name": row.business_name,
                    "temporal_behavior_claim": row.temporal_behavior_claim,
                    "temporal_behavior_claim_confidence": row.temporal_behavior_claim_confidence,
                }
            )
            column_ids.append(row.column_id)

    # Value-carried unit per column: most recent, then highest-confidence type
    # candidate — the load_persisted_annotations ordering (stale prior runs
    # must not leak a stale unit).
    detected_units: dict[str, str | None] = {}
    if column_ids:
        unit_rows = session.execute(
            select(TypeCandidate.column_id, TypeCandidate.detected_unit)
            .where(TypeCandidate.column_id.in_(column_ids))
            .order_by(TypeCandidate.detected_at.desc(), TypeCandidate.confidence.desc())
        ).all()
        for column_id, detected_unit in unit_rows:
            detected_units.setdefault(column_id, detected_unit)
    for annotation in rows_out:
        annotation["detected_unit"] = detected_units.get(annotation["column_id"])
    return rows_out


def _format_annotations(rows: list[dict[str, Any]]) -> str:
    """The object-grain role/term/claim block, grouped per table."""
    if not rows:
        return "No per-column annotations available."
    by_table: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_table.setdefault(row["table_name"], []).append(row)
    lines: list[str] = []
    for table_name, cols in by_table.items():
        lines.append(f"\n### {table_name}")
        for col in cols:
            parts = [f"role={col.get('semantic_role') or '(unknown)'}"]
            if col.get("entity_type"):
                parts.append(f"entity={col['entity_type']}")
            if col.get("business_name"):
                parts.append(f"term={col['business_name']!r}")
            claim = col.get("temporal_behavior_claim")
            if claim:
                conf = col.get("temporal_behavior_claim_confidence")
                conf_str = f"({conf:.2f})" if isinstance(conf, (int, float)) else ""
                parts.append(f"claim={claim}{conf_str}")
            if col.get("detected_unit"):
                parts.append(f"value_unit={col['detected_unit']}")
            lines.append(f"  - {col['column_name']}: {', '.join(parts)}")
    return "\n".join(lines)


def _format_structural_tables(
    session: Session,
    tables: list[Table],
    columns_by_table: dict[str, list[Column]],
    annotation_rows: list[dict[str, Any]],
    profiles: dict[str, StatisticalProfile],
    sampler: DataSampler,
    *,
    run_id: str,
    relationship_endpoint_ids: set[str],
) -> str:
    """This run's structural readings: role, grain, time/identity columns, measures.

    Identity columns carry their value samples here ONLY when they are not an
    endpoint of a served relationship — endpoint samples ride the relationship
    lines (the join chain is where the discrimination lives), so the table
    section doesn't duplicate them. Measures carry sign/range from the typed
    profile (section 7): whether a measure runs negative is a fact the meaning
    must survive.
    """
    entities: dict[str, TableEntity] = {
        e.table_id: e
        for e in session.execute(
            select(TableEntity).where(
                TableEntity.table_id.in_([t.table_id for t in tables]),
                TableEntity.run_id == run_id,
            )
        ).scalars()
    }
    role_by_column_id = {row["column_id"]: row.get("semantic_role") for row in annotation_rows}

    lines: list[str] = []
    for table in tables:
        columns = columns_by_table.get(table.table_id, [])
        col_id_by_name = {c.column_name: c.column_id for c in columns}
        entity = entities.get(table.table_id)
        header = f"\n### {table.table_name} — rows={table.row_count}"
        if entity is not None and entity.table_role:
            header += f", role={entity.table_role}"
        lines.append(header)
        if entity is None:
            lines.append("  (no structural reading for this table in this run)")
        else:
            if entity.grain_columns:
                lines.append(f"  grain: {', '.join(entity.grain_columns)}")
            for tc in entity.time_columns or []:
                anchor = ", anchor" if tc.get("is_anchor") else ""
                lines.append(
                    f"  time column {tc.get('column')} "
                    f"(aspect={tc.get('aspect')}, {tc.get('role')}{anchor}): {tc.get('note', '')}"
                )
            for ic in entity.identity_columns or []:
                name = ic.get("column", "")
                lines.append(f"  identity column {name}: {ic.get('note', '')}")
                column_id = col_id_by_name.get(name)
                if column_id and column_id not in relationship_endpoint_ids:
                    sample = _sample_line(sampler, name, profiles.get(column_id))
                    if sample:
                        lines.append(f"    values: {sample}")
        # Measure sign/range (section 7): from the typed profile at the
        # generation head — negative presence is decided by the measured
        # minimum, never guessed from the name.
        measure_lines: list[str] = []
        for column in columns:
            if role_by_column_id.get(column.column_id) != "measure":
                continue
            profile = profiles.get(column.column_id)
            numeric = (profile.profile_data or {}).get("numeric_stats") if profile else None
            if not numeric:
                continue
            # The writer's key shape: analysis/statistics persists
            # ``ColumnProfile.model_dump()`` whose NumericStats serializes as
            # ``min_value``/``max_value`` — NOT ``min``/``max``. Reading the
            # wrong keys rendered ``min=None max=None`` and silently dropped
            # the sign line for every measure (DAT-853 forensics).
            min_v, max_v = numeric.get("min_value"), numeric.get("max_value")
            sign = ""
            if isinstance(min_v, (int, float)):
                sign = (
                    " — negative values present" if min_v < 0 else " — no negative values observed"
                )
            measure_lines.append(f"    - {column.column_name}: min={min_v} max={max_v}{sign}")
        if measure_lines:
            lines.append("  measures (typed-profile range):")
            lines.extend(measure_lines)
    return "\n".join(lines)


def _evidence_metrics(evidence: dict[str, Any]) -> str:
    """The stored measured stats of one confirmed relationship, one bracket.

    Renders only what was measured (absence stays visible as absence), in the
    same vocabulary the candidate lines taught the judge — the numbers the
    stored direction was argued from (DAT-824 kept them for exactly this)."""
    parts: list[str] = []
    l_ri, r_ri = (
        evidence.get("left_referential_integrity"),
        evidence.get("right_referential_integrity"),
    )
    if l_ri is not None and r_ri is not None:
        parts.append(f"rows resolving: L={l_ri:.0f}% R={r_ri:.0f}%")
    l_cov, r_cov = evidence.get("left_key_coverage"), evidence.get("right_key_coverage")
    if l_cov is not None and r_cov is not None:
        parts.append(f"values covered: L={l_cov:.0f}% R={r_cov:.0f}%")
    l_orph, r_orph = evidence.get("left_orphan_count"), evidence.get("right_orphan_count")
    if l_orph is not None and r_orph is not None:
        parts.append(f"unresolved rows: L={l_orph} R={r_orph}")
    if evidence.get("introduces_duplicates") is not None:
        parts.append(f"fan trap: {evidence['introduces_duplicates']}")
    return f" [{', '.join(parts)}]" if parts else ""


def _qident(name: str) -> str:
    """Double-quote one DuckDB identifier (embedded quotes doubled)."""
    return '"' + name.replace('"', '""') + '"'


def _conditioned_top_values(
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    from_path: str,
    to_path: str,
    fk: str,
    key: str,
    label_column: str,
    limit: int,
) -> str | None:
    """One DuckDB aggregate: a label's top values over rows riding the join.

    The restriction is a semi-join (the fk RESOLVES into the key), not just
    ``fk IS NOT NULL``: an orphaned fk claims a link that never resolves, and
    those rows are already visible on the relationship line as unresolved
    counts — evidence labeled "joined rows" must describe the population that
    actually joins, or a 20%-orphan chain (the corpus's ORPHAN- invoice ids)
    would blend the orphan labels into it.

    Percentage denominator = ALL joined rows (NULL labels included), the same
    convention as the stored profile's flat ``top_values``; ordering is count
    DESC then value, keeping the render deterministic. Returns the rendered
    sample string, or None on empty/failed (fail-soft: a missing typed table
    must never sink the prompt build).
    """
    query = f"""
        SELECT value, cnt, pct FROM (
            SELECT CAST({_qident(label_column)} AS VARCHAR) AS value,
                   COUNT(*) AS cnt,
                   COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
            FROM {_qident(from_path)} src
            WHERE src.{_qident(fk)} IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM {_qident(to_path)} tgt
                  WHERE tgt.{_qident(key)} = src.{_qident(fk)}
              )
            GROUP BY 1
        )
        WHERE value IS NOT NULL
        ORDER BY cnt DESC, value
        LIMIT {int(limit)}
    """
    try:
        rows = duckdb_conn.execute(query).fetchall()
    except Exception as e:
        logger.warning(
            "conditioned_samples_failed", table=from_path, column=label_column, error=str(e)
        )
        return None
    if not rows:
        return None
    return ", ".join(f"'{_truncate(value)}' ({pct:.0f}%)" for value, _cnt, pct in rows)


def _sign_summary(min_v: float, max_v: float) -> str:
    """Plain-language sign statement of a served ``[min, max]`` range."""
    if max_v < 0:
        return "all negative"
    if min_v > 0:
        return "all positive"
    if min_v >= 0:
        return "none negative"
    if max_v <= 0:
        return "none positive"
    return "mixed signs"


def _conditioned_measure_range(
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    from_path: str,
    to_path: str,
    fk: str,
    key: str,
    measure_column: str,
) -> tuple[float, float] | None:
    """One DuckDB aggregate: a measure's min/max over rows riding the join.

    The same semi-join restriction as :func:`_conditioned_top_values` (the fk
    RESOLVES into the key — orphans and NULL fks do not ride), because the
    flow sign a chain carries is a property of the joined population: a
    measure globally mixed-sign can be uniformly one sign on the chain-linked
    rows, and that conditioned sign is the direction evidence. NULLs are
    ignored by MIN/MAX; an empty or all-NULL joined population serves nothing.
    Fail-soft: a missing typed table or a non-numeric result logs/returns
    None — the prompt build must survive it.
    """
    query = f"""
        SELECT MIN({_qident(measure_column)}), MAX({_qident(measure_column)})
        FROM {_qident(from_path)} src
        WHERE src.{_qident(fk)} IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM {_qident(to_path)} tgt
              WHERE tgt.{_qident(key)} = src.{_qident(fk)}
          )
    """
    try:
        row = duckdb_conn.execute(query).fetchone()
    except Exception as e:
        logger.warning(
            "conditioned_range_failed", table=from_path, column=measure_column, error=str(e)
        )
        return None
    if row is None:
        return None
    try:
        return float(row[0]), float(row[1])
    except TypeError, ValueError:
        # All-NULL joined population (MIN/MAX → None) or a non-numeric column.
        return None


def _conditioned_evidence(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    relationships: list[Relationship],
    sampler: DataSampler,
    *,
    run_id: str,
    endpoint_ids: set[str],
    measure_ids: set[str],
) -> dict[str, list[str]]:
    """Chain-conditioned evidence per confirmed reference relationship.

    For each ``A.fk -> B.key`` reference, two evidence classes restricted to
    the rows that ride the join (DAT-853):

    - **label samples** — the top values of A's label-bearing columns (this
      run's TableEntity identity columns, existence-filtered). This is THE
      discriminating evidence the flat samples blur: a label column shared by
      several populations looks mixed over all rows and unanimous on the
      chain-linked rows.
    - **measure ranges** — min/max + plain sign statement for A's measure
      columns (``measure_ids``, the annotation-derived selection the
      structural section already uses). The flow sign of the joined rows is
      direction evidence the flat table-level range blurs the same way.

    Join-key columns (any served relationship endpoint) are excluded — their
    values are the information-free IDs the flat endpoint samples already
    carry. A sensitive column name renders ``<REDACTED>`` without touching
    the data (the ``_sample_line`` convention: existence visible, values
    never leave). Conformed_dimension rows get nothing: no fk rides a
    shared-axis meeting. Fail-soft per relationship: no typed duckdb_path
    serves no conditioned lines; a table with no TableEntity at this run
    serves no LABEL lines (the measure selection rides the pinned
    annotations, not the entity).
    """
    references = [rel for rel in relationships if rel.relationship_type != "conformed_dimension"]
    if not references:
        return {}
    from_ids = list({rel.from_table_id for rel in references})
    entities: dict[str, TableEntity] = {
        e.table_id: e
        for e in session.execute(
            select(TableEntity).where(
                TableEntity.table_id.in_(from_ids), TableEntity.run_id == run_id
            )
        ).scalars()
    }
    real_columns: dict[str, dict[str, Column]] = {}
    for column in session.execute(select(Column).where(Column.table_id.in_(from_ids))).scalars():
        real_columns.setdefault(column.table_id, {})[column.column_name] = column

    out: dict[str, list[str]] = {}
    for rel in references:
        entity = entities.get(rel.from_table_id)
        from_table, to_table = rel.from_column.table, rel.to_column.table
        if not from_table.duckdb_path or not to_table.duckdb_path:
            continue
        columns = real_columns.get(rel.from_table_id, {})
        lines: list[str] = []
        identity_columns = (entity.identity_columns or []) if entity is not None else []
        for ic in identity_columns:
            if not isinstance(ic, dict):
                continue
            name = ic.get("column")
            if not isinstance(name, str):
                continue
            label_col = columns.get(name)
            if label_col is None or label_col.column_id in endpoint_ids:
                continue
            header = f"{name} ({rel.from_column.column_name}-joined rows)"
            if sampler.is_sensitive(name):
                lines.append(f"{header}: <REDACTED>")
                continue
            samples = _conditioned_top_values(
                duckdb_conn,
                from_path=from_table.duckdb_path,
                to_path=to_table.duckdb_path,
                fk=rel.from_column.column_name,
                key=rel.to_column.column_name,
                label_column=name,
                limit=sampler.config.max_sample_values,
            )
            if samples:
                lines.append(f"{header}: {samples}")
        # One aggregate per relationship × measure column, in column order.
        measure_columns = sorted(
            (
                c
                for c in columns.values()
                if c.column_id in measure_ids and c.column_id not in endpoint_ids
            ),
            key=lambda c: c.column_position,
        )
        for measure_col in measure_columns:
            header = f"{measure_col.column_name} ({rel.from_column.column_name}-joined rows)"
            if sampler.is_sensitive(measure_col.column_name):
                lines.append(f"{header}: <REDACTED>")
                continue
            value_range = _conditioned_measure_range(
                duckdb_conn,
                from_path=from_table.duckdb_path,
                to_path=to_table.duckdb_path,
                fk=rel.from_column.column_name,
                key=rel.to_column.column_name,
                measure_column=measure_col.column_name,
            )
            if value_range is None:
                continue
            min_v, max_v = value_range
            lines.append(f"{header}: min={min_v} max={max_v} — {_sign_summary(min_v, max_v)}")
        if lines:
            out[rel.relationship_id] = lines
    return out


def _format_relationships(
    relationships: list[Relationship],
    profiles: dict[str, StatisticalProfile],
    sampler: DataSampler,
    conditioned: dict[str, list[str]],
) -> str:
    """The confirmed relationship catalogue WITH evidence and endpoint samples.

    THE load-bearing section (DAT-823): the endpoint value samples ride the
    confirmed chain, so what a reference actually points at (the counterparty
    names behind a shared dimension key) is on the same line as the reference.
    ``conditioned`` (DAT-853) adds the chain-conditioned evidence lines (label
    samples + measure sign/ranges) per relationship_id — extra evidence next
    to, never instead of, the flat endpoint samples. ``relationship_type`` is
    served honestly (DAT-850): a
    ``conformed_dimension`` row is two facts meeting at a shared axis, named
    as such — never dressed as a genuine reference.
    """
    if not relationships:
        return "No confirmed relationships in this catalogue."
    lines: list[str] = []
    for rel in relationships:
        from_col, to_col = rel.from_column, rel.to_column
        from_name = f"{from_col.table.table_name}.{from_col.column_name}"
        to_name = f"{to_col.table.table_name}.{to_col.column_name}"
        kind = rel.relationship_type
        if kind == "conformed_dimension":
            kind = "conformed_dimension — two facts meeting at a shared axis, NOT a reference"
        card = f", {rel.cardinality}" if rel.cardinality else ""
        conf = f", confidence={rel.confidence:.2f}" if rel.confidence is not None else ""
        source = f", {rel.detection_method}"
        evidence = rel.evidence or {}
        lines.append(f"- {from_name} -> {to_name} ({kind}{card}{conf}{source})")
        metrics = _evidence_metrics(evidence)
        if metrics:
            lines.append(f"  measured:{metrics}")
        reasoning = evidence.get("reasoning")
        if reasoning:
            lines.append(f"  reasoning: {_truncate(reasoning, 400)}")
        for label, col in (("from", from_col), ("to", to_col)):
            sample = _sample_line(sampler, col.column_name, profiles.get(col.column_id))
            if sample:
                lines.append(f"  {label} values ({col.column_name}): {sample}")
        # Chain-conditioned evidence (DAT-853): label samples + measure
        # sign/ranges — additional, the flat endpoint samples above stay served.
        for conditioned_line in conditioned.get(rel.relationship_id, []):
            lines.append(f"  {conditioned_line}")
    return "\n".join(lines)


def _format_enriched_views(
    views: list[EnrichedView],
    table_names: dict[str, str],
    rel_by_id: dict[str, Relationship],
) -> str:
    """The composed fact×dimension views: view, fact, dimensions, join pairs."""
    if not views:
        return "No enriched views were composed for this catalogue."
    lines: list[str] = []
    for view in views:
        fact = table_names.get(view.fact_table_id, view.fact_table_id)
        dims = [table_names.get(tid, tid) for tid in view.dimension_table_ids or []]
        lines.append(f"- {view.view_name}: fact={fact}, dimensions=[{', '.join(dims)}]")
        for rel_id in view.relationship_ids or []:
            rel = rel_by_id.get(rel_id)
            if rel is None:
                continue
            lines.append(
                f"  joins {rel.from_column.table.table_name}.{rel.from_column.column_name}"
                f" -> {rel.to_column.table.table_name}.{rel.to_column.column_name}"
            )
    return "\n".join(lines)


def _format_shared_axes(
    slices: list[SliceDefinition],
    table_names: dict[str, str],
    *,
    scope: set[str],
) -> str:
    """The resolved slice axes + the deterministic shared-axis pairing, as facts.

    Same pairing the ``og_conformed_dimension`` element view derives — two
    DIFFERENT tables slicing by the SAME resolved ``(dimension_table_id,
    dimension_attribute)`` identity (folded slices, NULL dimension, excluded).
    Served as measured facts, deliberately NOT waiting for the
    dimension_hierarchies conform judge, which runs after this phase.

    ``slices`` is the WHOLE session's inventory: a pair needs aggregation
    across rows, so a scope pre-filter would silently drop the out-of-scope
    partner a scoped retry still needs to see. Scope filters only the
    RENDERING — per-fact axis lines for in-scope facts, pairing lines when any
    member touches the scope. Every ordering is keyed on resolved NAMES, never
    ids: a uuid sort key reshuffles identical catalogues between runs, the
    exact instability class DAT-725 fixed in the candidate serving.
    """
    dim_slices = [s for s in slices if s.dimension_table_id is not None]

    def _fact(s: SliceDefinition) -> str:
        return table_names.get(s.table_id, s.table_id)

    axis_lines: list[str] = []
    by_axis: dict[tuple[str, str], list[SliceDefinition]] = {}
    for s in sorted(dim_slices, key=lambda s: (_fact(s), s.column_name)):
        key = (s.dimension_table_id or "", s.dimension_attribute or "")
        by_axis.setdefault(key, []).append(s)
        if s.table_id not in scope and s.dimension_table_id not in scope:
            continue
        dim = table_names.get(s.dimension_table_id or "", s.dimension_table_id or "")
        attr = f".{s.dimension_attribute}" if s.dimension_attribute else " (by its key)"
        via = f" via {s.fk_role}" if s.fk_role else ""
        axis_lines.append(f"- {_fact(s)} slices by {dim}{attr}{via}")

    if not axis_lines and not dim_slices:
        return "No dimension-resolved slice axes in this catalogue."

    pair_lines: list[str] = []
    for (dim_table_id, attr), members in sorted(
        by_axis.items(), key=lambda kv: (table_names.get(kv[0][0], kv[0][0]), kv[0][1])
    ):
        facts = sorted({(_fact(s), s.fk_role or s.column_name) for s in members})
        if len({name for name, _ in facts}) < 2:
            continue
        if not any(s.table_id in scope or s.dimension_table_id in scope for s in members):
            continue
        dim = table_names.get(dim_table_id, dim_table_id)
        axis = f"{dim}.{attr}" if attr else f"{dim} (key)"
        member_strs = [f"{name} (via {role})" for name, role in facts]
        pair_lines.append(f"- {axis}: {' <-> '.join(member_strs)}")

    lines: list[str] = []
    if axis_lines:
        lines.append("Resolved dimension axes:")
        lines.extend(axis_lines)
    if pair_lines:
        if lines:
            lines.append("")
        lines.append("Shared axes (facts aligned on the same dimension attribute):")
        lines.extend(pair_lines)
    return "\n".join(lines) if lines else "No dimension-resolved slice axes in this catalogue."


def build_catalogue_inputs(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    table_ids: list[str],
    session_table_ids: list[str],
    run_id: str,
    sampler: DataSampler,
) -> dict[str, str]:
    """Assemble the catalogue prompt's evidence inputs (sections 1-7).

    ``table_ids`` is the AUTHORING scope (the coverage retry narrows it);
    ``session_table_ids`` is the whole session selection — cross-table evidence
    (relationships, views, axes) always loads over the full session and is
    filtered to lines touching the scope, so a scoped retry still sees the
    chains its tables ride. ``duckdb_conn`` serves the chain-conditioned label
    aggregates on the relationship lines (DAT-853) — every other read stays on
    the stored metadata. The ontology steer (section 8) is added by the agent,
    mirroring ``ground_columns``.
    """
    tables = list(
        session.execute(
            select(Table).where(Table.table_id.in_(table_ids)).order_by(Table.table_name)
        ).scalars()
    )
    columns_by_table: dict[str, list[Column]] = {}
    for column in session.execute(
        select(Column)
        .where(Column.table_id.in_(table_ids))
        .order_by(Column.table_id, Column.column_position)
    ).scalars():
        columns_by_table.setdefault(column.table_id, []).append(column)

    heads = _generation_heads(session, table_ids)
    if any(head is None for head in heads.values()):
        # Fail-closed serving, born loud: the affected tables render without
        # samples/ranges/annotations rather than borrowing an arbitrary run's.
        logger.warning(
            "catalogue_context_missing_generation_head",
            tables=[tid for tid, head in heads.items() if head is None],
        )
    profiles = _load_profiles(session, columns_by_table, heads)
    annotation_rows = _load_annotation_rows(session, table_ids, heads)

    scope = set(table_ids)
    relationships = [
        rel
        for rel in load_defined_relationships(
            session,
            session_table_ids,
            run_id=run_id,
            both_tables=True,
            eager_columns=True,
        )
        if rel.from_table_id in scope or rel.to_table_id in scope
    ]
    # Endpoint profiles may live outside the authoring scope (the other side of
    # a chain a scoped retry serves) — load those too, pinned the same way.
    endpoint_columns: dict[str, list[Column]] = {}
    for rel in relationships:
        for col in (rel.from_column, rel.to_column):
            if col.column_id not in profiles:
                endpoint_columns.setdefault(col.table_id, []).append(col)
    if endpoint_columns:
        profiles.update(
            _load_profiles(
                session, endpoint_columns, _generation_heads(session, list(endpoint_columns))
            )
        )
    endpoint_ids = {c.column_id for rel in relationships for c in (rel.from_column, rel.to_column)}
    # The measure selection the structural section renders sign/range for —
    # reused so the conditioned serve and the flat serve agree on what a
    # measure IS (the pinned annotations' semantic_role).
    measure_ids = {
        row["column_id"] for row in annotation_rows if row.get("semantic_role") == "measure"
    }
    conditioned = _conditioned_evidence(
        session,
        duckdb_conn,
        relationships,
        sampler,
        run_id=run_id,
        endpoint_ids=endpoint_ids,
        measure_ids=measure_ids,
    )

    all_table_names: dict[str, str] = dict(
        session.execute(
            select(Table.table_id, Table.table_name).where(
                Table.table_id.in_(list(set(session_table_ids) | scope))
            )
        )
        .tuples()
        .all()
    )
    # Dimension tables referenced by slice axes may sit outside the session
    # selection's Table rows loaded above only if never selected — resolve the
    # remainder defensively so an id never leaks into the prompt.
    views = [
        v
        for v in session.execute(
            select(EnrichedView).where(
                EnrichedView.fact_table_id.in_(session_table_ids),
                EnrichedView.run_id == run_id,
            )
        ).scalars()
        if v.fact_table_id in scope or scope & set(v.dimension_table_ids or [])
    ]
    # The WHOLE session's slice inventory — a shared-axis pair needs rows from
    # BOTH facts, so a scope pre-filter here would drop the out-of-scope
    # partner a scoped retry still needs; _format_shared_axes filters the
    # RENDERING by scope instead.
    slices = list(
        session.execute(
            select(SliceDefinition).where(
                SliceDefinition.table_id.in_(session_table_ids),
                SliceDefinition.run_id == run_id,
            )
        ).scalars()
    )
    missing_names = {
        tid
        for s in slices
        if s.dimension_table_id is not None and (tid := s.dimension_table_id) not in all_table_names
    }
    if missing_names:
        all_table_names.update(
            session.execute(
                select(Table.table_id, Table.table_name).where(
                    Table.table_id.in_(list(missing_names))
                )
            )
            .tuples()
            .all()
        )

    rel_by_id = {rel.relationship_id: rel for rel in relationships}
    return {
        "structural_tables": _format_structural_tables(
            session,
            tables,
            columns_by_table,
            annotation_rows,
            profiles,
            sampler,
            run_id=run_id,
            relationship_endpoint_ids=endpoint_ids,
        ),
        "column_annotations": _format_annotations(annotation_rows),
        "relationship_catalogue": _format_relationships(
            relationships, profiles, sampler, conditioned
        ),
        "enriched_views": _format_enriched_views(views, all_table_names, rel_by_id),
        "shared_axes": _format_shared_axes(slices, all_table_names, scope=scope),
    }


__all__ = ["build_catalogue_inputs"]
