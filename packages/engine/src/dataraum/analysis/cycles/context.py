"""Context builder for business cycle detection.

Assembles rich context from all available pipeline metadata:
slice definitions, statistical profiles, temporal profiles,
enriched views, semantic annotations, entity classifications,
and confirmed relationships.

The LLM receives pre-computed signals and synthesizes them
into business cycle analysis — no exploration tools needed.

In-run reader (ADR-0008, DAT-455): the builder never resolves snapshot heads
itself. Every run-versioned read (defined relationships, entity
classifications, slice definitions, per-column semantic annotations) is scoped
by the :class:`~dataraum.lifecycle.BaseRunMap` pinned once at run start by the
operating_model resolve activity and passed in. An absent pin reads EMPTY,
never cross-run (fail-closed, DAT-429).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.analysis.cycles.config import format_cycle_vocabulary_for_context
from dataraum.analysis.relationships.graph_topology import (
    analyze_graph_topology,
    format_graph_structure_for_context,
)
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity, TableRole
from dataraum.analysis.semantic.utils import load_column_concepts
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import CURATED_SLICE_BUDGET
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.temporal.db_models import TemporalColumnProfile
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.graphs.field_mapping import format_meanings_for_prompt, load_column_meanings
from dataraum.llm.privacy import DataSampler
from dataraum.storage import Column, Table

logger = get_logger(__name__)

# Arithmetic derivations are the numeric-completion signals a cycle can close on
# (a journal balances when a difference holds; a reconciliation when a ratio
# does). String transforms (concat/upper/lower/substr) carry no completion
# meaning — exclude them so the agent isn't served noise. The domain knowledge
# of WHICH balance means completion stays in the LLM + cycles.yaml, never here.
_ARITHMETIC_DERIVATIONS = frozenset({"sum", "difference", "product", "ratio"})

# Entity-flow sample budget: the stored typed profile carries up to the
# profiler's top_k inventory (hundreds of rows for a high-cardinality identity
# column — exactly the column class served here). Entity determination needs
# the VALUE PATTERN, not the inventory, and the cycles prompt is ONE cross-
# table call, so only the head of the frequency-ordered list is served, each
# value truncated like the semantic agents' samples.
_ENTITY_FLOW_SAMPLE_BUDGET = 10
_SAMPLE_VALUE_MAX_CHARS = 100

if TYPE_CHECKING:
    import duckdb

    from dataraum.lifecycle import BaseRunMap
    from dataraum.llm.config import LLMPrivacy


def build_cycle_detection_context(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    *,
    vertical: str,
    base_runs: BaseRunMap,
    privacy: LLMPrivacy | None = None,
) -> dict[str, Any]:
    """Build context for the business cycle detection agent.

    Loads all available pipeline metadata and formats it for the LLM.
    The context is rich enough for single-call cycle detection without
    exploration tools.

    Args:
        session: SQLAlchemy session
        duckdb_conn: DuckDB connection for row counts and the
            chain-conditioned label + measure-range aggregates on the
            reference lines (DAT-853)
        table_ids: Tables to analyze
        vertical: Vertical name (e.g. 'finance')
        base_runs: the run's pinned upstream heads (ADR-0008 in-run mode).
            ``relationship_run_id`` scopes the defined relationships, entity
            classifications, and slice definitions; ``semantic_runs`` scopes
            each table's per-column annotations AND its typed-profile reads
            (slice value counts, entity-flow value samples). An absent pin
            reads EMPTY — fail-closed (DAT-429), never a cross-run read.
        privacy: LLM privacy config; when provided, entity-flow value samples
            respect its sensitive-name patterns (a sensitive column serves no
            samples). ``None`` serves samples ungated (tests).

    Returns:
        Context dictionary with all pipeline metadata for cycle detection.
    """
    context: dict[str, Any] = {}

    # 1. Tables + columns. Semantic annotations are NOT loaded through the ORM
    # ``Column.semantic_annotation`` one-to-one — under multi-run coexistence
    # that silently picks one of N coexisting rows. The explicit run-pinned
    # query below (mirrors the validation resolver) replaces it.
    tables_stmt = (
        select(Table).where(Table.table_id.in_(table_ids)).options(selectinload(Table.columns))
    )
    tables = session.execute(tables_stmt).scalars().all()

    # Build lookup maps
    table_by_id = {t.table_id: t for t in tables}
    column_by_id: dict[str, Column] = {}
    for t in tables:
        for c in t.columns:
            column_by_id[c.column_id] = c

    annotations = _load_pinned_annotations(session, tables, base_runs.semantic_runs)
    # Catalogue-grain concepts (DAT-637) live under the catalogue head, scoped by
    # the same pinned begin_session run the relationship reads use.
    concepts = (
        load_column_concepts(session, [t.table_id for t in tables], base_runs.relationship_run_id)
        if base_runs.relationship_run_id
        else {}
    )

    # The pinned begin_session run (ADR-0008 in-run mode): the run-versioned
    # reads below — entity classifications, the defined relationships, AND slice
    # definitions (run-versioned since DAT-448: table-scoped + immortal was the
    # cross-session leak) — scope to the SAME run. **Fail-closed (DAT-429):**
    # with no pinned run we MUST NOT fall back to a cross-run read — that would
    # mix OTHER runs' rows into this context. Leave them empty instead.
    run_id = base_runs.relationship_run_id
    if run_id is None:
        logger.warning(
            "session_run_unresolved",
            detail="no pinned begin_session run; entity/relationship context is empty",
        )

    # 2. Entity classifications (fact vs dimension) — run-scoped (fail-closed above).
    # Loaded before the per-column pass below because their identity_columns feed
    # the entity-flow sample gate.
    entities: list[Any] = []
    if run_id is not None:
        entities = list(
            session.execute(
                select(TableEntity, Table.table_name)
                .join(Table, TableEntity.table_id == Table.table_id)
                .where(Table.table_id.in_(table_ids), TableEntity.run_id == run_id)
            ).all()
        )

    # 3. The defined relationships (not candidate) within the selection — run-scoped,
    # fail-closed above (empty when the session's run is unresolved). Split by the
    # edge-kind owner (DAT-850, the row's relationship_type): REFERENCE kinds
    # (foreign_key/hierarchy) are the edges entity flows ride on and feed the
    # graph topology below; a 'conformed_dimension' row is two facts meeting at a
    # shared axis — no entity flows through it — served to the LLM in its own
    # explicitly-labelled block (loud absence-from-references, never a silent drop).
    relationships = (
        load_defined_relationships(session, table_ids, run_id=run_id) if run_id is not None else []
    )

    rel_list: list[dict[str, Any]] = []
    conformed_list: list[dict[str, Any]] = []
    for rel in relationships:
        from_col = column_by_id.get(rel.from_column_id)
        to_col = column_by_id.get(rel.to_column_id)
        from_table = table_by_id.get(rel.from_table_id)
        to_table = table_by_id.get(rel.to_table_id)

        if from_col and to_col and from_table and to_table:
            entry = {
                "from_table_id": rel.from_table_id,
                "from_table": from_table.table_name,
                "from_column": from_col.column_name,
                "to_table_id": rel.to_table_id,
                "to_table": to_table.table_name,
                "to_column": to_col.column_name,
                "relationship_type": rel.relationship_type,
                "cardinality": rel.cardinality,
                "confidence": rel.confidence,
            }
            if rel.relationship_type == "conformed_dimension":
                conformed_list.append(entry)
            else:
                rel_list.append(entry)

    # Entity-flow candidate columns: the columns cycles' entity flows ride on —
    # confirmed-relationship endpoints plus each table's recurring identity
    # columns (semantic_per_table's "would-be foreign keys", DAT-565). These get
    # VALUE SAMPLES served below: when a column's NAME communicates nothing (an
    # obscured/renamed schema) the values are the remaining evidence for WHICH
    # entity a flow involves, so the judging LLM must see them instead of
    # inheriting a name-starved annotation's hedge. Structural gate only —
    # derived from served metadata, never from name patterns or value shapes.
    columns_by_table: dict[str, set[str]] = {
        t.table_name: {c.column_name for c in t.columns} for t in tables
    }
    served_identity: dict[str, list[dict[str, Any]]] = {
        ent_table_name: _served_identity_columns(
            ent.identity_columns, columns_by_table.get(ent_table_name, set())
        )
        for ent, ent_table_name in entities
    }
    entity_flow_columns: set[tuple[str, str]] = set()
    for r in rel_list:
        entity_flow_columns.add((r["from_table"], r["from_column"]))
        entity_flow_columns.add((r["to_table"], r["to_column"]))
    # Join keys — the endpoints alone: excluded from the chain-conditioned
    # labels below (their values are the information-free IDs the flat
    # endpoint samples already carry).
    endpoint_columns = set(entity_flow_columns)
    for ent_table_name, identity_cols in served_identity.items():
        for ic in identity_cols:
            entity_flow_columns.add((ent_table_name, ic["column"]))

    # Row counts from DuckDB
    row_counts: dict[str, int | None] = {}
    for t in tables:
        try:
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{t.duckdb_path}"').fetchone()
            row_counts[t.table_name] = result[0] if result else None
        except Exception:
            logger.warning("row_count_failed", table=t.table_name, duckdb_path=t.duckdb_path)
            row_counts[t.table_name] = None

    sampler = DataSampler(privacy) if privacy is not None else None

    # Chain-conditioned evidence on the reference lines (DAT-853): for each
    # A.fk -> B.key, aggregated over ONLY the rows that ride the join —
    # (a) the from-side identity labels' top values, and (b) the from-side
    # measure columns' min/max with a plain sign statement. The flat samples
    # and profiles served per column below blur populations sharing a table
    # (counterparty at 37% vendors flat vs 100% vendors on payment-linked
    # rows — the AP mislabel; a measure globally mixed-sign yet uniformly one
    # sign on the joined rows); the conditioned distribution and sign are the
    # direction evidence. Additional serving — the flat samples stay. Privacy
    # follows this builder's convention: a sensitive column serves NOTHING
    # (absence, never a placeholder).
    for r in rel_list:
        from_tbl = table_by_id.get(r["from_table_id"])
        to_tbl = table_by_id.get(r["to_table_id"])
        if from_tbl is None or to_tbl is None:
            continue
        if not from_tbl.duckdb_path or not to_tbl.duckdb_path:
            continue
        conditioned: list[dict[str, Any]] = []
        for ic in served_identity.get(r["from_table"], []):
            label_name = ic["column"]
            if (r["from_table"], label_name) in endpoint_columns:
                continue
            if sampler is not None and sampler.is_sensitive(label_name):
                continue
            top = _conditioned_top_values(
                duckdb_conn,
                from_path=from_tbl.duckdb_path,
                to_path=to_tbl.duckdb_path,
                fk=r["from_column"],
                key=r["to_column"],
                label_column=label_name,
                limit=_ENTITY_FLOW_SAMPLE_BUDGET,
            )
            if top:
                conditioned.append(
                    {
                        "column": label_name,
                        "samples": [
                            f"{_truncate_sample_value(value)} ({pct:.0f}%)" for value, pct in top
                        ],
                    }
                )
        if conditioned:
            r["conditioned_label_samples"] = conditioned
        # Measure sign/range over the same joined population — one aggregate
        # per relationship × measure column, the measure selection being this
        # builder's pinned annotations (semantic_role == "measure").
        ranges: list[dict[str, Any]] = []
        for measure_col in sorted(from_tbl.columns, key=lambda c: c.column_position):
            ann = annotations.get(measure_col.column_id)
            if ann is None or ann.semantic_role != "measure":
                continue
            if (r["from_table"], measure_col.column_name) in endpoint_columns:
                continue
            if sampler is not None and sampler.is_sensitive(measure_col.column_name):
                continue
            value_range = _conditioned_measure_range(
                duckdb_conn,
                from_path=from_tbl.duckdb_path,
                to_path=to_tbl.duckdb_path,
                fk=r["from_column"],
                key=r["to_column"],
                measure_column=measure_col.column_name,
            )
            if value_range is None:
                continue
            min_v, max_v = value_range
            ranges.append(
                {
                    "column": measure_col.column_name,
                    "min": min_v,
                    "max": max_v,
                    "summary": _sign_summary(min_v, max_v),
                }
            )
        if ranges:
            r["conditioned_measure_ranges"] = ranges

    # Build table info with columns and semantic annotations
    table_info = []
    for t in tables:
        columns = []
        for c in t.columns:
            col_info: dict[str, Any] = {
                "name": c.column_name,
                "type": c.resolved_type or c.raw_type,
            }
            ann = annotations.get(c.column_id)
            if ann is not None:
                col_info["semantic_role"] = ann.semantic_role
                col_info["entity_type"] = ann.entity_type
                col_info["business_name"] = ann.business_name
                col_info["business_description"] = ann.business_description
                # The annotator's confidence contract: this number encodes how
                # much the column NAME communicates, not how certain the
                # annotation is. Served so a low value reads as "unreadable
                # name — weigh the samples", never silently dropped.
                if ann.confidence is not None:
                    col_info["annotation_confidence"] = ann.confidence
            concept = concepts.get(c.column_id)
            if concept is not None:
                col_info["meaning"] = concept.meaning
                col_info["temporal_behavior"] = concept.temporal_behavior
            # Value samples for entity-flow candidates (gate above) — read at the
            # table's pinned generation head, the same run-scoped profile read
            # the slice value counts use (fail-closed on a missing pin). A
            # privacy-sensitive name serves NOTHING: a redaction placeholder
            # carries no entity evidence, so absence is the honest serving.
            if (t.table_name, c.column_name) in entity_flow_columns and not (
                sampler is not None and sampler.is_sensitive(c.column_name)
            ):
                value_counts = _get_value_counts_for_column(
                    session, c.column_id, run_id=base_runs.semantic_runs.get(t.table_id)
                )
                samples = [
                    _truncate_sample_value(vc["value"])
                    for vc in value_counts
                    if vc.get("value") is not None
                ][:_ENTITY_FLOW_SAMPLE_BUDGET]
                if samples:
                    col_info["sample_values"] = samples
            columns.append(col_info)

        table_info.append(
            {
                "table_id": t.table_id,
                "table_name": t.table_name,
                "row_count": row_counts.get(t.table_name),
                "columns": columns,
            }
        )

    context["tables"] = table_info

    context["entity_classifications"] = [
        {
            "table_name": table_name,
            "entity_type": ent.detected_entity_type,
            "description": ent.description,
            "table_role": ent.table_role,
            # A bare list of column names (DAT-775) — ``format_context_for_prompt``
            # below joins this directly into the prompt's "grain: ..." text.
            "grain_columns": ent.grain_columns,
            # Recurring identity columns (DAT-565) with their authored notes —
            # the columns entity flows ride on; rendered so the agent sees WHY
            # a column carries samples. Existence-filtered once above
            # (``served_identity``), shared with the entity-flow gate and the
            # chain-conditioned label serve.
            "identity_columns": served_identity.get(table_name, []),
        }
        for ent, table_name in entities
    ]

    context["relationships"] = rel_list
    context["conformed_meetings"] = conformed_list

    # 4. Graph topology
    table_name_map = {t.table_id: t.table_name for t in tables}
    graph_structure = analyze_graph_topology(
        table_ids=table_ids,
        relationships=rel_list,
        table_names=table_name_map,
    )
    context["graph_topology"] = graph_structure

    # 5. Slice definitions (pre-identified categorical dimensions = status columns)
    # Run-versioned (DAT-448): scope to the pinned begin_session run; fail-closed
    # like entities/relationships when the session has no pinned run (empty,
    # never a cross-run read). CURATED read (DAT-725): the catalog is the full
    # deterministic inventory now, so LLM-facing context takes the top-priority
    # budget (1 = most interesting; column_name tiebreak keeps the cut
    # deterministic across floor-priority structural rows).
    slices: list[SliceDefinition] = []
    if run_id is not None:
        slice_stmt = (
            select(SliceDefinition)
            .where(
                SliceDefinition.table_id.in_(table_ids),
                SliceDefinition.run_id == run_id,
            )
            .options(selectinload(SliceDefinition.table), selectinload(SliceDefinition.column))
            .order_by(SliceDefinition.slice_priority, SliceDefinition.column_name)
            .limit(CURATED_SLICE_BUDGET)
        )
        slices = list(session.execute(slice_stmt).scalars().all())

    slice_list = []
    for sd in slices:
        # Value counts from the statistical profile, scoped to the table's
        # add_source generation head (``semantic_runs``) — the same per-table pin
        # the annotations use, and the run the typed profile was written under.
        # The verify floor (DAT-630) builds its membership set from these values,
        # so an unscoped read would leak a stale run's values; fail-closed to []
        # when the table has no pinned generation run.
        value_counts = _get_value_counts_for_column(
            session, sd.column_id, run_id=base_runs.semantic_runs.get(sd.table_id)
        )

        slice_list.append(
            {
                "table_name": sd.table.table_name,
                "column_name": sd.column.column_name,
                "slice_type": sd.slice_type,
                "values": sd.distinct_values or [],
                "value_counts": value_counts,
                "confidence": sd.confidence,
                "business_context": sd.business_context,
                "priority": sd.slice_priority,
            }
        )

    context["slice_definitions"] = slice_list

    # 5b. Derived (numeric) relationships — the completion signal a status column
    # can't carry. The correlations phase already detected which arithmetic
    # relationships hold and how often (``match_rate``); a cycle that closes on a
    # balance/ratio (a GL journal, a reconciliation) grounds HERE, not on a status
    # value. Run-scoped + fail-closed like every other run-versioned read above.
    derived_list: list[dict[str, Any]] = []
    if run_id is not None:
        derived_rows = list(
            session.execute(
                select(DerivedColumn).where(
                    DerivedColumn.table_id.in_(table_ids),
                    DerivedColumn.run_id == run_id,
                    DerivedColumn.derivation_type.in_(_ARITHMETIC_DERIVATIONS),
                )
            )
            .scalars()
            .all()
        )
        for dc in derived_rows:
            derived_col = column_by_id.get(dc.derived_column_id)
            table = table_by_id.get(dc.table_id)
            source_cols = [
                column_by_id[cid].column_name for cid in dc.source_column_ids if cid in column_by_id
            ]
            # A derivation whose columns we can't resolve in-scope is unusable —
            # never serve a half-named relationship the agent could mis-ground on.
            if (
                derived_col is None
                or table is None
                or len(source_cols) != len(dc.source_column_ids)
            ):
                continue
            derived_list.append(
                {
                    "table_name": table.table_name,
                    "derived_column": derived_col.column_name,
                    "source_columns": source_cols,
                    "derivation_type": dc.derivation_type,
                    "formula": dc.formula,
                    "match_rate": dc.match_rate,
                }
            )

    context["derived_relationships"] = derived_list

    # 5c. The column meaning feed (DAT-769) — the SAME loader the metric graph
    # agent grounds with, so a cycle's completion concepts bind to real columns
    # instead of being improvised. Catalogue-grain, run-scoped.
    field_mappings = load_column_meanings(session, table_ids, catalogue_run_id=run_id)
    context["field_mappings"] = format_meanings_for_prompt(field_mappings)

    # 6. Temporal profiles
    temporal_stmt = (
        select(TemporalColumnProfile, Column.column_name, Table.table_name)
        .join(Column, TemporalColumnProfile.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
    )
    temporal_results = session.execute(temporal_stmt).all()

    context["temporal_profiles"] = [
        {
            "table_name": table_name,
            "column_name": col_name,
            "granularity": tp.detected_granularity,
            "date_range_start": str(tp.min_timestamp) if tp.min_timestamp else None,
            "date_range_end": str(tp.max_timestamp) if tp.max_timestamp else None,
            "completeness": tp.completeness_ratio,
            "is_stale": tp.is_stale,
        }
        for tp, col_name, table_name in temporal_results
    ]

    # 7. Enriched views (pre-joined table schemas)
    enriched_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
    enriched_views = session.execute(enriched_stmt).scalars().all()

    enriched_list = []
    for ev in enriched_views:
        fact_table = table_by_id.get(ev.fact_table_id)
        dim_tables = [
            table_by_id[tid].table_name
            for tid in (ev.dimension_table_ids or [])
            if tid in table_by_id
        ]
        enriched_list.append(
            {
                "view_name": ev.view_name,
                "fact_table": fact_table.table_name if fact_table else "unknown",
                "dimension_tables": dim_tables,
                "dimension_columns": ev.dimension_columns or [],
            }
        )

    context["enriched_views"] = enriched_list

    # 9. Summary statistics
    context["summary"] = {
        "total_tables": len(tables),
        "total_columns": sum(len(t.columns) for t in tables),
        "total_relationships": len(rel_list),
        "conformed_meetings_found": len(conformed_list),
        "slice_dimensions_found": len(slice_list),
        "derived_relationships_found": len(derived_list),
        "temporal_columns": len(context["temporal_profiles"]),
        "enriched_views": len(enriched_list),
        "fact_tables": sum(
            1
            for e in context["entity_classifications"]
            if e["table_role"] in (TableRole.FACT, TableRole.PERIODIC_SNAPSHOT)
        ),
        "dimension_tables": sum(
            1 for e in context["entity_classifications"] if e["table_role"] == TableRole.DIMENSION
        ),
        "graph_pattern": graph_structure.pattern,
    }

    # 10. Domain vocabulary
    vocabulary = format_cycle_vocabulary_for_context(vertical=vertical)
    context["domain_vocabulary"] = vocabulary

    return context


def _served_identity_columns(
    identity_columns: list[dict[str, Any]] | None,
    real_columns: set[str],
) -> list[dict[str, Any]]:
    """Filter LLM-authored identity columns to ones that physically exist.

    ``TableEntity.identity_columns`` is synthesis output persisted unvalidated —
    a hallucinated column name served verbatim would satisfy the prompt's
    cite-only-served contract while still being dropped by the membership floor
    (``verify.py``: "column not in workspace"), silently rejecting the whole
    cycle. Malformed entries (non-dict, missing ``column``) are dropped for the
    same reason: only names the workspace can ground are served.
    """
    return [
        ic
        for ic in identity_columns or []
        if isinstance(ic, dict) and ic.get("column") in real_columns
    ]


def _truncate_sample_value(value: Any) -> str:
    """Stringify + truncate one served sample value (mirrors the semantic agents)."""
    text = str(value)
    if len(text) > _SAMPLE_VALUE_MAX_CHARS:
        return text[:_SAMPLE_VALUE_MAX_CHARS] + "..."
    return text


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
) -> list[tuple[str, float]]:
    """One DuckDB aggregate: a label's top (value, pct) over rows riding the join.

    Twin of the catalogue builder's helper (analysis/catalogue/context.py) —
    same semantics, this builder's own rendering. The restriction is a
    semi-join (the fk RESOLVES into the key), not just ``fk IS NOT NULL``: an
    orphaned fk claims a link that never resolves, and evidence labeled
    "joined rows" must describe the population that actually joins.
    Percentage denominator = ALL joined rows (NULL labels included, the
    stored profile's top_values convention); ordering count DESC then value
    keeps the serve deterministic. Fail-soft: a missing typed table logs and
    serves nothing — the context build must survive it.
    """
    query = f"""
        SELECT value, pct FROM (
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
        return []
    return [(value, float(pct)) for value, pct in rows]


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

    Twin of the catalogue builder's helper (analysis/catalogue/context.py) —
    same semantics, this builder's own rendering. The same semi-join
    restriction as :func:`_conditioned_top_values` (the fk RESOLVES into the
    key — orphans and NULL fks do not ride): the flow sign a chain carries is
    a property of the joined population, and a measure globally mixed-sign
    can be uniformly one sign on the chain-linked rows. NULLs are ignored by
    MIN/MAX; an empty or all-NULL joined population serves nothing.
    Fail-soft: a missing typed table or a non-numeric result logs/returns
    None — the context build must survive it.
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


def _load_pinned_annotations(
    session: Session,
    tables: list[Table] | Any,
    semantic_runs: dict[str, str],
) -> dict[str, SemanticAnnotation]:
    """Load each table's semantic annotations at its PINNED run, keyed by column_id.

    Replaces the ``Column.semantic_annotation`` one-to-one ORM navigation,
    broken under multi-run coexistence (the table is run-versioned with a
    ``(column_id, run_id)`` UNIQUE — N runs leave N rows per column and the
    one-to-one silently picks one). A table with no pinned semantic run
    contributes nothing — fail-closed, never an arbitrary run's annotations.
    Mirrors the validation resolver's loader of the same name (DAT-455).
    """
    annotations: dict[str, SemanticAnnotation] = {}
    for table in tables:
        run_id = semantic_runs.get(table.table_id)
        if run_id is None:
            continue
        column_ids = [col.column_id for col in table.columns]
        if not column_ids:
            continue
        rows = (
            session.execute(
                select(SemanticAnnotation).where(
                    SemanticAnnotation.column_id.in_(column_ids),
                    SemanticAnnotation.run_id == run_id,
                )
            )
            .scalars()
            .all()
        )
        annotations.update({ann.column_id: ann for ann in rows})
    return annotations


def _get_value_counts_for_column(
    session: Session,
    column_id: str,
    *,
    run_id: str | None,
) -> list[dict[str, Any]]:
    """Get value counts from the typed statistical profile for a column.

    Run-scoped (DAT-413/630): the profile is read at ``run_id`` — the table's
    add_source generation head. ``None`` reads EMPTY (fail-closed), never an
    arbitrary coexisting run's profile, since the verify floor trusts these
    values as the workspace's value-set.

    Args:
        session: SQLAlchemy session.
        column_id: Column to look up.
        run_id: The table's pinned generation run; ``None`` ⇒ empty.

    Returns:
        List of {value, count, percentage} dicts, or empty list.
    """
    if run_id is None:
        return []
    profile_stmt = select(StatisticalProfile).where(
        StatisticalProfile.column_id == column_id,
        StatisticalProfile.run_id == run_id,
        StatisticalProfile.layer == "typed",
    )
    profile = session.execute(profile_stmt).scalars().first()

    if not profile or not profile.profile_data:
        return []

    top_values = profile.profile_data.get("top_values", [])
    return [
        {
            "value": tv.get("value", ""),
            "count": tv.get("count", 0),
            "percentage": round(tv.get("percentage", 0), 1),
        }
        for tv in top_values
    ]


def format_context_for_prompt(context: dict[str, Any]) -> str:
    """Format the context dictionary as a readable string for the LLM prompt.

    Organizes metadata into sections that support cycle detection:
    1. Domain vocabulary (reference framework)
    2. Dataset summary + table classifications
    3. Pre-identified categorical dimensions (= status-completion indicators)
    4. Derived numeric relationships (= numeric-completion signals)
    5. Semantic field mappings (concept → column)
    6. Enriched views (pre-joined tables)
    7. Relationships + graph topology
    8. Temporal patterns
    9. Column semantics by table

    Args:
        context: Context dictionary from build_cycle_detection_context

    Returns:
        Formatted string suitable for LLM prompt
    """
    lines: list[str] = []

    # Domain vocabulary first (provides reference framework)
    vocabulary = context.get("domain_vocabulary", "")
    if vocabulary:
        lines.append("# DOMAIN KNOWLEDGE")
        lines.append("")
        lines.append(vocabulary)
        lines.append("")
        lines.append("---")
        lines.append("")

    # Dataset summary
    lines.append("# DATASET CONTEXT")
    lines.append("")
    summary = context.get("summary", {})
    lines.append("## SUMMARY")
    lines.append(f"- Tables: {summary.get('total_tables', 0)}")
    lines.append(f"- Columns: {summary.get('total_columns', 0)}")
    lines.append(f"- Confirmed relationships: {summary.get('total_relationships', 0)}")
    lines.append(f"- Fact tables: {summary.get('fact_tables', 0)}")
    lines.append(f"- Dimension tables: {summary.get('dimension_tables', 0)}")
    lines.append(
        f"- Categorical dimensions (status/type columns): {summary.get('slice_dimensions_found', 0)}"
    )
    lines.append(
        f"- Derived numeric relationships: {summary.get('derived_relationships_found', 0)}"
    )
    lines.append(f"- Temporal columns: {summary.get('temporal_columns', 0)}")
    lines.append(f"- Graph pattern: {summary.get('graph_pattern', 'unknown')}")
    lines.append("")

    # Entity classifications
    lines.append("## TABLE CLASSIFICATIONS")
    for ent in context.get("entity_classifications", []):
        table_type = (ent.get("table_role") or "other").upper()
        table_info = context["tables"]
        row_count = next(
            (t["row_count"] for t in table_info if t["table_name"] == ent["table_name"]),
            None,
        )
        row_str = f", {row_count:,} rows" if row_count else ""
        grain = f", grain: {', '.join(ent['grain_columns'])}" if ent.get("grain_columns") else ""
        # Nullable since DAT-823 (the catalogue turn may honestly leave a table
        # unread after retries) — render declared ignorance, never the literal
        # string "None", matching the guarded siblings above.
        entity_type = ent.get("entity_type") or "(entity type undetermined)"
        lines.append(f"- {ent['table_name']} ({table_type}{row_str}{grain}): {entity_type}")
        if ent.get("description"):
            lines.append(f"  {ent['description'][:500]}")
        # Recurring identity columns (DAT-565): the entity-identifying columns
        # of this table, each with the synthesis agent's one-line note. The
        # builder pre-filters the list to real columns (_served_identity_columns);
        # the shape guard here only protects hand-built contexts (tests).
        identity_strs = [
            f"{ic['column']} ({ic['note']})" if ic.get("note") else str(ic["column"])
            for ic in ent.get("identity_columns") or []
            if isinstance(ic, dict) and ic.get("column")
        ]
        if identity_strs:
            lines.append(f"  identity columns: {'; '.join(identity_strs)}")
    lines.append("")

    # Pre-identified categorical dimensions (= cycle indicators)
    slice_defs = context.get("slice_definitions", [])
    if slice_defs:
        lines.append("## CATEGORICAL DIMENSIONS (Pre-Identified Cycle Indicators)")
        lines.append("")
        lines.append("These columns were identified by the semantic agent as key categorical")
        lines.append("dimensions. Status columns are strong cycle completion indicators.")
        lines.append("")
        for sd in slice_defs:
            # Structural inventory rows (DAT-725) carry no LLM confidence — the
            # header renders without one rather than formatting None.
            conf = sd.get("confidence")
            conf_part = f" (confidence: {conf:.0%})" if conf is not None else ""
            lines.append(f"### {sd['table_name']}.{sd['column_name']}{conf_part}")
            if sd.get("business_context"):
                lines.append(f"  Context: {sd['business_context'][:500]}")

            # Show values with counts if available
            value_counts = sd.get("value_counts", [])
            if value_counts:
                total = sum(vc["count"] for vc in value_counts)
                values_str = ", ".join(
                    f"{vc['value']} ({vc['count']:,}, {vc['percentage']}%)" for vc in value_counts
                )
                lines.append(f"  Values ({total:,} total): {values_str}")
            elif sd.get("values"):
                lines.append(f"  Values: {', '.join(sd['values'])}")
            lines.append("")

    # Derived (numeric) relationships — completion signals a status column can't carry
    derived = context.get("derived_relationships", [])
    if derived:
        lines.append("## DERIVED NUMERIC RELATIONSHIPS (Completion Signals)")
        lines.append("")
        lines.append("Arithmetic relationships the pipeline detected between columns, with how")
        lines.append("often each holds (match rate). A cycle that completes on a NUMERIC")
        lines.append("condition rather than a status value (e.g. a ledger that balances, a")
        lines.append("reconciliation that ties out) grounds on one of these — use the match")
        lines.append("rate as the completion_rate. Only relationships present here are real.")
        lines.append("")
        for dr in derived:
            srcs = ", ".join(dr["source_columns"])
            lines.append(
                f"- {dr['table_name']}.{dr['derived_column']} = {dr['formula']} "
                f"({dr['derivation_type']} of [{srcs}], holds {dr['match_rate']:.0%})"
            )
        lines.append("")

    # The column meaning feed (DAT-769) — the metric grounding context
    field_mappings = context.get("field_mappings", "")
    if field_mappings:
        lines.append(field_mappings)
        lines.append("")

    # Enriched views
    enriched = context.get("enriched_views", [])
    if enriched:
        lines.append("## ENRICHED VIEWS (Pre-Joined Tables)")
        lines.append("")
        lines.append("These DuckDB views join fact tables with their dimension tables.")
        lines.append("They represent confirmed business relationships.")
        lines.append("")
        for ev in enriched:
            dims = ", ".join(ev["dimension_tables"]) if ev["dimension_tables"] else "none"
            lines.append(f"- {ev['view_name']}: {ev['fact_table']} + [{dims}]")
            if ev.get("dimension_columns"):
                cols = ", ".join(ev["dimension_columns"])
                lines.append(f"  Added columns: {cols}")
        lines.append("")

    # Relationships (REFERENCE kinds only — the edges entity flows ride on)
    lines.append("## CONFIRMED RELATIONSHIPS")
    for rel in context.get("relationships", []):
        lines.append(
            f"- {rel['from_table']}.{rel['from_column']} → "
            f"{rel['to_table']}.{rel['to_column']} "
            f"({rel['relationship_type']}, {rel['cardinality']}, conf={rel['confidence']:.0%})"
        )
        # Chain-conditioned evidence (DAT-853): the from-side labels and
        # measure sign/ranges on ONLY the rows that ride this join — where
        # these differ from a column's flat samples/profile, they are the
        # evidence for what the chain itself carries (e.g. which party the
        # linked rows involve, and the flow sign of the linked rows).
        for cls_entry in rel.get("conditioned_label_samples", []):
            lines.append(
                f"    {rel['from_table']}.{cls_entry['column']} "
                f"({rel['from_column']}-joined rows only): " + ", ".join(cls_entry["samples"])
            )
        for range_entry in rel.get("conditioned_measure_ranges", []):
            lines.append(
                f"    {rel['from_table']}.{range_entry['column']} "
                f"({rel['from_column']}-joined rows only): "
                f"min={range_entry['min']} max={range_entry['max']} — {range_entry['summary']}"
            )
    lines.append("")

    # Conformed-dimension meetings (DAT-850): confirmed edges whose measured
    # cardinality refuted the reference claim — two facts sharing an axis. Served
    # under their own heading so their absence from the references above is loud,
    # and the LLM never routes an entity flow through a shared-axis join.
    conformed = context.get("conformed_meetings", [])
    if conformed:
        lines.append("## CONFORMED DIMENSION MEETINGS (shared axes — NOT references)")
        lines.append("These column pairs join two fact tables on a shared dimension axis.")
        lines.append("No entity flows through them; joining on one fans out both ways.")
        for rel in conformed:
            lines.append(
                f"- {rel['from_table']}.{rel['from_column']} ↔ "
                f"{rel['to_table']}.{rel['to_column']} "
                f"(shared axis, {rel['cardinality']}, conf={rel['confidence']:.0%})"
            )
        lines.append("")

    # Graph topology
    graph_topology = context.get("graph_topology")
    if graph_topology:
        lines.append(format_graph_structure_for_context(graph_topology))
        lines.append("")

    # Temporal patterns
    temporal = context.get("temporal_profiles", [])
    if temporal:
        lines.append("## TEMPORAL PATTERNS")
        for tp in temporal:
            stale_str = " [STALE]" if tp.get("is_stale") else ""
            # completeness is None on an irregular/unknown grain — there is no bucket to
            # count, so the ratio is not computable (DAT-810). Say so; a missing number
            # must not read as a number, and `:.0%` on None raises.
            comp = tp["completeness"]
            comp_str = f"{comp:.0%}" if comp is not None else "not computable (no grain)"
            lines.append(
                f"- {tp['table_name']}.{tp['column_name']}: "
                f"{tp['granularity']}, "
                f"{tp['date_range_start']} to {tp['date_range_end']}, "
                f"completeness={comp_str}{stale_str}"
            )
        lines.append("")

    # Column semantics by table
    lines.append("## COLUMN SEMANTICS BY TABLE")
    lines.append("")
    lines.append("annotation_confidence encodes how much the column NAME communicates —")
    lines.append("a low value marks an unreadable name, not weak data evidence. Identity")
    lines.append("and relationship-endpoint columns carry value samples where available;")
    lines.append("for a low-confidence annotation, ground the entity determination in the")
    lines.append("samples and confirmed relationships rather than the annotation's wording.")
    for table in context.get("tables", []):
        lines.append(f"\n### {table['table_name']}")
        for col in table["columns"]:
            parts = [col["name"]]
            if col.get("semantic_role"):
                parts.append(f"role={col['semantic_role']}")
            if col.get("meaning"):
                parts.append(f"meaning={col['meaning']}")
            if col.get("entity_type"):
                parts.append(f"entity={col['entity_type']}")
            if col.get("annotation_confidence") is not None:
                parts.append(f"annotation_confidence={col['annotation_confidence']:.2f}")
            lines.append(f"  - {', '.join(parts)}")
            if col.get("business_description"):
                lines.append(f"    {col['business_description'][:500]}")
            if col.get("sample_values"):
                lines.append(f"    samples: {', '.join(col['sample_values'])}")

    return "\n".join(lines)
