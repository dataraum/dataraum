"""Persist path for the catalogue_semantics phase (DAT-823).

Carries the machinery the rebalance moved off ``semantic_per_table``'s
processor: the sole ``ColumnConcept`` INSERT writer (``persist_column_concepts``
— resolve's ``temporal_behavior`` UPDATE stays the one second writer), the
bounded coverage-retry loop (DAT-725), the loud partial-coverage warning and the
zero-meaning fail-the-run gate (DAT-768/769) — plus the new catalogue duties:
the ``TableEntity`` business-reading UPDATE keyed ``(table_id, run_id)`` and the
persisted ``meaning_status`` determination (W2-A persisted-status precedent).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.catalogue.models import (
    CatalogueSemanticsOutput,
    ColumnConceptOutput,
    TableReadingOutput,
)
from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity
from dataraum.analysis.semantic.utils import load_column_mappings, load_table_mappings
from dataraum.core.logging import get_logger
from dataraum.core.models.base import DecisionSource, Result
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb

    from dataraum.analysis.catalogue.agent import CatalogueSemanticsAgent

logger = get_logger(__name__)

# Bounded re-prompts when the authoring turn under-covers (DAT-725, moved with
# the machinery): the contract is a meaning for EVERY column and a reading for
# EVERY table, but one wide call can truncate. Each retry serves the SAME
# prompt scoped to the tables that still have gaps, so it is cheap; warn-only
# stays the terminal state when the attempts are exhausted.
CONCEPT_COVERAGE_RETRIES = 2


@dataclass(frozen=True)
class ConceptPersistCounts:
    """Emitted vs resolved vs dropped for a ``column_concepts`` persist (DAT-768).

    ``emitted`` = concepts the agent produced; ``resolved`` = rows actually
    written (name matched a column, after the same-column dedup);
    ``dropped_unresolved`` = concepts whose ``(table, column)`` name matched no
    column in the batch. ``ambiguous`` counts resolved rows whose persisted
    ``meaning_status`` is the declared-ignorance state. A silently-empty
    load-bearing surface is a visible count, not indistinguishable from "no
    concepts to bind".
    """

    emitted: int
    resolved: int
    dropped_unresolved: int
    with_meaning: int  # resolved rows carrying a non-empty meaning (the load-bearing field)
    ambiguous: int


@dataclass(frozen=True)
class CatalogueSemanticsStats:
    """The catalogue authoring's observable outcome — a first-class phase output.

    The BusMatrixStats.as_output pattern: scalar counters flattened to a plain
    dict for both ``PhaseResult.outputs`` and structured logging.
    """

    authored_tables: int
    authored_columns: int
    ambiguous: int
    missing: int
    dropped_unresolved: int

    def as_output(self) -> dict[str, object]:
        return {
            "authored_tables": self.authored_tables,
            "authored_columns": self.authored_columns,
            "ambiguous": self.ambiguous,
            "missing": self.missing,
            "dropped_unresolved": self.dropped_unresolved,
        }


def persist_column_concepts(
    session: Session,
    column_concepts: list[ColumnConceptOutput],
    table_ids: list[str],
    *,
    annotated_by: str,
    run_id: str,
) -> ConceptPersistCounts:
    """Persist the catalogue agent's per-column semantics (DAT-637/823).

    Writes ``ColumnConcept`` rows under the begin_session (catalogue head) run —
    the SOLE INSERT writer. ``temporal_behavior`` is NOT seeded here (DAT-657):
    stock/flow is a data-format property, left NULL at authoring and written
    only by the data-grounded resolve pass (``entropy.resolve``), the one second
    writer. ``meaning_status`` persists the agent's determination — 'ambiguous'
    is declared ignorance WITH a meaning present (the meaning text states what
    is undetermined, DAT-769); a row without a meaning carries no status.
    Run-scoped upsert on ``(column_id, run_id)``; a column the agent did not
    bind this run has no row (absent = no concept), and run-scoped reads never
    see a prior run's.

    Returns:
        A :class:`ConceptPersistCounts` breakdown. The counts are logged so a
        name-resolution wipeout (every emitted concept dropped as unresolved,
        DAT-768 path #2) is diagnosable rather than indistinguishable from an
        empty emission; the caller gates begin_session on ``with_meaning``.
    """
    column_map = load_column_mappings(session, table_ids)

    rows: list[dict[str, Any]] = []
    dropped: list[tuple[str, str]] = []
    for cc in column_concepts:
        column_id = column_map.get((cc.table_name, cc.column_name))
        if not column_id:
            dropped.append((cc.table_name, cc.column_name))
            continue
        # Normalized like the formula hypothesis: an all-whitespace meaning is
        # absence, so the gate below and the feed's IS NOT NULL read agree.
        meaning = (cc.meaning or "").strip() or None
        rows.append(
            {
                "column_id": column_id,
                "run_id": run_id,
                "meaning": meaning,
                # No meaning -> no status: 'ambiguous' asserts a present meaning
                # that states what is undetermined; stamping it on an absent
                # meaning would dress a coverage gap as a judgment.
                "meaning_status": cc.determination if meaning else None,
                "unit_source_column": (cc.unit_source_column or "").strip() or None,
                "derived_formula_hypothesis": (cc.derived_formula_hypothesis or "").strip() or None,
                "derived_formula_confidence": cc.derived_formula_confidence,
                "annotation_source": DecisionSource.LLM.value,
                "annotated_by": annotated_by,
            }
        )

    # Dedup on the upsert key (column_id, run_id): the agent can emit the same
    # column twice, and Postgres ON CONFLICT cannot touch a row twice in one
    # batch (CardinalityViolation). Last mention wins.
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        deduped[(row["column_id"], row["run_id"])] = row
    rows = list(deduped.values())

    upsert(session, ColumnConcept, rows, index_elements=["column_id", "run_id"])

    counts = ConceptPersistCounts(
        emitted=len(column_concepts),
        resolved=len(rows),
        dropped_unresolved=len(dropped),
        with_meaning=sum(1 for r in rows if r["meaning"]),
        ambiguous=sum(1 for r in rows if r["meaning_status"] == "ambiguous"),
    )
    logger.info(
        "column_concepts_persisted",
        emitted=counts.emitted,
        resolved=counts.resolved,
        dropped_unresolved=counts.dropped_unresolved,
        with_meaning=counts.with_meaning,
        ambiguous=counts.ambiguous,
    )
    if dropped:
        # The exact names the agent echoed that resolved to no column — the signal
        # that distinguishes a naming drift (case, enriched prefix, display name)
        # from a genuinely empty emission.
        logger.debug("column_concepts_dropped_unresolved", dropped=dropped)
    return counts


def apply_table_readings(
    session: Session,
    readings: list[TableReadingOutput],
    table_ids: list[str],
    *,
    run_id: str,
) -> tuple[int, list[str]]:
    """UPDATE the run's TableEntity stubs with the authored business readings.

    Keyed ``(table_id, run_id)`` — the per-table tier INSERTed the structural
    stub with ``detected_entity_type IS NULL``; this closes the within-run NULL
    window (verified: zero readers of entity_type/description between the two
    phases). Idempotent under a Temporal at-least-once retry (same run_id, same
    UPDATE). A reading naming a table with no stub (a hallucinated name, or a
    table the structural turn did not classify) is dropped + logged, never
    fabricated into a row.

    Returns:
        ``(applied_count, dropped_table_names)``.
    """
    table_map = load_table_mappings(session, table_ids)
    entities: dict[str, TableEntity] = {
        e.table_id: e
        for e in session.execute(
            select(TableEntity).where(
                TableEntity.table_id.in_(table_ids), TableEntity.run_id == run_id
            )
        ).scalars()
    }
    applied: set[str] = set()
    dropped: list[str] = []
    for reading in readings:
        table_id = table_map.get(reading.table_name)
        entity = entities.get(table_id) if table_id else None
        if entity is None:
            dropped.append(reading.table_name)
            continue
        entity.detected_entity_type = reading.entity_type.strip() or None
        entity.description = reading.description.strip() or None
        applied.add(entity.table_id)
    if dropped:
        logger.warning("table_readings_dropped_unresolved", dropped=dropped)
    return len(applied), dropped


def _missing_concept_keys(
    column_map: dict[tuple[str, str], str],
    column_concepts: list[ColumnConceptOutput],
) -> list[tuple[str, str]]:
    """Catalogue columns not covered by a MEANINGFUL ``column_concepts`` entry.

    Covered = an entry whose ``meaning`` is non-empty after stripping — the same
    definition ``persist_column_concepts`` normalizes to (whitespace-only becomes
    NULL at persist), so the retry loop and the terminal partial-coverage warning
    agree with the persisted surface: a blank (re-)emission is still missing.
    """
    covered = {
        (cc.table_name, cc.column_name) for cc in column_concepts if (cc.meaning or "").strip()
    }
    return sorted(k for k in column_map if k not in covered)


def _missing_reading_tables(
    table_map: dict[str, str],
    readings: list[TableReadingOutput],
) -> list[str]:
    """Catalogue tables not covered by a non-empty business reading."""
    covered = {r.table_name for r in readings if (r.entity_type or "").strip()}
    return sorted(name for name in table_map if name not in covered)


def _retry_missing_coverage(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    agent: CatalogueSemanticsAgent,
    output: CatalogueSemanticsOutput,
    *,
    column_map: dict[tuple[str, str], str],
    table_map: dict[str, str],
    session_table_ids: list[str],
    ontology: str,
    run_id: str,
) -> None:
    """Fill coverage gaps with bounded scoped re-prompts, in place (DAT-725).

    Re-runs :meth:`CatalogueSemanticsAgent.author` for ONLY the tables that
    still have uncovered columns or no business reading, and merges the retry's
    entries for the still-missing keys into ``output`` — the first MEANINGFUL
    emission wins (a retry is only asked for what still lacks one, so it never
    displaces one; ``persist_column_concepts``' last-mention-wins dedup settles
    duplicate mentions inside one response). Best-effort by contract — a failed
    retry logs and stops (the first pass succeeded; coverage stays warn-only),
    and the caller persists ONCE after the loop (idempotent writer, ADR-0010).
    """
    missing_cols = _missing_concept_keys(column_map, output.column_concepts)
    missing_tables = _missing_reading_tables(table_map, output.table_readings)
    for _attempt in range(CONCEPT_COVERAGE_RETRIES):
        if not missing_cols and not missing_tables:
            return
        retry_tables = {t for t, _c in missing_cols} | set(missing_tables)
        retry_table_ids = [tid for name, tid in table_map.items() if name in retry_tables]
        if not retry_table_ids:
            return
        logger.info(
            "catalogue_coverage_retry",
            missing_columns=len(missing_cols),
            missing_readings=len(missing_tables),
            tables=sorted(retry_tables),
        )
        retry_result = agent.author(
            session,
            duckdb_conn,
            table_ids=retry_table_ids,
            session_table_ids=session_table_ids,
            ontology=ontology,
            run_id=run_id,
        )
        if not retry_result.success:
            logger.warning("catalogue_coverage_retry_failed", error=retry_result.error)
            return
        retry_output = retry_result.unwrap()
        still_missing_cols = set(missing_cols)
        output.column_concepts.extend(
            cc
            for cc in retry_output.column_concepts
            if (cc.table_name, cc.column_name) in still_missing_cols
        )
        still_missing_tables = set(missing_tables)
        output.table_readings.extend(
            r for r in retry_output.table_readings if r.table_name in still_missing_tables
        )
        missing_cols = _missing_concept_keys(column_map, output.column_concepts)
        missing_tables = _missing_reading_tables(table_map, output.table_readings)


def author_and_store_catalogue(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    agent: CatalogueSemanticsAgent,
    table_ids: list[str],
    ontology: str,
    *,
    run_id: str,
) -> Result[CatalogueSemanticsStats]:
    """One catalogue authoring turn + retries, persisted (DAT-823).

    Calls :meth:`CatalogueSemanticsAgent.author` over the whole session, fills
    coverage gaps with bounded scoped retries, then persists ONCE: the
    ``ColumnConcept`` rows (sole INSERT writer) and the ``TableEntity``
    business-reading UPDATEs. Terminal coverage posture (moved with the
    machinery, DAT-768/769): partial column coverage is a LOUD warning naming
    the uncovered columns; a table left without a reading keeps its NULL
    entity_type (honest ignorance, W2-A) under a loud warning; ZERO meaningful
    column rows fails the run — the meaning context every grounding prompt
    transports would be empty.
    """
    llm_result = agent.author(
        session,
        duckdb_conn,
        table_ids=table_ids,
        session_table_ids=table_ids,
        ontology=ontology,
        run_id=run_id,
    )
    if not llm_result.success:
        return Result.fail(llm_result.error or "Catalogue authoring failed")
    output = llm_result.unwrap()

    table_map = load_table_mappings(session, table_ids)
    column_map = load_column_mappings(session, table_ids)

    _retry_missing_coverage(
        session,
        duckdb_conn,
        agent,
        output,
        column_map=column_map,
        table_map=table_map,
        session_table_ids=table_ids,
        ontology=ontology,
        run_id=run_id,
    )

    annotated_by = agent.provider.get_model_for_tier(
        agent.config.features.semantic_analysis.model_tier
    )
    counts = persist_column_concepts(
        session,
        output.column_concepts,
        table_ids,
        annotated_by=annotated_by,
        run_id=run_id,
    )
    applied_tables, dropped_readings = apply_table_readings(
        session, output.table_readings, table_ids, run_id=run_id
    )

    missing_readings = _missing_reading_tables(table_map, output.table_readings)
    if missing_readings:
        # The stub keeps its NULL entity_type — declared ignorance, never a
        # fabricated reading. Loud so a wide-catalogue run is diagnosable.
        logger.warning("table_readings_partial_coverage", missing=missing_readings)

    # DAT-768/769: the column_concepts surface is load-bearing — the meaning
    # context every downstream grounding prompt (metric graph agent, cycles,
    # validation) transports. Every column carries a meaning by contract, so
    # ZERO resolved entries is never a judgment; it is an emptied surface (the
    # agent under-produced the whole field, or every name it echoed failed to
    # resolve). Fail begin_session loud rather than ship it green. Gates on
    # emptiness only — never on any content of a meaning or hint. Partial
    # coverage must be VISIBLE, never silent, and after the bounded retries it
    # is the terminal state; no hard threshold (the eval's consumer oracles
    # grade the outcome) — the warning names the uncovered columns.
    missing = _missing_concept_keys(column_map, output.column_concepts)
    total_columns = len(column_map)
    if counts.with_meaning < total_columns:
        logger.warning(
            "column_meanings_partial_coverage",
            with_meaning=counts.with_meaning,
            total_columns=total_columns,
            missing=missing[:40],
        )
    if counts.with_meaning == 0:
        return Result.fail(
            "column_concepts resolved to zero meaningful rows for a non-empty "
            f"schema (emitted={counts.emitted}, resolved={counts.resolved}, "
            f"with_meaning=0, dropped_unresolved={counts.dropped_unresolved}) — "
            "the meaning context every grounding prompt transports would be "
            "empty (DAT-768)."
        )

    return Result.ok(
        CatalogueSemanticsStats(
            authored_tables=applied_tables,
            authored_columns=counts.resolved,
            ambiguous=counts.ambiguous,
            missing=len(missing),
            dropped_unresolved=counts.dropped_unresolved + len(dropped_readings),
        )
    )


__all__ = [
    "CONCEPT_COVERAGE_RETRIES",
    "CatalogueSemanticsStats",
    "ConceptPersistCounts",
    "apply_table_readings",
    "author_and_store_catalogue",
    "persist_column_concepts",
]
