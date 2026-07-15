"""Resolved layer — collapse adjudications onto the read-surface row (ADR-0009).

The pooling engine produces per-token posteriors; the *resolved* layer writes the
column's best value where the consumer reads it. The query agent doesn't want a
posterior — it wants the column's null tokens, so it can treat them as NULL in
generated SQL. This runs inside the terminal ``detect`` transaction (same
``run_id``), updating the ``SemanticAnnotation`` that ``semantic_per_column``
already wrote for this run; ``current_semantic_annotations`` then surfaces it.

Two measurements resolve today: ``null_semantics`` → ``null_tokens`` and
``temporal_behavior`` → ``temporal_behavior``. Each is a no-op when a run wrote
no objects of its detector (e.g. begin_session), so both can live unconditionally
in the generic terminal detect step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import CursorResult, select, update

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.measurements.null_semantics import resolved_null_tokens

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def resolve_null_tokens(session: Session, run_id: str | None) -> int:
    """Write resolved ``null_tokens`` onto this run's ``SemanticAnnotation`` rows.

    Reads the ``null_semantics`` EntropyObject rows written this run, collapses
    each column's per-token adjudication to its is-null tokens, and UPDATEs the
    matching ``(column_id, run_id)`` annotation. Idempotent on retry (same run_id
    → same UPDATE). Returns the number of annotations updated.
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    records = session.execute(
        select(EntropyObjectRecord).where(
            EntropyObjectRecord.detector_id == "null_semantics",
            EntropyObjectRecord.run_id == run_id,
        )
    ).scalars()

    updated = 0
    for record in records:
        if record.column_id is None:
            continue
        evidence: list[Any] = record.evidence if isinstance(record.evidence, list) else []
        tokens = resolved_null_tokens(evidence)
        if not tokens:
            continue
        result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
            update(SemanticAnnotation)
            .where(
                SemanticAnnotation.column_id == record.column_id,
                SemanticAnnotation.run_id == run_id,
            )
            .values(null_tokens=tokens)
        )
        # rowcount, not attempts: on session runs the annotations carry
        # add_source run ids and every UPDATE matches zero rows — the intended
        # no-op must show as 0 in the detect log, not as N phantom resolves.
        updated += int(result.rowcount or 0)
    return updated


def resolve_temporal_behavior(session: Session, run_id: str | None) -> int:
    """Write the adjudicated stock/flow behaviour onto this run's annotations.

    Reads the ``temporal_behavior`` EntropyObject rows written this run (ADR-0009 /
    DAT-445) and, for each column whose adjudication resolved to a behaviour, UPDATEs
    the matching ``(column_id, run_id)`` annotation: ``temporal_behavior`` becomes the
    pooled-resolved value (the LLM stock/flow claim reconciled with the data-grounded
    structural witness — the ontology prior was dropped, DAT-657). This verdict is
    authoritative on its own — DAT-786 removed the parallel ``temporal_behavior_contested``
    column: propagating a doubt-flag downstream second-guessed a resolution that is
    already deterministic and correct. A disagreement between the LLM claim and the
    structural witness is logged here for observability, not persisted. Columns that
    resolved to total ignorance (no witness) are left untouched, preserving any prior
    value. Idempotent on retry (same run_id → same UPDATE). Returns the number of
    annotations updated.
    """
    # temporal_behavior is catalogue-grain (DAT-637): on ColumnConcept, authored by
    # the table agent and resolved here at session_detect (the run that holds
    # ColumnConcept). At add_source detect no ColumnConcept exists under the run, so
    # the UPDATE matches nothing — a harmless no-op, the correct grain.
    from dataraum.analysis.semantic.db_models import ColumnConcept

    records = session.execute(
        select(EntropyObjectRecord).where(
            EntropyObjectRecord.detector_id == "temporal_behavior",
            EntropyObjectRecord.run_id == run_id,
        )
    ).scalars()

    updated = 0
    for record in records:
        if record.column_id is None:
            continue
        evidence: list[Any] = record.evidence if isinstance(record.evidence, list) else []
        first = evidence[0] if evidence and isinstance(evidence[0], dict) else None
        if first is None:
            continue
        resolved = first.get("resolved")
        if resolved is None:
            continue  # total ignorance — leave any prior value in place
        if first.get("contested"):
            # Diagnostic only (DAT-786) — the resolved value below still wins.
            # debug, not info: per-item log inside a loop (entropy/ convention).
            logger.debug(
                "temporal_behavior_contested",
                column_id=record.column_id,
                run_id=run_id,
                resolved=resolved,
            )
        result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
            update(ColumnConcept)
            .where(
                ColumnConcept.column_id == record.column_id,
                ColumnConcept.run_id == run_id,
            )
            .values(temporal_behavior=resolved)
        )
        updated += int(result.rowcount or 0)  # see resolve_null_tokens — no-ops stay visible
    return updated
