"""Resolved layer — collapse adjudications onto the read-surface row (ADR-0009).

The pooling engine produces per-token posteriors; the *resolved* layer writes the
column's best value where the consumer reads it. The query agent doesn't want a
posterior — it wants the column's null tokens, so it can treat them as NULL in
generated SQL. This runs inside the terminal ``detect`` transaction (same
``run_id``), updating the ``SemanticAnnotation`` that ``semantic_per_column``
already wrote for this run; ``current_semantic_annotations`` then surfaces it.

First (and so far only) measurement: ``null_semantics`` → ``null_tokens``. The
function is a no-op when a run wrote no ``null_semantics`` objects (e.g.
begin_session), so it can live unconditionally in the generic detect step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.measurements.null_semantics import resolved_null_tokens

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


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
        session.execute(
            update(SemanticAnnotation)
            .where(
                SemanticAnnotation.column_id == record.column_id,
                SemanticAnnotation.run_id == run_id,
            )
            .values(null_tokens=tokens)
        )
        updated += 1
    return updated
