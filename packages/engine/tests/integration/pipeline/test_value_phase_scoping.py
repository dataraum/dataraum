"""Run-scoped fact reads of the revived value phases (DAT-403).

``TableEntity`` is run-versioned and coexists across runs (DAT-408/413).
``slicing_view`` must read only the current run's fact classification, exactly
like ``enriched_views`` — an unscoped read leaks a prior run's facts.

(The former cross-source slice-counting test pinned slice_analysis' cross-run
"All slices already analyzed" arm — deleted by DAT-502: slice tables are not
run-versioned, so their presence says nothing about the current run; see
``tests/unit/pipeline/test_slice_analysis_phase.py``.)
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.slicing_view_phase import SlicingViewPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


def _id() -> str:
    return str(uuid4())


def _typed_table(session: Session, source_id: str, name: str) -> str:
    """Create a Source (if new) + one typed Table; return the table id."""
    if session.get(Source, source_id) is None:
        session.add(Source(source_id=source_id, name=f"{name}_{source_id[:8]}", source_type="csv"))
        session.flush()
    table_id = _id()
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name=name,
            layer="typed",
            duckdb_path=f"typed_{name}",
            row_count=100,
        )
    )
    return table_id


class TestSlicingViewRunScopedFacts:
    """slicing_view reads only the current run's fact classification (DAT-408)."""

    def test_should_skip_reads_only_the_scoped_run(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A fact row under run 'old' must not satisfy a run='new' build.

        Deterministic flip on the same data: the ``TableEntity`` AND the
        ``SliceDefinition`` (run-versioned since DAT-448) belong to run 'old'.
        Scoped to 'old' the phase sees a sliceable fact (does not skip); scoped
        to 'new' it sees none and skips. Without run-scoping the 'old' rows
        leak into both, so the 'new' assertion fails.
        """
        src = _id()
        t = _typed_table(session, src, "orders")
        col = _id()
        session.add(Column(column_id=col, table_id=t, column_name="region", column_position=0))
        session.flush()
        session.add(
            SliceDefinition(
                table_id=t,
                column_id=col,
                run_id="old",
                slice_priority=1,
                slice_type="categorical",
                distinct_values=["us"],
                value_count=1,
                detection_source="llm",
            )
        )
        session.add(
            TableEntity(
                entity_id=_id(),
                table_id=t,
                run_id="old",
                detected_entity_type="fact",
                is_fact_table=True,
            )
        )
        session.commit()

        phase = SlicingViewPhase()

        # run 'old' resolves the fact -> sliceable, do not skip.
        ctx_old = PhaseContext(
            session=session, duckdb_conn=duckdb_conn, table_ids=[t], config={}, run_id="old"
        )
        assert phase.should_skip(ctx_old) is None

        # run 'new' has no fact row -> nothing to slice, skip.
        ctx_new = PhaseContext(
            session=session, duckdb_conn=duckdb_conn, table_ids=[t], config={}, run_id="new"
        )
        skip_new = phase.should_skip(ctx_new)
        assert skip_new is not None
        assert "No slice definitions found for fact tables" in skip_new
