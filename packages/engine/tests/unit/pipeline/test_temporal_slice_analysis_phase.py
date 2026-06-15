"""Unit tests for the temporal slice analysis phase's time-axis machinery.

Covers ``_resolve_time_columns_per_table`` — per-table time-axis resolution:
config > semantic priority, temporal-type filtering, the profile-less enriched
``fk__col`` axis (DAT-491), run scoping, and the ``no_time_column_resolved``
warning — plus the ``should_skip`` pass-through for a profile-less enriched axis.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy.orm import Session
from structlog.testing import capture_logs

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.temporal_slice_analysis_phase import TemporalSliceAnalysisPhase
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id

if TYPE_CHECKING:
    import duckdb


def _seed_typed_table(
    session: Session,
    table_name: str = "invoices",
    duckdb_path: str = "csv__invoices",
) -> Table:
    """Create a Source + typed Table pair (flushed so dependents can FK it)."""
    source = Source(source_id=str(uuid4()), name=f"src_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=table_name,
        layer="typed",
        duckdb_path=duckdb_path,
        row_count=10,
    )
    session.add(table)
    session.flush()
    return table


def _seed_column(
    session: Session,
    table_id: str,
    column_name: str,
    resolved_type: str | None = None,
    position: int = 0,
) -> Column:
    """Create a Column on a typed table."""
    column = Column(
        column_id=str(uuid4()),
        table_id=table_id,
        column_name=column_name,
        column_position=position,
        raw_type="VARCHAR",
        resolved_type=resolved_type,
    )
    session.add(column)
    session.flush()
    return column


def _seed_profile(session: Session, column_id: str) -> TemporalColumnProfile:
    """Create a TemporalColumnProfile with explicit, bounded 2024 timestamps."""
    profile = TemporalColumnProfile(
        profile_id=str(uuid4()),
        column_id=column_id,
        profiled_at=datetime(2024, 7, 1, tzinfo=UTC),
        min_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        max_timestamp=datetime(2024, 6, 30, tzinfo=UTC),
        detected_granularity="daily",
        profile_data={},
    )
    session.add(profile)
    session.flush()
    return profile


def _seed_entity(
    session: Session,
    table_id: str,
    time_column: str | None,
    run_id: str | None = None,
) -> TableEntity:
    """Create a TableEntity carrying the semantic time_column annotation."""
    entity = TableEntity(
        table_id=table_id,
        detected_entity_type="invoice",
        time_column=time_column,
        run_id=run_id,
    )
    session.add(entity)
    session.flush()
    return entity


def _seed_slice_definition(
    session: Session,
    table_id: str,
    column_id: str,
    run_id: str | None = None,
) -> SliceDefinition:
    """Create a SliceDefinition stamped with the given run."""
    slice_def = SliceDefinition(
        table_id=table_id,
        column_id=column_id,
        run_id=run_id,
        slice_priority=1,
        slice_type="categorical",
        distinct_values=["a", "b"],
        reasoning="segmentation",
    )
    session.add(slice_def)
    session.flush()
    return slice_def


def _make_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    config: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> PhaseContext:
    """Build a PhaseContext over the SQLite session fixture.

    ``run_id`` defaults to ``baseline_run_id()`` — the before_flush autofill
    stamps seeded run-versioned rows (slice definitions, entities) with the same
    baseline, so the phase's run-scoped reads resolve them (DAT-506).
    """
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        config=config or {},
        run_id=run_id if run_id is not None else baseline_run_id(),
    )


class TestResolveTimeColumnsPerTable:
    """Tests for TemporalSliceAnalysisPhase._resolve_time_columns_per_table."""

    def setup_method(self) -> None:
        self.phase = TemporalSliceAnalysisPhase()

    def test_enriched_axis_without_profiles_resolves_with_none_profile(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """An enriched ``fk__col`` axis with zero profiles resolves to (name, None)."""
        table = _seed_typed_table(session)
        _seed_entity(session, table.table_id, "invoice_id__date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert result == {table.table_id: ("invoice_id__date", None)}

    def test_plain_semantic_name_without_profile_omits_table(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A profile-less plain (non-enriched) semantic name leaves the table out."""
        table = _seed_typed_table(session)
        _seed_entity(session, table.table_id, "booking_date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert result == {}

    def test_profiled_semantic_axis_resolves_with_profile(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A profiled DATE column named by the semantic annotation carries its profile."""
        table = _seed_typed_table(session)
        date_col = _seed_column(session, table.table_id, "date", resolved_type="DATE")
        profile = _seed_profile(session, date_col.column_id)
        _seed_entity(session, table.table_id, "date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert table.table_id in result
        column_name, resolved_profile = result[table.table_id]
        assert column_name == "date"
        assert resolved_profile is profile

    def test_enriched_axis_wins_over_unrelated_profiled_column(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """An enriched semantic axis resolves (name, None) even with other profiles present."""
        table = _seed_typed_table(session)
        created_col = _seed_column(session, table.table_id, "created_at", resolved_type="DATE")
        _seed_profile(session, created_col.column_id)
        _seed_entity(session, table.table_id, "invoice_id__date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert result == {table.table_id: ("invoice_id__date", None)}

    def test_config_time_column_takes_precedence_over_semantic(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """ctx.config time_column beats the semantic annotation when both are profiled."""
        table = _seed_typed_table(session)
        created_col = _seed_column(
            session, table.table_id, "created_at", resolved_type="DATE", position=0
        )
        date_col = _seed_column(session, table.table_id, "date", resolved_type="DATE", position=1)
        created_profile = _seed_profile(session, created_col.column_id)
        _seed_profile(session, date_col.column_id)
        _seed_entity(session, table.table_id, "date")

        ctx = _make_ctx(
            session, duckdb_conn, [table.table_id], config={"time_column": "created_at"}
        )
        result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert table.table_id in result
        column_name, resolved_profile = result[table.table_id]
        assert column_name == "created_at"
        assert resolved_profile is created_profile

    def test_varchar_profile_filtered_out_omits_table(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A profile on a VARCHAR-resolved column is not a temporal axis — table omitted."""
        table = _seed_typed_table(session)
        date_col = _seed_column(session, table.table_id, "date", resolved_type="VARCHAR")
        _seed_profile(session, date_col.column_id)

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert result == {}

    def test_entity_run_scoping_filters_other_runs(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Under ctx.run_id=r1, an r0-stamped entity is invisible; an r1 entity resolves."""
        stale_table = _seed_typed_table(session, table_name="stale", duckdb_path="csv__stale")
        current_table = _seed_typed_table(session, table_name="current", duckdb_path="csv__current")
        _seed_entity(session, stale_table.table_id, "invoice_id__date", run_id="r0")
        _seed_entity(session, current_table.table_id, "invoice_id__date", run_id="r1")

        table_ids = [stale_table.table_id, current_table.table_id]
        ctx = _make_ctx(session, duckdb_conn, table_ids, run_id="r1")
        result = self.phase._resolve_time_columns_per_table(
            ctx, table_ids, [stale_table, current_table]
        )

        assert stale_table.table_id not in result
        assert result == {current_table.table_id: ("invoice_id__date", None)}

    def test_no_resolution_warns_with_candidates(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Profiles without a matching config/semantic name warn and omit the table."""
        table = _seed_typed_table(session)
        created_col = _seed_column(session, table.table_id, "created_at", resolved_type="DATE")
        _seed_profile(session, created_col.column_id)

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        with capture_logs() as logs:
            result = self.phase._resolve_time_columns_per_table(ctx, [table.table_id], [table])

        assert result == {}
        warnings = [e for e in logs if e["event"] == "no_time_column_resolved"]
        assert len(warnings) == 1
        assert warnings[0]["candidate_columns"] == ["created_at"]


class TestShouldSkipEnrichedAxis:
    """should_skip pass-through for a profile-less enriched time axis (DAT-491)."""

    def setup_method(self) -> None:
        self.phase = TemporalSliceAnalysisPhase()

    def _seed_base(self, session: Session, slice_run_id: str | None = None) -> Table:
        """Typed table + column + this-run slice definition, NO temporal profile."""
        table = _seed_typed_table(session)
        column = _seed_column(session, table.table_id, "customer")
        _seed_slice_definition(session, table.table_id, column.column_id, run_id=slice_run_id)
        return table

    def test_enriched_axis_passes_skip_gate(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A TableEntity time_column lets the phase run despite zero profiles."""
        table = self._seed_base(session)
        _seed_entity(session, table.table_id, "invoice_id__date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])

        assert self.phase.should_skip(ctx) is None

    def test_no_time_axis_skips_for_missing_profiles(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Without an entity time_column, zero profiles skip the phase."""
        table = self._seed_base(session)

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        reason = self.phase.should_skip(ctx)

        assert reason is not None
        assert "No temporal profiles" in reason

    def test_stale_run_entity_still_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """An entity stamped by a prior run is invisible to this run's axis check."""
        table = self._seed_base(session, slice_run_id="r1")
        _seed_entity(session, table.table_id, "invoice_id__date", run_id="r0")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id], run_id="r1")
        reason = self.phase.should_skip(ctx)

        assert reason is not None
        assert "No temporal profiles" in reason

    def test_run_completes_with_no_slice_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """With a resolved axis but no materialized slice tables, the phase completes clean.

        Periods are now derived from each slice table's data (no view bounds);
        with zero ``layer="slice"`` tables to scan, the phase resolves the axis
        and produces zero period analyses rather than failing.
        """
        table = self._seed_base(session)
        _seed_entity(session, table.table_id, "invoice_id__date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        result = self.phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["period_analyses"] == 0
        assert result.outputs["time_columns"] == ["invoice_id__date"]
