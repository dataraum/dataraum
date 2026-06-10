"""Unit tests for the temporal slice analysis phase's time-axis machinery.

Covers two legs of ``temporal_slice_analysis_phase``:

- ``_resolve_time_columns_per_table`` — per-table time-axis resolution:
  config > semantic priority, temporal-type filtering, the profile-less
  enriched ``fk__col`` axis (DAT-491), run scoping, and the
  ``no_time_column_resolved`` warning.
- ``_view_time_bounds`` — MIN/MAX over the slicing view in the lake catalog
  (every failure swallows to ``None``, so the happy path asserts exact dates)
  plus the ``should_skip`` pass-through for a profile-less enriched axis.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy.orm import Session
from structlog.testing import capture_logs

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.temporal_slice_analysis_phase import (
    TemporalSliceAnalysisPhase,
    _view_time_bounds,
)
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id

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
    """Build a PhaseContext over the SQLite session fixture."""
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        config=config or {},
        session_id=baseline_session_id(),
        run_id=run_id,
    )


def _attach_lake(conn: duckdb.DuckDBPyConnection) -> None:
    """Fake the DuckLake catalog: in-memory ATTACH as ``lake`` + typed schema."""
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE SCHEMA lake.typed")


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


class TestViewTimeBounds:
    """Tests for the module-level _view_time_bounds over a faked lake catalog.

    The function swallows every exception to ``None``, so the happy path must
    assert exact dates — a name typo would silently land in the None branch.
    """

    def test_happy_path_exact_bounds(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        """DATE column over the slicing view yields exact (min, max) dates.

        The literal view name pins the lockstep with ``slicing_view_name``:
        ``csv__invoices`` sanitizes with the underscore run collapsed.
        """
        _attach_lake(duckdb_conn)
        duckdb_conn.execute(
            'CREATE TABLE lake.typed."slicing_csv_invoices" ("invoice_id__date" DATE)'
        )
        duckdb_conn.execute(
            'INSERT INTO lake.typed."slicing_csv_invoices" VALUES '
            "(DATE '2024-01-15'), (DATE '2024-02-10'), (DATE '2024-03-02')"
        )

        bounds = _view_time_bounds(duckdb_conn, "csv__invoices", "invoice_id__date")

        assert bounds == (date(2024, 1, 15), date(2024, 3, 2))

    def test_varchar_iso_strings_cast_to_dates(
        self, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """VARCHAR ISO date strings cast cleanly to exact date bounds."""
        _attach_lake(duckdb_conn)
        duckdb_conn.execute(
            'CREATE TABLE lake.typed."slicing_csv_invoices" ("invoice_id__date" VARCHAR)'
        )
        duckdb_conn.execute(
            'INSERT INTO lake.typed."slicing_csv_invoices" VALUES '
            "('2024-03-02'), ('2024-01-15'), ('2024-02-10')"
        )

        bounds = _view_time_bounds(duckdb_conn, "csv__invoices", "invoice_id__date")

        assert bounds == (date(2024, 1, 15), date(2024, 3, 2))

    def test_all_null_column_returns_none(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        """An all-NULL time column hits the null-row branch (not the exception branch)."""
        _attach_lake(duckdb_conn)
        duckdb_conn.execute(
            'CREATE TABLE lake.typed."slicing_csv_invoices" ("invoice_id__date" DATE)'
        )
        duckdb_conn.execute('INSERT INTO lake.typed."slicing_csv_invoices" VALUES (NULL), (NULL)')

        assert _view_time_bounds(duckdb_conn, "csv__invoices", "invoice_id__date") is None

    def test_missing_view_returns_none(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        """A lake catalog without the slicing view swallows to None."""
        _attach_lake(duckdb_conn)

        assert _view_time_bounds(duckdb_conn, "csv__invoices", "invoice_id__date") is None

    def test_no_lake_catalog_returns_none(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        """A plain connection with no lake catalog at all swallows to None."""
        assert _view_time_bounds(duckdb_conn, "csv__invoices", "invoice_id__date") is None


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

    def test_run_completes_when_bounds_unresolvable(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The _run caller path warns and completes when the slicing view is missing.

        An enriched axis with no profile derives bounds via ``_view_time_bounds``;
        a plain connection (no lake catalog) yields None, so the table is skipped
        with a ``time_bounds_unresolvable`` warning instead of failing the phase.
        """
        table = self._seed_base(session)
        _seed_entity(session, table.table_id, "invoice_id__date")

        ctx = _make_ctx(session, duckdb_conn, [table.table_id])
        with capture_logs() as logs:
            result = self.phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert any(e["event"] == "time_bounds_unresolvable" for e in logs)
        assert result.outputs["drift_summaries"] == 0
        assert result.outputs["time_columns"] == ["invoice_id__date"]
