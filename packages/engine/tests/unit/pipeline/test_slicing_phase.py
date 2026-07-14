"""Unit tests for the DAT-491 time-axis legs of the slicing phase.

Part A pins ``SlicingPhase._build_context_data``: each table dict carries the
``time_columns`` read from THIS run's ``TableEntity`` (DAT-565 plural), and
enriched dimension entries are flagged ``is_dimension_time_column`` strictly via
the enriched view's relationship provenance (FK column -> dimension table ->
membership in that table's ``TableEntity.time_columns``), never by name inference.

Part B pins the post-analysis ``TableEntity.time_columns`` fill in ``_run``,
with the LLM boundary mocked at the phase module import site (the
``should_skip`` gates live in ``tests/integration/pipeline/test_slicing_phase.py``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session
from structlog.testing import capture_logs

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import SlicingAnalysisResult
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.slicing_phase import SlicingPhase
from dataraum.storage import Column, Source, Table
from dataraum.storage.upsert import upsert
from tests.conftest import baseline_run_id


def _axes(column: str | None) -> list[dict[str, str]]:
    """Plural ``time_columns`` JSON for a single named axis (DAT-565), or empty."""
    return [{"column": column, "aspect": "event", "note": "seed axis."}] if column else []


def _seed(
    session: Session,
    *,
    dim_time_column: str | None = "date",
    dim_axes: list[dict[str, str]] | None = None,
    fact_time_column: str | None = None,
    link_relationship: bool = True,
) -> dict[str, Any]:
    """Seed a fact table with an enriched view exposing FK-prefixed dim columns.

    Layout: ``invoices`` (typed fact, FK ``invoice_id``) joined to
    ``invoice_headers`` (typed dim, ``time_column=dim_time_column``) through a
    Relationship the EnrichedView references in ``relationship_ids`` (unless
    ``link_relationship=False``, which leaves the row in place but unreferenced).
    The view's registered Table carries ``invoice_id__date`` / ``invoice_id__status``
    Column rows. No StatisticalProfile rows — absent stats pass ``_pre_filter_columns``.
    Entities are seeded with ``run_id=None`` (before_flush autofills the baseline
    run, matching the default ``_ctx`` run scope, DAT-506).
    """
    src = Source(name="s", source_type="csv")
    session.add(src)
    session.flush()

    fact = Table(
        source_id=src.source_id,
        table_name="invoices",
        layer="typed",
        duckdb_path="csv__invoices",
        row_count=10,
    )
    session.add(fact)
    session.flush()
    fk_col = Column(
        table_id=fact.table_id,
        column_name="invoice_id",
        column_position=0,
        resolved_type="VARCHAR",
    )
    amount_col = Column(
        table_id=fact.table_id,
        column_name="amount",
        column_position=1,
        resolved_type="DOUBLE",
    )
    session.add_all([fk_col, amount_col])

    dim = Table(
        source_id=src.source_id,
        table_name="invoice_headers",
        layer="typed",
        duckdb_path="csv__invoice_headers",
        row_count=5,
    )
    session.add(dim)
    session.flush()
    dim_pk = Column(
        table_id=dim.table_id, column_name="id", column_position=0, resolved_type="VARCHAR"
    )
    session.add(dim_pk)
    session.flush()

    fact_entity = TableEntity(
        table_id=fact.table_id,
        run_id=None,
        detected_entity_type="transaction",
        time_columns=_axes(fact_time_column),
        detection_source="llm",
        confidence=0.9,
    )
    dim_entity = TableEntity(
        table_id=dim.table_id,
        run_id=None,
        detected_entity_type="document",
        time_columns=dim_axes if dim_axes is not None else _axes(dim_time_column),
        detection_source="llm",
        confidence=0.9,
    )
    session.add_all([fact_entity, dim_entity])

    rel = Relationship(
        run_id=None,
        from_table_id=fact.table_id,
        from_column_id=fk_col.column_id,
        to_table_id=dim.table_id,
        to_column_id=dim_pk.column_id,
        relationship_type="foreign_key",
        confidence=0.95,
        detection_method="llm",
    )
    session.add(rel)
    session.flush()

    view_table = Table(
        source_id=src.source_id,
        table_name="invoices_enriched",
        layer="enriched",
        duckdb_path="enriched_csv__invoices",
        row_count=10,
    )
    session.add(view_table)
    session.flush()
    session.add_all(
        [
            Column(
                table_id=view_table.table_id,
                column_name="invoice_id__date",
                column_position=0,
                resolved_type="DATE",
            ),
            Column(
                table_id=view_table.table_id,
                column_name="invoice_id__status",
                column_position=1,
                resolved_type="VARCHAR",
            ),
        ]
    )

    view = EnrichedView(
        fact_table_id=fact.table_id,
        view_table_id=view_table.table_id,
        view_name="enriched_csv__invoices",
        run_id=None,
        relationship_ids=[rel.relationship_id] if link_relationship else [],
        dimension_table_ids=[dim.table_id],
        dimension_columns=["invoice_id__date", "invoice_id__status"],
        is_grain_verified=True,
    )
    session.add(view)
    session.flush()

    return {"fact": fact, "fk_col": fk_col, "dim": dim, "fact_entity": fact_entity, "rel": rel}


def _ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str | None = None,
) -> PhaseContext:
    """Source-free ctx for the slicing phase, scoped by ``table_ids`` (DAT-401).

    ``run_id`` defaults to ``baseline_run_id()`` — the before_flush autofill
    stamps the seeded ``run_id=None`` entities/relationships/views with the same
    baseline, so the phase's run-scoped reads/writes resolve them (DAT-506).
    """
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        run_id=run_id if run_id is not None else baseline_run_id(),
    )


def _columns_by_name(table_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {c["column_name"]: c for c in table_data["columns"]}


class TestBuildContextDataTimeAxis:
    """Part A: the DAT-491 time-axis context ``_build_context_data`` assembles."""

    def test_dim_time_column_flagged_via_relationship_provenance(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The enriched column matching the dim's time_column is flagged; its
        column_id falls back to the FK column's id; the sibling stays False."""
        seeded = _seed(session)
        fact: Table = seeded["fact"]

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id]), [fact]
        )

        (table_data,) = data["tables"]
        # The fact has no own time axis — the agent must judge, not inherit.
        assert table_data["time_columns"] == []

        by_name = _columns_by_name(table_data)
        date_entry = by_name["invoice_id__date"]
        assert date_entry["is_dimension_time_column"] is True
        assert date_entry["is_enriched_dimension"] is True
        # column_id falls back to the FK column id (dim cols are not
        # individually registered on the fact).
        assert date_entry["column_id"] == seeded["fk_col"].column_id
        # Sibling dim column whose suffix is NOT the dim's time_column.
        assert by_name["invoice_id__status"]["is_dimension_time_column"] is False

    def test_dim_time_flag_matches_any_of_multiple_axes(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-565: the enriched suffix is flagged when it matches ANY of the dim's
        event-time axes — here the SECOND of two, exercising the set-membership
        match rather than equality-to-a-single-column."""
        seeded = _seed(
            session,
            dim_axes=[
                {"column": "other", "aspect": "x", "note": "n"},
                {"column": "date", "aspect": "event", "note": "n"},
            ],
        )
        fact: Table = seeded["fact"]

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id]), [fact]
        )

        by_name = _columns_by_name(data["tables"][0])
        assert by_name["invoice_id__date"]["is_dimension_time_column"] is True
        assert by_name["invoice_id__status"]["is_dimension_time_column"] is False

    def test_no_flag_when_dim_time_column_differs(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A dim whose time_column doesn't match any enriched suffix flags nothing."""
        seeded = _seed(session, dim_time_column="other")
        fact: Table = seeded["fact"]

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id]), [fact]
        )

        by_name = _columns_by_name(data["tables"][0])
        assert by_name["invoice_id__date"]["is_dimension_time_column"] is False
        assert by_name["invoice_id__status"]["is_dimension_time_column"] is False

    def test_no_flag_without_relationship_provenance(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """relationship_ids=[] -> no FK->dim resolution -> nothing flagged.

        The Relationship row exists and the column is literally named
        ``invoice_id__date`` — the flag must come from the view's relationship
        provenance, never from name inference.
        """
        seeded = _seed(session, link_relationship=False)
        fact: Table = seeded["fact"]

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id]), [fact]
        )

        by_name = _columns_by_name(data["tables"][0])
        assert by_name["invoice_id__date"]["is_dimension_time_column"] is False
        assert by_name["invoice_id__status"]["is_dimension_time_column"] is False

    def test_time_column_reads_this_runs_entity(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The table's time_column comes from THIS run's TableEntity row.

        Two coexisting entity rows for the fact (None-run with no time axis,
        run-A with one): a run-A ctx reads run-A's judgment, and the dim's
        None-run entity is invisible under run-A so nothing is flagged.
        """
        seeded = _seed(session)  # None-run fact entity has time_column=None
        fact: Table = seeded["fact"]
        session.add(
            TableEntity(
                table_id=fact.table_id,
                run_id="run-A",
                detected_entity_type="transaction",
                time_columns=_axes("booking_date"),
                detection_source="llm",
                confidence=0.9,
            )
        )
        session.flush()

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id], run_id="run-A"), [fact]
        )

        (table_data,) = data["tables"]
        assert [tc["column"] for tc in table_data["time_columns"]] == ["booking_date"]
        # The dim's entity is None-run — out of scope for run-A, so the
        # enriched date column is not flagged either.
        by_name = _columns_by_name(table_data)
        assert by_name["invoice_id__date"]["is_dimension_time_column"] is False


def _mock_llm_config() -> MagicMock:
    """Mock LLM config passing every gate in ``_run``.

    ``providers`` must be a REAL dict (``.get`` is called on it);
    ``features.slicing_analysis`` / ``.enabled`` are truthy MagicMock attributes.
    """
    config = MagicMock()
    config.active_provider = "anthropic"
    config.providers = {"anthropic": MagicMock()}
    return config


def _analysis_result(time_columns: dict[str, str]) -> Result[SlicingAnalysisResult]:
    """A successful agent result carrying only time-axis judgments.

    Empty recommendations keep ``_run`` from writing SliceDefinition rows, and
    the single-fact-table contexts below keep ``_propagate_enriched_dimensions``
    a no-op (it needs >= 2 tables) — no mocked-agent internals can leak into rows.
    """
    return Result.ok(SlicingAnalysisResult(recommendations=[], time_columns=time_columns))


@patch("dataraum.pipeline.phases.slicing_phase.SlicingAgent")
@patch("dataraum.pipeline.phases.slicing_phase.PromptRenderer")
@patch("dataraum.pipeline.phases.slicing_phase.create_provider")
@patch("dataraum.pipeline.phases.slicing_phase.load_llm_config")
class TestRunTimeAxisFill:
    """Part B: the post-analysis ``TableEntity.time_column`` fill in ``_run``.

    The LLM boundary is mocked at the phase module import site. Every test
    asserts ``analyze`` was reached: an unpatched ``load_llm_config`` raises
    FileNotFoundError and ``_run`` returns success("skipped") without the fill —
    asserting status alone would be vacuously green.
    """

    def test_happy_fill_lands_in_table_entity(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """An agent-judged enriched column fills the entity's None time_column."""
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result(
            {"invoices": "invoice_id__date"}
        )
        seeded = _seed(session)

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["invoice_id__date"]
        assert any(e["event"] == "time_axis_filled" and e["table"] == "invoices" for e in logs)
        # Empty recommendations — nothing mocked landed in SliceDefinition rows.
        assert session.execute(select(SliceDefinition)).scalars().all() == []

    def test_high_cardinality_time_axis_survives_prefilter(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """A real date axis is high-cardinality and prompt-prefiltered — fill still lands.

        ``_pre_filter_columns`` drops ``distinct_count > 200`` columns as
        slice-DIMENSION candidates, and a time axis is exactly such a column.
        Validating the agent's choice against the filtered list deterministically
        rejected every real enriched date axis (the live DAT-491 false-reject:
        ``journal_lines`` ← ``entry_id__date``); the check must run against the
        unfiltered universe instead.
        """
        from dataraum.analysis.statistics.db_models import StatisticalProfile

        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result(
            {"invoices": "invoice_id__date"}
        )
        seeded = _seed(session)
        date_col = session.execute(
            select(Column).where(Column.column_name == "invoice_id__date")
        ).scalar_one()
        session.add(
            StatisticalProfile(
                column_id=date_col.column_id,
                total_count=300,
                null_count=0,
                distinct_count=300,
                null_ratio=0.0,
                cardinality_ratio=1.0,
                profile_data={},
            )
        )
        session.flush()

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["invoice_id__date"]
        assert any(e["event"] == "time_axis_filled" for e in logs)
        assert not any(e["event"] == "time_axis_unknown_column" for e in logs)

    def test_hallucinated_column_is_rejected(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """A column name absent from the prompt's context never lands."""
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result(
            {"invoices": "ghost__col"}
        )
        seeded = _seed(session)

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        # The hallucinated agent pick is rejected by the RI check (never lands)...
        assert any(
            e["event"] == "time_axis_unknown_column" and e["table"] == "invoices" for e in logs
        )
        assert not any(e["event"] == "time_axis_filled" for e in logs)
        # ...but the deterministic is_dimension_time_column backstop (DAT-720) still
        # fills the real axis, so a flagged fact never silently goes empty.
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["invoice_id__date"]
        assert any(
            e["event"] == "time_axis_filled_deterministic" and e["table"] == "invoices"
            for e in logs
        )

    def test_existing_time_column_is_inherited_never_overridden(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """A pre-set entity time_column survives; neither fill event fires."""
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result(
            {"invoices": "invoice_id__date"}
        )
        seeded = _seed(session, fact_time_column="booking_date")

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["booking_date"]
        assert not any(e["event"] == "time_axis_filled" for e in logs)
        assert not any(e["event"] == "time_axis_unknown_column" for e in logs)

    def test_unknown_table_name_is_ignored(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """A judgment for a table outside the run neither crashes nor lands."""
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result(
            {"phantom": "invoice_id__date"}
        )
        seeded = _seed(session)

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        # The phantom-table judgment lands nowhere (no agent fill, no RI reject)...
        assert not any(e["event"] == "time_axis_filled" for e in logs)
        assert not any(e["event"] == "time_axis_unknown_column" for e in logs)
        # ...and the deterministic backstop (DAT-720) still fills invoices' flagged axis.
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["invoice_id__date"]
        assert any(
            e["event"] == "time_axis_filled_deterministic" and e["table"] == "invoices"
            for e in logs
        )

    def test_deterministic_backstop_fills_when_agent_returns_empty(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """DAT-720: the slice agent returned empty time_columns (the Sonnet 5
        effort:low degradation that silently disabled the structural stock/flow
        witness). The deterministic is_dimension_time_column backstop fills the
        axis anyway — the witness can no longer go inert on an LLM miss."""
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        seeded = _seed(session)

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        # No agent judgment landed, yet the flagged axis is filled deterministically.
        assert not any(e["event"] == "time_axis_filled" for e in logs)
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["invoice_id__date"]
        assert any(
            e["event"] == "time_axis_filled_deterministic" and e["table"] == "invoices"
            for e in logs
        )

    def test_deterministic_backstop_reads_prefilter_stash_not_filtered_columns(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """DAT-720: the backstop must read the pre-filter axis stash, not the
        prompt-filtered columns. A real date axis is high-cardinality, so
        ``_pre_filter_columns`` drops it from ``context_data["tables"]`` (the
        ``distinct_count > 200`` slice-dimension cut). The FIRST fix read those
        filtered columns and fired 0×; the stash (``dimension_time_axes``, built
        before the filter) is what makes the deterministic fill survive. With the
        agent returning empty AND the axis pre-filtered, the backstop must still
        fill it — this guards against a regression to reading the filtered list.
        """
        from dataraum.analysis.statistics.db_models import StatisticalProfile

        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        seeded = _seed(session)
        date_col = session.execute(
            select(Column).where(Column.column_name == "invoice_id__date")
        ).scalar_one()
        session.add(
            StatisticalProfile(
                column_id=date_col.column_id,
                total_count=300,
                null_count=0,
                distinct_count=300,  # > 200 → dropped from context_data["tables"]
                null_ratio=0.0,
                cardinality_ratio=1.0,
                profile_data={},
            )
        )
        session.flush()

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        assert not any(e["event"] == "time_axis_filled" for e in logs)
        assert [tc["column"] for tc in seeded["fact_entity"].time_columns] == ["invoice_id__date"]
        assert any(
            e["event"] == "time_axis_filled_deterministic" and e["table"] == "invoices"
            for e in logs
        )


@patch("dataraum.pipeline.phases.slicing_phase.SlicingAgent")
@patch("dataraum.pipeline.phases.slicing_phase.PromptRenderer")
@patch("dataraum.pipeline.phases.slicing_phase.create_provider")
@patch("dataraum.pipeline.phases.slicing_phase.load_llm_config")
class TestSliceDefinitionWriterIdempotent:
    """The SliceDefinition writer is a form-(a) upsert (DAT-502).

    One definition per ``(table_id, column_name, run_id)``: the batch dedups
    in-place (the agent can emit a dimension twice) and a redelivered ``_run``
    under the SAME run_id converges instead of duplicating. Prior runs'
    definitions coexist untouched.
    """

    @staticmethod
    def _result_with_recs(
        seeded: dict[str, Any], confidence: float
    ) -> Result[SlicingAnalysisResult]:
        from dataraum.analysis.slicing.models import SliceRecommendation

        rec = SliceRecommendation(
            table_id=seeded["fact"].table_id,
            table_name="invoices",
            column_id=seeded["fk_col"].column_id,
            column_name="invoice_id__status",
            slice_priority=1,
            distinct_values=["open", "paid"],
            value_count=2,
            reasoning="status partitions",
            confidence=confidence,
        )
        # The same dimension twice in one batch (agent duplicate) — the
        # in-batch dedup must keep one row (last wins).
        return Result.ok(
            SlicingAnalysisResult(
                recommendations=[rec, rec.model_copy(update={"confidence": confidence})],
                time_columns={},
            )
        )

    def test_redelivery_same_run_converges(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """Re-running the phase with the SAME run_id converges via the skip-guard.

        This pins the in-run KEEP-class skip-guard path: the second `_run`
        short-circuits before the LLM (this run already sliced the table), so
        convergence is by skip. The SliceDefinition UNIQUE + ``upsert()``
        DB-grain backstop is pinned separately by
        ``test_upsert_converges_on_redelivery`` (the skip-guard short-circuits
        before the upsert here, so this test never exercises it).
        """
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)
        # The seeded entities are None-run; this test pins the writer, so a
        # run-stamped context is fine (the time-axis fill is exercised above).
        mock_agent_cls.return_value.analyze.return_value = self._result_with_recs(seeded, 0.8)

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert len(rows) == 1, "in-batch duplicate deduped"
        assert rows[0].confidence == 0.8

        # The at-least-once redelivery: same run_id. The KEEP-class in-run
        # guard (this run already sliced the table) short-circuits before the
        # LLM — convergence by skip, with the UNIQUE as the DB-grain backstop.
        mock_agent_cls.return_value.analyze.return_value = self._result_with_recs(seeded, 0.9)
        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()
        session.expire_all()

        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert len(rows) == 1, "converged — no duplicate under the same run_id"
        assert rows[0].run_id == "run-A"
        assert rows[0].confidence == 0.8, "redelivery skipped re-derivation (in-run guard)"

    def test_upsert_converges_on_redelivery(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
    ) -> None:
        """The DB-grain backstop: the writer's ``upsert`` path itself converges.

        The skip-guard short-circuits before the upsert in the phase, so the
        idempotency test above never exercises the UNIQUE + ON CONFLICT. This
        test bypasses the guard and drives the writer's ``upsert`` directly
        (same model + ``index_elements`` as ``SlicingPhase._run``), proving
        ``UNIQUE(table_id, column_name, run_id)`` + ON CONFLICT DO UPDATE
        actually collapses a redelivered batch to one converged row. The LLM
        mocks are injected by the class-level patches but go unused here.
        """
        seeded = _seed(session)

        def _row(confidence: float) -> dict[str, Any]:
            return {
                "run_id": "run-A",
                "table_id": seeded["fact"].table_id,
                "column_id": seeded["fk_col"].column_id,
                "column_name": "invoice_id__status",
                "slice_priority": 1,
                "slice_type": "categorical",
                "distinct_values": ["open", "paid"],
                "value_count": 2,
                "reasoning": "status partitions",
                "confidence": confidence,
                "detection_source": "llm",
            }

        upsert(
            session,
            SliceDefinition,
            [_row(0.8)],
            index_elements=["table_id", "column_name", "run_id"],
        )
        session.commit()

        # At-least-once redelivery: same key, a changed value (confidence).
        upsert(
            session,
            SliceDefinition,
            [_row(0.9)],
            index_elements=["table_id", "column_name", "run_id"],
        )
        session.commit()
        session.expire_all()

        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert len(rows) == 1, "UNIQUE + upsert collapsed the redelivery to one row"
        assert rows[0].run_id == "run-A"
        assert rows[0].confidence == 0.9, (
            "the redelivered batch's value won (ON CONFLICT DO UPDATE)"
        )

    def test_prior_run_untouched(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """A new run's definitions coexist with a prior run's (no clear)."""
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)

        for run_id, confidence in (("run-A", 0.8), ("run-B", 0.7)):
            mock_agent_cls.return_value.analyze.return_value = self._result_with_recs(
                seeded, confidence
            )
            result = SlicingPhase()._run(
                _ctx(session, duckdb_conn, [seeded["fact"].table_id], run_id)
            )
            assert result.status == PhaseStatus.COMPLETED
            session.commit()

        session.expire_all()
        rows = session.execute(select(SliceDefinition)).scalars().all()
        by_run = {r.run_id: r for r in rows}
        assert set(by_run) == {"run-A", "run-B"}
        assert by_run["run-A"].confidence == 0.8, "prior run untouched"
        assert by_run["run-B"].confidence == 0.7


@patch("dataraum.pipeline.phases.slicing_phase.SlicingAgent")
@patch("dataraum.pipeline.phases.slicing_phase.PromptRenderer")
@patch("dataraum.pipeline.phases.slicing_phase.create_provider")
@patch("dataraum.pipeline.phases.slicing_phase.load_llm_config")
class TestReferencedDimensionIdentity:
    """DAT-756: each slice resolves its referenced-dimension identity at write.

    An enriched slice (``column_id`` is the fact's FK column) resolves
    ``(dimension_table_id, dimension_attribute, fk_role)`` from the enriched view's
    grain-safe relationship provenance — NOT from ``column_name``. A folded slice
    (an own categorical column with no grain-safe FK) resolves a null identity: it
    has no cross-table dimension identity in Phase A and abstains from conformed
    pairing (that residual is DAT-757).
    """

    @staticmethod
    def _rec(table_id: str, column_id: str, column_name: str) -> Result[SlicingAnalysisResult]:
        from dataraum.analysis.slicing.models import SliceRecommendation

        rec = SliceRecommendation(
            table_id=table_id,
            table_name="invoices",
            column_id=column_id,
            column_name=column_name,
            slice_priority=1,
            distinct_values=["a", "b"],
            value_count=2,
            reasoning="partitions",
            confidence=0.9,
        )
        return Result.ok(SlicingAnalysisResult(recommendations=[rec], time_columns={}))

    def test_enriched_slice_resolves_dim_table_identity(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """The FK-backed slice resolves (dim table, attribute=suffix, fk_role=prefix)."""
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)
        mock_agent_cls.return_value.analyze.return_value = self._rec(
            seeded["fact"].table_id, seeded["fk_col"].column_id, "invoice_id__status"
        )

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        mock_agent_cls.return_value.analyze.assert_called_once()
        session.commit()

        row = session.execute(
            select(SliceDefinition).where(SliceDefinition.column_name == "invoice_id__status")
        ).scalar_one()
        assert row.dimension_table_id == seeded["dim"].table_id
        assert row.dimension_attribute == "status"
        assert row.fk_role == "invoice_id"

    def test_folded_slice_resolves_null_identity(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """An own categorical column with no grain-safe FK resolves no identity."""
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)
        region = Column(
            table_id=seeded["fact"].table_id,
            column_name="region",
            column_position=9,
            resolved_type="VARCHAR",
        )
        session.add(region)
        session.flush()
        mock_agent_cls.return_value.analyze.return_value = self._rec(
            seeded["fact"].table_id, region.column_id, "region"
        )

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        row = session.execute(
            select(SliceDefinition).where(SliceDefinition.column_name == "region")
        ).scalar_one()
        assert row.dimension_table_id is None
        assert row.dimension_attribute is None
        assert row.fk_role is None

    def test_slice_by_fk_key_itself_has_null_attribute(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """Slicing directly by the FK key (no ``__`` enriched suffix) resolves the dim
        table with a NULL attribute and fk_role = the column name — not a folded slice
        (it still carries a referenced identity)."""
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)
        mock_agent_cls.return_value.analyze.return_value = self._rec(
            seeded["fact"].table_id, seeded["fk_col"].column_id, "invoice_id"
        )

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        row = session.execute(
            select(SliceDefinition).where(SliceDefinition.column_name == "invoice_id")
        ).scalar_one()
        assert row.dimension_table_id == seeded["dim"].table_id
        assert row.dimension_attribute is None
        assert row.fk_role == "invoice_id"
