"""Unit tests for the DAT-491 time-axis legs of the slicing phase.

Part A pins ``SlicingPhase._build_context_data``: each table dict carries the
``time_column`` read from THIS run's ``TableEntity``, and enriched dimension
entries are flagged ``is_dimension_time_column`` strictly via the enriched
view's relationship provenance (FK column -> dimension table -> that table's
``TableEntity.time_column``), never by name inference.

Part B pins the post-analysis ``TableEntity.time_column`` fill in ``_run``,
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
from tests.conftest import baseline_session_id


def _seed(
    session: Session,
    *,
    dim_time_column: str | None = "date",
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
    Entities are seeded with ``run_id=None`` to match a None-run PhaseContext.
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
        session_id=baseline_session_id(),
        table_id=fact.table_id,
        run_id=None,
        detected_entity_type="transaction",
        time_column=fact_time_column,
        detection_source="llm",
        confidence=0.9,
    )
    dim_entity = TableEntity(
        session_id=baseline_session_id(),
        table_id=dim.table_id,
        run_id=None,
        detected_entity_type="document",
        time_column=dim_time_column,
        detection_source="llm",
        confidence=0.9,
    )
    session.add_all([fact_entity, dim_entity])

    rel = Relationship(
        session_id=baseline_session_id(),
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
        session_id=baseline_session_id(),
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
    """Source-free ctx for the slicing phase, scoped by ``table_ids`` (DAT-401)."""
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        session_id=baseline_session_id(),
        run_id=run_id,
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
        assert table_data["time_column"] is None

        by_name = _columns_by_name(table_data)
        date_entry = by_name["invoice_id__date"]
        assert date_entry["is_dimension_time_column"] is True
        assert date_entry["is_enriched_dimension"] is True
        # column_id falls back to the FK column id (dim cols are not
        # individually registered on the fact).
        assert date_entry["column_id"] == seeded["fk_col"].column_id
        # Sibling dim column whose suffix is NOT the dim's time_column.
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
                session_id=baseline_session_id(),
                table_id=fact.table_id,
                run_id="run-A",
                detected_entity_type="transaction",
                time_column="booking_date",
                detection_source="llm",
                confidence=0.9,
            )
        )
        session.flush()

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id], run_id="run-A"), [fact]
        )

        (table_data,) = data["tables"]
        assert table_data["time_column"] == "booking_date"
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
    return Result.ok(
        SlicingAnalysisResult(recommendations=[], slice_queries=[], time_columns=time_columns)
    )


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
        assert seeded["fact_entity"].time_column == "invoice_id__date"
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

        ``_pre_filter_columns`` drops ``distinct_count > 50`` columns as
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
        assert seeded["fact_entity"].time_column == "invoice_id__date"
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
        assert seeded["fact_entity"].time_column is None
        assert any(
            e["event"] == "time_axis_unknown_column" and e["table"] == "invoices" for e in logs
        )
        assert not any(e["event"] == "time_axis_filled" for e in logs)

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
        assert seeded["fact_entity"].time_column == "booking_date"
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
        assert seeded["fact_entity"].time_column is None
        assert not any(e["event"] == "time_axis_filled" for e in logs)
        assert not any(e["event"] == "time_axis_unknown_column" for e in logs)
