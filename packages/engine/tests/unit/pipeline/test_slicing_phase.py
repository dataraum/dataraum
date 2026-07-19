"""Unit tests for the slicing phase.

Part A pins ``SlicingPhase._build_context_data``: each table dict carries the
``time_columns`` read from THIS run's ``TableEntity`` (DAT-565 plural), and
enriched dimension entries are flagged ``is_dimension_time_column`` strictly via
the enriched view's relationship provenance (FK column -> dimension table ->
membership in that table's ``TableEntity.time_columns``), never by name inference.

Part B pins the post-analysis ``TableEntity.time_columns`` fill in ``_run``,
with the LLM boundary mocked at the phase module import site (the
``should_skip`` gates live in ``tests/integration/pipeline/test_slicing_phase.py``).

Part C pins the DAT-725 rescope: slice EXISTENCE is deterministic (the eligible
set — grain-safe pre-filter survivors that are not measures/timestamps — is the
persisted inventory), and the agent output is enrichment only (priority /
context / reasoning / confidence on rows that exist regardless).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session
from structlog.testing import capture_logs

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import UNRANKED_SLICE_PRIORITY, SlicingAnalysisResult
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.slicing_phase import SlicingPhase
from dataraum.storage import Column, Source, Table
from dataraum.storage.upsert import upsert
from tests.conftest import baseline_run_id


def _axes(column: str | None) -> list[dict[str, object]]:
    """Plural ``time_columns`` JSON for a single named event axis (DAT-565/780), or empty."""
    return (
        [
            {
                "column": column,
                "aspect": "event",
                "role": "event",
                "is_anchor": True,
                "note": "seed axis.",
            }
        ]
        if column
        else []
    )


def _seed(
    session: Session,
    *,
    dim_time_column: str | None = "date",
    dim_axes: list[dict[str, Any]] | None = None,
    fact_time_column: str | None = None,
    fact_axes: list[dict[str, Any]] | None = None,
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
    dim_date = Column(
        table_id=dim.table_id, column_name="date", column_position=1, resolved_type="DATE"
    )
    dim_status = Column(
        table_id=dim.table_id, column_name="status", column_position=2, resolved_type="VARCHAR"
    )
    session.add_all([dim_pk, dim_date, dim_status])
    session.flush()

    # Semantic roles (DAT-725 existence gate): the fact's measure and the dim's
    # event DATE are excluded from the deterministic inventory (measure/timestamp);
    # the dim's status is a dimension (enriched ``invoice_id__status`` eligible via
    # provenance); ``invoice_id`` stays UNannotated — fail-open eligible, and the
    # referenced-key path. Default eligible set: {invoice_id, invoice_id__status}.
    session.add_all(
        [
            SemanticAnnotation(
                column_id=amount_col.column_id, run_id=None, semantic_role="measure"
            ),
            SemanticAnnotation(
                column_id=dim_date.column_id, run_id=None, semantic_role="timestamp"
            ),
            SemanticAnnotation(
                column_id=dim_status.column_id, run_id=None, semantic_role="dimension"
            ),
        ]
    )
    session.flush()

    fact_entity = TableEntity(
        table_id=fact.table_id,
        run_id=None,
        detected_entity_type="transaction",
        time_columns=fact_axes if fact_axes is not None else _axes(fact_time_column),
        detection_source="llm",
    )
    dim_entity = TableEntity(
        table_id=dim.table_id,
        run_id=None,
        detected_entity_type="document",
        time_columns=dim_axes if dim_axes is not None else _axes(dim_time_column),
        detection_source="llm",
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
                origin="dimension",
            ),
            Column(
                table_id=view_table.table_id,
                column_name="invoice_id__status",
                column_position=1,
                resolved_type="VARCHAR",
                origin="dimension",
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


class TestPreFilterColumns:
    """DAT-805: the slice-candidate gate excludes ONLY the definitive extremes —
    constant, majority-NULL, near-unique key — on a scale-invariant near-key
    FRACTION of rows, never an absolute distinct count. Mid-cardinality columns
    (the recall the old ``distinct > 200`` / ``cardinality_ratio > 0.5`` cuts
    silently killed) survive to the LLM; the near-key check is uniform (no
    enriched exemption).
    """

    @staticmethod
    def _survivors(*columns: dict[str, Any]) -> set[str]:
        ctx: dict[str, Any] = {"tables": [{"table_name": "t", "columns": list(columns)}]}
        SlicingPhase()._pre_filter_columns(ctx)
        return {c["column_name"] for c in ctx["tables"][0]["columns"]}

    def test_high_distinct_low_ratio_discriminator_survives(self) -> None:
        # Recall recovery: 5000 distinct in a 10M-row table (ratio ~0) is a fine
        # discriminator the old absolute ``distinct > 200`` cut wrongly dropped.
        assert self._survivors(
            {
                "column_name": "region_code",
                "distinct_count": 5000,
                "cardinality_ratio": 0.0005,
                "null_ratio": 0.0,
            }
        ) == {"region_code"}

    def test_mid_cardinality_survives(self) -> None:
        # 0.6 is well below near-unique; the old ``cardinality_ratio > 0.5`` cut
        # silently killed it. Downstream folds thin support.
        assert self._survivors(
            {
                "column_name": "sub_segment",
                "distinct_count": 60,
                "cardinality_ratio": 0.6,
                "null_ratio": 0.1,
            }
        ) == {"sub_segment"}

    def test_near_unique_key_excluded(self) -> None:
        assert (
            self._survivors(
                {
                    "column_name": "txn_ref",
                    "distinct_count": 9500,
                    "cardinality_ratio": 0.95,
                    "null_ratio": 0.0,
                }
            )
            == set()
        )

    def test_constant_excluded(self) -> None:
        assert (
            self._survivors(
                {
                    "column_name": "only_value",
                    "distinct_count": 1,
                    "cardinality_ratio": 0.0001,
                    "null_ratio": 0.0,
                }
            )
            == set()
        )

    def test_majority_null_excluded(self) -> None:
        assert (
            self._survivors(
                {
                    "column_name": "sparse",
                    "distinct_count": 10,
                    "cardinality_ratio": 0.02,
                    "null_ratio": 0.7,
                }
            )
            == set()
        )

    def test_near_unique_enriched_column_also_excluded(self) -> None:
        # No enriched exemption on the near-key check: a raw enriched date axis is
        # near-unique and just as useless a slice as an own near-key.
        assert (
            self._survivors(
                {
                    "column_name": "invoice_id__date",
                    "is_enriched_dimension": True,
                    "distinct_count": 300,
                    "cardinality_ratio": 1.0,
                    "null_ratio": 0.0,
                }
            )
            == set()
        )

    def test_low_cardinality_enriched_dimension_survives(self) -> None:
        assert self._survivors(
            {
                "column_name": "account_id__account_type",
                "is_enriched_dimension": True,
                "distinct_count": 6,
                "cardinality_ratio": 0.0001,
                "null_ratio": 0.0,
            }
        ) == {"account_id__account_type"}

    def test_missing_stats_pass_through(self) -> None:
        # No profile (None stats) — the gate can't judge, so the column is served.
        assert self._survivors({"column_name": "unprofiled"}) == {"unprofiled"}

    def test_dropped_column_preserved_in_snapshot(self) -> None:
        # DAT-491: a dropped near-key date axis stays resolvable for the time-axis
        # validation via the pre-filter ``col_id_by_name`` snapshot.
        ctx: dict[str, Any] = {
            "tables": [
                {
                    "table_name": "t",
                    "columns": [
                        {
                            "column_name": "entry_date",
                            "column_id": "c1",
                            "distinct_count": 400,
                            "cardinality_ratio": 1.0,
                            "null_ratio": 0.0,
                        }
                    ],
                }
            ]
        }
        SlicingPhase()._pre_filter_columns(ctx)
        assert ctx["tables"][0]["columns"] == []
        assert ctx["tables"][0]["col_id_by_name"] == {"entry_date": "c1"}

    def test_null_coded_binary_survives(self) -> None:
        # {value, NULL} is a valid 2-way split — distinct_count is null-blind (=1),
        # but the NULL bucket makes it a real dimension (not a constant).
        assert self._survivors(
            {
                "column_name": "flag",
                "distinct_count": 1,
                "null_count": 400,
                "cardinality_ratio": 0.0001,
                "null_ratio": 0.4,
            }
        ) == {"flag"}

    def test_true_constant_no_nulls_excluded(self) -> None:
        assert (
            self._survivors(
                {
                    "column_name": "only_value",
                    "distinct_count": 1,
                    "null_count": 0,
                    "cardinality_ratio": 0.0001,
                    "null_ratio": 0.0,
                }
            )
            == set()
        )

    def test_near_key_fraction_boundary(self) -> None:
        # >= 0.9 drops; just below survives.
        assert (
            self._survivors(
                {
                    "column_name": "a",
                    "distinct_count": 90,
                    "cardinality_ratio": 0.9,
                    "null_ratio": 0.0,
                }
            )
            == set()
        )
        assert self._survivors(
            {"column_name": "b", "distinct_count": 89, "cardinality_ratio": 0.89, "null_ratio": 0.0}
        ) == {"b"}

    def test_null_ratio_boundary(self) -> None:
        # > 0.5 drops; exactly 0.5 survives.
        assert self._survivors(
            {"column_name": "a", "distinct_count": 5, "cardinality_ratio": 0.01, "null_ratio": 0.5}
        ) == {"a"}
        assert (
            self._survivors(
                {
                    "column_name": "b",
                    "distinct_count": 5,
                    "cardinality_ratio": 0.01,
                    "null_ratio": 0.51,
                }
            )
            == set()
        )

    def test_distinct_floor_boundary(self) -> None:
        # distinct 2 survives; distinct 1 (no NULL) is a constant.
        assert self._survivors(
            {
                "column_name": "a",
                "distinct_count": 2,
                "null_count": 0,
                "cardinality_ratio": 0.01,
                "null_ratio": 0.0,
            }
        ) == {"a"}
        assert (
            self._survivors(
                {
                    "column_name": "b",
                    "distinct_count": 1,
                    "null_count": 0,
                    "cardinality_ratio": 0.01,
                    "null_ratio": 0.0,
                }
            )
            == set()
        )

    def test_exclusions_are_born_loud(self) -> None:
        # Every drop emits slice_column_excluded with its reason (no silent debug).
        ctx: dict[str, Any] = {
            "tables": [
                {
                    "table_name": "t",
                    "columns": [
                        {
                            "column_name": "k",
                            "distinct_count": 100,
                            "null_count": 0,
                            "cardinality_ratio": 0.99,
                            "null_ratio": 0.0,
                        },
                        {
                            "column_name": "c",
                            "distinct_count": 1,
                            "null_count": 0,
                            "cardinality_ratio": 0.001,
                            "null_ratio": 0.0,
                        },
                        {
                            "column_name": "n",
                            "distinct_count": 5,
                            "null_count": 0,
                            "cardinality_ratio": 0.05,
                            "null_ratio": 0.8,
                        },
                    ],
                }
            ]
        }
        with capture_logs() as logs:
            SlicingPhase()._pre_filter_columns(ctx)
        excl = {(e["column"], e["reason"]) for e in logs if e["event"] == "slice_column_excluded"}
        assert excl == {("k", "near_key"), ("c", "constant"), ("n", "mostly_null")}


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
                {"column": "other", "aspect": "x", "role": "event", "is_anchor": True, "note": "n"},
                {
                    "column": "date",
                    "aspect": "event",
                    "role": "event",
                    "is_anchor": False,
                    "note": "n",
                },
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

    def test_attribute_role_dim_date_is_not_flagged(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-780: a dim's ATTRIBUTE-role date (valid_until) whose suffix matches an
        enriched column must NOT be flagged is_dimension_time_column — otherwise the
        deterministic backstop would promote it to a false event axis on the fact."""
        seeded = _seed(
            session,
            dim_axes=[
                {
                    "column": "date",
                    "aspect": "due",
                    "role": "attribute",
                    "is_anchor": False,
                    "note": "A date the row refers to, not an event.",
                }
            ],
        )
        fact: Table = seeded["fact"]

        data = SlicingPhase()._build_context_data(
            _ctx(session, duckdb_conn, [fact.table_id]), [fact]
        )

        by_name = _columns_by_name(data["tables"][0])
        assert by_name["invoice_id__date"]["is_dimension_time_column"] is False

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

    Empty recommendations = an un-ranked inventory (DAT-725): ``_run`` still
    persists the deterministic eligible set, but every row is 'structural' at the
    priority floor — no mocked-agent internals can leak into enrichment fields.
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
        # Empty recommendations — the deterministic inventory still lands (DAT-725),
        # but every row is structural at the floor: no mocked enrichment leaked.
        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert rows, "the eligible set is persisted regardless of the ranking"
        assert all(r.detection_source == "structural" for r in rows)
        assert all(r.slice_priority == UNRANKED_SLICE_PRIORITY for r in rows)

    def test_high_cardinality_time_axis_survives_prefilter(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """A real date axis is near-unique and prompt-prefiltered — fill still lands.

        ``_pre_filter_columns`` drops near-unique columns (``cardinality_ratio >=
        _NEAR_KEY_FRAC``) as slice-DIMENSION candidates, and a raw date axis is
        exactly such a column. Validating the agent's choice against the filtered
        list deterministically rejected every real enriched date axis (the live
        DAT-491 false-reject: ``journal_lines`` ← ``entry_id__date``); the check
        must run against the unfiltered universe instead.
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
                layer="enriched",
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

    def test_attribute_only_fact_still_gets_backstop_axis_preserving_attribute(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """DAT-780: a fact whose only date is attribute-role still gets the event-axis
        backstop, and the attribute date is preserved (coverage, not an event axis).

        Pre-DAT-780 the guard was bare truthiness; an attribute-only list would
        suppress the backstop. The guard now tests role='event', and the fill
        appends rather than clobbers.
        """
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result(
            {"invoices": "invoice_id__date"}
        )
        seeded = _seed(
            session,
            fact_axes=[
                {
                    "column": "due_date",
                    "aspect": "due",
                    "role": "attribute",
                    "is_anchor": False,
                    "note": "A date the row refers to.",
                }
            ],
        )

        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))

        assert result.status == PhaseStatus.COMPLETED
        axes = seeded["fact_entity"].time_columns
        # The attribute date survives; the backstop appended the event axis + anchor.
        assert [tc["column"] for tc in axes] == ["due_date", "invoice_id__date"]
        assert [tc["role"] for tc in axes] == ["attribute", "event"]
        assert [tc["is_anchor"] for tc in axes] == [False, True]
        assert any(e["event"] == "time_axis_filled" for e in logs)

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
        prompt-filtered columns. A real date axis is near-unique, so
        ``_pre_filter_columns`` drops it from ``context_data["tables"]`` (the
        near-key slice-dimension cut, ``cardinality_ratio >= _NEAR_KEY_FRAC``). The
        FIRST fix read those filtered columns and fired 0×; the stash
        (``dimension_time_axes``, built before the filter) is what makes the
        deterministic fill survive. With the agent returning empty AND the axis
        pre-filtered, the backstop must still fill it — this guards against a
        regression to reading the filtered list.
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
                layer="enriched",
                total_count=300,
                null_count=0,
                distinct_count=300,
                null_ratio=0.0,
                cardinality_ratio=1.0,  # near-unique → dropped from context_data["tables"]
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
        # The eligible set (invoice_id + invoice_id__status), the ranked column
        # ONCE — the in-batch duplicate deduped into the single inventory row.
        assert sorted(r.column_name or "" for r in rows) == ["invoice_id", "invoice_id__status"]
        (ranked_row,) = [r for r in rows if r.column_name == "invoice_id__status"]
        assert ranked_row.confidence == 0.8

        # The at-least-once redelivery: same run_id. The KEEP-class in-run
        # guard (this run already sliced the table) short-circuits before the
        # LLM — convergence by skip, with the UNIQUE as the DB-grain backstop.
        mock_agent_cls.return_value.analyze.return_value = self._result_with_recs(seeded, 0.9)
        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()
        session.expire_all()

        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert sorted(r.column_name or "" for r in rows) == [
            "invoice_id",
            "invoice_id__status",
        ], "converged — no duplicate under the same run_id"
        (ranked_row,) = [r for r in rows if r.column_name == "invoice_id__status"]
        assert ranked_row.run_id == "run-A"
        assert ranked_row.confidence == 0.8, "redelivery skipped re-derivation (in-run guard)"

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
        ranked_rows = (
            session.execute(
                select(SliceDefinition).where(SliceDefinition.column_name == "invoice_id__status")
            )
            .scalars()
            .all()
        )
        by_run = {r.run_id: r for r in ranked_rows}
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


@patch("dataraum.pipeline.phases.slicing_phase.SlicingAgent")
@patch("dataraum.pipeline.phases.slicing_phase.PromptRenderer")
@patch("dataraum.pipeline.phases.slicing_phase.create_provider")
@patch("dataraum.pipeline.phases.slicing_phase.load_llm_config")
class TestDeterministicInventory:
    """Part C (DAT-725): existence is deterministic; the agent only enriches.

    The persisted slice set is a pure function of the data + code — the LLM
    influences priority/context/reasoning/confidence, never which rows exist.
    The default ``_seed`` eligible set is {invoice_id, invoice_id__status}:
    ``amount`` is a measure, the enriched ``invoice_id__date`` resolves the dim's
    timestamp role through provenance, both excluded deterministically.
    """

    @staticmethod
    def _ranked_status(seeded: dict[str, Any], priority: int = 1) -> Result[SlicingAnalysisResult]:
        from dataraum.analysis.slicing.models import SliceRecommendation

        rec = SliceRecommendation(
            table_id=seeded["fact"].table_id,
            table_name="invoices",
            column_id=seeded["fk_col"].column_id,
            column_name="invoice_id__status",
            slice_priority=priority,
            distinct_values=["open", "paid"],
            value_count=2,
            reasoning="status partitions",
            business_context="document lifecycle state",
            confidence=0.9,
        )
        return Result.ok(SlicingAnalysisResult(recommendations=[rec], time_columns={}))

    def test_existence_is_deterministic_across_rankings(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """Two runs with DIFFERENT agent outputs persist the SAME column set.

        run-A ranks a dimension; run-B's agent returns nothing. The pre-rescope
        election would have produced different catalogs; now only the enrichment
        fields differ — zero LLM influence on existence.
        """
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)

        mock_agent_cls.return_value.analyze.return_value = self._ranked_status(seeded)
        assert (
            SlicingPhase()
            ._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
            .status
            == PhaseStatus.COMPLETED
        )
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        assert (
            SlicingPhase()
            ._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-B"))
            .status
            == PhaseStatus.COMPLETED
        )
        session.commit()

        rows = session.execute(select(SliceDefinition)).scalars().all()
        cols_by_run: dict[str, set[str]] = {}
        for r in rows:
            cols_by_run.setdefault(r.run_id, set()).add(r.column_name or "")
        assert (
            cols_by_run["run-A"]
            == cols_by_run["run-B"]
            == {
                "invoice_id",
                "invoice_id__status",
            }
        )
        # Enrichment differs — priority/source follow the ranking, existence does not.
        a_status = next(
            r for r in rows if r.run_id == "run-A" and r.column_name == "invoice_id__status"
        )
        b_status = next(
            r for r in rows if r.run_id == "run-B" and r.column_name == "invoice_id__status"
        )
        assert (a_status.slice_priority, a_status.detection_source) == (1, "llm")
        assert (b_status.slice_priority, b_status.detection_source) == (
            UNRANKED_SLICE_PRIORITY,
            "structural",
        )

    def test_folded_key_without_fk_survives(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """THE TRAP: a low-cardinality KEY with no resolved FK is persisted.

        A folded dimension key (account_id inlined on a fact grain) is precisely
        a ``key`` with no FK — the pre-rescope election dropped it 0-2 times per
        run, and an "exclude keys without FK" gate would re-open that hole. It
        must land as a folded slice (null identity), unranked or not.
        """
        from dataraum.analysis.statistics.db_models import StatisticalProfile

        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        seeded = _seed(session)
        account = Column(
            table_id=seeded["fact"].table_id,
            column_name="account_id",
            column_position=5,
            resolved_type="VARCHAR",
        )
        session.add(account)
        session.flush()
        session.add_all(
            [
                SemanticAnnotation(column_id=account.column_id, run_id=None, semantic_role="key"),
                # Low-cardinality: 27 distinct over 25k rows — sails through the
                # near-key gate; only degenerate PKs die there.
                StatisticalProfile(
                    column_id=account.column_id,
                    layer="typed",
                    total_count=25_000,
                    null_count=0,
                    distinct_count=27,
                    null_ratio=0.0,
                    cardinality_ratio=0.00108,
                    profile_data={"top_values": [{"value": "1000"}, {"value": "1200"}]},
                ),
            ]
        )
        session.flush()

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        row = session.execute(
            select(SliceDefinition).where(SliceDefinition.column_name == "account_id")
        ).scalar_one()
        assert row.dimension_table_id is None, "folded — no referenced identity"
        assert row.fk_role is None
        assert row.detection_source == "structural"
        assert row.slice_priority == UNRANKED_SLICE_PRIORITY
        assert row.distinct_values == ["1000", "1200"], "profile top values as evidence"
        assert row.value_count == 27, "honest full distinct count"

    def test_measure_and_timestamp_roles_excluded(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """Measures and timestamps never enter the inventory — born loud.

        ``amount`` (own measure), an own timestamp, and the enriched
        ``invoice_id__date`` (the dim's timestamp, resolved through relationship
        provenance) are all excluded; the unannotated ``invoice_id`` stays
        (fail-open — no exclusion evidence).
        """
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        seeded = _seed(session)
        booked = Column(
            table_id=seeded["fact"].table_id,
            column_name="booked_at",
            column_position=6,
            resolved_type="TIMESTAMP",
        )
        session.add(booked)
        session.flush()
        session.add(
            SemanticAnnotation(column_id=booked.column_id, run_id=None, semantic_role="timestamp")
        )
        session.flush()

        with capture_logs() as logs:
            result = SlicingPhase()._run(
                _ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A")
            )
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        names = {r.column_name for r in session.execute(select(SliceDefinition)).scalars().all()}
        assert names == {"invoice_id", "invoice_id__status"}
        excluded = {
            (e["column"], e["semantic_role"])
            for e in logs
            if e["event"] == "slice_column_excluded" and e.get("reason") == "semantic_role"
        }
        assert excluded == {
            ("amount", "measure"),
            ("booked_at", "timestamp"),
            ("invoice_id__date", "timestamp"),
        }

    def test_attribute_role_is_deliberately_kept(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """The exclusion set is EXACTLY {measure, timestamp} — 'attribute' stays.

        An 'attribute' label is what a FOLDED descriptive dimension member draws
        (account_name inlined next to its key); the role is an LLM's soft
        per-table read, not a structural fact, so excluding it would re-open the
        trap in its descriptive form. Pre-registered in the DAT-725 design.
        """
        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        seeded = _seed(session)
        account_name = Column(
            table_id=seeded["fact"].table_id,
            column_name="account_name",
            column_position=7,
            resolved_type="VARCHAR",
        )
        session.add(account_name)
        session.flush()
        session.add(
            SemanticAnnotation(
                column_id=account_name.column_id, run_id=None, semantic_role="attribute"
            )
        )
        session.flush()

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        row = session.execute(
            select(SliceDefinition).where(SliceDefinition.column_name == "account_name")
        ).scalar_one()
        assert row.detection_source == "structural"

    def test_role_gate_scopes_to_promoted_generation_head(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """DAT-413 × DAT-725: coexisting annotation runs — the promoted generation
        head decides which row feeds the existence gate. Head-flip guard: with
        identical data, flipping the head flips eligibility; without run-scoping
        an arbitrary scan-order row would win and existence would flap on exactly
        the axis this rescope pins down.
        """
        from uuid import uuid4

        from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

        mock_load_config.return_value = _mock_llm_config()
        mock_agent_cls.return_value.analyze.return_value = _analysis_result({})
        seeded = _seed(session)
        amount_id = session.execute(
            select(Column.column_id).where(
                Column.table_id == seeded["fact"].table_id, Column.column_name == "amount"
            )
        ).scalar_one()
        # A second, coexisting run's annotation disagrees: 'dimension', not 'measure'.
        session.add(
            SemanticAnnotation(column_id=amount_id, run_id="alt", semantic_role="dimension")
        )
        head = MetadataSnapshotHead(
            head_id=str(uuid4()),
            target=f"table:{seeded['fact'].table_id}",
            stage=GENERATION_STAGE,
            run_id=baseline_run_id(),  # the seeded 'measure' row's autofilled run
        )
        session.add(head)
        session.flush()

        # Head at the baseline run → the 'measure' row is current → excluded.
        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        names_a = {
            r.column_name
            for r in session.execute(
                select(SliceDefinition).where(SliceDefinition.run_id == "run-A")
            ).scalars()
        }
        assert "amount" not in names_a

        # Flip the head to the 'alt' run → the 'dimension' row is current → eligible.
        head.run_id = "alt"
        session.flush()
        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-B"))
        assert result.status == PhaseStatus.COMPLETED
        names_b = {
            r.column_name
            for r in session.execute(
                select(SliceDefinition).where(SliceDefinition.run_id == "run-B")
            ).scalars()
        }
        assert "amount" in names_b

    def test_llm_config_missing_persists_inventory_and_backstop(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """No LLM config: the ranking is skipped, existence still lands (DAT-725).

        Pre-rescope this mode skipped the whole phase (zero rows), re-coupling
        existence to LLM availability. Now the deterministic inventory persists
        (all structural) and the DAT-720 time-axis backstop still fires; only
        the agent call is skipped.
        """
        mock_load_config.side_effect = FileNotFoundError("no llm.yaml")
        seeded = _seed(session)

        # Default (baseline) run ctx — the seeded entities autofill the baseline
        # run, and the DAT-720 backstop resolves THIS run's TableEntity rows.
        with capture_logs() as logs:
            result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id]))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        mock_agent_cls.return_value.analyze.assert_not_called()
        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert sorted(r.column_name or "" for r in rows) == ["invoice_id", "invoice_id__status"]
        assert all(r.detection_source == "structural" for r in rows)
        # The deterministic backstop is not gated on the ranker.
        assert any(e["event"] == "time_axis_filled_deterministic" for e in logs)
        assert any(
            e["event"] == "slice_ranking_skipped" and "config not found" in e["reason"]
            for e in logs
        )

    def test_feature_disabled_persists_inventory(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """Feature disabled: same as config-missing — inventory lands, no agent."""
        config = _mock_llm_config()
        config.features.slicing_analysis.enabled = False
        mock_load_config.return_value = config
        seeded = _seed(session)

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        mock_agent_cls.return_value.analyze.assert_not_called()
        rows = session.execute(select(SliceDefinition)).scalars().all()
        assert sorted(r.column_name or "" for r in rows) == ["invoice_id", "invoice_id__status"]
        assert all(r.slice_priority == UNRANKED_SLICE_PRIORITY for r in rows)

    def test_ranked_row_carries_enrichment_unranked_gets_floor(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_agent_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """The ranked row is 'llm' with the agent's fields; the rest floor as 'structural'."""
        mock_load_config.return_value = _mock_llm_config()
        seeded = _seed(session)
        mock_agent_cls.return_value.analyze.return_value = self._ranked_status(seeded, priority=2)

        result = SlicingPhase()._run(_ctx(session, duckdb_conn, [seeded["fact"].table_id], "run-A"))
        assert result.status == PhaseStatus.COMPLETED
        session.commit()

        by_name = {
            r.column_name: r for r in session.execute(select(SliceDefinition)).scalars().all()
        }
        ranked = by_name["invoice_id__status"]
        assert ranked.detection_source == "llm"
        assert ranked.slice_priority == 2
        assert ranked.confidence == 0.9
        assert ranked.business_context == "document lifecycle state"
        assert ranked.distinct_values == ["open", "paid"]
        floor = by_name["invoice_id"]
        assert floor.detection_source == "structural"
        assert floor.slice_priority == UNRANKED_SLICE_PRIORITY
        assert floor.confidence is None
        assert floor.reasoning is None
