"""Tests for entropy detector loader helpers."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.entropy.detectors.loaders import (
    load_correlation,
    load_semantic,
    load_statistics,
    load_typing,
)
from dataraum.storage import init_database


class TestLoadTyping:
    def test_returns_none_when_no_data(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.first.return_value = None
        assert load_typing(session, "col1") is None

    def test_returns_decision_with_candidate(self):
        session = MagicMock()
        td = MagicMock()
        td.decided_type = "DECIMAL"
        td.decision_source = "inference"
        td.decision_reason = "high confidence"

        tc = MagicMock()
        tc.confidence = 0.95
        tc.parse_success_rate = 0.99
        tc.failed_examples = ["N/A"]
        tc.detected_pattern = r"\d+\.\d+"
        tc.pattern_match_rate = 0.98
        tc.detected_unit = "USD"
        tc.unit_confidence = 0.8

        session.execute.return_value.scalars.return_value.first.side_effect = [td, tc]

        result = load_typing(session, "col1")
        assert result is not None
        assert result["resolved_type"] == "DECIMAL"
        assert result["confidence"] == 0.95
        assert result["detected_unit"] == "USD"

    def test_returns_candidate_only_when_no_decision(self):
        session = MagicMock()
        tc = MagicMock()
        tc.data_type = "INTEGER"
        tc.confidence = 0.8
        tc.parse_success_rate = 1.0
        tc.failed_examples = []
        tc.detected_pattern = None
        tc.pattern_match_rate = None
        tc.detected_unit = None
        tc.unit_confidence = None

        session.execute.return_value.scalars.return_value.first.side_effect = [None, tc]

        result = load_typing(session, "col1")
        assert result is not None
        assert result["data_type"] == "INTEGER"
        assert result["confidence"] == 0.8


class TestLoadStatistics:
    def test_returns_none_when_no_profile(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.first.return_value = None
        assert load_statistics(session, "col1") is None

    def test_returns_stats_with_quality(self):
        session = MagicMock()
        sp = MagicMock()
        sp.null_count = 5
        sp.total_count = 100
        sp.distinct_count = 80
        sp.cardinality_ratio = 0.8
        sp.profile_data = {"numeric_stats": {}}

        qm = MagicMock()
        qm.iqr_outlier_ratio = 0.02
        qm.zscore_outlier_ratio = 0.01
        qm.has_outliers = True
        qm.benford_compliant = True
        qm.quality_data = {"outlier_detection": {"iqr_outlier_count": 2}}

        session.execute.return_value.scalars.return_value.first.side_effect = [sp, qm]

        result = load_statistics(session, "col1")
        assert result is not None
        assert result["null_ratio"] == 0.05
        assert result["quality"]["benford_compliant"] is True
        assert "outlier_detection" in result["quality"]

    def test_excluded_column_omits_outlier_detection(self):
        """When outlier analysis was skipped (iqr_outlier_ratio is NULL),
        the quality dict should NOT contain outlier_detection so detectors
        return [] ('not assessed') instead of a false 0-score."""
        session = MagicMock()
        sp = MagicMock()
        sp.null_count = 0
        sp.total_count = 100
        sp.distinct_count = 50
        sp.cardinality_ratio = 0.5
        sp.profile_data = {}

        qm = MagicMock()
        qm.iqr_outlier_ratio = None
        qm.zscore_outlier_ratio = None
        qm.has_outliers = None
        qm.benford_compliant = True
        qm.quality_data = {"benford_analysis": {"is_compliant": True}}

        session.execute.return_value.scalars.return_value.first.side_effect = [sp, qm]

        result = load_statistics(session, "col1")
        assert result is not None
        assert "outlier_detection" not in result["quality"]
        assert result["quality"]["benford_compliant"] is True


class TestLoadSemantic:
    def test_returns_none_when_no_annotation(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.first.return_value = None
        assert load_semantic(session, "col1") is None

    def test_returns_semantic_dict(self):
        session = MagicMock()
        sa = MagicMock()
        sa.semantic_role = "measure"
        sa.entity_type = "monetary"
        sa.business_name = "Revenue"
        sa.business_description = "Total revenue"
        sa.confidence = 0.9
        sa.business_concept = "revenue"
        sa.unit_source_column = None

        session.execute.return_value.scalars.return_value.first.return_value = sa

        result = load_semantic(session, "col1")
        assert result is not None
        assert result["semantic_role"] == "measure"
        assert "unit_source_column" not in result

    def test_includes_unit_source_column(self):
        # unit_source_column is catalogue-grain (DAT-637): read from ColumnConcept
        # at the run, not SemanticAnnotation — the loader needs a run_id and a
        # second (ColumnConcept) query result.
        session = MagicMock()
        sa = MagicMock()
        sa.semantic_role = "measure"
        sa.entity_type = "monetary"
        sa.business_name = "Amount"
        sa.business_description = ""
        sa.confidence = 0.7
        sa.temporal_behavior_claim = None
        sa.temporal_behavior_claim_confidence = None
        cc = MagicMock()
        cc.business_concept = None
        cc.unit_source_column = "currency"
        cc.temporal_behavior = None
        cc.derived_formula_hypothesis = None
        cc.derived_formula_confidence = None

        sa_result = MagicMock()
        sa_result.scalar_one_or_none.return_value = sa
        cc_result = MagicMock()
        cc_result.scalar_one_or_none.return_value = cc
        session.execute.side_effect = [sa_result, cc_result]

        result = load_semantic(session, "col1", run_id="r1")
        assert result is not None
        assert result["unit_source_column"] == "currency"


class TestLoadCorrelation:
    def test_returns_none_when_no_derived_columns(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []
        assert load_correlation(session, "col1", "amount") is None

    def test_returns_derived_column_info(self):
        session = MagicMock()
        dc = MagicMock()
        dc.formula = "price * quantity"
        dc.match_rate = 0.98
        dc.derivation_type = "multiplication"
        dc.source_column_ids = ["col2", "col3"]

        session.execute.return_value.scalars.return_value.all.return_value = [dc]

        result = load_correlation(session, "col1", "total")
        assert result is not None
        assert len(result["derived_columns"]) == 1
        assert result["derived_columns"][0]["derived_column_name"] == "total"
        assert result["derived_columns"][0]["match_rate"] == 0.98


@pytest.fixture
def real_session():
    """In-memory SQLite session with all tables; FKs off so we skip parent rows."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        with factory() as s:
            yield s
    finally:
        engine.dispose()


class TestLoaderRunIdFilter:
    """DAT-413: the run_id-stamped loaders read only the requested run's row.

    Real-DB tests (the MagicMock tests above can't exercise the SQL filter): a
    single ``TypeDecision`` stamped with one run is returned when the loader is
    asked for that run and skipped for any other; ``run_id=None`` (non-detect
    callers) keeps the legacy unfiltered behavior.
    """

    def _insert_decision(self, session, column_id: str, run_id: str) -> None:
        from dataraum.analysis.typing.db_models import TypeDecision

        session.add(
            TypeDecision(
                decision_id=str(uuid4()),
                column_id=column_id,
                run_id=run_id,
                decided_type="DECIMAL",
                decision_source="automatic",
            )
        )
        session.flush()

    def test_load_typing_picks_matching_run(self, real_session):
        self._insert_decision(real_session, "col-1", "run-A")

        # Requested run matches the stored row → returned.
        matched = load_typing(real_session, "col-1", run_id="run-A")
        assert matched is not None
        assert matched["resolved_type"] == "DECIMAL"

        # A different run → the filter excludes the only row → None.
        assert load_typing(real_session, "col-1", run_id="run-B") is None

        # No run_id (non-detect / legacy callers) → unfiltered → still found.
        assert load_typing(real_session, "col-1") is not None


class TestLoaderHeadFallback:
    """DAT-405/448: session-detect reads fall back to the PINNED base run.

    A begin_session detect carries the SESSION run's run_id, but per-column
    analysis rows (semantic, statistics) were written by the add_source run.
    A strict this-run read returned None and silently disabled the semantic-
    gated session detectors (DAT-405). The orchestrator now resolves the
    ``(table:{id}, stage)`` snapshot heads ONCE at detect start
    (``resolve_base_runs``) and loaders consult that pin — per-call head
    resolution let a concurrent promote tear reads mid-run (DAT-448). Without
    a pin they keep returning None (never guess a run).
    """

    def _seed_column(self, session, column_id: str, table_id: str) -> None:
        from dataraum.storage import Column

        session.add(
            Column(
                column_id=column_id,
                table_id=table_id,
                column_name="amount",
                column_position=0,
                raw_type="VARCHAR",
            )
        )
        session.flush()

    def _seed_annotation(self, session, column_id: str, run_id: str, role: str = "measure") -> None:
        from dataraum.analysis.semantic.db_models import SemanticAnnotation

        session.add(
            SemanticAnnotation(
                annotation_id=str(uuid4()),
                column_id=column_id,
                run_id=run_id,
                semantic_role=role,
            )
        )
        session.flush()

    def _promote(self, session, table_id: str, run_id: str) -> None:
        from datetime import UTC, datetime

        from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

        session.add(
            MetadataSnapshotHead(
                target=f"table:{table_id}",
                stage=GENERATION_STAGE,
                run_id=run_id,
                promoted_at=datetime.now(UTC),
            )
        )
        session.flush()

    def test_semantic_falls_back_to_pinned_base_run(self, real_session):
        from dataraum.entropy.detectors.loaders import resolve_base_runs

        self._seed_column(real_session, "col-h1", "tbl-h1")
        self._seed_annotation(real_session, "col-h1", "run-addsource")
        self._promote(real_session, "tbl-h1", "run-addsource")

        # The orchestrator pins the promoted generation heads once at detect
        # start — per-table, DAT-506 collapses every upstream stage onto one head.
        base_runs = resolve_base_runs(real_session, ["tbl-h1"])
        assert base_runs == {"tbl-h1": "run-addsource"}

        # … and the session run (no annotation of its own) reads the pinned run.
        sem = load_semantic(real_session, "col-h1", run_id="run-session", base_runs=base_runs)
        assert sem is not None
        assert sem["semantic_role"] == "measure"

    def test_semantic_without_pin_stays_none(self, real_session):
        from dataraum.entropy.detectors.loaders import resolve_base_runs

        self._seed_column(real_session, "col-h2", "tbl-h2")
        self._seed_annotation(real_session, "col-h2", "run-addsource")

        # Nothing promoted → empty pin → no fallback → None (never guess a run).
        base_runs = resolve_base_runs(real_session, ["tbl-h2"])
        assert base_runs == {}
        assert (
            load_semantic(real_session, "col-h2", run_id="run-session", base_runs=base_runs) is None
        )

    def test_statistics_falls_back_to_pinned_base_run(self, real_session):
        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.entropy.detectors.loaders import resolve_base_runs

        self._seed_column(real_session, "col-h3", "tbl-h3")
        real_session.add(
            StatisticalProfile(
                profile_id=str(uuid4()),
                column_id="col-h3",
                run_id="run-addsource",
                total_count=100,
                null_count=0,
                distinct_count=90,
                cardinality_ratio=0.9,
                profile_data={},
            )
        )
        real_session.flush()
        self._promote(real_session, "tbl-h3", "run-addsource")

        base_runs = resolve_base_runs(real_session, ["tbl-h3"])
        stats = load_statistics(real_session, "col-h3", run_id="run-session", base_runs=base_runs)
        assert stats is not None
        assert stats["cardinality_ratio"] == 0.9

    def test_pin_is_immune_to_post_pin_promotes(self, real_session):
        """The torn-read guard (DAT-448): a promote AFTER the pin is invisible."""
        from dataraum.entropy.detectors.loaders import resolve_base_runs

        self._seed_column(real_session, "col-h4", "tbl-h4")
        self._seed_annotation(real_session, "col-h4", "run-a", role="measure")
        self._seed_annotation(real_session, "col-h4", "run-b", role="identifier")
        self._promote(real_session, "tbl-h4", "run-a")

        base_runs = resolve_base_runs(real_session, ["tbl-h4"])

        # A concurrent add_source re-run flips the head mid-detect …
        from dataraum.storage.snapshot_head import MetadataSnapshotHead

        head = real_session.query(MetadataSnapshotHead).filter_by(target="table:tbl-h4").one()
        head.run_id = "run-b"
        real_session.flush()

        # … but the pinned read still resolves run-a: one consistent base.
        sem = load_semantic(real_session, "col-h4", run_id="run-session", base_runs=base_runs)
        assert sem is not None
        assert sem["semantic_role"] == "measure"


class TestLoadHypothesisMatchRate:
    """The hypothesis-grading loader (derived_value second witness, docs/architecture/entropy.md).

    Real sqlite session (Column/Table resolution + name validation) + an
    in-memory DuckDB with a ``lake.typed`` table — the loader must grade the
    LLM-hypothesized formula with the discovery's own row statistic, resolve
    source names case-insensitively against REAL columns, and return ``None``
    (witness abstains) for anything it cannot ground.
    """

    def _seed(self, session, rows: list[tuple[float, float, float]]):
        import duckdb

        from dataraum.storage import Column as ColumnModel
        from dataraum.storage import Table as TableModel
        from dataraum.storage.models import Source

        session.add(Source(source_id="src-h", name="src-h", source_type="csv"))
        session.add(
            TableModel(
                table_id="tbl-hyp",
                source_id="src-h",
                table_name="orders",
                layer="typed",
                duckdb_path="orders_t",
                row_count=len(rows),
            )
        )
        for i, name in enumerate(["total", "Subtotal", "tax"]):
            session.add(
                ColumnModel(
                    column_id=f"col-hyp-{name.lower()}",
                    table_id="tbl-hyp",
                    column_name=name,
                    column_position=i,
                    raw_type="VARCHAR",
                    resolved_type="DOUBLE",
                )
            )
        session.flush()

        conn = duckdb.connect()
        conn.execute("ATTACH ':memory:' AS lake")
        conn.execute("CREATE SCHEMA lake.typed")
        conn.execute(
            'CREATE TABLE lake.typed."orders_t" (total DOUBLE, "Subtotal" DOUBLE, tax DOUBLE)'
        )
        conn.executemany('INSERT INTO lake.typed."orders_t" VALUES (?, ?, ?)', rows)
        return conn

    def test_grades_with_the_discovery_statistic(self, real_session):
        from dataraum.entropy.detectors.loaders import load_hypothesis_match_rate

        # 3 matching rows, 1 broken row, 1 zero-target row (excluded — carries
        # no discriminative power, same rule as the discovery sweep).
        conn = self._seed(
            real_session,
            [
                (30.0, 20.0, 10.0),
                (5.5, 3.0, 2.5),
                (7.0, 6.0, 1.0),
                (99.0, 1.0, 1.0),
                (0.0, 1.0, 1.0),
            ],
        )
        graded = load_hypothesis_match_rate(
            real_session, "col-hyp-total", conn, ("subtotal", "TAX"), "sum"
        )
        assert graded is not None
        assert graded["total"] == 4
        assert graded["matches"] == 3
        assert graded["match_rate"] == pytest.approx(0.75)

    def test_unknown_source_column_abstains(self, real_session):
        from dataraum.entropy.detectors.loaders import load_hypothesis_match_rate

        conn = self._seed(real_session, [(30.0, 20.0, 10.0)])
        assert (
            load_hypothesis_match_rate(
                real_session, "col-hyp-total", conn, ("phantom", "tax"), "sum"
            )
            is None
        )

    def test_unknown_operation_or_missing_conn_abstains(self, real_session):
        from dataraum.entropy.detectors.loaders import load_hypothesis_match_rate

        conn = self._seed(real_session, [(30.0, 20.0, 10.0)])
        assert (
            load_hypothesis_match_rate(
                real_session, "col-hyp-total", conn, ("subtotal", "tax"), "weird"
            )
            is None
        )
        assert (
            load_hypothesis_match_rate(
                real_session, "col-hyp-total", None, ("subtotal", "tax"), "sum"
            )
            is None
        )

    def test_nothing_gradable_abstains(self, real_session):
        from dataraum.entropy.detectors.loaders import load_hypothesis_match_rate

        # Only zero-target rows → total 0 → None, not a fake 0% match rate.
        conn = self._seed(real_session, [(0.0, 1.0, 2.0), (0.0, 3.0, 4.0)])
        assert (
            load_hypothesis_match_rate(
                real_session, "col-hyp-total", conn, ("subtotal", "tax"), "sum"
            )
            is None
        )
