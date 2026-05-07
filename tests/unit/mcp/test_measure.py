"""Tests for measure MCP tool."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.entropy.measurement import MeasurementResult
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.storage import Column, Source, Table


def _id() -> str:
    return str(uuid4())


def _setup_source_and_table(
    session: Session,
    table_name: str = "orders",
    columns: list[str] | None = None,
) -> tuple[str, str, list[tuple[str, str]]]:
    """Insert Source + Table + Columns."""
    source_id = _id()
    table_id = _id()
    cols = columns or ["amount", "region"]

    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name=table_name,
            layer="typed",
            duckdb_path=f"typed_{table_name}",
        )
    )
    col_ids = []
    for i, name in enumerate(cols):
        col_id = _id()
        col_ids.append((col_id, name))
        session.add(
            Column(
                column_id=col_id,
                table_id=table_id,
                column_name=name,
                column_position=i,
            )
        )
    session.flush()
    return source_id, table_id, col_ids


def _measurement_with_scores() -> MeasurementResult:
    return MeasurementResult(
        scores={
            "semantic.business_meaning.naming_clarity": 0.4,
            "structural.types.type_fidelity": 0.1,
            "computational.consistency.reconciliation": 0.09,
        },
        column_details={
            "semantic.business_meaning.naming_clarity": {
                "column:orders.amount": 0.8,
                "column:orders.region": 0.1,
            },
            "structural.types.type_fidelity": {
                "column:orders.amount": 0.05,
                "column:orders.region": 0.15,
            },
        },
        table_details={
            "computational.consistency.reconciliation": {
                "table:orders": 0.09,
            },
        },
    )


class TestMeasureComplete:
    def test_returns_points_and_scores(self, session: Session) -> None:
        """Complete measurement returns points, layer scores, and readiness."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        assert result["status"] == "complete"
        assert "points" in result
        assert "scores" in result
        assert "readiness" in result

    def test_points_shape(self, session: Session) -> None:
        """Each point has target, dimension, score."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        points = result["points"]
        assert len(points) > 0
        for p in points:
            assert "target" in p
            assert "dimension" in p
            assert "score" in p
            assert isinstance(p["score"], float)

        # Column points present
        col_targets = [p["target"] for p in points if p["target"].startswith("column:")]
        assert "column:orders.amount" in col_targets

        # Table points present
        table_targets = [p["target"] for p in points if p["target"].startswith("table:")]
        assert "table:orders" in table_targets

    def test_scores_aggregated_by_layer(self, session: Session) -> None:
        """Scores are aggregated as mean per top-level layer."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        scores = result["scores"]
        assert "semantic" in scores
        assert "structural" in scores
        assert "computational" in scores
        assert scores["semantic"] == 0.4  # single dimension
        assert scores["structural"] == 0.1  # single dimension

    def test_includes_bbn_readiness(self, session: Session) -> None:
        """Readiness per column, table (worst-of), and dataset from BBN."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        # Mock BBN results
        mock_network = MagicMock()
        amount_result = MagicMock()
        amount_result.readiness = "investigate"
        region_result = MagicMock()
        region_result.readiness = "ready"
        mock_network.columns = {
            "column:orders.amount": amount_result,
            "column:orders.region": region_result,
        }
        mock_network.overall_readiness = "investigate"

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
            patch(
                "dataraum.entropy.views.network_context.build_for_network",
                return_value=mock_network,
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        # Column readiness
        assert result["readiness"]["column:orders.amount"] == "investigate"
        assert result["readiness"]["column:orders.region"] == "ready"
        # Table readiness (worst-of columns)
        assert result["readiness"]["table:orders"] == "investigate"
        # Dataset readiness
        assert result["readiness"]["dataset"] == "investigate"


class TestMeasureNoData:
    def test_returns_no_data_without_pipeline(self, session: Session) -> None:
        """When no entropy records and no pipeline, returns no_data."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=MeasurementResult(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        assert result["status"] == "no_data"

    def test_returns_running_when_pipeline_active(self, session: Session) -> None:
        """When no entropy records but pipeline is running, returns running."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        # Insert a running pipeline — flush PipelineRun first (PhaseLog has FK)
        run_id = _id()
        session.add(
            PipelineRun(
                run_id=run_id,
                source_id=source_id,
                status="running",
                started_at=datetime.now(UTC),
            )
        )
        session.flush()
        session.add(
            PhaseLog(
                run_id=run_id,
                source_id=source_id,
                phase_name="import",
                status="completed",
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                duration_seconds=1.0,
            )
        )
        session.add(
            PhaseLog(
                run_id=run_id,
                source_id=source_id,
                phase_name="typing",
                status="completed",
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                duration_seconds=2.0,
            )
        )
        session.flush()

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=MeasurementResult(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        assert result["status"] == "running"
        assert "import" in result["phases_completed"]
        assert "typing" in result["phases_completed"]


class TestMeasureSurfacesPipelineFailure:
    """A failed pipeline must surface as failed — never as silent 'complete'
    with a shortened phases_completed list."""

    def test_no_data_plus_failed_run_surfaces_failure(self, session: Session) -> None:
        source_id, _, _ = _setup_source_and_table(session)

        run_id = _id()
        session.add(
            PipelineRun(
                run_id=run_id,
                source_id=source_id,
                status="failed",
                started_at=datetime.now(UTC),
                error="ontology induction LLM down",
            )
        )
        session.flush()
        session.add(
            PhaseLog(
                run_id=run_id,
                source_id=source_id,
                phase_name="import",
                status="completed",
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                duration_seconds=1.0,
            )
        )
        session.add(
            PhaseLog(
                run_id=run_id,
                source_id=source_id,
                phase_name="semantic",
                status="failed",
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                duration_seconds=0.5,
                error="Ontology induction failed: Anthropic API error (transient): 529",
            )
        )
        session.flush()

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=MeasurementResult(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        assert result["pipeline_status"] == "failed"
        assert result["status"] == "failed"
        assert "import" in result["phases_completed"]
        assert any(p["phase"] == "semantic" for p in result["phases_failed"])
        failed_semantic = next(p for p in result["phases_failed"] if p["phase"] == "semantic")
        assert "529" in failed_semantic["error"]
        assert "transient" in failed_semantic["error"]
        assert result["error"] == "ontology induction LLM down"
        assert "halted" in result["hint"].lower()

    def test_existing_entropy_plus_failed_run_still_surfaces_failure(
        self, session: Session
    ) -> None:
        """Even when stale entropy from an earlier run exists, a failed
        latest run must surface as failed — silent 'complete' would be
        the worst-case UX."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        run_id = _id()
        session.add(
            PipelineRun(
                run_id=run_id,
                source_id=source_id,
                status="failed",
                started_at=datetime.now(UTC),
                error="metric induction LLM down",
            )
        )
        session.flush()
        session.add(
            PhaseLog(
                run_id=run_id,
                source_id=source_id,
                phase_name="graph_execution",
                status="failed",
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                duration_seconds=0.0,
                error="Metric induction failed",
            )
        )
        session.flush()

        # Build a non-empty MeasurementResult to simulate stale entropy data.
        stale_measurement = MeasurementResult(
            scores={"value.distribution.benford_compliance": 0.5},
            column_details={
                "value.distribution.benford_compliance": {
                    f"column:typed_orders.{col_ids[0][1]}": 0.5,
                },
            },
        )

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=stale_measurement,
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        assert result["pipeline_status"] == "failed"
        assert result["status"] == "failed"
        # No stale points — measure() short-circuits to the failure response
        assert "points" not in result

    def test_run_level_error_with_no_phase_logs_synthesizes_entry(self, session: Session) -> None:
        """When the runner's outer try/except catches an exception (no
        per-phase PhaseLog written), measure() must still surface a
        phases_failed entry so the response is never 'failed but empty'."""
        source_id, _, _ = _setup_source_and_table(session)

        run_id = _id()
        session.add(
            PipelineRun(
                run_id=run_id,
                source_id=source_id,
                status="failed",
                started_at=datetime.now(UTC),
                error="(sqlite3.IntegrityError) UNIQUE constraint failed: tables.source_id",
            )
        )
        session.flush()
        # Note: NO PhaseLog rows — simulates the outer-boundary exception path.

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=MeasurementResult(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session)

        assert result["pipeline_status"] == "failed"
        assert len(result["phases_failed"]) == 1
        assert result["phases_failed"][0]["phase"] == "(unknown)"
        assert "IntegrityError" in result["phases_failed"][0]["error"]


class TestMeasureTargetFilter:
    def test_filter_by_table(self, session: Session) -> None:
        """Target='orders' filters points to that table's columns."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session, target="orders")

        # All points should be for orders table or its columns
        for p in result["points"]:
            assert "orders" in p["target"]

    def test_filter_by_column(self, session: Session) -> None:
        """Target='orders.amount' filters points to that column only."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session, target="orders.amount")

        # Only amount column points
        for p in result["points"]:
            assert p["target"] == "column:orders.amount"

    def test_short_table_name_resolves(self, session: Session) -> None:
        """Target='orders' resolves 'zone1__orders' via suffix match."""
        source_id, table_id, col_ids = _setup_source_and_table(session, table_name="zone1__orders")

        measurement = MeasurementResult(
            scores={"semantic.naming": 0.5},
            column_details={
                "semantic.naming": {
                    "column:zone1__orders.amount": 0.8,
                    "column:zone1__orders.region": 0.2,
                },
            },
            table_details={
                "computational.reconciliation": {"table:zone1__orders": 0.3},
            },
        )

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=measurement,
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session, target="orders")

        assert len(result["points"]) == 3
        for p in result["points"]:
            assert "zone1__orders" in p["target"]

    def test_error_for_nonexistent_target(self, session: Session) -> None:
        """Nonexistent target returns error, not empty results."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session, target="nonexistent")

        assert "error" in result
        assert "nonexistent" in result["error"]

    def test_error_for_nonexistent_column(self, session: Session) -> None:
        """Nonexistent column target returns error."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            result = _measure(session, target="orders.nonexistent")

        assert "error" in result
        assert "nonexistent" in result["error"]

    def test_scores_filtered_by_target(self, session: Session) -> None:
        """Scores recomputed from filtered points, not dataset-wide."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
        ):
            from dataraum.mcp.server import _measure

            all_result = _measure(session)
            filtered_result = _measure(session, target="orders.amount")

        # Filtered scores should differ from dataset-wide scores
        # amount has naming_clarity=0.8 and type_fidelity=0.05
        assert filtered_result["scores"]["semantic"] == 0.8
        assert filtered_result["scores"]["structural"] == 0.05
        # Dataset-wide averages are different
        assert all_result["scores"]["semantic"] == 0.4

    def test_readiness_populated_with_target(self, session: Session) -> None:
        """Readiness filter includes table-level readiness for table targets."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        mock_network = MagicMock()
        amount_result = MagicMock()
        amount_result.readiness = "investigate"
        region_result = MagicMock()
        region_result.readiness = "ready"
        mock_network.columns = {
            "column:orders.amount": amount_result,
            "column:orders.region": region_result,
        }
        mock_network.overall_readiness = "investigate"

        with (
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
            patch(
                "dataraum.entropy.measurement.measure_entropy",
                return_value=_measurement_with_scores(),
            ),
            patch(
                "dataraum.entropy.views.network_context.build_for_network",
                return_value=mock_network,
            ),
        ):
            from dataraum.mcp.server import _measure

            # Table-level target: 2 columns + 1 table readiness
            table_result = _measure(session, target="orders")
            assert table_result["readiness"]["column:orders.amount"] == "investigate"
            assert table_result["readiness"]["column:orders.region"] == "ready"
            assert table_result["readiness"]["table:orders"] == "investigate"
            assert "dataset" not in table_result["readiness"]

            # Column-level target: just the one column
            col_result = _measure(session, target="orders.amount")
            assert len(col_result["readiness"]) == 1
            assert col_result["readiness"]["column:orders.amount"] == "investigate"
