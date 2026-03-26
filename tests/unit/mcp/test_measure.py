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
        """Readiness per column from BBN inference."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        # Mock BBN results
        mock_network = MagicMock()
        col_result = MagicMock()
        col_result.readiness = "investigate"
        mock_network.columns = {"orders.amount": col_result}

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

        assert result["readiness"]["orders.amount"] == "investigate"


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
        """Readiness keys use 'column:' prefix — filter works with target."""
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

            # Table-level target
            table_result = _measure(session, target="orders")
            assert len(table_result["readiness"]) == 2
            assert table_result["readiness"]["column:orders.amount"] == "investigate"

            # Column-level target
            col_result = _measure(session, target="orders.amount")
            assert len(col_result["readiness"]) == 1
            assert col_result["readiness"]["column:orders.amount"] == "investigate"
