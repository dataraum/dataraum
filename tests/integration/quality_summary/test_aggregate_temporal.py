"""Tests for temporal data loading in aggregate_slice_results."""

from datetime import date
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.analysis.quality_summary.processor import aggregate_slice_results
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.temporal_slicing.db_models import (
    TemporalDriftAnalysis,
    TemporalSliceAnalysis,
    TemporalSliceRun,
)
from dataraum.storage import Column, Source, Table


def _setup_sliced_table(session: Session) -> SliceDefinition:
    """Create a source table with one data column, one slice column,
    a slice definition, and two slice tables.

    Returns the SliceDefinition.
    """
    source_id = str(uuid4())
    table_id = str(uuid4())
    data_col_id = str(uuid4())
    slice_col_id = str(uuid4())

    source = Source(source_id=source_id, name="src", source_type="csv")
    session.add(source)

    # Source (typed) table
    source_table = Table(
        table_id=table_id,
        source_id=source_id,
        table_name="typed_orders",
        layer="typed",
        duckdb_path="typed_orders",
        row_count=100,
    )
    session.add(source_table)

    # Columns
    data_col = Column(
        column_id=data_col_id,
        table_id=table_id,
        column_name="amount",
        column_position=0,
        raw_type="DOUBLE",
        resolved_type="DOUBLE",
    )
    slice_col = Column(
        column_id=slice_col_id,
        table_id=table_id,
        column_name="region",
        column_position=1,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add_all([data_col, slice_col])

    # Slice definition
    slice_def = SliceDefinition(
        table_id=table_id,
        column_id=slice_col_id,
        slice_priority=1,
        slice_type="categorical",
        distinct_values=["us", "eu"],
        reasoning="test",
    )
    session.add(slice_def)

    # Slice tables (match naming convention: slice_{col}_{value})
    for val in ["us", "eu"]:
        st = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name=f"slice_region_{val}",
            layer="slice",
            duckdb_path=f"slice_region_{val}",
            row_count=50,
        )
        session.add(st)

        # Mirror the data column in each slice table
        sc = Column(
            column_id=str(uuid4()),
            table_id=st.table_id,
            column_name="amount",
            column_position=0,
            raw_type="DOUBLE",
            resolved_type="DOUBLE",
        )
        session.add(sc)

    session.commit()
    return slice_def


class TestAggregateSliceResultsTemporalContext:
    """Tests for temporal context loading in aggregate_slice_results."""

    def test_no_temporal_data_gives_empty_context(self, session: Session):
        """When no temporal records exist, temporal_context is empty."""
        slice_def = _setup_sliced_table(session)

        result = aggregate_slice_results(session, slice_def)

        assert result.success
        columns = result.unwrap()
        assert len(columns) == 1  # only 'amount' (region excluded as slice col)
        assert columns[0].temporal_context == {}

    def test_temporal_data_attached_to_columns(self, session: Session):
        """When temporal records exist for slice tables, they are loaded."""
        slice_def = _setup_sliced_table(session)

        # Create a temporal run + analyses for one of the slice tables
        run = TemporalSliceRun(
            slice_table_name="slice_region_us",
            time_column="order_date",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 4, 1),
            time_grain="monthly",
            total_periods=3,
            incomplete_periods=1,
            anomaly_count=1,
            drift_detected=False,
        )
        session.add(run)
        session.flush()  # need run_id for FK

        # Period 1: complete
        a1 = TemporalSliceAnalysis(
            run_id=run.run_id,
            slice_table_name="slice_region_us",
            time_column="order_date",
            period_label="2024-01",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 2, 1),
            row_count=40,
            coverage_ratio=1.0,
            is_complete=True,
            is_volume_anomaly=False,
            issues_json=None,
        )
        # Period 2: incomplete with anomaly
        a2 = TemporalSliceAnalysis(
            run_id=run.run_id,
            slice_table_name="slice_region_us",
            time_column="order_date",
            period_label="2024-02",
            period_start=date(2024, 2, 1),
            period_end=date(2024, 3, 1),
            row_count=10,
            coverage_ratio=0.5,
            is_complete=False,
            is_volume_anomaly=True,
            period_over_period_change=-0.75,
            issues_json=["Low coverage: 50%", "Volume drop: z-score=-2.8"],
        )
        session.add_all([a1, a2])

        # Add a drift record for the 'amount' column
        drift = TemporalDriftAnalysis(
            run_id=run.run_id,
            slice_table_name="slice_region_us",
            column_name="amount",
            period_label="2024-02",
            js_divergence=0.35,
            has_significant_drift=True,
            has_category_changes=False,
        )
        session.add(drift)
        session.commit()

        result = aggregate_slice_results(session, slice_def)

        assert result.success
        columns = result.unwrap()
        assert len(columns) == 1
        tc = columns[0].temporal_context

        assert tc["incomplete_periods"] == 1
        assert tc["volume_anomalies"] == 1
        assert tc["drift_detected_count"] == 1
        assert len(tc["temporal_issues"]) == 2
        assert "Low coverage: 50%" in tc["temporal_issues"]
        assert len(tc["temporal_data"]) == 2

    def test_drift_count_is_per_column(self, session: Session):
        """drift_detected_count reflects only drifts for the specific column."""
        slice_def = _setup_sliced_table(session)

        run = TemporalSliceRun(
            slice_table_name="slice_region_us",
            time_column="order_date",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 3, 1),
            time_grain="monthly",
        )
        session.add(run)
        session.flush()

        # One analysis record so temporal_context is non-empty
        analysis = TemporalSliceAnalysis(
            run_id=run.run_id,
            slice_table_name="slice_region_us",
            time_column="order_date",
            period_label="2024-01",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 2, 1),
            row_count=50,
            coverage_ratio=1.0,
            is_complete=True,
            is_volume_anomaly=False,
        )
        session.add(analysis)

        # Drift on 'amount' column
        d1 = TemporalDriftAnalysis(
            run_id=run.run_id,
            slice_table_name="slice_region_us",
            column_name="amount",
            period_label="2024-02",
            has_significant_drift=True,
            has_category_changes=False,
        )
        # Drift on a different column (should NOT count for 'amount')
        d2 = TemporalDriftAnalysis(
            run_id=run.run_id,
            slice_table_name="slice_region_us",
            column_name="other_col",
            period_label="2024-02",
            has_significant_drift=True,
            has_category_changes=False,
        )
        session.add_all([d1, d2])
        session.commit()

        result = aggregate_slice_results(session, slice_def)
        assert result.success
        columns = result.unwrap()
        # 'amount' should have drift_detected_count=1, not 2
        assert columns[0].temporal_context["drift_detected_count"] == 1
