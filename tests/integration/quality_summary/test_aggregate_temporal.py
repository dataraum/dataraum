"""Tests for temporal data loading in aggregate_slice_results."""

from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.analysis.quality_summary.processor import aggregate_slice_results
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
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
        """When no drift summary records exist, temporal_context is empty."""
        slice_def = _setup_sliced_table(session)

        result = aggregate_slice_results(session, slice_def)

        assert result.success
        columns = result.unwrap()
        assert len(columns) == 1  # only 'amount' (region excluded as slice col)
        assert columns[0].temporal_context == {}

    def test_drift_data_attached_to_columns(self, session: Session):
        """When drift summary records exist for slice tables, they are loaded."""
        slice_def = _setup_sliced_table(session)

        # Add a drift summary for the 'amount' column in slice_region_us
        drift = ColumnDriftSummary(
            slice_table_name="slice_region_us",
            column_name="amount",
            time_column="order_date",
            max_js_divergence=0.35,
            mean_js_divergence=0.15,
            periods_analyzed=5,
            periods_with_drift=2,
            drift_evidence_json={
                "worst_period": "2024-02",
                "worst_js": 0.35,
                "top_shifts": [
                    {
                        "category": "Active",
                        "baseline_pct": 45.2,
                        "period_pct": 12.1,
                        "period": "2024-02",
                    }
                ],
                "emerged_categories": [],
                "vanished_categories": [],
                "change_points": ["2024-02"],
            },
        )
        session.add(drift)
        session.commit()

        result = aggregate_slice_results(session, slice_def)

        assert result.success
        columns = result.unwrap()
        assert len(columns) == 1
        tc = columns[0].temporal_context

        assert tc["incomplete_periods"] == 0
        assert tc["volume_anomalies"] == 0
        assert tc["drift_detected_count"] == 2  # periods_with_drift from summary
        assert len(tc["temporal_issues"]) == 1
        assert "Distribution drift in amount" in tc["temporal_issues"][0]

    def test_drift_count_is_per_column(self, session: Session):
        """drift_detected_count reflects only drifts for the specific column."""
        slice_def = _setup_sliced_table(session)

        # Drift on 'amount' column
        d1 = ColumnDriftSummary(
            slice_table_name="slice_region_us",
            column_name="amount",
            time_column="order_date",
            max_js_divergence=0.25,
            mean_js_divergence=0.12,
            periods_analyzed=5,
            periods_with_drift=1,
            drift_evidence_json={
                "worst_period": "2024-02",
                "worst_js": 0.25,
            },
        )
        # Drift on a different column (should NOT count for 'amount')
        d2 = ColumnDriftSummary(
            slice_table_name="slice_region_us",
            column_name="other_col",
            time_column="order_date",
            max_js_divergence=0.40,
            mean_js_divergence=0.20,
            periods_analyzed=5,
            periods_with_drift=3,
        )
        session.add_all([d1, d2])
        session.commit()

        result = aggregate_slice_results(session, slice_def)
        assert result.success
        columns = result.unwrap()
        # 'amount' should have drift_detected_count=1, not 4
        assert columns[0].temporal_context["drift_detected_count"] == 1
