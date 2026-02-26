"""Tests for SlicingView model."""

from dataraum.pipeline.registry import import_all_phase_models

import_all_phase_models()  # Ensure all SQLAlchemy mappers are registered before importing models

from dataraum.analysis.views.db_models import SlicingView  # noqa: E402


class TestSlicingView:
    """Tests for the SlicingView SQLAlchemy model."""

    def test_tablename(self):
        """SlicingView uses the correct table name."""
        assert SlicingView.__tablename__ == "slicing_views"

    def test_instantiation(self):
        """SlicingView can be instantiated with expected fields."""
        view = SlicingView(
            fact_table_id="table-123",
            view_name="slicing_orders",
            view_sql='CREATE OR REPLACE VIEW "slicing_orders" AS SELECT ...',
            slice_definition_ids=["slice-1", "slice-2"],
            slice_columns=["customers__region", "products__category"],
            is_grain_verified=True,
        )
        assert view.fact_table_id == "table-123"
        assert view.view_name == "slicing_orders"
        assert view.slice_columns == ["customers__region", "products__category"]
        assert view.slice_definition_ids == ["slice-1", "slice-2"]
        assert view.is_grain_verified is True

    def test_defaults(self):
        """SlicingView optional fields default to None/False."""
        view = SlicingView(
            fact_table_id="table-123",
            view_name="slicing_orders",
            view_sql="SELECT ...",
        )
        assert view.slice_columns is None
        assert view.slice_definition_ids is None

    def test_view_id_explicit(self):
        """SlicingView accepts an explicit view_id."""
        view = SlicingView(
            view_id="my-uuid",
            fact_table_id="t1",
            view_name="slicing_a",
            view_sql="SELECT 1",
        )
        assert view.view_id == "my-uuid"
