"""Tests for SlicingViewPhase."""

from unittest.mock import MagicMock

from dataraum.analysis.views.db_models import EnrichedView
from dataraum.pipeline.phases.slicing_view_phase import SlicingViewPhase
from dataraum.pipeline.registry import get_phase_class
from dataraum.storage import Column, Table


def _make_table(table_id: str, table_name: str, duckdb_path: str, row_count: int = 100) -> Table:
    t = MagicMock(spec=Table)
    t.table_id = table_id
    t.table_name = table_name
    t.duckdb_path = duckdb_path
    t.row_count = row_count
    return t


def _make_column(column_id: str, column_name: str, table_id: str) -> Column:
    c = MagicMock(spec=Column)
    c.column_id = column_id
    c.column_name = column_name
    c.table_id = table_id
    return c


def _make_enriched_view(
    fact_table_id: str, fact_table_name: str, dim_cols: list[str]
) -> EnrichedView:
    ev = MagicMock(spec=EnrichedView)
    ev.fact_table_id = fact_table_id
    ev.dimension_columns = dim_cols
    return ev


def _make_slice_def(
    slice_id: str, column_id: str, table_id: str, column_name: str | None = None
) -> MagicMock:
    sd = MagicMock()
    sd.slice_id = slice_id
    sd.column_id = column_id
    sd.column_name = column_name
    sd.table_id = table_id
    return sd


class TestSlicingViewPhaseRegistry:
    """Tests for phase registration and metadata."""

    def test_phase_registered(self):
        """SlicingViewPhase is registered in the phase registry."""
        cls = get_phase_class("slicing_view")
        assert cls is not None

    def test_phase_name(self):
        """Phase name is 'slicing_view'."""
        phase = SlicingViewPhase()
        assert phase.name == "slicing_view"

    def test_phase_dependencies(self):
        """Phase depends on slicing."""
        phase = SlicingViewPhase()
        assert "slicing" in phase.dependencies

    def test_phase_outputs(self):
        """Phase outputs slicing_views."""
        phase = SlicingViewPhase()
        assert "slicing_views" in phase.outputs


class TestBuildSlicingViewSql:
    """Tests for _build_slicing_view_sql logic."""

    def setup_method(self):
        self.phase = SlicingViewPhase()

    def test_native_fact_column_not_in_dim_cols(self):
        """Slice column on fact table is not added to slice_dim_cols."""
        fact_table = _make_table("t1", "orders", "typed_orders")
        fact_col = _make_column("c1", "status", "t1")

        slice_def = _make_slice_def("sd1", "c1", "t1")

        sql, slice_dim_cols, slice_def_ids = self.phase._build_slicing_view_sql(
            fact_table=fact_table,
            slice_defs=[slice_def],
            enriched_view=None,
            tables_by_id={"t1": fact_table},
            columns_by_id={"c1": fact_col},
            fact_columns=[fact_col],
        )

        # Native column is already in fact cols — not added to slice_dim_cols
        assert slice_dim_cols == []
        assert slice_def_ids == ["sd1"]

    def test_enriched_dim_columns_filtered_to_slice_definitions(self):
        """Only enriched dim columns whose FK prefix matches a slice def column are included.

        Enriched dim col format: "{fk_col}__{dim_col}" where fk_col is a fact-table FK column.
        If a slice def references the FK column (customer_id), all dim cols with that prefix
        are included. Dim cols with a different prefix are excluded.
        """
        fact_table = _make_table("t1", "orders", "typed_orders")
        # FK column in fact table — this is what slice def references
        fk_col = _make_column("c_fk", "customer_id", "t1")
        other_fk_col = _make_column("c_other_fk", "product_id", "t1")

        # Enriched dim cols: customer_id__region and customer_id__name match slice def;
        # product_id__name does not (different FK prefix)
        enriched_view = _make_enriched_view(
            "t1", "orders", ["customer_id__region", "customer_id__name", "product_id__name"]
        )

        # LLM recommended the full enriched dim col name; column_id points to the FK column
        slice_def = _make_slice_def("sd1", "c_fk", "t1", column_name="customer_id__region")

        sql, slice_dim_cols, slice_def_ids = self.phase._build_slicing_view_sql(
            fact_table=fact_table,
            slice_defs=[slice_def],
            enriched_view=enriched_view,
            tables_by_id={"t1": fact_table},
            columns_by_id={"c_fk": fk_col, "c_other_fk": other_fk_col},
            fact_columns=[fk_col, other_fk_col],
        )

        # Only the directly recommended enriched dim col is included
        assert "customer_id__region" in slice_dim_cols
        # customer_id__name and product_id__name are not in slice definitions — excluded
        assert "customer_id__name" not in slice_dim_cols
        assert "product_id__name" not in slice_dim_cols

    def test_no_dim_columns_when_enriched_view_has_none(self):
        """When enriched view has no dimension columns, slice_dim_cols is empty."""
        fact_table = _make_table("t1", "orders", "typed_orders")
        dim_table = _make_table("t2", "customers", "typed_customers")
        dim_col = _make_column("c2", "internal_code", "t2")

        enriched_view = _make_enriched_view("t1", "orders", [])

        slice_def = _make_slice_def("sd1", "c2", "t1")

        _, slice_dim_cols, _ = self.phase._build_slicing_view_sql(
            fact_table=fact_table,
            slice_defs=[slice_def],
            enriched_view=enriched_view,
            tables_by_id={"t1": fact_table, "t2": dim_table},
            columns_by_id={"c2": dim_col},
            fact_columns=[],
        )

        assert slice_dim_cols == []

    def test_sql_does_not_use_select_star(self):
        """SQL uses explicit column list, not SELECT *."""
        fact_table = _make_table("t1", "orders", "typed_orders")
        fact_col = _make_column("c1", "order_id", "t1")
        # FK column whose name matches the prefix of the enriched dim col
        fk_col = _make_column("c_fk", "customer_id", "t1")

        enriched_view = _make_enriched_view("t1", "orders", ["customer_id__region"])
        slice_def = _make_slice_def("sd1", "c_fk", "t1", column_name="customer_id__region")

        sql, _, _ = self.phase._build_slicing_view_sql(
            fact_table=fact_table,
            slice_defs=[slice_def],
            enriched_view=enriched_view,
            tables_by_id={"t1": fact_table},
            columns_by_id={"c1": fact_col, "c_fk": fk_col},
            fact_columns=[fact_col, fk_col],
        )

        assert "SELECT *" not in sql
        assert '"order_id"' in sql
        assert '"customer_id__region"' in sql

    def test_sql_sources_from_enriched_view(self):
        """SQL queries from the enriched view, not the raw table."""
        fact_table = _make_table("t1", "orders", "typed_orders")
        dim_table = _make_table("t2", "customers", "typed_customers")
        fact_col = _make_column("c1", "order_id", "t1")
        dim_col = _make_column("c2", "region", "t2")

        enriched_view = _make_enriched_view("t1", "orders", ["customers__region"])
        slice_def = _make_slice_def("sd1", "c2", "t1")

        sql, _, _ = self.phase._build_slicing_view_sql(
            fact_table=fact_table,
            slice_defs=[slice_def],
            enriched_view=enriched_view,
            tables_by_id={"t1": fact_table, "t2": dim_table},
            columns_by_id={"c1": fact_col, "c2": dim_col},
            fact_columns=[fact_col],
        )

        assert '"enriched_orders"' in sql
        assert "slicing_orders" in sql

    def test_no_duplicate_dim_cols(self):
        """Same dimension column is not added twice even if two slice defs reference it."""
        fact_table = _make_table("t1", "orders", "typed_orders")
        fk_col = _make_column("c_fk", "customer_id", "t1")

        enriched_view = _make_enriched_view("t1", "orders", ["customer_id__region"])
        sd1 = _make_slice_def("sd1", "c_fk", "t1", column_name="customer_id__region")
        sd2 = _make_slice_def("sd2", "c_fk", "t1", column_name="customer_id__region")

        _, slice_dim_cols, _ = self.phase._build_slicing_view_sql(
            fact_table=fact_table,
            slice_defs=[sd1, sd2],
            enriched_view=enriched_view,
            tables_by_id={"t1": fact_table},
            columns_by_id={"c_fk": fk_col},
            fact_columns=[],
        )

        assert slice_dim_cols.count("customer_id__region") == 1


class TestVerifyGrain:
    """Tests for the _verify_grain static method."""

    def test_returns_true_when_count_matches(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (100,)
        assert SlicingViewPhase._verify_grain(conn, "slicing_orders", 100) is True

    def test_returns_false_when_count_mismatches(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (120,)
        assert SlicingViewPhase._verify_grain(conn, "slicing_orders", 100) is False

    def test_returns_true_when_expected_count_is_none(self):
        conn = MagicMock()
        assert SlicingViewPhase._verify_grain(conn, "slicing_orders", None) is True
        conn.execute.assert_not_called()
