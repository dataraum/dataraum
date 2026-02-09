"""Tests for graphs/context.py - execution context builder."""

from __future__ import annotations

from dataraum.graphs.context import (
    ColumnContext,
    GraphExecutionContext,
    RelationshipContext,
    TableContext,
    format_context_for_prompt,
    format_entropy_for_prompt,
)


class TestColumnContext:
    """Tests for ColumnContext dataclass."""

    def test_create_minimal(self) -> None:
        """Create column context with required fields only."""
        ctx = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="transactions",
        )
        assert ctx.column_id == "col-1"
        assert ctx.column_name == "amount"
        assert ctx.table_name == "transactions"
        assert ctx.data_type is None
        assert ctx.flags == []

    def test_create_full(self) -> None:
        """Create column context with all fields."""
        ctx = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="transactions",
            data_type="DOUBLE",
            semantic_role="measure",
            entity_type="monetary_amount",
            null_ratio=0.05,
            cardinality_ratio=0.95,
            outlier_ratio=0.02,
            is_stale=False,
            detected_granularity="daily",
            flags=["high_cardinality"],
        )
        assert ctx.data_type == "DOUBLE"
        assert ctx.semantic_role == "measure"
        assert ctx.null_ratio == 0.05
        assert "high_cardinality" in ctx.flags


class TestTableContext:
    """Tests for TableContext dataclass."""

    def test_create_minimal(self) -> None:
        """Create table context with required fields only."""
        ctx = TableContext(
            table_id="tbl-1",
            table_name="transactions",
        )
        assert ctx.table_id == "tbl-1"
        assert ctx.table_name == "transactions"
        assert ctx.columns == []
        assert ctx.flags == []

    def test_create_with_columns(self) -> None:
        """Create table context with columns."""
        col1 = ColumnContext(
            column_id="col-1",
            column_name="id",
            table_name="transactions",
            semantic_role="key",
        )
        col2 = ColumnContext(
            column_id="col-2",
            column_name="amount",
            table_name="transactions",
            semantic_role="measure",
        )

        ctx = TableContext(
            table_id="tbl-1",
            table_name="transactions",
            row_count=1000,
            column_count=2,
            is_fact_table=True,
            columns=[col1, col2],
        )
        assert ctx.row_count == 1000
        assert ctx.column_count == 2
        assert ctx.is_fact_table is True
        assert len(ctx.columns) == 2


class TestRelationshipContext:
    """Tests for RelationshipContext dataclass."""

    def test_create(self) -> None:
        """Create relationship context."""
        ctx = RelationshipContext(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
            cardinality="many_to_one",
            confidence=0.95,
        )
        assert ctx.from_table == "orders"
        assert ctx.to_table == "customers"
        assert ctx.confidence == 0.95


class TestGraphExecutionContext:
    """Tests for GraphExecutionContext dataclass."""

    def test_create_empty(self) -> None:
        """Create empty execution context."""
        ctx = GraphExecutionContext()
        assert ctx.tables == []
        assert ctx.relationships == []
        assert ctx.total_tables == 0
        assert ctx.slice_column is None

    def test_create_full(self) -> None:
        """Create full execution context."""
        table = TableContext(
            table_id="tbl-1",
            table_name="transactions",
            row_count=1000,
            column_count=5,
        )
        rel = RelationshipContext(
            from_table="transactions",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
        )

        ctx = GraphExecutionContext(
            tables=[table],
            relationships=[rel],
            graph_pattern="star_schema",
            hub_tables=["transactions"],
            leaf_tables=["customers"],
            total_tables=2,
            total_columns=10,
            total_relationships=1,
            quality_issues_by_severity={"warning": 2, "error": 0},
            slice_column="region",
            slice_value="EMEA",
        )
        assert len(ctx.tables) == 1
        assert ctx.graph_pattern == "star_schema"
        assert ctx.slice_column == "region"
        assert ctx.slice_value == "EMEA"


class TestFormatContextForPrompt:
    """Tests for format_context_for_prompt function."""

    def test_format_empty_context(self) -> None:
        """Format empty context produces minimal output."""
        ctx = GraphExecutionContext()
        result = format_context_for_prompt(ctx)

        assert "DATASET CONTEXT" in result
        assert "Tables: 0" in result
        assert "Columns: 0" in result

    def test_format_with_tables(self) -> None:
        """Format context with tables."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="transactions",
            data_type="DOUBLE",
            semantic_role="measure",
            null_ratio=0.05,
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="transactions",
            row_count=1000,
            column_count=1,
            is_fact_table=True,
            columns=[col],
        )

        ctx = GraphExecutionContext(
            tables=[table],
            total_tables=1,
            total_columns=1,
        )
        result = format_context_for_prompt(ctx)

        assert "transactions" in result
        assert "amount" in result
        assert "DOUBLE" in result
        assert "measure" in result

    def test_format_with_relationships(self) -> None:
        """Format context with relationships."""
        rel = RelationshipContext(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
            cardinality="many_to_one",
            confidence=0.95,
        )

        ctx = GraphExecutionContext(
            relationships=[rel],
            total_relationships=1,
        )
        result = format_context_for_prompt(ctx)

        assert "orders" in result
        assert "customers" in result
        assert "customer_id" in result

    def test_format_with_slice(self) -> None:
        """Format context with slice filter."""
        ctx = GraphExecutionContext(
            slice_column="region",
            slice_value="EMEA",
        )
        result = format_context_for_prompt(ctx)

        assert "Slice filter" in result
        assert "region" in result
        assert "EMEA" in result

    def test_format_with_quality_flags(self) -> None:
        """Format context with quality flags."""
        col = ColumnContext(
            column_id="col-1",
            column_name="email",
            table_name="users",
            flags=["high_null_rate", "invalid_format"],
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="users",
            columns=[col],
            flags=["fact_table"],
        )

        ctx = GraphExecutionContext(
            tables=[table],
            quality_flags=["data_quality_issues"],
        )
        result = format_context_for_prompt(ctx)

        assert "high_null_rate" in result or "invalid_format" in result

    def test_format_with_topology(self) -> None:
        """Format context with graph topology."""
        ctx = GraphExecutionContext(
            graph_pattern="star_schema",
            hub_tables=["fact_sales"],
            leaf_tables=["dim_customer", "dim_product"],
        )
        result = format_context_for_prompt(ctx)

        assert "star_schema" in result
        assert "fact_sales" in result


class TestFormatEntropyForPrompt:
    """Tests for format_entropy_for_prompt function."""

    def test_no_entropy_returns_empty(self) -> None:
        """No entropy data returns empty string."""
        ctx = GraphExecutionContext()
        result = format_entropy_for_prompt(ctx)
        assert result == ""

    def test_ready_status(self) -> None:
        """Ready status shows appropriate message."""
        ctx = GraphExecutionContext(
            entropy_summary={
                "overall_readiness": "ready",
                "high_entropy_count": 0,
                "critical_entropy_count": 0,
                "compound_risk_count": 0,
                "readiness_blockers": [],
            }
        )
        result = format_entropy_for_prompt(ctx)

        assert "READY" in result
        assert "sufficient for reliable answers" in result

    def test_investigate_status(self) -> None:
        """Investigate status shows appropriate message."""
        ctx = GraphExecutionContext(
            entropy_summary={
                "overall_readiness": "investigate",
                "high_entropy_count": 3,
                "critical_entropy_count": 0,
                "compound_risk_count": 1,
                "readiness_blockers": [],
            }
        )
        result = format_entropy_for_prompt(ctx)

        assert "INVESTIGATE" in result
        assert "assumptions" in result.lower()
        assert "High entropy columns: 3" in result

    def test_blocked_status(self) -> None:
        """Blocked status shows appropriate message."""
        ctx = GraphExecutionContext(
            entropy_summary={
                "overall_readiness": "blocked",
                "high_entropy_count": 5,
                "critical_entropy_count": 2,
                "compound_risk_count": 2,
                "readiness_blockers": ["orders.amount", "orders.currency"],
            }
        )
        result = format_entropy_for_prompt(ctx)

        assert "BLOCKED" in result
        assert "Critical entropy columns: 2" in result
        assert "orders.amount" in result or "BLOCKING" in result

    def test_high_entropy_columns_shown(self) -> None:
        """High entropy columns are listed."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            entropy_scores={
                "composite_score": 0.75,
                "high_entropy_dimensions": ["semantic.units", "value.nulls"],
                "readiness": "investigate",
            },
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="orders",
            columns=[col],
        )
        ctx = GraphExecutionContext(
            tables=[table],
            entropy_summary={
                "overall_readiness": "investigate",
                "high_entropy_count": 1,
                "critical_entropy_count": 0,
                "compound_risk_count": 0,
                "readiness_blockers": [],
            },
        )
        result = format_entropy_for_prompt(ctx)

        assert "orders.amount" in result
        assert "0.75" in result

    def test_compound_risks_shown(self) -> None:
        """Compound risks are listed."""
        table = TableContext(
            table_id="tbl-1",
            table_name="orders",
            columns=[],
            table_entropy={
                "compound_risk_count": 2,
                "blocked_columns": ["amount", "currency"],
                "readiness": "blocked",
            },
        )
        ctx = GraphExecutionContext(
            tables=[table],
            entropy_summary={
                "overall_readiness": "blocked",
                "high_entropy_count": 2,
                "critical_entropy_count": 2,
                "compound_risk_count": 2,
                "readiness_blockers": [],
            },
        )
        result = format_entropy_for_prompt(ctx)

        assert "DANGEROUS COMBINATIONS" in result or "compound risks" in result.lower()


class TestEntropyInlineIndicators:
    """Tests for inline entropy indicators in formatted output."""

    def test_column_with_warning_indicator(self) -> None:
        """Column with high entropy shows warning indicator."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            entropy_scores={
                "composite_score": 0.65,
                "readiness": "investigate",
            },
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="orders",
            columns=[col],
        )
        ctx = GraphExecutionContext(
            tables=[table],
            total_tables=1,
            total_columns=1,
        )
        result = format_context_for_prompt(ctx)

        # Should have warning indicator
        assert "⚠" in result

    def test_column_with_blocked_indicator(self) -> None:
        """Column with critical entropy shows blocked indicator."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            entropy_scores={
                "composite_score": 0.9,
                "readiness": "blocked",
            },
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="orders",
            columns=[col],
        )
        ctx = GraphExecutionContext(
            tables=[table],
            total_tables=1,
            total_columns=1,
        )
        result = format_context_for_prompt(ctx)

        # Should have blocked indicator
        assert "⛔" in result

    def test_relationship_with_warning(self) -> None:
        """Relationship with non-deterministic join shows warning."""
        rel = RelationshipContext(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
            confidence=0.7,
            relationship_entropy={
                "is_deterministic": False,
                "composite_score": 0.6,
            },
        )
        ctx = GraphExecutionContext(
            relationships=[rel],
            total_relationships=1,
        )
        result = format_context_for_prompt(ctx)

        # Should have warning on relationship line
        assert "⚠" in result
