"""Tests for graphs/context.py - execution context builder."""

from __future__ import annotations

from dataraum.graphs.context import (
    BusinessCycleContext,
    ColumnContext,
    CycleStageContext,
    EntityFlowContext,
    GraphExecutionContext,
    RelationshipContext,
    TableContext,
    ValidationContext,
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
                "readiness_blockers": [],
            }
        )
        result = format_entropy_for_prompt(ctx)

        assert "INVESTIGATE" in result
        assert "assumptions" in result.lower()

    def test_blocked_status(self) -> None:
        """Blocked status shows appropriate message."""
        ctx = GraphExecutionContext(
            entropy_summary={
                "overall_readiness": "blocked",
                "high_entropy_count": 5,
                "critical_entropy_count": 2,
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
                "worst_intent_p_high": 0.75,
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
                "readiness_blockers": [],
            },
        )
        result = format_entropy_for_prompt(ctx)

        assert "orders.amount" in result
        assert "0.75" in result

    def test_blocked_columns_shown(self) -> None:
        """Blocked columns per table are listed."""
        table = TableContext(
            table_id="tbl-1",
            table_name="orders",
            columns=[],
            table_entropy={
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
                "readiness_blockers": [],
            },
        )
        result = format_entropy_for_prompt(ctx)

        assert "DANGEROUS COMBINATIONS" in result or "blocked" in result.lower()

    def test_baseline_columns_grouped(self) -> None:
        """Columns at baseline P(high)=0.30 should be grouped, not listed individually."""
        # One interesting column (above threshold)
        interesting_col = ColumnContext(
            column_id="col-1",
            column_name="rate",
            table_name="fx_rates",
            entropy_scores={
                "worst_intent_p_high": 0.64,
                "high_entropy_dimensions": ["value.outliers"],
                "readiness": "blocked",
            },
        )
        # Three baseline columns at 0.30
        baseline_cols = [
            ColumnContext(
                column_id=f"col-{i}",
                column_name=name,
                table_name="bank_transactions",
                entropy_scores={
                    "worst_intent_p_high": 0.30,
                    "high_entropy_dimensions": [],
                    "readiness": "ready",
                },
            )
            for i, name in enumerate(["currency", "date", "method"], start=2)
        ]
        table1 = TableContext(table_id="tbl-1", table_name="fx_rates", columns=[interesting_col])
        table2 = TableContext(
            table_id="tbl-2", table_name="bank_transactions", columns=baseline_cols
        )
        ctx = GraphExecutionContext(
            tables=[table1, table2],
            entropy_summary={
                "overall_readiness": "investigate",
                "high_entropy_count": 4,
                "critical_entropy_count": 0,
                "readiness_blockers": [],
            },
        )
        result = format_entropy_for_prompt(ctx)

        # Interesting column should be listed individually
        assert "fx_rates.rate" in result
        assert "0.64" in result

        # Baseline columns should NOT be listed individually
        assert "bank_transactions.currency" not in result
        assert "bank_transactions.date" not in result
        assert "bank_transactions.method" not in result

        # Should show baseline count
        assert "3 additional column(s) at baseline uncertainty" in result


class TestEntropyInlineIndicators:
    """Tests for inline entropy indicators in formatted output."""

    def test_column_with_warning_indicator(self) -> None:
        """Column with high entropy shows warning indicator."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            entropy_scores={
                "worst_intent_p_high": 0.45,
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
                "worst_intent_p_high": 0.9,
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


class TestValidationContext:
    """Tests for ValidationContext dataclass."""

    def test_create(self) -> None:
        """Create validation context."""
        ctx = ValidationContext(
            validation_id="double_entry_balance",
            status="failed",
            severity="critical",
            passed=False,
            message="Debits and credits do not balance: diff=42.50",
        )
        assert ctx.validation_id == "double_entry_balance"
        assert ctx.passed is False
        assert ctx.severity == "critical"


class TestFormatValidationSection:
    """Tests for validation section in format_context_for_prompt."""

    def test_no_validations_no_section(self) -> None:
        """No validation results means no section in output."""
        ctx = GraphExecutionContext()
        result = format_context_for_prompt(ctx)
        assert "VALIDATION RULE COMPLIANCE" not in result

    def test_failed_validations_shown(self) -> None:
        """Failed validation checks show details."""
        ctx = GraphExecutionContext(
            validations=[
                ValidationContext(
                    validation_id="double_entry_balance",
                    status="failed",
                    severity="critical",
                    passed=False,
                    message="Debits and credits do not balance",
                ),
                ValidationContext(
                    validation_id="trial_balance",
                    status="failed",
                    severity="error",
                    passed=False,
                    message="Trial balance failed: diff=100",
                ),
            ],
        )
        result = format_context_for_prompt(ctx)

        assert "VALIDATION RULE COMPLIANCE" in result
        assert "FAILED: 2 checks" in result
        assert "[CRITICAL] double_entry_balance" in result
        assert "[ERROR] trial_balance" in result

    def test_passed_validations_count_only(self) -> None:
        """Passed validations show count only, no details."""
        ctx = GraphExecutionContext(
            validations=[
                ValidationContext(
                    validation_id="non_negative_amounts",
                    status="passed",
                    severity="warning",
                    passed=True,
                    message="All amounts are non-negative",
                ),
                ValidationContext(
                    validation_id="referential_integrity",
                    status="passed",
                    severity="error",
                    passed=True,
                    message="All FKs resolve",
                ),
            ],
        )
        result = format_context_for_prompt(ctx)

        assert "VALIDATION RULE COMPLIANCE" in result
        assert "PASSED: 2 checks" in result
        # Passed check messages should NOT appear
        assert "non_negative_amounts" not in result

    def test_mixed_validations(self) -> None:
        """Mix of passed and failed validations."""
        ctx = GraphExecutionContext(
            validations=[
                ValidationContext(
                    validation_id="balance_check",
                    status="failed",
                    severity="critical",
                    passed=False,
                    message="Balance mismatch",
                ),
                ValidationContext(
                    validation_id="fk_check",
                    status="passed",
                    severity="warning",
                    passed=True,
                    message="OK",
                ),
            ],
        )
        result = format_context_for_prompt(ctx)

        assert "FAILED: 1 checks" in result
        assert "PASSED: 1 checks" in result
        assert "[CRITICAL] balance_check: Balance mismatch" in result


class TestFormatBusinessCycleSection:
    """Tests for expanded business cycle formatting."""

    def test_no_cycles_no_section(self) -> None:
        """No cycles means no section."""
        ctx = GraphExecutionContext()
        result = format_context_for_prompt(ctx)
        assert "DETECTED BUSINESS CYCLES" not in result

    def test_cycle_with_stages(self) -> None:
        """Cycle with stages shows ordered progression."""
        cycle = BusinessCycleContext(
            cycle_name="Accounts Receivable",
            cycle_type="accounts_receivable",
            tables_involved=["invoices", "payments"],
            completion_rate=0.85,
            description="Invoice to payment collection cycle.",
            business_value="high",
            confidence=0.94,
            stages=[
                CycleStageContext(
                    stage_name="Invoice Created",
                    stage_order=1,
                    indicator_column="invoices.status",
                    indicator_values=["new", "draft"],
                    completion_rate=0.98,
                ),
                CycleStageContext(
                    stage_name="Payment Received",
                    stage_order=2,
                    indicator_column="invoices.status",
                    indicator_values=["paid"],
                    completion_rate=0.85,
                ),
            ],
            status_column="invoices.status",
            completion_value="paid",
        )
        ctx = GraphExecutionContext(business_cycles=[cycle])
        result = format_context_for_prompt(ctx)

        assert "Accounts Receivable" in result
        assert "accounts_receivable" in result
        assert "high value" in result
        assert "94% confident" in result
        assert "Invoice to payment collection cycle." in result
        assert "Stages:" in result
        assert "1. Invoice Created" in result
        assert "2. Payment Received" in result
        assert "new, draft" in result
        assert "98% progress" in result
        assert 'invoices.status = "paid"' in result

    def test_cycle_with_entity_flows(self) -> None:
        """Cycle with entity flows shows FK paths."""
        cycle = BusinessCycleContext(
            cycle_name="Order to Cash",
            cycle_type="order_to_cash",
            tables_involved=["orders", "customers"],
            entity_flows=[
                EntityFlowContext(
                    entity_type="customer",
                    entity_column="customer_id",
                    entity_table="customers",
                    fact_table="orders",
                    relationship_type="FK",
                ),
            ],
        )
        ctx = GraphExecutionContext(business_cycles=[cycle])
        result = format_context_for_prompt(ctx)

        assert "Entity Flows:" in result
        assert "customer" in result
        assert "customers.customer_id" in result
        assert "orders" in result
        assert "FK" in result
