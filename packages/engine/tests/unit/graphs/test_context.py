"""Tests for graphs/context.py - execution context builder and metadata document."""

from __future__ import annotations

from dataraum.graphs.context import (
    BusinessCycleContext,
    ColumnContext,
    CycleStageContext,
    EnrichedViewContext,
    EntityFlowContext,
    GraphExecutionContext,
    RelationshipContext,
    SliceContext,
    TableContext,
    ValidationContext,
    format_metadata_document,
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
        assert ctx.business_name is None

    def test_create_full(self) -> None:
        """Create column context with all fields."""
        ctx = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="transactions",
            data_type="DOUBLE",
            semantic_role="measure",
            entity_type="monetary_amount",
            business_name="Invoice Amount",
            business_description="Total value before tax in local currency",
            unit_source_column="currency_code",
            min_timestamp="2024-01-01",
            max_timestamp="2024-12-31",
            completeness_ratio=0.98,
            null_ratio=0.05,
            cardinality_ratio=0.95,
            outlier_ratio=0.02,
            is_stale=False,
            detected_granularity="daily",
            flags=["high_cardinality"],
        )
        assert ctx.data_type == "DOUBLE"
        assert ctx.business_name == "Invoice Amount"
        assert ctx.unit_source_column == "currency_code"


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
        assert ctx.table_description is None
        assert ctx.grain_columns == []
        assert ctx.readiness_for_use is None

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
            table_description="Financial transactions table",
            grain_columns=["id"],
            time_column="created_at",
            columns=[col1, col2],
        )
        assert ctx.row_count == 1000
        assert ctx.table_description == "Financial transactions table"
        assert ctx.grain_columns == ["id"]
        assert ctx.time_column == "created_at"
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
            details={"summary": "Balance mismatch of 42.50"},
        )
        assert ctx.validation_id == "double_entry_balance"
        assert ctx.passed is False
        assert ctx.details == {"summary": "Balance mismatch of 42.50"}


# =============================================================================
# Tests for format_metadata_document
# =============================================================================


class TestFormatMetadataDocument:
    """Tests for format_metadata_document function."""

    def test_empty_context(self) -> None:
        """Format empty context produces minimal output."""
        ctx = GraphExecutionContext()
        result = format_metadata_document(ctx)

        assert "# Data Catalog:" in result
        assert "## Overview" in result
        assert "0 tables, 0 columns" in result

    def test_source_name_in_header(self) -> None:
        """Source name appears in document header."""
        ctx = GraphExecutionContext()
        result = format_metadata_document(ctx, source_name="Finance Dataset")

        assert "# Data Catalog: Finance Dataset" in result

    def test_overview_topology(self) -> None:
        """Overview includes schema topology."""
        ctx = GraphExecutionContext(
            graph_pattern="star_schema",
            hub_tables=["fact_sales"],
            leaf_tables=["dim_customer", "dim_product"],
            total_tables=3,
            total_columns=15,
        )
        result = format_metadata_document(ctx)

        assert "star_schema" in result
        assert "fact_sales" in result
        assert "dim_customer" in result

    def test_tables_with_entity_and_description(self) -> None:
        """Tables show entity type and description."""
        table = TableContext(
            table_id="tbl-1",
            table_name="transactions",
            duckdb_name="typed_transactions",
            row_count=5000,
            column_count=3,
            is_fact_table=True,
            entity_type="financial_transaction",
            table_description="Records of all financial transactions",
            grain_columns=["transaction_id"],
            time_column="created_at",
            columns=[
                ColumnContext(
                    column_id="col-1",
                    column_name="created_at",
                    table_name="transactions",
                    data_type="TIMESTAMP",
                    semantic_role="timestamp",
                    detected_granularity="daily",
                    min_timestamp="2024-01-01",
                    max_timestamp="2024-12-31",
                ),
            ],
        )
        ctx = GraphExecutionContext(tables=[table], total_tables=1, total_columns=3)
        result = format_metadata_document(ctx)

        assert "typed_transactions" in result
        assert "FACT" in result
        assert "financial_transaction" in result
        assert "Records of all financial transactions" in result
        assert "transaction_id" in result
        assert "created_at" in result

    def test_column_table_format(self) -> None:
        """Columns are formatted in a table with business metadata."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="invoices",
            data_type="DOUBLE",
            semantic_role="measure",
            business_name="Invoice Amount",
            business_description="Total before tax",
            unit_source_column="currency_code",
            is_derived=True,
            derived_formula="qty * price",
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="invoices",
            columns=[col],
        )
        ctx = GraphExecutionContext(tables=[table], total_tables=1)
        result = format_metadata_document(ctx)

        assert "| Column | Type | Role | Description | Notes |" in result
        assert "amount" in result
        assert "DOUBLE" in result
        assert "measure" in result
        assert "Invoice Amount" in result
        assert "currency_code" in result
        assert "qty * price" in result

    def test_entropy_scores_per_column(self) -> None:
        """Entropy scores shown per column in data quality notes."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            entropy_scores={
                "worst_intent_p_high": 0.75,
                "readiness": "investigate",
            },
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="orders",
            columns=[col],
        )
        ctx = GraphExecutionContext(tables=[table], total_tables=1)
        result = format_metadata_document(ctx)

        # Column notes include entropy readiness indicator
        assert "investigate" in result

    def test_relationships_table(self) -> None:
        """Relationships rendered as a table."""
        rel = RelationshipContext(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
            cardinality="many_to_one",
            confidence=0.95,
        )
        ctx = GraphExecutionContext(relationships=[rel], total_relationships=1)
        result = format_metadata_document(ctx)

        assert "## Relationships" in result
        assert "orders.customer_id" in result
        assert "customers.id" in result
        assert "many_to_one" in result

    def test_relationship_non_deterministic_warning(self) -> None:
        """Non-deterministic relationships show warning."""
        rel = RelationshipContext(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
            confidence=0.7,
            relationship_entropy={"is_deterministic": False},
        )
        ctx = GraphExecutionContext(relationships=[rel], total_relationships=1)
        result = format_metadata_document(ctx)

        assert "⚠ non-deterministic" in result

    def test_enriched_views(self) -> None:
        """Enriched views rendered with slice dimensions."""
        ev = EnrichedViewContext(
            view_name="enriched_sales",
            fact_table="sales",
            dimension_columns=["customer_name", "product_category"],
            is_grain_verified=True,
        )
        slc = SliceContext(
            column_name="region",
            table_name="sales",
            value_count=5,
            distinct_values=["EMEA", "APAC", "NA"],
        )
        ctx = GraphExecutionContext(
            enriched_views=[ev],
            available_slices=[slc],
        )
        result = format_metadata_document(ctx)

        assert "## Enriched Views" in result
        assert "enriched_sales" in result
        assert "grain verified" in result
        assert "region" in result
        assert "EMEA" in result

    def test_slice_filter_shown(self) -> None:
        """Active slice filter shown in overview."""
        ctx = GraphExecutionContext(
            slice_column="region",
            slice_value="EMEA",
        )
        result = format_metadata_document(ctx)

        assert "region" in result
        assert "EMEA" in result

    def test_business_processes(self) -> None:
        """Business processes with stages and entity flows."""
        cycle = BusinessCycleContext(
            cycle_name="Accounts Receivable",
            cycle_type="accounts_receivable",
            tables_involved=["invoices", "payments"],
            completion_rate=0.85,
            description="Invoice to payment collection cycle.",
            business_value="high",
            confidence=0.94,
            total_records=10000,
            completed_cycles=8500,
            evidence=["Status column tracks lifecycle", "Payment dates correlate"],
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
            entity_flows=[
                EntityFlowContext(
                    entity_type="customer",
                    entity_column="customer_id",
                    entity_table="customers",
                    fact_table="invoices",
                    relationship_type="FK",
                ),
            ],
            status_column="invoices.status",
            completion_value="paid",
        )
        ctx = GraphExecutionContext(business_cycles=[cycle])
        result = format_metadata_document(ctx)

        assert "## Business Processes" in result
        assert "Accounts Receivable" in result
        assert "accounts_receivable" in result
        assert "Invoice to payment collection cycle." in result
        assert "10,000 records" in result
        assert "8,500 completed" in result
        assert "Status column tracks lifecycle" in result
        assert "Invoice Created" in result
        assert "Payment Received" in result
        assert "customer" in result

    def test_business_processes_health_status(self) -> None:
        """Business processes show VERIFIED/PARTIAL/UNVERIFIED from health."""
        from dataraum.analysis.cycles.health import CycleHealthScore, HealthReport

        cycle = BusinessCycleContext(
            cycle_name="Accounts Receivable",
            cycle_type="accounts_receivable",
            tables_involved=["invoices"],
        )
        health = HealthReport(
            source_id="src-1",
            cycle_scores=[
                CycleHealthScore(
                    cycle_id="c1",
                    cycle_name="Accounts Receivable",
                    canonical_type="accounts_receivable",
                    completion_rate=0.95,
                    validation_pass_rate=1.0,
                    validations_run=3,
                    validations_passed=3,
                    composite_score=0.9,
                ),
            ],
        )
        ctx = GraphExecutionContext(
            business_cycles=[cycle],
            cycle_health=health,
        )
        result = format_metadata_document(ctx)

        assert "VERIFIED" in result
        assert "3/3 validations" in result

    def test_validation_results(self) -> None:
        """Validation results shown with pass/fail counts."""
        ctx = GraphExecutionContext(
            validations=[
                ValidationContext(
                    validation_id="double_entry_balance",
                    status="failed",
                    severity="critical",
                    passed=False,
                    message="Debits and credits do not balance",
                    details={"summary": "Balance mismatch of 42.50"},
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
        result = format_metadata_document(ctx)

        assert "## Validation Results" in result
        assert "PASSED: 1" in result
        assert "FAILED: 1" in result
        assert "[CRITICAL] double_entry_balance" in result
        assert "Balance mismatch of 42.50" in result

    def test_temporal_coverage(self) -> None:
        """Temporal coverage shown in overview from column profiles."""
        col = ColumnContext(
            column_id="col-1",
            column_name="date",
            table_name="transactions",
            min_timestamp="2024-01-01",
            max_timestamp="2024-12-31",
            detected_granularity="daily",
            completeness_ratio=0.95,
        )
        table = TableContext(
            table_id="tbl-1",
            table_name="transactions",
            columns=[col],
        )
        ctx = GraphExecutionContext(tables=[table], total_tables=1)
        result = format_metadata_document(ctx)

        assert "Temporal coverage: 2024-01-01 to 2024-12-31" in result
        assert "daily" in result

    def test_readiness_summary(self) -> None:
        """Data readiness shown in overview from entropy summary."""
        ctx = GraphExecutionContext(
            entropy_summary={
                "overall_readiness": "investigate",
                "critical_entropy_count": 1,
            },
        )
        result = format_metadata_document(ctx)

        assert "Data readiness: investigate" in result
        assert "1 blocked" in result

    def test_entropy_blocked_indicator_in_notes(self) -> None:
        """Blocked entropy indicator appears in column notes."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            data_type="DOUBLE",
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
        ctx = GraphExecutionContext(tables=[table], total_tables=1)
        result = format_metadata_document(ctx)

        assert "⛔" in result

    def test_entropy_investigate_indicator_in_notes(self) -> None:
        """Investigate entropy indicator appears in column notes."""
        col = ColumnContext(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            data_type="DOUBLE",
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
        ctx = GraphExecutionContext(tables=[table], total_tables=1)
        result = format_metadata_document(ctx)

        assert "⚠" in result
