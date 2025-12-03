"""Test single-table cycle classification with financial domain patterns."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from dataraum_context.quality.domains.financial import (
    FinancialDomainAnalyzer,
    detect_financial_cycles,
)
from dataraum_context.quality.models import CycleDetection


@pytest.fixture
def sample_ar_cycle():
    """Create a sample accounts receivable cycle."""
    return CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.1,
        death=0.5,
        persistence=0.4,
        involved_columns=[
            "customer_id",
            "invoice_number",
            "invoice_amount",
            "payment_date",
            "receivable_balance",
        ],
        cycle_type=None,  # Not yet classified
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )


@pytest.fixture
def sample_expense_cycle():
    """Create a sample expense/AP cycle."""
    return CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.2,
        death=0.6,
        persistence=0.4,
        involved_columns=[
            "vendor_id",
            "vendor_name",
            "purchase_order_id",
            "expense_amount",
            "payable_status",
        ],
        cycle_type=None,
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )


@pytest.fixture
def sample_inventory_cycle():
    """Create a sample inventory cycle."""
    return CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.15,
        death=0.55,
        persistence=0.4,
        involved_columns=[
            "product_id",
            "sku",
            "inventory_quantity",
            "stock_value",
            "cogs_amount",
        ],
        cycle_type=None,
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )


@pytest.fixture
def sample_unclassified_cycle():
    """Create a cycle that shouldn't match any financial pattern."""
    return CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.1,
        death=0.3,
        persistence=0.2,
        involved_columns=["random_field_a", "random_field_b", "unrelated_column"],
        cycle_type=None,
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )


def test_detect_financial_cycles_ar_cycle(sample_ar_cycle):
    """Test classification of accounts receivable cycle."""
    cycles = [sample_ar_cycle]
    classified = detect_financial_cycles(
        cycles=cycles, table_names=["transactions"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should be classified as AR cycle
    assert classified_cycle.cycle_type == "accounts_receivable_cycle"
    assert classified_cycle.cycle_id == sample_ar_cycle.cycle_id
    assert classified_cycle.persistence == sample_ar_cycle.persistence


def test_detect_financial_cycles_expense_cycle(sample_expense_cycle):
    """Test classification of expense/AP cycle."""
    cycles = [sample_expense_cycle]
    classified = detect_financial_cycles(
        cycles=cycles, table_names=["expenses"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should be classified as expense cycle
    assert classified_cycle.cycle_type == "expense_cycle"
    assert classified_cycle.cycle_id == sample_expense_cycle.cycle_id


def test_detect_financial_cycles_inventory_cycle(sample_inventory_cycle):
    """Test classification of inventory cycle."""
    cycles = [sample_inventory_cycle]
    classified = detect_financial_cycles(
        cycles=cycles, table_names=["inventory"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should be classified as inventory cycle
    assert classified_cycle.cycle_type == "inventory_cycle"


def test_detect_financial_cycles_unclassified(sample_unclassified_cycle):
    """Test that unrelated cycles remain unclassified."""
    cycles = [sample_unclassified_cycle]
    classified = detect_financial_cycles(
        cycles=cycles, table_names=["misc_data"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should NOT be classified (no matching pattern)
    assert classified_cycle.cycle_type is None


def test_detect_financial_cycles_multiple_cycles(
    sample_ar_cycle, sample_expense_cycle, sample_inventory_cycle, sample_unclassified_cycle
):
    """Test classification of multiple cycles simultaneously."""
    cycles = [
        sample_ar_cycle,
        sample_expense_cycle,
        sample_inventory_cycle,
        sample_unclassified_cycle,
    ]
    classified = detect_financial_cycles(
        cycles=cycles,
        table_names=["transactions", "expenses", "inventory"],
        column_relationships={},
    )

    assert len(classified) == 4

    # Extract cycle types
    cycle_types = [c.cycle_type for c in classified]

    # Should have 3 classified and 1 unclassified
    assert "accounts_receivable_cycle" in cycle_types
    assert "expense_cycle" in cycle_types
    assert "inventory_cycle" in cycle_types
    assert None in cycle_types  # Unclassified cycle


def test_financial_domain_analyzer_integration(sample_ar_cycle, sample_expense_cycle):
    """Test FinancialDomainAnalyzer with single-table cycle classification."""
    # Create a mock TopologicalQualityResult
    from dataraum_context.quality.models import (
        BettiNumbers,
        TopologicalQualityResult,
    )

    topological_result = TopologicalQualityResult(
        table_id="test_table_1",
        table_name="transactions",
        betti_numbers=BettiNumbers(
            betti_0=1,
            betti_1=2,
            betti_2=0,
            total_complexity=3,
            is_connected=True,  # betti_0 == 1
            has_cycles=True,  # betti_1 > 0
        ),
        persistent_cycles=[sample_ar_cycle, sample_expense_cycle],
        structural_complexity=3,
        orphaned_components=0,
        quality_score=0.8,
    )

    # Create analyzer and run analysis
    analyzer = FinancialDomainAnalyzer()
    result = analyzer.analyze(topological_result=topological_result)

    # Verify result structure
    assert result["domain"] == "financial"
    assert "classified_cycles" in result
    assert "cycle_classification_summary" in result
    assert "financial_quality_score" in result

    # Verify cycles were classified
    classified_cycles = result["classified_cycles"]
    assert len(classified_cycles) == 2

    cycle_types = [c.cycle_type for c in classified_cycles]
    assert "accounts_receivable_cycle" in cycle_types
    assert "expense_cycle" in cycle_types

    # Verify classification summary
    summary = result["cycle_classification_summary"]
    assert summary["total_cycles"] == 2
    assert summary["classified_count"] == 2
    assert summary["unclassified_count"] == 0
    assert summary["classification_rate"] == 1.0

    # Verify cycle types breakdown
    assert "accounts_receivable_cycle" in summary["cycle_types"]
    assert "expense_cycle" in summary["cycle_types"]


def test_anomalous_cycle_detection():
    """Test that anomalous cycles are preserved during classification."""
    anomalous_cycle = CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.1,
        death=0.9,  # Very high persistence
        persistence=0.8,
        involved_columns=["customer_id", "invoice_amount", "payment_date"],
        cycle_type=None,
        is_anomalous=True,  # Marked as anomalous
        anomaly_reason="Unusually high persistence for AR cycle",
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )

    classified = detect_financial_cycles(
        cycles=[anomalous_cycle], table_names=["transactions"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should be classified correctly
    assert classified_cycle.cycle_type == "accounts_receivable_cycle"

    # Anomaly flag should be preserved
    assert classified_cycle.is_anomalous is True
    assert classified_cycle.anomaly_reason == "Unusually high persistence for AR cycle"


def test_partial_pattern_match():
    """Test classification with partial column pattern matches."""
    # Cycle with only 2 matching columns (less obvious AR pattern)
    partial_ar_cycle = CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.1,
        death=0.4,
        persistence=0.3,
        involved_columns=[
            "customer_id",  # Matches AR pattern
            "invoice_total",  # Matches AR pattern
            "some_other_field",  # Doesn't match
            "random_column",  # Doesn't match
        ],
        cycle_type=None,
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )

    classified = detect_financial_cycles(
        cycles=[partial_ar_cycle], table_names=["sales"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should still classify as AR cycle (partial match is better than no match)
    assert classified_cycle.cycle_type == "accounts_receivable_cycle"


def test_empty_cycles_list():
    """Test handling of empty cycles list."""
    classified = detect_financial_cycles(
        cycles=[], table_names=["transactions"], column_relationships={}
    )

    assert classified == []


def test_cycle_with_no_columns():
    """Test handling of cycle with empty involved_columns."""
    empty_cycle = CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.1,
        death=0.3,
        persistence=0.2,
        involved_columns=[],  # Empty!
        cycle_type=None,
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )

    classified = detect_financial_cycles(
        cycles=[empty_cycle], table_names=["transactions"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should NOT be classified (no columns to match)
    assert classified_cycle.cycle_type is None


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    # Use uppercase column names
    uppercase_cycle = CycleDetection(
        cycle_id=str(uuid4()),
        dimension=1,
        birth=0.1,
        death=0.5,
        persistence=0.4,
        involved_columns=[
            "CUSTOMER_ID",  # Uppercase
            "INVOICE_NUMBER",  # Uppercase
            "PAYMENT_AMOUNT",  # Uppercase
        ],
        cycle_type=None,
        is_anomalous=False,
        first_detected=datetime.now(UTC),
        last_seen=datetime.now(UTC),
    )

    classified = detect_financial_cycles(
        cycles=[uppercase_cycle], table_names=["TRANSACTIONS"], column_relationships={}
    )

    assert len(classified) == 1
    classified_cycle = classified[0]

    # Should still classify as AR cycle (case-insensitive matching)
    assert classified_cycle.cycle_type == "accounts_receivable_cycle"
