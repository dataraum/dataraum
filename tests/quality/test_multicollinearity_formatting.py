"""Tests for multicollinearity LLM formatting."""

from datetime import UTC, datetime

from dataraum_context.analysis.correlation.models import (
    ColumnVIF,
    ConditionIndexAnalysis,
    MulticollinearityAnalysis,
)
from dataraum_context.core.models.base import ColumnRef
from dataraum_context.quality.formatting.multicollinearity import (
    _generate_multicollinearity_recommendations,
    _get_multicollinearity_interpretation,
    _get_vif_impact,
    _get_vif_interpretation,
    _get_vif_severity_label,
    format_multicollinearity_for_llm,
)


def test_vif_severity_labels():
    """Test VIF severity label assignment based on configured thresholds.

    Default thresholds: none=1.0, low=2.5, moderate=5.0, high=10.0
    Values above high threshold get default_severity="severe"
    """
    # At or below none threshold
    assert _get_vif_severity_label(1.0) == "none"
    assert _get_vif_severity_label(0.8) == "none"

    # Between none and low (1.0 < x <= 2.5)
    assert _get_vif_severity_label(2.0) == "low"
    assert _get_vif_severity_label(2.5) == "low"

    # Between low and moderate (2.5 < x <= 5.0)
    assert _get_vif_severity_label(3.0) == "moderate"
    assert _get_vif_severity_label(5.0) == "moderate"

    # Between moderate and high (5.0 < x <= 10.0)
    assert _get_vif_severity_label(7.5) == "high"
    assert _get_vif_severity_label(10.0) == "high"

    # Above high threshold (x > 10.0) - uses default_severity
    assert _get_vif_severity_label(15.0) == "severe"
    assert _get_vif_severity_label(100.0) == "severe"


def test_vif_interpretation():
    """Test natural language interpretation of VIF values."""
    # No multicollinearity
    interp = _get_vif_interpretation(1.0)
    assert "no multicollinearity" in interp.lower()

    # Low to moderate
    interp = _get_vif_interpretation(3.0)
    assert "low to moderate" in interp.lower()
    assert "acceptable" in interp.lower()

    # High
    interp = _get_vif_interpretation(7.0)
    assert "high" in interp.lower()
    assert "attention" in interp.lower()

    # Serious
    interp = _get_vif_interpretation(15.0)
    assert "serious" in interp.lower()
    assert "problematic" in interp.lower()


def test_vif_impact_explanations():
    """Test that VIF impact explanations are informative."""
    # No inflation
    impact = _get_vif_impact(1.0)
    assert "not inflated" in impact.lower()

    # Moderate inflation
    impact = _get_vif_impact(3.0)
    assert "inflated" in impact.lower()
    assert "%" in impact  # Should mention percentage

    # High inflation
    impact = _get_vif_impact(8.0)
    assert "variable" in impact.lower() or "redundancy" in impact.lower()

    # Severe - should mention redundancy percentage
    impact = _get_vif_impact(20.0)
    assert "redundant" in impact.lower()
    assert "%" in impact


def test_multicollinearity_interpretation_none():
    """Test interpretation for no multicollinearity."""
    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="none",
        num_problematic_columns=0,
    )

    interp = _get_multicollinearity_interpretation(analysis)
    assert "no significant multicollinearity" in interp.lower()
    assert "independent" in interp.lower()


def test_multicollinearity_interpretation_moderate():
    """Test interpretation for moderate multicollinearity."""
    # CI between 10-30 is moderate
    ci = ConditionIndexAnalysis(
        condition_index=20.0,
        eigenvalues=[2.0, 1.0, 0.1],
        has_multicollinearity=True,
        severity="moderate",
        problematic_dimensions=1,
    )

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="moderate",
        num_problematic_columns=2,
        condition_index=ci,
    )

    interp = _get_multicollinearity_interpretation(analysis)
    assert "moderate" in interp.lower()
    assert "2 column" in interp.lower()
    assert "20.0" in interp


def test_multicollinearity_interpretation_severe():
    """Test interpretation for severe multicollinearity."""
    ci = ConditionIndexAnalysis(
        condition_index=150.0,
        eigenvalues=[3.0, 1.0, 0.002],
        has_multicollinearity=True,
        severity="severe",
        problematic_dimensions=1,
    )

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="severe",
        num_problematic_columns=3,
        condition_index=ci,
        has_severe_multicollinearity=True,
    )

    interp = _get_multicollinearity_interpretation(analysis)
    assert "severe" in interp.lower()
    assert "3 columns" in interp.lower()
    assert "150.0" in interp
    assert "derived" in interp.lower() or "duplicate" in interp.lower()


def test_recommendations_none():
    """Test recommendations when no multicollinearity."""
    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="none",
    )

    recs = _generate_multicollinearity_recommendations(analysis)
    assert len(recs) == 1
    assert "no action needed" in recs[0].lower()


def test_recommendations_severe_columns():
    """Test recommendations for severe column-level multicollinearity."""
    vifs = [
        ColumnVIF(
            column_id="col1",
            column_ref=ColumnRef(table_name="test", column_name="TotalRevenue"),
            vif=15.0,
            tolerance=0.067,
            has_multicollinearity=True,
            severity="severe",
            correlated_with=["col2", "col3"],
        ),
        ColumnVIF(
            column_id="col2",
            column_ref=ColumnRef(table_name="test", column_name="GrossRevenue"),
            vif=12.0,
            tolerance=0.083,
            has_multicollinearity=True,
            severity="severe",
            correlated_with=["col1"],
        ),
    ]

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="severe",
        column_vifs=vifs,
        num_problematic_columns=2,
        has_severe_multicollinearity=True,
    )

    recs = _generate_multicollinearity_recommendations(analysis)

    # Should have critical recommendation
    critical_rec = [r for r in recs if "Critical" in r or "🔴" in r]
    assert len(critical_rec) > 0
    assert "TotalRevenue" in critical_rec[0] or "GrossRevenue" in critical_rec[0]


def test_recommendations_high_columns():
    """Test recommendations for high (but not severe) multicollinearity."""
    vifs = [
        ColumnVIF(
            column_id="col1",
            column_ref=ColumnRef(table_name="test", column_name="Price"),
            vif=7.0,
            tolerance=0.143,
            has_multicollinearity=False,
            severity="moderate",
        ),
        ColumnVIF(
            column_id="col2",
            column_ref=ColumnRef(table_name="test", column_name="Cost"),
            vif=6.5,
            tolerance=0.154,
            has_multicollinearity=False,
            severity="moderate",
        ),
    ]

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="moderate",
        column_vifs=vifs,
        num_problematic_columns=0,
    )

    recs = _generate_multicollinearity_recommendations(analysis)

    # Should have attention recommendation
    attention_rec = [r for r in recs if "Attention" in r or "🟡" in r]
    assert len(attention_rec) > 0


def test_recommendations_derived_columns():
    """Test recommendations detect derived columns."""
    vifs = [
        ColumnVIF(
            column_id="col1",
            column_ref=ColumnRef(table_name="test", column_name="Total"),
            vif=20.0,  # Very high
            tolerance=0.05,
            has_multicollinearity=True,
            severity="severe",
            correlated_with=["col2", "col3", "col4"],  # Multiple correlations
        ),
    ]

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="severe",
        column_vifs=vifs,
        num_problematic_columns=1,
        has_severe_multicollinearity=True,
    )

    recs = _generate_multicollinearity_recommendations(analysis)

    # Should mention derived columns
    derived_rec = [r for r in recs if "calculated" in r.lower() or "derived" in r.lower()]
    assert len(derived_rec) > 0


def test_recommendations_severe_condition_index():
    """Test recommendations for severe table-level multicollinearity."""
    ci = ConditionIndexAnalysis(
        condition_index=150.0,
        eigenvalues=[3.0, 1.0, 0.002],
        has_multicollinearity=True,
        severity="severe",
        problematic_dimensions=1,
    )

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="severe",
        condition_index=ci,
        has_severe_multicollinearity=True,
    )

    recs = _generate_multicollinearity_recommendations(analysis)

    # Should have table-level recommendation (CI > 30 is severe)
    table_rec = [
        r for r in recs if "table-level" in r.lower() or "condition index > 30" in r.lower()
    ]
    assert len(table_rec) > 0


def test_format_multicollinearity_for_llm_structure():
    """Test that formatted output has correct structure for LLM."""
    vifs = [
        ColumnVIF(
            column_id="col1",
            column_ref=ColumnRef(table_name="test", column_name="Revenue"),
            vif=8.0,
            tolerance=0.125,
            has_multicollinearity=False,
            severity="moderate",
            correlated_with=["col2"],
        ),
    ]

    ci = ConditionIndexAnalysis(
        condition_index=45.0,
        eigenvalues=[2.5, 1.0, 0.055],
        has_multicollinearity=True,
        severity="moderate",
        problematic_dimensions=1,
    )

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="moderate",
        column_vifs=vifs,
        num_problematic_columns=0,
        condition_index=ci,
    )

    formatted = format_multicollinearity_for_llm(analysis)

    # Check top-level structure
    assert "multicollinearity_assessment" in formatted
    assessment = formatted["multicollinearity_assessment"]

    # Check required fields
    assert "overall_severity" in assessment
    assert "summary" in assessment
    assert "num_problematic_columns" in assessment
    assert "table_level" in assessment
    assert "problematic_columns" in assessment
    assert "recommendations" in assessment
    assert "technical_details" in assessment

    # Verify table level details
    assert assessment["table_level"]["condition_index"] == 45.0
    assert assessment["table_level"]["severity"] == "moderate"

    # Verify technical details
    assert "VIF" in assessment["technical_details"]["analysis_method"]


def test_format_only_includes_problematic_columns():
    """Test that only columns with VIF > 4 are included in output."""
    vifs = [
        ColumnVIF(
            column_id="col1",
            column_ref=ColumnRef(table_name="test", column_name="Good"),
            vif=2.0,
            tolerance=0.5,
            has_multicollinearity=False,
            severity="none",
        ),
        ColumnVIF(
            column_id="col2",
            column_ref=ColumnRef(table_name="test", column_name="Problematic"),
            vif=8.0,
            tolerance=0.125,
            has_multicollinearity=False,
            severity="moderate",
        ),
    ]

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="moderate",
        column_vifs=vifs,
    )

    formatted = format_multicollinearity_for_llm(analysis)
    problematic = formatted["multicollinearity_assessment"]["problematic_columns"]

    # Should only include the one with VIF > 4
    assert len(problematic) == 1
    assert problematic[0]["column"] == "Problematic"
    assert problematic[0]["vif"] == 8.0


def test_format_includes_correlation_info():
    """Test that correlated columns are included when available."""
    vifs = [
        ColumnVIF(
            column_id="col1",
            column_ref=ColumnRef(table_name="test", column_name="Total"),
            vif=12.0,
            tolerance=0.083,
            has_multicollinearity=True,
            severity="severe",
            correlated_with=["col2", "col3", "col4", "col5"],  # More than 3
        ),
    ]

    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="severe",
        column_vifs=vifs,
        num_problematic_columns=1,
        has_severe_multicollinearity=True,
    )

    formatted = format_multicollinearity_for_llm(analysis)
    problematic = formatted["multicollinearity_assessment"]["problematic_columns"]

    # Should include correlated columns (limited to 3)
    assert "highly_correlated_with" in problematic[0]
    assert len(problematic[0]["highly_correlated_with"]) == 3


def test_format_no_table_level_when_missing():
    """Test that table_level is None when Condition Index not computed."""
    analysis = MulticollinearityAnalysis(
        table_id="test-table",
        table_name="test",
        computed_at=datetime.now(UTC),
        overall_severity="none",
        condition_index=None,  # No CI computed
    )

    formatted = format_multicollinearity_for_llm(analysis)

    assert formatted["multicollinearity_assessment"]["table_level"] is None
