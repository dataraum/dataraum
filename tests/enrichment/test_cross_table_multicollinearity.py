"""Tests for cross-table multicollinearity detection."""

from datetime import UTC, datetime

import pytest

from dataraum_context.analysis.relationships.models import (
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    SingleRelationshipJoin,
)
from dataraum_context.core.models.base import (
    Cardinality,
    RelationshipType,
)
from dataraum_context.enrichment.cross_table_multicollinearity import (
    gather_relationships,
)
from dataraum_context.enrichment.db_models import Relationship
from dataraum_context.quality.formatting.multicollinearity import (
    _format_cross_table_dependency_groups,
    _generate_cross_table_recommendations,
    _get_cross_table_group_recommendation,
    _get_cross_table_interpretation,
    format_cross_table_multicollinearity_for_llm,
)
from dataraum_context.storage import (
    Column,
    Source,
    Table,
)

# === Context Formatting Tests ===


def test_cross_table_interpretation_none():
    """Test interpretation for no cross-table multicollinearity."""
    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "customers"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=10,
        total_relationships_used=2,
        overall_condition_index=5.0,
        overall_severity="none",
        dependency_groups=[],
        cross_table_groups=[],
    )

    interp = _get_cross_table_interpretation(analysis)
    assert "no significant cross-table multicollinearity" in interp.lower()
    assert "2 tables" in interp.lower()
    assert "independent" in interp.lower()


def test_cross_table_interpretation_moderate():
    """Test interpretation for moderate cross-table multicollinearity."""
    join_path = SingleRelationshipJoin(
        from_table="orders",
        from_column="customer_id",
        to_table="customers",
        to_column="id",
        relationship_id="rel-1",
        relationship_type=RelationshipType.FOREIGN_KEY,
        cardinality=Cardinality.ONE_TO_MANY,
        confidence=0.95,
        detection_method="tda",
    )

    cross_table_group = CrossTableDependencyGroup(
        dimension=2,
        eigenvalue=0.05,
        condition_index=15.0,
        severity="moderate",
        involved_columns=[("orders", "customer_id"), ("customers", "id")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.6, 0.7],
        join_paths=[join_path],
        relationship_types=[RelationshipType.FOREIGN_KEY],
    )

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "customers"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=10,
        total_relationships_used=2,
        overall_condition_index=15.0,
        overall_severity="moderate",
        dependency_groups=[cross_table_group],
        cross_table_groups=[cross_table_group],
    )

    interp = _get_cross_table_interpretation(analysis)
    assert "moderate" in interp.lower()
    assert "1 dependency" in interp.lower()
    assert "2 tables" in interp.lower()
    assert "15.0" in interp


def test_cross_table_interpretation_severe():
    """Test interpretation for severe cross-table multicollinearity."""
    join_path = SingleRelationshipJoin(
        from_table="orders",
        from_column="total_price",
        to_table="invoices",
        to_column="amount",
        relationship_id="rel-1",
        relationship_type=RelationshipType.SEMANTIC,
        cardinality=None,
        confidence=0.85,
        detection_method="semantic",
    )

    severe_group = CrossTableDependencyGroup(
        dimension=3,
        eigenvalue=0.001,
        condition_index=45.0,
        severity="severe",
        involved_columns=[
            ("orders", "total_price"),
            ("invoices", "amount"),
            ("invoices", "tax_included_amount"),
        ],
        column_ids=["col-1", "col-2", "col-3"],
        variance_proportions=[0.8, 0.85, 0.9],
        join_paths=[join_path],
        relationship_types=[RelationshipType.SEMANTIC],
    )

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "invoices"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=15,
        total_relationships_used=3,
        overall_condition_index=45.0,
        overall_severity="severe",
        dependency_groups=[severe_group],
        cross_table_groups=[severe_group],
    )

    interp = _get_cross_table_interpretation(analysis)
    assert "severe" in interp.lower()
    assert "45.0" in interp
    assert "2 tables" in interp.lower()
    assert "redundancy" in interp.lower() or "overlap" in interp.lower()


def test_cross_table_group_recommendation_single_table():
    """Test recommendations for single-table groups."""
    # Single-table severe case
    group = CrossTableDependencyGroup(
        dimension=1,
        eigenvalue=0.01,
        condition_index=35.0,
        severity="severe",
        involved_columns=[("orders", "col1"), ("orders", "col2")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.9, 0.85],
        join_paths=[],
        relationship_types=[],
    )

    rec = _get_cross_table_group_recommendation(group)
    assert "single-table redundancy" in rec.lower()
    assert "orders" in rec.lower()
    assert "remove" in rec.lower()


def test_cross_table_group_recommendation_fk_severe():
    """Test recommendations for severe FK-based dependencies."""
    join_path = SingleRelationshipJoin(
        from_table="orders",
        from_column="customer_id",
        to_table="customers",
        to_column="id",
        relationship_id="rel-1",
        relationship_type=RelationshipType.FOREIGN_KEY,
        cardinality=Cardinality.ONE_TO_MANY,
        confidence=0.95,
        detection_method="tda",
    )

    group = CrossTableDependencyGroup(
        dimension=2,
        eigenvalue=0.001,
        condition_index=40.0,
        severity="severe",
        involved_columns=[("orders", "customer_id"), ("customers", "id")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.85, 0.9],
        join_paths=[join_path],
        relationship_types=[RelationshipType.FOREIGN_KEY],
    )

    rec = _get_cross_table_group_recommendation(group)
    # Should now correctly detect FK and say "redundancy"
    assert "severe cross-table" in rec.lower()
    assert "redundancy" in rec.lower()
    assert "foreign key" in rec.lower()
    assert "consolidating" in rec.lower() or "consolidate" in rec.lower()
    assert "orders" in rec.lower()
    assert "customers" in rec.lower()


def test_cross_table_group_recommendation_semantic_severe():
    """Test recommendations for severe semantic dependencies."""
    join_path = SingleRelationshipJoin(
        from_table="orders",
        from_column="total_amount",
        to_table="invoices",
        to_column="amount",
        relationship_id="rel-1",
        relationship_type=RelationshipType.SEMANTIC,
        cardinality=None,
        confidence=0.85,
        detection_method="semantic",
    )

    group = CrossTableDependencyGroup(
        dimension=3,
        eigenvalue=0.002,
        condition_index=38.0,
        severity="severe",
        involved_columns=[("orders", "total_amount"), ("invoices", "amount")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.88, 0.92],
        join_paths=[join_path],
        relationship_types=[RelationshipType.SEMANTIC],
    )

    rec = _get_cross_table_group_recommendation(group)
    # Semantic without FK falls into "else" branch - says "dependency" not "redundancy"
    assert "severe cross-table" in rec.lower()
    assert "dependency" in rec.lower() or "redundancy" in rec.lower()
    assert "linear combination" in rec.lower() or "denormalization" in rec.lower()


def test_cross_table_recommendations_none():
    """Test recommendations when no multicollinearity detected."""
    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "customers"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=10,
        total_relationships_used=2,
        overall_condition_index=5.0,
        overall_severity="none",
        dependency_groups=[],
        cross_table_groups=[],
    )

    recommendations = _generate_cross_table_recommendations(analysis)
    assert len(recommendations) == 1
    assert "no action needed" in recommendations[0].lower()
    assert "independence" in recommendations[0].lower()


def test_cross_table_recommendations_with_denormalization():
    """Test recommendations detect denormalization patterns."""
    # Create 2 FK-based groups to trigger denormalization warning
    join_path1 = SingleRelationshipJoin(
        from_table="orders",
        from_column="customer_name",
        to_table="customers",
        to_column="name",
        relationship_id="rel-1",
        relationship_type=RelationshipType.FOREIGN_KEY,
        cardinality=Cardinality.ONE_TO_MANY,
        confidence=0.9,
        detection_method="tda",
    )

    join_path2 = SingleRelationshipJoin(
        from_table="orders",
        from_column="customer_email",
        to_table="customers",
        to_column="email",
        relationship_id="rel-2",
        relationship_type=RelationshipType.FOREIGN_KEY,
        cardinality=Cardinality.ONE_TO_MANY,
        confidence=0.88,
        detection_method="tda",
    )

    group1 = CrossTableDependencyGroup(
        dimension=2,
        eigenvalue=0.05,
        condition_index=12.0,
        severity="moderate",
        involved_columns=[("orders", "customer_name"), ("customers", "name")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.6, 0.65],
        join_paths=[join_path1],
        relationship_types=[RelationshipType.FOREIGN_KEY],
    )

    group2 = CrossTableDependencyGroup(
        dimension=3,
        eigenvalue=0.04,
        condition_index=13.0,
        severity="moderate",
        involved_columns=[("orders", "customer_email"), ("customers", "email")],
        column_ids=["col-3", "col-4"],
        variance_proportions=[0.62, 0.68],
        join_paths=[join_path2],
        relationship_types=[RelationshipType.FOREIGN_KEY],
    )

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "customers"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=12,
        total_relationships_used=3,
        overall_condition_index=18.0,
        overall_severity="moderate",
        dependency_groups=[group1, group2],
        cross_table_groups=[group1, group2],
    )

    recommendations = _generate_cross_table_recommendations(analysis)

    # Should mention denormalization
    denorm_recs = [r for r in recommendations if "denormalization" in r.lower()]
    assert len(denorm_recs) > 0


def test_cross_table_recommendations_with_semantic_duplication():
    """Test recommendations detect semantic duplication."""
    join_path = SingleRelationshipJoin(
        from_table="orders",
        from_column="total",
        to_table="invoices",
        to_column="amount",
        relationship_id="rel-1",
        relationship_type=RelationshipType.SEMANTIC,
        cardinality=None,
        confidence=0.85,
        detection_method="semantic",
    )

    group = CrossTableDependencyGroup(
        dimension=2,
        eigenvalue=0.03,
        condition_index=14.0,
        severity="moderate",
        involved_columns=[("orders", "total"), ("invoices", "amount")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.65, 0.7],
        join_paths=[join_path],
        relationship_types=[RelationshipType.SEMANTIC],
    )

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "invoices"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=10,
        total_relationships_used=2,
        overall_condition_index=14.0,
        overall_severity="moderate",
        dependency_groups=[group],
        cross_table_groups=[group],
    )

    recommendations = _generate_cross_table_recommendations(analysis)

    # Should mention cross-table dependencies and moderate severity
    assert len(recommendations) > 0

    # Check for cross-table dependency mention
    cross_table_recs = [r for r in recommendations if "cross-table" in r.lower()]
    assert len(cross_table_recs) > 0

    # Should mention semantic OR general moderate advice
    has_semantic = any("semantic" in r.lower() for r in recommendations)
    has_moderate = any("moderate" in r.lower() for r in recommendations)
    assert has_semantic or has_moderate


def test_format_cross_table_multicollinearity_for_llm():
    """Test complete formatting for LLM consumption."""
    join_path = SingleRelationshipJoin(
        from_table="orders",
        from_column="customer_id",
        to_table="customers",
        to_column="id",
        relationship_id="rel-1",
        relationship_type=RelationshipType.FOREIGN_KEY,
        cardinality=Cardinality.ONE_TO_MANY,
        confidence=0.95,
        detection_method="tda",
    )

    group = CrossTableDependencyGroup(
        dimension=2,
        eigenvalue=0.05,
        condition_index=15.0,
        severity="moderate",
        involved_columns=[("orders", "customer_id"), ("customers", "id")],
        column_ids=["col-1", "col-2"],
        variance_proportions=[0.6, 0.7],
        join_paths=[join_path],
        relationship_types=[RelationshipType.FOREIGN_KEY],
    )

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "customers"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=10,
        total_relationships_used=2,
        overall_condition_index=15.0,
        overall_severity="moderate",
        dependency_groups=[group],
        cross_table_groups=[group],
    )

    result = format_cross_table_multicollinearity_for_llm(analysis)

    # Check top-level structure
    assert "cross_table_multicollinearity_assessment" in result
    assessment = result["cross_table_multicollinearity_assessment"]

    # Check required fields
    assert assessment["overall_severity"] == "moderate"
    assert "summary" in assessment
    assert assessment["scope"]["num_tables"] == 2
    assert assessment["scope"]["total_columns_analyzed"] == 10
    assert assessment["unified_analysis"]["overall_condition_index"] == 15.0

    # Check cross-table dependencies
    assert assessment["cross_table_dependencies"]["count"] == 1
    assert len(assessment["cross_table_dependencies"]["groups"]) == 1

    # Check dependency group formatting
    formatted_group = assessment["cross_table_dependencies"]["groups"][0]
    assert formatted_group["severity"] == "moderate"
    assert formatted_group["num_tables"] == 2
    assert "orders.customer_id" in formatted_group["columns"]
    assert "customers.id" in formatted_group["columns"]
    assert len(formatted_group["join_paths"]) == 1
    assert formatted_group["join_paths"][0]["from"] == "orders.customer_id"
    assert formatted_group["join_paths"][0]["to"] == "customers.id"

    # Check recommendations
    assert "recommendations" in assessment
    assert len(assessment["recommendations"]) > 0

    # Check technical details
    assert "technical_details" in assessment
    assert (
        assessment["technical_details"]["analysis_method"]
        == "Belsley VDP on unified correlation matrix"
    )


# === Integration Tests with Database ===


@pytest.mark.asyncio
async def test_gather_relationships_applies_thresholds(db_session):
    """Test that gather_relationships applies differentiated confidence thresholds."""
    # Create source
    source = Source(source_id="src1", name="test_source", source_type="csv")
    db_session.add(source)
    await db_session.flush()

    # Create tables
    table1 = Table(table_id="t1", source_id="src1", table_name="orders", layer="typed")
    table2 = Table(table_id="t2", source_id="src1", table_name="customers", layer="typed")

    db_session.add_all([table1, table2])
    await db_session.flush()

    # Create columns - need 3 pairs for 3 different relationships
    col1 = Column(
        column_id="c1",
        table_id="t1",
        column_name="customer_id",
        column_position=0,
        raw_type="INTEGER",
    )
    col2 = Column(
        column_id="c2",
        table_id="t2",
        column_name="id",
        column_position=0,
        raw_type="INTEGER",
    )
    col3 = Column(
        column_id="c3",
        table_id="t1",
        column_name="customer_name",
        column_position=1,
        raw_type="VARCHAR",
    )
    col4 = Column(
        column_id="c4",
        table_id="t2",
        column_name="name",
        column_position=1,
        raw_type="VARCHAR",
    )
    col5 = Column(
        column_id="c5",
        table_id="t1",
        column_name="customer_email",
        column_position=2,
        raw_type="VARCHAR",
    )
    col6 = Column(
        column_id="c6",
        table_id="t2",
        column_name="email",
        column_position=2,
        raw_type="VARCHAR",
    )

    db_session.add_all([col1, col2, col3, col4, col5, col6])
    await db_session.flush()

    # Create relationships with different types and confidences (different column pairs)
    rel1 = Relationship(
        relationship_id="rel-1",
        from_table_id="t1",
        from_column_id="c1",
        to_table_id="t2",
        to_column_id="c2",
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=0.75,  # Above FK threshold (0.7)
        detection_method="tda",
    )

    rel2 = Relationship(
        relationship_id="rel-2",
        from_table_id="t1",
        from_column_id="c3",  # Different column pair
        to_table_id="t2",
        to_column_id="c4",
        relationship_type=RelationshipType.SEMANTIC,
        confidence=0.55,  # Below semantic threshold (0.6) - should be filtered
        detection_method="semantic",
    )

    rel3 = Relationship(
        relationship_id="rel-3",
        from_table_id="t1",
        from_column_id="c5",  # Different column pair
        to_table_id="t2",
        to_column_id="c6",
        relationship_type=RelationshipType.CORRELATION,
        confidence=0.52,  # Above correlation threshold (0.5)
        detection_method="profiling",
    )

    db_session.add_all([rel1, rel2, rel3])
    await db_session.commit()

    # Test

    relationships = await gather_relationships(["t1", "t2"], db_session)

    # Should have 2 relationships (rel2 filtered out)
    assert len(relationships) == 2

    # Check that rel2 was filtered
    rel_ids = [r.relationship_id for r in relationships]
    assert "rel-1" in rel_ids
    assert "rel-2" not in rel_ids  # Filtered due to low confidence
    assert "rel-3" in rel_ids


@pytest.mark.asyncio
async def test_gather_relationships_returns_multiple_relationships(db_session):
    """Test that gather_relationships returns all qualifying relationships."""
    # Create source
    source = Source(source_id="src1", name="test_source", source_type="csv")
    db_session.add(source)
    await db_session.flush()

    # Create tables
    table1 = Table(table_id="t1", source_id="src1", table_name="orders", layer="typed")
    table2 = Table(table_id="t2", source_id="src1", table_name="customers", layer="typed")

    db_session.add_all([table1, table2])
    await db_session.flush()

    # Create columns for two different relationships
    col1 = Column(
        column_id="c1",
        table_id="t1",
        column_name="customer_id",
        column_position=0,
        raw_type="INTEGER",
    )
    col2 = Column(
        column_id="c2",
        table_id="t2",
        column_name="id",
        column_position=0,
        raw_type="INTEGER",
    )
    col3 = Column(
        column_id="c3",
        table_id="t1",
        column_name="customer_name",
        column_position=1,
        raw_type="VARCHAR",
    )
    col4 = Column(
        column_id="c4",
        table_id="t2",
        column_name="name",
        column_position=1,
        raw_type="VARCHAR",
    )

    db_session.add_all([col1, col2, col3, col4])
    await db_session.flush()

    # Create two relationships with different column pairs
    rel1 = Relationship(
        relationship_id="rel-1",
        from_table_id="t1",
        from_column_id="c1",
        to_table_id="t2",
        to_column_id="c2",
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=0.95,
        detection_method="tda",
    )

    rel2 = Relationship(
        relationship_id="rel-2",
        from_table_id="t1",
        from_column_id="c3",
        to_table_id="t2",
        to_column_id="c4",
        relationship_type=RelationshipType.SEMANTIC,
        confidence=0.85,
        detection_method="semantic",
    )

    db_session.add_all([rel1, rel2])
    await db_session.commit()

    # Test

    relationships = await gather_relationships(["t1", "t2"], db_session)

    # Should return both relationships
    assert len(relationships) == 2
    confidences = sorted([r.confidence for r in relationships], reverse=True)
    assert confidences == [0.95, 0.85]


@pytest.mark.asyncio
async def test_gather_relationships_empty_when_no_relationships(db_session):
    """Test that gather_relationships returns empty list when no relationships exist."""
    # Create source
    source = Source(source_id="src1", name="test_source", source_type="csv")
    db_session.add(source)
    await db_session.flush()

    # Create tables without relationships
    table1 = Table(table_id="t1", source_id="src1", table_name="orders", layer="typed")
    table2 = Table(table_id="t2", source_id="src1", table_name="customers", layer="typed")

    db_session.add_all([table1, table2])
    await db_session.commit()

    # Test

    relationships = await gather_relationships(["t1", "t2"], db_session)

    assert len(relationships) == 0


# === Edge Case Tests ===


def test_cross_table_group_with_many_columns():
    """Test formatting handles groups with many columns correctly."""
    # Create a group with 5 columns
    group = CrossTableDependencyGroup(
        dimension=2,
        eigenvalue=0.01,
        condition_index=25.0,
        severity="moderate",
        involved_columns=[
            ("orders", "col1"),
            ("orders", "col2"),
            ("customers", "col3"),
            ("invoices", "col4"),
            ("invoices", "col5"),
        ],
        column_ids=["c1", "c2", "c3", "c4", "c5"],
        variance_proportions=[0.6, 0.65, 0.7, 0.68, 0.72],
        join_paths=[],
        relationship_types=[RelationshipType.SEMANTIC],
    )

    rec = _get_cross_table_group_recommendation(group)

    # Should truncate to 3 columns and mention "and 2 others"
    assert "orders.col1" in rec
    assert "orders.col2" in rec
    assert "customers.col3" in rec
    assert "and 2 others" in rec


def test_format_dependency_groups_empty_list():
    """Test formatting handles empty dependency groups list."""
    formatted = _format_cross_table_dependency_groups([])
    assert formatted == []


def test_cross_table_interpretation_single_table():
    """Test interpretation when dependency groups only involve single tables."""
    # Create a "cross-table" analysis with only single-table groups
    group = CrossTableDependencyGroup(
        dimension=1,
        eigenvalue=0.05,
        condition_index=12.0,
        severity="moderate",
        involved_columns=[("orders", "col1"), ("orders", "col2")],
        column_ids=["c1", "c2"],
        variance_proportions=[0.6, 0.65],
        join_paths=[],
        relationship_types=[],
    )

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=["table1", "table2"],
        table_names=["orders", "customers"],
        computed_at=datetime.now(UTC),
        total_columns_analyzed=10,
        total_relationships_used=0,
        overall_condition_index=12.0,
        overall_severity="moderate",
        dependency_groups=[group],
        cross_table_groups=[],  # No actual cross-table groups
    )

    interp = _get_cross_table_interpretation(analysis)

    # Should mention minimal cross-table dependencies
    assert "moderate" in interp.lower()
    assert "minimal cross-table" in interp.lower()
