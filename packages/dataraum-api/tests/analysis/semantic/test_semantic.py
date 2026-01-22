"""Tests for semantic enrichment."""

from unittest.mock import MagicMock
from uuid import uuid4

from dataraum.analysis.semantic import (
    EntityDetection,
    Relationship,
    SemanticAnnotation,
    SemanticEnrichmentResult,
    enrich_semantic,
)
from dataraum.core.models.base import (
    ColumnRef,
    DecisionSource,
    RelationshipType,
    Result,
    SemanticRole,
)


def test_enrich_semantic_stores_annotations(session):
    """Test that semantic enrichment stores annotations in database."""
    # Create mock semantic agent
    agent = MagicMock()
    agent.analyze = MagicMock()

    # Setup test data - create tables and columns
    from dataraum.storage import Column, Source, Table

    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        source_id=source.source_id,
        table_name="customers",
        layer="raw",
        row_count=100,
    )
    session.add(table)
    session.flush()

    col1 = Column(
        table_id=table.table_id,
        column_name="customer_id",
        column_position=0,
        raw_type="VARCHAR",
    )
    col2 = Column(
        table_id=table.table_id,
        column_name="revenue",
        column_position=1,
        raw_type="VARCHAR",
    )
    session.add(col1)
    session.add(col2)
    session.commit()

    # Mock LLM response
    mock_result = SemanticEnrichmentResult(
        annotations=[
            SemanticAnnotation(
                column_id=col1.column_id,
                column_ref=ColumnRef(table_name="customers", column_name="customer_id"),
                semantic_role=SemanticRole.KEY,
                entity_type="customer",
                business_name="Customer ID",
                business_description="Unique identifier for customers",
                annotation_source=DecisionSource.LLM,
                annotated_by="test-model",
                confidence=0.9,
            ),
            SemanticAnnotation(
                column_id=col2.column_id,
                column_ref=ColumnRef(table_name="customers", column_name="revenue"),
                semantic_role=SemanticRole.MEASURE,
                entity_type=None,
                business_name="Revenue",
                business_description="Total revenue amount",
                annotation_source=DecisionSource.LLM,
                annotated_by="test-model",
                confidence=0.85,
            ),
        ],
        entity_detections=[
            EntityDetection(
                table_id=table.table_id,
                table_name="customers",
                entity_type="customer",
                description="Customer master data",
                confidence=0.9,
                evidence={},
                grain_columns=["customer_id"],
                is_fact_table=False,
                is_dimension_table=True,
            )
        ],
        relationships=[],
        source="llm",
    )

    agent.analyze.return_value = Result.ok(mock_result)

    # Run enrichment
    result = enrich_semantic(
        session=session,
        agent=agent,
        table_ids=[table.table_id],
        ontology="general",
    )

    # Verify result
    assert result.success, f"Enrichment failed: {result.error}"
    assert result.value is not None
    assert len(result.value.annotations) == 2
    assert len(result.value.entity_detections) == 1

    # Verify annotations stored in database
    from sqlalchemy import select

    from dataraum.analysis.semantic import SemanticAnnotationDB

    stmt = select(SemanticAnnotationDB)
    db_result = session.execute(stmt)
    annotations = db_result.scalars().all()

    assert len(annotations) == 2
    assert annotations[0].semantic_role == "key"
    assert annotations[0].business_name == "Customer ID"
    assert annotations[1].semantic_role == "measure"
    assert annotations[1].business_name == "Revenue"

    # Verify entity detection stored
    from dataraum.analysis.semantic import TableEntity

    stmt = select(TableEntity)
    db_result = session.execute(stmt)
    entities = db_result.scalars().all()

    assert len(entities) == 1
    assert entities[0].detected_entity_type == "customer"
    assert entities[0].is_dimension_table is True


def test_enrich_semantic_handles_missing_columns(session):
    """Test that enrichment handles references to non-existent columns gracefully."""
    agent = MagicMock()
    agent.analyze = MagicMock()

    # Create test table
    from dataraum.storage import Column, Source, Table

    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        source_id=source.source_id,
        table_name="products",
        layer="raw",
        row_count=50,
    )
    session.add(table)
    session.flush()

    col1 = Column(
        table_id=table.table_id,
        column_name="product_id",
        column_position=0,
        raw_type="VARCHAR",
    )
    session.add(col1)
    session.commit()

    # Mock LLM response with reference to non-existent column
    mock_result = SemanticEnrichmentResult(
        annotations=[
            SemanticAnnotation(
                column_id="",
                column_ref=ColumnRef(table_name="products", column_name="nonexistent_col"),
                semantic_role=SemanticRole.ATTRIBUTE,
                entity_type=None,
                business_name="Non-existent",
                business_description="This column doesn't exist",
                annotation_source=DecisionSource.LLM,
                annotated_by="test-model",
                confidence=0.8,
            ),
        ],
        entity_detections=[],
        relationships=[],
        source="llm",
    )

    agent.analyze.return_value = Result.ok(mock_result)

    # Run enrichment - should not fail
    result = enrich_semantic(
        session=session,
        agent=agent,
        table_ids=[table.table_id],
        ontology="general",
    )

    # Should succeed but skip the non-existent column
    assert result.success

    # Verify no annotations stored
    from sqlalchemy import select

    from dataraum.analysis.semantic import SemanticAnnotationDB

    stmt = select(SemanticAnnotationDB)
    db_result = session.execute(stmt)
    annotations = db_result.scalars().all()

    assert len(annotations) == 0


def test_enrich_semantic_stores_relationships(session):
    """Test that enrichment stores detected relationships."""
    agent = MagicMock()
    agent.analyze = MagicMock()

    # Create two related tables
    from dataraum.storage import Column, Source, Table

    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    customers_table = Table(
        source_id=source.source_id,
        table_name="customers",
        layer="raw",
        row_count=100,
    )
    orders_table = Table(
        source_id=source.source_id,
        table_name="orders",
        layer="raw",
        row_count=500,
    )
    session.add_all([customers_table, orders_table])
    session.flush()

    cust_id_col = Column(
        table_id=customers_table.table_id,
        column_name="customer_id",
        column_position=0,
    )
    order_cust_col = Column(
        table_id=orders_table.table_id,
        column_name="customer_id",
        column_position=1,
    )
    session.add_all([cust_id_col, order_cust_col])
    session.commit()

    # Mock LLM response with relationship
    mock_result = SemanticEnrichmentResult(
        annotations=[],
        entity_detections=[],
        relationships=[
            Relationship(
                relationship_id=str(uuid4()),
                from_table="orders",
                from_column="customer_id",
                to_table="customers",
                to_column="customer_id",
                relationship_type=RelationshipType.FOREIGN_KEY,
                cardinality=None,
                confidence=0.95,
                detection_method="llm",
                evidence={"source": "semantic_analysis"},
            )
        ],
        source="llm",
    )

    agent.analyze.return_value = Result.ok(mock_result)

    # Run enrichment
    result = enrich_semantic(
        session=session,
        agent=agent,
        table_ids=[customers_table.table_id, orders_table.table_id],
        ontology="general",
    )

    assert result.success

    # Verify relationship stored
    from sqlalchemy import select

    from dataraum.analysis.relationships import Relationship as RelationshipModel

    stmt = select(RelationshipModel)
    db_result = session.execute(stmt)
    relationships = db_result.scalars().all()

    assert len(relationships) == 1
    assert relationships[0].relationship_type == "foreign_key"
    assert relationships[0].confidence == 0.95
    assert relationships[0].detection_method == "llm"
