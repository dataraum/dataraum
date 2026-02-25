"""E2E tests: verify GraphExecutionContext built from real pipeline output.

After the full pipeline (including LLM phases) runs against testdata,
`build_execution_context()` should produce a rich, populated context.
"""

from __future__ import annotations

import pytest

from dataraum.core.connections import ConnectionManager
from dataraum.graphs.context import GraphExecutionContext, build_execution_context

pytestmark = pytest.mark.e2e


@pytest.fixture
def execution_context(
    output_manager: ConnectionManager,
    typed_table_ids: list[str],
) -> GraphExecutionContext:
    """Build execution context from the real pipeline output."""
    with output_manager.session_scope() as session:
        with output_manager.duckdb_cursor() as cursor:
            return build_execution_context(
                session=session,
                table_ids=typed_table_ids,
                duckdb_conn=cursor,
            )


# =============================================================================
# Context completeness
# =============================================================================


class TestContextCompleteness:
    """Verify build_execution_context populates all expected sections."""

    def test_all_tables_present(self, execution_context: GraphExecutionContext) -> None:
        """Context should contain all 8 typed tables."""
        assert len(execution_context.tables) == 8, (
            f"Expected 8 tables, got {len(execution_context.tables)}: "
            f"{[t.table_name for t in execution_context.tables]}"
        )

    def test_tables_have_columns(self, execution_context: GraphExecutionContext) -> None:
        """Each table should have columns."""
        for table in execution_context.tables:
            assert len(table.columns) > 0, f"{table.table_name} has no columns"

    def test_tables_have_row_counts(self, execution_context: GraphExecutionContext) -> None:
        """Tables should have row counts."""
        for table in execution_context.tables:
            assert table.row_count is not None and table.row_count > 0, (
                f"{table.table_name}: row_count={table.row_count}"
            )

    def test_aggregate_stats(self, execution_context: GraphExecutionContext) -> None:
        """Aggregate statistics should be populated."""
        assert execution_context.total_tables == 8
        assert execution_context.total_columns > 0


# =============================================================================
# Semantic context (requires LLM semantic phase)
# =============================================================================


class TestSemanticContext:
    """Verify semantic annotations flow into execution context."""

    def test_columns_have_data_types(self, execution_context: GraphExecutionContext) -> None:
        """Columns should have data_type populated."""
        all_cols = [c for t in execution_context.tables for c in t.columns]
        with_type = [c for c in all_cols if c.data_type]
        assert len(with_type) > len(all_cols) * 0.8

    def test_columns_have_semantic_roles(self, execution_context: GraphExecutionContext) -> None:
        """Some columns should have semantic_role from LLM annotation."""
        all_cols = [c for t in execution_context.tables for c in t.columns]
        with_role = [c for c in all_cols if c.semantic_role]
        assert len(with_role) > 0, "No columns have semantic_role"

    def test_columns_have_business_concept(self, execution_context: GraphExecutionContext) -> None:
        """Some columns should have business_concept from ontology mapping."""
        all_cols = [c for t in execution_context.tables for c in t.columns]
        with_concept = [c for c in all_cols if c.business_concept]
        assert len(with_concept) > 0, "No columns have business_concept"


# =============================================================================
# Relationships in context
# =============================================================================


class TestRelationshipContext:
    """Verify relationships are present in execution context."""

    def test_relationships_present(self, execution_context: GraphExecutionContext) -> None:
        """Context should contain LLM-confirmed relationships."""
        assert len(execution_context.relationships) > 0, "No relationships in context"

    def test_relationship_fields_complete(self, execution_context: GraphExecutionContext) -> None:
        """Relationships should have all fields populated."""
        for rel in execution_context.relationships:
            assert rel.from_table, "from_table empty"
            assert rel.from_column, "from_column empty"
            assert rel.to_table, "to_table empty"
            assert rel.to_column, "to_column empty"


# =============================================================================
# Quality context
# =============================================================================


class TestQualityContext:
    """Verify quality information flows into context."""

    def test_quality_grades_present(self, execution_context: GraphExecutionContext) -> None:
        """Some columns should have quality grades."""
        all_cols = [c for t in execution_context.tables for c in t.columns]
        with_grade = [c for c in all_cols if c.quality_grade]
        assert len(with_grade) > 0, (
            f"No columns have quality_grade out of {len(all_cols)}"
        )

    def test_quality_grades_valid(self, execution_context: GraphExecutionContext) -> None:
        """Quality grades should be valid letter grades."""
        valid = {"A", "B", "C", "D", "F"}
        for table in execution_context.tables:
            for col in table.columns:
                if col.quality_grade:
                    assert col.quality_grade in valid, (
                        f"{col.table_name}.{col.column_name}: "
                        f"invalid grade '{col.quality_grade}'"
                    )


# =============================================================================
# Graph topology
# =============================================================================


class TestGraphTopology:
    """Verify graph topology analysis."""

    def test_graph_pattern_detected(self, execution_context: GraphExecutionContext) -> None:
        """Graph pattern should be detected (star, mesh, etc.)."""
        assert execution_context.graph_pattern is not None, "No graph pattern detected"

    def test_hub_tables_identified(self, execution_context: GraphExecutionContext) -> None:
        """Hub tables should be identified."""
        assert len(execution_context.hub_tables) > 0, "No hub tables identified"


# =============================================================================
# Field mappings
# =============================================================================


class TestFieldMappings:
    """Verify field mappings resolve business concepts to columns."""

    def test_field_mappings_loaded(self, execution_context: GraphExecutionContext) -> None:
        """Field mappings should be loaded from semantic annotations."""
        assert execution_context.field_mappings is not None, "No field mappings"
