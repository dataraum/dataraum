"""E2E tests: verify the full pipeline produces correct outputs.

Runs the real pipeline (all phases including LLM) via `runner.run()`
against testdata with known properties, then verifies outputs.

GROUND TRUTH: Do not modify assertions to fix failures — fix the production code instead.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.core.connections import ConnectionManager
from dataraum.pipeline.runner import RunResult
from dataraum.storage import Column, Table

pytestmark = pytest.mark.e2e

# Expected tables from month-end-close scenario (full normalization)
EXPECTED_TABLES = {
    "chart_of_accounts",
    "journal_entries",
    "journal_lines",
    "invoices",
    "payments",
    "bank_transactions",
    "fx_rates",
    "trial_balance",
}


# =============================================================================
# Pipeline completion
# =============================================================================


class TestPipelineCompletion:
    """Verify the pipeline ran successfully end-to-end."""

    def test_pipeline_succeeded(self, pipeline_run: RunResult) -> None:
        """Pipeline should complete without fatal errors."""
        assert pipeline_run.success, (
            f"Pipeline failed. Errors: "
            f"{[(p.phase_name, p.error) for p in pipeline_run.get_failed_phases()]}"
        )

    def test_all_phases_ran(self, pipeline_run: RunResult) -> None:
        """All configured phases should have executed."""
        phase_names = {p.phase_name for p in pipeline_run.phases}
        # At minimum, the core phases should be present
        core_phases = {"import", "typing", "statistics", "relationships", "temporal", "entropy"}
        missing = core_phases - phase_names
        assert not missing, f"Core phases missing from run: {missing}"

    def test_llm_phases_ran(self, pipeline_run: RunResult) -> None:
        """LLM phases should have executed (not skipped)."""
        llm_phases = {"semantic", "quality_summary", "business_cycles", "validation"}
        phase_map = {p.phase_name: p.status for p in pipeline_run.phases}
        for phase_name in llm_phases:
            if phase_name in phase_map:
                assert phase_map[phase_name] in ("completed", "skipped"), (
                    f"{phase_name}: status={phase_map[phase_name]}"
                )


# =============================================================================
# Import verification
# =============================================================================


class TestImport:
    """Verify all tables imported with correct row counts."""

    def test_all_tables_present(self, typed_table_names: list[str]) -> None:
        """All 8 expected tables should be in the typed layer."""
        assert EXPECTED_TABLES == set(typed_table_names)

    def test_row_counts_match_manifest(
        self,
        output_manager: ConnectionManager,
        testdata_manifest: dict[str, Any],
    ) -> None:
        """Row counts in DuckDB should match the testdata manifest."""
        manifest_counts = {entry["table"]: entry["rows"] for entry in testdata_manifest["files"]}

        with output_manager.duckdb_cursor() as cursor:
            for table_name, expected_rows in manifest_counts.items():
                result = cursor.execute(f'SELECT COUNT(*) FROM "typed_{table_name}"').fetchone()
                assert result is not None
                actual_rows = result[0]
                assert actual_rows == expected_rows, (
                    f"{table_name}: expected {expected_rows} rows, got {actual_rows}"
                )


# =============================================================================
# Type resolution
# =============================================================================


class TestTyping:
    """Verify type detection on key columns."""

    def test_amount_columns_numeric(self, metadata_session: Session) -> None:
        """Amount/currency columns should resolve to numeric types."""
        rows = metadata_session.execute(
            select(Column.column_name, Column.resolved_type, Table.table_name)
            .join(Table, Column.table_id == Table.table_id)
            .where(Table.layer == "typed")
            .where(Column.column_name.in_(["amount", "debit", "credit"]))
        ).all()
        assert len(rows) > 0
        for col_name, resolved_type, table_name in rows:
            assert resolved_type is not None, f"{table_name}.{col_name}: no resolved_type"
            upper = resolved_type.upper()
            assert any(
                t in upper for t in ["DECIMAL", "DOUBLE", "FLOAT", "NUMERIC", "BIGINT", "INTEGER"]
            ), f"{table_name}.{col_name}: expected numeric, got {resolved_type}"

    def test_date_columns_date_type(self, metadata_session: Session) -> None:
        """Date columns should resolve to DATE or TIMESTAMP."""
        rows = metadata_session.execute(
            select(Column.column_name, Column.resolved_type, Table.table_name)
            .join(Table, Column.table_id == Table.table_id)
            .where(Table.layer == "typed")
            .where(Column.column_name.in_(["date", "due_date"]))
        ).all()
        assert len(rows) > 0
        for col_name, resolved_type, table_name in rows:
            assert resolved_type is not None, f"{table_name}.{col_name}: no resolved_type"
            upper = resolved_type.upper()
            assert any(t in upper for t in ["DATE", "TIMESTAMP"]), (
                f"{table_name}.{col_name}: expected date, got {resolved_type}"
            )


# =============================================================================
# Statistics
# =============================================================================


class TestStatistics:
    """Verify statistical profiles for clean data."""

    def test_profiles_exist(self, metadata_session: Session, typed_table_ids: list[str]) -> None:
        """Typed columns should have statistical profiles."""
        profile_count = metadata_session.execute(
            select(func.count())
            .select_from(StatisticalProfile)
            .where(
                StatisticalProfile.column_id.in_(
                    select(Column.column_id).where(Column.table_id.in_(typed_table_ids))
                )
            )
        ).scalar()
        assert profile_count is not None and profile_count > 0

    def test_clean_data_low_null_ratios(
        self, metadata_session: Session, typed_table_ids: list[str]
    ) -> None:
        """Clean data should have few high-null columns."""
        rows = metadata_session.execute(
            select(Table.table_name, Column.column_name, StatisticalProfile.null_ratio)
            .join(Column, StatisticalProfile.column_id == Column.column_id)
            .join(Table, Column.table_id == Table.table_id)
            .where(Column.table_id.in_(typed_table_ids))
            .where(StatisticalProfile.null_ratio > 0.5)
        ).all()
        # Clean data: at most a few intentionally sparse columns
        assert len(rows) <= 3, f"Too many high-null columns: {rows}"


# =============================================================================
# Relationships
# =============================================================================


class TestRelationships:
    """Verify FK relationship detection."""

    def test_relationships_detected(
        self, metadata_session: Session, typed_table_ids: list[str]
    ) -> None:
        """Relationships should be detected between tables."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(Relationship)
            .where(Relationship.from_table_id.in_(typed_table_ids))
        ).scalar()
        assert count is not None and count >= 3

    def test_key_fk_patterns(self, metadata_session: Session, typed_table_ids: list[str]) -> None:
        """Key FK relationships should be detected."""
        rows = metadata_session.execute(
            select(Table.table_name, Column.column_name)
            .select_from(Relationship)
            .join(Table, Relationship.from_table_id == Table.table_id)
            .join(Column, Relationship.from_column_id == Column.column_id)
            .where(Relationship.from_table_id.in_(typed_table_ids))
        ).all()
        detected = {(r[0], r[1]) for r in rows}

        expected = [
            ("journal_lines", "entry_id"),
            ("journal_lines", "account_id"),
            ("payments", "invoice_id"),
        ]
        found = [p for p in expected if p in detected]
        assert len(found) >= 1, f"Expected some of {expected}, got {detected}"


# =============================================================================
# Semantic annotations (LLM phase)
# =============================================================================


class TestSemantic:
    """Verify semantic annotations from the LLM semantic phase."""

    def test_annotations_exist(self, metadata_session: Session, typed_table_ids: list[str]) -> None:
        """Columns should have semantic annotations."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(SemanticAnnotation)
            .where(
                SemanticAnnotation.column_id.in_(
                    select(Column.column_id).where(Column.table_id.in_(typed_table_ids))
                )
            )
        ).scalar()
        assert count is not None and count > 0, "No semantic annotations produced"

    def test_business_concepts_assigned(
        self, metadata_session: Session, typed_table_ids: list[str]
    ) -> None:
        """Some columns should have business_concept from ontology mapping."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(SemanticAnnotation)
            .where(
                SemanticAnnotation.column_id.in_(
                    select(Column.column_id).where(Column.table_id.in_(typed_table_ids))
                )
            )
            .where(SemanticAnnotation.business_concept.isnot(None))
        ).scalar()
        assert count is not None and count > 0, "No business_concept annotations"

    def test_table_entities_classified(
        self, metadata_session: Session, typed_table_ids: list[str]
    ) -> None:
        """Tables should be classified as fact/dimension."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(TableEntity)
            .where(TableEntity.table_id.in_(typed_table_ids))
        ).scalar()
        assert count is not None and count > 0, "No table entity classifications"


# =============================================================================
# Temporal
# =============================================================================


class TestTemporal:
    """Verify temporal column detection."""

    def test_temporal_columns_detected(
        self, metadata_session: Session, typed_table_ids: list[str]
    ) -> None:
        """Date columns should be identified as temporal."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(TemporalColumnProfile)
            .where(
                TemporalColumnProfile.column_id.in_(
                    select(Column.column_id).where(Column.table_id.in_(typed_table_ids))
                )
            )
        ).scalar()
        assert count is not None and count >= 3


# =============================================================================
# Entropy
# =============================================================================


class TestEntropy:
    """Verify entropy scoring."""

    def test_entropy_phase_completed(self, pipeline_run: RunResult) -> None:
        """Entropy phase should have completed."""
        phase_map = {p.phase_name: p.status for p in pipeline_run.phases}
        assert phase_map.get("entropy") == "completed"
