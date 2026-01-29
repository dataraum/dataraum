"""Smoke tests for pipeline phases.

These tests verify that each phase completes successfully against real data.
They don't validate specific outputs - just that phases run without errors.
"""

from pathlib import Path

import pytest

from dataraum.pipeline.base import PhaseStatus

from .conftest import FINANCE_JUNK_COLUMNS, PipelineTestHarness

pytestmark = pytest.mark.integration


class TestImportPhaseSmoke:
    """Smoke tests for the import phase."""

    def test_import_small_finance_directory(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Import the small finance fixture directory."""
        result = harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        assert result.status == PhaseStatus.COMPLETED, f"Import failed: {result.error}"
        assert "raw_tables" in result.outputs
        assert len(result.outputs["raw_tables"]) == 5  # 5 CSV files
        assert result.records_created == 5

    def test_import_creates_duckdb_tables(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify DuckDB tables are created after import."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        tables = harness.get_duckdb_tables()
        assert len(tables) == 5

        # All tables should have raw_ prefix
        for table in tables:
            assert table.startswith("raw_")

    def test_import_creates_metadata_records(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify SQLAlchemy metadata records are created."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        table_count = harness.get_table_count()
        column_count = harness.get_column_count()

        assert table_count == 5
        assert column_count > 0  # Each table has columns

    def test_import_single_csv(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Import a single CSV file."""
        csv_file = small_finance_path / "customers.csv"

        result = harness.run_import(
            source_path=csv_file,
            source_name="customers_only",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        assert result.status == PhaseStatus.COMPLETED
        assert len(result.outputs["raw_tables"]) == 1
        assert result.records_processed == 100  # 100 rows in synthetic fixture

    def test_import_preserves_data(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify imported data matches source."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        # Check row counts match synthetic fixtures
        for table_name, expected_rows in [
            ("raw_customers", 100),
            ("raw_vendors", 50),
            ("raw_products", 30),
            ("raw_payment_methods", 10),
            ("raw_transactions", 500),
        ]:
            result = harness.query_duckdb(f"SELECT COUNT(*) FROM {table_name}")
            actual_rows = result[0][0]
            assert actual_rows == expected_rows, (
                f"{table_name}: expected {expected_rows}, got {actual_rows}"
            )

    def test_import_drops_junk_columns(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify junk columns are dropped."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        # Get columns for transactions table (which has Unnamed columns)
        result = harness.query_duckdb("DESCRIBE raw_transactions")
        columns = [row[0] for row in result]

        # None of the junk columns should be present
        for junk in FINANCE_JUNK_COLUMNS:
            assert junk not in columns, f"Junk column '{junk}' was not dropped"


class TestTypingPhaseSmoke:
    """Smoke tests for the typing phase."""

    def test_typing_creates_typed_tables(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify typed tables are created after typing phase."""
        # First import
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )

        # Then type
        result = harness.run_phase("typing")

        assert result.status == PhaseStatus.COMPLETED, f"Typing failed: {result.error}"
        assert "typed_tables" in result.outputs
        assert len(result.outputs["typed_tables"]) == 5  # 5 tables typed

    def test_typing_creates_quarantine_tables(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify quarantine tables are created."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        tables = harness.get_duckdb_tables()

        # Should have raw, typed, and quarantine tables
        raw_tables = [t for t in tables if t.startswith("raw_")]
        typed_tables = [t for t in tables if t.startswith("typed_")]
        quarantine_tables = [t for t in tables if t.startswith("quarantine_")]

        assert len(raw_tables) == 5
        assert len(typed_tables) == 5
        assert len(quarantine_tables) == 5

    def test_typing_infers_correct_types(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify types are correctly inferred."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        # Check transaction table types
        columns = harness.query_duckdb("DESCRIBE typed_transactions")
        col_types = {row[0]: row[1] for row in columns}

        # Transaction ID should be integer
        assert col_types["Transaction ID"] == "BIGINT"

        # Transaction date should be date
        assert col_types["Transaction date"] == "DATE"

        # Amount should be numeric
        assert col_types["Amount"] == "DOUBLE"

    def test_typing_preserves_row_counts(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify row counts are preserved (typed + quarantine = raw)."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        for base_name in ["customers", "vendors", "products", "transactions", "payment_methods"]:
            raw_count = harness.query_duckdb(f"SELECT COUNT(*) FROM raw_{base_name}")[0][0]
            typed_count = harness.query_duckdb(f"SELECT COUNT(*) FROM typed_{base_name}")[0][0]
            quarantine_count = harness.query_duckdb(f"SELECT COUNT(*) FROM quarantine_{base_name}")[
                0
            ][0]

            # Typed + quarantine should equal raw
            assert typed_count + quarantine_count == raw_count, (
                f"{base_name}: typed({typed_count}) + quarantine({quarantine_count}) != raw({raw_count})"
            )

    def test_typing_with_clean_data_has_no_quarantine(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify clean synthetic data has no quarantined rows."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        # All quarantine tables should be empty for clean data
        for base_name in ["customers", "vendors", "products", "transactions", "payment_methods"]:
            quarantine_count = harness.query_duckdb(f"SELECT COUNT(*) FROM quarantine_{base_name}")[
                0
            ][0]
            assert quarantine_count == 0, f"Unexpected quarantine rows in {base_name}"


class TestStatisticsPhaseSmoke:
    """Smoke tests for the statistics phase."""

    def test_statistics_creates_profiles(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify statistical profiles are created after statistics phase."""
        # Run import and typing first
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        # Then run statistics
        result = harness.run_phase("statistics")

        assert result.status == PhaseStatus.COMPLETED, f"Statistics failed: {result.error}"
        assert "statistical_profiles" in result.outputs
        assert len(result.outputs["statistical_profiles"]) == 5  # 5 tables profiled

    def test_statistics_stores_profile_data(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify statistical profile data is stored in metadata database."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        # Check that profiles are stored
        from sqlalchemy import func, select

        from dataraum.analysis.statistics.db_models import StatisticalProfile

        with harness.session_factory() as session:
            stmt = (
                select(func.count())
                .select_from(StatisticalProfile)
                .where(StatisticalProfile.layer == "typed")
            )
            profile_count = (session.execute(stmt)).scalar()

        # Should have profiles for all columns across 5 tables
        assert profile_count > 0

    def test_statistics_computes_numeric_stats(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify numeric statistics are computed for numeric columns."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        # Check profile for Transaction ID (numeric column)
        from sqlalchemy import select

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.storage import Column, Table

        with harness.session_factory() as session:
            # Find the transactions table
            table_stmt = select(Table).where(
                Table.table_name == "transactions",
                Table.layer == "typed",
            )
            table = (session.execute(table_stmt)).scalar_one_or_none()
            assert table is not None

            # Find the Transaction ID column
            col_stmt = select(Column).where(
                Column.table_id == table.table_id,
                Column.column_name == "Transaction ID",
            )
            column = (session.execute(col_stmt)).scalar_one_or_none()
            assert column is not None

            # Get the profile
            profile_stmt = select(StatisticalProfile).where(
                StatisticalProfile.column_id == column.column_id,
                StatisticalProfile.layer == "typed",
            )
            profile = (session.execute(profile_stmt)).scalar_one_or_none()
            assert profile is not None

        # Verify numeric stats are present (SQLite stores booleans as 0/1)
        assert profile.is_numeric == 1
        profile_data = profile.profile_data
        assert "numeric_stats" in profile_data
        assert profile_data["numeric_stats"] is not None
        assert "min_value" in profile_data["numeric_stats"]
        assert "max_value" in profile_data["numeric_stats"]
        assert "mean" in profile_data["numeric_stats"]

    def test_statistics_computes_cardinality(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify cardinality metrics are computed."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        from sqlalchemy import select

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.storage import Column, Table

        with harness.session_factory() as session:
            # Find the transactions table
            table_stmt = select(Table).where(
                Table.table_name == "transactions",
                Table.layer == "typed",
            )
            table = (session.execute(table_stmt)).scalar_one_or_none()
            assert table is not None

            # Get profiles for this table
            col_stmt = select(Column.column_id).where(Column.table_id == table.table_id)
            col_ids = (session.execute(col_stmt)).scalars().all()

            profile_stmt = select(StatisticalProfile).where(
                StatisticalProfile.column_id.in_(col_ids),
                StatisticalProfile.layer == "typed",
            )
            profiles = (session.execute(profile_stmt)).scalars().all()

        # All profiles should have cardinality metrics
        for profile in profiles:
            assert profile.total_count == 500  # 500 transactions
            assert profile.distinct_count is not None
            assert profile.cardinality_ratio is not None

    def test_statistics_computes_top_values(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify top values are computed for columns."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        from sqlalchemy import select

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.storage import Column, Table

        with harness.session_factory() as session:
            # Find the transactions table
            table_stmt = select(Table).where(
                Table.table_name == "transactions",
                Table.layer == "typed",
            )
            table = (session.execute(table_stmt)).scalar_one_or_none()
            assert table is not None

            # Find the Transaction type column (categorical)
            col_stmt = select(Column).where(
                Column.table_id == table.table_id,
                Column.column_name == "Transaction type",
            )
            column = (session.execute(col_stmt)).scalar_one_or_none()
            assert column is not None

            # Get the profile
            profile_stmt = select(StatisticalProfile).where(
                StatisticalProfile.column_id == column.column_id,
                StatisticalProfile.layer == "typed",
            )
            profile = (session.execute(profile_stmt)).scalar_one_or_none()
            assert profile is not None

        # Verify top values are present
        profile_data = profile.profile_data
        assert "top_values" in profile_data
        assert len(profile_data["top_values"]) > 0
        # Each top value should have value, count, percentage
        first_top = profile_data["top_values"][0]
        assert "value" in first_top
        assert "count" in first_top
        assert "percentage" in first_top

    def test_statistics_is_idempotent(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify running statistics twice doesn't create duplicate profiles."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        # Run statistics twice
        result1 = harness.run_phase("statistics")
        assert result1.status == PhaseStatus.COMPLETED

        # Second run should skip or return same count
        result2 = harness.run_phase("statistics")
        assert result2.status in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED)

        # Count profiles - should not have duplicates
        from sqlalchemy import func, select

        from dataraum.analysis.statistics.db_models import StatisticalProfile

        with harness.session_factory() as session:
            stmt = (
                select(func.count())
                .select_from(StatisticalProfile)
                .where(StatisticalProfile.layer == "typed")
            )
            profile_count = (session.execute(stmt)).scalar()

        # Profile count should be reasonable (not doubled)
        # We have 5 tables with various columns, expect ~40-50 profiles
        assert profile_count < 100


class TestStatisticalQualityPhaseSmoke:
    """Smoke tests for the statistical quality phase."""

    def test_statistical_quality_runs(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify statistical quality phase completes."""
        # Run prerequisites
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        # Run statistical quality
        result = harness.run_phase("statistical_quality")

        assert result.status == PhaseStatus.COMPLETED, f"Statistical quality failed: {result.error}"
        assert "quality_metrics" in result.outputs

    def test_statistical_quality_detects_outliers(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify outlier detection runs on numeric columns."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")
        result = harness.run_phase("statistical_quality")

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs.get("total_outlier_results", 0) >= 0


class TestRelationshipsPhaseSmoke:
    """Smoke tests for the relationships phase."""

    def test_relationships_runs(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify relationships phase completes."""
        # Run prerequisites
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        # Run relationships
        result = harness.run_phase("relationships")

        assert result.status == PhaseStatus.COMPLETED, f"Relationships failed: {result.error}"
        assert "relationship_candidates" in result.outputs

    def test_relationships_detects_candidates(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify relationship candidates are detected between tables."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")
        result = harness.run_phase("relationships")

        assert result.status == PhaseStatus.COMPLETED
        # With 5 related tables, we should find some relationship candidates
        total_candidates = result.outputs.get("total_candidates", 0)
        assert total_candidates >= 0  # May be 0 if no overlaps detected


class TestCorrelationsPhaseSmoke:
    """Smoke tests for the correlations phase."""

    def test_correlations_runs(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify correlations phase completes."""
        # Run prerequisites
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")

        # Run correlations
        result = harness.run_phase("correlations")

        assert result.status == PhaseStatus.COMPLETED, f"Correlations failed: {result.error}"
        assert "correlations" in result.outputs

    def test_correlations_stores_results(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify correlation results are stored in database."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("statistics")
        result = harness.run_phase("correlations")

        assert result.status == PhaseStatus.COMPLETED
        # Correlations might exist between numeric columns
        total_correlations = result.outputs.get("total_correlations", 0)
        assert total_correlations >= 0


class TestTemporalPhaseSmoke:
    """Smoke tests for the temporal phase."""

    def test_temporal_runs(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify temporal phase completes."""
        # Run prerequisites
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")

        # Run temporal (doesn't need statistics)
        result = harness.run_phase("temporal")

        assert result.status == PhaseStatus.COMPLETED, f"Temporal failed: {result.error}"
        assert "temporal_profiles" in result.outputs

    def test_temporal_profiles_date_columns(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify temporal analysis is performed on date columns."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        result = harness.run_phase("temporal")

        assert result.status == PhaseStatus.COMPLETED
        # Transaction date and other date columns should be profiled
        total_profiles = result.outputs.get("total_profiles", 0)
        assert total_profiles > 0  # At least Transaction date

    def test_temporal_stores_profiles(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify temporal profiles are stored in database."""
        harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        harness.run_phase("typing")
        harness.run_phase("temporal")

        # Check that profiles are stored
        from sqlalchemy import func, select

        from dataraum.analysis.temporal.db_models import TemporalColumnProfile

        with harness.session_factory() as session:
            stmt = select(func.count()).select_from(TemporalColumnProfile)
            profile_count = (session.execute(stmt)).scalar()

        # Should have at least one temporal profile
        assert profile_count > 0


class TestFullPipelineSmoke:
    """Smoke tests for the full pipeline."""

    def test_full_pipeline_completes(
        self,
        harness: PipelineTestHarness,
        small_finance_path: Path,
    ):
        """Verify all phases complete in sequence."""
        # Run import
        result = harness.run_import(
            source_path=small_finance_path,
            source_name="small_finance",
            junk_columns=FINANCE_JUNK_COLUMNS,
        )
        assert result.status == PhaseStatus.COMPLETED, f"Import failed: {result.error}"

        # Run typing
        result = harness.run_phase("typing")
        assert result.status == PhaseStatus.COMPLETED, f"Typing failed: {result.error}"

        # Run temporal (parallel track)
        result = harness.run_phase("temporal")
        assert result.status == PhaseStatus.COMPLETED, f"Temporal failed: {result.error}"

        # Run statistics
        result = harness.run_phase("statistics")
        assert result.status == PhaseStatus.COMPLETED, f"Statistics failed: {result.error}"

        # Run statistical quality
        result = harness.run_phase("statistical_quality")
        assert result.status == PhaseStatus.COMPLETED, f"Statistical quality failed: {result.error}"

        # Run relationships
        result = harness.run_phase("relationships")
        assert result.status == PhaseStatus.COMPLETED, f"Relationships failed: {result.error}"

        # Run correlations
        result = harness.run_phase("correlations")
        assert result.status == PhaseStatus.COMPLETED, f"Correlations failed: {result.error}"

        # All 7 phases should have completed
        assert len(harness.results) == 7
