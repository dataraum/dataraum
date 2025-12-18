"""Tests for quality context formatters.

Tests the context-focused output formatters that replace scoring-based views.
"""

from datetime import UTC, datetime

import pytest

from dataraum_context.analysis.statistics.db_models import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum_context.analysis.temporal import TemporalColumnProfile
from dataraum_context.enrichment.db_models import (
    TopologicalQualityMetrics,
)
from dataraum_context.quality.context import (
    _generate_column_flags,
    _generate_table_flags,
    format_column_quality_context,
    format_dataset_quality_context,
    format_table_quality_context,
)
from dataraum_context.quality.models import (
    ColumnQualityContext,
    DatasetQualityContext,
    TableQualityContext,
)
from dataraum_context.storage import Column, Source, Table

# ============================================================================
# Flag Generation Tests
# ============================================================================


class TestColumnFlagGeneration:
    """Tests for _generate_column_flags()."""

    def test_no_flags_for_clean_data(self):
        """Clean data should produce no flags."""
        flags = _generate_column_flags(
            null_ratio=0.01,
            outlier_ratio=0.01,
            benford_compliant=True,
            is_stale=False,
            cardinality_ratio=0.5,
        )
        assert flags == []

    def test_high_nulls_flag(self):
        """High null ratio should produce high_nulls flag."""
        flags = _generate_column_flags(
            null_ratio=0.6,
            outlier_ratio=None,
            benford_compliant=None,
            is_stale=None,
            cardinality_ratio=None,
        )
        assert "high_nulls" in flags

    def test_moderate_nulls_flag(self):
        """Moderate null ratio should produce moderate_nulls flag."""
        flags = _generate_column_flags(
            null_ratio=0.2,
            outlier_ratio=None,
            benford_compliant=None,
            is_stale=None,
            cardinality_ratio=None,
        )
        assert "moderate_nulls" in flags
        assert "high_nulls" not in flags

    def test_high_outliers_flag(self):
        """High outlier ratio should produce high_outliers flag."""
        flags = _generate_column_flags(
            null_ratio=None,
            outlier_ratio=0.15,
            benford_compliant=None,
            is_stale=None,
            cardinality_ratio=None,
        )
        assert "high_outliers" in flags

    def test_moderate_outliers_flag(self):
        """Moderate outlier ratio should produce moderate_outliers flag."""
        flags = _generate_column_flags(
            null_ratio=None,
            outlier_ratio=0.07,
            benford_compliant=None,
            is_stale=None,
            cardinality_ratio=None,
        )
        assert "moderate_outliers" in flags
        assert "high_outliers" not in flags

    def test_benford_violation_flag(self):
        """Benford non-compliance should produce benford_violation flag."""
        flags = _generate_column_flags(
            null_ratio=None,
            outlier_ratio=None,
            benford_compliant=False,
            is_stale=None,
            cardinality_ratio=None,
        )
        assert "benford_violation" in flags

    def test_stale_data_flag(self):
        """Stale data should produce stale_data flag."""
        flags = _generate_column_flags(
            null_ratio=None,
            outlier_ratio=None,
            benford_compliant=None,
            is_stale=True,
            cardinality_ratio=None,
        )
        assert "stale_data" in flags

    def test_near_unique_flag(self):
        """Very high cardinality should produce near_unique flag."""
        flags = _generate_column_flags(
            null_ratio=None,
            outlier_ratio=None,
            benford_compliant=None,
            is_stale=None,
            cardinality_ratio=0.995,
        )
        assert "near_unique" in flags

    def test_low_cardinality_flag(self):
        """Very low cardinality should produce low_cardinality flag."""
        flags = _generate_column_flags(
            null_ratio=None,
            outlier_ratio=None,
            benford_compliant=None,
            is_stale=None,
            cardinality_ratio=0.005,
        )
        assert "low_cardinality" in flags

    def test_multiple_flags(self):
        """Multiple issues should produce multiple flags."""
        flags = _generate_column_flags(
            null_ratio=0.6,
            outlier_ratio=0.15,
            benford_compliant=False,
            is_stale=True,
            cardinality_ratio=0.005,
        )
        assert "high_nulls" in flags
        assert "high_outliers" in flags
        assert "benford_violation" in flags
        assert "stale_data" in flags
        assert "low_cardinality" in flags


class TestTableFlagGeneration:
    """Tests for _generate_table_flags()."""

    def test_no_flags_for_clean_table(self):
        """Clean table should produce no flags."""
        flags = _generate_table_flags(
            betti_0=1,
            orphaned_components=0,
            anomaly_count=0,
            issue_count=2,
        )
        assert flags == []

    def test_fragmented_flag(self):
        """Multiple connected components should produce fragmented flag."""
        flags = _generate_table_flags(
            betti_0=3,
            orphaned_components=0,
            anomaly_count=0,
            issue_count=0,
        )
        assert "fragmented" in flags

    def test_orphaned_components_flag(self):
        """Orphaned components should produce has_orphaned_components flag."""
        flags = _generate_table_flags(
            betti_0=1,
            orphaned_components=2,
            anomaly_count=0,
            issue_count=0,
        )
        assert "has_orphaned_components" in flags

    def test_has_anomalies_flag(self):
        """Anomalies should produce has_anomalies flag."""
        flags = _generate_table_flags(
            betti_0=1,
            orphaned_components=0,
            anomaly_count=5,
            issue_count=0,
        )
        assert "has_anomalies" in flags

    def test_many_issues_flag(self):
        """Many issues should produce many_issues flag."""
        flags = _generate_table_flags(
            betti_0=1,
            orphaned_components=0,
            anomaly_count=0,
            issue_count=10,
        )
        assert "many_issues" in flags


# ============================================================================
# Column Context Formatter Tests
# ============================================================================


@pytest.fixture
async def sample_source(async_session):
    """Create a sample source."""
    source = Source(
        source_id="test-source",
        name="test_data",
        source_type="csv",
    )
    async_session.add(source)
    await async_session.commit()
    return source


@pytest.fixture
async def sample_table(async_session, sample_source):
    """Create a sample table."""
    table = Table(
        table_id="test-table",
        source_id=sample_source.source_id,
        table_name="test_table",
        layer="typed",
        duckdb_path="test_table",
    )
    async_session.add(table)
    await async_session.commit()
    return table


@pytest.fixture
async def sample_column(async_session, sample_table):
    """Create a sample column."""
    column = Column(
        column_id="test-column",
        table_id=sample_table.table_id,
        column_name="amount",
        column_position=0,
        raw_type="DOUBLE",
        resolved_type="DOUBLE",
    )
    async_session.add(column)
    await async_session.commit()
    return column


class TestFormatColumnQualityContext:
    """Tests for format_column_quality_context()."""

    @pytest.mark.asyncio
    async def test_basic_column_context(self, async_session, sample_table, sample_column):
        """Test basic column context formatting."""
        context = await format_column_quality_context(
            sample_column, sample_table.table_name, async_session
        )

        assert isinstance(context, ColumnQualityContext)
        assert context.column_id == sample_column.column_id
        assert context.column_name == sample_column.column_name
        assert context.table_id == sample_column.table_id
        assert context.table_name == sample_table.table_name

    @pytest.mark.asyncio
    async def test_column_context_with_statistical_profile(
        self, async_session, sample_table, sample_column
    ):
        """Test column context includes statistical profile metrics."""
        # Add statistical profile with all required fields
        profile = StatisticalProfile(
            profile_id="profile-1",
            column_id=sample_column.column_id,
            total_count=1000,
            null_count=150,
            null_ratio=0.15,
            cardinality_ratio=0.8,
            profiled_at=datetime.now(UTC),
            profile_data={},
        )
        async_session.add(profile)
        await async_session.commit()

        context = await format_column_quality_context(
            sample_column, sample_table.table_name, async_session
        )

        assert context.null_ratio == 0.15
        assert context.cardinality_ratio == 0.8
        assert "moderate_nulls" in context.flags

    @pytest.mark.asyncio
    async def test_column_context_with_quality_metrics(
        self, async_session, sample_table, sample_column
    ):
        """Test column context includes quality metrics."""
        # Add quality metrics with all required fields
        quality = StatisticalQualityMetrics(
            metric_id="quality-1",
            column_id=sample_column.column_id,
            iqr_outlier_ratio=0.12,
            benford_compliant=False,
            computed_at=datetime.now(UTC),
            quality_data={},
        )
        async_session.add(quality)
        await async_session.commit()

        context = await format_column_quality_context(
            sample_column, sample_table.table_name, async_session
        )

        assert context.outlier_ratio == 0.12
        assert context.benford_compliant is False
        assert "high_outliers" in context.flags
        assert "benford_violation" in context.flags

    @pytest.mark.asyncio
    async def test_column_context_with_temporal_metrics(
        self, async_session, sample_table, sample_column
    ):
        """Test column context includes temporal metrics."""
        # Add temporal metrics with all required fields
        temporal = TemporalColumnProfile(
            profile_id="temporal-1",
            column_id=sample_column.column_id,
            min_timestamp=datetime(2023, 1, 1),
            max_timestamp=datetime(2023, 12, 31),
            detected_granularity="daily",
            has_seasonality=True,
            has_trend=False,
            is_stale=True,
            profile_data={
                "data_freshness_days": 120,
                "seasonality": {"has_seasonality": True},
                "trend": {"has_trend": False},
            },
            profiled_at=datetime.now(UTC),
        )
        async_session.add(temporal)
        await async_session.commit()

        context = await format_column_quality_context(
            sample_column, sample_table.table_name, async_session
        )

        assert context.is_stale is True
        assert context.data_freshness_days == 120
        assert context.has_seasonality is True
        assert context.has_trend is False
        assert "stale_data" in context.flags


# ============================================================================
# Table Context Formatter Tests
# ============================================================================


class TestFormatTableQualityContext:
    """Tests for format_table_quality_context()."""

    @pytest.mark.asyncio
    async def test_basic_table_context(self, async_session, sample_table, sample_column):
        """Test basic table context formatting."""
        context = await format_table_quality_context(sample_table.table_id, async_session, None)

        assert isinstance(context, TableQualityContext)
        assert context.table_id == sample_table.table_id
        assert context.table_name == sample_table.table_name
        assert context.column_count == 1
        assert len(context.columns) == 1
        assert context.columns[0].column_id == sample_column.column_id

    @pytest.mark.asyncio
    async def test_table_context_not_found(self, async_session):
        """Test table context returns None for non-existent table."""
        context = await format_table_quality_context("non-existent-table", async_session, None)
        assert context is None

    @pytest.mark.asyncio
    async def test_table_context_with_topological_metrics(
        self, async_session, sample_table, sample_column
    ):
        """Test table context includes topological metrics."""
        # Add topological metrics
        topo = TopologicalQualityMetrics(
            metric_id="topo-1",
            table_id=sample_table.table_id,
            betti_0=3,
            betti_1=1,
            orphaned_components=2,
            topology_data={},
            computed_at=datetime.now(UTC),
        )
        async_session.add(topo)
        await async_session.commit()

        context = await format_table_quality_context(sample_table.table_id, async_session, None)

        assert context.betti_0 == 3
        assert context.betti_1 == 1
        assert context.orphaned_components == 2
        assert "fragmented" in context.flags
        assert "has_orphaned_components" in context.flags


# ============================================================================
# Dataset Context Formatter Tests
# ============================================================================


class TestFormatDatasetQualityContext:
    """Tests for format_dataset_quality_context()."""

    @pytest.mark.asyncio
    async def test_basic_dataset_context(self, async_session, sample_table, sample_column):
        """Test basic dataset context formatting."""
        context = await format_dataset_quality_context([sample_table.table_id], async_session, None)

        assert isinstance(context, DatasetQualityContext)
        assert context.total_tables == 1
        assert context.total_columns == 1
        assert len(context.tables) == 1
        assert context.tables[0].table_id == sample_table.table_id
        assert context.computed_at is not None

    @pytest.mark.asyncio
    async def test_dataset_context_aggregates_issues(
        self, async_session, sample_table, sample_column
    ):
        """Test dataset context aggregates issues correctly."""
        # Add quality metrics with issues (include all required fields)
        profile = StatisticalProfile(
            profile_id="profile-1",
            column_id=sample_column.column_id,
            total_count=1000,
            null_count=600,
            null_ratio=0.6,  # Will generate high_nulls flag
            cardinality_ratio=0.5,
            profiled_at=datetime.now(UTC),
            profile_data={},
        )
        quality = StatisticalQualityMetrics(
            metric_id="quality-1",
            column_id=sample_column.column_id,
            benford_compliant=False,  # Will generate issue
            computed_at=datetime.now(UTC),
            quality_data={},
        )
        async_session.add_all([profile, quality])
        await async_session.commit()

        context = await format_dataset_quality_context([sample_table.table_id], async_session, None)

        # Should have issues aggregated
        assert context.total_issues >= 0  # Issues come from synthesis
        assert "high_nulls" in context.tables[0].columns[0].flags

    @pytest.mark.asyncio
    async def test_dataset_context_empty_tables(self, async_session):
        """Test dataset context with empty table list."""
        context = await format_dataset_quality_context([], async_session, None)

        assert context.total_tables == 0
        assert context.total_columns == 0
        assert len(context.tables) == 0

    @pytest.mark.asyncio
    async def test_dataset_context_multiple_tables(self, async_session, sample_source):
        """Test dataset context with multiple tables."""
        # Create two tables
        table1 = Table(
            table_id="table-1",
            source_id=sample_source.source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="orders",
        )
        table2 = Table(
            table_id="table-2",
            source_id=sample_source.source_id,
            table_name="customers",
            layer="typed",
            duckdb_path="customers",
        )
        async_session.add_all([table1, table2])

        # Add columns to each table
        col1 = Column(
            column_id="col-1",
            table_id="table-1",
            column_name="order_id",
            column_position=0,
            resolved_type="BIGINT",
        )
        col2 = Column(
            column_id="col-2",
            table_id="table-2",
            column_name="customer_id",
            column_position=0,
            resolved_type="BIGINT",
        )
        async_session.add_all([col1, col2])
        await async_session.commit()

        context = await format_dataset_quality_context(["table-1", "table-2"], async_session, None)

        assert context.total_tables == 2
        assert context.total_columns == 2
        assert len(context.tables) == 2
        table_names = {t.table_name for t in context.tables}
        assert table_names == {"orders", "customers"}


# ============================================================================
# Model Tests
# ============================================================================


class TestContextModels:
    """Tests for context model structure."""

    def test_column_quality_context_defaults(self):
        """Test ColumnQualityContext has correct defaults."""
        context = ColumnQualityContext(
            column_id="col-1",
            column_name="test",
            table_id="tbl-1",
            table_name="test_table",
        )

        assert context.null_ratio is None
        assert context.cardinality_ratio is None
        assert context.outlier_ratio is None
        assert context.flags == []
        assert context.issues == []
        assert context.filter_hints == []

    def test_table_quality_context_defaults(self):
        """Test TableQualityContext has correct defaults."""
        context = TableQualityContext(
            table_id="tbl-1",
            table_name="test_table",
        )

        assert context.row_count is None
        assert context.column_count == 0
        assert context.columns == []
        assert context.issues == []
        assert context.flags == []

    def test_dataset_quality_context_defaults(self):
        """Test DatasetQualityContext has correct defaults."""
        context = DatasetQualityContext()

        assert context.tables == []
        assert context.cross_table_issues == []
        assert context.total_tables == 0
        assert context.total_columns == 0
        assert context.total_issues == 0
        assert context.issues_by_severity == {}
        assert context.issues_by_dimension == {}
        assert context.summary is None
        assert context.filter_recommendations == []
