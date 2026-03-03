"""Integration test fixtures.

Provides shared fixtures for running pipeline integration tests against
real or fixture data, including agent validation fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.orchestrator import Pipeline, PipelineConfig
from dataraum.pipeline.phases.correlations_phase import CorrelationsPhase
from dataraum.pipeline.phases.entropy_phase import EntropyPhase
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.pipeline.phases.relationships_phase import RelationshipsPhase
from dataraum.pipeline.phases.statistical_quality_phase import StatisticalQualityPhase
from dataraum.pipeline.phases.statistics_phase import StatisticsPhase
from dataraum.pipeline.phases.temporal_phase import TemporalPhase
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.storage import init_database

# Paths to test data
FIXTURES_DIR = Path(__file__).parent / "fixtures"
SMALL_FINANCE_DIR = FIXTURES_DIR / "small_finance"

# Common junk columns in the finance data
FINANCE_JUNK_COLUMNS = [
    "Unnamed: 0",
    "Unnamed: 0.1",
    "Unnamed: 0.2",
    "column0",
    "column00",
]


@dataclass
class PipelineTestHarness:
    """Test harness for running pipeline phases.

    Provides a convenient interface for integration tests to:
    - Run individual phases or the full pipeline
    - Access database sessions and DuckDB connections
    - Query results and verify outputs
    """

    engine: Engine
    session_factory: sessionmaker[Session]
    duckdb_conn: duckdb.DuckDBPyConnection
    pipeline: Pipeline
    source_id: str = field(default_factory=lambda: str(uuid4()))

    # Track phase results
    results: dict[str, PhaseResult] = field(default_factory=dict)

    def run_phase(
        self,
        phase_name: str,
        config: dict[str, Any] | None = None,
        table_ids: list[str] | None = None,
    ) -> PhaseResult:
        """Run a single phase.

        Args:
            phase_name: Name of the phase to run
            config: Configuration overrides
            table_ids: Optional list of table IDs to process

        Returns:
            PhaseResult from the phase execution
        """
        phase = self.pipeline.phases.get(phase_name)
        if not phase:
            raise ValueError(f"Phase '{phase_name}' not registered")

        with self.session_factory() as session:
            # Build previous outputs from stored results
            previous_outputs = {
                name: result.outputs
                for name, result in self.results.items()
                if result.status == PhaseStatus.COMPLETED
            }

            ctx = PhaseContext(
                session=session,
                duckdb_conn=self.duckdb_conn,
                source_id=self.source_id,
                table_ids=table_ids or [],
                previous_outputs=previous_outputs,
                config=config or {},
            )

            # Check skip condition
            skip_reason = phase.should_skip(ctx)
            if skip_reason:
                result = PhaseResult.skipped(skip_reason)
            else:
                result = phase.run(ctx)

            session.commit()

        self.results[phase_name] = result
        return result

    def run_import(
        self,
        source_path: str | Path,
        source_name: str | None = None,
        junk_columns: list[str] | None = None,
    ) -> PhaseResult:
        """Convenience method to run the import phase.

        Args:
            source_path: Path to CSV file or directory
            source_name: Optional name for the source
            junk_columns: Columns to drop after import

        Returns:
            PhaseResult from import phase
        """
        config = {
            "source_path": str(source_path),
            "junk_columns": junk_columns or [],
        }
        if source_name:
            config["source_name"] = source_name

        return self.run_phase("import", config=config)

    def get_duckdb_tables(self) -> list[str]:
        """Get list of tables in DuckDB."""
        result = self.duckdb_conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def query_duckdb(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute a SQL query against DuckDB."""
        return self.duckdb_conn.execute(sql).fetchall()

    def get_table_count(self) -> int:
        """Get count of tables in metadata database."""
        from sqlalchemy import func, select

        from dataraum.storage import Table

        with self.session_factory() as session:
            stmt = select(func.count()).select_from(Table)
            result = session.execute(stmt)
            return result.scalar() or 0

    def get_column_count(self) -> int:
        """Get count of columns in metadata database."""
        from sqlalchemy import func, select

        from dataraum.storage import Column

        with self.session_factory() as session:
            stmt = select(func.count()).select_from(Column)
            result = session.execute(stmt)
            return result.scalar() or 0


@pytest.fixture
def integration_engine() -> Engine:
    """Create an in-memory SQLite engine for integration tests."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    init_database(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def integration_duckdb() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection for integration tests."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def integration_pipeline() -> Pipeline:
    """Create a pipeline with registered phases for testing."""
    pipeline = Pipeline(config=PipelineConfig())
    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())
    pipeline.register(StatisticsPhase())
    pipeline.register(StatisticalQualityPhase())
    pipeline.register(RelationshipsPhase())
    pipeline.register(CorrelationsPhase())
    pipeline.register(TemporalPhase())
    return pipeline


@pytest.fixture
def harness(
    integration_engine: Engine,
    integration_duckdb: duckdb.DuckDBPyConnection,
    integration_pipeline: Pipeline,
) -> PipelineTestHarness:
    """Create a pipeline test harness.

    This is the main fixture for integration tests. It provides:
    - Isolated database connections
    - Pre-configured pipeline
    - Convenience methods for running phases
    """
    session_factory = sessionmaker(
        bind=integration_engine,
        expire_on_commit=False,
    )

    return PipelineTestHarness(
        engine=integration_engine,
        session_factory=session_factory,
        duckdb_conn=integration_duckdb,
        pipeline=integration_pipeline,
    )


@pytest.fixture
def small_finance_path() -> Path:
    """Path to small finance fixture data."""
    return SMALL_FINANCE_DIR


@pytest.fixture
def finance_junk_columns() -> list[str]:
    """Common junk columns in finance data."""
    return FINANCE_JUNK_COLUMNS


# =============================================================================
# Agent Validation Fixtures (Phase 0)
# =============================================================================


@pytest.fixture
def agent_pipeline() -> Pipeline:
    """Pipeline with phases needed for agent testing (through entropy)."""
    pipeline = Pipeline(config=PipelineConfig())
    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())
    pipeline.register(StatisticsPhase())
    pipeline.register(StatisticalQualityPhase())
    pipeline.register(RelationshipsPhase())
    pipeline.register(CorrelationsPhase())
    pipeline.register(TemporalPhase())
    pipeline.register(EntropyPhase())
    return pipeline


@pytest.fixture
def agent_harness(
    integration_engine: Engine,
    integration_duckdb: duckdb.DuckDBPyConnection,
    agent_pipeline: Pipeline,
) -> PipelineTestHarness:
    """Harness with entropy phase for agent validation tests."""
    session_factory = sessionmaker(
        bind=integration_engine,
        expire_on_commit=False,
    )

    return PipelineTestHarness(
        engine=integration_engine,
        session_factory=session_factory,
        duckdb_conn=integration_duckdb,
        pipeline=agent_pipeline,
    )


@pytest.fixture
def analyzed_small_finance(
    agent_harness: PipelineTestHarness,
    small_finance_path: Path,
) -> PipelineTestHarness:
    """Harness with small finance data fully analyzed through entropy.

    Runs: import -> typing -> statistics -> statistical_quality ->
          relationships -> correlations -> temporal -> entropy
    """
    result = agent_harness.run_import(
        source_path=small_finance_path,
        source_name="small_finance",
        junk_columns=FINANCE_JUNK_COLUMNS,
    )
    assert result.status == PhaseStatus.COMPLETED, f"Import failed: {result.error}"

    for phase_name in [
        "typing",
        "statistics",
        "statistical_quality",
        "relationships",
        "correlations",
        "temporal",
        "entropy",
    ]:
        result = agent_harness.run_phase(phase_name)
        assert result.status == PhaseStatus.COMPLETED, f"{phase_name} failed: {result.error}"

    return agent_harness


@pytest.fixture
def analyzed_session(analyzed_small_finance: PipelineTestHarness) -> Session:
    """A fresh session from the analyzed harness."""
    with analyzed_small_finance.session_factory() as session:
        yield session


@pytest.fixture
def analyzed_table_ids(analyzed_small_finance: PipelineTestHarness) -> list[str]:
    """Table IDs for typed tables in the analyzed dataset."""
    from sqlalchemy import select

    from dataraum.storage import Table

    with analyzed_small_finance.session_factory() as session:
        stmt = select(Table.table_id).where(Table.layer == "typed")
        return list(session.execute(stmt).scalars().all())


@pytest.fixture
def mock_llm_config() -> MagicMock:
    """Mock LLM configuration for agent tests."""
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    config.limits.cache_ttl_seconds = 3600
    config.limits.max_input_tokens_per_request = 8000
    return config


@pytest.fixture
def mock_llm_provider() -> MagicMock:
    """Mock LLM provider that doesn't call any real API."""
    provider = MagicMock()
    provider.get_model_for_tier.return_value = "test-model"
    return provider


@pytest.fixture
def mock_prompt_renderer() -> MagicMock:
    """Mock prompt renderer."""
    renderer = MagicMock()
    renderer.render_split.return_value = ("System prompt", "User prompt", 0.0)
    return renderer
