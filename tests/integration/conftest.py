"""Integration test fixtures.

Provides shared fixtures for running pipeline integration tests against
real or fixture data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dataraum_context.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum_context.pipeline.orchestrator import Pipeline, PipelineConfig
from dataraum_context.pipeline.phases import (
    CorrelationsPhase,
    ImportPhase,
    RelationshipsPhase,
    StatisticalQualityPhase,
    StatisticsPhase,
    TemporalPhase,
    TypingPhase,
)
from dataraum_context.storage import init_database

# Paths to test data
FIXTURES_DIR = Path(__file__).parent / "fixtures"
SMALL_FINANCE_DIR = FIXTURES_DIR / "small_finance"
REAL_FINANCE_DIR = Path(__file__).parent.parent.parent / "examples" / "finance_csv_example"

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

    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    duckdb_conn: duckdb.DuckDBPyConnection
    pipeline: Pipeline
    source_id: str = field(default_factory=lambda: str(uuid4()))

    # Track phase results
    results: dict[str, PhaseResult] = field(default_factory=dict)

    async def run_phase(
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

        async with self.session_factory() as session:
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
            skip_reason = await phase.should_skip(ctx)
            if skip_reason:
                result = PhaseResult.skipped(skip_reason)
            else:
                result = await phase.run(ctx)

            await session.commit()

        self.results[phase_name] = result
        return result

    async def run_import(
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

        return await self.run_phase("import", config=config)

    def get_duckdb_tables(self) -> list[str]:
        """Get list of tables in DuckDB."""
        result = self.duckdb_conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def query_duckdb(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute a SQL query against DuckDB."""
        return self.duckdb_conn.execute(sql).fetchall()

    async def get_table_count(self) -> int:
        """Get count of tables in metadata database."""
        from sqlalchemy import func, select

        from dataraum_context.storage import Table

        async with self.session_factory() as session:
            stmt = select(func.count()).select_from(Table)
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def get_column_count(self) -> int:
        """Get count of columns in metadata database."""
        from sqlalchemy import func, select

        from dataraum_context.storage import Column

        async with self.session_factory() as session:
            stmt = select(func.count()).select_from(Column)
            result = await session.execute(stmt)
            return result.scalar() or 0


@pytest.fixture
async def integration_engine() -> AsyncEngine:
    """Create an in-memory SQLite engine for integration tests."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        future=True,
    )

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await init_database(engine)
    yield engine
    await engine.dispose()


@pytest.fixture
def integration_duckdb() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection for integration tests."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def integration_pipeline() -> Pipeline:
    """Create a pipeline with registered phases for testing."""
    pipeline = Pipeline(config=PipelineConfig(skip_llm_phases=True))
    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())
    pipeline.register(StatisticsPhase())
    pipeline.register(StatisticalQualityPhase())
    pipeline.register(RelationshipsPhase())
    pipeline.register(CorrelationsPhase())
    pipeline.register(TemporalPhase())
    return pipeline


@pytest.fixture
async def harness(
    integration_engine: AsyncEngine,
    integration_duckdb: duckdb.DuckDBPyConnection,
    integration_pipeline: Pipeline,
) -> PipelineTestHarness:
    """Create a pipeline test harness.

    This is the main fixture for integration tests. It provides:
    - Isolated database connections
    - Pre-configured pipeline
    - Convenience methods for running phases
    """
    session_factory = async_sessionmaker(
        integration_engine,
        class_=AsyncSession,
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
def real_finance_path() -> Path:
    """Path to real finance example data."""
    return REAL_FINANCE_DIR


@pytest.fixture
def finance_junk_columns() -> list[str]:
    """Common junk columns in finance data."""
    return FINANCE_JUNK_COLUMNS
