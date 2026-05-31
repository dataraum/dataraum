"""Integration test fixtures.

Provides shared fixtures for running pipeline integration tests against
real or fixture data, including agent validation fixtures.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum.entropy.engine import run_detector_post_step
from dataraum.pipeline.base import Phase, PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.phases.correlations_phase import CorrelationsPhase
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.pipeline.phases.relationships_phase import RelationshipsPhase
from dataraum.pipeline.phases.statistical_quality_phase import StatisticalQualityPhase
from dataraum.pipeline.phases.statistics_phase import StatisticsPhase
from dataraum.pipeline.phases.temporal_phase import TemporalPhase
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.storage import init_database
from tests.conftest import baseline_session_id

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
    phases: dict[str, Phase]
    source_id: str = field(default_factory=lambda: str(uuid4()))

    # Track phase results
    results: dict[str, PhaseResult] = field(default_factory=dict)

    # YAML declarations cache (loaded once, shared across run_phase calls)
    _declarations: dict[str, Any] | None = field(default=None, repr=False)

    def _get_detector_ids(self, phase_name: str) -> list[str]:
        """Get detector IDs declared for a phase in pipeline.yaml."""
        if self._declarations is None:
            self._declarations = load_phase_declarations()
        decl = self._declarations.get(phase_name)
        return decl.detectors if decl else []

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
        phase = self.phases.get(phase_name)
        if not phase:
            raise ValueError(f"Phase '{phase_name}' not registered")

        with self.session_factory() as session:
            ctx = PhaseContext(
                session=session,
                duckdb_conn=self.duckdb_conn,
                source_id=self.source_id,
                table_ids=table_ids or [],
                config=config or {},
                session_id=baseline_session_id(),
            )

            # Check skip condition
            skip_reason = phase.should_skip(ctx)
            if skip_reason:
                result = PhaseResult.skipped(skip_reason)
            else:
                result = phase.run(ctx)

            session.commit()

        # Run post-step detectors declared in pipeline.yaml
        if result.status == PhaseStatus.COMPLETED:
            detector_ids = self._get_detector_ids(phase_name)
            if detector_ids:
                with self.session_factory() as detector_session:
                    for detector_id in detector_ids:
                        run_detector_post_step(
                            detector_session,
                            self.source_id,
                            detector_id,
                            self.duckdb_conn,
                            session_id=baseline_session_id(),
                        )
                    detector_session.commit()

        self.results[phase_name] = result
        return result

    # Data-file extensions the harness enumerates out of a directory fixture,
    # mirroring the loader-dispatch suffixes (CSV/TSV, Parquet, JSON).
    _FIXTURE_DATA_SUFFIXES = (".csv", ".tsv", ".parquet", ".pq", ".json", ".jsonl")

    def run_import(
        self,
        source_path: str | Path,
        source_name: str | None = None,
        junk_columns: list[str] | None = None,
    ) -> PhaseResult:
        """Convenience method to run the import phase against a local fixture path.

        Pre-seeds a Source row (mimicking what ``begin_session`` /
        ``setup_pipeline`` write), then drives the import loader on a local
        readable file — or, for a directory fixture, on each contained data
        file in turn.

        Per DAT-389 the production import ingress (``ImportPhase._run``) gates
        each source URI through ``validate_source_uri`` — only
        ``s3://<lake-bucket>/<key>`` reaches a loader. That gate is covered by
        the unit tests. These integration tests exercise the *real* DuckDB read +
        downstream phases against local fixture files (there is no object store
        in the test process), so the harness invokes the post-validation loader
        entry (``_load_file_source``) directly rather than the ``s3://`` gate.

        For a directory fixture (e.g. ``small_finance/`` — 5 CSVs with
        *different* schemas) the harness stands in for the cockpit ``select``
        enumeration (DAT-378): it enumerates the contained data files (sorted for
        determinism) and hands the whole list to ``_load_file_source`` under one
        ``source_id`` / ``source_name``. The per-URI loop loads each in turn and
        aggregates the per-file ``raw_tables`` into one COMPLETED ``PhaseResult``
        — the same multi-URI shape production now runs (DAT-378). The loader
        names each raw table ``<source_name>__<file_stem>``, so the loop
        reproduces the exact multi-table dataset (``small_finance__customers``,
        ``small_finance__payment_methods``, …). If any per-file load fails, the
        aggregate fails with that error.

        Args:
            source_path: Path to a CSV / Parquet / JSON fixture file, or a
                directory containing such files.
            source_name: Optional name for the source (derived from path stem if omitted).
            junk_columns: Columns to drop after import.

        Returns:
            PhaseResult from the import loader.
        """
        import re

        from dataraum.storage import Source

        path = Path(source_path)
        raw_name = source_name or path.stem.lower()
        clean_name = re.sub(r"[^a-z0-9_]", "_", raw_name).strip("_") or "source"

        # Resolve the file(s) to load. A directory fixture is enumerated into its
        # contained data files (sorted), each loaded as its own table — the
        # harness stand-in for the DAT-378 enumeration. A single file is loaded
        # as-is.
        if path.is_dir():
            file_paths = sorted(
                p
                for p in path.iterdir()
                if p.is_file() and p.suffix.lower() in self._FIXTURE_DATA_SUFFIXES
            )
            if not file_paths:
                raise ValueError(f"No loadable data files found in directory {path}")
        else:
            file_paths = [path]

        # Infer the Source row's ``source_type`` from the first file's extension
        # (CSV-default for unknowns); it is informational here — the loader
        # dispatches on each file's own URI suffix.
        suffix = file_paths[0].suffix.lower()
        if suffix in {".parquet", ".pq"}:
            source_type = "parquet"
        elif suffix in {".json", ".jsonl"}:
            source_type = "json"
        elif suffix in {".yaml", ".yml"}:
            source_type = "db_recipe"
        else:
            source_type = "csv"

        import_phase = self.phases["import"]
        assert isinstance(import_phase, ImportPhase)

        file_uris = [str(p) for p in file_paths]

        with self.session_factory() as session:
            # Seed the Source row idempotently — the test harness shares one
            # source_id across phases, so repeat run_import calls must not collide.
            source = session.get(Source, self.source_id)
            if source is None:
                source = Source(
                    source_id=self.source_id,
                    name=clean_name,
                    source_type=source_type,
                    connection_config={"file_uris": file_uris},
                    status="configured",
                )
                session.add(source)
                session.commit()
                source = session.get(Source, self.source_id)
            assert source is not None

            ctx = PhaseContext(
                session=session,
                duckdb_conn=self.duckdb_conn,
                source_id=self.source_id,
                config={"junk_columns": junk_columns or []},
                session_id=baseline_session_id(),
            )

            # The per-URI loop is the production multi-file path (DAT-378): one
            # call loads every URI in turn, one raw table per object. A per-file
            # failure fails the aggregate with that file's error (no swallow).
            result = import_phase._load_file_source(ctx, source, clean_name, file_uris)
            session.commit()

        self.results["import"] = result
        return result

    def get_duckdb_tables(self, layer: str | None = None) -> list[str]:
        """Get list of tables across workspace layer schemas.

        Post-DAT-341 tables live in ``lake.raw`` / ``lake.typed`` /
        ``lake.quarantine`` rather than the connection's USE'd schema.
        ``SHOW TABLES`` only sees the current schema, so we query
        ``duckdb_tables()`` directly.

        Args:
            layer: If provided, restrict to a single layer schema (e.g.
                ``"raw"``). Otherwise return tables across all layer schemas.
        """
        from dataraum.server.storage import LAKE_CATALOG_ALIAS, LAKE_LAYER_SCHEMAS

        if layer is not None:
            schemas = [layer]
        else:
            schemas = list(LAKE_LAYER_SCHEMAS)
        placeholders = ",".join(repr(s) for s in schemas)
        result = self.duckdb_conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
            f"AND schema_name IN ({placeholders})"
        ).fetchall()
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


def _build_phase_dict(*phase_instances: Phase) -> dict[str, Phase]:
    """Build a name -> Phase dict from phase instances."""
    return {p.name: p for p in phase_instances}


@pytest.fixture
def integration_engine(pg_url_clean: str) -> Engine:
    """Create a Postgres engine on the session-scoped testcontainer.

    Integration tests target the real Postgres dialect post-DAT-321 so that
    SQLite-permissive quirks (case insensitivity, JSON-vs-JSONB, looser
    transaction semantics) can't mask real bugs. Per-test isolation comes
    from ``pg_url_clean`` (TRUNCATE CASCADE over every Base table).

    Seeds a baseline ``Source`` + ``InvestigationSession`` so the global
    ``before_flush`` autofill hook in ``tests/conftest.py`` has a valid
    FK target for any per-session row a test constructs without explicit
    ``session_id=``. Production code always sets it explicitly.
    """
    from datetime import UTC, datetime

    from sqlalchemy import event, text

    from dataraum.investigation.db_models import InvestigationSession
    from dataraum.server.workspace import schema_name_for
    from dataraum.storage import Source

    engine = create_engine(pg_url_clean, echo=False, future=True)

    # Mirror ConnectionManager._init_sqlalchemy schema-per-workspace
    # bootstrap (post-DAT-339 Commit B). This fixture creates the engine
    # directly rather than going through ConnectionManager, so we have to
    # repeat the listener-then-create-schema dance here. The conftest
    # module-level os.environ["DATARAUM_WORKSPACE_ID"]="test" guarantees
    # the workspace_id is stable across the pytest invocation.
    schema_name = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_conn, _conn_record):  # noqa: ANN001
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute(f'SET search_path TO "{schema_name}", public')
        finally:
            cursor.close()

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))

    init_database(engine)

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as sess:
        sess.add(
            Source(
                source_id="00000000-0000-0000-0000-000000000002",
                name="test_baseline",
                source_type="csv",
            )
        )
        sess.flush()
        sess.add(
            InvestigationSession(
                session_id=baseline_session_id(),
                source_id="00000000-0000-0000-0000-000000000002",
                intent="integration baseline",
                status="active",
                started_at=datetime.now(UTC),
            )
        )
        sess.commit()

    yield engine
    engine.dispose()


@pytest.fixture
def integration_duckdb(lake_anchor, lake_clean):
    """Open a DuckLake-anchored DuckDB connection scoped to ``lake.typed``.

    Post-DAT-341 the loaders write to ``lake.raw.<source>__<table>`` via FQN,
    so a plain ``:memory:`` DuckDB no longer suffices — the ``lake`` catalog
    must be ATTACHed. The session-scoped ``lake_anchor`` does the bootstrap
    once; ``lake_clean`` drops per-test residue from the three layer schemas.

    Mirrors :class:`ConnectionManager._init_duckdb`: USE ``lake.typed`` on
    the raw connection AND wrap with ``_LakeScopedConnection`` so derived
    cursors (which DuckDB does NOT auto-inherit USE on, per the API
    documented in core/connections.py) re-apply the USE statement. Without
    the wrapper, analysis modules that open cursors find their unqualified
    SELECTs resolving against ``memory.main`` instead of the typed schema.
    """
    from dataraum.core.connections import _LakeScopedConnection
    from dataraum.server.storage import LAKE_CATALOG_ALIAS, connect_session

    qualified = f"{LAKE_CATALOG_ALIAS}.typed"
    raw_conn = connect_session()
    raw_conn.execute(f"USE {qualified}")
    wrapped = _LakeScopedConnection(raw_conn, qualified)
    yield wrapped
    try:
        raw_conn.close()
    except Exception as exc:
        warnings.warn(f"Failed to close integration DuckDB connection cleanly: {exc}", stacklevel=2)


@pytest.fixture
def duckdb_conn(integration_duckdb) -> duckdb.DuckDBPyConnection:
    """Override the root ``duckdb_conn`` (``:memory:``) for integration tests.

    Many integration tests under ``tests/integration/`` request the root
    fixture ``duckdb_conn`` (defined as plain ``:memory:`` in
    ``tests/conftest.py``). Post-DAT-341 they need the DuckLake-anchored
    connection scoped to ``lake.typed`` — this override forwards them to
    ``integration_duckdb`` without churning every test signature.
    """
    return integration_duckdb


@pytest.fixture
def integration_phases() -> dict[str, Phase]:
    """Create a dict of phase instances for testing."""
    return _build_phase_dict(
        ImportPhase(),
        TypingPhase(),
        StatisticsPhase(),
        StatisticalQualityPhase(),
        RelationshipsPhase(),
        CorrelationsPhase(),
        TemporalPhase(),
    )


@pytest.fixture
def harness(
    integration_engine: Engine,
    integration_duckdb: duckdb.DuckDBPyConnection,
    integration_phases: dict[str, Phase],
) -> PipelineTestHarness:
    """Create a pipeline test harness.

    This is the main fixture for integration tests. It provides:
    - Isolated database connections
    - Pre-configured pipeline phases
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
        phases=integration_phases,
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
def agent_phases() -> dict[str, Phase]:
    """Phase dict with phases needed for agent testing (through entropy)."""
    return _build_phase_dict(
        ImportPhase(),
        TypingPhase(),
        StatisticsPhase(),
        StatisticalQualityPhase(),
        RelationshipsPhase(),
        CorrelationsPhase(),
        TemporalPhase(),
    )


@pytest.fixture
def agent_harness(
    integration_engine: Engine,
    integration_duckdb: duckdb.DuckDBPyConnection,
    agent_phases: dict[str, Phase],
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
        phases=agent_phases,
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
