"""E2E test fixtures.

Runs the full pipeline (including LLM phases) via `runner.run()` against
testdata with known properties. Tests then query the output databases
to verify correctness.

Requires:
- `uv sync --group e2e` to install dataraum-testdata
- ANTHROPIC_API_KEY set in environment (for LLM phases)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.pipeline.runner import RunConfig, RunResult, run
from dataraum.storage import Table

# Load .env for ANTHROPIC_API_KEY (same as CLI does in cli/common.py)
load_dotenv()

pytestmark = pytest.mark.e2e


# =============================================================================
# Testdata generation (session-scoped)
# =============================================================================


@pytest.fixture(scope="session")
def testdata_csvs(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate testdata CSVs using the clean strategy (no entropy injections).

    Returns the directory containing the exported CSV files and manifest.
    """
    from testdata.scenarios.runner import run_scenario

    output_dir = tmp_path_factory.mktemp("testdata")
    run_scenario(
        "month-end-close",
        strategy_name="clean",
        seed=42,
        output_dir=output_dir,
        fmt="csv",
    )
    return output_dir


@pytest.fixture(scope="session")
def testdata_manifest(testdata_csvs: Path) -> dict[str, Any]:
    """Parsed manifest.yaml from testdata export."""
    with open(testdata_csvs / "manifest.yaml") as f:
        return yaml.safe_load(f)


# =============================================================================
# Full pipeline run (session-scoped — runs once, all phases including LLM)
# =============================================================================


@pytest.fixture(scope="session")
def pipeline_output_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Output directory for the pipeline run."""
    return tmp_path_factory.mktemp("pipeline_output")


@pytest.fixture(scope="session")
def pipeline_run(
    testdata_csvs: Path,
    pipeline_output_dir: Path,
) -> RunResult:
    """Run the full pipeline against testdata.

    Uses `runner.run()` — the same code path as `dataraum run`.
    All phases from pipeline.yaml execute, including LLM phases.
    """
    config = RunConfig(
        source_path=testdata_csvs,
        output_dir=pipeline_output_dir,
        source_name="e2e_testdata",
    )

    result = run(config)
    return result.unwrap()


# =============================================================================
# Database access for assertions
# =============================================================================


@pytest.fixture(scope="session")
def output_manager(
    pipeline_run: RunResult,
    pipeline_output_dir: Path,
) -> ConnectionManager:
    """ConnectionManager pointing at the pipeline output databases.

    Opens the metadata.db and data.duckdb produced by the pipeline run.
    """
    conn_config = ConnectionConfig.for_directory(pipeline_output_dir)
    manager = ConnectionManager(conn_config)
    manager.initialize()
    return manager


@pytest.fixture
def metadata_session(output_manager: ConnectionManager) -> Session:  # type: ignore[misc]
    """Fresh SQLAlchemy session for querying pipeline metadata."""
    with output_manager.session_scope() as session:
        yield session


@pytest.fixture(scope="session")
def typed_table_ids(output_manager: ConnectionManager) -> list[str]:
    """Table IDs for typed tables in the pipeline output."""
    with output_manager.session_scope() as session:
        stmt = select(Table.table_id).where(Table.layer == "typed")
        return list(session.execute(stmt).scalars().all())


@pytest.fixture(scope="session")
def typed_table_names(output_manager: ConnectionManager) -> list[str]:
    """Table names for typed tables in the pipeline output."""
    with output_manager.session_scope() as session:
        stmt = select(Table.table_name).where(Table.layer == "typed")
        return list(session.execute(stmt).scalars().all())


# =============================================================================
# Medium strategy testdata + pipeline (for entropy detection tests)
# =============================================================================


@pytest.fixture(scope="session")
def _medium_testdata(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Generate testdata with medium strategy entropy injections.

    Internal fixture — returns the full result dict with an added '_csv_dir' key.
    """
    from testdata.scenarios.runner import run_scenario

    output_dir = tmp_path_factory.mktemp("testdata_medium")
    result = run_scenario(
        "month-end-close",
        strategy_name="medium",
        seed=42,
        output_dir=output_dir,
        fmt="csv",
    )
    result["_csv_dir"] = output_dir
    return result


@pytest.fixture(scope="session")
def entropy_injections(_medium_testdata: dict[str, Any]) -> list[Any]:
    """Ground truth: list of EntropyInjection from medium strategy."""
    return _medium_testdata["registry"].injections


@pytest.fixture(scope="session")
def medium_pipeline_output_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Output directory for the medium pipeline run."""
    return tmp_path_factory.mktemp("pipeline_medium")


@pytest.fixture(scope="session")
def medium_pipeline_run(
    _medium_testdata: dict[str, Any],
    medium_pipeline_output_dir: Path,
) -> RunResult:
    """Run full pipeline on medium-strategy (entropy-injected) data."""
    config = RunConfig(
        source_path=_medium_testdata["_csv_dir"],
        output_dir=medium_pipeline_output_dir,
        source_name="e2e_medium",
    )
    return run(config).unwrap()


@pytest.fixture(scope="session")
def medium_output_manager(
    medium_pipeline_run: RunResult,
    medium_pipeline_output_dir: Path,
) -> ConnectionManager:
    """ConnectionManager for medium pipeline output."""
    conn_config = ConnectionConfig.for_directory(medium_pipeline_output_dir)
    manager = ConnectionManager(conn_config)
    manager.initialize()
    return manager
