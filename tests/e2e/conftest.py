"""E2E test fixtures.

Runs the full pipeline (including LLM phases) via `runner.run()` against
testdata with known properties. Tests then query the output databases
to verify correctness.

Pipeline output is cached in `.e2e/` at the project root. If output already
exists, the pipeline is not re-run. Use `--e2e-fresh` to force a full re-run.

Requires:
- `uv sync --group e2e` to install dataraum-testdata
- ANTHROPIC_API_KEY set in environment (for LLM phases)
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.pipeline.runner import PhaseRunResult, RunConfig, RunResult, run
from dataraum.storage import Table

# Load .env for ANTHROPIC_API_KEY (same as CLI does in cli/common.py)
load_dotenv()

pytestmark = pytest.mark.e2e

# Fixed output root — cached between test sessions
E2E_ROOT = Path(__file__).resolve().parents[2] / ".e2e"


# =============================================================================
# pytest hook: --e2e-fresh flag
# =============================================================================


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--e2e-fresh",
        action="store_true",
        default=False,
        help="Delete cached E2E output and re-run pipelines from scratch",
    )


@pytest.fixture(scope="session")
def e2e_fresh(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--e2e-fresh", default=False))


# =============================================================================
# Testdata generation (session-scoped, cached)
# =============================================================================


def _generate_testdata(output_dir: Path, strategy: str, fresh: bool) -> Path:
    """Generate testdata CSVs if not already cached."""
    manifest = output_dir / "manifest.yaml"
    if manifest.exists() and not fresh:
        return output_dir

    # Fresh start
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    from testdata.scenarios.runner import run_scenario

    result = run_scenario(
        "month-end-close",
        strategy_name=strategy,
        seed=42,
        output_dir=output_dir,
        fmt="csv",
    )

    # For medium strategy, persist injection registry as YAML for debugging
    if strategy == "medium" and "registry" in result:
        injections = result["registry"].injections
        injection_dicts = [
            {
                "target_file": inj.target_file,
                "target_column": inj.target_column,
                "detector_id": inj.detector_id,
                "injection_type": inj.injection_type,
                "severity": inj.severity,
            }
            for inj in injections
        ]
        with open(output_dir / "injections.yaml", "w") as f:
            yaml.dump(injection_dicts, f)

    return output_dir


def _pipeline_completed(output_dir: Path) -> bool:
    """Check if a cached pipeline run actually completed all phases."""
    from sqlalchemy import create_engine, text

    db_path = output_dir / "metadata.db"
    if not db_path.exists():
        return False
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT status FROM pipeline_runs LIMIT 1")
            ).fetchone()
            return row is not None and row[0] == "completed"
    finally:
        engine.dispose()


def _run_pipeline_cached(
    csv_dir: Path, output_dir: Path, source_name: str, fresh: bool
) -> RunResult:
    """Run pipeline if not already cached."""
    if not fresh and _pipeline_completed(output_dir):
        # Pipeline completed successfully — return cached result with phases
        return RunResult(
            success=True,
            source_id=_read_source_id(output_dir),
            duration_seconds=0.0,
            output_dir=output_dir,
            phases=_read_phases(output_dir),
        )

    # Fresh start
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    config = RunConfig(
        source_path=csv_dir,
        output_dir=output_dir,
        source_name=source_name,
    )
    return run(config).unwrap()


def _read_source_id(output_dir: Path) -> str:
    """Read source_id from an existing pipeline output."""
    from sqlalchemy import create_engine, text

    db_path = output_dir / "metadata.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        row = conn.execute(text("SELECT source_id FROM sources LIMIT 1")).fetchone()
        if row is None:
            raise RuntimeError(f"No source found in {db_path}")
        result: str = row[0]
    engine.dispose()
    return result


def _read_phases(output_dir: Path) -> list[PhaseRunResult]:
    """Read phase results from an existing pipeline output."""
    from sqlalchemy import create_engine, text

    db_path = output_dir / "metadata.db"
    engine = create_engine(f"sqlite:///{db_path}")
    phases: list[PhaseRunResult] = []
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT phase_name, status, duration_seconds FROM phase_checkpoints")
        ).fetchall()
        for row in rows:
            phases.append(
                PhaseRunResult(
                    phase_name=row[0],
                    status=row[1],
                    duration_seconds=row[2] or 0.0,
                )
            )
    engine.dispose()
    return phases


# =============================================================================
# Clean strategy fixtures
# =============================================================================


@pytest.fixture(scope="session")
def testdata_csvs(e2e_fresh: bool) -> Path:
    """Generate testdata CSVs using the clean strategy (no entropy injections)."""
    return _generate_testdata(E2E_ROOT / "clean" / "testdata", "clean", e2e_fresh)


@pytest.fixture(scope="session")
def testdata_manifest(testdata_csvs: Path) -> dict[str, Any]:
    """Parsed manifest.yaml from testdata export."""
    with open(testdata_csvs / "manifest.yaml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def pipeline_output_dir() -> Path:
    """Output directory for the clean pipeline run."""
    return E2E_ROOT / "clean" / "pipeline"


@pytest.fixture(scope="session")
def pipeline_run(
    testdata_csvs: Path,
    pipeline_output_dir: Path,
    e2e_fresh: bool,
) -> RunResult:
    """Run the full pipeline against clean testdata.

    Cached: skips if .e2e/clean/pipeline/metadata.db exists.
    Use --e2e-fresh to force a re-run.
    """
    return _run_pipeline_cached(
        testdata_csvs, pipeline_output_dir, "e2e_testdata", e2e_fresh
    )


# =============================================================================
# Database access for assertions
# =============================================================================


@pytest.fixture(scope="session")
def output_manager(
    pipeline_run: RunResult,
    pipeline_output_dir: Path,
) -> ConnectionManager:
    """ConnectionManager pointing at the pipeline output databases."""
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
# Medium strategy fixtures (for entropy detection tests)
# =============================================================================


@pytest.fixture(scope="session")
def _medium_testdata(e2e_fresh: bool) -> Path:
    """Generate testdata with medium strategy entropy injections."""
    return _generate_testdata(E2E_ROOT / "medium" / "testdata", "medium", e2e_fresh)


@pytest.fixture(scope="session")
def entropy_injections(_medium_testdata: Path) -> list[Any]:
    """Ground truth: list of injections from medium strategy.

    Reads from cached injections.yaml, or regenerates from testdata if needed.
    """
    injections_file = _medium_testdata / "injections.yaml"
    if injections_file.exists():
        with open(injections_file) as f:
            dicts = yaml.safe_load(f)
        # Convert dicts to SimpleNamespace for attribute access (matches EntropyInjection API)
        return [SimpleNamespace(**d) for d in dicts]

    # Fallback: re-generate to get registry (shouldn't happen with cache)
    from testdata.scenarios.runner import run_scenario

    result = run_scenario(
        "month-end-close",
        strategy_name="medium",
        seed=42,
        output_dir=_medium_testdata,
        fmt="csv",
    )
    return result["registry"].injections


@pytest.fixture(scope="session")
def medium_pipeline_output_dir() -> Path:
    """Output directory for the medium pipeline run."""
    return E2E_ROOT / "medium" / "pipeline"


@pytest.fixture(scope="session")
def medium_pipeline_run(
    _medium_testdata: Path,
    medium_pipeline_output_dir: Path,
    e2e_fresh: bool,
) -> RunResult:
    """Run full pipeline on medium-strategy (entropy-injected) data."""
    return _run_pipeline_cached(
        _medium_testdata, medium_pipeline_output_dir, "e2e_medium", e2e_fresh
    )


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
