"""Phase wiring for ``dimension_hierarchies`` (DAT-762).

Pins the two contracts the phase itself owns: judge MISCONFIGURATION fails
the phase (the standard agent posture — no judge-off state, no silent lane
drop), and the ``bus_matrix`` stats ride the phase outputs with their
documented keys (the conform lane's observability surface).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.hierarchies.bus_matrix import BusMatrixStats
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.dimension_hierarchies_phase import DimensionHierarchiesPhase
from tests.conftest import baseline_run_id

_MOD = "dataraum.pipeline.phases.dimension_hierarchies_phase"


def _ctx(session: Session, duckdb_conn: duckdb.DuckDBPyConnection) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["t1"],
        run_id=baseline_run_id(),
    )


def _llm_config() -> MagicMock:
    config = MagicMock()
    config.active_provider = "anthropic"
    config.providers = {"anthropic": MagicMock()}
    return config


@patch(f"{_MOD}.load_llm_config", side_effect=FileNotFoundError("no config"))
def test_missing_llm_config_fails_phase(
    _load: MagicMock, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    result = DimensionHierarchiesPhase()._run(_ctx(session, duckdb_conn))
    assert result.status == PhaseStatus.FAILED
    assert "LLM config not found" in (result.error or "")


@patch(f"{_MOD}.load_llm_config")
def test_unconfigured_provider_fails_phase(
    load: MagicMock, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    config = _llm_config()
    config.providers = {}
    load.return_value = config
    result = DimensionHierarchiesPhase()._run(_ctx(session, duckdb_conn))
    assert result.status == PhaseStatus.FAILED
    assert "not configured" in (result.error or "")


@patch(f"{_MOD}.create_provider", side_effect=RuntimeError("boom"))
@patch(f"{_MOD}.load_llm_config")
def test_provider_creation_failure_fails_phase(
    load: MagicMock,
    _create: MagicMock,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    load.return_value = _llm_config()
    result = DimensionHierarchiesPhase()._run(_ctx(session, duckdb_conn))
    assert result.status == PhaseStatus.FAILED
    assert "Failed to create LLM provider" in (result.error or "")


@patch(f"{_MOD}.derive_bus_matrix")
@patch(f"{_MOD}.discover_dimension_hierarchies")
@patch(f"{_MOD}.PromptRenderer")
@patch(f"{_MOD}.create_provider")
@patch(f"{_MOD}.load_llm_config")
def test_outputs_carry_bus_matrix(
    load: MagicMock,
    _create: MagicMock,
    _renderer: MagicMock,
    discover: MagicMock,
    derive: MagicMock,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    load.return_value = _llm_config()
    bus = BusMatrixStats(status="ran", referenced=1, folded=2, conform_pairs=1, conformed=1)
    discover.return_value = 4
    derive.return_value = (3, bus)

    result = DimensionHierarchiesPhase()._run(_ctx(session, duckdb_conn))

    assert result.status == PhaseStatus.COMPLETED
    assert result.outputs["hierarchies"] == 4
    assert result.outputs["bus_matrix"] == bus.as_output()
    assert "veto_lane" not in result.outputs
    # The documented observability keys — eval liveness asserts read these.
    assert {
        "status",
        "referenced",
        "folded",
        "conform_pairs",
        "conformed",
        "abstained",
        "unanswered",
    } <= set(result.outputs["bus_matrix"])
    assert result.records_created == 7
