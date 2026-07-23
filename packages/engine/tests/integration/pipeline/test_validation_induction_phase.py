"""The validation induction phase — generate-over-graph + nothing_declared guard (DAT-735).

The graph-serve and the induction LLM are mocked at their boundaries; the phase's own
logic — the generated count (NEVER a declared count), persistence as source='generated',
and fault-isolated degradation — runs against the real session fixture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.validation.db_models import Validation
from dataraum.analysis.validation.induction import Membership
from dataraum.analysis.validation.models import ValidationSeverity, ValidationSpec
from dataraum.core.models.base import Result
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.validation_induction_phase import ValidationInductionPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_SERVE_TARGET = "dataraum.pipeline.phases.validation_induction_phase.build_served_context"
_INDUCE_TARGET = (
    "dataraum.pipeline.phases.validation_induction_phase.ValidationInductionAgent.induce"
)


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so the phase can initialize without config."""
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}

    with (
        patch(
            "dataraum.pipeline.phases.validation_induction_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.validation_induction_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.validation_induction_phase.PromptRenderer",
            return_value=MagicMock(),
        ),
    ):
        yield


@pytest.fixture
def workspace_table(session: Session) -> Table:
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()
    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="journal_entries",
        layer="typed",
        duckdb_path="typed_journal_entries",
        row_count=10,
    )
    session.add(table)
    session.flush()
    session.add(
        Column(table_id=table.table_id, column_name="amount", column_position=0, raw_type="VARCHAR")
    )
    session.commit()
    return table


def _make_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    *,
    vertical: str | None = "finance",
) -> PhaseContext:
    config: dict = {"base_runs": {"relationship_run_id": "cat-run"}, "workspace_id": "ws-test"}
    if vertical is not None:
        config["vertical"] = vertical
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        run_id="run-om-1",
        config=config,
    )


def _gen_spec(validation_id: str) -> ValidationSpec:
    return ValidationSpec(
        validation_id=validation_id,
        name=validation_id,
        description="induced",
        category="data_quality",
        severity=ValidationSeverity.WARNING,
        check_type="constraint",
        tolerance=0.02,
        guidance="ground it",
    )


def _generated(session: Session) -> dict[str, Validation]:
    return {
        r.validation_id: r
        for r in session.execute(
            select(Validation).where(
                Validation.source == "generated", Validation.superseded_at.is_(None)
            )
        ).scalars()
    }


class TestValidationInductionPhase:
    def test_generated_count_is_never_a_declared_count(
        self, session, duckdb_conn, workspace_table, _mock_llm
    ) -> None:
        """DAT-735/deliverable-4: outputs carry `generated`, NEVER `declared`.

        The nothing_declared gate keys on validation/cycles/metrics `declared`; if this
        phase ever emitted `declared`, a thin-graph zero could flip the gate. It must not.
        """
        with (
            patch(_SERVE_TARGET, return_value=("<graph>", "conv", Membership())),
            patch(_INDUCE_TARGET, return_value=Result.ok([_gen_spec("induced_x")])),
        ):
            result = ValidationInductionPhase()._run(
                _make_ctx(session, duckdb_conn, [workspace_table.table_id])
            )

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["generated"] == 1
        assert "declared" not in result.outputs
        assert "induced_x" in _generated(session)

    def test_thin_graph_zero_generated_is_not_declared(
        self, session, duckdb_conn, workspace_table, _mock_llm
    ) -> None:
        """A thin graph → zero generated. Still no `declared` key; supersedes prior."""
        with (
            patch(_SERVE_TARGET, return_value=("<graph>", "", Membership())),
            patch(_INDUCE_TARGET, return_value=Result.ok([])),
        ):
            result = ValidationInductionPhase()._run(
                _make_ctx(session, duckdb_conn, [workspace_table.table_id])
            )

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["generated"] == 0
        assert "declared" not in result.outputs

    def test_degraded_induction_does_not_sink_the_run(
        self, session, duckdb_conn, workspace_table, _mock_llm
    ) -> None:
        """A parse/render failure degrades to generated=0 — never fails the OM run."""
        with (
            patch(_SERVE_TARGET, return_value=("<graph>", "", Membership())),
            patch(_INDUCE_TARGET, return_value=Result.fail("parse failed")),
        ):
            result = ValidationInductionPhase()._run(
                _make_ctx(session, duckdb_conn, [workspace_table.table_id])
            )

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "degraded"
        assert result.outputs["generated"] == 0

    def test_no_vertical_is_a_loud_zero(self, session, duckdb_conn, workspace_table) -> None:
        result = ValidationInductionPhase()._run(
            _make_ctx(session, duckdb_conn, [workspace_table.table_id], vertical=None)
        )
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_vertical"
        assert result.outputs["generated"] == 0

    def test_missing_workspace_id_fails_loud(
        self, session, duckdb_conn, workspace_table, _mock_llm
    ) -> None:
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        del ctx.config["workspace_id"]
        result = ValidationInductionPhase()._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "workspace_id missing" in (result.error or "")
