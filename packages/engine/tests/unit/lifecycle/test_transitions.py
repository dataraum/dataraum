"""Lifecycle substrate tests — transition matrix, stage guard, supersession (DAT-438)."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import InvestigationSession
from dataraum.lifecycle import (
    ArtifactState,
    IllegalTransitionError,
    LifecycleArtifact,
    StageNotAuthorizedError,
    declare_artifact,
    transition,
)

_SESSION = "sess-lifecycle"
_STAGE = "operating_model"


@pytest.fixture
def journey_session(session: Session) -> str:
    """A seeded InvestigationSession the artifact FK can target."""
    session.add(InvestigationSession(session_id=_SESSION, intent="test"))
    session.flush()
    return _SESSION


def _declare(run_id: str = "run-1", key: str = "double_entry_balance") -> LifecycleArtifact:
    return declare_artifact(
        session_id=_SESSION,
        artifact_type="validation",
        artifact_key=key,
        run_id=run_id,
        stage=_STAGE,
        teaches={"validation_id": key, "vertical": "finance", "version": "1.0"},
    )


class TestTransitionMatrix:
    def test_declare_creates_declared(self, session: Session, journey_session: str) -> None:
        artifact = _declare()
        session.add(artifact)
        session.flush()

        assert artifact.state == ArtifactState.DECLARED.value
        assert artifact.stage == _STAGE
        assert artifact.strictness is None  # D3: no invented default

    def test_bind_then_execute(self, session: Session, journey_session: str) -> None:
        artifact = _declare()
        session.add(artifact)

        transition(
            artifact,
            operation="bind",
            stage=_STAGE,
            grounded_against={"session_detect_run": "run-bs", "semantic_runs": {"t1": "run-a"}},
        )
        assert artifact.state == ArtifactState.GROUNDED.value
        assert artifact.grounded_against == {
            "session_detect_run": "run-bs",
            "semantic_runs": {"t1": "run-a"},
        }

        transition(artifact, operation="execute", stage=_STAGE)
        assert artifact.state == ArtifactState.EXECUTED.value

    def test_execute_without_bind_rejected(self, session: Session, journey_session: str) -> None:
        artifact = _declare()
        with pytest.raises(IllegalTransitionError, match="requires state 'grounded'"):
            transition(artifact, operation="execute", stage=_STAGE)
        assert artifact.state == ArtifactState.DECLARED.value  # unchanged on rejection

    def test_double_bind_rejected(self, session: Session, journey_session: str) -> None:
        artifact = _declare()
        transition(artifact, operation="bind", stage=_STAGE)
        with pytest.raises(IllegalTransitionError, match="requires state 'declared'"):
            transition(artifact, operation="bind", stage=_STAGE)

    def test_declare_is_not_a_transition(self, session: Session, journey_session: str) -> None:
        artifact = _declare()
        with pytest.raises(IllegalTransitionError, match="declare creates"):
            transition(artifact, operation="declare", stage=_STAGE)

    def test_unknown_operation_fails_closed(self) -> None:
        artifact = _declare()
        with pytest.raises(StageNotAuthorizedError, match="no stage is authorized"):
            transition(artifact, operation="promote", stage=_STAGE)

    def test_ungroundable_reason_is_recorded(self, session: Session, journey_session: str) -> None:
        # "Visibly impossible": a failed bind leaves the artifact declared with
        # the reason on the row, never silently absent.
        artifact = _declare()
        session.add(artifact)
        artifact.state_reason = "no column annotated as debit/credit in the workspace"
        session.flush()

        stored = session.execute(
            select(LifecycleArtifact).where(LifecycleArtifact.artifact_key == artifact.artifact_key)
        ).scalar_one()
        assert stored.state == ArtifactState.DECLARED.value
        assert stored.state_reason is not None


class TestStageAuthorization:
    def test_bind_from_foreign_stage_rejected(self) -> None:
        artifact = _declare()
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            transition(artifact, operation="bind", stage="begin_session")

    def test_declare_from_foreign_stage_rejected(self) -> None:
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            declare_artifact(
                session_id=_SESSION,
                artifact_type="validation",
                artifact_key="x",
                run_id="run-1",
                stage="add_source",
            )

    def test_endorse_defined_but_no_authority(self) -> None:
        # executed → canonical exists in the state machine; no stage may invoke
        # it until the endorsement workflow exists.
        artifact = _declare()
        transition(artifact, operation="bind", stage=_STAGE)
        transition(artifact, operation="execute", stage=_STAGE)
        with pytest.raises(StageNotAuthorizedError, match="no authority workflow"):
            transition(artifact, operation="endorse", stage=_STAGE)
        assert artifact.state == ArtifactState.EXECUTED.value

    def test_unknown_artifact_type_fails_closed(self) -> None:
        # metrics is a later slice — no authorization rows yet.
        with pytest.raises(StageNotAuthorizedError, match="no stage is authorized"):
            declare_artifact(
                session_id=_SESSION,
                artifact_type="metric",
                artifact_key="ebitda",
                run_id="run-1",
                stage=_STAGE,
            )

    def test_cycle_family_flows_through_the_lifecycle(self) -> None:
        # cycles are the second lifecycle family (DAT-455): declare → bind →
        # execute are all authorized for operating_model, mirroring validation.
        artifact = declare_artifact(
            session_id=_SESSION,
            artifact_type="cycle",
            artifact_key="order_to_cash",
            run_id="run-1",
            stage=_STAGE,
        )
        assert artifact.state == ArtifactState.DECLARED.value
        transition(artifact, operation="bind", stage=_STAGE)
        assert artifact.state == ArtifactState.GROUNDED.value
        transition(artifact, operation="execute", stage=_STAGE)
        assert artifact.state == ArtifactState.EXECUTED.value
        # endorse is defined but has no authority workflow yet.
        with pytest.raises(StageNotAuthorizedError, match="no authority workflow"):
            transition(artifact, operation="endorse", stage=_STAGE)

    def test_cycle_declare_from_foreign_stage_rejected(self) -> None:
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            declare_artifact(
                session_id=_SESSION,
                artifact_type="cycle",
                artifact_key="order_to_cash",
                run_id="run-1",
                stage="begin_session",
            )


class TestSupersession:
    def test_identity_unique_within_run(self, session: Session, journey_session: str) -> None:
        session.add(_declare(run_id="run-1"))
        session.flush()
        session.add(_declare(run_id="run-1"))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_two_runs_coexist_with_independent_states(
        self, session: Session, journey_session: str
    ) -> None:
        # Supersession: the re-run declares anew under its run_id; the prior
        # run's executed row is never mutated.
        prior = _declare(run_id="run-1")
        session.add(prior)
        transition(prior, operation="bind", stage=_STAGE)
        transition(prior, operation="execute", stage=_STAGE)
        session.flush()

        current = _declare(run_id="run-2")
        session.add(current)
        session.flush()

        session.expire_all()  # autoflush-independent: read back from the DB
        rows = (
            session.execute(
                select(LifecycleArtifact)
                .where(LifecycleArtifact.session_id == _SESSION)
                .order_by(LifecycleArtifact.run_id)
            )
            .scalars()
            .all()
        )
        assert [(r.run_id, r.state) for r in rows] == [
            ("run-1", ArtifactState.EXECUTED.value),
            ("run-2", ArtifactState.DECLARED.value),
        ]
