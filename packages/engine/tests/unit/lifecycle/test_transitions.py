"""Lifecycle substrate tests — transition matrix, stage guard, supersession (DAT-438)."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.lifecycle import (
    ArtifactState,
    IllegalTransitionError,
    LifecycleArtifact,
    StageNotAuthorizedError,
    declare_artifact,
    transition,
)

_STAGE = "operating_model"


def _declare(
    session: Session, run_id: str = "run-1", key: str = "double_entry_balance"
) -> LifecycleArtifact:
    return declare_artifact(
        session,
        artifact_type="validation",
        artifact_key=key,
        run_id=run_id,
        stage=_STAGE,
        teaches={"validation_id": key, "vertical": "finance", "version": "1.0"},
    )


class TestTransitionMatrix:
    def test_declare_creates_declared(self, session: Session) -> None:
        artifact = _declare(session)
        session.flush()

        assert artifact.state == ArtifactState.DECLARED.value
        assert artifact.stage == _STAGE
        assert artifact.strictness is None  # D3: no invented default

    def test_bind_then_execute(self, session: Session) -> None:
        artifact = _declare(session)

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

    def test_execute_without_bind_rejected(self, session: Session) -> None:
        artifact = _declare(session)
        with pytest.raises(IllegalTransitionError, match="requires state 'grounded'"):
            transition(artifact, operation="execute", stage=_STAGE)
        assert artifact.state == ArtifactState.DECLARED.value  # unchanged on rejection

    def test_double_bind_rejected(self, session: Session) -> None:
        artifact = _declare(session)
        transition(artifact, operation="bind", stage=_STAGE)
        with pytest.raises(IllegalTransitionError, match="requires state 'declared'"):
            transition(artifact, operation="bind", stage=_STAGE)

    def test_declare_is_not_a_transition(self, session: Session) -> None:
        artifact = _declare(session)
        with pytest.raises(IllegalTransitionError, match="declare creates"):
            transition(artifact, operation="declare", stage=_STAGE)

    def test_unknown_operation_fails_closed(self, session: Session) -> None:
        artifact = _declare(session)
        with pytest.raises(StageNotAuthorizedError, match="no stage is authorized"):
            transition(artifact, operation="promote", stage=_STAGE)

    def test_ungroundable_reason_is_recorded(self, session: Session) -> None:
        # "Visibly impossible": a failed bind leaves the artifact declared with
        # the reason on the row, never silently absent.
        artifact = _declare(session)
        artifact.state_reason = "no column annotated as debit/credit in the workspace"
        session.flush()

        stored = session.execute(
            select(LifecycleArtifact).where(LifecycleArtifact.artifact_key == artifact.artifact_key)
        ).scalar_one()
        assert stored.state == ArtifactState.DECLARED.value
        assert stored.state_reason is not None


class TestStageAuthorization:
    def test_bind_from_foreign_stage_rejected(self, session: Session) -> None:
        artifact = _declare(session)
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            transition(artifact, operation="bind", stage="begin_session")

    def test_declare_from_foreign_stage_rejected(self, session: Session) -> None:
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            declare_artifact(
                session,
                artifact_type="validation",
                artifact_key="x",
                run_id="run-1",
                stage="add_source",
            )

    def test_endorse_defined_but_no_authority(self, session: Session) -> None:
        # executed → canonical exists in the state machine; no stage may invoke
        # it until the endorsement workflow exists.
        artifact = _declare(session)
        transition(artifact, operation="bind", stage=_STAGE)
        transition(artifact, operation="execute", stage=_STAGE)
        with pytest.raises(StageNotAuthorizedError, match="no authority workflow"):
            transition(artifact, operation="endorse", stage=_STAGE)
        assert artifact.state == ArtifactState.EXECUTED.value

    def test_unknown_artifact_type_fails_closed(self, session: Session) -> None:
        # relationship/explanation are still-deferred types — no authorization
        # rows yet, so any declare fails closed.
        with pytest.raises(StageNotAuthorizedError, match="no stage is authorized"):
            declare_artifact(
                session,
                artifact_type="relationship",
                artifact_key="journal_to_ledger",
                run_id="run-1",
                stage=_STAGE,
            )

    def test_metric_family_flows_through_the_lifecycle(self, session: Session) -> None:
        # metrics are the third lifecycle family (DAT-456): declare → compose →
        # execute are all authorized for operating_model. The grounding verb is
        # ``compose`` (not ``bind``), per architecture-future.
        artifact = declare_artifact(
            session,
            artifact_type="metric",
            artifact_key="dso",
            run_id="run-1",
            stage=_STAGE,
        )
        assert artifact.state == ArtifactState.DECLARED.value
        transition(artifact, operation="compose", stage=_STAGE)
        assert artifact.state == ArtifactState.GROUNDED.value
        transition(artifact, operation="execute", stage=_STAGE)
        assert artifact.state == ArtifactState.EXECUTED.value
        # endorse is defined but has no authority workflow yet.
        with pytest.raises(StageNotAuthorizedError, match="no authority workflow"):
            transition(artifact, operation="endorse", stage=_STAGE)

    def test_metric_grounds_via_compose_not_bind(self, session: Session) -> None:
        # The verbs are family-specific: a metric grounds via ``compose`` and a
        # validation/cycle via ``bind`` — the cross verbs are unauthorized, so
        # the audit trail can't lie about which operation grounded an artifact.
        metric = declare_artifact(
            session,
            artifact_type="metric",
            artifact_key="dso",
            run_id="run-1",
            stage=_STAGE,
        )
        # ("metric", "bind") is not an authorized pair at all → fail closed.
        with pytest.raises(StageNotAuthorizedError, match="no stage is authorized"):
            transition(metric, operation="bind", stage=_STAGE)
        assert metric.state == ArtifactState.DECLARED.value  # unchanged on rejection

        validation = _declare(session)
        with pytest.raises(StageNotAuthorizedError, match="no stage is authorized"):
            transition(validation, operation="compose", stage=_STAGE)

    def test_metric_declare_from_foreign_stage_rejected(self, session: Session) -> None:
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            declare_artifact(
                session,
                artifact_type="metric",
                artifact_key="dso",
                run_id="run-1",
                stage="begin_session",
            )

    def test_cycle_family_flows_through_the_lifecycle(self, session: Session) -> None:
        # cycles are the second lifecycle family (DAT-455): declare → bind →
        # execute are all authorized for operating_model, mirroring validation.
        artifact = declare_artifact(
            session,
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

    def test_cycle_declare_from_foreign_stage_rejected(self, session: Session) -> None:
        with pytest.raises(StageNotAuthorizedError, match="not authorized"):
            declare_artifact(
                session,
                artifact_type="cycle",
                artifact_key="order_to_cash",
                run_id="run-1",
                stage="begin_session",
            )


class TestSupersession:
    def test_redeclare_within_run_reuses_the_row(self, session: Session) -> None:
        """Declare twice in one run → reuse, not an IntegrityError (DAT-502).

        The identity UNIQUE still guards the grain; declare-or-reuse is how a
        success-redelivery converges on it instead of violating it.
        """
        first = _declare(session, run_id="run-1")
        session.flush()
        again = _declare(session, run_id="run-1")
        session.flush()  # no IntegrityError — same row, reset to declared
        assert again.artifact_id == first.artifact_id
        rows = session.execute(select(LifecycleArtifact)).scalars().all()
        assert len(rows) == 1

    def test_redeclare_resets_state_and_reflows(self, session: Session) -> None:
        """The redelivered declare RESETS a flowed row so transition() accepts it.

        transition() requires exact from-states: a leftover executed state from
        the first delivery would reject the redelivered bind. Declare-or-reuse
        clears state/state_reason/grounded_against back to declared and the
        run re-flows the lifecycle on the same row.
        """
        artifact = _declare(session, run_id="run-1")
        transition(artifact, operation="bind", stage=_STAGE, grounded_against={"x": "run-a"})
        transition(artifact, operation="execute", stage=_STAGE)
        artifact.state_reason = "left over from attempt 1"
        session.commit()  # the success-redelivery sees committed rows

        redeclared = _declare(session, run_id="run-1")
        assert redeclared.artifact_id == artifact.artifact_id
        assert redeclared.state == ArtifactState.DECLARED.value
        assert redeclared.state_reason is None
        assert redeclared.grounded_against is None
        # The redelivered run re-flows on the same row.
        transition(redeclared, operation="bind", stage=_STAGE)
        transition(redeclared, operation="execute", stage=_STAGE)
        session.commit()
        rows = session.execute(select(LifecycleArtifact)).scalars().all()
        assert len(rows) == 1
        assert rows[0].state == ArtifactState.EXECUTED.value

    def test_two_runs_coexist_with_independent_states(self, session: Session) -> None:
        # Supersession: the re-run declares anew under its run_id; the prior
        # run's executed row is never mutated.
        prior = _declare(session, run_id="run-1")
        transition(prior, operation="bind", stage=_STAGE)
        transition(prior, operation="execute", stage=_STAGE)
        session.flush()

        _declare(session, run_id="run-2")
        session.flush()

        session.expire_all()  # autoflush-independent: read back from the DB
        rows = (
            session.execute(
                select(LifecycleArtifact)
                .where(LifecycleArtifact.artifact_key == "double_entry_balance")
                .order_by(LifecycleArtifact.run_id)
            )
            .scalars()
            .all()
        )
        assert [(r.run_id, r.state) for r in rows] == [
            ("run-1", ArtifactState.EXECUTED.value),
            ("run-2", ArtifactState.DECLARED.value),
        ]
