"""Unit tests for the pure grounding-loop decision (DAT-551 P3c, ported in DAT-708).

The grounding loop's wiring is compose-smoke covered; this pins the branch
logic of :func:`decide_grounding_step` — the same five cases the cockpit's
``grounding-step.test.ts`` pinned before the workflow moved to Python.
"""

from __future__ import annotations

from dataraum.worker.contracts import AssessAndGroundResult
from dataraum.worker.workflows import GroundingStep, decide_grounding_step


def test_replays_when_teaches_applied_and_attempts_remain() -> None:
    step = decide_grounding_step(
        AssessAndGroundResult(appliedCount=2, needsJudgement=False, judgementNote=None),
        2,
    )
    assert step == GroundingStep(action="replay")


def test_surfaces_exhausted_when_teaches_applied_but_no_attempts_remain() -> None:
    step = decide_grounding_step(
        AssessAndGroundResult(appliedCount=1, needsJudgement=False, judgementNote="x"),
        0,
    )
    assert step == GroundingStep(action="surface", reason="exhausted", note="x")


def test_done_when_nothing_applied_and_no_judgement_needed() -> None:
    step = decide_grounding_step(
        AssessAndGroundResult(appliedCount=0, needsJudgement=False, judgementNote=None),
        3,
    )
    assert step == GroundingStep(action="done")


def test_surfaces_judgement_when_nothing_mechanical_but_human_gap_remains() -> None:
    step = decide_grounding_step(
        AssessAndGroundResult(
            appliedCount=0,
            needsJudgement=True,
            judgementNote="payments.method needs a concept",
        ),
        3,
    )
    assert step == GroundingStep(
        action="surface", reason="judgement", note="payments.method needs a concept"
    )


def test_prioritises_replay_over_judgement_note_while_attempts_remain() -> None:
    # Applied teaches + a flagged judgement gap + attempts left → replay; the
    # judgement is re-evaluated next round on fresh readiness.
    step = decide_grounding_step(
        AssessAndGroundResult(appliedCount=1, needsJudgement=True, judgementNote="later"),
        1,
    )
    assert step == GroundingStep(action="replay")
