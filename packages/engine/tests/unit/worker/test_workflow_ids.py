"""Tests for the child workflow ID convention (DAT-364, DAT-506).

Parent workflow IDs are owned by the cockpit Client; the engine derives only the
CHILD ``processTableWorkflow`` id, as a pure function of the parent's own
``workflow.info().workflow_id`` (DAT-506) — no workspace/session segment of its
own. These pin that the child nests under the parent + a ``-table-<raw>`` suffix
and that distinct parents never collide on a child id, without spinning up a
Temporal worker.
"""

from __future__ import annotations

from dataraum.worker.contracts import (
    cockpit_task_queue_for,
    operating_model_workflow_id,
    process_table_workflow_id,
)

_PARENT_A = "addsource-12345678-1234-1234-1234-123456789abc-run-7"
_PARENT_B = "addsource-00000000-0000-0000-0000-000000000001-run-7"
_RAW = "raw-3"


def test_child_id_is_parent_prefixed() -> None:
    """The child ID nests under the parent ID + a ``-table-<raw>`` suffix."""
    child = process_table_workflow_id(_PARENT_A, _RAW)
    assert child == f"{_PARENT_A}-table-{_RAW}"
    assert child.startswith(_PARENT_A)


def test_distinct_parents_do_not_collide() -> None:
    """The DAT-364 anti-footgun: identical raw table, distinct parent → distinct ids."""
    assert process_table_workflow_id(_PARENT_A, _RAW) != process_table_workflow_id(_PARENT_B, _RAW)


def test_operating_model_id_matches_the_cockpit_convention() -> None:
    """`operatingmodel-<ws>` — the cascade and the cockpit's manual re-trigger share it.

    The cockpit derives the identical id in ``src/temporal/workflow-id.ts``
    (``operatingModelWorkflowId``); the literal here pins the hand-mirrored
    convention so a drift on either side fails a test, not a live run.
    """
    assert operating_model_workflow_id("ws-1") == "operatingmodel-ws-1"


def test_cockpit_task_queue_matches_the_cockpit_convention() -> None:
    """`cockpit-<ws>` — the per-workspace cockpit activity queue (DAT-818).

    The orchestration workflows derive the callback queue from their input
    ``workspace_id``; the cockpit's activity-only worker derives the identical
    name from its boot identity in ``src/temporal/task-queue.ts``
    (``cockpitTaskQueueFor``). The literal here pins the hand-mirrored
    convention — a drift strands callbacks on an unpolled queue.
    """
    assert cockpit_task_queue_for("ws-1") == "cockpit-ws-1"
