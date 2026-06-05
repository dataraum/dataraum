"""Tests for the workspace-scoped workflow ID convention (DAT-364, DAT-422).

The parent (``addSourceWorkflow``) and child (``processTableWorkflow``) IDs
encode ``workspace_id`` as their first segment so slice 2+ multi-workspace
routing is a no-op and two workspaces sharing a ``session_id`` never collide on
a workflow ID. The parent is keyed by ``session_id`` (DAT-422) — a run ingests a
SET of objects from 1–N sources, so the run, not a source, is the identity (it
mirrors ``begin_session_workflow_id``). These pin the format + the cross-workspace
distinctness without spinning up a Temporal worker (the parent ID is built
cockpit-side; this Python helper backs the child-ID builder + these tests).
"""

from __future__ import annotations

from dataraum.worker.contracts import add_source_workflow_id, process_table_workflow_id

_WS_A = "12345678-1234-1234-1234-123456789abc"
_WS_B = "00000000-0000-0000-0000-000000000001"
_SESSION = "sess-7"
_RAW = "raw-3"


def test_parent_id_encodes_workspace_then_session() -> None:
    assert add_source_workflow_id(_WS_A, _SESSION) == f"addsource-{_WS_A}-{_SESSION}"


def test_child_id_is_parent_prefixed() -> None:
    """The child ID nests under the parent ID + a ``-table-<raw>`` suffix."""
    child = process_table_workflow_id(_WS_A, _SESSION, _RAW)
    assert child == f"{add_source_workflow_id(_WS_A, _SESSION)}-table-{_RAW}"
    assert child.startswith(f"addsource-{_WS_A}-{_SESSION}")


def test_same_session_different_workspaces_do_not_collide() -> None:
    """The DAT-364 anti-footgun: identical session+table, distinct workspace → distinct IDs."""
    assert add_source_workflow_id(_WS_A, _SESSION) != add_source_workflow_id(_WS_B, _SESSION)
    assert process_table_workflow_id(_WS_A, _SESSION, _RAW) != process_table_workflow_id(
        _WS_B, _SESSION, _RAW
    )
