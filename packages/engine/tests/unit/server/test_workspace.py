"""Tests for ``dataraum.server.workspace.bootstrap_workspace``.

Post-DAT-343 the bootstrap is a thin pointer-setter: pull the workspace_id
from ``DATARAUM_WORKSPACE_ID`` (typed via Settings/Pydantic) and stash it
on a module-level pointer so ``get_active_workspace_id()`` returns without
a DB hit. The DAT-358 filesystem overlay (writable config_dir under
``DATARAUM_HOME``, ``_adhoc`` vertical scaffold) is gone — teach edits now
live in the per-workspace ``config_overlay`` Postgres table.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pydantic import ValidationError

import dataraum.server.workspace as _ws


@pytest.fixture(autouse=True)
def _isolate_active_workspace() -> Iterator[None]:
    """Reset the module-level pointer around each test, then restore.

    Pre-reset so tests like ``test_get_active_workspace_id_raises_before_bootstrap``
    see a clean None state. Restore on teardown rather than zero — matters
    because ``tests/conftest.py`` stamps ``_active_workspace_id`` at import
    time so every unit test that exercises a Postgres engine resolves a
    workspace_id without running ``bootstrap_workspace`` itself. If this
    fixture left the pointer at ``None`` after the module finished, any
    later test module touching Postgres-dialect code would hit
    ``RuntimeError: No active workspace``.
    """
    saved_pointer = _ws._active_workspace_id
    _ws.reset_active_workspace_id_for_tests()
    yield
    _ws._active_workspace_id = saved_pointer


_FIXED_WS_ID = "00000000-0000-0000-0000-0000000000aa"


def _set_matching_queue(monkeypatch: pytest.MonkeyPatch, workspace_id: str) -> None:
    """Point ``TEMPORAL_TASK_QUEUE`` at the workspace's queue (DAT-505).

    ``bootstrap_workspace`` asserts the queue is ``engine-<workspace_id>``, so a
    test that bootstraps a non-default workspace must set the matching queue or
    the boot assertion fires.
    """
    monkeypatch.setenv("TEMPORAL_TASK_QUEUE", _ws.task_queue_for(workspace_id))


def test_bootstrap_returns_workspace_id_from_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)
    _set_matching_queue(monkeypatch, _FIXED_WS_ID)

    workspace_id = _ws.bootstrap_workspace()

    assert workspace_id == _FIXED_WS_ID


def test_bootstrap_sets_module_pointer_for_get_active_workspace_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)
    _set_matching_queue(monkeypatch, _FIXED_WS_ID)

    _ws.bootstrap_workspace()

    assert _ws.get_active_workspace_id() == _FIXED_WS_ID


def test_bootstrap_asserts_queue_matches_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The single workspace-isolation guard (DAT-505): a queue env that does not
    match the workspace fails loud at boot — the per-workspace queue is the
    isolation boundary that replaced the 8 per-activity mismatch checks."""
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)
    # A queue for a DIFFERENT workspace — a misconfigured container.
    monkeypatch.setenv("TEMPORAL_TASK_QUEUE", "engine-some-other-workspace")

    with pytest.raises(RuntimeError, match="Workspace/queue mismatch"):
        _ws.bootstrap_workspace()


class TestTaskQueueFor:
    """``task_queue_for`` derives the per-workspace Temporal queue (DAT-505)."""

    def test_prefixes_engine(self) -> None:
        assert _ws.task_queue_for(_FIXED_WS_ID) == f"engine-{_FIXED_WS_ID}"

    def test_keeps_id_verbatim(self) -> None:
        # Dashes are NOT translated (unlike schema_name_for) — Temporal queue
        # names have no charset restriction, matching the workflow-ID convention.
        assert _ws.task_queue_for("test") == "engine-test"


def test_get_active_workspace_id_raises_before_bootstrap() -> None:
    # autouse fixture has already reset the pointer; calling without a
    # bootstrap is the precondition.
    with pytest.raises(RuntimeError, match="No active workspace"):
        _ws.get_active_workspace_id()


def test_bootstrap_raises_when_workspace_id_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATARAUM_WORKSPACE_ID", raising=False)

    # Resolution flows through typed settings (DAT-363): a missing var surfaces
    # as a pydantic ValidationError naming the field.
    with pytest.raises(ValidationError, match="dataraum_workspace_id"):
        _ws.bootstrap_workspace()


class TestSchemaNameFor:
    """``schema_name_for`` derives a Postgres schema from a workspace_id."""

    def test_uuid_dashes_become_underscores(self) -> None:
        assert (
            _ws.schema_name_for("00000000-0000-0000-0000-0000000000aa")
            == "ws_00000000_0000_0000_0000_0000000000aa"
        )

    def test_short_identifier_passes_through(self) -> None:
        assert _ws.schema_name_for("test") == "ws_test"

    def test_rejects_invalid_identifier_chars(self) -> None:
        with pytest.raises(ValueError, match="not a valid"):
            _ws.schema_name_for("bad name with spaces")

    def test_rejects_overlong_identifier(self) -> None:
        # 60-char workspace id → "ws_" + 60 = 63 chars (exactly the PG
        # limit; allowed). 61 char id → 64 chars (over; rejected).
        ok = "a" * 60
        too_long = "a" * 61
        assert _ws.schema_name_for(ok) == "ws_" + ok
        with pytest.raises(ValueError, match="max out at 63"):
            _ws.schema_name_for(too_long)
