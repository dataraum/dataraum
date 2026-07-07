"""Tests for the pinned base-run map resolution (docs/architecture/persistence.md in-run mode, DAT-438)."""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.lifecycle import resolve_operating_model_base_runs
from dataraum.storage.snapshot_head import (
    GENERATION_STAGE,
    MetadataSnapshotHead,
    catalog_head_target,
)


def test_resolves_promoted_heads_once(session: Session) -> None:
    session.add_all(
        [
            MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id="run-bs"),
            MetadataSnapshotHead(target="table:t1", stage=GENERATION_STAGE, run_id="run-a"),
            MetadataSnapshotHead(target="table:t2", stage=GENERATION_STAGE, run_id="run-b"),
        ]
    )
    session.flush()

    pins = resolve_operating_model_base_runs(session, ["t1", "t2"])

    assert pins.relationship_run_id == "run-bs"
    assert pins.semantic_runs == {"t1": "run-a", "t2": "run-b"}


def test_unresolved_heads_are_absent_not_guessed(session: Session) -> None:
    # Only t1 has a promoted generation head; the workspace has no begin_session
    # catalog head at all.
    session.add(MetadataSnapshotHead(target="table:t1", stage=GENERATION_STAGE, run_id="run-a"))
    session.flush()

    pins = resolve_operating_model_base_runs(session, ["t1", "t2"])

    assert pins.relationship_run_id is None  # fail-closed at the readers
    assert pins.semantic_runs == {"t1": "run-a"}  # t2 absent, never guessed


def test_map_is_json_round_trippable(session: Session) -> None:
    # The map is recorded verbatim as grounded_against provenance (D2) and
    # travels through Temporal contracts — it must serialize cleanly.
    session.add(MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id="r"))
    session.flush()

    pins = resolve_operating_model_base_runs(session, [])
    assert pins.model_dump(mode="json") == {"relationship_run_id": "r", "semantic_runs": {}}
