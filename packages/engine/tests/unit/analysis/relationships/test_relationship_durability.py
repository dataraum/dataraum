"""Relationship catalog durability + suppression + readiness read-gating (DAT-408).

- candidate rows re-derive each run; llm/manual are durable (user-drop only).
- a user drop (ConfigOverlay action="reject") suppresses re-creation + readiness.
- relationship readiness reads are head-resolved and gated on a live, non-suppressed
  Relationship — no ghost readiness.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.detector import _store_candidates
from dataraum.analysis.relationships.utils import (
    load_confirmed_relationship_pairs,
    load_defined_relationships,
    load_suppressed_relationship_pairs,
)
from dataraum.entropy.db_models import EntropyReadinessRecord
from dataraum.entropy.models import relationship_target_key
from dataraum.entropy.views.readiness_context import load_relationship_readiness
from dataraum.storage import Column, ConfigOverlay, Source, Table
from dataraum.storage.snapshot_head import MetadataSnapshotHead, session_head_target
from tests.conftest import baseline_session_id


def _seed_tables_columns(session: Session) -> None:
    session.add(Source(source_id="s1", name="s1", source_type="csv"))
    session.add(Table(table_id="t1", source_id="s1", table_name="orders", layer="typed"))
    session.add(Table(table_id="t2", source_id="s1", table_name="customers", layer="typed"))
    session.add(Column(column_id="ca", table_id="t1", column_name="customer_id", column_position=0))
    session.add(Column(column_id="cb", table_id="t2", column_name="id", column_position=0))
    session.flush()


def _rel(session: Session, frm: str, to: str, method: str, run_id: str | None = None) -> None:
    session.add(
        Relationship(
            session_id=baseline_session_id(),
            run_id=run_id,
            from_table_id="t1",
            from_column_id=frm,
            to_table_id="t2",
            to_column_id=to,
            relationship_type="candidate" if method == "candidate" else "foreign_key",
            confidence=0.9,
            detection_method=method,
        )
    )


def test_redrive_deletes_candidates_keeps_llm_and_manual(session: Session) -> None:
    """A re-derive clears this session's candidate rows; llm/manual survive."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "candidate")
    _rel(session, "ca", "cb", "llm")
    _rel(session, "ca", "cb", "manual")
    session.flush()

    # Empty candidate set → only the delete path runs (no DuckDB needed).
    _store_candidates(session, baseline_session_id(), ["t1", "t2"], candidates=[])
    session.flush()

    methods = {r.detection_method for r in session.query(Relationship).all()}
    assert methods == {"llm", "manual"}, "candidate re-derive must not touch llm/manual"


def test_redrive_skips_a_suppressed_candidate(session: Session) -> None:
    """A user-dropped pair is not re-created by candidate re-derivation (AC4)."""
    from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate

    _seed_tables_columns(session)
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "reject", "from_column_id": "ca", "to_column_id": "cb"},
        )
    )
    session.flush()

    candidate = RelationshipCandidate(
        table1="orders",
        table2="customers",
        join_candidates=[
            JoinCandidate(
                column1="customer_id", column2="id", join_confidence=0.9, cardinality="many-to-one"
            )
        ],
    )
    _store_candidates(session, baseline_session_id(), ["t1", "t2"], [candidate])
    session.flush()

    made = session.query(Relationship).filter_by(detection_method="candidate").all()
    assert made == [], "a suppressed pair must not be re-created on re-derive"


def test_load_defined_relationships_excludes_candidates_and_scopes_run(session: Session) -> None:
    """The shared 'defined' read = not candidate, run-scoped (DAT-408)."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "candidate", run_id="run-A")
    _rel(session, "ca", "cb", "llm", run_id="run-A")
    _rel(session, "ca", "cb", "manual", run_id="run-A")
    _rel(session, "ca", "cb", "llm", run_id="run-B")  # a different run
    session.flush()

    defined = load_defined_relationships(session, ["t1", "t2"], run_id="run-A")
    methods = sorted(r.detection_method for r in defined)
    assert methods == ["llm", "manual"], "candidate excluded; run-B's llm out of scope"
    assert all(r.run_id == "run-A" for r in defined)


def test_suppressed_pairs_read_from_reject_overlay(session: Session) -> None:
    """``load_suppressed_relationship_pairs`` returns only active reject pairs."""
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "reject", "from_column_id": "ca", "to_column_id": "cb"},
        )
    )
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "confirm", "from_column_id": "cc", "to_column_id": "cd"},
        )
    )
    session.flush()
    assert load_suppressed_relationship_pairs(session) == {("ca", "cb")}


def test_confirmed_pairs_read_from_confirm_overlay(session: Session) -> None:
    """``load_confirmed_relationship_pairs`` returns active confirm pairs, undirected.

    One relationship-overlay shape (DAT-409): confirm/reject differ only by ``action``
    and both key on the column pair. Confirmation is undirected (frozenset), so a
    detector matches it whichever way it names the endpoints; reject rows are excluded.
    """
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "confirm", "from_column_id": "ca", "to_column_id": "cb"},
        )
    )
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "reject", "from_column_id": "cc", "to_column_id": "cd"},
        )
    )
    session.flush()
    confirmed = load_confirmed_relationship_pairs(session)
    assert confirmed == {frozenset({"ca", "cb"})}
    # Undirected: matches whichever way the detector names the endpoints.
    assert frozenset({"cb", "ca"}) in confirmed


def _readiness_row(session: Session, target: str, run_id: str) -> None:
    session.add(
        EntropyReadinessRecord(
            session_id=baseline_session_id(),
            target=target,
            table_id=None,
            column_id=None,
            run_id=run_id,
            band="investigate",
            worst_intent_risk=0.5,
        )
    )


def _seal(session: Session, run_id: str) -> None:
    """Seal the session at ``run_id`` via the per-session head (DAT-408)."""
    session.add(
        MetadataSnapshotHead(
            target=session_head_target(baseline_session_id()), stage="detect", run_id=run_id
        )
    )


def test_relationship_readiness_promoted_run_only_and_gated(session: Session) -> None:
    """Reader returns the sealed run's readiness, gated on that run's live pairs."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="run-B")  # live in the promoted run
    live_target = relationship_target_key("ca", "cb")
    ghost_target = relationship_target_key("ca", "zz")  # no Relationship row

    _readiness_row(session, live_target, "run-A")  # not the sealed run → excluded
    _readiness_row(session, live_target, "run-B")  # sealed run → surfaces
    _readiness_row(session, ghost_target, "run-B")  # no live rel this run → excluded
    _seal(session, "run-B")
    session.flush()

    out = load_relationship_readiness(session, baseline_session_id())
    assert {(r.target, r.run_id) for r in out} == {(live_target, "run-B")}


def test_relationship_readiness_excludes_suppressed(session: Session) -> None:
    """A user-dropped (rejected) relationship's readiness is not surfaced."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="run-B")
    target = relationship_target_key("ca", "cb")
    _readiness_row(session, target, "run-B")
    _seal(session, "run-B")
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "reject", "from_column_id": "ca", "to_column_id": "cb"},
        )
    )
    session.flush()

    assert load_relationship_readiness(session, baseline_session_id()) == []
