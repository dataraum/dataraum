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
from dataraum.analysis.relationships.materialize import (
    materialize_relationship_overlays,
    write_relationship_keepers,
)
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


def test_redrive_upserts_candidates_keeps_llm_and_manual(session: Session) -> None:
    """A re-derive converges candidate rows by upsert; llm/manual survive (DAT-502).

    The candidate writer is a form-(a) upsert on the same-pair-same-method key
    — no run-scoped clear. A redelivered derivation for the SAME pair updates
    the candidate row in place and never touches the durable llm/manual rows
    sharing the column pair (``detection_method`` is part of the key).
    """
    from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate

    _seed_tables_columns(session)
    # run_id stamped: the upsert key is NULLS-DISTINCT, so only stamped rows
    # converge (the workflow path always stamps; DAT-502).
    _rel(session, "ca", "cb", "candidate", run_id="run-A")
    _rel(session, "ca", "cb", "llm", run_id="run-A")
    _rel(session, "ca", "cb", "manual", run_id="run-A")
    session.flush()

    candidate = RelationshipCandidate(
        table1="orders",
        table2="customers",
        join_candidates=[
            JoinCandidate(
                column1="customer_id", column2="id", join_confidence=0.7, cardinality="many-to-one"
            )
        ],
    )
    _store_candidates(session, baseline_session_id(), ["t1", "t2"], [candidate], run_id="run-A")
    session.flush()
    session.expire_all()

    rows = session.query(Relationship).all()
    by_method = {r.detection_method: r for r in rows}
    assert set(by_method) == {"candidate", "llm", "manual"}, "one row per method (converged)"
    assert len(rows) == 3
    assert by_method["candidate"].confidence == 0.7, "candidate updated in place"
    assert by_method["llm"].confidence == 0.9, "llm untouched"
    assert by_method["manual"].confidence == 0.9, "manual untouched"


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


# --- Materialize-from-overlay (DAT-409 C2) ---------------------------------------


def _overlay(session: Session, action: str, frm: str, to: str) -> None:
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": action, "from_column_id": frm, "to_column_id": to},
        )
    )


def _materialized(session: Session) -> list[Relationship]:
    return (
        session.query(Relationship)
        .filter(Relationship.detection_method.in_(("manual", "keeper")))
        .all()
    )


def test_materialize_add_overlay_creates_manual(session: Session) -> None:
    """An `add` overlay materializes a durable `manual` row stamped with the run."""
    _seed_tables_columns(session)
    _overlay(session, "add", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(
        session, baseline_session_id(), run_id="r1", table_ids=["t1", "t2"]
    )
    session.flush()

    assert count == 1
    rows = _materialized(session)
    assert len(rows) == 1
    row = rows[0]
    assert row.detection_method == "manual"
    assert row.run_id == "r1"
    assert (row.from_table_id, row.to_table_id) == ("t1", "t2")
    assert row.is_confirmed is True


def test_materialize_keep_overlay_creates_keeper(session: Session) -> None:
    """A `keep` overlay (silent-accept) materializes as `keeper`, not `manual`."""
    _seed_tables_columns(session)
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    materialize_relationship_overlays(
        session, baseline_session_id(), run_id="r1", table_ids=["t1", "t2"]
    )
    session.flush()

    rows = _materialized(session)
    assert [r.detection_method for r in rows] == ["keeper"]


def test_materialize_skips_pair_already_llm_this_run(session: Session) -> None:
    """No duplicate: an overlay for a pair the run already produced as llm is skipped."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(
        session, baseline_session_id(), run_id="r1", table_ids=["t1", "t2"]
    )
    session.flush()

    assert count == 0
    # The pair stays a single llm row — never duplicated as keeper.
    pairs = session.query(Relationship).filter(Relationship.run_id == "r1").all()
    assert [p.detection_method for p in pairs] == ["llm"]


def test_materialize_skips_a_rejected_pair(session: Session) -> None:
    """A reject overlay wins over a stale add/keep for the same pair."""
    _seed_tables_columns(session)
    _overlay(session, "add", "ca", "cb")
    _overlay(session, "reject", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(
        session, baseline_session_id(), run_id="r1", table_ids=["t1", "t2"]
    )
    session.flush()

    assert count == 0
    assert _materialized(session) == []


def test_materialize_is_idempotent_per_run(session: Session) -> None:
    """Re-running the same run_id clears its own durable rows first — no accumulation."""
    _seed_tables_columns(session)
    _overlay(session, "add", "ca", "cb")
    session.flush()

    for _ in range(2):
        materialize_relationship_overlays(
            session, baseline_session_id(), run_id="r1", table_ids=["t1", "t2"]
        )
        session.flush()

    assert len(_materialized(session)) == 1


def test_materialize_skips_endpoint_outside_selection(session: Session) -> None:
    """An overlay referencing a column outside the session's tables is skipped."""
    _seed_tables_columns(session)
    # cx lives on a table NOT in the materialize scope.
    session.add(Table(table_id="t3", source_id="s1", table_name="other", layer="typed"))
    session.add(Column(column_id="cx", table_id="t3", column_name="x", column_position=0))
    _overlay(session, "add", "ca", "cx")
    session.flush()

    count = materialize_relationship_overlays(
        session, baseline_session_id(), run_id="r1", table_ids=["t1", "t2"]
    )
    session.flush()

    assert count == 0
    assert _materialized(session) == []


# --- Silent-accept keeper writer (DAT-409 C3) ------------------------------------


def _seed_prior_promoted_run(session: Session, run_id: str) -> None:
    """A prior begin_session run sealed (head points at it) with one llm relationship."""
    _rel(session, "ca", "cb", "llm", run_id=run_id)
    session.add(
        MetadataSnapshotHead(
            target=session_head_target(baseline_session_id()), stage="detect", run_id=run_id
        )
    )
    session.flush()


def _keep_overlays(session: Session) -> list[dict]:
    return [
        o.payload
        for o in session.query(ConfigOverlay).filter(ConfigOverlay.type == "relationship").all()
        if (o.payload or {}).get("action") == "keep"
    ]


def test_keeper_lifts_unreproduced_prior_llm(session: Session) -> None:
    """A promoted llm the current run didn't reproduce becomes a keep overlay."""
    _seed_tables_columns(session)
    _seed_prior_promoted_run(session, "r0")
    # Current run r1 reproduced nothing for this pair.

    count = write_relationship_keepers(session, baseline_session_id(), current_run_id="r1")
    session.flush()

    assert count == 1
    assert _keep_overlays(session) == [
        {"action": "keep", "from_column_id": "ca", "to_column_id": "cb"}
    ]


def test_keeper_skips_reproduced_pair(session: Session) -> None:
    """A pair the current run DID reproduce is not lifted (it's still live)."""
    _seed_tables_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _rel(session, "ca", "cb", "llm", run_id="r1")  # reproduced this run
    session.flush()

    count = write_relationship_keepers(session, baseline_session_id(), current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_skips_rejected_pair(session: Session) -> None:
    """A user reject suppresses the silent-accept — the drop is honored, not resurrected."""
    _seed_tables_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _overlay(session, "reject", "ca", "cb")
    session.flush()

    count = write_relationship_keepers(session, baseline_session_id(), current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_skips_when_already_kept(session: Session) -> None:
    """An existing keep overlay isn't duplicated on a later unreproduced run."""
    _seed_tables_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    count = write_relationship_keepers(session, baseline_session_id(), current_run_id="r1")
    session.flush()

    assert count == 0
    assert len(_keep_overlays(session)) == 1


def test_keeper_noop_on_first_run(session: Session) -> None:
    """No promoted prior run (no head) → nothing to compare, no keepers."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    session.flush()

    count = write_relationship_keepers(session, baseline_session_id(), current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_noop_when_head_already_names_current_run(session: Session) -> None:
    """Defensive: if the head already names the current run (e.g. a retry after
    promote), there is no prior run to compare against — no keepers."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    session.add(
        MetadataSnapshotHead(
            target=session_head_target(baseline_session_id()), stage="detect", run_id="r1"
        )
    )
    session.flush()

    count = write_relationship_keepers(session, baseline_session_id(), current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []
