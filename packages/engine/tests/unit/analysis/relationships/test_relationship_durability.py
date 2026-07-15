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
from dataraum.storage.snapshot_head import MetadataSnapshotHead, catalog_head_target


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
    _store_candidates(session, ["t1", "t2"], [candidate], run_id="run-A")
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
    _store_candidates(session, ["t1", "t2"], [candidate])
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


def test_load_defined_relationships_has_no_confidence_gate(session: Session) -> None:
    """A defined row is judge-verified or user-asserted — a numeric floor here
    would pre-empt downstream judges with an uncalibrated number (DAT-699).
    Low-confidence rows are served as evidence, never filtered."""
    _seed_tables_columns(session)
    session.add(
        Relationship(
            run_id="run-A",
            from_table_id="t1",
            from_column_id="ca",
            to_table_id="t2",
            to_column_id="cb",
            relationship_type="foreign_key",
            confidence=0.3,
            detection_method="llm",
        )
    )
    session.flush()

    defined = load_defined_relationships(session, ["t1", "t2"], run_id="run-A")
    assert [r.confidence for r in defined] == [0.3]


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
    # Undirected (DAT-777): a reject identifies the edge, not a direction.
    assert load_suppressed_relationship_pairs(session) == {frozenset({"ca", "cb"})}


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
            target=target,
            table_id=None,
            column_id=None,
            run_id=run_id,
            band="investigate",
            worst_intent_risk=0.5,
        )
    )


def _seal(session: Session, run_id: str) -> None:
    """Seal the workspace at ``run_id`` via the catalog head (DAT-506)."""
    session.add(MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id=run_id))


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

    out = load_relationship_readiness(session)
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

    assert load_relationship_readiness(session) == []


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

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 1
    rows = _materialized(session)
    assert len(rows) == 1
    row = rows[0]
    assert row.detection_method == "manual"
    assert row.run_id == "r1"
    assert (row.from_table_id, row.to_table_id) == ("t1", "t2")
    # An `add` teach is an explicit human assertion (DAT-776).
    assert row.confirmation_source == "user"


def test_materialize_keep_overlay_creates_keeper_with_last_measured_evidence(
    session: Session,
) -> None:
    """A `keep` overlay materializes as `keeper` CARRYING its last measurement
    (DAT-699): the newest llm row's confidence/cardinality/evidence, stamped
    not_remeasured — never a fabricated confidence=1.0 with no cardinality."""
    _seed_tables_columns(session)
    session.add(
        Relationship(
            run_id="r0",
            from_table_id="t1",
            from_column_id="ca",
            to_table_id="t2",
            to_column_id="cb",
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.85,
            detection_method="llm",
            evidence={"coverage": 0.92},
        )
    )
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    rows = _materialized(session)
    assert [r.detection_method for r in rows] == ["keeper"]
    keeper = rows[0]
    assert keeper.confidence == 0.85
    assert keeper.cardinality == "many-to-one"
    assert keeper.evidence == {
        "coverage": 0.92,
        "source": "config_overlay",
        "action": "keep",
        "measured_run_id": "r0",
        "not_remeasured": True,
    }


def test_keep_overlay_without_measurement_is_skipped_loud(session: Session) -> None:
    """A keeper is lifted FROM an llm row; with no measurement recoverable it
    has no basis — any stamped confidence would be fabricated. Loud skip."""
    _seed_tables_columns(session)
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 0
    assert _materialized(session) == []


def test_materialize_joins_a_pair_already_llm_this_run(session: Session) -> None:
    """Human-witness rows COEXIST with the llm row (DAT-447 — the teach circuit).

    The old skip-any-defined-pair rule made manual/keeper structurally
    impossible on exactly the pairs the system asks the user to confirm. Dedup
    is per (pair, method): the keeper row joins the llm row; the adjudication
    reads them side by side.
    """
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 1
    pairs = session.query(Relationship).filter(Relationship.run_id == "r1").all()
    assert sorted(p.detection_method for p in pairs) == ["keeper", "llm"]


def test_confirm_overlay_materializes_a_manual_row_beside_llm(session: Session) -> None:
    """An explicit confirm becomes the manual_curation witness on the confirmed pair.

    Before DAT-447 a confirm materialized NOTHING — the human verdict the
    system itself asked for vanished instead of becoming a witness.
    """
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    _overlay(session, "confirm", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 1
    pairs = session.query(Relationship).filter(Relationship.run_id == "r1").all()
    by_method = {p.detection_method: p for p in pairs}
    assert set(by_method) == {"llm", "manual"}
    manual = by_method["manual"]
    # The user's assertion keeps confidence 1.0 (their voice, not a fabrication)
    # and copies the pair's last measurement alongside the overlay stamp.
    assert manual.confidence == 1.0
    assert manual.evidence["source"] == "config_overlay"
    assert manual.evidence["action"] == "confirm"
    assert manual.evidence["not_remeasured"] is True
    assert manual.evidence["measured_run_id"] == "r1"


def test_add_and_confirm_overlays_never_duplicate_the_manual_row(session: Session) -> None:
    """Both map to manual; the per-(pair, method) dedup keeps exactly one row."""
    _seed_tables_columns(session)
    _overlay(session, "add", "ca", "cb")
    _overlay(session, "confirm", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 1
    pairs = session.query(Relationship).filter(Relationship.run_id == "r1").all()
    assert [p.detection_method for p in pairs] == ["manual"]
    assert pairs[0].evidence["action"] == "add"  # listed first, wins the dedup


def test_materialize_skips_a_rejected_pair(session: Session) -> None:
    """A reject overlay wins over a stale add/keep for the same pair."""
    _seed_tables_columns(session)
    _overlay(session, "add", "ca", "cb")
    _overlay(session, "reject", "ca", "cb")
    session.flush()

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 0
    assert _materialized(session) == []


def test_materialize_is_idempotent_per_run(session: Session) -> None:
    """Re-running the same run_id clears its own durable rows first — no accumulation."""
    _seed_tables_columns(session)
    _overlay(session, "add", "ca", "cb")
    session.flush()

    for _ in range(2):
        materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
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

    count = materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    assert count == 0
    assert _materialized(session) == []


# --- Silent-accept keeper writer (DAT-409 C3) ------------------------------------


def _seed_prior_promoted_run(session: Session, run_id: str) -> None:
    """A prior begin_session run sealed (head points at it) with one llm relationship."""
    _rel(session, "ca", "cb", "llm", run_id=run_id)
    session.add(MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id=run_id))
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

    count = write_relationship_keepers(session, current_run_id="r1")
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

    count = write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_skips_rejected_pair(session: Session) -> None:
    """A user reject suppresses the silent-accept — the drop is honored, not resurrected."""
    _seed_tables_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _overlay(session, "reject", "ca", "cb")
    session.flush()

    count = write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_skips_when_already_kept(session: Session) -> None:
    """An existing keep overlay isn't duplicated on a later unreproduced run."""
    _seed_tables_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _overlay(session, "keep", "ca", "cb")
    session.flush()

    count = write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    assert count == 0
    assert len(_keep_overlays(session)) == 1


def test_keeper_noop_on_first_run(session: Session) -> None:
    """No promoted prior run (no head) → nothing to compare, no keepers."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    session.flush()

    count = write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_noop_when_head_already_names_current_run(session: Session) -> None:
    """Defensive: if the head already names the current run (e.g. a retry after
    promote), there is no prior run to compare against — no keepers."""
    _seed_tables_columns(session)
    _rel(session, "ca", "cb", "llm", run_id="r1")
    session.add(MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id="r1"))
    session.flush()

    count = write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


# --- Adjudication outranks silence (DAT-697) --------------------------------------
#
# The judge's per-run composite verdicts (SurrogateKeyIntent.status) gate the
# silent-accept machinery: a declined composite's pairs are not lifted, its
# stale keep overlays are superseded, and a keep overlay on an adjudicated
# pair does not materialize a keeper row. Pairs the run did NOT rule on keep
# full DAT-409 flake protection.


def _seed_more_columns(session: Session) -> None:
    session.add(Column(column_id="cx", table_id="t1", column_name="business_id", column_position=1))
    session.add(Column(column_id="cy", table_id="t2", column_name="business_id", column_position=1))
    session.add(Column(column_id="sk1", table_id="t1", column_name="_sk__x", column_position=2))
    session.add(Column(column_id="sk2", table_id="t2", column_name="_sk__x", column_position=2))
    session.flush()


def _intent(session: Session, pairs: list[list[str]], status: str, run_id: str = "r1") -> None:
    from dataraum.analysis.relationships.db_models import SurrogateKeyIntent
    from dataraum.analysis.relationships.surrogate import composite_intent_digest

    session.add(
        SurrogateKeyIntent(
            run_id=run_id,
            intent_digest=composite_intent_digest(pairs),
            status=status,
            from_table_id="t1",
            to_table_id="t2",
            column_pairs=pairs,
            confidence=0.0 if status == "declined" else 0.9,
        )
    )
    session.flush()


def _surrogate_rel(
    session: Session, run_id: str, natural_ids: list[list[str]], frm: str = "sk1", to: str = "sk2"
) -> None:
    session.add(
        Relationship(
            run_id=run_id,
            from_table_id="t1",
            from_column_id=frm,
            to_table_id="t2",
            to_column_id=to,
            relationship_type="foreign_key",
            confidence=0.9,
            detection_method="llm",
            evidence={"surrogate": {"natural_column_ids": natural_ids}},
        )
    )
    session.flush()


def test_keeper_skips_declined_component_pair(session: Session) -> None:
    """A prior llm pair the judge declined this run (as a composite component,
    anchor included) is a verdict, not silence — no lift."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _seed_prior_promoted_run(session, "r0")  # prior llm on (ca, cb)
    _intent(session, [["ca", "cb"], ["cx", "cy"]], "declined")

    count = write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    assert count == 0
    assert _keep_overlays(session) == []


def test_keeper_skips_declined_pair_whichever_direction(session: Session) -> None:
    """Decline matching is direction-neutral — the verdict covers (b, a) too."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _intent(session, [["cb", "ca"], ["cy", "cx"]], "declined")  # reversed orientation

    assert write_relationship_keepers(session, current_run_id="r1") == 0
    assert _keep_overlays(session) == []


def test_keeper_still_lifts_unadjudicated_pair(session: Session) -> None:
    """A verdict on one composite must not erode flake protection elsewhere."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _seed_prior_promoted_run(session, "r0")  # prior llm on (ca, cb)
    _intent(session, [["cx", "cy"], ["sk1", "sk2"]], "declined")  # unrelated pairs

    assert write_relationship_keepers(session, current_run_id="r1") == 1
    assert _keep_overlays(session) == [
        {"action": "keep", "from_column_id": "ca", "to_column_id": "cb"}
    ]


def test_keeper_skips_surrogate_row_of_adjudicated_composite(session: Session) -> None:
    """A surrogate pair matches by digest recomputed from its mint provenance —
    a CONFIRMED verdict supersedes a stale differently-anchored twin of itself."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _surrogate_rel(session, "r0", natural_ids=[["ca", "cb"], ["cx", "cy"]])
    session.add(MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id="r0"))
    session.flush()
    # This run confirms the same composite — provenance arrives reordered and
    # direction-flipped; identity must hold regardless.
    _intent(session, [["cy", "cx"], ["cb", "ca"]], "confirmed")

    assert write_relationship_keepers(session, current_run_id="r1") == 0
    assert _keep_overlays(session) == []


def test_keeper_retracts_stale_keep_on_adjudicated_pair(session: Session) -> None:
    """An existing keep overlay on a judged pair is superseded — without this the
    overlay re-materializes a keeper row every run and the declined composite
    (and its hollow surrogate columns) is resurrected indefinitely."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _seed_prior_promoted_run(session, "r0")
    _overlay(session, "keep", "ca", "cb")
    _intent(session, [["ca", "cb"], ["cx", "cy"]], "declined")
    session.flush()

    write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    kept = [
        o
        for o in session.query(ConfigOverlay).filter(ConfigOverlay.type == "relationship").all()
        if (o.payload or {}).get("action") == "keep"
    ]
    assert len(kept) == 1 and kept[0].superseded_at is not None
    # …and the retracted overlay is invisible to the layered readers.
    from dataraum.analysis.relationships.utils import relationship_overlay_pairs

    assert relationship_overlay_pairs(session, "keep") == []


def test_keeper_retraction_recovers_surrogate_provenance(session: Session) -> None:
    """A keep overlay on a surrogate pair carries no evidence — provenance comes
    from the newest llm row on the same pair, then digest-matches the verdict."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _surrogate_rel(session, "r0", natural_ids=[["ca", "cb"], ["cx", "cy"]])
    session.add(MetadataSnapshotHead(target=catalog_head_target(), stage="catalog", run_id="r0"))
    _overlay(session, "keep", "sk1", "sk2")
    _intent(session, [["ca", "cb"], ["cx", "cy"]], "declined")
    session.flush()

    write_relationship_keepers(session, current_run_id="r1")
    session.flush()

    kept = [
        o
        for o in session.query(ConfigOverlay).filter(ConfigOverlay.type == "relationship").all()
        if (o.payload or {}).get("action") == "keep"
    ]
    assert len(kept) == 1 and kept[0].superseded_at is not None


def test_materialize_skips_keep_on_adjudicated_pair_but_honors_manual(session: Session) -> None:
    """A stale keep on a judged pair must not materialize a keeper row this run —
    but a USER assertion (add/confirm → manual) on the same pair always wins."""
    _seed_tables_columns(session)
    _seed_more_columns(session)
    _overlay(session, "keep", "ca", "cb")
    _overlay(session, "confirm", "ca", "cb")
    _intent(session, [["ca", "cb"], ["cx", "cy"]], "declined")
    session.flush()

    materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    methods = [r.detection_method for r in _materialized(session)]
    assert methods == ["manual"]  # keeper suppressed by the verdict; manual untouched


# --- FK orientation enforced on every write path (DAT-777) ------------------------


def test_detector_candidate_persists_oriented_many_to_one(session: Session) -> None:
    """Write path 1: a detector candidate measured one-to-many is stored many→one
    child→parent — the same convention the llm + overlay paths use."""
    from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate

    _seed_tables_columns(session)  # t1.ca = orders.customer_id, t2.cb = customers.id
    # Measured parent(customers.id) → child(orders.customer_id) as one-to-many.
    candidate = RelationshipCandidate(
        table1="customers",
        table2="orders",
        join_candidates=[
            JoinCandidate(
                column1="id",
                column2="customer_id",
                join_confidence=0.8,
                cardinality="one-to-many",
                left_uniqueness=1.0,
                right_uniqueness=0.2,
            )
        ],
    )
    _store_candidates(session, ["t1", "t2"], [candidate], run_id="run-A")
    session.flush()

    row = session.query(Relationship).filter_by(detection_method="candidate").one()
    # Flipped to child(orders.customer_id) → parent(customers.id), many-to-one.
    assert (row.from_column_id, row.to_column_id) == ("ca", "cb")
    assert row.cardinality == "many-to-one"
    assert row.confirmation_source == "unconfirmed"
    # Directional evidence followed the swap (uniqueness is left/right too).
    assert row.evidence["left_uniqueness"] == 0.2
    assert row.evidence["right_uniqueness"] == 1.0


def test_materialize_reversed_manual_overlay_adopts_canonical_orientation(
    session: Session,
) -> None:
    """Write path 3 + watch-item: a user teach naming the pair REVERSED still
    materializes on the canonical oriented pair — it adopts the measured llm
    row's orientation, so the durable row coexists with the llm row it confirms
    instead of orphaning as a different-looking relationship."""
    _seed_tables_columns(session)
    session.add(
        Relationship(
            run_id="r1",
            from_table_id="t1",
            from_column_id="ca",  # canonical: orders.customer_id → customers.id
            to_table_id="t2",
            to_column_id="cb",
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.9,
            detection_method="llm",
            confirmation_source="judge",
        )
    )
    _overlay(session, "add", "cb", "ca")  # taught REVERSED (parent → child)
    session.flush()

    materialize_relationship_overlays(session, run_id="r1", table_ids=["t1", "t2"])
    session.flush()

    manual = session.query(Relationship).filter_by(detection_method="manual").one()
    assert (manual.from_column_id, manual.to_column_id) == ("ca", "cb")  # canonical, not (cb, ca)
    assert manual.cardinality == "many-to-one"
    assert manual.confirmation_source == "user"


def test_reject_overlay_suppresses_undirected(session: Session) -> None:
    """Watch-item: a reject taught in the opposite orientation to the row still
    suppresses it — orientation-canonicalization must not orphan the teach."""
    from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate

    _seed_tables_columns(session)
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "reject", "from_column_id": "cb", "to_column_id": "ca"},
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
    _store_candidates(session, ["t1", "t2"], [candidate])
    session.flush()

    assert session.query(Relationship).filter_by(detection_method="candidate").all() == []
