"""Materialize durable relationship overlays into a begin_session run (DAT-409).

The relationship catalog of a begin_session run is built in layers: ``relationships``
derives ephemeral ``candidate`` rows, ``semantic_per_table`` confirms a subset as
``llm``, and THIS step materializes the user's durable teaches —
``ConfigOverlay(type='relationship')`` with ``action`` ``add`` or ``confirm``
(→ ``manual``, the explicit human assertion) or ``keep`` (→ ``keeper``, the
silent-accept method, DAT-409 C3) — as run-stamped ``Relationship`` rows so they
re-appear in every run without ever mutating derived metadata.

Runs AFTER ``semantic_per_table`` (so the ``llm`` set exists) and before
``session_detect``. Dedup is per (pair, METHOD), not per pair (DAT-447): the
relationship_discovery adjudication reads the per-method rows side by side, so a
human verdict must be able to coexist with the ``llm`` row it confirms — the old
skip-any-defined-pair rule made the ``manual_curation`` witness structurally
impossible on exactly the pairs the system asks the user to confirm, breaking the
teach circuit (system asks for the verdict → the verdict becomes a witness → the
witness's reliability gets measured). One row per (pair, method) is still
guaranteed (the ``uq_relationship_columns_method`` key); the catalog enumeration
de-duplicates pairs, and join-path counting collapses methods by column pair.
Rejected pairs are skipped (a reject wins over a stale add/confirm/keep).
Re-running the same ``run_id`` (a Temporal retry) clears only this run's own
``manual``/``keeper`` rows first, so it is idempotent and non-destructive to
other runs.

Adjudication outranks silence (DAT-697): silent-accept exists for pairs the
run did NOT rule on (an LLM flake must not erase catalog state the user
relied on), but ``semantic_per_table`` records a VERDICT for every composite
whose rescue hint it offered (``SurrogateKeyIntent.status``), and a verdict
is not silence. A ``keep`` overlay on an adjudicated pair is neither
materialized nor lifted — it is superseded — so a judge-declined composite
(and its hollow surrogate columns, via the mint's keeper-grace window)
cannot be resurrected run after run by machinery meant to guard against
flakes. User assertions are untouched: ``manual`` overlays never yield to
the judge, and ``keep`` has exactly one author (this module — the cockpit
teach surface rejects it as a user action).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship, SurrogateKeyIntent
from dataraum.analysis.relationships.evaluator import (
    compute_actual_cardinality,
    compute_introduces_duplicates,
)
from dataraum.analysis.relationships.surrogate import composite_intent_digest
from dataraum.analysis.relationships.utils import (
    load_suppressed_relationship_pairs,
    relationship_overlay_pairs,
    relationship_overlay_rows,
)
from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.core.logging import get_logger
from dataraum.server.storage import LAKE_CATALOG_ALIAS
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)

# action → the durable detection_method it materializes as. ``add`` and
# ``confirm`` are both explicit human assertions (manual); ``add`` is listed
# first so it wins the per-(pair, method) dedup when both overlays exist.
_ACTION_METHOD = {"add": "manual", "confirm": "manual", "keep": "keeper"}


@dataclass(frozen=True)
class _Adjudication:
    """One run's composite verdicts, in the shapes the keeper machinery matches on.

    ``digests``: direction-neutral identities of EVERY composite the judge ruled
    on this run (confirmed or declined) — matched against a surrogate
    relationship's mint provenance, so a confirmed composite also supersedes a
    stale differently-named/anchored surrogate twin of itself.
    ``declined_pairs``: the unordered natural component pairs of DECLINED
    composites — a declined verdict covers the composite's own column pairs
    (anchor included: the judge was told to decline the relationship entirely).
    Component pairs of CONFIRMED composites are deliberately absent: a second,
    independent relationship between the same tables (two real FKs) must keep
    its flake protection.
    """

    digests: frozenset[str]
    declined_pairs: frozenset[frozenset[str]]

    @property
    def empty(self) -> bool:
        return not self.digests and not self.declined_pairs


def _load_adjudication(session: Session, run_id: str) -> _Adjudication:
    """This run's composite verdicts (both statuses), from the intent rows."""
    intents = session.execute(
        select(SurrogateKeyIntent).where(SurrogateKeyIntent.run_id == run_id)
    ).scalars()
    digests: set[str] = set()
    declined_pairs: set[frozenset[str]] = set()
    for intent in intents:
        digests.add(composite_intent_digest(intent.column_pairs))
        if intent.status == "declined":
            declined_pairs.update(frozenset(pair) for pair in intent.column_pairs)
    return _Adjudication(frozenset(digests), frozenset(declined_pairs))


def _natural_id_pairs(evidence: dict[str, Any] | None) -> list[Any] | None:
    """The mint's natural component-id pairs from a relationship's evidence."""
    surrogate = (evidence or {}).get("surrogate")
    ids = surrogate.get("natural_column_ids") if isinstance(surrogate, dict) else None
    return ids if isinstance(ids, list) and len(ids) >= 2 else None


def _row_adjudicated(
    adjudication: _Adjudication, pair: tuple[str, str], evidence: dict[str, Any] | None
) -> bool:
    """Whether this run RULED on the composite behind a relationship row.

    Declined verdicts match the natural component pairs directly; surrogate
    rows (whose pair is the minted ``_sk__`` columns, not the components)
    match via the digest recomputed from their mint provenance — recomputed,
    never string-compared, so digest-format changes cannot strand old rows.
    """
    if frozenset(pair) in adjudication.declined_pairs:
        return True
    natural = _natural_id_pairs(evidence)
    return natural is not None and composite_intent_digest(natural) in adjudication.digests


def _pair_adjudicated(session: Session, adjudication: _Adjudication, pair: tuple[str, str]) -> bool:
    """`_row_adjudicated` for a bare pair (a keep overlay carries no evidence).

    Surrogate provenance is recovered from the newest ``llm`` row on the same
    pair (the original mint — mirrors the mint phase's own fallback for
    overlay-materialized rows). The llm lookup is UNDIRECTED (DAT-777): the llm
    row is stored canonically many→one while the keep overlay names the pair as
    it was taught, so a directional match would miss its own measurement.
    """
    if frozenset(pair) in adjudication.declined_pairs:
        return True
    if not adjudication.digests:
        return False
    evidences = session.execute(
        select(Relationship.evidence)
        .where(
            _pair_matches_undirected(pair),
            Relationship.detection_method == "llm",
        )
        .order_by(Relationship.detected_at.desc())
    ).scalars()
    for evidence in evidences:
        natural = _natural_id_pairs(evidence)
        if natural is not None:
            return composite_intent_digest(natural) in adjudication.digests
    return False


def materialize_relationship_overlays(
    session: Session,
    *,
    run_id: str,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Write this run's durable (`manual`/`keeper`) relationships from overlays.

    Returns the number of rows materialized. Idempotent per ``run_id``.

    Materialized rows carry their LAST MEASURED evidence (DAT-699): the newest
    llm row on the same pair supplies cardinality, evidence and — for keepers —
    confidence, stamped ``not_remeasured`` so no consumer mistakes a copied
    measurement for a fresh one. A keeper is silence over a prior measurement;
    materializing it at a fabricated ``confidence=1.0`` with no cardinality
    laundered "not re-measured this run" into full certainty (it also sailed
    over the late confidence gate this fix's sibling removed). A keeper whose
    measurement is unrecoverable has no basis and is skipped LOUD. ``manual``
    rows keep ``confidence=1.0`` — that one is the user's own assertion, not a
    fabrication — but copy the measured evidence too when it exists.

    A manual teach on a pair with NO prior row at all is measured HERE
    (DAT-790): ``duckdb_conn`` probes the typed tables for empirical
    cardinality + ``introduces_duplicates``, so the cockpit's fan-out caution
    is live exactly where the join is least verified. ``None`` (or a failed
    probe) leaves both NULL — an honest "unmeasured", never a fabrication.
    """
    # Retry-safe clear: drop only THIS run's prior durable rows (run_id-scoped, like
    # the candidate clear), never another run's. candidate/llm are owned by their
    # own steps and untouched.
    session.execute(
        delete(Relationship).where(
            Relationship.run_id == run_id,
            Relationship.detection_method.in_(tuple(_ACTION_METHOD.values())),
        )
    )

    suppressed = load_suppressed_relationship_pairs(session)
    adjudication = _load_adjudication(session, run_id)

    # The run's DEFINED rows so far, keyed (UNDIRECTED pair, method) — candidates
    # are ephemeral and excluded. Dedup is per METHOD: an overlay row may join a
    # pair the llm already confirmed (that coexistence IS the witness pool), but
    # never duplicate a row of its own method. Undirected (DAT-777): the llm row is
    # stored canonically while the overlay names the pair as taught, so the key must
    # ignore orientation or the same edge materializes twice under two names.
    defined_stmt = select(
        Relationship.from_column_id,
        Relationship.to_column_id,
        Relationship.detection_method,
    ).where(
        Relationship.run_id == run_id,
        Relationship.detection_method != "candidate",
    )
    written: set[tuple[frozenset[str], str]] = {
        (frozenset((f, t)), str(m)) for f, t, m in session.execute(defined_stmt).tuples()
    }

    # Resolve column → owning table for every column an overlay references, bounded
    # to the session's tables — an overlay pointing outside the selection is skipped.
    table_by_column = _column_table_map(session, table_ids)

    count = 0
    for action, method in _ACTION_METHOD.items():
        for from_col, to_col in relationship_overlay_pairs(session, action):
            pair = (from_col, to_col)
            if frozenset(pair) in suppressed or (frozenset(pair), method) in written:
                continue
            if action == "keep" and _pair_adjudicated(session, adjudication, pair):
                # This run RULED on the pair's composite (DAT-697) — a verdict
                # is not silence, so the stale keep does not materialize. The
                # overlay itself is superseded at the end of the run by
                # ``write_relationship_keepers``.
                continue
            from_table = table_by_column.get(from_col)
            to_table = table_by_column.get(to_col)
            if from_table is None or to_table is None:
                # Endpoint outside the session's tables (or unknown) — nothing to
                # anchor the row to; skip rather than write a dangling relationship.
                continue
            measured = _last_measured_row(session, pair)
            if method == "keeper" and measured is None:
                # A keeper is lifted FROM an llm row; with no measurement
                # recoverable it has no basis, and any confidence we stamped
                # would be fabricated. Loud skip — the overlay stays for a
                # future run where the measurement may reappear.
                logger.warning(
                    "keeper_without_measurement_skipped",
                    from_column=from_col,
                    to_column=to_col,
                )
                continue
            # A manual teach on a pair the detector already found (a ``candidate``
            # row, oriented by detector.py) but the llm never confirmed adopts that
            # row's canonical orientation + measurement too (DAT-777 watch-item):
            # the durable row must coexist with the detected row on the SAME oriented
            # pair, and a reversed teach must not slip past as a disconnected phantom
            # (its NULL cardinality would sail over the orientation CHECK). keeper
            # never reaches this fallback — it requires an llm measurement, skipped
            # above.
            reference = measured if measured is not None else _last_candidate_row(session, pair)
            evidence: dict[str, Any] = {"source": "config_overlay", "action": action}
            confidence = 1.0
            if reference is not None:
                # Adopt the detected row's CANONICAL orientation (DAT-777): the overlay
                # may name the pair either way, but the durable row must coexist with
                # the row it confirms on the SAME oriented pair, or the two read as
                # different relationships. Carry its last measurement (DAT-699) —
                # stamped ``not_remeasured`` so no consumer mistakes a copied
                # measurement for a fresh one.
                from_table_r = reference.from_table_id
                from_col_r = reference.from_column_id
                to_table_r = reference.to_table_id
                to_col_r = reference.to_column_id
                cardinality = reference.cardinality
                evidence = {
                    **(reference.evidence or {}),
                    **evidence,
                    "measured_run_id": reference.run_id,
                    "not_remeasured": True,
                }
                if method == "keeper":
                    confidence = reference.confidence
            else:
                # A manual ``add`` of a relationship the system never detected at all:
                # no row to canonicalize against, so trust the teach's own from =
                # FK-side orientation (teach.validation). Measure the join HERE
                # (DAT-790): a never-detected pair skipped every measuring writer
                # (processor synthesis, surrogate mint), so it used to persist
                # cardinality=None with no introduces_duplicates — the cockpit's
                # fan-out caution silently absent exactly where the join is least
                # verified. A measured one-to-many is re-oriented by
                # ``oriented_row`` below (DAT-777), same as every other writer.
                from_table_r, from_col_r = from_table, from_col
                to_table_r, to_col_r = to_table, to_col
                cardinality, measured_evidence = _measure_taught_pair(
                    session,
                    duckdb_conn,
                    from_table_id=from_table,
                    from_column_id=from_col,
                    to_table_id=to_table,
                    to_column_id=to_col,
                )
                evidence = {**measured_evidence, **evidence}
            session.add(
                Relationship(
                    **Relationship.oriented_row(
                        run_id=run_id,
                        from_table_id=from_table_r,
                        from_column_id=from_col_r,
                        to_table_id=to_table_r,
                        to_column_id=to_col_r,
                        relationship_type="foreign_key",
                        cardinality=cardinality,
                        confidence=confidence,
                        detection_method=method,
                        # ``manual`` is an explicit human assertion (user); ``keeper``
                        # is silent-accept retention of a prior judge row (DAT-776).
                        confirmation_source="user" if method == "manual" else "keeper",
                        evidence=evidence,
                    )
                )
            )
            written.add((frozenset(pair), method))
            count += 1

    logger.info(
        "relationship_overlays_materialized",
        run_id=run_id,
        count=count,
    )
    return count


def _pair_matches_undirected(pair: tuple[str, str]) -> Any:
    """A WHERE clause matching a relationship on the pair in EITHER orientation.

    The overlay names the pair as it was taught while the llm row is stored
    canonically many→one (DAT-777), so an overlay-vs-row lookup must ignore
    orientation or it misses its own measurement.
    """
    return or_(
        and_(
            Relationship.from_column_id == pair[0],
            Relationship.to_column_id == pair[1],
        ),
        and_(
            Relationship.from_column_id == pair[1],
            Relationship.to_column_id == pair[0],
        ),
    )


def _last_measured_row(session: Session, pair: tuple[str, str]) -> Relationship | None:
    """The newest ``llm`` row on a pair — the last time this edge was MEASURED.

    Undirected (DAT-777): the overlay names the pair as taught while the llm row
    is stored canonically, so match either orientation. Overlay materialization
    copies its cardinality/evidence (and, for keepers, confidence) — and adopts its
    canonical orientation — so a durable row never asserts more than was measured.
    """
    return (
        session.execute(
            select(Relationship)
            .where(
                _pair_matches_undirected(pair),
                Relationship.detection_method == "llm",
            )
            .order_by(Relationship.detected_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _last_candidate_row(session: Session, pair: tuple[str, str]) -> Relationship | None:
    """The newest ``candidate`` row on a pair — the detector's canonical detection.

    The fallback reference for a MANUAL teach on a pair the detector found but the
    llm never confirmed (DAT-777): candidates are stored oriented (detector.py →
    ``oriented_row``), so a durable ``manual`` row adopts that orientation instead
    of trusting the overlay's taught direction. Undirected match, like the llm
    lookup. keeper never uses this — it requires an llm measurement.
    """
    return (
        session.execute(
            select(Relationship)
            .where(
                _pair_matches_undirected(pair),
                Relationship.detection_method == "candidate",
            )
            .order_by(Relationship.detected_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _column_table_map(session: Session, table_ids: list[str]) -> dict[str, str]:
    """``column_id -> table_id`` for every column in ``table_ids``."""
    # `.tuples().all()` materializes the rows — `dict()` on a bare Result takes the
    # mapping path (Result has .keys()) and raises "not subscriptable".
    rows = session.execute(
        select(Column.column_id, Column.table_id).where(Column.table_id.in_(table_ids))
    ).tuples()
    return dict(rows.all())


def _measure_taught_pair(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
    *,
    from_table_id: str,
    from_column_id: str,
    to_table_id: str,
    to_column_id: str,
) -> tuple[str | None, dict[str, Any]]:
    """Empirically measure a never-detected taught pair's join (DAT-790).

    Probes the two typed tables in the lake for the join's actual cardinality
    (:func:`compute_actual_cardinality`) and fan-trap signal
    (:func:`compute_introduces_duplicates`) — the same measurements the LLM
    synthesis path and the surrogate mint make, so a manual teach carries the
    same empirical evidence as every other defined relationship.

    Returns ``(cardinality, evidence_fields)``. Degrades honestly — a loud log
    and an absent field, never a fabricated value: ``(None, {})`` when no
    connection is supplied or a typed table/column cannot be resolved to a
    physical name; each probe that fails simply leaves its own field unset
    (a failed fan-out probe does not discard an already-measured cardinality
    — mirrors the surrogate mint's per-probe guards).
    """
    if duckdb_conn is None:
        logger.warning(
            "manual_add_unmeasured_no_duckdb",
            from_column=from_column_id,
            to_column=to_column_id,
        )
        return None, {}

    paths = {
        t.table_id: t.duckdb_path
        for t in session.execute(
            select(Table).where(Table.table_id.in_([from_table_id, to_table_id]))
        ).scalars()
    }
    names = dict(
        session.execute(
            select(Column.column_id, Column.column_name).where(
                Column.column_id.in_([from_column_id, to_column_id])
            )
        )
        .tuples()
        .all()
    )
    from_path, to_path = paths.get(from_table_id), paths.get(to_table_id)
    from_name, to_name = names.get(from_column_id), names.get(to_column_id)
    if not from_path or not to_path or not from_name or not to_name:
        logger.warning(
            "manual_add_unmeasured_unresolvable",
            from_column=from_column_id,
            to_column=to_column_id,
        )
        return None, {}

    typed_schema = schema_for_layer("typed")
    from_fqn = f'{LAKE_CATALOG_ALIAS}.{typed_schema}."{from_path}"'
    to_fqn = f'{LAKE_CATALOG_ALIAS}.{typed_schema}."{to_path}"'
    # ``compute_actual_cardinality`` self-guards (logs + returns None on a failed
    # probe); ``compute_introduces_duplicates`` does not — guard it ALONE, so a
    # failed fan-out probe degrades to (cardinality, {}) instead of discarding a
    # cardinality that already measured (mirrors the surrogate mint's guards).
    cardinality = compute_actual_cardinality(from_fqn, to_fqn, from_name, to_name, duckdb_conn)
    measured: dict[str, Any] = {}
    try:
        measured["introduces_duplicates"] = compute_introduces_duplicates(
            from_fqn, to_fqn, from_name, to_name, duckdb_conn
        )
    except Exception as e:  # measurement is evidence, never a materialize blocker
        logger.warning(
            "manual_add_measurement_failed",
            from_column=from_column_id,
            to_column=to_column_id,
            error=str(e),
        )
    if cardinality is not None:
        measured["cardinality_verified"] = True
    return cardinality, measured


def write_relationship_keepers(
    session: Session,
    *,
    current_run_id: str,
) -> int:
    """Lift silently-accepted relationships into ``keep`` overlays (DAT-409 C3).

    The silent-accept rule: an ``llm`` relationship the **promoted prior run** found,
    that the **current run did not reproduce** and the user did **not reject**, was
    accepted by the user's silence. Make that explicit — write a
    ``ConfigOverlay(action='keep', …)`` so the next run materializes it as a durable
    ``keeper`` (the invisible "silence = acceptance" becomes an auditable record).

    Runs as a pre-promote step: the workspace catalog head still points at the
    *prior* promoted run (``session_promote_to_latest`` flips it after this), which
    is the "promoted prior run" the rule compares against. Returns the number of
    keep overlays written. First run (no promoted prior) → no-op.

    The lifted relationship is absent from the run that detected its absence; the
    ``keep`` overlay materializes it as ``keeper`` from the NEXT run onward (the
    accepted one-run gap, by spec).

    Adjudication outranks silence (DAT-697): a prior pair whose composite this
    run's judge RULED on — declined on evidence, or confirmed under its
    canonical surrogate identity (superseding a stale differently-named twin)
    — is not lifted, and any existing keep overlay on such a pair is
    superseded, so an already-polluted workspace self-heals instead of
    resurrecting the pair every run.
    """
    from dataraum.storage import ConfigOverlay
    from dataraum.storage.snapshot_head import catalog_head_target, head_run_id

    prior_run = head_run_id(session, catalog_head_target(), "catalog")
    if prior_run is None or prior_run == current_run_id:
        return 0

    adjudication = _load_adjudication(session, current_run_id)
    retracted = _retract_adjudicated_keeps(session, adjudication)

    prior_llm = list(
        session.execute(
            select(
                Relationship.from_column_id, Relationship.to_column_id, Relationship.evidence
            ).where(
                Relationship.run_id == prior_run,
                Relationship.detection_method == "llm",
            )
        ).tuples()
    )
    reproduced = _run_pairs(session, current_run_id)
    # Reject + keep overlays are matched UNDIRECTED (DAT-777): they name the pair as
    # taught/lifted while the prior_llm rows are stored canonically, so a directional
    # test would silently fail to skip a rejected or already-kept edge.
    rejected = load_suppressed_relationship_pairs(session)
    already_kept = {frozenset(p) for p in relationship_overlay_pairs(session, "keep")}

    count = 0
    for from_col, to_col, evidence in prior_llm:
        pair = (from_col, to_col)
        if pair in reproduced or frozenset(pair) in rejected or frozenset(pair) in already_kept:
            continue
        if _row_adjudicated(adjudication, pair, evidence):
            continue  # the judge ruled this run — a verdict is not silence
        session.add(
            ConfigOverlay(
                type="relationship",
                payload={
                    "action": "keep",
                    "from_column_id": from_col,
                    "to_column_id": to_col,
                },
            )
        )
        already_kept.add(frozenset(pair))
        count += 1

    logger.info(
        "relationship_keepers_written",
        prior_run=prior_run,
        current_run=current_run_id,
        count=count,
        retracted=retracted,
    )
    return count


def _retract_adjudicated_keeps(session: Session, adjudication: _Adjudication) -> int:
    """Supersede active ``keep`` overlays whose pair this run's judge ruled on.

    Without this, a keep overlay written before the verdict re-materializes a
    ``keeper`` row every subsequent run and the mint's grace window keeps the
    declined composite's hollow ``_sk__`` columns alive indefinitely.
    Supersede (the overlay system's own undo marker), never delete — the
    record that the pair was silently kept until a verdict landed stays
    auditable. Safe by construction: ``keep`` overlays are machine-authored
    only (the cockpit teach validator rejects the action), so no user
    assertion can be retracted here.
    """
    if adjudication.empty:
        return 0
    retracted = 0
    # One provenance query per keep overlay (`_pair_adjudicated`) — fine at
    # keeper volumes (a handful per workspace); revisit if that ever grows.
    for overlay in relationship_overlay_rows(session, "keep"):
        pair = (overlay.payload["from_column_id"], overlay.payload["to_column_id"])
        if _pair_adjudicated(session, adjudication, pair):
            overlay.superseded_at = datetime.now(UTC)
            retracted += 1
    if retracted:
        session.flush()
    return retracted


def _run_pairs(
    session: Session,
    run_id: str,
    *,
    method: str | None = None,
) -> set[tuple[str, str]]:
    """Directional ``(from_col, to_col)`` pairs of a run's catalog.

    ``method`` filters to one detection method; ``None`` = the whole defined catalog
    (``!= candidate``) — i.e. what the run actually reproduced.
    """
    stmt = select(Relationship.from_column_id, Relationship.to_column_id).where(
        Relationship.run_id == run_id,
    )
    stmt = stmt.where(
        Relationship.detection_method == method
        if method is not None
        else Relationship.detection_method != "candidate"
    )
    return set(session.execute(stmt).tuples())
