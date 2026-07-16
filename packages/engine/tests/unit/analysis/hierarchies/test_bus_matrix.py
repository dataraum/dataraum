"""Bus-matrix derivation (DAT-762 Part 2) — scripted conform judge.

Pins the two legs and the posture rules: referenced cells are structural with
role multiplicity and a weakest-link provenance floor; folded cells come from
the stats groups with cross-fact identity decided (or abstained) by the
conform judge; undecided and role structures never become cells; a failed
conform call keeps the per-fact cells and is observable.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.hierarchies.bus_matrix import derive_bus_matrix
from dataraum.analysis.hierarchies.db_models import BusMatrixEntry, DimensionHierarchy
from dataraum.analysis.hierarchies.judge import ConformVerdict, DimensionIdentityJudge
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.storage import Column

from .conftest import RUN, seed_view


def _conform_judge(verdicts: list[ConformVerdict] | None = None) -> MagicMock:
    judge = MagicMock(spec=DimensionIdentityJudge)
    judge.conform.return_value = Result.ok(verdicts or [])
    return judge


def _seed_structure(
    session: Session,
    table_id: str,
    members: list[tuple[str, str, int | None]],  # (column_name, column_id, distinct)
    *,
    kind: str = "drilldown",
    needs_confirmation: bool = False,
    detection_source: str = "g3",
) -> None:
    session.add(
        DimensionHierarchy(
            run_id=RUN,
            table_id=table_id,
            kind=kind,
            members=[
                {"column_name": n, "column_id": cid, "distinct_count": d, "level": i}
                for i, (n, cid, d) in enumerate(members)
            ],
            canonical_label=" > ".join(m[0] for m in members),
            signature=f"{kind}:{table_id}:" + "|".join(sorted(m[0] for m in members)),
            g3=0.0,
            detection_source=detection_source,
            needs_confirmation=needs_confirmation,
        )
    )
    session.flush()


def _col_ids(session: Session, table_id: str) -> dict[str, str]:
    return {
        c.column_name: c.column_id
        for c in session.execute(select(Column).where(Column.table_id == table_id)).scalars()
    }


def _cells(session: Session) -> list[BusMatrixEntry]:
    return list(
        session.execute(select(BusMatrixEntry).order_by(BusMatrixEntry.signature)).scalars()
    )


def _seed_fold_fact(
    session: Session,
    duck: duckdb.DuckDBPyConnection,
    view: str,
    *,
    key: str,
    attr: str,
) -> str:
    """A fact whose {key -> attr} folded group is already discovered (seeded)."""
    tid = seed_view(
        session,
        duck,
        view,
        {
            key: [f"A{i % 6}" for i in range(30)],
            attr: [f"name-{i % 6}" for i in range(30)],
        },
    )
    ids = _col_ids(session, tid)
    _seed_structure(session, tid, [(key, ids[key], 6), (attr, ids[attr], 6)], kind="alias")
    return tid


class TestReferencedLeg:
    def test_role_multiplicity_and_provenance_floor(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_view(
            real_session,
            duck,
            "fact_a",
            {"billto_id": ["x", "y"], "payto_id": ["x", "y"]},
        )
        dim_tid = seed_view(real_session, duck, "dim_party", {"party_id": ["x", "y"]})
        ids = _col_ids(real_session, tid)
        rel_confirmed = Relationship(
            run_id=RUN,
            from_table_id=tid,
            from_column_id=ids["billto_id"],
            to_table_id=dim_tid,
            to_column_id=_col_ids(real_session, dim_tid)["party_id"],
            relationship_type="foreign_key",
            confidence=1.0,
            confirmation_source="user",
        )
        rel_unconfirmed = Relationship(
            run_id=RUN,
            from_table_id=tid,
            from_column_id=ids["payto_id"],
            to_table_id=dim_tid,
            to_column_id=_col_ids(real_session, dim_tid)["party_id"],
            relationship_type="foreign_key",
            confidence=0.9,
            confirmation_source="unconfirmed",
        )
        real_session.add_all([rel_confirmed, rel_unconfirmed])
        real_session.flush()
        ev = real_session.execute(
            select(EnrichedView).where(EnrichedView.fact_table_id == tid)
        ).scalar_one()
        ev.relationship_ids = [rel_confirmed.relationship_id, rel_unconfirmed.relationship_id]
        for role, col in (("billto_id", "billto_id"), ("payto_id", "payto_id")):
            real_session.add(
                SliceDefinition(
                    run_id=RUN,
                    table_id=tid,
                    column_id=ids[col],
                    column_name=col,
                    dimension_table_id=dim_tid,
                    dimension_attribute=None,
                    fk_role=role,
                    slice_priority=1,
                    slice_type="categorical",
                    detection_source="llm",
                )
            )
        real_session.flush()

        n, stats = derive_bus_matrix(
            real_session,
            table_ids=[tid, dim_tid],
            run_id=RUN,
            judge=_conform_judge(),
        )

        referenced = [c for c in _cells(real_session) if c.attachment == "referenced"]
        assert stats.referenced == len(referenced) == 1
        cell = referenced[0]
        assert cell.fact_table_id == tid
        assert cell.dimension_table_id == dim_tid
        assert cell.concept_label == "dim_party"
        assert cell.roles == ["billto_id", "payto_id"]  # one cell, both roles
        # weakest-link floor: user + unconfirmed -> unconfirmed
        assert cell.confirmation_source == "unconfirmed"


class TestFoldedLeg:
    def test_conform_shares_concept_and_judge_provenance(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        t1 = _seed_fold_fact(real_session, duck, "gl", key="account_id", attr="account_name")
        t2 = _seed_fold_fact(real_session, duck, "tb", key="account_id", attr="account_name")
        judge = _conform_judge(
            [
                ConformVerdict(
                    pair_ref="pair:0:1",
                    verdict="conform",
                    concept_label="account",
                    reason="same key and attributes",
                )
            ]
        )

        derive_bus_matrix(real_session, table_ids=[t1, t2], run_id=RUN, judge=judge)

        folded = [c for c in _cells(real_session) if c.attachment == "folded"]
        assert {c.fact_table_id for c in folded} == {t1, t2}
        assert all(c.concept_label == "account" for c in folded)
        assert all(c.confirmation_source == "judge" for c in folded)
        assert all(c.roles == ["account_id"] for c in folded)
        assert all(c.attributes == ["account_name"] for c in folded)
        assert all(c.needs_confirmation is False for c in folded)
        # ONE conformed_group across the pair — the DAT-800 group key.
        groups = {c.conformed_group for c in folded}
        assert len(groups) == 1 and None not in groups

    def test_label_drift_within_one_component_stays_one_group(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Chained conforms with drifting labels: ONE group, first label wins.

        Keying on the label would split {f1,f2,f3} into {f1,f2} + {f3} and the
        singleton would drop out of every >=2-facts consumer — a conform
        verdict silently discarded.
        """
        t1 = _seed_fold_fact(real_session, duck, "f1", key="account_id", attr="account_name")
        t2 = _seed_fold_fact(real_session, duck, "f2", key="account_id", attr="account_name")
        t3 = _seed_fold_fact(real_session, duck, "f3", key="account_id", attr="account_name")
        judge = _conform_judge(
            [
                ConformVerdict(
                    pair_ref="pair:0:1", verdict="conform", concept_label="account", reason="same"
                ),
                ConformVerdict(
                    pair_ref="pair:0:2", verdict="conform", concept_label="client", reason="same"
                ),
            ]
        )

        derive_bus_matrix(real_session, table_ids=[t1, t2, t3], run_id=RUN, judge=judge)

        folded = [c for c in _cells(real_session) if c.attachment == "folded"]
        assert {c.fact_table_id for c in folded} == {t1, t2, t3}
        assert len({c.conformed_group for c in folded}) == 1
        assert {c.concept_label for c in folded} == {"account"}  # first verdict names the group

    def test_same_label_on_distinct_components_stays_two_groups(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A generic label shared by two DISTINCT-judged groups never merges them.

        Keying on the label would join all four facts on "status" — exactly
        the false merge the conform prompt warns about, reintroduced by the
        consumer; the group key keeps the judge's DISTINCT verdict standing.
        """
        t1 = _seed_fold_fact(real_session, duck, "f1", key="status", attr="status_name")
        t2 = _seed_fold_fact(real_session, duck, "f2", key="status", attr="status_name")
        t3 = _seed_fold_fact(real_session, duck, "f3", key="state", attr="state_name")
        t4 = _seed_fold_fact(real_session, duck, "f4", key="state", attr="state_name")
        distinct = [
            ConformVerdict(pair_ref=ref, verdict="distinct", concept_label=None, reason="other")
            for ref in ("pair:0:2", "pair:0:3", "pair:1:2", "pair:1:3")
        ]
        judge = _conform_judge(
            [
                ConformVerdict(
                    pair_ref="pair:0:1", verdict="conform", concept_label="status", reason="same"
                ),
                ConformVerdict(
                    pair_ref="pair:2:3", verdict="conform", concept_label="status", reason="same"
                ),
                *distinct,
            ]
        )

        derive_bus_matrix(real_session, table_ids=[t1, t2, t3, t4], run_id=RUN, judge=judge)

        folded = {c.fact_table_id: c for c in _cells(real_session) if c.attachment == "folded"}
        assert all(c.concept_label == "status" for c in folded.values())
        assert folded[t1].conformed_group == folded[t2].conformed_group
        assert folded[t3].conformed_group == folded[t4].conformed_group
        assert folded[t1].conformed_group != folded[t3].conformed_group

    def test_unanswered_pairs_are_counted(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        t1 = _seed_fold_fact(real_session, duck, "f1", key="account_id", attr="account_name")
        t2 = _seed_fold_fact(real_session, duck, "f2", key="account_id", attr="account_name")
        t3 = _seed_fold_fact(real_session, duck, "f3", key="account_id", attr="account_name")
        judge = _conform_judge(
            [
                ConformVerdict(
                    pair_ref="pair:0:1", verdict="distinct", concept_label=None, reason="other"
                )
            ]
        )

        _, stats = derive_bus_matrix(real_session, table_ids=[t1, t2, t3], run_id=RUN, judge=judge)

        assert stats.conform_pairs == 3
        assert stats.unanswered == 2  # silence is visible, never uphold-by-omission

    def test_conform_candidates_are_chunked(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """9 facts -> 36 cross-fact pairs -> two judge calls (32 + 4)."""
        tids = [
            _seed_fold_fact(real_session, duck, f"f{i}", key="account_id", attr="account_name")
            for i in range(9)
        ]
        judge = _conform_judge()

        _, stats = derive_bus_matrix(real_session, table_ids=tids, run_id=RUN, judge=judge)

        assert stats.conform_pairs == 36
        assert judge.conform.call_count == 2
        sizes = [len(c.kwargs["candidates"]) for c in judge.conform.call_args_list]
        assert sizes == [32, 4]

    def test_user_taught_fold_conformed_by_judge_keeps_user_provenance_and_group(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """STRUCTURE provenance (user) is orthogonal to cross-fact IDENTITY.

        The cell keeps the teach's 'user' source, but the judge-asserted
        ``conformed_group`` still lands — so a user-taught fold participates
        in the DAT-800 shared-dims pool (which filters on the group, never on
        provenance).
        """
        tids = []
        for view in ("gl", "tb"):
            tid = seed_view(real_session, duck, view, {"k": ["1", "2", "3"], "v": ["x", "y", "z"]})
            ids = _col_ids(real_session, tid)
            _seed_structure(
                real_session,
                tid,
                [("k", ids["k"], 3), ("v", ids["v"], 3)],
                kind="alias",
                detection_source="manual",
            )
            tids.append(tid)
        judge = _conform_judge(
            [
                ConformVerdict(
                    pair_ref="pair:0:1", verdict="conform", concept_label="thing", reason="same"
                )
            ]
        )

        derive_bus_matrix(real_session, table_ids=tids, run_id=RUN, judge=judge)

        folded = [c for c in _cells(real_session) if c.attachment == "folded"]
        assert all(c.confirmation_source == "user" for c in folded)
        assert len({c.conformed_group for c in folded}) == 1
        assert all(c.conformed_group is not None for c in folded)

    def test_abstain_surfaces_per_fact_cells(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        t1 = _seed_fold_fact(real_session, duck, "gl", key="account_id", attr="account_name")
        t2 = _seed_fold_fact(real_session, duck, "tb", key="acct_code", attr="acct_label")
        judge = _conform_judge(
            [
                ConformVerdict(
                    pair_ref="pair:0:1",
                    verdict="abstain",
                    concept_label=None,
                    reason="insufficient evidence",
                )
            ]
        )

        _, stats = derive_bus_matrix(real_session, table_ids=[t1, t2], run_id=RUN, judge=judge)

        folded = [c for c in _cells(real_session) if c.attachment == "folded"]
        assert len(folded) == 2  # both cells exist — abstain never erases the fold
        assert all(c.needs_confirmation is True for c in folded)
        assert all(c.confirmation_source == "unconfirmed" for c in folded)
        assert {c.concept_label for c in folded} == {"account_id", "acct_code"}  # own labels
        assert stats.abstained == 1

    def test_undecided_and_role_structures_never_become_cells(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_view(
            real_session,
            duck,
            "gl",
            {"a": ["1", "2"], "b": ["x", "y"], "c": ["p", "q"], "d": ["m", "n"]},
        )
        ids = _col_ids(real_session, tid)
        _seed_structure(
            real_session,
            tid,
            [("a", ids["a"], 2), ("b", ids["b"], 2)],
            kind="alias",
            needs_confirmation=True,  # the stats surfaced it undecided
        )
        _seed_structure(real_session, tid, [("c", ids["c"], 2), ("d", ids["d"], 2)], kind="role")

        _, stats = derive_bus_matrix(
            real_session,
            table_ids=[tid],
            run_id=RUN,
            judge=_conform_judge(),
        )

        assert stats.folded == 0
        assert [c for c in _cells(real_session) if c.attachment == "folded"] == []

    def test_manual_teach_is_user_provenance(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_view(real_session, duck, "gl", {"k": ["1", "2"], "v": ["x", "y"]})
        ids = _col_ids(real_session, tid)
        _seed_structure(
            real_session,
            tid,
            [("k", ids["k"], 2), ("v", ids["v"], 2)],
            kind="alias",
            detection_source="manual",
        )

        derive_bus_matrix(
            real_session,
            table_ids=[tid],
            run_id=RUN,
            judge=_conform_judge(),
        )

        (cell,) = [c for c in _cells(real_session) if c.attachment == "folded"]
        assert cell.confirmation_source == "user"

    def test_conform_failure_keeps_cells_and_is_observable(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        t1 = _seed_fold_fact(real_session, duck, "gl", key="account_id", attr="account_name")
        t2 = _seed_fold_fact(real_session, duck, "tb", key="account_id", attr="account_name")
        judge = MagicMock(spec=DimensionIdentityJudge)
        judge.conform.return_value = Result.fail("api down")

        _, stats = derive_bus_matrix(real_session, table_ids=[t1, t2], run_id=RUN, judge=judge)

        assert stats.status == "failed"
        folded = [c for c in _cells(real_session) if c.attachment == "folded"]
        assert len(folded) == 2  # stats-derived cells survive an unjudged run
        assert all(c.confirmation_source == "unconfirmed" for c in folded)

    def test_single_fact_makes_no_llm_call(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_fold_fact(real_session, duck, "mega", key="account_id", attr="account_name")
        judge = _conform_judge()

        _, stats = derive_bus_matrix(real_session, table_ids=[tid], run_id=RUN, judge=judge)

        judge.conform.assert_not_called()
        assert stats.conform_pairs == 0
        assert stats.folded == 1  # the single-fact folded cell still exists


def test_rerun_converges(real_session: Session, duck: duckdb.DuckDBPyConnection) -> None:
    tid = _seed_fold_fact(real_session, duck, "gl", key="account_id", attr="account_name")
    for _ in range(2):
        derive_bus_matrix(
            real_session,
            table_ids=[tid],
            run_id=RUN,
            judge=_conform_judge(),
        )
        real_session.commit()
    rows = real_session.execute(select(BusMatrixEntry)).scalars().all()
    assert len(rows) == len({r.signature for r in rows})  # form-(a): one row per signature+run


def test_retry_with_changed_structure_leaves_no_stale_cells(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    """A folded cell's signature carries its component's member set.

    A crash-after-commit redelivery re-runs derive with the SAME run_id. If the
    run's structures changed in between — a teach landing, or a structure the
    stats now surface as undecided — the fold component's member_key, hence the
    cell signature, changes, and an upsert alone would strand attempt 1's cell
    under the promoted run. Delete-then-insert must leave only attempt 2's view.
    """
    tid = _seed_fold_fact(real_session, duck, "gl", key="account_id", attr="account_name")
    derive_bus_matrix(real_session, table_ids=[tid], run_id=RUN, judge=_conform_judge())
    real_session.commit()
    assert [c for c in _cells(real_session) if c.attachment == "folded"] != []

    # Attempt 2 sees the structure surfaced undecided — no fold group.
    structure = real_session.execute(select(DimensionHierarchy)).scalar_one()
    structure.needs_confirmation = True
    real_session.flush()
    derive_bus_matrix(real_session, table_ids=[tid], run_id=RUN, judge=_conform_judge())
    real_session.commit()

    assert [c for c in _cells(real_session) if c.attachment == "folded"] == []
