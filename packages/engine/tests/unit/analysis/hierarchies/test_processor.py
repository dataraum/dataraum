"""Stack-v4 dimension-identity discovery — DAT-761 (gate stack from DAT-757).

Deterministic FD pass over a fact's grain-verified enriched view. The shared
``seed_sales`` fixture (conftest) encodes a known ``zip → city → state`` chain, two
1:1 aliases, a constant, and a near-key id; the ``TestStackV4`` fixtures encode
the DAT-757 matrix cells the old distinct-ratio g3 could not decide (vacuous
skew, dirty-true edges, role pairs, null-coded columns, measure exclusion).
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dataraum.analysis.hierarchies.db_models import (
    DimensionHierarchy,
    HierarchyMember,
    RoleEvidence,
)
from dataraum.analysis.hierarchies.processor import (
    _break_cycles,
    _maximal_chains,
    _validated_members,
    discover_dimension_hierarchies,
)
from dataraum.analysis.hierarchies.stats import RoleResult, RoleVerdict
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column

from .conftest import RUN, StubIdentityJudge, approving_judge, seed_sales, seed_view


def _rows(session: Session, table_id: str, kind: str) -> list[DimensionHierarchy]:
    return list(
        session.execute(
            select(DimensionHierarchy).where(
                DimensionHierarchy.table_id == table_id,
                DimensionHierarchy.kind == kind,
            )
        ).scalars()
    )


def _members(row: DimensionHierarchy) -> list[str]:
    """Member column names in persisted array order (coarse → fine for a drilldown)."""
    return [m["column_name"] for m in row.members]


def _levels(row: DimensionHierarchy) -> list[int]:
    """The ``level`` of each member, in persisted array order."""
    return [m["level"] for m in row.members]


class TestIdentityJudge:
    """DAT-762: the within-view identity judge on relabeling bijections.

    On the fact-grain view a folded key + its attributes REPEAT, so a code↔name
    alias (``account_id ⇄ account_name``) and a COINCIDENTAL 1:1 (``account_id ⇄
    opened_date``) both pass perm-BH as non-key bijections and reach the judge —
    only meaning separates them. 120 rows (3 accounts × 40) keeps support above
    ``MIN_SUPPORT_ROWS`` so ``needs_confirmation`` reflects the judge's call, not
    thin support.
    """

    @staticmethod
    def _fact_view(
        session: Session,
        duck: duckdb.DuckDBPyConnection,
        *,
        second: str,
        mapping: dict[str, str],
    ) -> str:
        acct = [f"A{i % 3}" for i in range(120)]
        cols = {
            "account_id": acct,
            second: [mapping[a] for a in acct],
            "region": [["north", "south"][i % 2] for i in range(120)],  # inert, ⊥ account
        }
        return seed_view(session, duck, "facts", cols)

    def test_relabeling_alias_merged_with_confidence(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A high identity confidence merges the axes and records the confidence."""
        tid = self._fact_view(
            real_session,
            duck,
            second="account_name",
            mapping={"A0": "Cash", "A1": "Receivable", "A2": "Payable"},
        )
        stub = StubIdentityJudge(confidence=0.95)
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=stub
        )
        assert stub.calls, "the relabeling bijection must reach the judge"
        aliases = {frozenset(_members(r)): r for r in _rows(real_session, tid, "alias")}
        row = aliases[frozenset({"account_id", "account_name"})]
        assert row.needs_confirmation is False  # merged (collapses in the driver tree)
        assert row.identity_confidence == pytest.approx(0.95)

    def test_coincidental_bijection_surfaced_not_merged(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A coincidental 1:1 the judge scores low surfaces needs_confirmation, uncollapsed."""
        tid = self._fact_view(
            real_session,
            duck,
            second="opened_date",
            mapping={"A0": "2020-01-01", "A1": "2020-02-01", "A2": "2020-03-01"},
        )
        stub = StubIdentityJudge(confidence=0.03)
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=stub
        )
        aliases = {frozenset(_members(r)): r for r in _rows(real_session, tid, "alias")}
        row = aliases[frozenset({"account_id", "opened_date"})]
        assert row.needs_confirmation is True  # NOT collapsed (drivers skip these)
        assert row.identity_confidence == pytest.approx(0.03)

    def test_merge_boundary_is_pinned_to_identity_merge_min(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The merge boundary is IDENTITY_MERGE_MIN, regression-locked with HARDCODED
        confidences bracketing it: 0.69 surfaces, 0.70 merges. Every other identity
        test uses 0.95/0.03, so a silent drift of the floor anywhere in (0.03, 0.95)
        — e.g. back to 0.85 or down to 0.5 — would pass them all; this one flips."""
        from dataraum.analysis.hierarchies.processor import IDENTITY_MERGE_MIN

        assert IDENTITY_MERGE_MIN == 0.7  # the validated operating point — change deliberately

        mapping = {"A0": "Cash", "A1": "Receivable", "A2": "Payable"}
        acct = [f"A{i % 3}" for i in range(120)]

        def merged_at(view: str, conf: float) -> bool:
            cols = {
                "account_id": acct,
                "account_name": [mapping[a] for a in acct],
                "region": [["north", "south"][i % 2] for i in range(120)],  # inert, ⊥ account
            }
            tid = seed_view(real_session, duck, view, cols)
            discover_dimension_hierarchies(
                real_session,
                duckdb_conn=duck,
                table_ids=[tid],
                run_id=RUN,
                judge=StubIdentityJudge(confidence=conf),
            )
            row = {frozenset(_members(r)): r for r in _rows(real_session, tid, "alias")}[
                frozenset({"account_id", "account_name"})
            ]
            return row.needs_confirmation is False  # False ⇒ merged (collapses in drivers)

        assert merged_at("facts_below_floor", 0.69) is False  # just below → surfaced
        assert merged_at("facts_at_floor", 0.70) is True  # at the floor → merged

    def test_judge_failure_surfaces_not_merges(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """An unjudged bijection (failed call) is surfaced, never silently merged."""
        tid = self._fact_view(
            real_session,
            duck,
            second="opened_date",
            mapping={"A0": "2020-01-01", "A1": "2020-02-01", "A2": "2020-03-01"},
        )
        stub = StubIdentityJudge(fail=True)
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=stub
        )
        aliases = {frozenset(_members(r)): r for r in _rows(real_session, tid, "alias")}
        row = aliases[frozenset({"account_id", "opened_date"})]
        assert row.needs_confirmation is True
        assert row.identity_confidence is None  # absence of judgment, not a low score

    def test_redelivery_verdict_flip_leaves_no_stale_merged_group(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A Temporal redelivery whose judge verdict FLIPS must not strand a prior
        delivery's merged GROUP row. The alias-group signature depends on the
        (nondeterministic) verdict, so without replace-semantics the delivery-1
        3-member row would survive delivery-2's smaller group and drivers would
        collapse it (needs_confirmation=False) — dropping the declined axis."""
        acct = [f"A{i % 3}" for i in range(120)]
        cols = {
            "account_id": acct,
            "account_code": acct,  # exact copy (rate 0) → merged without the judge
            "account_name": [{"A0": "Cash", "A1": "Receivable", "A2": "Payable"}[a] for a in acct],
        }
        tid = seed_view(real_session, duck, "facts", cols)

        class _Flip:
            def __init__(self) -> None:
                self.n = 0

            def alias_identity(self, *, candidates: list[dict]):  # noqa: ANN202
                from dataraum.analysis.hierarchies.judge import AliasIdentityVerdict
                from dataraum.core.models.base import Result

                self.n += 1
                same = self.n == 1  # merge on delivery 1, decline on delivery 2
                return Result.ok(
                    [
                        AliasIdentityVerdict(
                            pair_ref=c["ref"],
                            confidence=0.95 if same else 0.03,
                            reason="flip",
                        )
                        for c in candidates
                    ]
                )

        judge = _Flip()
        for _ in range(2):  # same run_id — a success-redelivery
            discover_dimension_hierarchies(
                real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=judge
            )
            real_session.commit()

        # After the flip, NO auto-collapsing (needs_confirmation=False) alias may
        # still carry account_name — that stale row is the corruption.
        confirmed = [r for r in _rows(real_session, tid, "alias") if r.needs_confirmation is False]
        assert all("account_name" not in _members(r) for r in confirmed)


class TestDiscoverDimensionHierarchies:
    def test_drilldown_chain_coarsest_to_finest_by_level(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        n = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        assert n > 0
        drills = _rows(real_session, tid, "drilldown")
        assert len(drills) == 1
        row = drills[0]
        # Aliases collapse to canonical (zip < zip_code, state < state_name); the
        # chain is stored coarse → fine with the transitive state → zip edge reduced
        # out. ``level`` carries the direction: 0 = coarsest (DAT-779).
        assert _members(row) == ["state", "city", "zip"]
        assert _levels(row) == [0, 1, 2]
        assert row.canonical_label == "state → city → zip"
        # g3 replaces the overloaded ``score`` (kind-invariant, DAT-784).
        assert row.g3 is not None and row.g3 <= 0.01
        assert row.role_verdict is None and row.role_evidence is None
        assert row.run_id == RUN
        assert row.needs_confirmation is False

    def test_drilldown_member_shape_matches_cockpit_contract(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The persisted ``members`` JSON is exactly the shape the cockpit reader
        (query-context.ts ``CatalogHierarchyRow``) expects — the offline-DDL seam
        contract (DAT-779): each entry is {column_name, column_id, distinct_count,
        level}, and ``level`` (0 = coarsest) is the authoritative order, not array
        position."""
        tid = seed_sales(real_session, duck)
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        row = _rows(real_session, tid, "drilldown")[0]
        for member in row.members:
            assert set(member) == {"column_name", "column_id", "distinct_count", "level"}
        # level is a contiguous 0..n-1 permutation; sorting by it yields coarse → fine.
        assert sorted(m["level"] for m in row.members) == list(range(len(row.members)))
        by_level = [m["column_name"] for m in sorted(row.members, key=lambda m: m["level"])]
        assert by_level == ["state", "city", "zip"]

    def test_one_to_one_aliases_collapse(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        aliases = {tuple(_members(r)): r for r in _rows(real_session, tid, "alias")}
        assert ("zip", "zip_code") in aliases
        assert ("state", "state_name") in aliases
        # Canonical = lexicographically first member.
        assert aliases[("zip", "zip_code")].canonical_label == "zip"
        assert aliases[("state", "state_name")].canonical_label == "state"

    def test_degenerate_and_near_key_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        all_members = {
            m
            for r in _rows(real_session, tid, "drilldown") + _rows(real_session, tid, "alias")
            for m in _members(r)
        }
        # 'country' is a constant (dropped both roles); 'order_id' is unique (near-key,
        # never a determinant) — neither appears in any structure.
        assert "country" not in all_members
        assert "order_id" not in all_members

    def test_low_support_flags_needs_confirmation(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Below MIN_SUPPORT_ROWS (100): the chain is found but flagged, not asserted.
        tid = seed_sales(real_session, duck, rows_per_zip=2)  # 12 rows
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        drills = _rows(real_session, tid, "drilldown")
        assert drills and all(r.needs_confirmation for r in drills)

    def test_rerun_is_idempotent(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Success-redelivery (same run_id) converges by upsert on (signature, run_id)."""
        tid = seed_sales(real_session, duck)
        first = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        real_session.commit()
        second = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        real_session.commit()
        assert first == second
        rows = real_session.execute(select(DimensionHierarchy)).scalars().all()
        assert len(rows) == first

    def test_no_enriched_view_no_rows(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Without a grain-verified enriched view there is no queryable substrate.
        tid = seed_sales(real_session, duck)
        real_session.execute(EnrichedView.__table__.update().values(is_grain_verified=False))
        real_session.flush()
        n = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        assert n == 0


class TestStackV4:
    """The DAT-757 cells the distinct-ratio g3 could not decide (DAT-761)."""

    def test_vacuous_skew_killed_true_skew_edge_kept(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """λ ≥ 0.5 kills the vacuous-skew edge; the exact FD onto a skewed flag survives.

        ``vacuous`` is 99.5%-dominant with its minority spread evenly across
        ``det`` groups: row-g3 = 0.005 passes the effect screen vacuously, but
        λ ≈ 0 (no reduction over the majority baseline). ``flag`` is 98%-dominant
        but an exact FD of ``det`` — λ = 1, the edge must survive (the naive
        "skip skewed dependents" rule would have killed it).
        """
        n = 10_000
        det = [f"d{i % 50}" for i in range(n)]
        vacuous = ["1" if (i // 50) % 200 == 0 else "0" for i in range(n)]
        flag = ["1" if i % 50 == 0 else "0" for i in range(n)]
        tid = seed_view(
            real_session, duck, "skewed", {"det": det, "vacuous": vacuous, "flag": flag}
        )
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        chains = [_members(r) for r in _rows(real_session, tid, "drilldown")]
        # Stored coarse → fine: ``flag`` (2 distinct, coarsest) determines nothing
        # finer than ``det`` (50 distinct) — det → flag reversed for storage.
        assert ["flag", "det"] in chains
        assert all("vacuous" not in c for c in chains)

    def test_dirty_true_edge_kept(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A true hierarchy with 0.2% dirty rows stays asserted (row-g3 tolerance)."""
        n = 5_000
        city = [f"c{i % 40}" for i in range(n)]
        state = [
            f"s{((i % 40) // 5 + 1) % 8}" if i % 500 == 0 else f"s{(i % 40) // 5}" for i in range(n)
        ]
        tid = seed_view(real_session, duck, "dirty_geo", {"city": city, "state": state})
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        drills = _rows(real_session, tid, "drilldown")
        # Coarse → fine: state (coarser) before city (finer).
        assert [_members(r) for r in drills] == [["state", "city"]]
        assert drills[0].g3 is not None and 0.0 < drills[0].g3 <= 0.01

    def test_role_pair_kept_apart(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A role-playing near-copy (bill-to ≠ sold-to on dropship rows) is persisted
        as ``kind='role'`` — never merged as an alias, never stacked as an edge.

        Pairwise, ``soldto``/``billto`` agree on 99.25% of rows and pass the alias
        screen; the disagreement set is fully driven by ``channel`` (T1), which is
        exactly the SAP SALT BILLTO↔PAYER over-merge the round caught and reversed.
        """
        n_cust, rows_per = 800, 10
        soldto, billto, channel = [], [], []
        for r in range(n_cust * rows_per):
            c = r % n_cust
            drop = c < 6
            soldto.append(f"c{c}")
            billto.append("hub" if drop else f"c{c}")
            channel.append("dropship" if drop else "standard")
        tid = seed_view(
            real_session,
            duck,
            "orders",
            {"soldto": soldto, "billto": billto, "channel": channel},
        )
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        roles = _rows(real_session, tid, "role")
        assert [_members(r) for r in roles] == [["billto", "soldto"]]
        assert roles[0].needs_confirmation is False
        # The verdict + evidence are persisted, not lost to a log line (DAT-784):
        # 'role' with T1 (channel) as the discriminating context, no g3 (a role pair
        # has no functional dependency), and the disagreement rate in the evidence.
        row = roles[0]
        assert row.role_verdict == "role"
        assert row.g3 is None
        assert row.role_evidence is not None
        assert row.role_evidence["t1_context"] == "channel"
        assert row.role_evidence["k_disagree"] > 0
        assert 0.0 < row.role_evidence["disagree_rate"] < 0.05
        # Not merged, and no drilldown stacks the two role siblings as levels.
        for r in _rows(real_session, tid, "alias") + _rows(real_session, tid, "drilldown"):
            assert not {"soldto", "billto"} <= set(_members(r))

    def test_partial_null_edge_rescued_by_pairwise_deletion(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The null-policy lane, edge arm: 10% join-miss NULLs in the dependent
        used to cost the whole edge (row-g3 = 0.10 under null-as-category);
        pairwise deletion recovers the exact FD over the complete rows."""
        n = 5_000
        city = [f"c{i % 40}" for i in range(n)]
        state = [None if i % 10 == 0 else f"s{(i % 40) // 5}" for i in range(n)]
        tid = seed_view(real_session, duck, "null_geo", {"city": city, "state": state})
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        drills = _rows(real_session, tid, "drilldown")
        assert [_members(r) for r in drills] == [["state", "city"]]  # coarse → fine

    def test_sub_floor_null_support_surfaces_not_drops(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """An edge whose pairwise-complete support falls below the floor is
        surfaced with ``needs_confirmation`` — never silently dropped (the
        review-finding posture: same as the tiny-view flag)."""
        n = 400
        city = [f"c{i % 20}" for i in range(n)]
        # Nulls in 20-row blocks (coprime with the city cycle): 80 complete rows
        # covering every city, so only SUPPORT is thin — not the FD structure.
        state = [f"s{(i % 20) // 5}" if (i // 20) % 5 == 0 else None for i in range(n)]
        tid = seed_view(real_session, duck, "sparse_geo", {"city": city, "state": state})
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        drills = _rows(real_session, tid, "drilldown")
        assert [_members(r) for r in drills] == [["state", "city"]]  # coarse → fine
        assert drills[0].needs_confirmation is True

    def test_null_coded_columns_alias(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The null-policy lane: ``{1, NULL}`` columns are axes, not constants.

        SQL ``COUNT(DISTINCT)`` sees 1 distinct value and the old pass dropped
        them silently (the rel-hm FN/Active lesson); null-as-category keeps them
        eligible and the exact copy merges as an alias.
        """
        n = 3_000
        fn = ["1" if i % 7 < 2 else None for i in range(n)]
        city = [f"c{i % 30}" for i in range(n)]
        tid = seed_view(
            real_session, duck, "customers", {"fn": fn, "active": list(fn), "city": city}
        )
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        aliases = [_members(r) for r in _rows(real_session, tid, "alias")]
        assert ["active", "fn"] in aliases

    def test_measure_columns_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The additivity lane: a ``semantic_role='measure'`` column never enters
        FD discovery, even when it would form a perfect structure."""
        n = 600
        zips = [f"z{i % 6}" for i in range(n)]
        amount = [str((i % 6) * 100) for i in range(n)]
        city = [f"c{i % 3}" for i in range(n)]
        tid = seed_view(
            real_session, duck, "billing", {"zip": zips, "amount": amount, "city": city}
        )
        amount_id = real_session.execute(
            select(Column.column_id).where(Column.table_id == tid, Column.column_name == "amount")
        ).scalar_one()
        real_session.add(
            SemanticAnnotation(
                annotation_id=str(uuid4()),
                column_id=amount_id,
                run_id=RUN,
                semantic_role="measure",
            )
        )
        real_session.flush()
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        all_members = {
            m
            for kind in ("drilldown", "alias", "role")
            for r in _rows(real_session, tid, kind)
            for m in _members(r)
        }
        assert "amount" not in all_members
        assert "zip" in all_members  # the zip → city structure itself is found

    def test_unregistered_column_participates(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A view column with no catalog ``Column`` row still participates —
        provenance is metadata (``column_id=''``), not a gate (the widened
        candidate universe is the view, not the slice catalog)."""
        n = 500
        state = [f"s{i % 8}" for i in range(n)]
        tid = seed_view(
            real_session,
            duck,
            "wide",
            {"state": state, "mystery": list(state)},
            register={"state"},
        )
        discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN, judge=approving_judge()
        )
        aliases = _rows(real_session, tid, "alias")
        assert [_members(r) for r in aliases] == [["mystery", "state"]]
        by_name = {m["column_name"]: m["column_id"] for m in aliases[0].members}
        assert by_name["mystery"] == "" and by_name["state"] != ""


class TestChainAssemblyGuards:
    """Cycle and scale guards on the decided-DAG walk (DAT-761 review findings)."""

    def test_break_cycles_drops_cyclic_core_keeps_spur(self) -> None:
        # A rep-level 2-cycle is contradictory determination evidence: its
        # internal edges go (loudly), the spur INTO it survives as a chain to
        # a now-sink node — never a hang, never a silent full drop.
        kept = _break_cycles({("s", "a"), ("a", "b"), ("b", "a")})
        assert kept == {("s", "a")}
        assert _maximal_chains(kept) == [["s", "a"]]

    def test_pure_two_cycle_yields_no_chains_loudly(self) -> None:
        assert _break_cycles({("a", "b"), ("b", "a")}) == set()

    def test_maximal_chains_is_iterative_beyond_recursion_depth(self) -> None:
        # The old recursive walk died at ~1000 nodes; the iterative walk must
        # handle a path far deeper than any recursion limit.
        edges = {(f"n{i:04d}", f"n{i + 1:04d}") for i in range(5000)}
        chains = _maximal_chains(edges)
        assert len(chains) == 1
        assert len(chains[0]) == 5001


class TestDimensionHierarchiesPhaseSkip:
    """The phase's should_skip preconditions (deterministic re-run discipline)."""

    def _ctx(self, session: Session, duck: duckdb.DuckDBPyConnection, table_ids: list[str]):
        from dataraum.pipeline.base import PhaseContext

        return PhaseContext(
            session=session,
            duckdb_conn=duck,
            table_ids=table_ids,
            run_id=RUN,
            config={},
        )

    def test_skips_when_no_enriched_view(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        from dataraum.pipeline.phases.dimension_hierarchies_phase import DimensionHierarchiesPhase

        # A table id with no grain-verified enriched view → no substrate.
        reason = DimensionHierarchiesPhase().should_skip(self._ctx(real_session, duck, ["nope"]))
        assert reason is not None and "enriched views" in reason

    def test_does_not_skip_with_catalog(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        from dataraum.pipeline.phases.dimension_hierarchies_phase import DimensionHierarchiesPhase

        tid = seed_sales(real_session, duck)
        assert DimensionHierarchiesPhase().should_skip(self._ctx(real_session, duck, [tid])) is None


def _member_json(name: str, level: int) -> dict[str, object]:
    return {"column_name": name, "column_id": "", "distinct_count": None, "level": level}


def _bare_row(**overrides: object) -> DimensionHierarchy:
    """A minimal well-formed row; overrides poke one field for a rejection test."""
    fields: dict[str, object] = {
        "run_id": RUN,
        "table_id": "t1",
        "kind": "alias",
        "members": [_member_json("a", 0), _member_json("b", 1)],
        "canonical_label": "a",
        "signature": f"alias:t1:{uuid4()}",
    }
    fields.update(overrides)
    return DimensionHierarchy(**fields)


class TestPersistenceContract:
    """The verdict/evidence persistence contract (DAT-784) and the member-level
    JSON contract (DAT-779) — the two-layer standard: a DB CheckConstraint on the
    closed-vocabulary column, and strict Pydantic submodels on the JSON interiors."""

    def test_role_verdict_check_rejects_unknown(self, real_session: Session) -> None:
        """An out-of-vocabulary ``role_verdict`` is rejected at the DB layer
        (the DAT-781 two-layer standard flush-rejection test)."""
        real_session.add(_bare_row(role_verdict="bogus"))
        with pytest.raises(IntegrityError):
            real_session.flush()

    def test_value_systematic_and_abstain_are_distinguishable(self, real_session: Session) -> None:
        """The exact DAT-784 bug: the two undecidable verdicts used to collapse to
        one bare ``needs_confirmation`` alias. Now the column tells them apart, and
        the evidence survives."""
        vs = RoleEvidence(
            t1_p=0.3, t1_context="ctx", t2_p=0.01, k_disagree=40, alpha=0.01, disagree_rate=0.02
        ).model_dump()
        ab = RoleEvidence(
            t1_p=1.0, t1_context=None, t2_p=1.0, k_disagree=3, alpha=0.01, disagree_rate=0.003
        ).model_dump()
        real_session.add(
            _bare_row(role_verdict="value_systematic", role_evidence=vs, needs_confirmation=True)
        )
        real_session.add(
            _bare_row(role_verdict="abstain", role_evidence=ab, needs_confirmation=True)
        )
        real_session.flush()
        real_session.expire_all()
        verdicts = {
            r.role_verdict: r
            for r in real_session.execute(select(DimensionHierarchy)).scalars().all()
        }
        assert set(verdicts) == {"value_systematic", "abstain"}
        assert verdicts["value_systematic"].role_evidence["k_disagree"] == 40
        assert verdicts["abstain"].role_evidence["t1_context"] is None

    def test_hierarchy_member_submodel_rejects_bad_shape(self) -> None:
        # extra key forbidden, negative level rejected, level required.
        with pytest.raises(ValidationError):
            HierarchyMember(column_name="a", column_id="", distinct_count=1, level=0, extra="x")  # type: ignore[call-arg]
        with pytest.raises(ValidationError):
            HierarchyMember(column_name="a", column_id="", distinct_count=1, level=-1)
        with pytest.raises(ValidationError):
            HierarchyMember(column_name="a", column_id="", distinct_count=1)  # type: ignore[call-arg]

    def test_role_evidence_submodel_rejects_bad_shape(self) -> None:
        with pytest.raises(ValidationError):
            RoleEvidence(  # type: ignore[call-arg]
                t1_p=0.1, t1_context=None, t2_p=0.1, k_disagree=1, alpha=0.01
            )  # missing disagree_rate
        with pytest.raises(ValidationError):
            RoleEvidence(
                t1_p=0.1,
                t1_context=None,
                t2_p=0.1,
                k_disagree=1,
                alpha=0.01,
                disagree_rate=0.1,
                extra="x",  # type: ignore[call-arg]
            )

    def test_validated_members_requires_contiguous_levels(self) -> None:
        # A gap in the level set is a mis-numbered writer → loud failure (DAT-779).
        good = [
            HierarchyMember(column_name="a", column_id="", distinct_count=None, level=0),
            HierarchyMember(column_name="b", column_id="", distinct_count=None, level=1),
        ]
        assert [m["column_name"] for m in _validated_members(good)] == ["a", "b"]
        gapped = [
            HierarchyMember(column_name="a", column_id="", distinct_count=None, level=0),
            HierarchyMember(column_name="b", column_id="", distinct_count=None, level=2),
        ]
        with pytest.raises(ValueError, match="contiguous"):
            _validated_members(gapped)

    def test_role_row_helper_requires_disagree_rate(self) -> None:
        """``_hierarchy_row`` refuses to persist a verdict without its rate — the
        evidence must never be half-formed."""
        from dataraum.analysis.hierarchies.processor import _hierarchy_row

        result = RoleResult(
            verdict=RoleVerdict.ROLE,
            t1_p=0.001,
            t1_context="ctx",
            t2_p=0.5,
            k_disagree=20,
            alpha=0.01,
        )
        with pytest.raises(ValueError, match="disagree_rate"):
            _hierarchy_row(
                run_id=RUN,
                table_id="t1",
                kind="role",
                members=[_member_json("a", 0)],
                canonical_label="a ⇄ b",
                signature="role:t1:a|b",
                role=result,
                detection_source="g3",
                needs_confirmation=False,
            )
