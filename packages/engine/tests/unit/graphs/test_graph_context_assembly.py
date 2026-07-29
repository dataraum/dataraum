"""Unit tests for the graph-read assembly fold (DAT-734).

``_assemble_concept_contexts`` is the pure fold from traversal rows to
``ConceptContext`` — exercised here with fake rows so every loud-absence branch
(dropped edge, empty uses, missing relation, unresolved concept) is pinned
without Postgres. The live PGQ reads are covered by
``tests/integration/graphs/test_graph_context.py``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.reconciliation_db_models import (
    ConceptReconciliation,
    ReconciliationStatus,
    ReconciliationVerdict,
)
from dataraum.graphs.context_reads import (
    _assemble_concept_contexts,
    _read_reconciliation_rows,
)


def _row(**kw: Any) -> SimpleNamespace:
    return SimpleNamespace(**kw)


def _grounding_row(**overrides: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "concept_name": "revenue",
        "snippet_id": "sn_1",
        "relation": "enriched_sales",
        "select_expr": 'SUM("amount")',
        "where_predicates": "[\"x IN ('a')\"]",
        "statement": "income_statement",
        "aggregation": "sum",
        "description": "d",
        "failed": False,
        # og_grounding vertex properties (DAT-671 R6): the failure keys ride the
        # grounded_by MATCH, not a second read of the provenance JSON.
        "failure_mode": None,
        "failure_reason": None,
    }
    base.update(overrides)
    return _row(**base)


_TABLES = {"t1": ("enriched_sales", "enriched")}


def test_groundings_fold_with_uses_and_where() -> None:
    uses = [
        _row(snippet_id="sn_1", role="filter", column_name="x", table_id="t1"),
        _row(snippet_id="sn_1", role="measure", column_name="amount", table_id="t1"),
    ]
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, [_grounding_row()], uses, {}, _TABLES, {}, []
    )
    assert len(out) == 1
    g = out[0].groundings[0]
    assert g.where == ["x IN ('a')"]
    # deterministic role-first ordering (filter < measure alphabetically)
    assert [(u.role, u.column_name) for u in g.uses] == [("filter", "x"), ("measure", "amount")]


def test_healthy_grounding_without_relation_skipped_loud() -> None:
    rows = [_grounding_row(relation=None, select_expr=None, where_predicates=None)]
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, rows, [], {}, _TABLES, {}, []
    )
    assert out[0].groundings == []


def test_failed_grounding_served_with_failure_keys() -> None:
    """The failure keys are ``og_grounding`` vertex PROPERTIES — they arrive on the
    grounded_by MATCH row itself, no companion provenance read."""
    rows = [
        _grounding_row(
            failed=True,
            relation=None,
            snippet_id="sn_f",
            failure_mode="execution_failed",
            failure_reason="boom",
        )
    ]
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, rows, [], {"sn_f": "revenue"}, _TABLES, {}, []
    )
    g = out[0].groundings[0]
    assert g.failed is True
    assert g.failure_mode == "execution_failed"
    assert g.failure_reason == "boom"
    assert g.uses == []


def test_uses_with_unresolvable_table_endpoint_dropped_not_crashed() -> None:
    uses = [_row(snippet_id="sn_1", role="measure", column_name="amount", table_id="t_gone")]
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, [_grounding_row()], uses, {}, _TABLES, {}, []
    )
    assert out[0].groundings[0].uses == []


def test_concept_edge_buckets_and_ordering() -> None:
    edges = [
        _row(from_name="ap", predicate="part_of", tolerance=None, to_name="wc"),
        _row(from_name="ap", predicate="disjoint_with", tolerance=None, to_name="ar"),
        _row(from_name="ar", predicate="disjoint_with", tolerance=None, to_name="ap"),
        _row(from_name="ap", predicate="reconciles_with", tolerance=0.01, to_name="ap"),
    ]
    out = _assemble_concept_contexts(
        [("ap", "measure"), ("ar", "measure"), ("wc", "measure")],
        edges,
        {"ap": ["fin_position"]},
        [],
        [],
        {},
        _TABLES,
        {},
        [],
    )
    ap = next(c for c in out if c.name == "ap")
    wc = next(c for c in out if c.name == "wc")
    assert ap.part_of_parents == ["wc"]
    assert wc.part_of_children == ["ap"]
    assert ap.part_of_ancestry == ["fin_position"]
    assert ap.disjoint_with == ["ar"]
    assert len(ap.reconciles_with) == 1
    assert ap.reconciles_with[0].partner == "ap"
    assert ap.reconciles_with[0].tolerance == 0.01


def test_where_predicates_non_list_json_degrades_loud_not_crash() -> None:
    """``json.loads`` SUCCEEDS on the JSON literal ``null`` (→ Python None) and on
    bare scalars — parse-time exceptions alone don't cover them. The fold must
    serve the grounding with an empty where + a warning, never iterate None
    (reviewer critical: that crash escaped the loader guard and killed the whole
    context build)."""
    for bad in ("null", '"a string"', "42"):
        rows = [_grounding_row(where_predicates=bad)]
        out = _assemble_concept_contexts(
            [("revenue", "measure")], [], {}, rows, [], {}, _TABLES, {}, []
        )
        g = out[0].groundings[0]
        assert g.where == []
        assert g.select_expr == 'SUM("amount")'  # grounding itself still served


def test_unresolved_concept_vertex_dropped_not_crashed() -> None:
    """An ``og_grounding`` vertex whose concept names no active Concept has no
    grounded_by MATCH row — it must drop loud (log) and never surface, and the
    fold must not crash on it."""
    vertices = {"sn_1": "revenue", "sn_orphan": "expenses"}
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, [_grounding_row()], [], vertices, _TABLES, {}, []
    )
    served = {g.snippet_id for c in out for g in c.groundings}
    assert "sn_orphan" not in served
    assert served == {"sn_1"}


def test_concept_order_is_input_order_and_multi_grounding_sorted() -> None:
    rows = [
        _grounding_row(snippet_id="sn_b", statement="balance_sheet"),
        _grounding_row(snippet_id="sn_a", statement="trial_balance"),
        _grounding_row(snippet_id="sn_x", statement="cash_flow", failed=True),
    ]
    out = _assemble_concept_contexts([("revenue", None)], [], {}, rows, [], {}, _TABLES, {}, [])
    ids = [g.snippet_id for g in out[0].groundings]
    # healthy first (failed sorts last), then (relation, snippet_id)
    assert ids == ["sn_a", "sn_b", "sn_x"]


def test_evaluated_tie_out_rides_the_assertion() -> None:
    """The served assertion carries what the last promoted run observed (DAT-739)."""
    edges = [_row(from_name="ap", predicate="reconciles_with", tolerance=None, to_name="ap")]
    observed = {
        ("ap", "ap"): {
            "status": "evaluated",
            "verdict": "no_tolerance_declared",
            "abstain_reason": None,
            "delta": -920000.0,
            "relative_delta": 0.294,
            "pairs": 1,
            "evaluated_pairs": 1,
        }
    }
    out = _assemble_concept_contexts(
        [("ap", "measure")], edges, {}, [], [], {}, _TABLES, observed, []
    )

    (rec,) = out[0].reconciles_with
    assert rec.status == "evaluated"
    assert rec.verdict == "no_tolerance_declared"
    assert rec.observed_delta == -920000.0
    assert rec.evaluated_pairs == 1


def test_an_unevaluated_assertion_carries_no_observation() -> None:
    """Absent evidence stays absent — never folded into an implied agreement."""
    edges = [_row(from_name="ap", predicate="reconciles_with", tolerance=None, to_name="ap")]
    out = _assemble_concept_contexts([("ap", "measure")], edges, {}, [], [], {}, _TABLES, {}, [])

    (rec,) = out[0].reconciles_with
    assert rec.status is None
    assert rec.observed_delta is None
    assert rec.pairs == 0


def _additivity_row(**overrides: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "concept_name": "revenue",
        "axis_kind": "time",
        "axis_key": "*",
        "status": "classified",
        "verdict": "semi_additive",
        "reason": "stock",
        "abstain_reason": None,
        "bucket_grain": None,
    }
    base.update(overrides)
    return _row(**base)


class TestAdditivityFold:
    """has_additivity rows folded onto their concept (DAT-671 R4)."""

    def test_verdicts_land_on_the_concept_class_row_before_refinements(self) -> None:
        """A concrete axis REFINES the class row, so it must be read after it."""
        rows = [
            _additivity_row(axis_kind="time", axis_key="posting_period", bucket_grain="month"),
            _additivity_row(axis_kind="categorical", verdict="additive", reason=None),
            _additivity_row(axis_kind="time", axis_key="*"),
        ]
        out = _assemble_concept_contexts(
            [("revenue", "measure")], [], {}, [], [], {}, _TABLES, {}, rows
        )

        assert [(a.axis_kind, a.axis_key) for a in out[0].additivity] == [
            ("categorical", "*"),
            ("time", "*"),
            ("time", "posting_period"),
        ]
        refined = out[0].additivity[-1]
        assert refined.verdict == "semi_additive"
        assert refined.reason == "stock"
        assert refined.bucket_grain == "month"

    def test_a_concept_with_no_verdict_carries_an_empty_list(self) -> None:
        """Never judged is not judged-and-additive: absence stays absence."""
        out = _assemble_concept_contexts(
            [("revenue", "measure"), ("expenses", "measure")],
            [],
            {},
            [],
            [],
            {},
            _TABLES,
            {},
            [_additivity_row(concept_name="revenue")],
        )

        assert len(out[1].additivity) == 0
        assert out[1].name == "expenses"

    def test_abstention_keeps_its_typed_reason_and_no_verdict(self) -> None:
        rows = [
            _additivity_row(
                status="abstained",
                verdict=None,
                reason=None,
                abstain_reason="unknown_temporal",
            )
        ]
        out = _assemble_concept_contexts(
            [("revenue", "measure")], [], {}, [], [], {}, _TABLES, {}, rows
        )

        (axis,) = out[0].additivity
        assert axis.status == "abstained"
        assert axis.verdict is None
        assert axis.abstain_reason == "unknown_temporal"

    def test_verdict_for_an_unserved_concept_drops_loud(self) -> None:
        """The two reads are scoped by DIFFERENT verticals — ``og_has_additivity``
        rides ``workspace_settings.active_vertical`` while the served concept list
        rides the runtime vertical the caller passed. A verdict for a concept this
        context does not carry has nowhere honest to go, so it drops (warned)
        rather than being attached to a neighbouring name."""
        rows = [
            _additivity_row(concept_name="revenue"),
            _additivity_row(concept_name="marketing_spend"),
        ]
        out = _assemble_concept_contexts(
            [("revenue", "measure")], [], {}, [], [], {}, _TABLES, {}, rows
        )

        assert [c.name for c in out] == ["revenue"]
        assert len(out[0].additivity) == 1


class TestReadReconciliationRows:
    """The served fold over real rows (DAT-739).

    Exercised through an actual view over the real table rather than a
    hand-built dict, because the fold, the ordering and the mirror-key
    registration are the parts that make ONE stored row serve BOTH endpoints of
    a symmetric assertion — and a pre-built dict proves none of them.
    """

    @staticmethod
    def _served(session: Session) -> dict[tuple[str, str], Any]:
        session.flush()
        session.execute(
            text(
                "CREATE VIEW IF NOT EXISTS current_concept_reconciliation AS "
                "SELECT * FROM concept_reconciliation"
            )
        )
        return _read_reconciliation_rows(session, "main")

    @staticmethod
    def _row(**kw: Any) -> ConceptReconciliation:
        base: dict[str, Any] = {
            "run_id": "om-1",
            "vertical": "finance",
            "status": ReconciliationStatus.EVALUATED.value,
            "verdict": ReconciliationVerdict.NO_TOLERANCE_DECLARED.value,
        }
        base.update(kw)
        return ConceptReconciliation(**base)

    def test_a_partner_assertion_serves_both_directions(self, session: Session) -> None:
        """One stored row, both endpoints — the mirror-key registration."""
        session.add(
            self._row(
                from_concept="ap",
                to_concept="purchases",
                pair_key="s-a|s-b",
                left_snippet_id="s-a",
                right_snippet_id="s-b",
                left_value=Decimal(100),
                right_value=Decimal(90),
                delta=Decimal(10),
                relative_delta=Decimal("0.1"),
            )
        )
        served = self._served(session)

        assert ("ap", "purchases") in served
        # The mirrored edge direction must find the SAME evaluation, not a hole.
        assert served[("ap", "purchases")] is served[("purchases", "ap")]
        assert served[("ap", "purchases")]["delta"] == 10.0

    def test_the_widest_divergence_wins_across_pairs(self, session: Session) -> None:
        """Three pairs, and the one that puts the tie-out most in question wins."""
        for pair_key, delta, relative in (
            ("s-a|s-b", 10, "0.1"),
            ("s-a|s-c", 50, "0.5"),
            ("s-b|s-c", 20, "0.2"),
        ):
            session.add(
                self._row(
                    from_concept="ap",
                    to_concept="ap",
                    pair_key=pair_key,
                    left_snippet_id=pair_key.split("|")[0],
                    right_snippet_id=pair_key.split("|")[1],
                    left_value=Decimal(100),
                    right_value=Decimal(100) - Decimal(delta),
                    delta=Decimal(delta),
                    relative_delta=Decimal(relative),
                )
            )
        served = self._served(session)[("ap", "ap")]

        assert served["pairs"] == 3
        assert served["evaluated_pairs"] == 3
        # Not the first row read, and not the last — the widest.
        assert served["relative_delta"] == 0.5
        assert served["delta"] == 50.0

    def test_a_partly_evaluated_assertion_stays_evaluated(self, session: Session) -> None:
        """One comparable pair among abstentions still yields a measurement …

        … and the counts keep the remainder visible, so a consumer can never read
        a partial evaluation as a whole one.
        """
        session.add(
            self._row(
                from_concept="cash",
                to_concept="cash",
                pair_key="s-a|s-b",
                left_snippet_id="s-a",
                right_snippet_id="s-b",
                left_value=Decimal(100),
                right_value=Decimal(100),
                delta=Decimal(0),
                relative_delta=Decimal(0),
            )
        )
        session.add(
            self._row(
                from_concept="cash",
                to_concept="cash",
                pair_key="s-a|s-c",
                left_snippet_id="s-a",
                right_snippet_id="s-c",
                status=ReconciliationStatus.ABSTAINED.value,
                verdict=None,
                abstain_reason="different_reporting_instants",
            )
        )
        served = self._served(session)[("cash", "cash")]

        assert served["status"] == "evaluated"
        assert served["pairs"] == 2
        assert served["evaluated_pairs"] == 1

    def test_an_all_abstained_assertion_carries_its_shared_reason(self, session: Session) -> None:
        session.add(
            self._row(
                from_concept="cash",
                to_concept="cash",
                pair_key="s-a|s-b",
                left_snippet_id="s-a",
                right_snippet_id="s-b",
                status=ReconciliationStatus.ABSTAINED.value,
                verdict=None,
                abstain_reason="different_reporting_instants",
            )
        )
        served = self._served(session)[("cash", "cash")]

        assert served["status"] == "abstained"
        assert served["abstain_reason"] == "different_reporting_instants"
        assert served["delta"] is None
