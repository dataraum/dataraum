"""Evaluating the reconciles_with assertion (DAT-739).

Pins the executor: which groundings pair, when a comparison is refused, and —
the design centre — that a tie-out is never graded against a tolerance nobody
declared. The groundings execute for real against DuckDB tables seeded with
known values, so a passing test means the stored statement ran and the numbers
came from data, not from a stubbed execution seam.
"""

from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import ConceptEdge, ConceptEdgePredicate
from dataraum.analysis.semantic.reconciliation import evaluate_reconciliations
from dataraum.analysis.semantic.reconciliation_db_models import (
    PAIR_KEY_UNPAIRED,
    ConceptReconciliation,
    ReconciliationAbstainReason,
    ReconciliationStatus,
    ReconciliationVerdict,
)
from dataraum.query.snippet_models import SQLSnippetRecord

VERTICAL = "finance"
OM_RUN = "om-run-1"


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    """A DuckDB the seeded groundings actually read."""
    return duckdb.connect(":memory:")


def _relation(conn: duckdb.DuckDBPyConnection, name: str, amounts: list[float]) -> None:
    """A relation whose SUM(amount) is a known number."""
    conn.execute(f"CREATE TABLE {name} (amount DOUBLE)")
    for amount in amounts:
        conn.execute(f"INSERT INTO {name} VALUES ({amount})")


def _empty_relation(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    conn.execute(f"CREATE TABLE {name} (amount DOUBLE)")


def _grounding(
    session: Session,
    sid: str,
    concept: str,
    relation: str,
    *,
    aggregation: str = "sum",
    as_of: str | None = None,
    expr: str = "SUM(amount)",
    failed: bool = False,
    parts: bool = True,
) -> None:
    """A grounding as the metrics phase's warm pass leaves it."""
    clause_parts: dict | None = None
    if parts:
        clause_parts = {
            "select": [{"expr": expr, "alias": "value"}],
            "from": [relation],
            "where": [],
        }
        if as_of is not None:
            clause_parts["period_binding"] = {"as_of": as_of, "window_close": as_of}
    session.add(
        SQLSnippetRecord(
            snippet_id=sid,
            workspace_id="test",
            snippet_type="extract",
            standard_field=concept,
            statement=f"{concept}@{relation}",
            aggregation=aggregation,
            schema_mapping_id="test",
            sql=f"SELECT {expr} AS value\nFROM {relation}",
            description="d",
            source=f"graph:{concept}",
            parts=clause_parts,
            failure_count=1 if failed else 0,
        )
    )


def _edge(
    session: Session, concept: str, partner: str | None = None, *, tolerance: float | None = None
) -> None:
    session.add(
        ConceptEdge(
            vertical=VERTICAL,
            predicate=ConceptEdgePredicate.RECONCILES_WITH.value,
            from_concept=concept,
            to_concept=partner or concept,
            tolerance=tolerance,
            source="derived",
        )
    )


def _rows(session: Session) -> list[ConceptReconciliation]:
    session.flush()
    return list(
        session.execute(
            select(ConceptReconciliation).order_by(ConceptReconciliation.pair_key)
        ).scalars()
    )


def _run(session: Session, conn: duckdb.DuckDBPyConnection):  # noqa: ANN202 - test helper
    return evaluate_reconciliations(session, conn, vertical=VERTICAL, run_id=OM_RUN)


class TestTheHonestTolerancePosture:
    """No tolerance is declared anywhere, so no pass/fail may be claimed."""

    def test_an_agreeing_pair_records_the_observed_delta_and_claims_nothing(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "gl", [100.0, 121.0])
        _relation(conn, "subledger", [221.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.EVALUATED.value
        assert row.verdict == ReconciliationVerdict.NO_TOLERANCE_DECLARED.value
        assert row.delta == Decimal(0)
        assert row.tolerance is None
        assert outcome.evaluated == 1
        # An agreeing tie-out is not a warning and not an alarm.
        assert not outcome.breached
        assert not outcome.failures

    def test_a_disagreeing_pair_is_disclosed_not_failed(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The AP 2.21M-vs-3.13M class: measured, localized, ungraded."""
        _relation(conn, "gl", [2_210_000.0])
        _relation(conn, "subledger", [3_130_000.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.EVALUATED.value
        # The break is REAL and large — and still carries no verdict, because
        # nothing declared the band that would make it a failure.
        assert row.verdict == ReconciliationVerdict.NO_TOLERANCE_DECLARED.value
        assert abs(row.delta or Decimal(0)) == Decimal(920_000)
        assert not outcome.breached
        assert outcome.observed  # disclosed as structured output, not a warning

    def test_the_pair_localizes_the_disagreement_to_both_groundings(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Localization names both angles; it does not elect a culprit."""
        _relation(conn, "gl", [2_210_000.0])
        _relation(conn, "subledger", [3_130_000.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")

        _run(session, conn)

        (row,) = _rows(session)
        assert {row.left_snippet_id, row.right_snippet_id} == {"s-gl", "s-sub"}
        assert {row.left_relation, row.right_relation} == {"gl", "subledger"}
        by_snippet = {
            row.left_snippet_id: row.left_value,
            row.right_snippet_id: row.right_value,
        }
        assert by_snippet["s-gl"] == Decimal(2_210_000)
        assert by_snippet["s-sub"] == Decimal(3_130_000)

    def test_a_declared_band_grades_the_same_delta(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The graded path activates the moment a band exists — nothing else changes."""
        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [101.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable", tolerance=0.05)

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.verdict == ReconciliationVerdict.WITHIN_TOLERANCE.value
        assert row.tolerance == pytest.approx(0.05)
        assert not outcome.breached

    def test_a_breached_band_is_a_warning(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [180.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable", tolerance=0.01)

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.verdict == ReconciliationVerdict.BEYOND_TOLERANCE.value
        assert "accounts_payable" in outcome.breached
        # A graded failure is NOT also an ungraded observation.
        assert not outcome.observed

    def test_the_database_refuses_a_pass_without_a_declared_band(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The posture is structural: the CHECK, not the executor, is the guarantee."""
        from sqlalchemy.exc import IntegrityError

        session.add(
            ConceptReconciliation(
                run_id=OM_RUN,
                vertical=VERTICAL,
                from_concept="accounts_payable",
                to_concept="accounts_payable",
                pair_key="a|b",
                left_snippet_id="a",
                right_snippet_id="b",
                left_value=Decimal(1),
                right_value=Decimal(1),
                delta=Decimal(0),
                relative_delta=Decimal(0),
                tolerance=None,  # no band declared …
                status=ReconciliationStatus.EVALUATED.value,
                verdict=ReconciliationVerdict.WITHIN_TOLERANCE.value,  # … yet a pass
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()


class TestComparability:
    """Two numbers are a tie-out only in the same frame."""

    def test_different_reporting_instants_abstain_without_subtracting(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "bs_q1", [2_210_000.0])
        _relation(conn, "bs_q2", [3_130_000.0])
        _grounding(session, "s-q1", "cash", "bs_q1", as_of="2024-03-31")
        _grounding(session, "s-q2", "cash", "bs_q2", as_of="2024-06-30")
        _edge(session, "cash")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.ABSTAINED.value
        assert row.abstain_reason == ReconciliationAbstainReason.DIFFERENT_REPORTING_INSTANTS.value
        # The delta is the thing being refused — a stock moved between the two
        # instants for calendar reasons, and subtracting would call that a break.
        assert row.delta is None
        assert row.relative_delta is None
        # The row still says WHY, by naming both instants.
        assert {row.left_as_of, row.right_as_of} == {"2024-03-31", "2024-06-30"}

    def test_a_shared_instant_is_comparable(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "bs", [500.0])
        _relation(conn, "tb", [500.0])
        _grounding(session, "s-bs", "cash", "bs", as_of="2024-03-31")
        _grounding(session, "s-tb", "cash", "tb", as_of="2024-03-31")
        _edge(session, "cash")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.EVALUATED.value

    def test_different_aggregations_abstain(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "gl", [100.0, 121.0])
        _grounding(session, "s-sum", "invoice_amount", "gl", aggregation="sum")
        _grounding(
            session, "s-cnt", "invoice_amount", "gl", aggregation="count", expr="COUNT(amount)"
        )
        _edge(session, "invoice_amount")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.ABSTAINED.value
        assert row.abstain_reason == ReconciliationAbstainReason.DIFFERENT_AGGREGATIONS.value

    def test_a_declared_row_restriction_is_not_a_comparability_barrier(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The executor does not read `predicate` as a reason to refuse.

        Two populations reaching one quantity IS the second angle (the GL's AP
        accounts against the whole subledger), so a differing declared
        restriction must not abstain. This pins exactly that — the snippets'
        SQL is unchanged, so what it guards is the ABSENCE of a predicate check
        in the comparability rules, not the execution of disjoint populations.
        """
        _relation(conn, "gl", [50.0, 50.0])
        _relation(conn, "ap_subledger", [100.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "ap_subledger")
        # The GL side restricts rows; the subledger side does not.
        gl = session.get(SQLSnippetRecord, "s-gl")
        assert gl is not None
        gl.predicate = "account_type = 'payable'"
        _edge(session, "accounts_payable")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.EVALUATED.value
        assert row.delta == Decimal(0)

    def test_a_non_numeric_result_is_louder_than_no_support(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A grounding measuring the wrong KIND of thing is not "nothing to measure"."""
        _relation(conn, "gl", [100.0])
        conn.execute("CREATE TABLE ledger_names (amount VARCHAR)")
        conn.execute("INSERT INTO ledger_names VALUES ('opening')")
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-txt", "accounts_payable", "ledger_names", expr="MAX(amount)")
        _edge(session, "accounts_payable")

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.abstain_reason == ReconciliationAbstainReason.NON_NUMERIC_VALUE.value
        # It points at the grounding, so it rides the loud channel — not the
        # "correctly declined to compare" one.
        assert "accounts_payable" in outcome.failures
        assert "accounts_payable" not in outcome.withheld

    def test_a_null_aggregate_is_no_value_never_zero(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """An empty relation measured nothing; reading it as 0 would fabricate a break."""
        _relation(conn, "gl", [100.0])
        _empty_relation(conn, "subledger")
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.status == ReconciliationStatus.ABSTAINED.value
        assert row.abstain_reason == ReconciliationAbstainReason.NO_VALUE.value
        assert row.delta is None
        assert not outcome.failures  # no support is not a malfunction


class TestWhatIsAsserted:
    def test_a_concept_with_no_edge_is_untouched(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """No assertion, no row — a single-grounding concept is never gated."""
        _relation(conn, "gl", [100.0])
        _grounding(session, "s-gl", "revenue", "gl")

        outcome = _run(session, conn)

        assert _rows(session) == []
        assert outcome.rows == 0

    def test_an_assertion_with_one_grounding_abstains_as_unpaired(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The witness producer's shape: asserted, and with no second SQL angle."""
        _relation(conn, "gl", [100.0])
        _grounding(session, "s-gl", "revenue", "gl")
        _edge(session, "revenue")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.pair_key == PAIR_KEY_UNPAIRED
        assert row.abstain_reason == ReconciliationAbstainReason.NO_EVALUABLE_PAIR.value
        assert row.left_snippet_id is None

    def test_a_superseded_assertion_is_not_evaluated(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        from datetime import UTC, datetime

        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [100.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")
        session.flush()
        edge = session.execute(select(ConceptEdge)).scalar_one()
        edge.superseded_at = datetime.now(UTC)

        _run(session, conn)

        assert _rows(session) == []

    def test_a_failed_grounding_is_not_a_second_angle(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [100.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger", failed=True)
        _edge(session, "accounts_payable")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.pair_key == PAIR_KEY_UNPAIRED

    def test_partner_concepts_pair_across_sides(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "ap", [900.0])
        _relation(conn, "purchases", [900.0])
        _grounding(session, "s-ap", "accounts_payable", "ap")
        _grounding(session, "s-pur", "purchases", "purchases")
        _edge(session, "accounts_payable", "purchases")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.from_concept == "accounts_payable"
        assert row.to_concept == "purchases"
        assert row.status == ReconciliationStatus.EVALUATED.value
        assert {row.left_snippet_id, row.right_snippet_id} == {"s-ap", "s-pur"}

    def test_a_mirrored_partner_assertion_is_evaluated_once(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """reconciles_with is symmetric and stored BOTH ways — one comparison."""
        _relation(conn, "ap", [900.0])
        _relation(conn, "purchases", [900.0])
        _grounding(session, "s-ap", "accounts_payable", "ap")
        _grounding(session, "s-pur", "purchases", "purchases")
        _edge(session, "accounts_payable", "purchases")
        _edge(session, "purchases", "accounts_payable")  # the stored mirror

        outcome = _run(session, conn)

        rows = _rows(session)
        assert len(rows) == 1
        # Recorded under the name-ordered endpoints, so the two spellings of one
        # assertion cannot become two homes for one fact.
        assert (rows[0].from_concept, rows[0].to_concept) == ("accounts_payable", "purchases")
        assert outcome.evaluated == 1

    def test_a_band_declared_on_either_direction_grades_the_assertion(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "ap", [100.0])
        _relation(conn, "purchases", [180.0])
        _grounding(session, "s-ap", "accounts_payable", "ap")
        _grounding(session, "s-pur", "purchases", "purchases")
        _edge(session, "accounts_payable", "purchases")
        _edge(session, "purchases", "accounts_payable", tolerance=0.01)

        _run(session, conn)

        (row,) = _rows(session)
        assert row.tolerance == pytest.approx(0.01)
        assert row.verdict == ReconciliationVerdict.BEYOND_TOLERANCE.value

    def test_three_groundings_produce_every_pair(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [100.0])
        _relation(conn, "statement", [140.0])
        _grounding(session, "s-a", "accounts_payable", "gl")
        _grounding(session, "s-b", "accounts_payable", "subledger")
        _grounding(session, "s-c", "accounts_payable", "statement")
        _edge(session, "accounts_payable")

        _run(session, conn)

        rows = _rows(session)
        assert len(rows) == 3
        assert {r.pair_key for r in rows} == {"s-a|s-b", "s-a|s-c", "s-b|s-c"}
        # Two of the three pairs disagree — and the third pins where they agree,
        # which is what makes a third angle informative at all.
        assert sum(1 for r in rows if r.delta == Decimal(0)) == 1


class TestRowContract:
    def test_the_pair_key_is_canonical(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The assertion is symmetric; the pair identity must not depend on order."""
        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [100.0])
        # Seeded so the enumeration order (by snippet id) is the REVERSE of the
        # canonical one would be if it followed insertion.
        _grounding(session, "s-zzz", "accounts_payable", "gl")
        _grounding(session, "s-aaa", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.pair_key == "s-aaa|s-zzz"
        assert row.left_snippet_id == "s-aaa"

    def test_a_grounding_without_parts_is_unresolved(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        _relation(conn, "gl", [100.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-bare", "accounts_payable", "gl", parts=False)
        _edge(session, "accounts_payable")

        _run(session, conn)

        (row,) = _rows(session)
        assert row.abstain_reason == ReconciliationAbstainReason.UNRESOLVED_GROUNDING.value

    def test_a_broken_grounding_is_a_warning(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A stored statement that no longer runs is reported, never repaired."""
        _relation(conn, "gl", [100.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-gone", "accounts_payable", "vanished_table")
        _edge(session, "accounts_payable")

        outcome = _run(session, conn)

        (row,) = _rows(session)
        assert row.abstain_reason == ReconciliationAbstainReason.EXECUTION_FAILED.value
        assert "accounts_payable" in outcome.failures
        # A malfunction rides ONE channel — the loud one.
        assert "accounts_payable" not in outcome.withheld

    def test_every_broken_pair_is_disclosed_not_just_the_last(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """An assertion's channel entry accumulates; it does not overwrite.

        Three pairs failing for three reasons must not surface as one line — the
        channel is a disclosure, and keeping only the last one defeats it.
        """
        _relation(conn, "gl", [100.0])
        _grounding(session, "s-a", "accounts_payable", "gl")
        _grounding(session, "s-gone1", "accounts_payable", "vanished_one")
        _grounding(session, "s-gone2", "accounts_payable", "vanished_two")
        _edge(session, "accounts_payable")

        outcome = _run(session, conn)

        assert len(_rows(session)) == 3
        disclosed = outcome.failures["accounts_payable"]
        assert "s-gone1" in disclosed
        assert "s-gone2" in disclosed

    def test_an_evaluated_row_cannot_omit_its_measurement(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """status='evaluated' with no delta would read as a tie-out nobody computed."""
        from sqlalchemy.exc import IntegrityError

        session.add(
            ConceptReconciliation(
                run_id=OM_RUN,
                vertical=VERTICAL,
                from_concept="ap",
                to_concept="ap",
                pair_key="a|b",
                left_snippet_id="a",
                right_snippet_id="b",
                status=ReconciliationStatus.EVALUATED.value,
                verdict=ReconciliationVerdict.NO_TOLERANCE_DECLARED.value,
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_the_unpaired_sentinel_cannot_name_groundings(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """'*' means no pair was formed; naming one would contradict it."""
        from sqlalchemy.exc import IntegrityError

        session.add(
            ConceptReconciliation(
                run_id=OM_RUN,
                vertical=VERTICAL,
                from_concept="ap",
                to_concept="ap",
                pair_key=PAIR_KEY_UNPAIRED,
                left_snippet_id="a",
                right_snippet_id="b",
                status=ReconciliationStatus.ABSTAINED.value,
                abstain_reason=ReconciliationAbstainReason.NO_EVALUABLE_PAIR.value,
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_re_evaluating_the_same_run_is_idempotent(
        self, session: Session, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """ADR-0010: an at-least-once activity retry must not duplicate rows."""
        _relation(conn, "gl", [100.0])
        _relation(conn, "subledger", [140.0])
        _grounding(session, "s-gl", "accounts_payable", "gl")
        _grounding(session, "s-sub", "accounts_payable", "subledger")
        _edge(session, "accounts_payable")

        _run(session, conn)
        _run(session, conn)

        rows = _rows(session)
        assert len(rows) == 1
        assert rows[0].delta == Decimal(-40)
