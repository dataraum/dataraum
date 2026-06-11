"""Derived-formula adjudication measurement (ADR-0009, derived-value 2nd witness).

Two witnesses per canonical formula claim: the data's row grading (discovery /
loader match rate) vs the LLM's name-based hypothesis. Asserts canonicalization
(claims are structures, never raw strings), witness directions, abstention
discipline (no hypothesis / ungradable → abstain, not dissent), and the grounded
divergence case (name expects one formula, data grades it broken → conflict).
Properties/orderings, not point thresholds.
"""

from __future__ import annotations

import pytest

from dataraum.entropy.measurements.derived_value import (
    CLAIM_SPACE,
    canonicalize_discovered,
    discovery_distribution,
    llm_hypothesis_distribution,
    measure_derived_value,
    parse_formula,
)

_HOLDS = CLAIM_SPACE.index("holds")


# --- canonicalization (claims are canonical, never raw strings) ---------------
class TestParseFormula:
    def test_binary_arithmetic_parses(self) -> None:
        c = parse_formula("subtotal + tax_amount")
        assert c is not None
        assert c.operation == "sum"
        assert c.operands == ("subtotal", "tax_amount")

    def test_commutative_operands_sort_to_one_identity(self) -> None:
        a = parse_formula("tax_amount + subtotal")
        b = parse_formula("subtotal + tax_amount")
        assert a is not None and b is not None
        assert a.identity == b.identity

    def test_non_commutative_order_is_preserved(self) -> None:
        a = parse_formula("gross - tax")
        b = parse_formula("tax - gross")
        assert a is not None and b is not None
        assert a.identity != b.identity

    def test_syntax_noise_collapses(self) -> None:
        # Casing, whitespace, parens: structure decides identity, not the string.
        a = parse_formula("(Quantity)*UNIT_PRICE")
        b = parse_formula("unit_price * quantity")
        assert a is not None and b is not None
        assert a.identity == b.identity

    def test_equation_form_takes_the_expression_side(self) -> None:
        c = parse_formula("total = subtotal + tax")
        assert c is not None
        assert c.identity == parse_formula("subtotal + tax").identity  # type: ignore[union-attr]

    def test_unsupported_shapes_return_none(self) -> None:
        assert parse_formula(None) is None
        assert parse_formula("") is None
        assert parse_formula("UPPER(name)") is None  # function, not arithmetic
        assert parse_formula("amount * 0.19") is None  # literal operand
        assert parse_formula("a + b + c") is None  # more than two operands
        assert parse_formula("just some text ???") is None

    def test_discovered_entry_falls_back_to_typed_sources(self) -> None:
        # A formula string with spaces in column names won't parse; the row's
        # derivation_type + source names still yield the canonical structure.
        c = canonicalize_discovered(
            {
                "formula": "net amount + tax amount",
                "derivation_type": "sum",
                "source_column_names": ["net amount", "tax amount"],
            }
        )
        assert c is not None
        assert c.operation == "sum"


# --- witness extractors --------------------------------------------------------
class TestWitnessDistributions:
    def test_discovery_grading_is_the_match_rate(self) -> None:
        assert discovery_distribution(0.99)["holds"] == pytest.approx(0.99)
        assert discovery_distribution(0.1)["holds"] == pytest.approx(0.1)

    def test_ungraded_discovery_abstains(self) -> None:
        assert discovery_distribution(None)["holds"] == 0.5

    def test_hypothesis_leans_holds_by_confidence(self) -> None:
        strong = llm_hypothesis_distribution(0.9)["holds"]
        weak = llm_hypothesis_distribution(0.2)["holds"]
        assert strong > weak > 0.5

    def test_hypothesis_without_confidence_abstains(self) -> None:
        # No invented default strength: absent confidence is no opinion.
        assert llm_hypothesis_distribution(None)["holds"] == 0.5
        assert llm_hypothesis_distribution(0.0)["holds"] == 0.5


# --- pooled adjudication --------------------------------------------------------
class TestMeasure:
    def test_nothing_in_play_measures_nothing(self) -> None:
        assert measure_derived_value("orders", "total", [], None) == []

    def test_matching_hypothesis_corroborates_one_quiet_slot(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "subtotal + tax", "match_rate": 0.99, "derivation_type": "sum"}],
            {"formula": "tax + subtotal", "confidence": 0.9, "match_rate": None},
        )
        assert len(adjs) == 1  # canonical match → ONE claim slot, not two
        adj = adjs[0]
        assert adj.discovered and adj.hypothesized
        assert {w.witness_id for w in adj.witnesses} == {"formula_discovery", "llm_hypothesis"}
        assert adj.result.conflict < 0.1
        assert adj.result.posterior[_HOLDS] > 0.5

    def test_grounded_divergence_raises_conflict(self) -> None:
        """THE case: name expects subtotal+tax, data grades that formula broken."""
        divergent = measure_derived_value(
            "orders",
            "total",
            [{"formula": "subtotal * tax_rate", "match_rate": 0.99, "derivation_type": "product"}],
            {"formula": "subtotal + tax", "confidence": 0.9, "match_rate": 0.1},
        )
        agreeing = measure_derived_value(
            "orders",
            "total",
            [{"formula": "subtotal + tax", "match_rate": 0.99, "derivation_type": "sum"}],
            {"formula": "subtotal + tax", "confidence": 0.9, "match_rate": None},
        )
        assert len(divergent) == 2  # the discovered slot + the hypothesis slot
        hyp_slot = next(a for a in divergent if a.hypothesized)
        agree_slot = agreeing[0]
        assert hyp_slot.result.conflict > agree_slot.result.conflict
        assert hyp_slot.result.conflict > 0.3

    def test_llm_abstains_on_formulas_it_did_not_hypothesize(self) -> None:
        # Absence of a hypothesis is not dissent (the type_claim lesson): the
        # discovered slot keeps a lone data witness, conflict stays zero there.
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "subtotal * tax_rate", "match_rate": 0.99, "derivation_type": "product"}],
            {"formula": "subtotal + tax", "confidence": 0.9, "match_rate": 0.95},
        )
        disc_slot = next(a for a in adjs if a.discovered)
        assert [w.witness_id for w in disc_slot.witnesses] == ["formula_discovery"]
        assert disc_slot.result.conflict == 0.0

    def test_collinear_hypothesis_stays_quiet(self) -> None:
        # The hypothesis differs from the discovery but ALSO holds in the data
        # (both formulas true) → its slot agrees with its grading → no conflict.
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "subtotal * tax_rate", "match_rate": 0.99, "derivation_type": "product"}],
            {"formula": "subtotal + tax", "confidence": 0.9, "match_rate": 0.98},
        )
        hyp_slot = next(a for a in adjs if a.hypothesized)
        assert hyp_slot.result.conflict < 0.1
        assert hyp_slot.result.posterior[_HOLDS] > 0.5

    def test_no_hypothesis_keeps_the_lone_data_witness(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "a + b", "match_rate": 0.97, "derivation_type": "sum"}],
            None,
        )
        assert len(adjs) == 1
        assert [w.witness_id for w in adjs[0].witnesses] == ["formula_discovery"]
        assert adjs[0].result.conflict == 0.0

    def test_ungradable_hypothesis_routes_to_ignorance_not_conflict(self) -> None:
        # Hypothesis over a hallucinated source column: the loader returned no
        # grading → the data witness abstains on that slot → lone LLM witness →
        # ignorance, never manufactured conflict.
        adjs = measure_derived_value(
            "orders",
            "total",
            [],
            {"formula": "phantom_a + phantom_b", "confidence": 0.9, "match_rate": None},
        )
        assert len(adjs) == 1
        assert [w.witness_id for w in adjs[0].witnesses] == ["llm_hypothesis"]
        assert adjs[0].result.conflict == 0.0
        graded = measure_derived_value(
            "orders",
            "total",
            [],
            {"formula": "a + b", "confidence": 0.9, "match_rate": 0.97},
        )
        assert adjs[0].result.ignorance > graded[0].result.ignorance

    def test_unparseable_hypothesis_is_no_hypothesis(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "a + b", "match_rate": 0.99, "derivation_type": "sum"}],
            {"formula": "some prose, not a formula", "confidence": 0.9, "match_rate": None},
        )
        assert len(adjs) == 1
        assert not adjs[0].hypothesized

    def test_self_referential_hypothesis_is_degenerate(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [],
            {"formula": "total - discount", "confidence": 0.9, "match_rate": 0.99},
        )
        assert adjs == []

    def test_claim_field_identity(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "tax + subtotal", "match_rate": 0.99, "derivation_type": "sum"}],
            None,
        )
        assert adjs[0].claim_field == "derived_formula:orders.total:sum(subtotal,tax)"

    def test_reliabilities_thread_into_witnesses(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [{"formula": "a + b", "match_rate": 0.9, "derivation_type": "sum"}],
            {"formula": "a + b", "confidence": 0.8, "match_rate": None},
            reliabilities={"formula_discovery": 0.55, "llm_hypothesis": 0.44},
        )
        by_id = {w.witness_id: w.reliability for w in adjs[0].witnesses}
        assert by_id == {"formula_discovery": 0.55, "llm_hypothesis": 0.44}

    def test_duplicate_discovered_rows_collapse_to_one_slot(self) -> None:
        adjs = measure_derived_value(
            "orders",
            "total",
            [
                {"formula": "a + b", "match_rate": 0.92, "derivation_type": "sum"},
                {"formula": "b + a", "match_rate": 0.97, "derivation_type": "sum"},
            ],
            None,
        )
        assert len(adjs) == 1
        assert adjs[0].match_rate == pytest.approx(0.97)  # best grading wins
