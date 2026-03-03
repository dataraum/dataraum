"""Tests for _build_candidate_metrics_lookup direction handling."""

import pytest

from dataraum.analysis.semantic.processor import _build_candidate_metrics_lookup


def _make_candidate(
    table1: str,
    table2: str,
    col1: str,
    col2: str,
    *,
    cardinality: str | None = None,
    left_ri: float | None = None,
    right_ri: float | None = None,
    introduces_duplicates: bool | None = None,
    join_success_rate: float | None = None,
    orphan_count: int | None = None,
    cardinality_verified: bool | None = None,
    topology_similarity: float | None = None,
) -> dict:
    jc: dict = {"column1": col1, "column2": col2}
    if cardinality is not None:
        jc["cardinality"] = cardinality
    if left_ri is not None:
        jc["left_referential_integrity"] = left_ri
    if right_ri is not None:
        jc["right_referential_integrity"] = right_ri
    if orphan_count is not None:
        jc["orphan_count"] = orphan_count
    if cardinality_verified is not None:
        jc["cardinality_verified"] = cardinality_verified

    candidate: dict = {"table1": table1, "table2": table2, "join_columns": [jc]}
    if introduces_duplicates is not None:
        candidate["introduces_duplicates"] = introduces_duplicates
    if join_success_rate is not None:
        candidate["join_success_rate"] = join_success_rate
    if topology_similarity is not None:
        candidate["topology_similarity"] = topology_similarity
    return candidate


class TestForwardDirection:
    def test_preserves_original_metrics(self):
        candidates = [
            _make_candidate(
                "trial_balance",
                "chart_of_accounts",
                "account_id",
                "id",
                cardinality="one-to-many",
                left_ri=1.0,
                right_ri=0.85,
                introduces_duplicates=True,
                join_success_rate=0.95,
            )
        ]
        lookup = _build_candidate_metrics_lookup(candidates)
        forward = lookup[("trial_balance", "account_id", "chart_of_accounts", "id")]

        assert forward["cardinality"] == "one-to-many"
        assert forward["left_referential_integrity"] == 1.0
        assert forward["right_referential_integrity"] == 0.85
        assert forward["introduces_duplicates"] is True
        assert forward["join_success_rate"] == 0.95


class TestReverseDirection:
    def test_flips_one_to_many_to_many_to_one(self):
        candidates = [
            _make_candidate(
                "trial_balance",
                "chart_of_accounts",
                "account_id",
                "id",
                cardinality="one-to-many",
            )
        ]
        lookup = _build_candidate_metrics_lookup(candidates)
        reverse = lookup[("chart_of_accounts", "id", "trial_balance", "account_id")]

        assert reverse["cardinality"] == "many-to-one"

    def test_flips_many_to_one_to_one_to_many(self):
        candidates = [
            _make_candidate(
                "payments",
                "invoices",
                "invoice_id",
                "id",
                cardinality="many-to-one",
            )
        ]
        lookup = _build_candidate_metrics_lookup(candidates)
        reverse = lookup[("invoices", "id", "payments", "invoice_id")]

        assert reverse["cardinality"] == "one-to-many"

    def test_swaps_left_right_ri(self):
        candidates = [
            _make_candidate(
                "trial_balance",
                "chart_of_accounts",
                "account_id",
                "id",
                left_ri=1.0,
                right_ri=0.85,
            )
        ]
        lookup = _build_candidate_metrics_lookup(candidates)
        reverse = lookup[("chart_of_accounts", "id", "trial_balance", "account_id")]

        assert reverse["right_referential_integrity"] == 1.0
        assert reverse["left_referential_integrity"] == 0.85

    def test_omits_introduces_duplicates(self):
        candidates = [
            _make_candidate(
                "trial_balance",
                "chart_of_accounts",
                "account_id",
                "id",
                introduces_duplicates=True,
            )
        ]
        lookup = _build_candidate_metrics_lookup(candidates)
        reverse = lookup[("chart_of_accounts", "id", "trial_balance", "account_id")]

        assert "introduces_duplicates" not in reverse


class TestSymmetricCardinalities:
    @pytest.mark.parametrize("card", ["one-to-one", "many-to-many"])
    def test_symmetric_cardinality_unchanged_in_reverse(self, card: str):
        candidates = [_make_candidate("a", "b", "col_a", "col_b", cardinality=card)]
        lookup = _build_candidate_metrics_lookup(candidates)

        assert lookup[("a", "col_a", "b", "col_b")]["cardinality"] == card
        assert lookup[("b", "col_b", "a", "col_a")]["cardinality"] == card


class TestEdgeCases:
    def test_empty_candidates_returns_empty_lookup(self):
        assert _build_candidate_metrics_lookup([]) == {}
        assert _build_candidate_metrics_lookup(None) == {}

    def test_missing_optional_fields_no_error(self):
        candidates = [_make_candidate("a", "b", "col_a", "col_b", cardinality="one-to-many")]
        lookup = _build_candidate_metrics_lookup(candidates)
        forward = lookup[("a", "col_a", "b", "col_b")]
        reverse = lookup[("b", "col_b", "a", "col_a")]

        assert "left_referential_integrity" not in forward
        assert "right_referential_integrity" not in forward
        assert "introduces_duplicates" not in forward
        assert "left_referential_integrity" not in reverse
        assert "right_referential_integrity" not in reverse

    def test_forward_and_reverse_are_independent_dicts(self):
        """Mutating one direction must not affect the other."""
        candidates = [
            _make_candidate(
                "a",
                "b",
                "col_a",
                "col_b",
                cardinality="one-to-many",
                left_ri=1.0,
                right_ri=0.5,
            )
        ]
        lookup = _build_candidate_metrics_lookup(candidates)
        forward = lookup[("a", "col_a", "b", "col_b")]
        reverse = lookup[("b", "col_b", "a", "col_a")]

        # Mutate forward
        forward["cardinality"] = "MUTATED"
        assert reverse["cardinality"] == "many-to-one"
