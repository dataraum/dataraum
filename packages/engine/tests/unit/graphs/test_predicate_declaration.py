"""An extract step's DECLARED row restriction (DAT-838).

Before this, an extract carried only standard_field + statement + aggregation, so
"count the rows WHERE status is reconciled" could not be declared. The model wrote
the closest expressible thing — counting a dimension column, which counts every row
— and the resulting rate was 1.0 by construction while its declared bound check
passed. These tests pin the three places the declaration has to survive: parsing,
the authoring surface, and enforcement against what the model actually grounded.
"""

from __future__ import annotations

from pathlib import Path

from dataraum.graphs.grounding_validation import validate_grounding_basis
from dataraum.graphs.loader import GraphLoader
from dataraum.graphs.models import (
    ConceptGroundingBasis,
    ConceptGroundingEntry,
    ExtractGroundingOutput,
    GraphProvenanceOutput,
)

_SCHEMA = {"bank_txn": {"amount", "reconciliation_status"}}


def _grounding(where: list[str], filter_columns: list[str]) -> ExtractGroundingOutput:
    return ExtractGroundingOutput(
        grounding="reconciled rows via reconciliation_status = 'reconciled'",
        relation="bank_txn",
        where=where,
        select_expr="COUNT(*)",
        description="Reconciled transaction count",
        assumptions=[],
        provenance=GraphProvenanceOutput(
            column_mappings_basis=[
                ConceptGroundingEntry(
                    concept="reconciled_count",
                    basis=ConceptGroundingBasis(
                        measure_columns=[],
                        filter_columns=filter_columns,
                        filter="reconciliation_status = 'reconciled'" if filter_columns else "",
                        filter_members=[],
                    ),
                )
            ]
        ),
    )


class TestLoader:
    def test_parses_a_declared_predicate(self) -> None:
        step = GraphLoader()._parse_step(
            Path("<t>"),
            "reconciled_count",
            {
                "type": "extract",
                "aggregation": "count",
                "source": {
                    "standard_field": "bank_transaction",
                    "statement": "",
                    "predicate": "status is reconciled",
                },
            },
        )
        assert step.source is not None
        assert step.source.predicate == "status is reconciled"

    def test_absent_and_null_both_mean_unrestricted(self) -> None:
        for source in (
            {"standard_field": "revenue"},
            {"standard_field": "revenue", "predicate": None},
        ):
            step = GraphLoader()._parse_step(Path("<t>"), "revenue", {"source": source})
            assert step.source is not None
            assert step.source.predicate == ""


class TestEnforcement:
    """A declared restriction that never reached the SQL is the whole defect."""

    def test_declared_predicate_grounded_to_nothing_is_a_violation(self) -> None:
        violations = validate_grounding_basis(
            _grounding(where=[], filter_columns=[]),
            _SCHEMA,
            None,
            None,
            "status is reconciled",
        )
        assert any("status is reconciled" in v and "`where` is empty" in v for v in violations)

    def test_whitespace_only_predicate_does_not_count_as_grounded(self) -> None:
        violations = validate_grounding_basis(
            _grounding(where=["  "], filter_columns=[]), _SCHEMA, None, None, "status is reconciled"
        )
        assert any("`where` is empty" in v for v in violations)

    def test_a_grounded_restriction_passes(self) -> None:
        violations = validate_grounding_basis(
            _grounding(
                where=["reconciliation_status = 'reconciled'"],
                filter_columns=["reconciliation_status"],
            ),
            _SCHEMA,
            None,
            None,
            "status is reconciled",
        )
        assert violations == []

    def test_an_undeclared_step_is_not_forced_to_filter(self) -> None:
        """Only a DECLARED restriction is enforced — this is not "every extract must filter"."""
        assert (
            validate_grounding_basis(_grounding(where=[], filter_columns=[]), _SCHEMA, None, None)
            == []
        )

    def test_the_fall_loud_shape_stays_exempt(self) -> None:
        """A concept that cannot be grounded falls loud; that is not a predicate failure."""
        out = _grounding(where=[], filter_columns=[])
        out.relation = ""
        assert validate_grounding_basis(out, _SCHEMA, None, None, "status is reconciled") == []

    def test_the_text_itself_is_never_compared(self) -> None:
        """Only PRESENCE is enforced.

        Whether 'status is reconciled' is faithfully rendered is a grounding judgment
        made against the served value sets, not something a string comparison can
        decide — a predicate on a different column still satisfies this check and is
        caught, if at all, by the filter_members membership rules.
        """
        assert (
            validate_grounding_basis(
                _grounding(where=["amount > 0"], filter_columns=["amount"]),
                _SCHEMA,
                None,
                None,
                "status is reconciled",
            )
            == []
        )
