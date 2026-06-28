"""The relationship-candidate block must show the LLM real overlap scores.

The DB candidate-dict wire format (load_relationship_candidates_for_semantic) keys
the value-overlap score ``confidence``; the formatter previously read only
``join_confidence`` and rendered ``overlap=0.00`` for every candidate (and truncated
the top-N arbitrarily), degrading relationship confirmation.
"""

from __future__ import annotations

from dataraum.analysis.semantic.agent import SemanticAgent


def _candidates() -> list[dict]:
    # Exactly the shape load_relationship_candidates_for_semantic emits.
    return [
        {
            "table1": "orders",
            "table2": "customers",
            "join_columns": [
                {"column1": "customer_id", "column2": "id", "confidence": 0.92, "cardinality": "many-to-one"},
                {"column1": "region", "column2": "region", "confidence": 0.30, "cardinality": "many-to-many"},
            ],
        }
    ]


def test_overlap_score_is_rendered_from_the_confidence_key() -> None:
    agent = SemanticAgent.__new__(SemanticAgent)  # _format_* is self-contained
    out = agent._format_relationship_candidates(_candidates())
    assert "overlap=0.92" in out
    assert "overlap=0.00" not in out  # the regression


def test_candidates_are_sorted_by_real_overlap() -> None:
    """The stronger pair must come first (top-N truncation relies on it)."""
    agent = SemanticAgent.__new__(SemanticAgent)
    out = agent._format_relationship_candidates(_candidates())
    assert out.index("customer_id <-> id") < out.index("region <-> region")
