"""The relationship-candidate block must show the LLM real overlap scores.

The DB candidate-dict wire format (load_relationship_candidates_for_semantic) keys
the value-overlap score ``confidence``; the formatter previously read only
``join_confidence`` and rendered ``overlap=0.00`` for every candidate, degrading
relationship confirmation. Every candidate pair is now served (no cap, DAT-649).
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
                {
                    "column1": "customer_id",
                    "column2": "id",
                    "confidence": 0.92,
                    "cardinality": "many-to-one",
                },
                {
                    "column1": "region",
                    "column2": "region",
                    "confidence": 0.30,
                    "cardinality": "many-to-many",
                },
            ],
        }
    ]


def test_overlap_score_is_rendered_from_the_confidence_key() -> None:
    agent = SemanticAgent.__new__(SemanticAgent)  # _format_* is self-contained
    out = agent._format_relationship_candidates(_candidates())
    assert "overlap=0.92" in out
    assert "overlap=0.00" not in out  # the regression


def test_candidates_are_sorted_by_real_overlap() -> None:
    """The stronger pair must come first (strongest-overlap-first ordering)."""
    agent = SemanticAgent.__new__(SemanticAgent)
    out = agent._format_relationship_candidates(_candidates())
    assert out.index("customer_id <-> id") < out.index("region <-> region")


def test_composite_rescue_hint_is_rendered() -> None:
    """A candidate carrying a composite_key hint renders the rescue block (DAT-277)."""
    agent = SemanticAgent.__new__(SemanticAgent)
    cands = _candidates()
    cands[0]["composite_key"] = {
        "column_pairs": [["customer_id", "id"], ["region", "region"]],
        "cardinality": "many-to-one",
        "coverage": 0.003,
    }
    out = agent._format_relationship_candidates(cands)
    assert "COMPOSITE-KEY RESCUE" in out
    assert "customer_id <-> id, region <-> region" in out
    assert "many-to-one" in out
    assert "matches 0.3% of rows" in out  # the judge sees a hollow key's number (DAT-695)


def test_no_hint_renders_no_rescue_block() -> None:
    agent = SemanticAgent.__new__(SemanticAgent)
    out = agent._format_relationship_candidates(_candidates())
    assert "COMPOSITE-KEY RESCUE" not in out
