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
        "coverage_table": "orders",
    }
    out = agent._format_relationship_candidates(cands)
    assert "COMPOSITE-KEY RESCUE" in out
    assert "customer_id <-> id, region <-> region" in out
    assert "many-to-one" in out
    # The judge sees a hollow key's number AND whose rows it describes (DAT-695),
    # leading the decision frame rather than trailing it.
    assert "MEASURED USAGE: 0.3% of orders's populated-key rows" in out
    assert "DECLINE the relationship entirely" in out


def test_no_hint_renders_no_rescue_block() -> None:
    agent = SemanticAgent.__new__(SemanticAgent)
    out = agent._format_relationship_candidates(_candidates())
    assert "COMPOSITE-KEY RESCUE" not in out


def test_uniqueness_asymmetry_is_rendered() -> None:
    """The per-side uniqueness bracket renders from the DB wire format (DAT-725).

    The loader now serves ``left_uniqueness``/``right_uniqueness`` from the
    candidate evidence; the formatter must render the asymmetry — it is the
    judge's orientation evidence (FK side = non-unique side).
    """
    agent = SemanticAgent.__new__(SemanticAgent)
    cands = _candidates()
    cands[0]["join_columns"][0]["left_uniqueness"] = 0.02
    cands[0]["join_columns"][0]["right_uniqueness"] = 1.0
    out = agent._format_relationship_candidates(cands)
    assert "[uniq: L=0.02 R=1.00]" in out


def test_role_annotations_are_rendered_pair_local() -> None:
    """DAT-723: established per-column roles render on the candidate line.

    The judge weighs "period identifier × containment" on the line where the
    containment is printed, instead of cross-referencing the far-away
    annotation block.
    """
    agent = SemanticAgent.__new__(SemanticAgent)
    cands = _candidates()
    jc = cands[0]["join_columns"][0]
    jc["column1_role"] = "timestamp"
    jc["column1_entity_type"] = "fiscal_period"
    jc["column2_role"] = "key"
    out = agent._format_relationship_candidates(cands)
    assert "[role: L=timestamp(fiscal_period) R=key]" in out


def test_role_bracket_renders_only_annotated_sides() -> None:
    """A one-sided annotation renders one side — the bare side stays absent."""
    agent = SemanticAgent.__new__(SemanticAgent)
    cands = _candidates()
    cands[0]["join_columns"][0]["column2_role"] = "key"
    out = agent._format_relationship_candidates(cands)
    assert "[role: R=key]" in out
    assert "L=" not in out.split("[role: ")[1].split("]")[0]


def test_role_bracket_omitted_when_no_annotations() -> None:
    """Candidates without annotation fields render no role bracket at all."""
    agent = SemanticAgent.__new__(SemanticAgent)
    out = agent._format_relationship_candidates(_candidates())
    assert "[role:" not in out
