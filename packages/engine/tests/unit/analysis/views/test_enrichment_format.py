"""The enrichment feed renders measured join coverage (DAT-695).

A grain-safe key can still match almost nothing (a lookalike dimension) — the
judge must see the number next to the grain marker so it can decline a join
that would enrich nothing. No coverage in evidence → no note (older rows).
"""

from __future__ import annotations

from dataraum.analysis.views.enrichment_agent import EnrichmentAgent


def _rel(coverage: float | None) -> dict:
    rel = {
        "from_table": "txn",
        "from_column": "_sk__customer_name__business_id",
        "to_table": "customer_table",
        "to_column": "_sk__customer_name__business_id",
        "cardinality": "many-to-one",
        "confidence": 0.85,
    }
    if coverage is not None:
        rel["coverage"] = coverage
    return rel


def test_low_coverage_is_rendered_next_to_the_grain_marker() -> None:
    agent = EnrichmentAgent.__new__(EnrichmentAgent)  # _format_* is self-contained
    out = agent._format_relationships([_rel(0.003)])
    assert "[GRAIN-SAFE]" in out
    assert "[matches 0.3% of fact rows]" in out


def test_missing_coverage_renders_no_note() -> None:
    agent = EnrichmentAgent.__new__(EnrichmentAgent)
    out = agent._format_relationships([_rel(None)])
    assert "matches" not in out
