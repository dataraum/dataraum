"""The enrichment feed renders measured join coverage (DAT-695).

A grain-safe key can still match almost nothing (a lookalike dimension) — the
judge must see the number next to the grain marker so it can decline a join
that would enrich nothing. No coverage in evidence → no note (older rows).
"""

from __future__ import annotations

from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import (
    EnrichmentAnalysisOutput,
    EnrichmentColumnOutput,
    MainDatasetOutput,
    RelatedTableJoinOutput,
)


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


def _output(related_table: str) -> EnrichmentAnalysisOutput:
    return EnrichmentAnalysisOutput(
        main_datasets=[
            MainDatasetOutput(
                table_name="orders",
                recommended_enrichments=[
                    RelatedTableJoinOutput(
                        related_table=related_table,
                        join_fact_column="customer_id",
                        join_related_column="id",
                        relationship_role="reference/lookup",
                        enrichment_columns=[
                            EnrichmentColumnOutput(column_name="country", enrichment_value="high")
                        ],
                        confidence=0.9,
                        reasoning="geography",
                    )
                ],
            )
        ]
    )


_IDENTITY = {
    "orders": {"table_id": "t-orders", "duckdb_path": "csv__orders"},
    "customers": {"table_id": "t-customers", "duckdb_path": "csv__customers"},
}


def test_returned_names_resolve_through_table_identity() -> None:
    """The model answers in NAMES; the caller resolves them (DAT-671).

    Ids and physical paths are no longer served in the prompt, so the conversion
    reads ``table_identity`` — not the served ``tables`` list — to turn the two
    names the model returned into a join it can build.
    """
    agent = EnrichmentAgent.__new__(EnrichmentAgent)

    result = agent._convert_output_to_result(
        _output("customers"), {"table_identity": _IDENTITY}, "model-x"
    )

    (rec,) = result.unwrap().recommendations
    assert rec.fact_table_id == "t-orders"
    (join,) = rec.dimension_joins
    assert join.dim_table_name == "customers"
    assert join.dim_duckdb_path == "csv__customers"
    assert join.include_columns == ["country"]


def test_a_fabricated_table_name_resolves_to_nothing_and_is_skipped() -> None:
    """A name outside the identity map is a table the model invented — dropped,
    never joined against a guessed path."""
    agent = EnrichmentAgent.__new__(EnrichmentAgent)

    result = agent._convert_output_to_result(
        _output("no_such_table"), {"table_identity": _IDENTITY}, "model-x"
    )

    assert result.unwrap().recommendations == []
