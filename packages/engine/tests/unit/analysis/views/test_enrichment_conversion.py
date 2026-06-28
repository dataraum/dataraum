"""Enrichment-agent assembly of the LLM's join decision (DAT-277).

The enrichment LLM DECIDES the join; the agent ASSEMBLES it into a DimensionJoin.
A composite decision (``additional_join_columns``) becomes a multi-column
``key_pairs``; an ordinary decision leaves ``key_pairs`` empty. The phase/builder
then emit the join verbatim — nothing downstream re-derives the key.
"""

from __future__ import annotations

from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import (
    DimensionEnrichmentOutput,
    EnrichmentAnalysisOutput,
    EnrichmentColumnOutput,
    JoinColumnPair,
    MainDatasetOutput,
)


def _ctx() -> dict:
    return {
        "tables": [
            {"table_name": "master_txn", "table_id": "t_fact", "duckdb_path": "master_txn",
             "columns": [{"column_name": "account"}, {"column_name": "business_id"}]},
            {"table_name": "coa", "table_id": "t_dim", "duckdb_path": "coa",
             "columns": [{"column_name": "account_name"}, {"column_name": "account_type"}]},
        ]
    }


def _output(additional: list[JoinColumnPair]) -> EnrichmentAnalysisOutput:
    return EnrichmentAnalysisOutput(
        summary="s",
        main_datasets=[
            MainDatasetOutput(
                table_name="master_txn",
                is_primary_fact=True,
                recommended_enrichments=[
                    DimensionEnrichmentOutput(
                        dimension_table="coa",
                        join_fact_column="account",
                        join_dimension_column="account_name",
                        additional_join_columns=additional,
                        dimension_type="reference",
                        enrichment_columns=[
                            EnrichmentColumnOutput(
                                column_name="account_type", enrichment_value="high", reasoning="r"
                            )
                        ],
                        confidence=0.9,
                        reasoning="r",
                    )
                ],
            )
        ],
    )


def _convert(output: EnrichmentAnalysisOutput):
    agent = EnrichmentAgent.__new__(EnrichmentAgent)  # method is self-contained
    return agent._convert_output_to_result(output, _ctx(), "m").unwrap()


def test_composite_decision_becomes_multi_column_key_pairs() -> None:
    """LLM emits additional_join_columns → DimensionJoin.key_pairs is the FULL key."""
    result = _convert(
        _output([JoinColumnPair(join_fact_column="business_id", join_dimension_column="business_id")])
    )
    join = result.recommendations[0].dimension_joins[0]
    assert join.key_pairs == [("account", "account_name"), ("business_id", "business_id")]
    assert join.fact_fk_column == "account"  # primary pair → column prefix


def test_plain_decision_leaves_key_pairs_empty() -> None:
    """No additional_join_columns → single-column join (key_pairs empty)."""
    result = _convert(_output([]))
    join = result.recommendations[0].dimension_joins[0]
    assert join.key_pairs == []
    assert join.fact_fk_column == "account"
    assert join.dim_pk_column == "account_name"
