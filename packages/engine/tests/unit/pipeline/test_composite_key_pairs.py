"""Composite-key orientation for enriched-view joins (DAT-277 B2b).

``_composite_key_pairs`` turns a stored composite relationship group into the
fact→dim ``(column_name, column_name)`` pairs the multi-column ON clause needs,
regardless of which way each component relationship was stored.
"""

from __future__ import annotations

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.pipeline.phases.enriched_views_phase import EnrichedViewsPhase
from dataraum.storage.base import load_all_models

# Constructing a mapped Relationship configures the ORM mappers, which reference
# models across modules (TemporalColumnProfile, …); register them all first.
load_all_models()


def _rel(frm_col: str, frm_tbl: str, to_col: str, to_tbl: str, position: int) -> Relationship:
    return Relationship(
        run_id="r",
        from_table_id=frm_tbl,
        from_column_id=frm_col,
        to_table_id=to_tbl,
        to_column_id=to_col,
        relationship_type="foreign_key",
        cardinality="many-to-one",
        relationship_group_id="g1",
        key_position=position,
        confidence=0.9,
        detection_method="llm",
    )


_NAMES = {
    "fa": "account",
    "fb": "business_id",
    "da": "account_name",
    "db": "business_id",
}
_TABLES = {"fa": "fact", "fb": "fact", "da": "dim", "db": "dim"}


def test_anchor_first_fact_to_dim_orientation() -> None:
    """Anchor (position 0) leads; each pair is oriented fact-column → dim-column."""
    # Anchor stored fact→dim, scope stored dim→fact (mixed orientation on purpose).
    group = [
        _rel("fa", "fact", "da", "dim", 0),
        _rel("db", "dim", "fb", "fact", 1),
    ]
    pairs = EnrichedViewsPhase._composite_key_pairs(group, "fact", _NAMES, _TABLES)
    assert pairs == [("account", "account_name"), ("business_id", "business_id")]


def test_unorientable_component_yields_empty() -> None:
    """A component touching neither side of the fact → [] (caller degrades to anchor)."""
    group = [
        _rel("fa", "fact", "da", "dim", 0),
        _rel("xx", "other", "yy", "elsewhere", 1),
    ]
    assert EnrichedViewsPhase._composite_key_pairs(group, "fact", _NAMES, _TABLES) == []
