"""Dossier-fingerprint re-offer for the sticky enrichment shape (DAT-699).

DAT-516 froze the enrichment judge's per-pair VERDICT, but the dossier the
verdict was made on isn't frozen: pairs judged before DAT-695 existed never
saw the coverage note, and inheritance meant they never would. Each considered
entry now carries the fingerprint of the evidence the judge saw; a pair whose
current dossier differs counts as undecided and is re-offered. Not a
threshold — any material evidence change re-opens the question.
"""

from __future__ import annotations

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.views.enrichment_agent import dossier_fingerprint
from dataraum.pipeline.phases.enriched_views_phase import (
    _dossier,
    _unchanged_considered_pairs,
)


def _rel(
    cardinality: str = "many-to-one",
    confidence: float = 0.9,
    coverage: float | None = 0.85,
) -> Relationship:
    return Relationship(
        run_id="r1",
        from_table_id="t1",
        from_column_id="ca",
        to_table_id="t2",
        to_column_id="cb",
        relationship_type="foreign_key",
        cardinality=cardinality,
        confidence=confidence,
        detection_method="llm",
        evidence={"coverage": coverage} if coverage is not None else {},
    )


def test_unchanged_dossier_stays_considered() -> None:
    rel = _rel()
    entries = [["ca", "cb", _dossier(rel)]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): rel}) == {("ca", "cb")}


def test_changed_dossier_reopens_the_pair() -> None:
    """Judged at coverage 0.85; the measurement now reads 0.003 — the verdict
    was made on a different dossier, so the pair is undecided again."""
    entries = [["ca", "cb", _dossier(_rel(coverage=0.85))]]
    current = {("ca", "cb"): _rel(coverage=0.003)}

    assert _unchanged_considered_pairs(entries, current) == set()


def test_new_evidence_field_reopens_the_pair() -> None:
    """The live case: judged before coverage existed (dossier had coverage=None);
    the mint now measures it — re-ask the judge exactly once."""
    entries = [["ca", "cb", _dossier(_rel(coverage=None))]]
    current = {("ca", "cb"): _rel(coverage=0.0027)}

    assert _unchanged_considered_pairs(entries, current) == set()


def test_entry_without_fingerprint_reopens_conservatively() -> None:
    """A pre-DAT-699 entry carries no fingerprint — the dossier the judge saw
    is unknown, so the pair re-opens once and sticks under the new shape."""
    entries = [["ca", "cb"]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): _rel()}) == set()


def test_pair_absent_from_catalog_stays_considered() -> None:
    """Nothing to re-judge; the Layer-A prune owns real drop+re-adds."""
    entries = [["ca", "cb", "whatever"]]

    assert _unchanged_considered_pairs(entries, {}) == {("ca", "cb")}


def test_fingerprint_mirrors_the_rendered_evidence_fields() -> None:
    """Cardinality, confidence and coverage each move the fingerprint; the
    identity fields (names/ids) do not participate — the pair key carries them."""
    base = dossier_fingerprint("many-to-one", 0.9, 0.85)
    assert dossier_fingerprint("one-to-one", 0.9, 0.85) != base
    assert dossier_fingerprint("many-to-one", 0.8, 0.85) != base
    assert dossier_fingerprint("many-to-one", 0.9, None) != base
    assert dossier_fingerprint("many-to-one", 0.9, 0.85) == base
