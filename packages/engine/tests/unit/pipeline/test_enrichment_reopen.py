"""Topology-only re-open for the sticky enrichment shape (DAT-791).

DAT-516 froze the enrichment judge's per-pair VERDICT; the retired DAT-699
dossier then re-opened a pair whenever cardinality|confidence|coverage
changed — but confidence is LLM-produced and coverage a measured float, so
run-to-run jitter on IDENTICAL topology re-invoked the judge and the shape
drifted (DAT-516's original symptom). The re-open basis is now structural
only: each considered entry carries the measured cardinality its verdict was
made on, and only a cardinality change (or an entry whose basis is unknown)
re-opens the pair. Identity of inputs → identity of shape.
"""

from __future__ import annotations

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.pipeline.phases.enriched_views_phase import _unchanged_considered_pairs
from dataraum.storage.base import load_all_models

# Instantiating ``Relationship`` configures the whole mapper registry; resolve
# every cross-module relationship() target so the file runs standalone too.
load_all_models()


def _rel(
    cardinality: str | None = "many-to-one",
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


def test_same_topology_stays_considered() -> None:
    rel = _rel()
    entries = [["ca", "cb", rel.cardinality]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): rel}) == {("ca", "cb")}


def test_confidence_and_coverage_jitter_never_reopen() -> None:
    """THE DAT-791 pin: judged at confidence 0.9 / coverage 0.85; a later run
    re-measures 0.42 / 0.003 on the SAME topology — the verdict sticks, the
    judge is not re-invoked, the shape does not drift."""
    entries = [["ca", "cb", "many-to-one"]]
    current = {("ca", "cb"): _rel(confidence=0.42, coverage=0.003)}

    assert _unchanged_considered_pairs(entries, current) == {("ca", "cb")}


def test_cardinality_flip_reopens_the_pair() -> None:
    """Judged under many-to-one; the data now measures one-to-many — the
    grain-safety premise the judge saw flipped, so the pair is undecided again."""
    entries = [["ca", "cb", "many-to-one"]]
    current = {("ca", "cb"): _rel(cardinality="one-to-many")}

    assert _unchanged_considered_pairs(entries, current) == set()


def test_unmeasured_cardinality_matches_unmeasured() -> None:
    """A pair judged with no cardinality measurement (stored ``None``) sticks
    while the measurement stays absent — ``None`` is a real basis, not unknown."""
    entries = [["ca", "cb", None]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): _rel(cardinality=None)}) == {
        ("ca", "cb")
    }


def test_newly_measured_cardinality_reopens_once() -> None:
    """Judged before cardinality was measured (basis ``None``); a later run
    measures it — the structural basis changed, re-ask the judge exactly once."""
    entries = [["ca", "cb", None]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): _rel()}) == set()


def test_entry_without_basis_reopens_conservatively() -> None:
    """A 2-element pre-DAT-699 entry carries no basis at all — the topology the
    judge saw is unknown, so the pair re-opens once and sticks under the new
    entry shape. Distinct from ``None`` (a known unmeasured basis)."""
    entries = [["ca", "cb"]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): _rel(cardinality=None)}) == set()


def test_legacy_dossier_hash_reopens_once() -> None:
    """A retired DAT-699 entry stores a sha16 dossier hash in the third slot —
    it matches no cardinality string, so the pair re-opens once and then sticks
    under the cardinality basis."""
    entries = [["ca", "cb", "8f1f0c8f2f3a4b5c"]]

    assert _unchanged_considered_pairs(entries, {("ca", "cb"): _rel()}) == set()


def test_pair_absent_from_catalog_stays_considered() -> None:
    """Nothing to re-judge; the Layer-A prune owns real drop+re-adds."""
    entries = [["ca", "cb", "many-to-one"]]

    assert _unchanged_considered_pairs(entries, {}) == {("ca", "cb")}
