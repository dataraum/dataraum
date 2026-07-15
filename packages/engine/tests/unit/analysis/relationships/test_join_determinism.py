"""DAT-794 regression: Layer-A candidate detection is deterministic.

Before DAT-794, pairs whose max distinct count fell in 10K–1M went through an
unseeded reservoir-sampling estimator: a subset FK whose true Jaccard sits
below ``min_score`` (child ⊂ parent, |child| ≪ |parent|) survived only through
the containment>=0.95 rescue, and the sampled containment estimate dropped it
in ~30% of runs (measured on the calibration corpus, 50 reps). With the
sampled band deleted, the same pair must be found by the exact algorithm,
identically, every run.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from dataraum.analysis.relationships.joins import find_join_columns

PARENT_DISTINCT = 15_000  # above the old 10K exact-computation ceiling
CHILD_DISTINCT = 4_000  # subset: true Jaccard 4000/15000 ≈ 0.27 < min_score 0.3


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(":memory:")
    c.execute(f"CREATE TABLE parent AS SELECT range AS pk FROM range({PARENT_DISTINCT})")
    c.execute(f"CREATE TABLE child AS SELECT range AS fk FROM range({CHILD_DISTINCT})")
    try:
        yield c
    finally:
        c.close()


def test_subset_fk_above_10k_detected_via_exact_containment(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    results = [
        find_join_columns(
            conn,
            "child",
            "parent",
            ["fk"],
            ["pk"],
            column_types1={"fk": "BIGINT"},
            column_types2={"pk": "BIGINT"},
        )
        for _ in range(5)
    ]

    first = results[0]
    assert len(first) == 1
    (candidate,) = first
    # Containment rescues the below-gate Jaccard (~0.27): child ⊂ parent → 1.0,
    # computed exactly (confidence 1.0), not estimated from a random sample.
    assert candidate["join_confidence"] == 1.0
    assert candidate["algorithm"] == "exact"
    assert candidate["statistical_confidence"] == 1.0
    # Deterministic: every repetition returns the identical candidate list.
    assert all(r == first for r in results[1:])


def test_dirty_subset_fk_rescued_at_fractional_containment(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    # A dirty FK: 2% of child values are orphans (absent from parent). True
    # containment 0.98 >= 0.95 must still produce the candidate — the
    # referential-integrity evaluator needs it to exist to quantify the
    # orphans. The old exact path required 100% containment and would drop
    # this pair deterministically; the old sampled band rescued it only
    # noisily. Jaccard stays below the gate (3920/(19000-3920) ≈ 0.26).
    orphans = int(CHILD_DISTINCT * 0.02)
    conn.execute(
        f"CREATE TABLE dirty_child AS SELECT range + {PARENT_DISTINCT} AS fk "
        f"FROM range({orphans}) UNION ALL "
        f"SELECT range AS fk FROM range({CHILD_DISTINCT - orphans})"
    )

    candidates = find_join_columns(
        conn,
        "dirty_child",
        "parent",
        ["fk"],
        ["pk"],
        column_types1={"fk": "BIGINT"},
        column_types2={"pk": "BIGINT"},
    )

    assert len(candidates) == 1
    (candidate,) = candidates
    # Score is the honest fractional containment, not a snapped 1.0.
    assert candidate["join_confidence"] == pytest.approx(0.98)
    assert candidate["algorithm"] == "exact"
    assert candidate["statistical_confidence"] == 1.0
