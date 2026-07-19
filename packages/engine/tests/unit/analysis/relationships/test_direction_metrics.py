"""Unit tests for the per-side relationship measurement (DAT-725).

``_measure_direction`` is THE measurement, run once per endpoint, so every
``left_*`` metric has a real ``right_*`` mirror. That symmetry is load-bearing:
``db_models.swap_directional_evidence`` relabels these keys when a pair's
endpoints flip, which is only correct if the two sides answer the SAME question.
They did not before — the left number was row-weighted and the right one
distinct-weighted, so a flip renamed a coverage figure into a referential
-integrity slot and every consumer believed it.

These pin the mirror property itself, the row-vs-distinct divergence that makes
both weightings worth keeping, and the fan-out bias the old ``LEFT JOIN`` form
carried.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from dataraum.analysis.relationships.evaluator import _measure_direction, compute_ri_metrics


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(":memory:")
    try:
        yield c
    finally:
        c.close()


def _child_parent(conn: duckdb.DuckDBPyConnection) -> None:
    """A child with an orphan against a parent with an unreferenced key.

    child: 1, 2, 2, 99   (99 resolves nowhere)
    parent: 1, 2, 3      (3 is never referenced)
    """
    conn.execute("CREATE TABLE child AS SELECT * FROM (VALUES (1), (2), (2), (99)) AS v(fk)")
    conn.execute("CREATE TABLE parent AS SELECT * FROM (VALUES (1), (2), (3)) AS v(pk)")


def test_measures_the_same_question_on_both_sides(conn: duckdb.DuckDBPyConnection) -> None:
    _child_parent(conn)

    child_to_parent = _measure_direction("child", "fk", "parent", "pk", conn)
    parent_to_child = _measure_direction("parent", "pk", "child", "fk", conn)

    # 3 of the child's 4 rows resolve; 2 of the parent's 3 do.
    assert child_to_parent.referential_integrity == 75.0
    assert parent_to_child.referential_integrity == pytest.approx(66.67)
    # 2 of the child's 3 distinct keys exist in the parent; 2 of the parent's 3
    # exist in the child. Both are the same question on the value SETS.
    assert child_to_parent.key_coverage == pytest.approx(66.67)
    assert parent_to_child.key_coverage == pytest.approx(66.67)
    # Unresolved ROWS, per side — the count a flip must carry with it.
    assert (child_to_parent.orphan_count, child_to_parent.total_count) == (1, 4)
    assert (parent_to_child.orphan_count, parent_to_child.total_count) == (1, 3)


def test_row_and_distinct_weighting_diverge_on_duplicated_orphans(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Why both weightings are kept rather than one standing in for the other."""
    conn.execute("CREATE TABLE child AS SELECT 99 AS fk FROM range(9) UNION ALL SELECT 1")
    conn.execute("CREATE TABLE parent AS SELECT * FROM (VALUES (1), (2)) AS v(pk)")

    m = _measure_direction("child", "fk", "parent", "pk", conn)

    # One row in ten resolves, but one key set of two is contained. A consumer
    # asking "how broken is this join?" and one asking "is this a key subset?"
    # need different answers; collapsing them loses one of the two.
    assert m.referential_integrity == 10.0
    assert m.key_coverage == 50.0


def test_referential_integrity_is_not_inflated_by_the_other_sides_duplicates(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """The semi-join form, versus the LEFT JOIN + COUNT(*) it replaced.

    With duplicates on the TARGET side, a LEFT JOIN multiplies the source rows,
    so the "share of source rows that resolve" was computed over join output
    rows instead. Here the one resolving source row becomes three, and the
    orphan stays one: the old form reads 3/4 = 75%, the truth is 1/2 = 50%.
    """
    conn.execute("CREATE TABLE child AS SELECT * FROM (VALUES (1), (99)) AS v(fk)")
    conn.execute("CREATE TABLE parent AS SELECT 1 AS pk FROM range(3)")

    m = _measure_direction("child", "fk", "parent", "pk", conn)
    assert m.referential_integrity == 50.0
    assert (m.orphan_count, m.total_count) == (1, 2)

    fanned_out = conn.execute(
        """
        SELECT COUNT(*), COUNT(*) FILTER (WHERE p.pk IS NOT NULL)
        FROM child c LEFT JOIN parent p ON c.fk = p.pk WHERE c.fk IS NOT NULL
        """
    ).fetchone()
    assert fanned_out == (4, 3)  # the biased denominator the old form used


def test_nothing_measurable_is_none_not_zero(conn: duckdb.DuckDBPyConnection) -> None:
    """Absence is ignorance. A 0.0 here reads as "every row is broken" and
    scores 1.0 in ``relationship_entropy`` — on a side with no broken rows at
    all, because it has no rows at all."""
    conn.execute("CREATE TABLE parent AS SELECT * FROM (VALUES (1)) AS v(pk)")
    conn.execute("CREATE TABLE empty_child (fk INTEGER)")
    conn.execute("CREATE TABLE null_child AS SELECT CAST(NULL AS INTEGER) AS fk")

    for table in ("empty_child", "null_child"):
        assert _measure_direction(table, "fk", "parent", "pk", conn) is None, table
    # ...and an empty side leaves its metrics unset rather than at 0.
    assert _measure_direction("parent", "pk", "empty_child", "fk", conn) == (0.0, 0.0, 1, 1)


def test_castable_string_and_numeric_keys_still_measure(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """``EXISTS ... =`` coerces where ``IN`` refuses.

    DuckDB rejects cross-family comparison in an ``IN``/``ANY`` clause but
    coerces it under ``=``. An ``IN`` form raised here — fatal on the structural
    path, and silently all-``None`` evidence on the judge path, where the LLM
    may volunteer a VARCHAR-to-INTEGER pair the type gate never screened.
    """
    conn.execute("CREATE TABLE child AS SELECT * FROM (VALUES ('1'), ('2'), ('2')) AS v(fk)")
    conn.execute("CREATE TABLE parent AS SELECT * FROM (VALUES (1), (2), (3)) AS v(pk)")

    m = _measure_direction("child", "fk", "parent", "pk", conn)
    assert m is not None
    assert m.referential_integrity == 100.0
    assert m.key_coverage == 100.0


def test_missing_column_on_the_probed_table_fails_loudly(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """The subquery aliases its table and qualifies its column, so a column that
    is absent there cannot silently bind to the outer row.

    Unqualified, a ``to_column`` missing from ``to_table`` but present on
    ``from_table`` turned the semi-join into a self-comparison: a schema-drift
    error became fabricated "0% resolves" evidence, scored 1.0.
    """
    conn.execute("CREATE TABLE child AS SELECT * FROM (VALUES (1), (99)) AS v(shared)")
    conn.execute("CREATE TABLE parent AS SELECT * FROM (VALUES (1)) AS v(pk)")

    with pytest.raises(duckdb.BinderException):
        _measure_direction("child", "shared", "parent", "shared", conn)


def test_self_join_scopes_the_probe_to_the_inner_table(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """A self-referential FK (DAT-763) probes the same table under an alias."""
    conn.execute(
        "CREATE TABLE employee AS SELECT * FROM "
        "(VALUES (1, NULL), (2, 1), (3, 99)) AS v(employee_id, manager_id)"
    )

    m = _measure_direction("employee", "manager_id", "employee", "employee_id", conn)
    assert m is not None
    # Of the two non-NULL manager_ids, one (1) is an employee and one (99) is not.
    assert m.referential_integrity == 50.0
    assert (m.orphan_count, m.total_count) == (1, 2)


def test_qualified_lake_paths_and_awkward_names(conn: duckdb.DuckDBPyConnection) -> None:
    """Callers pass fully-qualified ``lake.typed."name"`` paths, not bare names."""
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE SCHEMA lake.typed")
    conn.execute('CREATE TABLE lake.typed."order items" AS SELECT * FROM (VALUES (1)) AS v(fk)')
    conn.execute('CREATE TABLE lake.typed."ref.data" AS SELECT * FROM (VALUES (1)) AS v(pk)')

    m = _measure_direction('lake.typed."order items"', "fk", 'lake.typed."ref.data"', "pk", conn)
    assert m is not None and m.referential_integrity == 100.0


def test_compute_ri_metrics_emits_every_metric_on_both_sides(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """The evidence contract: no per-side key may exist without its mirror.

    A key present on one side only cannot survive an endpoint flip — the reader
    looks for the prefixed name and finds nothing.
    """
    _child_parent(conn)

    metrics = compute_ri_metrics("child", "fk", "parent", "pk", conn)

    per_side = {k for k in metrics if k.startswith(("left_", "right_"))}
    assert {k.removeprefix("left_") for k in per_side if k.startswith("left_")} == {
        k.removeprefix("right_") for k in per_side if k.startswith("right_")
    }
    assert metrics["left_referential_integrity"] == 75.0
    assert metrics["right_orphan_count"] == 1
