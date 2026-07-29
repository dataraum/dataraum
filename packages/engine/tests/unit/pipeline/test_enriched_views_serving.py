"""The enriched-views phase serves no mint-owned surrogate columns (DAT-878).

Two independent cuts, pinned separately because they fail independently:

- ``_build_context_data`` never OFFERS a surrogate to the enrichment agent, and
- ``_served_join_payloads`` strips one from a join's ``include_columns`` even when
  it arrives anyway — via the inherit path, which replays a persisted view's
  ``exposed_dimension_joins`` verbatim, or via a model that named a column it was
  not shown.

The second exists because the view builder renames joined dimension columns to
``{fact_fk}__{col}``: a surrogate that survives becomes ``{fk}___sk__…``, which no
longer matches the prefix predicate and cannot be detected anywhere downstream.
"""

from __future__ import annotations

from uuid import uuid4

from dataraum.analysis.relationships.surrogate import SURROGATE_PREFIX
from dataraum.analysis.views.builder import DimensionJoin
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.enriched_views_phase import (
    EnrichedViewsPhase,
    _served_join_payloads,
)
from dataraum.storage import Column, Source, Table

SURROGATE_COL = f"{SURROGATE_PREFIX}account__business_id"


def _join(include: list[str]) -> DimensionJoin:
    return DimensionJoin(
        dim_table_name="accounts",
        dim_duckdb_path="lake.typed.accounts",
        fact_fk_column=SURROGATE_COL,
        dim_pk_column=SURROGATE_COL,
        include_columns=include,
        relationship_id="rel-1",
    )


class TestServedJoinPayloads:
    """The enforcement cut — payload filtered, legs untouched."""

    def test_surrogate_dropped_from_include_columns(self) -> None:
        pair = ("c-from", "c-to")

        out = _served_join_payloads([(_join(["name", SURROGATE_COL, "region"]), pair)])

        assert [j.include_columns for j, _ in out] == [["name", "region"]]

    def test_join_legs_are_never_filtered(self) -> None:
        """A cured composite's legs ARE the surrogate pair — filtering them would
        destroy the join itself, which is why only the payload is touched."""
        out = _served_join_payloads([(_join(["name"]), ("c-from", "c-to"))])

        join = out[0][0]
        assert join.fact_fk_column == SURROGATE_COL
        assert join.dim_pk_column == SURROGATE_COL

    def test_column_pair_and_order_survive(self) -> None:
        pairs = [("a", "b"), ("c", "d")]
        joins = [(_join(["x"]), pairs[0]), (_join([SURROGATE_COL, "y"]), pairs[1])]

        out = _served_join_payloads(joins)

        assert [p for _, p in out] == pairs
        assert [j.include_columns for j, _ in out] == [["x"], ["y"]]

    def test_inherited_spec_self_heals(self) -> None:
        """A warm workspace's persisted join, replayed verbatim by the inherit path.

        The cleaned spec is what gets persisted, so the next run stores a join with
        no surrogate — rather than re-creating the undetectable renamed form forever.
        """
        inherited = _join(["name", SURROGATE_COL])

        out = _served_join_payloads([(inherited, ("c-from", "c-to"))])

        assert SURROGATE_COL not in out[0][0].include_columns


def test_build_context_data_never_offers_a_surrogate(session, duckdb_conn) -> None:
    """The prevention cut: a surrogate is not in the agent's candidate columns.

    Seeded on the FACT table, where a surrogate is not near-unique and carries no
    annotation — so nothing else in the phase would filter it out.
    """
    src = Source(source_id=str(uuid4()), name="csv", source_type="csv")
    session.add(src)
    session.flush()
    fact = Table(
        table_id=str(uuid4()),
        source_id=src.source_id,
        table_name="ledger",
        layer="typed",
        duckdb_path="csv__ledger",
        row_count=10,
    )
    session.add(fact)
    session.flush()
    cols = [
        Column(
            column_id=str(uuid4()),
            table_id=fact.table_id,
            column_name="amount",
            column_position=0,
            resolved_type="DOUBLE",
        ),
        Column(
            column_id=str(uuid4()),
            table_id=fact.table_id,
            column_name=SURROGATE_COL,
            column_position=1,
            resolved_type="VARCHAR",
        ),
    ]
    session.add_all(cols)
    session.flush()

    data = EnrichedViewsPhase()._build_context_data(
        PhaseContext(session=session, duckdb_conn=duckdb_conn, table_ids=[fact.table_id]),
        [fact],
        [],
        [],
        {fact.table_id: cols},
        {fact.table_id: fact},
    )

    served = [c["column_name"] for c in data["tables"][0]["columns"]]
    assert served == ["amount"]


def test_served_tables_carry_names_only(session, duckdb_conn) -> None:
    """The enrichment agent is addressed by NAME, so it is served names (DAT-671).

    ``EnrichmentAnalysisOutput`` names tables and columns — it can neither use nor
    return a ``table_id``, a ``duckdb_path`` or a ``row_count``. Those went into the
    prompt anyway, once per table and once per column. The two the CALLER needs to
    turn a returned name back into a join now live in ``table_identity``, resolved
    after the answer instead of shipped inside the question.
    """
    src = Source(source_id=str(uuid4()), name="csv", source_type="csv")
    session.add(src)
    session.flush()
    fact = Table(
        table_id=str(uuid4()),
        source_id=src.source_id,
        table_name="ledger",
        layer="typed",
        duckdb_path="csv__ledger",
        row_count=10,
    )
    session.add(fact)
    session.flush()
    col = Column(
        column_id=str(uuid4()),
        table_id=fact.table_id,
        column_name="amount",
        column_position=0,
        resolved_type="DOUBLE",
    )
    session.add(col)
    session.flush()

    data = EnrichedViewsPhase()._build_context_data(
        PhaseContext(session=session, duckdb_conn=duckdb_conn, table_ids=[fact.table_id]),
        [fact],
        [],
        [],
        {fact.table_id: [col]},
        {fact.table_id: fact},
    )

    (served_table,) = data["tables"]
    assert set(served_table) == {"table_name", "is_fact_table", "columns"}
    assert set(served_table["columns"][0]) == {"column_name", "resolved_type"}
    # Resolution stays available to the caller, keyed by the name the model returns.
    assert data["table_identity"]["ledger"] == {
        "table_id": fact.table_id,
        "duckdb_path": "csv__ledger",
    }
