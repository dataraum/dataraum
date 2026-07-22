"""Persist path of the catalogue_semantics phase (DAT-823).

Covers the machinery the rebalance moved off ``semantic_per_table`` — the sole
ColumnConcept INSERT writer, the bounded coverage retry, the partial warning and
the DAT-768 zero-meaning gate — plus the new duties: the persisted
``meaning_status`` determination and the TableEntity business-reading UPDATE.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
from sqlalchemy import select

from dataraum.analysis.catalogue.models import (
    CatalogueSemanticsOutput,
    ColumnConceptOutput,
    TableReadingOutput,
)
from dataraum.analysis.catalogue.processor import (
    apply_table_readings,
    author_and_store_catalogue,
    persist_column_concepts,
)
from dataraum.analysis.semantic.db_models import ColumnConcept as ColumnConceptDB
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.core.models.base import Result
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id


def _table_with_columns(session, name: str, columns: list[str]) -> Table:
    src = Source(name=f"src_{name}", source_type="csv")
    session.add(src)
    session.flush()
    table = Table(source_id=src.source_id, table_name=name, layer="typed", row_count=10)
    session.add(table)
    session.flush()
    for pos, col in enumerate(columns):
        session.add(
            Column(
                table_id=table.table_id, column_name=col, column_position=pos, raw_type="VARCHAR"
            )
        )
    session.flush()
    return table


def _entity_stub(session, table: Table, run_id: str) -> TableEntity:
    """The structural stub semantic_per_table INSERTs — no business reading."""
    entity = TableEntity(
        run_id=run_id,
        table_id=table.table_id,
        detected_entity_type=None,
        description=None,
        grain_columns=["id"],
        table_role="fact",
        detection_source="llm",
    )
    session.add(entity)
    session.flush()
    return entity


def _cc(
    table: str,
    column: str,
    meaning: str,
    determination: str = "determined",
    **kw,
) -> ColumnConceptOutput:
    return ColumnConceptOutput(
        table_name=table,
        column_name=column,
        meaning=meaning,
        determination=determination,  # type: ignore[arg-type]
        unit_source_column=kw.get("unit_source_column", ""),
        derived_formula_hypothesis=kw.get("derived_formula_hypothesis", ""),
        derived_formula_confidence=kw.get("derived_formula_confidence", 0.0),
    )


def _reading(table: str, entity_type: str = "orders", description: str = "d") -> TableReadingOutput:
    return TableReadingOutput(table_name=table, entity_type=entity_type, description=description)


class TestPersistColumnConcepts:
    """The catalogue-grain authoring the catalogue agent owns (DAT-637/823)."""

    def test_persists_concept_unit_and_normalizes_formula(self, session) -> None:
        """meaning / status / unit source / derived-formula land on ColumnConcept.

        Whitespace-only hypotheses normalize to None so the detector's
        truthiness read ("no hypothesis → witness abstains") holds.
        """
        table = _table_with_columns(session, "orders", ["total", "discount"])
        concepts = [
            _cc(
                "orders",
                "total",
                "Order total including tax",
                unit_source_column="currency_code",
                derived_formula_hypothesis="subtotal + tax",
                derived_formula_confidence=0.85,
            ),
            _cc(
                "orders",
                "discount",
                "Per-order discount amount",
                determination="ambiguous",
                derived_formula_hypothesis="   ",
            ),
        ]

        result = persist_column_concepts(
            session, concepts, [table.table_id], annotated_by="m", run_id=baseline_run_id()
        )
        session.flush()

        assert result.resolved == 2
        assert result.emitted == 2
        assert result.dropped_unresolved == 0
        assert result.ambiguous == 1
        rows = {r.column_id: r for r in session.execute(select(ColumnConceptDB)).scalars()}
        cols = {c.column_name: c.column_id for c in session.execute(select(Column)).scalars()}
        total = rows[cols["total"]]
        assert total.meaning == "Order total including tax"
        assert total.meaning_status == "determined"
        assert total.unit_source_column == "currency_code"
        assert total.derived_formula_hypothesis == "subtotal + tax"
        assert total.derived_formula_confidence == 0.85
        # DAT-807: "" sentinels normalize back to NULL, or every `IS NOT NULL`
        # reader silently changes meaning.
        assert rows[cols["discount"]].derived_formula_hypothesis is None
        assert rows[cols["discount"]].unit_source_column is None
        assert rows[cols["discount"]].meaning_status == "ambiguous"

    def test_blank_meaning_carries_no_status(self, session) -> None:
        """No meaning → no status: a coverage gap must not be dressed as a judgment."""
        table = _table_with_columns(session, "orders", ["total"])
        result = persist_column_concepts(
            session,
            [_cc("orders", "total", "   ", determination="ambiguous")],
            [table.table_id],
            annotated_by="m",
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.with_meaning == 0
        assert result.ambiguous == 0
        row = session.execute(select(ColumnConceptDB)).scalars().one()
        assert row.meaning is None
        assert row.meaning_status is None

    def test_duplicate_column_concepts_collapse_to_one_row(self, session) -> None:
        """The agent can list the same column twice; the upsert batch must dedup.

        Two entries for one (table, column) share the (column_id, run_id) upsert
        key — without dedup Postgres raises CardinalityViolation. Last wins.
        """
        table = _table_with_columns(session, "orders", ["total"])
        concepts = [_cc("orders", "total", "gross"), _cc("orders", "total", "net")]

        result = persist_column_concepts(
            session, concepts, [table.table_id], annotated_by="m", run_id=baseline_run_id()
        )
        session.flush()

        assert result.resolved == 1  # collapsed
        assert result.emitted == 2  # both mentions counted as emitted
        rows = list(session.execute(select(ColumnConceptDB)).scalars())
        assert len(rows) == 1
        assert rows[0].meaning == "net"  # last mention wins

    def test_unresolvable_concept_dropped_and_counted(self, session) -> None:
        """DAT-768 path #2: a concept whose (table, column) name resolves to no
        column is dropped and the breakdown surfaces it — never indistinguishable
        from an empty emission."""
        table = _table_with_columns(session, "orders", ["total"])

        result = persist_column_concepts(
            session,
            [_cc("orders", "ghost", "phantom")],
            [table.table_id],
            annotated_by="m",
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.emitted == 1
        assert result.resolved == 0
        assert result.dropped_unresolved == 1
        assert list(session.execute(select(ColumnConceptDB)).scalars()) == []


class TestApplyTableReadings:
    def test_updates_the_stub_in_place(self, session) -> None:
        table = _table_with_columns(session, "orders", ["id"])
        entity = _entity_stub(session, table, baseline_run_id())

        applied, dropped = apply_table_readings(
            session,
            [_reading("orders", "customer orders", "Order lines.")],
            [table.table_id],
            run_id=baseline_run_id(),
        )
        session.flush()

        assert (applied, dropped) == (1, [])
        assert entity.detected_entity_type == "customer orders"
        assert entity.description == "Order lines."
        # The structural half is untouched — same row, same run.
        assert entity.table_role == "fact"
        assert entity.run_id == baseline_run_id()

    def test_unresolvable_reading_dropped_never_fabricated(self, session) -> None:
        """A reading naming a table with no stub (hallucinated name, or a table
        the structural turn did not classify) is dropped + counted — never
        fabricated into a row."""
        table = _table_with_columns(session, "orders", ["id"])
        _entity_stub(session, table, baseline_run_id())

        applied, dropped = apply_table_readings(
            session, [_reading("ghost_table")], [table.table_id], run_id=baseline_run_id()
        )
        session.flush()

        assert (applied, dropped) == (0, ["ghost_table"])
        entity = session.execute(select(TableEntity)).scalars().one()
        assert entity.detected_entity_type is None  # honest ignorance survives

    def test_update_is_run_scoped(self, session) -> None:
        """Only THIS run's stub is updated — a prior run's reading is never touched."""
        table = _table_with_columns(session, "orders", ["id"])
        prior = TableEntity(
            run_id="run-prior",
            table_id=table.table_id,
            detected_entity_type="stale reading",
            grain_columns=["id"],
            table_role="fact",
            detection_source="llm",
        )
        session.add(prior)
        _entity_stub(session, table, baseline_run_id())
        session.flush()

        applied, _dropped = apply_table_readings(
            session, [_reading("orders", "fresh")], [table.table_id], run_id=baseline_run_id()
        )
        session.flush()

        assert applied == 1
        by_run = {e.run_id: e for e in session.execute(select(TableEntity)).scalars()}
        assert by_run["run-prior"].detected_entity_type == "stale reading"
        assert by_run[baseline_run_id()].detected_entity_type == "fresh"


def _output(
    concepts: list[ColumnConceptOutput], readings: list[TableReadingOutput]
) -> Result[CatalogueSemanticsOutput]:
    return Result.ok(CatalogueSemanticsOutput(table_readings=readings, column_concepts=concepts))


def _agent(results: list[Result]) -> MagicMock:
    agent = MagicMock()
    agent.author = MagicMock(side_effect=results)
    agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
    return agent


def _duck() -> duckdb.DuckDBPyConnection:
    """Pass-through conn for the plumbed duckdb parameter — the agent is
    mocked here, so nothing ever queries it (the conditioned-sample SQL is
    covered by test_context.py against real tables)."""
    return duckdb.connect()


class TestAuthorAndStoreCatalogue:
    def test_full_coverage_single_call(self, session) -> None:
        table = _table_with_columns(session, "orders", ["id"])
        _entity_stub(session, table, baseline_run_id())
        agent = _agent([_output([_cc("orders", "id", "row key")], [_reading("orders")])])

        result = author_and_store_catalogue(
            session, _duck(), agent, [table.table_id], "general", run_id=baseline_run_id()
        )
        session.flush()

        assert result.success
        stats = result.unwrap()
        assert stats.authored_tables == 1
        assert stats.authored_columns == 1
        assert stats.missing == 0
        assert stats.dropped_unresolved == 0
        assert agent.author.call_count == 1
        entity = session.execute(select(TableEntity)).scalars().one()
        assert entity.detected_entity_type == "orders"

    def test_zero_meaningful_rows_fails_the_run(self, session) -> None:
        """DAT-768: an emptied load-bearing surface fails begin_session loud."""
        table = _table_with_columns(session, "orders", ["id"])
        _entity_stub(session, table, baseline_run_id())
        empty = _output([], [])
        agent = _agent([empty, empty, empty])  # initial + exhausted retries

        result = author_and_store_catalogue(
            session, _duck(), agent, [table.table_id], "general", run_id=baseline_run_id()
        )

        assert not result.success
        assert "zero meaningful rows" in (result.error or "")
        assert "DAT-768" in (result.error or "")

    def test_retry_is_scoped_to_missing_tables_and_merges(self, session) -> None:
        alpha = _table_with_columns(session, "alpha", ["a1", "a2"])
        beta = _table_with_columns(session, "beta", ["b1", "b2"])
        _entity_stub(session, alpha, baseline_run_id())
        _entity_stub(session, beta, baseline_run_id())
        agent = _agent(
            [
                # First (full-catalogue) call truncated: alpha covered, beta absent.
                _output(
                    [_cc("alpha", "a1", "m1"), _cc("alpha", "a2", "m2")],
                    [_reading("alpha"), _reading("beta")],
                ),
                # Scoped retry supplies beta.
                _output([_cc("beta", "b1", "m3"), _cc("beta", "b2", "m4")], []),
            ]
        )

        result = author_and_store_catalogue(
            session,
            _duck(),
            agent,
            [alpha.table_id, beta.table_id],
            "general",
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.success
        assert agent.author.call_count == 2
        retry_kwargs = agent.author.call_args_list[1].kwargs
        # Scoped to the uncovered table only; the session set still travels so
        # cross-table evidence keeps serving the chains.
        assert retry_kwargs["table_ids"] == [beta.table_id]
        assert set(retry_kwargs["session_table_ids"]) == {alpha.table_id, beta.table_id}
        rows = session.execute(select(ColumnConceptDB)).scalars().all()
        assert len(rows) == 4

    def test_missing_reading_triggers_retry_and_null_survives_exhaustion(self, session) -> None:
        """A table without a business reading is retried; after exhaustion the
        stub keeps its NULL entity_type — declared ignorance, warn-only."""
        alpha = _table_with_columns(session, "alpha", ["a1"])
        beta = _table_with_columns(session, "beta", ["b1"])
        _entity_stub(session, alpha, baseline_run_id())
        beta_entity = _entity_stub(session, beta, baseline_run_id())
        covered = _output([_cc("alpha", "a1", "m1"), _cc("beta", "b1", "m2")], [_reading("alpha")])
        agent = _agent([covered, _output([], []), _output([], [])])

        result = author_and_store_catalogue(
            session,
            _duck(),
            agent,
            [alpha.table_id, beta.table_id],
            "general",
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.success  # warn-only terminal state
        assert agent.author.call_count == 3  # initial + CONCEPT_COVERAGE_RETRIES
        retry_kwargs = agent.author.call_args_list[1].kwargs
        assert retry_kwargs["table_ids"] == [beta.table_id]  # reading gap drives scope
        assert beta_entity.detected_entity_type is None

    def test_retry_never_overwrites_the_first_emission(self, session) -> None:
        alpha = _table_with_columns(session, "alpha", ["a1", "a2"])
        _entity_stub(session, alpha, baseline_run_id())
        agent = _agent(
            [
                _output([_cc("alpha", "a1", "first")], [_reading("alpha", "first reading")]),
                # Retry re-emits a1 + the reading (already covered) alongside a2.
                _output(
                    [_cc("alpha", "a1", "second"), _cc("alpha", "a2", "filled")],
                    [_reading("alpha", "second reading")],
                ),
            ]
        )

        result = author_and_store_catalogue(
            session, _duck(), agent, [alpha.table_id], "general", run_id=baseline_run_id()
        )
        session.flush()

        assert result.success
        rows = {r.column_id: r for r in session.execute(select(ColumnConceptDB)).scalars()}
        cols = {c.column_name: c.column_id for c in session.execute(select(Column)).scalars()}
        assert rows[cols["a1"]].meaning == "first"
        assert rows[cols["a2"]].meaning == "filled"
        entity = session.execute(select(TableEntity)).scalars().one()
        assert entity.detected_entity_type == "first reading"

    def test_blank_meaning_counts_as_missing_and_is_refilled(self, session) -> None:
        """A whitespace-only meaning is absence by the persist contract, so
        coverage re-asks — and the meaningful re-emission wins at persist."""
        alpha = _table_with_columns(session, "alpha", ["a1", "a2"])
        _entity_stub(session, alpha, baseline_run_id())
        agent = _agent(
            [
                _output([_cc("alpha", "a1", "m1"), _cc("alpha", "a2", "   ")], [_reading("alpha")]),
                _output([_cc("alpha", "a2", "filled")], []),
            ]
        )

        result = author_and_store_catalogue(
            session, _duck(), agent, [alpha.table_id], "general", run_id=baseline_run_id()
        )
        session.flush()

        assert result.success
        assert agent.author.call_count == 2
        rows = {r.column_id: r for r in session.execute(select(ColumnConceptDB)).scalars()}
        cols = {c.column_name: c.column_id for c in session.execute(select(Column)).scalars()}
        assert rows[cols["a2"]].meaning == "filled"

    def test_failed_retry_is_best_effort(self, session) -> None:
        """A failing retry never fails the phase — the first pass stands."""
        alpha = _table_with_columns(session, "alpha", ["a1"])
        beta = _table_with_columns(session, "beta", ["b1"])
        _entity_stub(session, alpha, baseline_run_id())
        _entity_stub(session, beta, baseline_run_id())
        agent = _agent(
            [
                _output([_cc("alpha", "a1", "m1")], [_reading("alpha"), _reading("beta")]),
                Result.fail("LLM down"),
            ]
        )

        result = author_and_store_catalogue(
            session,
            _duck(),
            agent,
            [alpha.table_id, beta.table_id],
            "general",
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.success
        assert agent.author.call_count == 2  # stopped after the failure
        assert len(session.execute(select(ColumnConceptDB)).scalars().all()) == 1

    def test_propagates_agent_failure(self, session) -> None:
        agent = _agent([Result.fail("LLM down")])
        result = author_and_store_catalogue(
            session, _duck(), agent, ["t1"], "general", run_id=baseline_run_id()
        )
        assert not result.success
        assert "LLM down" in (result.error or "")

    def test_rerun_same_run_is_idempotent(self, session) -> None:
        """A Temporal at-least-once retry (same run_id) converges: the concept
        upsert refreshes in place and the reading UPDATE re-applies."""
        table = _table_with_columns(session, "orders", ["id"])
        _entity_stub(session, table, baseline_run_id())
        output = [_output([_cc("orders", "id", "row key")], [_reading("orders")])]
        for _ in range(2):
            agent = _agent(list(output))
            assert author_and_store_catalogue(
                session, _duck(), agent, [table.table_id], "general", run_id=baseline_run_id()
            ).success
            session.flush()

        assert len(session.execute(select(ColumnConceptDB)).scalars().all()) == 1
        assert len(session.execute(select(TableEntity)).scalars().all()) == 1
