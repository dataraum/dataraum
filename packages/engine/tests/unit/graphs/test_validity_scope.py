"""Default composition of the canonical validity scope (DAT-733).

Pure tests over the resolver + composer: no LLM, no DB container. The bypass
branch uses a real in-memory DuckDB so the where-constraint detection runs its
actual catalog-free parse (not a lexical coincidence).
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.context import BusinessCycleContext, EnrichedViewContext, TableContext
from dataraum.graphs.models import (
    ExtractGroundingOutput,
    GraphAssumptionOutput,
    GraphProvenanceOutput,
)
from dataraum.graphs.validity_scope import (
    ValidityScope,
    compose_scoped_where,
    resolve_validity_scopes,
)


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _cycle(
    *,
    status_table: str | None = "journal",
    status_column: str | None = "status",
    completion_value: str | None = "posted",
    completion_rate: float | None = 0.9,
) -> BusinessCycleContext:
    return BusinessCycleContext(
        cycle_name="Posting Cycle",
        cycle_type="posting",
        status_table=status_table,
        status_column=status_column,
        completion_value=completion_value,
        completion_rate=completion_rate,
    )


def _output(*, relation: str = "journal", where: list[str] | None = None) -> ExtractGroundingOutput:
    return ExtractGroundingOutput(
        grounding="evidence",
        relation=relation,
        where=where or [],
        select_expr="SUM(amount)",
        description="d",
        provenance=GraphProvenanceOutput(column_mappings_basis=[]),
        assumptions=[],
    )


# --- ValidityScope.render ---------------------------------------------------


def test_render_is_a_bare_column_equality() -> None:
    assert ValidityScope("status", "=", "posted").render() == "status = 'posted'"


def test_render_doubles_single_quotes() -> None:
    assert ValidityScope("owner", "=", "O'Brien").render() == "owner = 'O''Brien'"


# --- resolve_validity_scopes ------------------------------------------------


def test_measured_cycle_on_typed_relation_yields_the_scope() -> None:
    scopes = resolve_validity_scopes([_cycle()], "journal", {"amount", "status"}, [], [])
    assert scopes == [ValidityScope("status", "=", "posted")]


def test_enriched_view_over_the_status_table_resolves_through_its_fact() -> None:
    view = EnrichedViewContext(view_name="enriched_journal", fact_table="journal")
    scopes = resolve_validity_scopes(
        [_cycle()], "enriched_journal", {"amount", "status"}, [view], []
    )
    assert scopes == [ValidityScope("status", "=", "posted")]


def test_status_table_matches_either_name_form() -> None:
    """The cycle's status_table matches the relation via EITHER name form — the
    Python mirror of the element view's ``table_name OR duckdb_path`` match."""
    tables = [TableContext(table_id="t1", table_name="journal", duckdb_name="journal_raw")]
    # The cycle stored the duckdb-name form; the served relation is the table_name form.
    scopes = resolve_validity_scopes(
        [_cycle(status_table="journal_raw")], "journal", {"amount", "status"}, [], tables
    )
    assert scopes == [ValidityScope("status", "=", "posted")]


def test_duplicate_scopes_collapse_to_one() -> None:
    """Two cycles sharing a status column + value append ONE predicate, not two."""
    scopes = resolve_validity_scopes([_cycle(), _cycle()], "journal", {"amount", "status"}, [], [])
    assert scopes == [ValidityScope("status", "=", "posted")]


def test_unmeasured_cycle_contributes_no_filter() -> None:
    scopes = resolve_validity_scopes(
        [_cycle(completion_rate=None)], "journal", {"amount", "status"}, [], []
    )
    assert scopes == []


def test_relation_not_over_the_status_table_gets_no_scope() -> None:
    # A different served relation (its own fact) — the journal status must not leak.
    scopes = resolve_validity_scopes([_cycle()], "ledger", {"amount", "status"}, [], [])
    assert scopes == []


def test_status_column_absent_from_the_relation_gets_no_scope() -> None:
    # honest presence test — never append a column the relation does not serve.
    scopes = resolve_validity_scopes([_cycle()], "journal", {"amount"}, [], [])
    assert scopes == []


def test_missing_completion_value_gets_no_scope() -> None:
    scopes = resolve_validity_scopes(
        [_cycle(completion_value=None)], "journal", {"amount", "status"}, [], []
    )
    assert scopes == []


# --- compose_scoped_where: the two branches ---------------------------------


def test_default_branch_appends_the_scope_when_the_llm_omits_it(conn) -> None:
    """The LLM's where has no status constraint → the engine appends it, no assumption."""
    where_parts, assumptions = compose_scoped_where(
        _output(where=["amount > 0"]),
        "journal",
        {"amount", "status"},
        [_cycle()],
        [],
        [],
        conn,
    )
    assert where_parts == ["amount > 0", "status = 'posted'"]
    assert assumptions == []


def test_bypass_branch_defers_and_records_a_typed_assumption(conn) -> None:
    """The LLM already constrains status → the scope defers, recorded VISIBLY."""
    where_parts, assumptions = compose_scoped_where(
        _output(where=["status = 'draft'"]),
        "journal",
        {"amount", "status"},
        [_cycle()],
        [],
        [],
        conn,
    )
    # Not double-filtered: the engine did not append its own status predicate.
    assert where_parts == ["status = 'draft'"]
    assert len(assumptions) == 1
    only = assumptions[0]
    assert isinstance(only, GraphAssumptionOutput)
    assert only.dimension == "scope.validity"
    assert only.target == "column:journal.status"
    assert only.basis.value == "inferred"
    assert "not applied" in only.assumption


def test_string_literal_status_token_does_not_trigger_a_false_bypass(conn) -> None:
    """A status-shaped token inside a string literal is NOT a status constraint (the
    parse excludes literals) → the scope is still applied, no bypass assumption."""
    where_parts, assumptions = compose_scoped_where(
        _output(where=["description ILIKE '%status%'"]),
        "journal",
        {"amount", "status", "description"},
        [_cycle()],
        [],
        [],
        conn,
    )
    assert "status = 'posted'" in where_parts
    assert assumptions == []


def test_unparseable_fragment_does_not_justify_a_bypass(conn) -> None:
    """An unparseable where fragment must never let a lexical status token justify
    dropping the scope — the engine appends the scope conservatively (finding-2)."""
    where_parts, assumptions = compose_scoped_where(
        _output(where=["status = "]),  # trailing operator → validator parse fails
        "journal",
        {"amount", "status"},
        [_cycle()],
        [],
        [],
        conn,
    )
    assert "status = 'posted'" in where_parts
    assert assumptions == []


def test_no_connection_never_bypasses() -> None:
    """No DuckDB parser ⇒ no confident constraint ⇒ the scope is applied, never skipped."""
    where_parts, assumptions = compose_scoped_where(
        _output(where=["status = 'draft'"]),
        "journal",
        {"amount", "status"},
        [_cycle()],
        [],
        [],
        None,
    )
    assert "status = 'posted'" in where_parts
    assert assumptions == []


def test_fall_loud_relation_leaves_where_untouched(conn) -> None:
    where_parts, assumptions = compose_scoped_where(
        _output(where=["amount > 0"]), None, set(), [_cycle()], [], [], conn
    )
    assert where_parts == ["amount > 0"]
    assert assumptions == []


def test_no_applicable_cycle_leaves_where_untouched(conn) -> None:
    where_parts, assumptions = compose_scoped_where(
        _output(where=["amount > 0"]),
        "journal",
        {"amount", "status"},
        [_cycle(completion_rate=None)],
        [],
        [],
        conn,
    )
    assert where_parts == ["amount > 0"]
    assert assumptions == []


def test_composed_sql_and_persisted_parts_carry_the_posted_only_scope(conn) -> None:
    """The end of the deterministic path: both the rendered SQL AND the persisted
    clause parts (the where[] substrate og_grounding / current_groundings read)
    carry the default posted-only scope — the composed-SQL acceptance evidence."""
    from dataraum.graphs.formula_composer import compose_extract_sql, extract_parts_dict

    output = _output(where=["amount > 0"])
    where_parts, _ = compose_scoped_where(
        output, "journal", {"amount", "status"}, [_cycle()], [], [], conn
    )
    sql = compose_extract_sql(output.select_expr, "journal", where_parts)
    assert "status = 'posted'" in sql
    parts = extract_parts_dict(output.select_expr, "journal", where_parts)
    assert "status = 'posted'" in parts["where"]
