"""Operating-model property-graph DDL generation (ADR-0021).

Pure tests (no DB): they pin the generated artifact's shape, the element-view
coverage, and the drop-before-view ordering baked into the dump. The live
properties — the graph binding, PGQ ``MATCH`` correctness, recursive-CTE closure
— are exercised against real Postgres 19 in
``tests/integration/storage/test_property_graph.py``.
"""

from __future__ import annotations

from dataraum.storage.property_graph import (
    _ELEMENT_VIEWS,
    PROPERTY_GRAPH_NAME,
    READ_TOKEN,
    WS_TOKEN,
    dump_graph_ddl,
    graph_statements,
)


def test_graph_statements_cover_every_element_view_then_the_graph() -> None:
    """The generator emits each element view once, with the graph last."""
    names = [name for name, _ in graph_statements()]
    assert names == [*_ELEMENT_VIEWS, PROPERTY_GRAPH_NAME]


def test_element_views_and_graph_are_tokenized() -> None:
    """Every statement targets the read/ws tokens — never a literal schema."""
    for name, sql in graph_statements():
        assert READ_TOKEN in sql, name
        assert "ws_" not in sql, f"{name} leaks a concrete schema"


def test_graph_statement_binds_each_element_view_with_keys() -> None:
    """CREATE PROPERTY GRAPH references every og_ view and declares explicit keys."""
    graph_sql = dict(graph_statements())[PROPERTY_GRAPH_NAME]
    assert graph_sql.startswith(f"CREATE PROPERTY GRAPH {READ_TOKEN}.{PROPERTY_GRAPH_NAME}")
    for view in _ELEMENT_VIEWS:
        assert f"{READ_TOKEN}.{view}" in graph_sql, view
    # Views have no primary key, so vertex KEY + edge SOURCE/DESTINATION KEY are mandatory.
    assert "KEY (table_id) LABEL table_node" in graph_sql
    assert "KEY (column_id) LABEL column_node" in graph_sql
    # Twelve edges: refs, has_dimension, derived_from, concept_edge (DAT-729),
    # conformed_dimension (DAT-756), grounded_by + uses (DAT-727), the three
    # DAT-730 additions — temporal_coverage, rolls_up_to, period_rolls_up_to — and
    # the two DAT-731 additions — has_additivity, measured_in.
    assert graph_sql.count("SOURCE KEY") == 12
    assert graph_sql.count("DESTINATION KEY") == 12
    # The measure→materialization MATCH reads these vertex properties.
    assert "semantic_role, materialization" in graph_sql
    # The concept_edge edge binds concept → concept, carrying the predicate property.
    assert "KEY (concept_id) LABEL concept_node" in graph_sql
    assert "SOURCE KEY (from_concept_id) REFERENCES og_concepts (concept_id)" in graph_sql
    assert "DESTINATION KEY (to_concept_id) REFERENCES og_concepts (concept_id)" in graph_sql
    assert "LABEL concept_edge" in graph_sql
    assert "PROPERTIES (predicate, tolerance)" in graph_sql
    # The conformed_dimension edge binds fact → fact over the shared dim AXIS
    # (attribute grain — the alignable drill-across GROUP BY the SQL agents author).
    assert "SOURCE KEY (from_table_id) REFERENCES og_tables (table_id)" in graph_sql
    assert "LABEL conformed_dimension" in graph_sql
    assert "PROPERTIES (dimension_table_id, dimension_attribute)" in graph_sql
    # The grounding vertex (DAT-727) carries the round-trippable clause parts
    # plus the failed discriminator (a retained DAT-543 failure is a node too).
    assert "KEY (snippet_id) LABEL grounding_node" in graph_sql
    assert "relation, select_expr, where_predicates, description, failed" in graph_sql
    # grounded_by binds concept → grounding; uses binds grounding → column.
    assert "SOURCE KEY (concept_id) REFERENCES og_concepts (concept_id)" in graph_sql
    assert "DESTINATION KEY (snippet_id) REFERENCES og_grounding (snippet_id)" in graph_sql
    assert "LABEL grounded_by" in graph_sql
    assert "SOURCE KEY (snippet_id) REFERENCES og_grounding (snippet_id)" in graph_sql
    assert "DESTINATION KEY (column_id) REFERENCES og_columns (column_id)" in graph_sql
    assert "LABEL uses" in graph_sql
    assert "PROPERTIES (role)" in graph_sql
    # DAT-730 — the concept vertex carries the dimension-ordering fact.
    assert "PROPERTIES (concept_id, vertical, name, kind, ordering)" in graph_sql
    # DAT-730 — the constant period-grain ladder vertex.
    assert "KEY (grain) LABEL period_grain" in graph_sql
    assert "PROPERTIES (grain, ordinal, fiscal_year_start_month, calendar_source)" in graph_sql
    # DAT-730 — temporal_coverage binds table → column with the observed coverage facts.
    assert "SOURCE KEY (table_id) REFERENCES og_tables (table_id)" in graph_sql
    assert "LABEL temporal_coverage" in graph_sql
    assert "observed_grain, completeness_ratio" in graph_sql
    assert "last_period_complete" in graph_sql
    # DAT-730 — rolls_up_to binds column → column (dimension drill levels).
    assert "SOURCE KEY (from_column_id) REFERENCES og_columns (column_id)" in graph_sql
    assert "LABEL rolls_up_to" in graph_sql
    # DAT-730 — the calendar ladder binds grain → grain.
    assert "SOURCE KEY (from_grain) REFERENCES og_period_grain (grain)" in graph_sql
    assert "LABEL period_rolls_up_to" in graph_sql
    # DAT-731 — the additivity_verdict vertex + its two edges.
    assert "KEY (additivity_id) LABEL additivity_verdict" in graph_sql
    assert "PROPERTIES (additivity_id, target_kind, target_key, categorical_additive," in graph_sql
    # has_additivity binds concept → verdict; measured_in binds column → column.
    assert "DESTINATION KEY (additivity_id) REFERENCES og_additivity (additivity_id)" in graph_sql
    assert "LABEL has_additivity" in graph_sql
    assert "SOURCE KEY (measure_column_id) REFERENCES og_columns (column_id)" in graph_sql
    assert "DESTINATION KEY (unit_column_id) REFERENCES og_columns (column_id)" in graph_sql
    assert "LABEL measured_in" in graph_sql
    assert "PROPERTIES (unit_source, self_denominated)" in graph_sql


def test_dump_drops_graph_before_its_element_views() -> None:
    """Idempotent teardown respects the dependency order (graph depends on views)."""
    ddl = dump_graph_ddl()
    drop_graph = ddl.index(f"DROP PROPERTY GRAPH IF EXISTS {READ_TOKEN}.{PROPERTY_GRAPH_NAME}")
    first_drop_view = ddl.index(f"DROP VIEW IF EXISTS {READ_TOKEN}.{_ELEMENT_VIEWS[0]}")
    create_graph = ddl.index(f"CREATE PROPERTY GRAPH {READ_TOKEN}.{PROPERTY_GRAPH_NAME}")
    assert drop_graph < first_drop_view < create_graph
    assert WS_TOKEN in ddl and READ_TOKEN in ddl


def test_dump_is_deterministic() -> None:
    """The dump is stable — the CI drift gate diffs it byte-for-byte."""
    assert dump_graph_ddl() == dump_graph_ddl()
