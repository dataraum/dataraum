"""Operating-model property graph over real Postgres 19 (ADR-0021, DAT-726).

Seeds one controlled, fully-promoted workspace (deterministic — no pipeline, no
LLM), materializes the read views + the property graph exactly as the engine
bootstrap does, then exercises the P1 acceptance criteria on the live SQL/PGQ
engine:

* the graph binds over the ``current_*`` views (element views + CREATE PROPERTY
  GRAPH succeed);
* a PGQ ``MATCH`` returns correct rows — every measure column → its stock/flow
  materialization;
* ``derived_from`` edges are enumerable (enriched view → fact + dim bases);
* the two-mechanism query model is de-risked: a bounded recursive-CTE closure over
  the ``refs`` edge view returns the correct transitive reachability and terminates,
  and the fixed-depth PGQ unroll agrees at the same depth;
* the drop-before-refresh bootstrap is idempotent across a re-boot.

These are the live half of ADR-0021; the pure DDL-shape tests live in
``tests/unit/storage/test_property_graph.py``.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import Engine, text

from dataraum.server.workspace import schema_name_for
from dataraum.storage.property_graph import (
    PROPERTY_GRAPH_NAME,
    drop_property_graph,
    materialize_property_graph,
)
from dataraum.storage.read_views import materialize_read_schema, read_schema_name_for

RUN = "00000000-0000-0000-0000-000000000001"  # the autofill baseline run_id
SRC = "00000000-0000-0000-0000-000000000002"  # the baseline Source seeded by the fixture
TS = "2026-01-01 00:00:00"

# A 5-table snowflake chain t1→t2→t3→t4→t5 (4 refs edges) — the reachability the
# recursive-CTE closure walks — plus an enriched-view table over (t1 fact, t2 dim).
_TABLES = [
    ("t1", "journal"),
    ("t2", "accounts"),
    ("t3", "account_group"),
    ("t4", "statement"),
    ("t5", "root"),
    ("t_enr", "journal_enriched"),
]
_COLUMNS = [
    ("c_amt", "t1", "amount", 1),
    ("c_k1", "t1", "account_id", 2),
    ("c_k2", "t2", "account_id", 1),
    ("c_k3", "t3", "group_id", 1),
    ("c_k4", "t4", "statement_id", 1),
    ("c_k5", "t5", "root_id", 1),
]
_REFS = [
    ("r1", "t1", "c_k1", "t2", "c_k2"),
    ("r2", "t2", "c_k2", "t3", "c_k3"),
    ("r3", "t3", "c_k3", "t4", "c_k4"),
    ("r4", "t4", "c_k4", "t5", "c_k5"),
]


def _seed(engine: Engine) -> None:
    """Insert a fully head-promoted metadata state into the workspace schema.

    The connection's search_path is the workspace schema (fixture listener), so
    unqualified inserts land there. Every run-versioned row carries the baseline
    ``run_id``; a per-table ``generation`` head + one ``catalog`` head promote them
    so the ``current_*`` views (and the graph's element views) resolve them.
    """
    stmts: list[str] = []
    for tid, name in _TABLES:
        stmts.append(
            f"INSERT INTO tables (table_id, source_id, table_name, layer, created_at) "
            f"VALUES ('{tid}', '{SRC}', '{name}', 'typed', '{TS}')"
        )
        stmts.append(
            f"INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at) "
            f"VALUES ('h_{tid}', 'table:{tid}', 'generation', '{RUN}', '{TS}')"
        )
    stmts.append(
        f"INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at) "
        f"VALUES ('h_cat', 'catalog', 'catalog', '{RUN}', '{TS}')"
    )
    for cid, tid, name, pos in _COLUMNS:
        stmts.append(
            f"INSERT INTO columns (column_id, table_id, column_name, column_position) "
            f"VALUES ('{cid}', '{tid}', '{name}', {pos})"
        )
    # has_role: amount is a measure, account_id a key.
    for cid, role in [("c_amt", "measure"), ("c_k1", "key")]:
        stmts.append(
            f"INSERT INTO semantic_annotations "
            f"(annotation_id, column_id, run_id, semantic_role, annotated_at) "
            f"VALUES ('sa_{cid}', '{cid}', '{RUN}', '{role}', '{TS}')"
        )
    # materializes_as: the witness pattern (flow) wins over the concept claim (stock)
    # via COALESCE(pattern, temporal_behavior) — exercises the LEFT JOIN precedence.
    stmts.append(
        "INSERT INTO column_concepts (concept_id, column_id, run_id, temporal_behavior, annotated_at) "
        f"VALUES ('cc_amt', 'c_amt', '{RUN}', 'stock', '{TS}')"
    )
    stmts.append(
        "INSERT INTO measure_aggregation_lineage "
        "(lineage_id, run_id, measure_table_id, measure_column_id, event_table_id, "
        " slice_dimension, convention_sql, period_grain, pattern, match_rate, "
        " r_flow_median, r_stock_median, n_entities, n_entities_fired, created_at) "
        f"VALUES ('mal_amt', '{RUN}', 't1', 'c_amt', 't1', 'month', 'SUM(amount)', "
        f"'month', 'flow', 1.0, 0.9, 0.1, 10, 10, '{TS}')"
    )
    for rid, ft, fc, tt, tc in _REFS:
        stmts.append(
            "INSERT INTO relationships "
            "(relationship_id, run_id, from_table_id, from_column_id, to_table_id, "
            " to_column_id, relationship_type, cardinality, confidence, is_confirmed, detected_at) "
            f"VALUES ('{rid}', '{RUN}', '{ft}', '{fc}', '{tt}', '{tc}', "
            f"'foreign_key', 'many_to_one', 0.9, true, '{TS}')"
        )
    # has_dimension: journal's account_id slice.
    stmts.append(
        "INSERT INTO slice_definitions "
        "(slice_id, run_id, table_id, column_id, column_name, slice_priority, "
        " slice_type, detection_source, created_at) "
        f"VALUES ('sl_1', '{RUN}', 't1', 'c_k1', 'account_id', 1, 'categorical', 'llm', '{TS}')"
    )
    for eid, tid, fact, dim in [("e_j", "t1", "true", "false"), ("e_a", "t2", "false", "true")]:
        stmts.append(
            "INSERT INTO table_entities "
            "(entity_id, table_id, run_id, detected_entity_type, is_fact_table, "
            " is_dimension_table, detected_at) "
            f"VALUES ('{eid}', '{tid}', '{RUN}', 'entity', {fact}, {dim}, '{TS}')"
        )
    # derived_from: journal_enriched view over the journal fact + the accounts dim.
    stmts.append(
        "INSERT INTO enriched_views "
        "(view_id, fact_table_id, view_table_id, view_name, run_id, "
        " dimension_table_ids, is_grain_verified, created_at) "
        f"VALUES ('v_1', 't1', 't_enr', 'journal_enriched', '{RUN}', "
        f"'[\"t2\"]'::json, true, '{TS}')"
    )
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


def _boot(engine: Engine, schema: str) -> None:
    """Run the bootstrap graph sequence exactly as ConnectionManager does."""
    with engine.begin() as conn:
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)


@pytest.fixture
def graph_engine(integration_engine: Engine) -> Engine:
    """A seeded, promoted workspace with the read views + property graph live."""
    schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    _seed(integration_engine)
    _boot(integration_engine, schema)
    return integration_engine


def _read_schema() -> str:
    return read_schema_name_for(schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"]))


def _graph_ref() -> str:
    return f'"{_read_schema()}".{PROPERTY_GRAPH_NAME}'


def test_measure_column_matches_its_materialization(graph_engine: Engine) -> None:
    """The P1 AC MATCH: every measure column → its stock/flow materialization."""
    sql = (
        f"SELECT column_name, materialization FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS column_node WHERE c.semantic_role = 'measure') "
        "COLUMNS (c.column_name AS column_name, c.materialization AS materialization))"
    )
    with graph_engine.connect() as conn:
        rows = conn.execute(text(sql)).all()
    # amount is the only measure; the witness pattern 'flow' beats the 'stock' claim.
    assert rows == [("amount", "flow")]


def test_references_edges_match_the_fk_topology(graph_engine: Engine) -> None:
    """A 1-hop MATCH enumerates the detected FK edges table→table."""
    sql = (
        f"SELECT src, dst FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node)-[e IS refs]->(b IS table_node) "
        "COLUMNS (a.table_name AS src, b.table_name AS dst))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.src, r.dst) for r in conn.execute(text(sql))}
    assert rows == {
        ("journal", "accounts"),
        ("accounts", "account_group"),
        ("account_group", "statement"),
        ("statement", "root"),
    }


def test_derived_from_edges_enumerable(graph_engine: Engine) -> None:
    """derived_from: the enriched view resolves to its fact + dimension bases."""
    sql = (
        f"SELECT base, role FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (v IS table_node)-[e IS derived_from]->(base IS table_node) "
        "COLUMNS (base.table_name AS base, e.base_role AS role))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.base, r.role) for r in conn.execute(text(sql))}
    assert rows == {("journal", "fact"), ("accounts", "dimension")}


def test_has_dimension_edge(graph_engine: Engine) -> None:
    """has_dimension: a fact table points at its slice (dimension) columns."""
    sql = (
        f"SELECT tname, cname FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (t IS table_node)-[e IS has_dimension]->(c IS column_node) "
        "COLUMNS (t.table_name AS tname, c.column_name AS cname))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.tname, r.cname) for r in conn.execute(text(sql))}
    assert rows == {("journal", "account_id")}


def test_bootstrap_is_idempotent(graph_engine: Engine) -> None:
    """A second boot (drop → refresh views → recreate graph) does not error."""
    schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    _boot(graph_engine, schema)  # re-boot on top of the live graph
    with graph_engine.connect() as conn:
        n = conn.execute(
            text(
                f"SELECT count(*) FROM GRAPH_TABLE ({_graph_ref()} "
                "MATCH (a IS table_node)-[e IS refs]->(b IS table_node) "
                "COLUMNS (1 AS one))"
            )
        ).scalar_one()
    assert n == 4


# --- Query-model de-risk (ADR-0021): fixed-depth PGQ + bounded recursive CTE ---


def test_recursive_cte_closure_over_refs_edge(graph_engine: Engine) -> None:
    """Bounded recursive-CTE transitive closure over the refs edge view.

    The mechanism P4 (part_of ancestry) and P5 (calendar roll-up) inherit: walk a
    metadata edge to arbitrary-but-bounded depth with a cycle guard, and terminate.
    From t1 the full reachable set is {t2,t3,t4,t5} at depths 1–4.
    """
    read = _read_schema()
    sql = (
        "WITH RECURSIVE reach(src, dst, depth, path) AS ("
        f"  SELECT from_table_id, to_table_id, 1, ARRAY[from_table_id, to_table_id] "
        f'  FROM "{read}".og_references '
        "  UNION ALL "
        "  SELECT r.src, e.to_table_id, r.depth + 1, r.path || e.to_table_id "
        f'  FROM reach r JOIN "{read}".og_references e ON e.from_table_id = r.dst '
        "  WHERE r.depth < 4 AND NOT e.to_table_id = ANY(r.path)"  # depth bound + cycle guard
        ") SELECT dst, depth FROM reach WHERE src = 't1' ORDER BY depth"
    )
    with graph_engine.connect() as conn:
        rows = conn.execute(text(sql)).all()
    assert rows == [("t2", 1), ("t3", 2), ("t4", 3), ("t5", 4)]


def test_fixed_depth_unroll_agrees_at_depth_four(graph_engine: Engine) -> None:
    """The static 4-hop PGQ unroll reaches t1→t5 — the closure's depth-4 endpoint."""
    sql = (
        f"SELECT src, reached FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node)-[IS refs]->()-[IS refs]->()-[IS refs]->()-[IS refs]->(z IS table_node) "
        "COLUMNS (a.table_name AS src, z.table_name AS reached))"
    )
    with graph_engine.connect() as conn:
        rows = conn.execute(text(sql)).all()
    assert rows == [("journal", "root")]
