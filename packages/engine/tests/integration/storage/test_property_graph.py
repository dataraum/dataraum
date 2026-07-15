"""Operating-model property graph over real Postgres 19 (ADR-0021, DAT-726).

Seeds one controlled, fully-promoted workspace (deterministic — no pipeline, no
LLM), materializes the read views + the property graph exactly as the engine
bootstrap does, then exercises the P1 acceptance criteria on the live SQL/PGQ
engine:

* the graph binds over the ``current_*`` views (element views + CREATE PROPERTY
  GRAPH succeed);
* a PGQ ``MATCH`` returns correct rows — every measure column → its stock/flow
  materialization, seeded with the REAL pipeline vocabularies;
* ``derived_from`` edges are enumerable (enriched view → fact + dim bases);
* the ``cockpit_reader`` role can query the graph (a property graph needs its own
  GRANT — table grants don't reach GRAPH_TABLE);
* the two-mechanism query model is de-risked on a chain that OUTRUNS the depth cap
  and contains a cycle: the bounded recursive-CTE closure truncates at the cap
  (a node one hop past it stays unreached), the cycle guard fires (a back edge
  never re-enters its source) and the walk terminates, and the fixed-depth PGQ
  unroll agrees with the closure at the same depth;
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
    grant_reader_on_graph,
    materialize_property_graph,
)
from dataraum.storage.read_views import (
    READER_ROLE,
    ensure_reader_role,
    materialize_read_schema,
    read_schema_name_for,
)

RUN = "00000000-0000-0000-0000-000000000001"  # the autofill baseline run_id
SRC = "00000000-0000-0000-0000-000000000002"  # the baseline Source seeded by the fixture
TS = "2026-01-01 00:00:00"
READER_PW = "graph-reader-test-pw"

# A 6-table chain t1→t2→t3→t4→t5→t6 (LONGER than the depth-4 closure cap) plus a
# back edge t6→t4 (the cycle t4→t5→t6→t4). This lets the de-risk tests distinguish
# "hit the depth cap" (t6 exists but is unreachable from t1 within 4 hops) from
# "ran out of edges", and fire the cycle guard (from t4, t4 never re-enters).
# t_enr is the enriched-view table over (t1 fact, t2 dim).
_TABLES = [
    ("t1", "journal"),
    ("t2", "accounts"),
    ("t3", "account_group"),
    ("t4", "statement"),
    ("t5", "division"),
    ("t6", "root"),
    ("t_enr", "journal_enriched"),
]
_COLUMNS = [
    ("c_amt", "t1", "amount", 1),
    ("c_k1", "t1", "account_id", 2),
    # Two role-playing DATE axes on the journal fact (DAT-730): booking vs value date.
    ("c_bd", "t1", "booking_date", 3),
    ("c_vd", "t1", "value_date", 4),
    ("c_k2", "t2", "account_id", 1),
    # A geo drill-down over the accounts dim (DAT-730 rolls_up_to): city → state → country.
    ("c_city", "t2", "city", 2),
    ("c_state", "t2", "state", 3),
    ("c_country", "t2", "country", 4),
    ("c_k3", "t3", "group_id", 1),
    ("c_k3b", "t3", "account_id", 2),  # t3's own account_id — a cross-LEVEL accounts slice
    ("c_k4", "t4", "statement_id", 1),
    ("c_k4b", "t4", "account_id", 2),  # t4's own account_id — a 2nd fact→accounts slice
    ("c_k5", "t5", "division_id", 1),
    ("c_k6", "t6", "root_id", 1),
]
# (relationship_id, from_table, from_col, to_table, to_col)
_REFS = [
    ("r1", "t1", "c_k1", "t2", "c_k2"),
    ("r2", "t2", "c_k2", "t3", "c_k3"),
    ("r3", "t3", "c_k3", "t4", "c_k4"),
    ("r4", "t4", "c_k4", "t5", "c_k5"),
    ("r5", "t5", "c_k5", "t6", "c_k6"),
    ("r6", "t6", "c_k6", "t4", "c_k4"),  # back edge → cycle t4→t5→t6→t4
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
    # materializes_as: REAL vocabularies. The witness pattern 'per_period' (→ flow)
    # is the data-reconciled posterior and outranks the concept claim 'point_in_time'
    # (→ stock); the view normalizes both and the COALESCE prefers the posterior, so
    # materialization = 'flow'. (Neither raw value is ever 'flow'/'stock' in the DB.)
    stmts.append(
        "INSERT INTO column_concepts (concept_id, column_id, run_id, temporal_behavior, annotated_at) "
        f"VALUES ('cc_amt', 'c_amt', '{RUN}', 'point_in_time', '{TS}')"
    )
    stmts.append(
        "INSERT INTO measure_aggregation_lineage "
        "(lineage_id, run_id, measure_table_id, measure_column_id, event_table_id, "
        " slice_dimension, convention_sql, period_grain, pattern, match_rate, "
        " r_flow_median, r_stock_median, n_entities, n_entities_fired, created_at) "
        f"VALUES ('mal_amt', '{RUN}', 't1', 'c_amt', 't1', 'month', 'SUM(amount)', "
        f"'month', 'per_period', 1.0, 0.9, 0.1, 10, 10, '{TS}')"
    )
    for rid, ft, fc, tt, tc in _REFS:
        stmts.append(
            "INSERT INTO relationships "
            "(relationship_id, run_id, from_table_id, from_column_id, to_table_id, "
            " to_column_id, relationship_type, cardinality, confidence, is_confirmed, detected_at) "
            f"VALUES ('{rid}', '{RUN}', '{ft}', '{fc}', '{tt}', '{tc}', "
            f"'foreign_key', 'many_to_one', 0.9, true, '{TS}')"
        )
    # has_dimension: three facts each sliced by their own account_id FK, all resolving
    # the referenced identity dimension_table_id='t2' (the accounts dim). journal (t1)
    # and statement (t4) slice the SAME attribute (account_type) → a CONFORMED pair
    # (an alignable drill-across axis). account_group (t3) slices a DIFFERENT attribute
    # (region) → shares the dim TABLE but NOT the axis, so it does NOT conform (the
    # conformed edge is ATTRIBUTE grain). fk_role is the FK column name, carried but not
    # a Phase-A identity key.
    for sid, tid, cid, colname, attr in [
        ("sl_1", "t1", "c_k1", "account_id__account_type", "account_type"),
        ("sl_2", "t4", "c_k4b", "account_id__account_type", "account_type"),
        ("sl_3", "t3", "c_k3b", "account_id__region", "region"),
    ]:
        stmts.append(
            "INSERT INTO slice_definitions "
            "(slice_id, run_id, table_id, column_id, column_name, dimension_table_id, "
            " dimension_attribute, fk_role, slice_priority, slice_type, detection_source, created_at) "
            f"VALUES ('{sid}', '{RUN}', '{tid}', '{cid}', '{colname}', 't2', "
            f"'{attr}', 'account_id', 1, 'categorical', 'llm', '{TS}')"
        )
    # SPURIOUS fact↔fact relationships between account_id slice columns (the DAT-723
    # fan trap): both endpoints resolve dimension_table_id='t2', so each is a conformed
    # dimension, NOT an FK — og_references must drop BOTH (table grain, regardless of
    # slice attribute). r_fan links the same-attribute pair (t1↔t4, which DOES get a
    # conformed edge); r_fan_xlevel links the cross-level pair (t1↔t3, which does NOT —
    # proving the exclusion and the edge are decoupled: excluded yet unedged is correct).
    for rid, ft, fc, tt, tc in [
        ("r_fan", "t1", "c_k1", "t4", "c_k4b"),
        ("r_fan_xlevel", "t1", "c_k1", "t3", "c_k3b"),
    ]:
        stmts.append(
            "INSERT INTO relationships "
            "(relationship_id, run_id, from_table_id, from_column_id, to_table_id, "
            " to_column_id, relationship_type, cardinality, confidence, is_confirmed, detected_at) "
            f"VALUES ('{rid}', '{RUN}', '{ft}', '{fc}', '{tt}', '{tc}', "
            f"'foreign_key', 'many_to_one', 0.9, true, '{TS}')"
        )
    # table_entities. journal (e_j) carries its two event-time axes (DAT-565/730):
    # booking_date FIRST (the primary axis → the measure anchor), value_date second.
    # accounts (e_a) is a dimension with no time axes.
    time_cols_json = (
        '[{"column": "booking_date", "aspect": "booking", "note": "booked"}, '
        '{"column": "value_date", "aspect": "value", "note": "settled"}]'
    )
    for eid, tid, role, tcols in [
        ("e_j", "t1", "fact", time_cols_json),
        ("e_a", "t2", "dimension", None),
    ]:
        tcols_sql = f"'{tcols}'::json" if tcols else "NULL"
        stmts.append(
            "INSERT INTO table_entities "
            "(entity_id, table_id, run_id, detected_entity_type, table_role, time_columns, "
            " detected_at) "
            f"VALUES ('{eid}', '{tid}', '{RUN}', 'entity', '{role}', {tcols_sql}, '{TS}')"
        )
    # derived_from: journal_enriched view over the journal fact + the accounts dim.
    stmts.append(
        "INSERT INTO enriched_views "
        "(view_id, fact_table_id, view_table_id, view_name, run_id, "
        " dimension_table_ids, is_grain_verified, created_at) "
        f"VALUES ('v_1', 't1', 't_enr', 'journal_enriched', '{RUN}', "
        f"'[\"t2\"]'::json, true, '{TS}')"
    )
    # Concept vertices + a disjoint_with concept edge (DAT-729): the vocabulary graph.
    # Concepts/edges are workspace-persistent (NOT run-versioned) — plain active rows,
    # no head to promote. The og_concept_edges view resolves the edge's (vertical, name)
    # endpoints to these concepts' ids for the concept→concept PGQ binding.
    for cid, cname in [("con_ap", "accounts_payable"), ("con_ar", "accounts_receivable")]:
        stmts.append(
            "INSERT INTO concepts (concept_id, vertical, name, kind, created_at) "
            f"VALUES ('{cid}', 'finance', '{cname}', 'measure', '{TS}')"
        )
    # disjoint_with is symmetric → both directions, so a directed MATCH finds it either way.
    for eid, frm, to in [
        ("ce_1", "accounts_payable", "accounts_receivable"),
        ("ce_2", "accounts_receivable", "accounts_payable"),
    ]:
        stmts.append(
            "INSERT INTO concept_edges "
            "(edge_id, vertical, predicate, from_concept, to_concept, source, created_at) "
            f"VALUES ('{eid}', 'finance', 'disjoint_with', '{frm}', '{to}', 'seed', '{TS}')"
        )
    # A part_of concept spine (DAT-729): a 3-level chain comp_a → comp_b → comp_c PLUS a
    # back edge comp_c → comp_a (a pathological cycle bad authoring could introduce). The
    # concept_ids are the readable 'cmp_*' so the closure can assert on them; the
    # og_concept_edges view resolves the (vertical, name) endpoints to exactly these ids.
    for cid, cname in [("cmp_a", "comp_a"), ("cmp_b", "comp_b"), ("cmp_c", "comp_c")]:
        stmts.append(
            "INSERT INTO concepts (concept_id, vertical, name, kind, created_at) "
            f"VALUES ('{cid}', 'finance', '{cname}', 'measure', '{TS}')"
        )
    for eid, frm, to in [
        ("pe_1", "comp_a", "comp_b"),
        ("pe_2", "comp_b", "comp_c"),
        ("pe_3", "comp_c", "comp_a"),  # back edge → cycle comp_a→comp_b→comp_c→comp_a
    ]:
        stmts.append(
            "INSERT INTO concept_edges "
            "(edge_id, vertical, predicate, from_concept, to_concept, source, created_at) "
            f"VALUES ('{eid}', 'finance', 'part_of', '{frm}', '{to}', 'seed', '{TS}')"
        )
    # DimensionConcepts (DAT-730 ordered|nominal): fiscal_period is authored 'ordered'
    # (a temporal dimension); region leaves dimension_order UNSET → the og_concepts
    # view defaults a dimension to 'nominal'. This exercises both the stored value and
    # the view's default so every DimensionConcept carries an order (AC7).
    for cid, cname, order in [("dc_fp", "fiscal_period", "ordered"), ("dc_rg", "region", None)]:
        order_sql = f"'{order}'" if order else "NULL"
        stmts.append(
            "INSERT INTO concepts (concept_id, vertical, name, kind, dimension_order, created_at) "
            f"VALUES ('{cid}', 'finance', '{cname}', 'dimension', {order_sql}, '{TS}')"
        )
    # temporal_column_profiles (DAT-730): the journal's two date axes. booking_date
    # is a full series (last period complete); value_date spills into a trailing
    # PARTIAL period (last_period_complete = False) — the two roles are distinct
    # coverage edges. profile_data is a non-null json payload (the model requires it);
    # booking_date carries a DETECTED fiscal alignment (March year-end → April start),
    # so og_calendar derives fiscal_year_start_month = 4 from it (not the default 1).
    fiscal_json = (
        '{"fiscal_calendar": {"fiscal_alignment_detected": true, "fiscal_year_end_month": 3}}'
    )
    for pid, cid, mn, mx, grain, comp, last_ok, pdata in [
        ("tp_bd", "c_bd", "2025-01-01", "2025-12-31", "day", 1.0, "true", fiscal_json),
        ("tp_vd", "c_vd", "2025-01-01", "2026-02-15", "day", 0.9, "false", "{}"),
    ]:
        stmts.append(
            "INSERT INTO temporal_column_profiles "
            "(profile_id, column_id, run_id, profiled_at, min_timestamp, max_timestamp, "
            " detected_granularity, completeness_ratio, last_period_complete, is_stale, "
            " profile_data) "
            f"VALUES ('{pid}', '{cid}', '{RUN}', '{TS}', '{mn}', '{mx}', '{grain}', "
            f"{comp}, {last_ok}, false, '{pdata}'::json)"
        )
    # dimension_hierarchies (DAT-730 rolls_up_to): a geo drill-down over the accounts
    # dim, members finest→coarsest. The unnest yields level→level edges city→state,
    # state→country; the recursive-CTE closure walks the whole chain.
    geo_members = (
        '[{"column_name": "city", "column_id": "c_city"}, '
        '{"column_name": "state", "column_id": "c_state"}, '
        '{"column_name": "country", "column_id": "c_country"}]'
    )
    stmts.append(
        "INSERT INTO dimension_hierarchies "
        "(hierarchy_id, run_id, table_id, kind, members, canonical_label, signature, "
        " score, detection_source, needs_confirmation, created_at) "
        f"VALUES ('dh_geo', '{RUN}', 't2', 'drilldown', '{geo_members}'::json, "
        f"'city>state>country', 'drilldown:t2:city|country|state', 0.0, 'g3', false, '{TS}')"
    )
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


def _boot(engine: Engine, schema: str) -> None:
    """Run the full bootstrap graph sequence exactly as ConnectionManager does."""
    with engine.begin() as conn:
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)
        ensure_reader_role(conn, schema, READER_PW)
        grant_reader_on_graph(conn, schema)


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
    # amount is the only measure; the witness 'per_period' normalizes to flow and
    # beats the 'point_in_time' concept claim (→ stock).
    assert rows == [("amount", "flow")]


def test_references_edges_match_the_fk_topology(graph_engine: Engine) -> None:
    """A 1-hop MATCH enumerates the detected FK edges table→table (incl. the cycle)."""
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
        ("statement", "division"),
        ("division", "root"),
        ("root", "statement"),  # back edge → cycle statement→division→root→statement
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
    """has_dimension: each fact points at its slice column, CARRYING the referenced
    identity (DAT-756) — dimension_table_id resolves to the shared accounts dim."""
    sql = (
        f"SELECT tname, cname, dim FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (t IS table_node)-[e IS has_dimension]->(c IS column_node) "
        "COLUMNS (t.table_name AS tname, c.column_name AS cname, "
        "e.dimension_table_id AS dim))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.tname, r.cname, r.dim) for r in conn.execute(text(sql))}
    # All three facts slice their own account_id, each bound to the accounts dim (t2).
    assert rows == {
        ("journal", "account_id", "t2"),
        ("statement", "account_id", "t2"),
        ("account_group", "account_id", "t2"),
    }


def test_conformed_dimension_is_attribute_grain(graph_engine: Engine) -> None:
    """conformed_dimension (DAT-756): ATTRIBUTE grain — two facts conform iff they
    share the same AXIS (dim table + attribute), the alignable drill-across GROUP BY
    a SQL author needs. journal and statement both slice accounts by account_type →
    they conform (both directions, carrying the shared attribute). account_group slices
    accounts by REGION — same dim table, different axis — so it conforms with NEITHER
    (a shared table is not an alignable axis)."""
    sql = (
        f"SELECT src, dst, dim, attr FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node)-[e IS conformed_dimension]->(b IS table_node) "
        "COLUMNS (a.table_name AS src, b.table_name AS dst, "
        "e.dimension_table_id AS dim, e.dimension_attribute AS attr))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.src, r.dst, r.dim, r.attr) for r in conn.execute(text(sql))}
    assert rows == {
        ("journal", "statement", "t2", "account_type"),
        ("statement", "journal", "t2", "account_type"),
    }


def test_conformed_pair_excluded_from_refs(graph_engine: Engine) -> None:
    """The DAT-723 fan trap: a spurious fact↔fact relationship whose endpoints both
    resolve the accounts dim is NOT surfaced as a reference. The exclusion is TABLE
    grain — it fires on BOTH the same-attribute pair (journal↔statement) and the
    cross-level pair (journal↔account_group), regardless of slice attribute. This is
    deliberately DECOUPLED from the attribute-grain conformed edge: the cross-level
    pair is excluded from refs yet has no conformed edge, and that is correct. The
    genuine fact→dim FK (journal→accounts) survives (a dim key is never a slice
    column, so the exclusion cannot fire on it)."""
    sql = (
        f"SELECT src, dst FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node)-[e IS refs]->(b IS table_node) "
        "COLUMNS (a.table_name AS src, b.table_name AS dst))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.src, r.dst) for r in conn.execute(text(sql))}
    assert ("journal", "statement") not in rows, "same-attribute fan trap leaked into refs"
    assert ("journal", "account_group") not in rows, "cross-level fan trap leaked into refs"
    assert ("journal", "accounts") in rows, "genuine fact→dim FK was wrongly excluded"


def test_concept_edge_disjoint_with_matches(graph_engine: Engine) -> None:
    """DAT-729: the concept→concept binding — a disjoint_with edge is enumerable via PGQ.

    This is the P4 de-risk: P1 bound only table→table and table→column edges; a concept
    edge over the og_concepts vertex is a new element shape. A directed MATCH filtering on
    the predicate property must return both stored directions of the symmetric edge.
    """
    sql = (
        f"SELECT a, b FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS concept_node)-[e IS concept_edge WHERE e.predicate = 'disjoint_with']"
        "->(b IS concept_node) "
        "COLUMNS (a.name AS a, b.name AS b))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.a, r.b) for r in conn.execute(text(sql))}
    assert rows == {
        ("accounts_payable", "accounts_receivable"),
        ("accounts_receivable", "accounts_payable"),
    }


def test_concept_edge_part_of_matches(graph_engine: Engine) -> None:
    """part_of is a directed 1-hop concept→concept edge, selected by the predicate filter."""
    sql = (
        f"SELECT a, b FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS concept_node)-[e IS concept_edge WHERE e.predicate = 'part_of']"
        "->(b IS concept_node) "
        "COLUMNS (a.name AS a, b.name AS b))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.a, r.b) for r in conn.execute(text(sql))}
    # The 3-level spine + its back edge, and NOTHING from the disjoint_with edges.
    assert rows == {("comp_a", "comp_b"), ("comp_b", "comp_c"), ("comp_c", "comp_a")}


def _part_of_closure(conn, read: str, start: str) -> list[tuple[str, int]]:
    """Bounded recursive-CTE ancestor closure over the part_of concept edges.

    The P1 mechanism (test_property_graph ``_closure_from``) applied to the concept_edge
    view: depth < 4 caps traversal, ``NOT to_concept_id = ANY(path)`` is the cycle guard,
    ``predicate = 'part_of'`` selects the one relation. Returns (ancestor_id, depth).
    """
    sql = (
        "WITH RECURSIVE reach(src, dst, depth, path) AS ("
        "  SELECT from_concept_id, to_concept_id, 1, ARRAY[from_concept_id, to_concept_id] "
        f"  FROM \"{read}\".og_concept_edges WHERE predicate = 'part_of' "
        "  UNION ALL "
        "  SELECT r.src, e.to_concept_id, r.depth + 1, r.path || e.to_concept_id "
        f'  FROM reach r JOIN "{read}".og_concept_edges e '
        "    ON e.from_concept_id = r.dst AND e.predicate = 'part_of' "
        "  WHERE r.depth < 4 AND NOT e.to_concept_id = ANY(r.path)"
        ") SELECT dst, depth FROM reach WHERE src = :s ORDER BY depth, dst"
    )
    return [(r.dst, r.depth) for r in conn.execute(text(sql), {"s": start})]


def test_part_of_closure_walks_ancestors_and_guards_cycle(graph_engine: Engine) -> None:
    """part_of ancestry via the recursive CTE: transitive ancestors, cycle-guarded (P4 AC).

    From comp_a the closure reaches its transitive wholes comp_b (depth 1) and comp_c
    (depth 2); the back edge comp_c→comp_a does NOT re-enter comp_a — the cycle guard
    fires and the walk terminates.
    """
    with graph_engine.connect() as conn:
        rows = _part_of_closure(conn, _read_schema(), "cmp_a")
    assert rows == [("cmp_b", 1), ("cmp_c", 2)]
    assert "cmp_a" not in {dst for dst, _ in rows}  # cycle guard blocked the back edge


def test_reader_role_can_query_the_graph(graph_engine: Engine) -> None:
    """cockpit_reader (ADR-0008) can run GRAPH_TABLE — the graph grant reached it."""
    sql = (
        f"SELECT count(*) AS n FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node)-[e IS refs]->(b IS table_node) COLUMNS (1 AS one))"
    )
    with graph_engine.connect() as conn:
        conn.execute(text(f"SET ROLE {READER_ROLE}"))
        n = conn.execute(text(sql)).scalar_one()
        conn.execute(text("RESET ROLE"))
    assert n == 6


def test_bootstrap_is_idempotent(graph_engine: Engine) -> None:
    """A second boot (drop → refresh views → recreate graph → re-grant) does not error."""
    schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    _boot(graph_engine, schema)  # re-boot on top of the live graph
    with graph_engine.connect() as conn:
        n = conn.execute(
            text(
                f"SELECT count(*) FROM GRAPH_TABLE ({_graph_ref()} "
                "MATCH (a IS table_node)-[e IS refs]->(b IS table_node) COLUMNS (1 AS one))"
            )
        ).scalar_one()
    assert n == 6


# --- Query-model de-risk (ADR-0021): fixed-depth PGQ + bounded recursive CTE ---


def _closure_from(conn, read: str, start: str) -> list[tuple[str, int]]:
    """Bounded recursive-CTE transitive closure over the refs edge view.

    depth < 4 caps traversal; ``NOT to_table_id = ANY(path)`` is the cycle guard.
    Returns (dst, depth) reachable from ``start``, ordered.
    """
    sql = (
        "WITH RECURSIVE reach(src, dst, depth, path) AS ("
        f"  SELECT from_table_id, to_table_id, 1, ARRAY[from_table_id, to_table_id] "
        f'  FROM "{read}".og_references '
        "  UNION ALL "
        "  SELECT r.src, e.to_table_id, r.depth + 1, r.path || e.to_table_id "
        f'  FROM reach r JOIN "{read}".og_references e ON e.from_table_id = r.dst '
        "  WHERE r.depth < 4 AND NOT e.to_table_id = ANY(r.path)"
        ") SELECT dst, depth FROM reach WHERE src = :s ORDER BY depth, dst"
    )
    return [(r.dst, r.depth) for r in conn.execute(text(sql), {"s": start})]


def test_recursive_closure_truncates_at_depth_cap(graph_engine: Engine) -> None:
    """From t1 the closure stops at the depth-4 node; t6 (one hop past) stays unreached.

    Distinguishes the depth cap from edge-exhaustion: t6 IS reachable (t1→…→t6) but
    lies at depth 5, so its absence proves the cap fired rather than the chain ending.
    """
    with graph_engine.connect() as conn:
        rows = _closure_from(conn, _read_schema(), "t1")
    assert rows == [("t2", 1), ("t3", 2), ("t4", 3), ("t5", 4)]
    assert "t6" not in {dst for dst, _ in rows}  # reachable at depth 5 → capped out


def test_recursive_closure_cycle_guard_fires_and_terminates(graph_engine: Engine) -> None:
    """From t4 the cycle t4→t5→t6→t4 does not re-enter t4, and the walk terminates.

    Without the ``NOT ... = ANY(path)`` guard this would loop t4→t5→t6→t4→… until the
    depth cap; the guard stops it at the back edge, so t4 never reappears as a dst.
    """
    with graph_engine.connect() as conn:
        rows = _closure_from(conn, _read_schema(), "t4")
    assert rows == [("t5", 1), ("t6", 2)]
    assert "t4" not in {dst for dst, _ in rows}  # cycle guard blocked the back edge


def test_fixed_depth_unroll_agrees_with_closure_at_depth_four(graph_engine: Engine) -> None:
    """The static 4-hop PGQ unroll reaches t1→division — the closure's depth-4 node."""
    sql = (
        f"SELECT reached FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node WHERE a.table_name = 'journal')"
        "-[IS refs]->()-[IS refs]->()-[IS refs]->()-[IS refs]->(z IS table_node) "
        "COLUMNS (z.table_name AS reached))"
    )
    with graph_engine.connect() as conn:
        reached = {r.reached for r in conn.execute(text(sql))}
    # t5 = 'division' is the closure's depth-4 node from t1 (see the closure test).
    assert reached == {"division"}


# --- Temporal coverage & calendar (DAT-730 / P5) ---


def test_temporal_coverage_per_time_column(graph_engine: Engine) -> None:
    """temporal_coverage is per (relation × time column): the journal's booking_date and
    value_date each surface as a distinct edge with its own window/grain/last-period flag.

    This is AC1+AC2: role-playing dates are separate coverage edges (the metric basis is
    choosable), and value_date's trailing partial period flags incomplete."""
    sql = (
        f"SELECT tname, cname, gran, last_ok FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (t IS table_node)-[e IS temporal_coverage]->(c IS column_node) "
        "COLUMNS (t.table_name AS tname, e.column_name AS cname, "
        "e.detected_granularity AS gran, e.last_period_complete AS last_ok))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.tname, r.cname, r.gran, r.last_ok) for r in conn.execute(text(sql))}
    assert rows == {
        ("journal", "booking_date", "day", True),
        ("journal", "value_date", "day", False),  # trailing partial period → incomplete
    }


def test_measure_time_axis_anchor_and_alternates(graph_engine: Engine) -> None:
    """Every measure has exactly ONE anchor time axis, with the alternates enumerable and
    role-labelled (AC3). The journal's amount anchors on booking_date (the primary axis)
    and carries value_date as a labelled alternate."""
    sql = (
        f"SELECT mname, tname, role, is_anchor FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (m IS column_node)-[e IS measure_time_axis]->(a IS column_node) "
        "COLUMNS (m.column_name AS mname, a.column_name AS tname, "
        "e.role AS role, e.is_anchor AS is_anchor))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.mname, r.tname, r.role, r.is_anchor) for r in conn.execute(text(sql))}
    assert rows == {
        ("amount", "booking_date", "booking", True),
        ("amount", "value_date", "value", False),
    }
    anchors = [r for r in rows if r[3]]
    assert len(anchors) == 1, "a measure must have exactly one anchor axis"


def test_measure_time_axis_empty_time_columns_is_graceful(graph_engine: Engine) -> None:
    """A measure on a fact with NO time_columns yields no axis edges — not an error.

    The COALESCE(time_columns, '[]') + inner joins degrade to zero rows. Per-test
    TRUNCATE + re-seed isolates this mutation."""
    ws_schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    with graph_engine.begin() as conn:
        conn.execute(
            text(
                f'UPDATE "{ws_schema}".table_entities SET time_columns = NULL '
                "WHERE table_id = 't1'"
            )
        )
    sql = (
        f"SELECT count(*) AS n FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (m IS column_node)-[e IS measure_time_axis]->(a IS column_node) "
        "COLUMNS (1 AS one))"
    )
    with graph_engine.connect() as conn:
        assert conn.execute(text(sql)).scalar_one() == 0


def test_measure_time_axis_deduplicates_repeated_column(graph_engine: Engine) -> None:
    """A malformed time_columns naming one column TWICE (two aspects) must not produce a
    colliding edge KEY — the DISTINCT ON keeps the lowest-ordinality entry (one edge)."""
    ws_schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    dup = (
        '[{"column": "booking_date", "aspect": "booking", "note": "a"}, '
        '{"column": "booking_date", "aspect": "audit", "note": "b"}, '
        '{"column": "value_date", "aspect": "value", "note": "c"}]'
    )
    with graph_engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE \"{ws_schema}\".table_entities SET time_columns = '{dup}'::json "
                "WHERE table_id = 't1'"
            )
        )
    sql = (
        f"SELECT tname, role, is_anchor FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (m IS column_node)-[e IS measure_time_axis]->(a IS column_node) "
        "COLUMNS (a.column_name AS tname, e.role AS role, e.is_anchor AS is_anchor))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.tname, r.role, r.is_anchor) for r in conn.execute(text(sql))}
    # booking_date collapses to its first (booking, anchor) entry — the 'audit' dup drops.
    assert rows == {("booking_date", "booking", True), ("value_date", "value", False)}


def test_rolls_up_to_edges_enumerable(graph_engine: Engine) -> None:
    """rolls_up_to: dimension_hierarchies.members unnests into direct level→level edges,
    finest→coarsest (AC4, 1-hop PGQ half)."""
    sql = (
        f"SELECT frm, to_ FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS column_node)-[e IS rolls_up_to]->(b IS column_node) "
        "COLUMNS (a.column_name AS frm, b.column_name AS to_))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.frm, r.to_) for r in conn.execute(text(sql))}
    assert rows == {("city", "state"), ("state", "country")}


def _rolls_up_closure(conn, read: str, start: str) -> list[tuple[str, int]]:
    """Bounded recursive-CTE roll-up closure over og_rolls_up_to (the P1 mechanism).

    depth < 4 caps traversal; ``NOT to_column_id = ANY(path)`` is the cycle guard.
    Returns (to_column_id, depth) reachable from ``start``, ordered.
    """
    sql = (
        "WITH RECURSIVE reach(src, dst, depth, path) AS ("
        "  SELECT from_column_id, to_column_id, 1, ARRAY[from_column_id, to_column_id] "
        f'  FROM "{read}".og_rolls_up_to '
        "  UNION ALL "
        "  SELECT r.src, e.to_column_id, r.depth + 1, r.path || e.to_column_id "
        f'  FROM reach r JOIN "{read}".og_rolls_up_to e ON e.from_column_id = r.dst '
        "  WHERE r.depth < 4 AND NOT e.to_column_id = ANY(r.path)"
        ") SELECT dst, depth FROM reach WHERE src = :s ORDER BY depth, dst"
    )
    return [(r.dst, r.depth) for r in conn.execute(text(sql), {"s": start})]


def test_rolls_up_to_closure_walks_the_drill_chain(graph_engine: Engine) -> None:
    """The dimension roll-up CHAIN is the recursive-CTE closure (AC4): from city the walk
    reaches state (depth 1) and country (depth 2)."""
    with graph_engine.connect() as conn:
        rows = _rolls_up_closure(conn, _read_schema(), "c_city")
    assert rows == [("c_state", 1), ("c_country", 2)]


def _period_grain_closure(conn, read: str, start: str) -> list[tuple[str, int]]:
    """Bounded recursive-CTE closure over the period-grain ladder og_period_grain."""
    sql = (
        "WITH RECURSIVE reach(src, dst, depth, path) AS ("
        "  SELECT from_grain, to_grain, 1, ARRAY[from_grain, to_grain] "
        f'  FROM "{read}".og_period_grain '
        "  UNION ALL "
        "  SELECT r.src, e.to_grain, r.depth + 1, r.path || e.to_grain "
        f'  FROM reach r JOIN "{read}".og_period_grain e ON e.from_grain = r.dst '
        "  WHERE r.depth < 4 AND NOT e.to_grain = ANY(r.path)"
        ") SELECT dst, depth FROM reach WHERE src = :s ORDER BY depth, dst"
    )
    return [(r.dst, r.depth) for r in conn.execute(text(sql), {"s": start})]


def test_period_grain_rollup_via_recursive_cte(graph_engine: Engine) -> None:
    """Period-grain roll-up is the recursive-CTE traversal (AC5): from month the ladder
    reaches quarter (depth 1) then year (depth 2) — so last-complete-quarter is derivable
    from last-complete-month."""
    with graph_engine.connect() as conn:
        rows = _period_grain_closure(conn, _read_schema(), "month")
    assert rows == [("quarter", 1), ("year", 2)]


def test_calendar_derived_from_profiles(graph_engine: Engine) -> None:
    """og_calendar is the workspace calendar (AC5): window = min/max across the profiled
    time columns, base_grain = the FINEST detected grain (derived, not a config constant),
    fiscal_year_start DERIVED from the detected fiscal-year end (March end → April = 4)."""
    with graph_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT window_start, window_end, base_grain, fiscal_year_start_month "
                f'FROM "{_read_schema()}".og_calendar'
            )
        ).one()
    assert str(row.window_start) == "2025-01-01 00:00:00"
    assert str(row.window_end) == "2026-02-15 00:00:00"
    assert row.base_grain == "day"
    # booking_date's profile detected a March fiscal-year end → (3 % 12) + 1 = April.
    assert row.fiscal_year_start_month == 4


def test_calendar_defaults_fiscal_start_when_undetected(graph_engine: Engine) -> None:
    """When no time column detected a fiscal alignment, the calendar falls back to the
    calendar year (fiscal_year_start_month = 1) — the COALESCE else-branch of og_calendar.

    Per-test TRUNCATE + re-seed isolates this mutation from the other tests."""
    ws_schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    with graph_engine.begin() as conn:
        # Blank the one detected fiscal_calendar payload; the mode subquery now finds
        # no alignment and COALESCE returns the default 1. (The view reads the raw table
        # live, so no re-boot is needed.)
        conn.execute(
            text(
                f'UPDATE "{ws_schema}".temporal_column_profiles '
                "SET profile_data = '{}'::json WHERE profile_id = 'tp_bd'"
            )
        )
    with graph_engine.connect() as conn:
        start = conn.execute(
            text(f'SELECT fiscal_year_start_month FROM "{_read_schema()}".og_calendar')
        ).scalar_one()
    assert start == 1


def test_dimension_order_ordered_vs_nominal(graph_engine: Engine) -> None:
    """Every DimensionConcept carries ordered|nominal (AC7): fiscal_period (timestamp role)
    is ordered; region is nominal. Non-dimension concepts carry no order."""
    sql = (
        f"SELECT name, dord FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS concept_node WHERE c.kind = 'dimension') "
        "COLUMNS (c.name AS name, c.dimension_order AS dord))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.name, r.dord) for r in conn.execute(text(sql))}
    assert rows == {("fiscal_period", "ordered"), ("region", "nominal")}
    # Non-dimension kinds carry NO order (measure/entity/unit have no window axis).
    non_dim_sql = (
        f"SELECT dord FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS concept_node WHERE c.name = 'accounts_payable') "
        "COLUMNS (c.dimension_order AS dord))"
    )
    with graph_engine.connect() as conn:
        assert conn.execute(text(non_dim_sql)).scalar_one() is None
