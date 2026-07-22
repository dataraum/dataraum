"""Operating-model property graph over real Postgres 19 (ADR-0021, DAT-726).

Seeds one controlled, fully-promoted workspace (deterministic — no pipeline, no
LLM), materializes the read views + the property graph exactly as the engine
bootstrap does, then exercises the acceptance criteria on the live SQL/PGQ
engine:

* the graph binds over the ``current_*`` views (element views + CREATE PROPERTY
  GRAPH succeed);
* a PGQ ``MATCH`` returns correct rows — every measure column → its stock/flow
  materialization, seeded with the REAL pipeline vocabularies;
* ``derived_from`` edges are enumerable from a REALISTIC enriched-VIEW table
  (layer='enriched', head-scoped via enriched_views — the DAT-774 fix that made
  ``og_tables`` admit enriched vertices) → its fact + dim bases; the edge_key is deduped
  against a duplicate dimension id, and an un-materialized (NULL) view yields nothing;
* the per-workspace reader role can query the graph (a property graph needs its
  own GRANT — table grants don't reach GRAPH_TABLE);
* the two-mechanism query model is de-risked on a chain that OUTRUNS the depth cap
  and contains a cycle: the bounded recursive-CTE closure truncates at the cap
  (a node one hop past it stays unreached), the cycle guard fires (a back edge
  never re-enters its source) and the walk terminates, and the fixed-depth PGQ
  unroll agrees with the closure at the same depth;
* the drop-before-refresh bootstrap is idempotent across a re-boot;
* the grounding reification (DAT-727): ``current_groundings`` membership
  (graph-authored extracts only — the cockpit's query rows never surface), the
  Grounding vertex set incl. the ``failed`` discriminator, the clause-parts
  round-trip (parity vs ``compose_extract_sql``), ``grounded_by`` with the
  multi-grounding enumeration (``account_balance`` across trial_balance /
  balance_sheet), and ``uses`` un-nested from the enforced provenance contract
  onto the SERVED relation's column vertices (enriched columns for an enriched
  relation — DAT-811; typed columns for the no-enriched-view fallback).

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
    ensure_workspace_roles,
    materialize_read_schema,
    read_schema_name_for,
    reader_role_for,
)

RUN = "00000000-0000-0000-0000-000000000001"  # the autofill baseline run_id
SRC = "00000000-0000-0000-0000-000000000002"  # the baseline Source seeded by the fixture
TS = "2026-01-01 00:00:00"
READER_PW = "graph-reader-test-pw"
WRITER_PW = "graph-writer-test-pw"

# A 6-table chain t1→t2→t3→t4→t5→t6 (LONGER than the depth-4 closure cap) plus a
# back edge t6→t4 (the cycle t4→t5→t6→t4). This lets the de-risk tests distinguish
# "hit the depth cap" (t6 exists but is unreachable from t1 within 4 hops) from
# "ran out of edges", and fire the cycle guard (from t4, t4 never re-enters).
# t7/t8 are two extra facts carrying ONLY enriched views (no refs / slices / entity, so
# the refs/conformed counts are untouched) — for the DAT-774 derived_from cases: t7
# backs the duplicate-dim dedup view, t8 the un-materialized (NULL view_table_id) view.
_TYPED_TABLES = [
    ("t1", "journal"),
    ("t2", "accounts"),
    ("t3", "account_group"),
    ("t4", "statement"),
    ("t5", "division"),
    ("t6", "root"),
    ("t7", "ledger"),
    ("t8", "paylog"),
]
# Enriched-VIEW tables (DAT-774): layer='enriched' and — like the real pipeline — NO
# generation head. An enriched view is minted in begin_session, never promoted under a
# (table:{id}, generation) head; its currency flows from the (catalog) enriched_views
# head, resolved through current_enriched_views. og_tables surfaces these as table
# vertices via that same view, or every derived_from edge dangles at its source — the
# exact bug DAT-774 fixes.
_ENRICHED_TABLES = [
    ("t_enr", "enriched_journal"),  # v_1 — realistic: journal fact + accounts dim
    ("t_enr2", "enriched_ledger"),  # v_2 — duplicate dim id, dedup proof
]
_COLUMNS = [
    ("c_amt", "t1", "amount", 1),
    # A 2nd measure on t1 with NO lineage witness — its anchor_time_axis must fall
    # back to t1's DECLARED anchor (DAT-780 witness-precedence fallback path).
    ("c_amt2", "t1", "amount_declared", 3),
    ("c_k1", "t1", "account_id", 2),
    ("c_k2", "t2", "account_id", 1),
    ("c_at", "t2", "account_type", 2),  # accounts dim attribute — a DAT-811 dim-column source
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
    for tid, name in _TYPED_TABLES:
        stmts.append(
            f"INSERT INTO tables (table_id, source_id, table_name, layer, created_at) "
            f"VALUES ('{tid}', '{SRC}', '{name}', 'typed', '{TS}')"
        )
        stmts.append(
            f"INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at) "
            f"VALUES ('h_{tid}', 'table:{tid}', 'generation', '{RUN}', '{TS}')"
        )
    # Enriched-view tables: layer='enriched', and NO generation head (the pipeline
    # promotes none for them). They're referenced by the enriched_views rows below (FK)
    # and surfaced as vertices only through current_enriched_views (DAT-774).
    for tid, name in _ENRICHED_TABLES:
        stmts.append(
            f"INSERT INTO tables (table_id, source_id, table_name, layer, created_at) "
            f"VALUES ('{tid}', '{SRC}', '{name}', 'enriched', '{TS}')"
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
    # DAT-811: served columns of the enriched view t_enr (enriched_journal, v_1). Each is a
    # real column vertex under the enriched table with its OWN column_id, carrying ``origin``
    # and the typed ``source_column_id`` og_columns resolves semantics through:
    #   ec_amt — f.* passthrough of the fact measure c_amt (origin='fact'): inherits
    #            semantic_role='measure' + materialization='flow' from its source.
    #   ec_at  — joined dim column account_id__account_type (origin='dimension') sourced
    #            from the accounts attribute c_at (unannotated → NULL semantics).
    for cid, name, pos, origin, src in [
        ("ec_amt", "amount", 0, "fact", "c_amt"),
        ("ec_at", "account_id__account_type", 1, "dimension", "c_at"),
    ]:
        stmts.append(
            "INSERT INTO columns "
            "(column_id, table_id, column_name, column_position, origin, source_column_id) "
            f"VALUES ('{cid}', 't_enr', '{name}', {pos}, '{origin}', '{src}')"
        )
    # has_role: amount / amount_declared are measures, account_id a key.
    for cid, role in [("c_amt", "measure"), ("c_amt2", "measure"), ("c_k1", "key")]:
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
    # measure_time_axis_column_id / event_time_axis_column_id are left NULL — this
    # fixture doesn't seed a TableEntity.time_columns axis, so there is nothing to
    # resolve (DAT-778's honest-NULL case). measure_slice_column_id /
    # event_slice_column_id are NOT NULL (sourced from SliceDefinition.column_id in
    # the real writer): 'c_k1' stands in for both, same as the real writer would use
    # when measure_table_id == event_table_id.
    stmts.append(
        "INSERT INTO measure_aggregation_lineage "
        "(lineage_id, run_id, measure_table_id, measure_column_id, event_table_id, "
        " measure_time_axis_column, event_time_axis_column, "
        " measure_slice_column_id, event_slice_column_id, "
        " slice_dimension, convention_sql, period_grain, pattern, match_rate, "
        " r_flow_median, r_stock_median, n_entities, n_entities_fired, created_at) "
        f"VALUES ('mal_amt', '{RUN}', 't1', 'c_amt', 't1', "
        f"'period_date', 'period_date', 'c_k1', 'c_k1', "
        f"'month', 'SUM(amount)', "
        f"'month', 'per_period', 1.0, 0.9, 0.1, 10, 10, '{TS}')"
    )
    # detection_method='llm': og_references serves the DEFINED catalog only
    # (DAT-850) — a row without a real method would be dropped by the view.
    for rid, ft, fc, tt, tc in _REFS:
        stmts.append(
            "INSERT INTO relationships "
            "(relationship_id, run_id, from_table_id, from_column_id, to_table_id, "
            " to_column_id, relationship_type, cardinality, confidence, confirmation_source, "
            " detection_method, detected_at) "
            f"VALUES ('{rid}', '{RUN}', '{ft}', '{fc}', '{tt}', '{tc}', "
            f"'foreign_key', 'many-to-one', 0.9, 'judge', 'llm', '{TS}')"
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
    # Fact↔fact MEETINGS between account_id slice columns (the DAT-723 fan trap),
    # persisted the way the write site now produces them (DAT-850): the judge's fk
    # claim refuted by the measured many-to-many resolves to 'conformed_dimension'
    # at oriented_row, so og_references drops them BY KIND — no slice-identity
    # re-derivation. r_fan links the same-attribute pair (t1↔t4, which DOES get a
    # conformed edge from the slice axis); r_fan_xlevel links the cross-level pair
    # (t1↔t3, which does NOT — typed out of refs yet unedged is correct: no common
    # axis to drill across).
    for rid, ft, fc, tt, tc in [
        ("r_fan", "t1", "c_k1", "t4", "c_k4b"),
        ("r_fan_xlevel", "t1", "c_k1", "t3", "c_k3b"),
    ]:
        stmts.append(
            "INSERT INTO relationships "
            "(relationship_id, run_id, from_table_id, from_column_id, to_table_id, "
            " to_column_id, relationship_type, cardinality, confidence, confirmation_source, "
            " detection_method, detected_at) "
            f"VALUES ('{rid}', '{RUN}', '{ft}', '{fc}', '{tt}', '{tc}', "
            f"'conformed_dimension', 'many-to-many', 0.9, 'judge', 'llm', '{TS}')"
        )
    # A judge-DECLINED pair (detection_method='candidate') under the same catalog
    # run: NOT part of the defined catalog, so og_references must not serve it —
    # before DAT-850 the view had no detection_method filter and leaked these to
    # the GraphAgent as FK edges.
    stmts.append(
        "INSERT INTO relationships "
        "(relationship_id, run_id, from_table_id, from_column_id, to_table_id, "
        " to_column_id, relationship_type, cardinality, confidence, confirmation_source, "
        " detection_method, detected_at) "
        f"VALUES ('r_declined', '{RUN}', 't3', 'c_k3b', 't1', 'c_k1', "
        f"'foreign_key', 'many-to-one', 0.4, 'unconfirmed', 'candidate', '{TS}')"
    )
    # t1 carries a DECLARED time-axis set with the anchor at index 1 (NOT 0) and an
    # attribute date mixed in — the scrambled-order proof (DAT-780): a positional
    # reader would wrongly pick 'created_date' at index 0, the view must pick the
    # is_anchor='txn_date' regardless of position, and never the attribute 'due_date'.
    t1_time_columns = (
        '[{"column": "created_date", "aspect": "created", "role": "event", '
        '"is_anchor": false, "note": "x"}, '
        '{"column": "txn_date", "aspect": "txn", "role": "event", '
        '"is_anchor": true, "note": "x"}, '
        '{"column": "due_date", "aspect": "due", "role": "attribute", '
        '"is_anchor": false, "note": "x"}]'
    )
    time_cols_by_entity = {"t1": t1_time_columns}
    for eid, tid, role in [("e_j", "t1", "fact"), ("e_a", "t2", "dimension")]:
        tcs = time_cols_by_entity.get(tid)
        if tcs is not None:
            stmts.append(
                "INSERT INTO table_entities "
                "(entity_id, table_id, run_id, detected_entity_type, table_role, "
                " time_columns, detected_at) "
                f"VALUES ('{eid}', '{tid}', '{RUN}', 'entity', '{role}', "
                f"'{tcs}'::json, '{TS}')"
            )
        else:
            stmts.append(
                "INSERT INTO table_entities "
                "(entity_id, table_id, run_id, detected_entity_type, table_role, detected_at) "
                f"VALUES ('{eid}', '{tid}', '{RUN}', 'entity', '{role}', '{TS}')"
            )
    # derived_from (DAT-774): three enriched-view rows exercising the fixed edge.
    #  v_1 REALISTIC — enriched_journal over the journal fact (t1) + accounts dim (t2).
    #     Its source vertex (t_enr, layer='enriched') resolves via current_enriched_views,
    #     so BOTH the view→fact and view→dim edges MATCH.
    #  v_2 DEDUP — enriched_ledger whose dimension_table_ids carries a DUPLICATE id
    #     (["t2","t2"]); the og_derived_from SELECT DISTINCT collapses it to one view→dim
    #     edge (a non-deduped view would emit two rows sharing one edge_key — a non-unique
    #     PGQ KEY).
    #  v_3 ABSENCE — paylog's view was never materialized (view_table_id NULL); the WHERE
    #     view_table_id IS NOT NULL guard drops it: no vertex, no edge.
    for vid, fact, view_tid, vname, dims in [
        ("v_1", "t1", "'t_enr'", "enriched_journal", '["t2"]'),
        ("v_2", "t7", "'t_enr2'", "enriched_ledger", '["t2", "t2"]'),
        ("v_3", "t8", "NULL", "enriched_paylog", '["t2"]'),
    ]:
        stmts.append(
            "INSERT INTO enriched_views "
            "(view_id, fact_table_id, view_table_id, view_name, run_id, "
            " dimension_table_ids, is_grain_verified, created_at) "
            f"VALUES ('{vid}', '{fact}', {view_tid}, '{vname}', '{RUN}', "
            f"'{dims}'::json, true, '{TS}')"
        )
    # Bind the workspace's active vertical (DAT-848): the concept read views
    # (og_concepts / og_concept_edges read __READ__.concepts / .concept_edges) scope
    # to workspace_settings.active_vertical, falling back to '_adhoc' when unbound —
    # so an unbound workspace would serve only its '_adhoc' vocabulary (none exists in
    # this seed). Every concept/edge below is 'finance' — bind it so the vocabulary
    # graph is populated.
    stmts.append(
        "INSERT INTO workspace_settings (pin, active_vertical, bound_at) "
        f"VALUES (true, 'finance', '{TS}')"
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
    stmts.extend(_coverage_and_rollup_stmts())
    stmts.extend(_grounding_stmts())
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


def _coverage_and_rollup_stmts() -> list[str]:
    """DAT-730 fixture rows: temporal coverage, drill-down roll-up, dimension ordering.

    - t1's three DECLARED time columns (txn_date anchor/event, created_date event,
      due_date attribute) become real ``columns`` rows so the time_columns JSON names
      resolve to og_columns vertices. txn_date carries a COMPLETE monthly profile,
      created_date a partial-trailing DAILY profile, due_date NO profile at all — the
      absence-falls-loud case (coverage edge with NULL observed_*).
    - a ``drilldown`` dimension_hierarchies row over c_k1→c_k2→c_k3 (levels 2→1→0,
      finer→coarser) plus a level-3 member with NO catalog column ('') to prove the
      skip, and an ``alias`` row to prove kind!='drilldown' emits no rolls_up_to.
    - a ``dimension`` concept carrying ordering='ordered' (the DAT-730 typed fact).
    """
    import json

    stmts: list[str] = []
    for cid, name in [("c_txn", "txn_date"), ("c_created", "created_date"), ("c_due", "due_date")]:
        stmts.append(
            f"INSERT INTO columns (column_id, table_id, column_name, column_position) "
            f"VALUES ('{cid}', 't1', '{name}', 10)"
        )
    # txn_date: a COMPLETE monthly series (last_period_complete=true, ratio 1.0).
    stmts.append(
        "INSERT INTO temporal_column_profiles "
        "(profile_id, column_id, run_id, profiled_at, min_timestamp, max_timestamp, span_days, "
        " detected_granularity, granularity_confidence, completeness_ratio, expected_periods, "
        " actual_periods, gap_count, largest_gap_days, last_period_complete, is_stale, gaps) "
        f"VALUES ('tp_txn', 'c_txn', '{RUN}', '{TS}', '2023-01-31', '2023-12-31', 334, "
        f"'month', 0.9, 1.0, 12, 12, 0, NULL, true, false, '[]'::json)"
    )
    # created_date: a partial-trailing DAILY series (last_period_complete=false).
    stmts.append(
        "INSERT INTO temporal_column_profiles "
        "(profile_id, column_id, run_id, profiled_at, min_timestamp, max_timestamp, span_days, "
        " detected_granularity, granularity_confidence, completeness_ratio, expected_periods, "
        " actual_periods, gap_count, largest_gap_days, last_period_complete, is_stale, gaps) "
        f"VALUES ('tp_created', 'c_created', '{RUN}', '{TS}', '2024-01-01', '2024-03-31', 90, "
        f"'day', 0.9, 0.9, 91, 82, 3, 5, false, false, '[]'::json)"
    )
    # due_date (c_due): NO temporal profile — the LEFT JOIN yields NULL observed_*.
    # A drill-down hierarchy c_k1 (lvl2, finest) → c_k2 (lvl1) → c_k3 (lvl0, coarsest),
    # plus a level-3 member with '' column_id (skipped) — level is the SOLE direction.
    dh_members = json.dumps(
        [
            {"column_name": "group_id", "column_id": "c_k3", "distinct_count": 5, "level": 0},
            {"column_name": "account_id", "column_id": "c_k2", "distinct_count": 50, "level": 1},
            {"column_name": "acct_own", "column_id": "c_k1", "distinct_count": 500, "level": 2},
            {"column_name": "orphan", "column_id": "", "distinct_count": None, "level": 3},
        ]
    ).replace("'", "''")
    stmts.append(
        "INSERT INTO dimension_hierarchies "
        "(hierarchy_id, run_id, table_id, kind, members, canonical_label, signature, "
        " detection_source, needs_confirmation, created_at) "
        f"VALUES ('dh_1', '{RUN}', 't1', 'drilldown', '{dh_members}'::json, "
        f"'group → account → acct', 'drilldown:t1:acct', 'g3', false, '{TS}')"
    )
    # An ALIAS structure over the same members must emit NO rolls_up_to edge.
    alias_members = json.dumps(
        [
            {"column_name": "division_id", "column_id": "c_k5", "distinct_count": 3, "level": 0},
            {"column_name": "statement_id", "column_id": "c_k4", "distinct_count": 3, "level": 1},
        ]
    ).replace("'", "''")
    stmts.append(
        "INSERT INTO dimension_hierarchies "
        "(hierarchy_id, run_id, table_id, kind, members, canonical_label, signature, "
        " detection_source, needs_confirmation, created_at) "
        f"VALUES ('dh_2', '{RUN}', 't4', 'alias', '{alias_members}'::json, "
        f"'division_id', 'alias:t4:division', 'g3', false, '{TS}')"
    )
    # A dimension concept carrying the DAT-730 ordering fact (og_concepts.ordering).
    stmts.append(
        "INSERT INTO concepts (concept_id, vertical, name, kind, ordering, created_at) "
        f"VALUES ('con_sev', 'finance', 'severity', 'dimension', 'ordered', '{TS}')"
    )
    return stmts


# Grounding fixtures (DAT-727): the snippet-KB rows current_groundings
# surfaces and og_grounding reifies. Each spec is (snippet_id, standard_field,
# statement, relation, select_expr, where[], provenance, failed) — sql + parts
# are produced by the REAL render path (compose_extract_sql / extract_parts_dict),
# so the parity test compares the graph's round-trip against the exact
# production artifact.
#
# The finance multi-grounding AC case: account_balance grounded TWICE (via the
# trial_balance statement and the balance_sheet statement) over the served
# enriched view. Enumerated names are SERVED names (DAT-811): the view's own
# columns — 'amount' (ec_amt, the f.* passthrough) and 'account_id__account_type'
# (ec_at, the joined dim attribute) — exactly what the enforcement validated the
# basis against at authoring. sn_rev reads the typed fact directly (the
# no-enriched-view fallback branch → c_amt). sn_old is a healthy pre-v2 row:
# still a Grounding vertex, but its v1 basis yields no uses edges and its field
# names no active concept — no grounded_by edge either (the graph never
# dangles). sn_fail is a retained DAT-543 failure: a vertex with failed=true,
# reachable from its concept via grounded_by, with NO uses edges (its
# provenance carries failure keys, no basis).
_SNIPPETS: list[tuple[str, str, str, str, str, list[str], dict | None, bool]] = [
    (
        "sn_tb",
        "account_balance",
        "trial_balance",
        "enriched_journal",
        "SUM(amount)",
        [
            "account_id__account_type IN ('asset','liability')",
            "account_id__account_type IS NOT NULL",
        ],
        {
            "column_mappings_basis": {
                "account_balance": {
                    "measure_columns": ["amount"],
                    "filter_columns": ["account_id__account_type"],
                    "filter": "asset, liability",
                }
            },
            "assumptions": [],
        },
        False,
    ),
    (
        "sn_bs",
        "account_balance",
        "balance_sheet",
        "enriched_journal",
        "SUM(amount)",
        [],
        {
            "column_mappings_basis": {"account_balance": {"measure_columns": ["amount"]}},
            "assumptions": [],
        },
        False,
    ),
    (
        "sn_rev",
        "revenue",
        "income_statement",
        "journal",  # the typed fact directly — no enriched view named 'journal'
        "SUM(amount)",
        [],
        {
            "column_mappings_basis": {"revenue": {"measure_columns": ["amount"]}},
            "assumptions": [],
        },
        False,
    ),
    (
        "sn_old",
        "expenses",  # no active concept row → no grounded_by edge
        "income_statement",
        "enriched_journal",
        "SUM(amount)",
        [],
        # Pre-v2 provenance shape: no measure_columns/filter_columns arrays →
        # zero uses edges by construction (clean cut, no backfill).
        {"column_mappings_basis": {"expenses": {"column": "amount"}}},
        False,
    ),
    (
        "sn_fail",
        "revenue",
        "balance_sheet",
        "enriched_journal",
        "SUM(amount)",
        [],
        # Retained DAT-543 failure: the typed failed payload — no basis.
        {"failure_mode": "verifier_rejected", "failure_reason": "no support"},
        True,
    ),
]


def _grounding_stmts() -> list[str]:
    """INSERT statements for the grounding fixtures (see ``_SNIPPETS``)."""
    import json

    from dataraum.graphs.formula_composer import compose_extract_sql, extract_parts_dict

    def lit(s: str) -> str:
        return s.replace("'", "''")

    stmts: list[str] = []
    for cid, cname in [("con_bal", "account_balance"), ("con_rev", "revenue")]:
        stmts.append(
            "INSERT INTO concepts (concept_id, vertical, name, kind, created_at) "
            f"VALUES ('{cid}', 'finance', '{cname}', 'measure', '{TS}')"
        )
    # The DAT-727c derived reconciles_with SELF-LOOP (what derive_reconciles_with
    # writes for account_balance's two healthy groundings): source='derived'
    # (CHECK-admitted), from == to, resolved by og_concept_edges to one
    # self-edge on the concept vertex.
    stmts.append(
        "INSERT INTO concept_edges "
        "(edge_id, vertical, predicate, from_concept, to_concept, source, created_at) "
        f"VALUES ('ce_rw', 'finance', 'reconciles_with', 'account_balance', "
        f"'account_balance', 'derived', '{TS}')"
    )
    for sid, field, statement, relation, select_expr, where, provenance, failed in _SNIPPETS:
        parts = json.dumps(extract_parts_dict(select_expr, relation, where))
        sql = compose_extract_sql(select_expr, relation, where)
        prov = f"'{lit(json.dumps(provenance))}'::json" if provenance else "NULL"
        stmts.append(
            "INSERT INTO sql_snippets "
            "(snippet_id, workspace_id, snippet_type, standard_field, statement, "
            " aggregation, schema_mapping_id, sql, description, source, provenance, "
            " parts, execution_count, failure_count, created_at, updated_at) "
            f"VALUES ('{sid}', 'test', 'extract', '{field}', '{statement}', 'sum', "
            f"'test', '{lit(sql)}', 'd', 'graph:{field}', {prov}, "
            f"'{lit(parts)}'::json, 0, {1 if failed else 0}, '{TS}', '{TS}')"
        )
    # A healthy pre-parts row: still a grounding (no backfill — it heals by
    # re-authoring), surfacing with NULL relation/select_expr.
    stmts.append(
        "INSERT INTO sql_snippets "
        "(snippet_id, workspace_id, snippet_type, standard_field, statement, "
        " aggregation, schema_mapping_id, sql, description, source, "
        " execution_count, failure_count, created_at, updated_at) "
        "VALUES ('sn_nul', 'test', 'extract', 'expenses', 'cash_flow', 'sum', "
        f"'test', 'SELECT 1', 'no parts', 'graph:expenses', 0, 0, '{TS}', '{TS}')"
    )
    # NEVER groundings (DAT-727 membership): the cockpit writes into the SAME
    # table — a query-typed row (snippet_type='query') AND a query-SOURCED
    # extract (source='query:%') must both stay off the surface, as must a
    # per-metric formula composition.
    stmts.append(
        "INSERT INTO sql_snippets "
        "(snippet_id, workspace_id, snippet_type, schema_mapping_id, sql, "
        " description, source, execution_count, failure_count, created_at, updated_at) "
        f"VALUES ('sn_qry', 'test', 'query', 'test', 'SELECT 2', 'q', 'query:exec_9', "
        f"0, 0, '{TS}', '{TS}')"
    )
    stmts.append(
        "INSERT INTO sql_snippets "
        "(snippet_id, workspace_id, snippet_type, standard_field, statement, "
        " aggregation, schema_mapping_id, sql, description, source, "
        " execution_count, failure_count, created_at, updated_at) "
        "VALUES ('sn_qext', 'test', 'extract', 'revenue', 'income_statement', 'avg', "
        f"'test', 'SELECT 3', 'promoted', 'query:exec_9', 0, 0, '{TS}', '{TS}')"
    )
    stmts.append(
        "INSERT INTO sql_snippets "
        "(snippet_id, workspace_id, snippet_type, schema_mapping_id, sql, "
        " description, source, normalized_expression, "
        " execution_count, failure_count, created_at, updated_at) "
        f"VALUES ('sn_frm', 'test', 'formula', 'test', 'SELECT 4', 'f', 'graph:margin', "
        f"'a/b', 0, 0, '{TS}', '{TS}')"
    )
    return stmts


def _boot(engine: Engine, schema: str) -> None:
    """Run the full bootstrap graph sequence exactly as ConnectionManager does."""
    with engine.begin() as conn:
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)
        ensure_workspace_roles(conn, schema, READER_PW, WRITER_PW)
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
    """The headline MATCH: every measure column → its stock/flow materialization."""
    sql = (
        f"SELECT column_name, materialization FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS column_node WHERE c.semantic_role = 'measure') "
        "COLUMNS (c.column_name AS column_name, c.materialization AS materialization))"
    )
    with graph_engine.connect() as conn:
        rows = dict(conn.execute(text(sql)).all())
    # 'amount' has a witness: 'per_period' normalizes to flow and beats the
    # 'point_in_time' concept claim (→ stock). 'amount_declared' has neither witness
    # nor concept claim → NULL materialization.
    assert rows == {"amount": "flow", "amount_declared": None}


def test_measure_anchor_time_axis_prefers_the_witness(graph_engine: Engine) -> None:
    """DAT-780 Gap 2: a measure's anchor is the DAT-778 witness event-side axis where
    a witness exists — precedence over the table's declared anchor, expressed by the
    COALESCE order (mirrors materialization's witness-over-claim precedence)."""
    sql = (
        f"SELECT anchor FROM GRAPH_TABLE ({_graph_ref()} "
        # Scope to the typed fact vertex: DAT-811 adds an enriched 'amount' vertex on
        # t_enr (same name, own id) that inherits the same witness axis via its source.
        "MATCH (c IS column_node WHERE c.column_name = 'amount' AND c.table_id = 't1') "
        "COLUMNS (c.anchor_time_axis AS anchor))"
    )
    with graph_engine.connect() as conn:
        rows = conn.execute(text(sql)).all()
    # c_amt's witness row set event_time_axis_column='period_date'; it wins over t1's
    # declared anchor 'txn_date'.
    assert rows == [("period_date",)]


def test_measure_anchor_time_axis_falls_back_to_declared_anchor(graph_engine: Engine) -> None:
    """DAT-780 Gap 2: with no witness, the anchor is the table's TYPED declared anchor
    (is_anchor=true, role='event') — never array position, never an attribute date.

    t1's declared time_columns list the anchor 'txn_date' at index 1, with
    'created_date' at index 0 (a positional reader's wrong pick) and the attribute
    'due_date' present. 'amount_declared' has no witness, so it resolves the declared
    anchor — proving the scrambled-order case and the attribute exclusion at once.
    """
    sql = (
        f"SELECT anchor FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS column_node WHERE c.column_name = 'amount_declared') "
        "COLUMNS (c.anchor_time_axis AS anchor))"
    )
    with graph_engine.connect() as conn:
        rows = conn.execute(text(sql)).all()
    assert rows == [("txn_date",)]


def test_enriched_view_columns_carry_source_resolved_semantics(graph_engine: Engine) -> None:
    """DAT-811 AC: an enriched view's served columns are column vertices UNDER the enriched
    table, each keeping its OWN column_id (the KEY stays unique — a typed id never appears
    twice) but resolving EVERY semantic THROUGH its typed ``source_column_id``. A MATCH over
    the enriched table_id returns its full served set with semantics attached — the view is
    self-describing, no walk back to origin tables (the ``og_derived_from`` lesson: a
    structurally-dead surface is invisible without a MATCH-returns-rows test).
    """
    sql = (
        f"SELECT column_id, column_name, role, mat FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS column_node WHERE c.table_id = 't_enr') "
        "COLUMNS (c.column_id AS column_id, c.column_name AS column_name, "
        "c.semantic_role AS role, c.materialization AS mat))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.column_id, r.column_name, r.role, r.mat) for r in conn.execute(text(sql))}
    assert rows == {
        # f.* passthrough measure: its OWN id (ec_amt, NOT the typed c_amt), semantics
        # resolved through source c_amt → measure + flow. This is what a MATCH-driven
        # GraphAgent and the cadence resolver (DAT-812) will read off the view.
        ("ec_amt", "amount", "measure", "flow"),
        # joined dim column: surfaces with its own id; source c_at is unannotated → NULL.
        ("ec_at", "account_id__account_type", None, None),
    }


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
    """derived_from (DAT-774): the enriched-VIEW table (layer='enriched') resolves as a
    table vertex, so both the view→fact and view→dimension edges MATCH.

    Before the fix ``og_tables`` was typed-only (it sat on ``current_tables``, filtered
    ``layer='typed'``), the ``derived_from`` SOURCE endpoint dangled, and NO such edge
    ever instantiated in a MATCH. Scoped to the realistic v_1 view (enriched_journal);
    the projected source ``layer`` proves the enriched vertex — not a typed one — anchors
    the edge.
    """
    sql = (
        f"SELECT vlayer, base, role FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (v IS table_node WHERE v.table_name = 'enriched_journal')"
        "-[e IS derived_from]->(base IS table_node) "
        "COLUMNS (v.layer AS vlayer, base.table_name AS base, e.base_role AS role))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.vlayer, r.base, r.role) for r in conn.execute(text(sql))}
    assert rows == {
        ("enriched", "journal", "fact"),
        ("enriched", "accounts", "dimension"),
    }


def test_derived_from_edge_key_unique_under_duplicate_dim(graph_engine: Engine) -> None:
    """DAT-774 secondary defect: a DUPLICATE id in dimension_table_ids must not emit two
    edges sharing one edge_key (a non-unique PGQ KEY).

    v_2 (enriched_ledger) carries dimension_table_ids=["t2","t2"]; the og_derived_from
    dimension branch SELECT DISTINCTs it, so exactly ONE view→dim edge exists alongside
    the one view→fact edge. Collected as a LIST (not a set): a leaked duplicate would
    surface as a repeated ('accounts','dimension') row.
    """
    sql = (
        f"SELECT base, role FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (v IS table_node WHERE v.table_name = 'enriched_ledger')"
        "-[e IS derived_from]->(base IS table_node) "
        "COLUMNS (base.table_name AS base, e.base_role AS role))"
    )
    with graph_engine.connect() as conn:
        rows = sorted((r.base, r.role) for r in conn.execute(text(sql)))
    assert rows == [("accounts", "dimension"), ("ledger", "fact")]


def test_derived_from_unmaterialized_view_has_no_edge(graph_engine: Engine) -> None:
    """DAT-774: an enriched_views row whose view was never materialized (view_table_id
    NULL — v_3 over paylog) contributes NO vertex and NO edge.

    The ``WHERE view_table_id IS NOT NULL`` guard drops exactly the rows guaranteed to
    dangle, so every derived_from edge's source is one of the two materialized views and
    'enriched_paylog' is never a source vertex.
    """
    sql = (
        f"SELECT vname FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (v IS table_node)-[e IS derived_from]->(base IS table_node) "
        "COLUMNS (v.table_name AS vname))"
    )
    with graph_engine.connect() as conn:
        sources = {r.vname for r in conn.execute(text(sql))}
    assert sources == {"enriched_journal", "enriched_ledger"}
    assert "enriched_paylog" not in sources


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
    """The DAT-723 fan trap is typed out of refs BY KIND (DAT-850).

    og_references consumes the edge-kind owner — the row's relationship_type,
    resolved at the write site — instead of re-deriving the meeting from slice
    identity. Both meeting rows (same-attribute journal↔statement AND
    cross-level journal↔account_group) persist as 'conformed_dimension' and are
    not reference kinds; the cross-level pair also has no conformed edge (no
    common axis), and typed-out-yet-unedged is correct. The genuine fact→dim FK
    (journal→accounts) survives, and a judge-DECLINED row
    (detection_method='candidate') never appears — the view serves the defined
    catalog only."""
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
    assert ("account_group", "journal") not in rows, "declined candidate leaked into refs"


def test_concept_edge_disjoint_with_matches(graph_engine: Engine) -> None:
    """DAT-729: the concept→concept binding — a disjoint_with edge is enumerable via PGQ.

    This is the concept-edge de-risk: the graph previously bound only table→table and
    table→column edges; a concept
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

    The same mechanism as ``_closure_from`` above, applied to the concept_edge
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
    """part_of ancestry via the recursive CTE: transitive ancestors, cycle-guarded.

    From comp_a the closure reaches its transitive wholes comp_b (depth 1) and comp_c
    (depth 2); the back edge comp_c→comp_a does NOT re-enter comp_a — the cycle guard
    fires and the walk terminates.
    """
    with graph_engine.connect() as conn:
        rows = _part_of_closure(conn, _read_schema(), "cmp_a")
    assert rows == [("cmp_b", 1), ("cmp_c", 2)]
    assert "cmp_a" not in {dst for dst, _ in rows}  # cycle guard blocked the back edge


# --- Grounding reification (DAT-727) ----------------------------------------------


def test_current_groundings_is_graph_authored_extracts_only(graph_engine: Engine) -> None:
    """The cross-lane read contract: current_groundings = extract snippets from
    the grounding pass (source graph:%), healthy AND retained-failed, with
    (concept, relation) exposed. The cockpit's query-typed row (sn_qry), a
    query-SOURCED extract (sn_qext), and a formula composition (sn_frm) never
    surface."""
    sql = f'SELECT snippet_id, concept, relation, failed FROM "{_read_schema()}".current_groundings'
    with graph_engine.connect() as conn:
        rows = {r.snippet_id: r for r in conn.execute(text(sql))}
    assert set(rows) == {"sn_tb", "sn_bs", "sn_rev", "sn_old", "sn_fail", "sn_nul"}
    assert rows["sn_tb"].concept == "account_balance"
    assert rows["sn_tb"].relation == "enriched_journal"
    assert rows["sn_rev"].relation == "journal"
    assert rows["sn_nul"].relation is None  # pre-parts row: NULL, heals by re-authoring
    assert {sid for sid, r in rows.items() if r.failed} == {"sn_fail"}


def test_grounding_vertices_carry_the_failed_discriminator(graph_engine: Engine) -> None:
    """Every grounding — healthy, pre-v2, pre-parts, retained-failed — is a
    grounding_node; ``failed`` discriminates knowledge from a DAT-543 retained
    failure ("why is this concept ungrounded?" is a graph question)."""
    sql = (
        f"SELECT sid, failed FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (g IS grounding_node) COLUMNS (g.snippet_id AS sid, g.failed AS failed))"
    )
    with graph_engine.connect() as conn:
        rows = {r.sid: r.failed for r in conn.execute(text(sql))}
    assert rows == {
        "sn_tb": False,
        "sn_bs": False,
        "sn_rev": False,
        "sn_old": False,
        "sn_fail": True,
        "sn_nul": False,
    }


def test_grounding_parts_round_trip_vs_compose_extract_sql(graph_engine: Engine) -> None:
    """The parity AC: the vertex properties (relation / select_expr /
    where_predicates) re-render through compose_extract_sql to EXACTLY the
    snippet's persisted sql — the graph is a lossless reading of the parts."""
    import json

    from dataraum.graphs.formula_composer import compose_extract_sql

    props_sql = (
        f"SELECT sid, relation, select_expr, where_predicates FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (g IS grounding_node WHERE g.relation IS NOT NULL) "
        "COLUMNS (g.snippet_id AS sid, g.relation AS relation, "
        "g.select_expr AS select_expr, g.where_predicates AS where_predicates))"
    )
    with graph_engine.connect() as conn:
        props = {r.sid: r for r in conn.execute(text(props_sql))}
        persisted = dict(
            conn.execute(text(f'SELECT snippet_id, sql FROM "{_read_schema()}".sql_snippets')).all()
        )
    assert set(props) == {"sn_tb", "sn_bs", "sn_rev", "sn_old", "sn_fail"}
    for sid, row in props.items():
        rendered = compose_extract_sql(
            row.select_expr, row.relation, json.loads(row.where_predicates)
        )
        assert rendered == persisted[sid], sid
    # The multi-predicate where[] round-trips as a JSON array, order preserved.
    assert json.loads(props["sn_tb"].where_predicates) == [
        "account_id__account_type IN ('asset','liability')",
        "account_id__account_type IS NOT NULL",
    ]


def test_grounded_by_enumerates_each_concepts_groundings(graph_engine: Engine) -> None:
    """grounded_by: concept → grounding, INNER-join resolved on the ACTIVE
    concept. sn_old/sn_nul name no active concept → no edge (the graph never
    dangles); the FAILED sn_fail keeps its edge — the failed attempt is
    reachable from its concept, discriminated by the vertex property."""
    sql = (
        f"SELECT cname, sid FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS concept_node)-[e IS grounded_by]->(g IS grounding_node) "
        "COLUMNS (c.name AS cname, g.snippet_id AS sid))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.cname, r.sid) for r in conn.execute(text(sql))}
    assert rows == {
        ("account_balance", "sn_tb"),
        ("account_balance", "sn_bs"),
        ("revenue", "sn_rev"),
        ("revenue", "sn_fail"),
    }


def test_multi_grounding_concepts_are_enumerable(graph_engine: Engine) -> None:
    """The multi-grounding AC (the finance ws shape): account_balance holds
    TWO healthy groundings — trial_balance and balance_sheet — and a plain
    aggregate over the PGQ match surfaces exactly it. revenue's second row is
    a retained FAILURE, which the vertex filter excludes: failure retention
    must never read as multi-grounding."""
    sql = (
        "SELECT cname, n FROM ("
        f"  SELECT cname, count(*) AS n FROM GRAPH_TABLE ({_graph_ref()} "
        "   MATCH (c IS concept_node)-[e IS grounded_by]->"
        "         (g IS grounding_node WHERE NOT g.failed) "
        "   COLUMNS (c.name AS cname, g.snippet_id AS sid)) "
        "  GROUP BY cname"
        ") multi WHERE n > 1"
    )
    with graph_engine.connect() as conn:
        rows = {(r.cname, r.n) for r in conn.execute(text(sql))}
    assert rows == {("account_balance", 2)}


def test_uses_edges_land_on_the_served_relations_columns(graph_engine: Engine) -> None:
    """uses: grounding → column from the TYPED v2 basis, resolved onto the
    SERVED relation's own column vertices (DAT-811): the enriched relation's
    names resolve to its enriched columns (ec_amt, ec_at — semantics ride via
    source_column_id), the typed relation to its typed column (c_amt). The
    pre-v2 row (sn_old) has no arrays and the failed row (sn_fail) has no
    basis → no edges, by construction."""
    sql = (
        f"SELECT sid, cid, cname, role FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (g IS grounding_node)-[u IS uses]->(c IS column_node) "
        "COLUMNS (g.snippet_id AS sid, c.column_id AS cid, "
        "c.column_name AS cname, u.role AS role))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.sid, r.cid, r.cname, r.role) for r in conn.execute(text(sql))}
    assert rows == {
        ("sn_tb", "ec_amt", "amount", "measure"),
        ("sn_tb", "ec_at", "account_id__account_type", "filter"),
        ("sn_bs", "ec_amt", "amount", "measure"),
        ("sn_rev", "c_amt", "amount", "measure"),
    }


def test_derived_reconciles_with_self_loop_resolves_in_the_graph(graph_engine: Engine) -> None:
    """DAT-727c: the derived concept-grain self-loop (account_balance must tie
    out across its groundings) binds as a concept_edge self-edge — both
    endpoints resolve to the SAME active concept vertex, and the grounding
    PAIR stays derivable from the grounded_by fan-out (no Grounding→Grounding
    edge; one home)."""
    sql = (
        f"SELECT src, dst, pred FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS concept_node)-[e IS concept_edge "
        "WHERE e.predicate = 'reconciles_with']->(b IS concept_node) "
        "COLUMNS (a.concept_id AS src, b.concept_id AS dst, e.predicate AS pred))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.src, r.dst, r.pred) for r in conn.execute(text(sql))}
    assert rows == {("con_bal", "con_bal", "reconciles_with")}


def test_concept_groundings_and_their_columns_in_one_match(graph_engine: Engine) -> None:
    """The headline AC verbatim: per concept, all its groundings and the
    columns each uses — one 2-hop PGQ MATCH."""
    sql = (
        f"SELECT sid, cname FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS concept_node WHERE c.name = 'account_balance')"
        "-[e IS grounded_by]->(g IS grounding_node)-[u IS uses]->(col IS column_node) "
        "COLUMNS (g.snippet_id AS sid, col.column_name AS cname))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.sid, r.cname) for r in conn.execute(text(sql))}
    assert rows == {
        ("sn_tb", "amount"),
        ("sn_tb", "account_id__account_type"),
        ("sn_bs", "amount"),
    }


def test_reader_role_can_query_the_graph(graph_engine: Engine) -> None:
    """The workspace reader role (ADR-0008) can run GRAPH_TABLE — the grant reached it.

    Also covers a DAT-730 element (temporal_coverage): the single
    ``GRANT SELECT ON PROPERTY GRAPH`` covers every label, and
    ``GRANT SELECT ON ALL TABLES`` the new og_* views — so no per-element grant is
    needed (the empirical answer to the module docstring's caution)."""
    reader = reader_role_for(schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"]))
    sql = (
        f"SELECT count(*) AS n FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS table_node)-[e IS refs]->(b IS table_node) COLUMNS (1 AS one))"
    )
    cov_sql = (
        f"SELECT count(*) AS n FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (t IS table_node)-[c IS temporal_coverage]->(col IS column_node) "
        "COLUMNS (1 AS one))"
    )
    with graph_engine.connect() as conn:
        conn.execute(text(f"SET ROLE {reader}"))
        n = conn.execute(text(sql)).scalar_one()
        cov = conn.execute(text(cov_sql)).scalar_one()
        conn.execute(text("RESET ROLE"))
    assert n == 6
    assert cov == 3  # t1's three declared time columns — the new element is reader-visible


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


# --- DAT-730: temporal coverage, roll-up, period ladder, dimension ordering ---


def test_temporal_coverage_edges_expose_the_persisted_window(graph_engine: Engine) -> None:
    """temporal_coverage (DAT-730): one edge per (relation × DECLARED time column),
    carrying the PERSISTED profile — role/aspect/anchor from time_columns, observed grain
    = detected_granularity, plus last_period_complete (the trailing-bucket signal)."""
    sql = (
        f"SELECT src, col, role, aspect, anchor, grain, ratio, lpc "
        f"FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (t IS table_node)-[cov IS temporal_coverage]->(c IS column_node) "
        "COLUMNS (t.table_name AS src, c.column_name AS col, cov.role AS role, "
        "cov.aspect AS aspect, cov.is_anchor AS anchor, cov.observed_grain AS grain, "
        "cov.completeness_ratio AS ratio, cov.last_period_complete AS lpc))"
    )
    with graph_engine.connect() as conn:
        rows = {r.col: r for r in conn.execute(text(sql))}
    # Only t1 (journal) declares time_columns → exactly its three date axes.
    assert set(rows) == {"txn_date", "created_date", "due_date"}
    assert all(r.src == "journal" for r in rows.values())
    # The anchor event axis: a COMPLETE monthly series.
    txn = rows["txn_date"]
    assert (txn.role, txn.aspect, txn.anchor) == ("event", "txn", True)
    assert txn.grain == "month"  # observed = detected_granularity, NOT a config echo
    assert txn.ratio == 1.0
    assert txn.lpc is True
    # A non-anchor event axis with a PARTIAL trailing period.
    created = rows["created_date"]
    assert (created.role, created.anchor, created.grain) == ("event", False, "day")
    assert created.lpc is False


def test_temporal_coverage_absence_falls_loud(graph_engine: Engine) -> None:
    """A declared time column with NO temporal profile keeps its coverage edge but with
    NULL observed_* — absence is visible, never a fabricated window (DAT-730 / DAT-853)."""
    sql = (
        f"SELECT grain, ratio, lpc, span FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (t IS table_node)-[cov IS temporal_coverage]->(c IS column_node "
        "WHERE c.column_name = 'due_date') "
        "COLUMNS (cov.observed_grain AS grain, cov.completeness_ratio AS ratio, "
        "cov.last_period_complete AS lpc, cov.span_days AS span))"
    )
    with graph_engine.connect() as conn:
        rows = conn.execute(text(sql)).all()
    # due_date is a declared ATTRIBUTE axis that was never profiled → the edge exists
    # (the column is a declared time column) but every observed fact is NULL.
    assert rows == [(None, None, None, None)]


def test_rolls_up_to_edges_follow_drilldown_levels(graph_engine: Engine) -> None:
    """rolls_up_to (DAT-730): a drill-down's members become ordered level→level edges,
    finer→coarser, keyed on column_id (level is the sole direction carrier). An alias
    structure emits none, and a member with no catalog column ('') is skipped."""
    sql = (
        f"SELECT a_id, b_id, fl, tl FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS column_node)-[e IS rolls_up_to]->(b IS column_node) "
        "COLUMNS (a.column_id AS a_id, b.column_id AS b_id, "
        "e.from_level AS fl, e.to_level AS tl))"
    )
    with graph_engine.connect() as conn:
        rows = {(r.a_id, r.b_id, r.fl, r.tl) for r in conn.execute(text(sql))}
    # c_k1 (lvl2, finest) → c_k2 (lvl1) → c_k3 (lvl0, coarsest). The alias dh_2 and the
    # level-3 orphan ('' column_id) contribute nothing.
    assert rows == {("c_k1", "c_k2", 2, 1), ("c_k2", "c_k3", 1, 0)}


def test_period_grain_ladder_default_then_declared(graph_engine: Engine) -> None:
    """period_grain (DAT-730): the constant day/month/quarter/year nodes carry the
    workspace fiscal boundary — an UNSET workspace stamps the calendar-year default
    VISIBLY (calendar_source='default'); a declaration flips it to 'declared'."""
    grain_sql = (
        f"SELECT grain, ord, fym, src FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (g IS period_grain) COLUMNS (g.grain AS grain, g.ordinal AS ord, "
        "g.fiscal_year_start_month AS fym, g.calendar_source AS src))"
    )
    with graph_engine.connect() as conn:
        default_rows = {r.grain: r for r in conn.execute(text(grain_sql))}
    assert set(default_rows) == {"day", "month", "quarter", "year"}
    assert default_rows["month"].ord == 1 and default_rows["year"].ord == 3
    # Unset workspace: calendar-year default, stamped visibly (never silent).
    assert all(r.fym == 1 and r.src == "default" for r in default_rows.values())

    # The ladder edges day→month→quarter→year.
    edge_sql = (
        f"SELECT from_grain, to_grain FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (a IS period_grain)-[e IS period_rolls_up_to]->(b IS period_grain) "
        "COLUMNS (a.grain AS from_grain, b.grain AS to_grain))"
    )
    with graph_engine.connect() as conn:
        edges = {(r.from_grain, r.to_grain) for r in conn.execute(text(edge_sql))}
    assert edges == {("day", "month"), ("month", "quarter"), ("quarter", "year")}

    # Declare a fiscal calendar (April start): the boundary flips to 'declared'.
    with graph_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO workspace_calendar (pin, fiscal_year_start_month, declared_at) "
                f"VALUES (true, 4, '{TS}')"
            )
        )
    with graph_engine.connect() as conn:
        declared = {r.grain: r for r in conn.execute(text(grain_sql))}
    assert all(r.fym == 4 and r.src == "declared" for r in declared.values())


def test_period_ladder_is_walkable_by_recursive_cte(graph_engine: Engine) -> None:
    """The calendar ladder is walkable by the SAME bounded recursive CTE the part_of
    closure uses, so last-complete-quarter is derivable from last-complete-month: a walk
    up from 'month' reaches 'quarter' then 'year'."""
    read = _read_schema()
    sql = (
        "WITH RECURSIVE reach(src, dst, depth, path) AS ("
        f"  SELECT from_grain, to_grain, 1, ARRAY[from_grain, to_grain] "
        f'  FROM "{read}".og_period_rolls_up_to '
        "  UNION ALL "
        "  SELECT r.src, e.to_grain, r.depth + 1, r.path || e.to_grain "
        f'  FROM reach r JOIN "{read}".og_period_rolls_up_to e ON e.from_grain = r.dst '
        "  WHERE r.depth < 4 AND NOT e.to_grain = ANY(r.path)"
        ") SELECT dst, depth FROM reach WHERE src = 'month' ORDER BY depth, dst"
    )
    with graph_engine.connect() as conn:
        rows = [(r.dst, r.depth) for r in conn.execute(text(sql))]
    assert rows == [("quarter", 1), ("year", 2)]


def test_concept_ordering_property_is_queryable(graph_engine: Engine) -> None:
    """ordering (DAT-730): the dimension-axis fact rides the concept vertex — 'ordered'
    where declared, NULL (⇒ nominal, windows withheld) otherwise."""
    sql = (
        f"SELECT name, ordering FROM GRAPH_TABLE ({_graph_ref()} "
        "MATCH (c IS concept_node) COLUMNS (c.name AS name, c.ordering AS ordering))"
    )
    with graph_engine.connect() as conn:
        rows = {r.name: r.ordering for r in conn.execute(text(sql))}
    assert rows["severity"] == "ordered"
    # A measure concept carries no ordering fact.
    assert rows["accounts_payable"] is None
