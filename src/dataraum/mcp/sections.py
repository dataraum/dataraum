"""Section builders for get_context sectioned responses.

Each builder takes a GraphExecutionContext (or session/source for independent
sections) and returns a focused structured dict. Sections are composable —
callers can request one or many.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.context import GraphExecutionContext

_log = logging.getLogger(__name__)

# Valid section names for the get_context tool.
VALID_SECTIONS = frozenset(
    {"schema", "semantics", "quality", "validations", "cycles", "snippets", "contracts"}
)

# Sections that require a GraphExecutionContext (built via build_execution_context).
CONTEXT_SECTIONS = frozenset({"schema", "semantics", "quality", "validations", "cycles"})


def build_schema_section(context: GraphExecutionContext) -> dict[str, Any]:
    """Build schema section: tables, columns, types, relationships, views."""
    overview: dict[str, Any] = {
        "total_tables": context.total_tables,
        "total_columns": context.total_columns,
    }
    if context.graph_pattern:
        overview["graph_pattern"] = context.graph_pattern
    if context.hub_tables:
        overview["hub_tables"] = context.hub_tables
    if context.leaf_tables:
        overview["leaf_tables"] = context.leaf_tables

    tables = []
    for table in context.tables:
        t: dict[str, Any] = {
            "name": table.table_name,
        }
        if table.duckdb_name:
            t["duckdb_name"] = table.duckdb_name
        if table.is_fact_table:
            t["type"] = "fact"
        elif table.is_dimension_table:
            t["type"] = "dimension"
        if table.entity_type:
            t["entity_type"] = table.entity_type
        if table.row_count is not None:
            t["row_count"] = table.row_count
        if table.grain_columns:
            t["grain_columns"] = table.grain_columns
        if table.time_column:
            t["time_column"] = table.time_column

        columns = []
        for col in table.columns:
            c: dict[str, Any] = {"name": col.column_name}
            if col.data_type:
                c["data_type"] = col.data_type
            if col.semantic_role:
                c["semantic_role"] = col.semantic_role
            if col.null_ratio is not None:
                c["null_ratio"] = round(col.null_ratio, 4)
            if col.cardinality_ratio is not None:
                c["cardinality_ratio"] = round(col.cardinality_ratio, 4)
            columns.append(c)

        t["columns"] = columns
        tables.append(t)

    relationships = []
    for rel in context.relationships:
        r: dict[str, Any] = {
            "from_table": rel.from_table,
            "from_column": rel.from_column,
            "to_table": rel.to_table,
            "to_column": rel.to_column,
        }
        if rel.cardinality:
            r["cardinality"] = rel.cardinality
        if rel.confidence:
            r["confidence"] = round(rel.confidence, 2)
        if rel.relationship_entropy and not rel.relationship_entropy.get("is_deterministic", True):
            r["non_deterministic"] = True
        relationships.append(r)

    views = []
    for ev in context.enriched_views:
        v: dict[str, Any] = {
            "view_name": ev.view_name,
            "fact_table": ev.fact_table,
        }
        if ev.dimension_columns:
            v["dimension_columns"] = ev.dimension_columns
        if ev.is_grain_verified:
            v["grain_verified"] = True
        views.append(v)

    result: dict[str, Any] = {"overview": overview, "tables": tables}
    if relationships:
        result["relationships"] = relationships
    if views:
        result["enriched_views"] = views
    return result


def build_semantics_section(context: GraphExecutionContext) -> dict[str, Any]:
    """Build semantics section: business names, descriptions, derived columns, slices."""
    tables = []
    for table in context.tables:
        t: dict[str, Any] = {"name": table.table_name}
        if table.table_description:
            t["description"] = table.table_description
        if table.entity_type:
            t["entity_type"] = table.entity_type

        columns = []
        for col in table.columns:
            # Only include columns with semantic data
            if not any(
                [
                    col.business_name,
                    col.business_description,
                    col.business_concept,
                    col.is_derived,
                ]
            ):
                continue
            c: dict[str, Any] = {"name": col.column_name}
            if col.business_name:
                c["business_name"] = col.business_name
            if col.business_description:
                c["business_description"] = col.business_description
            if col.entity_type:
                c["entity_type"] = col.entity_type
            if col.business_concept:
                c["business_concept"] = col.business_concept
            if col.temporal_behavior:
                c["temporal_behavior"] = col.temporal_behavior
            if col.unit_source_column:
                c["unit_source_column"] = col.unit_source_column
            if col.is_derived:
                c["is_derived"] = True
                if col.derived_formula:
                    c["derived_formula"] = col.derived_formula
            columns.append(c)

        if columns:
            t["columns"] = columns
        tables.append(t)

    slices = []
    for s in context.available_slices:
        entry: dict[str, Any] = {
            "column_name": s.column_name,
            "table_name": s.table_name,
            "value_count": s.value_count,
        }
        if s.business_context:
            entry["business_context"] = s.business_context
        if s.distinct_values:
            entry["distinct_values"] = s.distinct_values[:20]
        slices.append(entry)

    result: dict[str, Any] = {"tables": tables}
    if slices:
        result["slices"] = slices

    # Warn if semantic data not yet available — mirror the column inclusion filter
    has_semantics = any(
        col.business_name or col.business_description or col.business_concept or col.is_derived
        for table in context.tables
        for col in table.columns
    )
    if not has_semantics:
        result["availability"] = {
            "status": "not_yet_available",
            "hint": "Semantic annotations require the 'semantic' pipeline phase. "
            "Run analyze to produce them.",
        }

    return result


def build_quality_section(context: GraphExecutionContext) -> dict[str, Any]:
    """Build quality section: grades, entropy, interpretations, assumptions."""
    # Check what's available
    has_entropy = any(col.entropy_scores for table in context.tables for col in table.columns)
    has_interpretation = any(
        col.entropy_explanation for table in context.tables for col in table.columns
    )

    availability: dict[str, Any] = {}
    if not has_entropy:
        availability["entropy_scores"] = "not_yet_available"
    if not has_interpretation:
        availability["entropy_interpretation"] = "not_yet_available"

    tables = []
    for table in context.tables:
        t: dict[str, Any] = {"name": table.table_name}

        if table.readiness_for_use:
            t["readiness"] = table.readiness_for_use
        if table.table_entropy_explanation:
            t["explanation"] = table.table_entropy_explanation

        columns = []
        for col in table.columns:
            # Only include columns with quality data
            if not any(
                [
                    col.entropy_scores,
                    col.entropy_explanation,
                    col.flags,
                ]
            ):
                continue

            c: dict[str, Any] = {"name": col.column_name}
            if col.entropy_scores:
                c["entropy_scores"] = col.entropy_scores
            if col.entropy_explanation:
                c["explanation"] = col.entropy_explanation
            if col.flags:
                c["flags"] = col.flags
            if col.entropy_assumptions:
                c["assumptions"] = col.entropy_assumptions
            if col.entropy_resolution_actions:
                c["resolution_actions"] = col.entropy_resolution_actions
            columns.append(c)

        if columns:
            t["columns"] = columns
        tables.append(t)

    result: dict[str, Any] = {}

    if context.entropy_summary:
        result["overall_readiness"] = context.entropy_summary.get("overall_readiness")
        score = context.entropy_summary.get("avg_entropy_score")
        if score is not None:
            result["entropy_score"] = round(score, 3)

    result["tables"] = tables

    if context.active_assumptions:
        result["assumptions_in_effect"] = context.active_assumptions

    if availability:
        result["availability"] = availability
        result["hint"] = (
            "Some quality data is not yet available. The pipeline produces quality "
            "data progressively: entropy_scores after each phase's detectors run. "
            "Use get_quality(gate=...) for "
            "zone-specific violations and fix actions."
        )

    return result


def build_validations_section(context: GraphExecutionContext) -> dict[str, Any]:
    """Build validations section: validation check results."""
    if not context.validations:
        return {
            "status": "not_yet_available",
            "hint": "Validation results require the 'validation' pipeline phase.",
        }

    failed = [v for v in context.validations if not v.passed]
    passed = [v for v in context.validations if v.passed]

    results = []
    for v in context.validations:
        entry: dict[str, Any] = {
            "validation_id": v.validation_id,
            "status": v.status,
            "severity": v.severity,
            "passed": v.passed,
            "message": v.message,
        }
        if v.details:
            entry["details"] = v.details
        results.append(entry)

    return {
        "summary": {"passed": len(passed), "failed": len(failed)},
        "results": results,
    }


def build_cycles_section(context: GraphExecutionContext) -> dict[str, Any]:
    """Build cycles section: business cycles with stages and health."""
    if not context.business_cycles:
        return {
            "status": "not_yet_available",
            "hint": "Business cycle detection requires the 'business_cycles' pipeline phase.",
        }

    cycles = []
    for cycle in context.business_cycles:
        c: dict[str, Any] = {
            "name": cycle.cycle_name,
            "type": cycle.cycle_type,
        }
        if cycle.description:
            c["description"] = cycle.description
        if cycle.tables_involved:
            c["tables_involved"] = cycle.tables_involved
        if cycle.confidence:
            c["confidence"] = round(cycle.confidence, 2)

        if cycle.stages:
            c["stages"] = [
                {
                    "name": s.stage_name,
                    "order": s.stage_order,
                    **({"indicator_column": s.indicator_column} if s.indicator_column else {}),
                    **({"indicator_values": s.indicator_values} if s.indicator_values else {}),
                    **(
                        {"completion_rate": round(s.completion_rate, 3)}
                        if s.completion_rate is not None
                        else {}
                    ),
                }
                for s in cycle.stages
            ]

        if cycle.entity_flows:
            c["entity_flows"] = [
                {
                    "entity_type": ef.entity_type,
                    "entity_column": ef.entity_column,
                    "entity_table": ef.entity_table,
                    **({"fact_table": ef.fact_table} if ef.fact_table else {}),
                }
                for ef in cycle.entity_flows
            ]

        volume: dict[str, Any] = {}
        if cycle.total_records is not None:
            volume["total_records"] = cycle.total_records
        if cycle.completed_cycles is not None:
            volume["completed_cycles"] = cycle.completed_cycles
        if cycle.completion_rate is not None:
            volume["completion_rate"] = round(cycle.completion_rate, 3)
        if volume:
            c["volume"] = volume

        if cycle.evidence:
            c["evidence"] = cycle.evidence

        cycles.append(c)

    result: dict[str, Any] = {"cycles": cycles}

    if context.cycle_health:
        health = context.cycle_health
        total_v = sum(c.validations_run for c in health.cycle_scores)
        passed_v = sum(c.validations_passed for c in health.cycle_scores)
        health_dict: dict[str, Any] = {
            "total_validations": total_v,
            "passed_validations": passed_v,
        }
        if health.overall_health is not None:
            health_dict["overall_health"] = round(health.overall_health, 3)
        result["health"] = health_dict

    return result


def build_snippets_section(session: Session, source_id: str) -> dict[str, Any]:
    """Build snippets section: full SQL graphs with column_mappings and vocabulary.

    This section is loaded independently from GraphExecutionContext via SnippetLibrary.
    """
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session)
    graphs = library.find_all_graphs(schema_mapping_id=source_id)
    vocabulary = library.get_search_vocabulary(schema_mapping_id=source_id)

    if not graphs:
        return {
            "total_snippets": 0,
            "hint": "No SQL snippets yet. Snippets are created by the graph execution "
            "phase or by query agent runs.",
        }

    formatted_graphs = []
    total_snippets = 0
    for graph in graphs:
        snippets = []
        for s in graph.snippets:
            total_snippets += 1
            entry: dict[str, Any] = {
                "snippet_id": s.snippet_id,
                "step_id": s.standard_field or s.snippet_id[:8],
                "sql": s.sql,
                "description": s.description,
                "snippet_type": s.snippet_type,
            }
            if s.statement:
                entry["statement"] = s.statement
            if s.aggregation:
                entry["aggregation"] = s.aggregation
            if s.parameter_value is not None:
                entry["parameter_value"] = s.parameter_value
            if s.column_mappings:
                entry["column_mappings"] = s.column_mappings
            if s.input_fields:
                entry["input_fields"] = s.input_fields
            snippets.append(entry)

        formatted_graphs.append(
            {
                "graph_id": graph.graph_id,
                "source": graph.source,
                "source_type": graph.source_type,
                "snippets": snippets,
            }
        )

    result: dict[str, Any] = {
        "total_snippets": total_snippets,
        "graphs": formatted_graphs,
    }

    if vocabulary:
        # Only include non-empty vocabulary lists
        vocab: dict[str, list[str]] = {}
        for key in ("standard_fields", "statements", "aggregations", "graph_ids"):
            values = vocabulary.get(key, [])
            if values:
                vocab[key] = values
        if vocab:
            result["vocabulary"] = vocab

    return result


def build_contracts_section(
    session: Session,
    table_ids: list[str],
) -> dict[str, Any]:
    """Build contracts section: catalog with live compliance status.

    Reuses the existing contract evaluation logic.
    """
    from dataraum.entropy.contracts import (
        evaluate_all_contracts,
        find_best_contract,
        list_contracts,
    )

    contracts = list_contracts()
    if not contracts:
        return {"contracts": [], "hint": "No contracts configured."}

    # Try to get column summaries for live evaluation
    column_summaries = None
    try:
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import network_to_column_summaries

        network_context = build_for_network(session, table_ids)
        if network_context and network_context.total_columns > 0:
            column_summaries = network_to_column_summaries(network_context)
    except Exception:
        _log.warning("Failed to load entropy data for contract evaluation", exc_info=True)

    catalog: list[dict[str, Any]] = []

    if column_summaries:
        evaluations = evaluate_all_contracts(column_summaries)
        best_name, _ = find_best_contract(column_summaries)

        for c in contracts:
            entry: dict[str, Any] = {
                "name": c["name"],
                "display_name": c["display_name"],
                "description": c["description"],
                "threshold": c["overall_threshold"],
            }
            ev = evaluations.get(c["name"])
            if ev:
                entry["score"] = round(ev.overall_score, 2)
                entry["status"] = "PASS" if ev.is_compliant else "FAIL"
                if c["name"] == best_name:
                    entry["recommended"] = True
            catalog.append(entry)

        result: dict[str, Any] = {"contracts": catalog}
        if best_name:
            result["recommended"] = best_name
        else:
            result["hint"] = (
                "No contracts pass. Use get_quality(gate=...) to see violations "
                "and fix actions, then apply_fix to address them."
            )
    else:
        for c in contracts:
            catalog.append(
                {
                    "name": c["name"],
                    "display_name": c["display_name"],
                    "description": c["description"],
                    "threshold": c["overall_threshold"],
                }
            )
        result = {
            "contracts": catalog,
            "hint": (
                "Compliance status available after pipeline runs entropy phase. "
                "Pass contract to analyze to select one."
            ),
        }

    return result
