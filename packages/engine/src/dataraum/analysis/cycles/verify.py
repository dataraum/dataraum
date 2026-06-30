"""Membership floor for detected cycles (DAT-630).

A guardrail on the cycle agent's output — not a detector. The LLM authors the
cycles; this rejects any whose cited **categorical** reference (a
``status_column``, a ``completion_value``, a stage ``indicator_value``, an
entity/fact column) does NOT appear in the served context. An improvised column
or value is a hallucination; dropping the cycle is the same role
``graphs/verifier.py`` plays for metrics.

**Scope — categorical only, by design.** This floor covers the status-completion
path: it catches a made-up column or value. It does NOT police the
numeric-completion path — a cycle that completes on a derived relationship
carries no status column/stages and so passes through untouched. The numeric
path's honesty rests on the prompt's cite-only-served discipline plus the phase
confidence gate (low confidence → flagged), not on this floor. We deliberately
don't cross-check the claimed number here: re-judging a numeric signal would
creep back toward the deterministic detector this design rejects. If a fabricated
numeric cycle ever surfaces on real data, tighten then — not preemptively against
synthetic edge cases.

Membership-only: a value is rejected ONLY when the cited column has a served
value-set the value is absent from — when no value-set was served we cannot prove
improvisation, so we don't reject (no false-loud).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.analysis.cycles.models import DetectedCycle

# Cycle context cites tables by plain name; the field-mapping feed prefixes the
# storage layer (``typed_journal``). Tolerate the prefix so a cycle grounded via
# a mapping isn't falsely rejected for naming the same table differently.
_LAYER_PREFIXES = ("typed_", "raw_", "quarantine_", "staged_")


def _resolve_table(name: str | None, cols_by_table: dict[str, set[str]]) -> str | None:
    """Resolve a cited table name to a served table, stripping a layer prefix."""
    if not name:
        return None
    if name in cols_by_table:
        return name
    for prefix in _LAYER_PREFIXES:
        if name.startswith(prefix) and name[len(prefix) :] in cols_by_table:
            return name[len(prefix) :]
    return None


def _column_absent(
    table: str | None,
    column: str | None,
    cols_by_table: dict[str, set[str]],
) -> str | None:
    """Reason string if ``table.column`` isn't served, else ``None``."""
    if not column:
        return None
    resolved = _resolve_table(table, cols_by_table)
    if resolved is None:
        return f"table '{table}' not in workspace"
    if column not in cols_by_table[resolved]:
        return f"column '{table}.{column}' not in workspace"
    return None


def _verify_cycle(
    cycle: DetectedCycle,
    cols_by_table: dict[str, set[str]],
    slice_values: dict[tuple[str, str], set[str]],
) -> str | None:
    """Reason a cycle is improvised, or ``None`` if every reference is served."""
    # Status column + its completion value.
    if cycle.status_column:
        reason = _column_absent(cycle.status_table, cycle.status_column, cols_by_table)
        if reason:
            return reason
        resolved = _resolve_table(cycle.status_table, cols_by_table)
        key = (resolved, cycle.status_column) if resolved else None
        values = slice_values.get(key) if key else None
        # Only reject a value when the column HAS a served value-set (a slice) and
        # the cited value is absent from it — otherwise we cannot prove it's made up.
        if values is not None and cycle.completion_value and cycle.completion_value not in values:
            return f"completion_value '{cycle.completion_value}' not in {cycle.status_column}"

    # Stage indicator columns + values.
    for stage in cycle.stages:
        reason = _column_absent(cycle.status_table, stage.indicator_column, cols_by_table)
        if reason:
            return reason
        resolved = _resolve_table(cycle.status_table, cols_by_table)
        key = (resolved, stage.indicator_column) if resolved and stage.indicator_column else None
        values = slice_values.get(key) if key else None
        if values is not None:
            for val in stage.indicator_values:
                if val not in values:
                    return f"indicator_value '{val}' not in {stage.indicator_column}"

    # Entity flows — the entity column and (when present) the fact column.
    for flow in cycle.entity_flows:
        reason = _column_absent(flow.entity_table, flow.entity_column, cols_by_table)
        if reason:
            return reason
        if flow.fact_column:
            reason = _column_absent(flow.fact_table, flow.fact_column, cols_by_table)
            if reason:
                return reason

    return None


def verify_cycles(
    cycles: list[DetectedCycle],
    context: dict[str, Any],
) -> tuple[list[DetectedCycle], list[str]]:
    """Drop cycles citing references absent from the served context.

    Args:
        cycles: the agent's detected cycles.
        context: the cycle-detection context (``build_cycle_detection_context``).

    Returns:
        ``(kept, rejections)`` — the surviving cycles and a human-readable reason
        per dropped cycle (for loud logging at the call site).
    """
    cols_by_table: dict[str, set[str]] = {
        t["table_name"]: {c["name"] for c in t["columns"]} for t in context.get("tables", [])
    }
    slice_values: dict[tuple[str, str], set[str]] = {}
    for sd in context.get("slice_definitions", []):
        key = (sd["table_name"], sd["column_name"])
        served = {str(vc["value"]) for vc in sd.get("value_counts", [])} | {
            str(v) for v in sd.get("values", [])
        }
        slice_values[key] = served

    kept: list[DetectedCycle] = []
    rejections: list[str] = []
    for cycle in cycles:
        reason = _verify_cycle(cycle, cols_by_table, slice_values)
        if reason is None:
            kept.append(cycle)
        else:
            rejections.append(f"{cycle.cycle_name}: {reason}")
    return kept, rejections


__all__ = ["verify_cycles"]
