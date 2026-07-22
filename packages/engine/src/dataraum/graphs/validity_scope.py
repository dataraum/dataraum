"""Default composition of the canonical validity scope onto a grounding (DAT-733).

The analytical-universe predicate (posted-/reconciled-/active-only) is a MEASURED
business cycle's ``status_column = completion_value``. A grounding composes it BY
DEFAULT: the engine resolves the scope applicable to the extract's relation and
appends it as an additional typed WHERE part, so it rides the SAME
parts/where_predicates substrate every consumer already reads (current_groundings,
og_grounding, the cockpit drill). Opting out is explicit and visible — the caller
records a typed assumption when a grounding legitimately constrains the status
column itself; it is never a silent absence of the filter.

Sourced RUN-SCOPED from the served rich context (``business_cycles``), NOT the
promoted-head ``og_validity_filter`` graph view: mid-run the operating_model head is
not yet promoted, and the metrics phase passes ``om_run_id`` so the context sees
this run's cycles. Same source (``detected_business_cycles``), two correct scopes.

TYPED PREDICATES ONLY: a scope is a ``(column, operator, value)`` triple; the SQL
string is a deterministic render of that triple, never free-text authoring.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataraum.graphs.grounding_validation import where_filter_columns
from dataraum.graphs.models import AssumptionBasis, GraphAssumptionOutput

if TYPE_CHECKING:
    import duckdb

    from dataraum.graphs.context import (
        BusinessCycleContext,
        EnrichedViewContext,
        TableContext,
    )
    from dataraum.graphs.models import ExtractGroundingOutput


@dataclass(frozen=True)
class ValidityScope:
    """One typed validity predicate over a relation: ``(column, operator, value)``."""

    column: str  # bare column name, resolved to exist on the relation
    operator: str  # currently always "=" (a completion value is a single value)
    value: str  # the cycle's completion value

    def render(self) -> str:
        """The SQL predicate text over the relation (single quotes doubled)."""
        escaped = self.value.replace("'", "''")
        return f"{self.column} {self.operator} '{escaped}'"


def _name_forms(name: str, tables: list[TableContext]) -> set[str]:
    """Both name forms (``table_name`` and ``duckdb_name``) of the table ``name`` names.

    So a cycle's ``status_table`` matches whichever namespace it was stored in —
    the Python mirror of the element views' ``table_name OR duckdb_path`` match (the
    two can legitimately diverge; nothing enforces they don't).
    """
    forms = {name}
    for table in tables:
        if name in (table.table_name, table.duckdb_name):
            forms.add(table.table_name)
            if table.duckdb_name:
                forms.add(table.duckdb_name)
    return forms


def resolve_validity_scopes(
    business_cycles: list[BusinessCycleContext],
    relation: str,
    served_columns: set[str],
    enriched_views: list[EnrichedViewContext],
    tables: list[TableContext],
) -> list[ValidityScope]:
    """The validity scopes that apply to a grounding reading ``relation``.

    A cycle contributes its ``status_column = completion_value`` scope when ALL hold:

    * it is MEASURED — ``completion_rate is not None``, the persisted proxy for the
      cycles contract's ``measured`` discriminator (an unmeasured cycle NULLs its
      three metrics; DAT-807). An unmeasured cycle contributes NO filter;
    * it carries both a bare ``status_column`` and a ``completion_value``;
    * ``relation`` resolves to the cycle's ``status_table`` — the typed table itself
      (matched on EITHER name form via ``tables``), or an enriched view whose fact
      base IS that table;
    * the bare status column is present among the relation's served columns — the
      honest presence test, so the appended predicate can only reference a column
      that actually exists on what the grounding queries.

    Duplicate scopes (two cycles sharing a status column + value) collapse to one.
    Absence falls loud: no qualifying cycle ⇒ ``[]`` ⇒ no predicate, never a
    fabricated default-true scope.
    """
    # relation → the base tables it reads, in BOTH name forms: itself, plus the fact
    # base of an enriched view of that name (an enriched view carries its fact's
    # status column).
    scope_tables = _name_forms(relation, tables)
    for view in enriched_views:
        if view.view_name == relation and view.fact_table:
            scope_tables |= _name_forms(view.fact_table, tables)

    scopes: list[ValidityScope] = []
    seen: set[tuple[str, str]] = set()
    for cycle in business_cycles:
        if cycle.completion_rate is None:  # unmeasured → no filter
            continue
        column, value, table = (
            cycle.status_column,
            cycle.completion_value,
            cycle.status_table,
        )
        if not column or not value or not table:
            continue
        if table not in scope_tables:
            continue
        if column not in served_columns:  # honest presence test — no fabrication
            continue
        if (column, value) in seen:  # two cycles, same predicate → one part
            continue
        seen.add((column, value))
        scopes.append(ValidityScope(column=column, operator="=", value=value))
    return scopes


def compose_scoped_where(
    output: ExtractGroundingOutput,
    relation: str | None,
    served_columns: set[str],
    business_cycles: list[BusinessCycleContext],
    enriched_views: list[EnrichedViewContext],
    tables: list[TableContext],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> tuple[list[str], list[GraphAssumptionOutput]]:
    """Compose the default validity scope onto a grounding's WHERE parts (DAT-733).

    Returns ``(where_parts, scope_assumptions)``:

    * ``where_parts`` — the grounding's ``where`` plus each applicable scope
      predicate the grounding does not ALREADY constrain, so the scope rides the
      same parts substrate every consumer reads;
    * ``scope_assumptions`` — one typed ``INFERRED`` assumption per DEFERRED scope:
      when the grounding already filters on the status column (the LLM's judgment),
      the default is not applied and the opt-out is recorded VISIBLY rather than
      being a silent absence.

    A fall-loud grounding (``relation`` None) or one with no applicable cycle gets
    its ``where`` back unchanged and no assumptions — absence falls loud.
    """
    where_parts = list(output.where)
    scope_assumptions: list[GraphAssumptionOutput] = []
    if relation is None:
        return where_parts, scope_assumptions
    scopes = resolve_validity_scopes(
        business_cycles, relation, served_columns, enriched_views, tables
    )
    if not scopes:
        return where_parts, scope_assumptions
    constrained = where_filter_columns(output, served_columns, duckdb_conn)
    for scope in scopes:
        if scope.column in constrained:
            scope_assumptions.append(
                GraphAssumptionOutput(
                    dimension="scope.validity",
                    target=f"column:{relation}.{scope.column}",
                    assumption=(
                        f"default validity scope {scope.render()} not applied — "
                        f"grounding constrains {scope.column} directly"
                    ),
                    basis=AssumptionBasis.INFERRED,
                    confidence=1.0,
                )
            )
        else:
            where_parts.append(scope.render())
    return where_parts, scope_assumptions
