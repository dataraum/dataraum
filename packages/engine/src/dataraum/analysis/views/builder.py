"""Enriched view SQL builder.

Generates DuckDB CREATE VIEW statements that LEFT JOIN fact tables
with their confirmed dimension tables. Views are grain-preserving:
only many_to_one and one_to_one relationships are used.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DimensionJoin:
    """Specification for a single dimension table join."""

    dim_table_name: str
    dim_duckdb_path: str
    fact_fk_column: str
    dim_pk_column: str
    include_columns: list[str] = field(default_factory=list)
    relationship_id: str = ""


@dataclass(frozen=True)
class EnrichedDimColumn:
    """A dimension column exposed on an enriched view, with its typed source.

    ``name`` is the physical view column (``{fk}__{col}``, numeric-suffix disambiguated
    by :func:`build_enriched_view_sql`). The source is named *relationally* — by
    ``(dim_table_name, source_column_name)`` — so the enriched_views phase resolves the
    typed ``source_column_id`` (DAT-811) from the catalog WITHOUT ever reverse-parsing
    ``name``. The builder is the single authority on both the SQL and these refs, so the
    physical name here is byte-identical to the view's column.
    """

    name: str
    dim_table_name: str
    source_column_name: str


def build_enriched_view_sql(
    view_fqn: str,
    fact_fqn: str,
    dimension_joins: list[DimensionJoin],
) -> tuple[str, list[EnrichedDimColumn]]:
    """Build the ``CREATE OR REPLACE VIEW`` SQL for an enriched view.

    Every table identity is a fully-qualified DuckDB name
    (``catalog.schema."name"``) **composed by the caller** — the view target and
    each source — so the view is collision-free across sources (the bare
    ``enriched_{table}`` name would clash for two sources that each have an
    ``orders`` fact). The caller derives ``view_fqn`` from the fact's
    collision-safe ``duckdb_path`` (``enriched_{source}__{table}``), so a
    ``CREATE OR REPLACE`` only ever replaces *this* view on a re-run.

    Column naming:
    - Fact columns: ``f.*`` (all columns from the fact table)
    - Dimension columns: ``{fact_fk_column}__{column}`` (FK-prefixed so repeated
      joins of the same dimension table stay distinct)

    Args:
        view_fqn: Fully-qualified create target, e.g. ``lake.typed."enriched_csv__orders"``.
        fact_fqn: Fully-qualified fact-table source.
        dimension_joins: Joins to include; each ``dim_duckdb_path`` is the
            dimension's FQN.

    Returns:
        Tuple of ``(create_view_sql, dim_columns)`` where ``dim_columns`` is the list of
        :class:`EnrichedDimColumn` refs (physical view name + its typed source), in view
        order. Empty when the view is a bare ``SELECT *`` passthrough (no joins).
    """
    if not dimension_joins:
        # No joins — view is just the fact table.
        sql = f"CREATE OR REPLACE VIEW {view_fqn} AS SELECT * FROM {fact_fqn}"
        return sql, []

    # Build SELECT clause
    select_parts = ["f.*"]
    dim_columns: list[EnrichedDimColumn] = []

    # Track used aliases to avoid duplicates
    used_aliases: dict[str, int] = {}

    def get_unique_alias(table_name: str) -> str:
        """Generate a unique alias for a dimension table."""
        base_alias = _table_alias(table_name)
        if base_alias not in used_aliases:
            used_aliases[base_alias] = 1
            return base_alias
        # Add numeric suffix for duplicates
        used_aliases[base_alias] += 1
        return f"{base_alias}{used_aliases[base_alias]}"

    # Pre-compute aliases for all joins
    join_aliases = [get_unique_alias(join.dim_table_name) for join in dimension_joins]

    # Output column names must be unique by construction — the ``{fact_fk}__{col}``
    # prefix does NOT guarantee it (two joins can share a fact column), and a
    # duplicate would violate the enriched-layer ``uq_table_column`` on registration.
    # Disambiguate with a numeric suffix, mirroring ``get_unique_alias``.
    used_column_names: dict[str, int] = {}

    def get_unique_column_name(base: str) -> str:
        if base not in used_column_names:
            used_column_names[base] = 1
            return base
        used_column_names[base] += 1
        return f"{base}_{used_column_names[base]}"

    for join, alias in zip(dimension_joins, join_aliases, strict=True):
        # Use fact FK column as prefix so repeated joins of the same dim table are distinct:
        # e.g. kontonummer_des_gegenkontos__beschriftung vs kontonummer_des_kontos__beschriftung
        col_prefix = join.fact_fk_column
        for col in join.include_columns:
            qualified_name = get_unique_column_name(f"{col_prefix}__{col}")
            select_parts.append(f'"{alias}"."{col}" AS "{qualified_name}"')
            dim_columns.append(
                EnrichedDimColumn(
                    name=qualified_name,
                    dim_table_name=join.dim_table_name,
                    source_column_name=col,
                )
            )

    select_clause = ",\n    ".join(select_parts)

    # Build FROM + JOIN clauses
    join_clauses = []
    for join, alias in zip(dimension_joins, join_aliases, strict=True):
        join_clauses.append(
            f'LEFT JOIN {join.dim_duckdb_path} AS "{alias}" '
            f'ON f."{join.fact_fk_column}" = "{alias}"."{join.dim_pk_column}"'
        )

    joins_sql = "\n".join(join_clauses)

    sql = (
        f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
        f"SELECT\n    {select_clause}\n"
        f"FROM {fact_fqn} AS f\n"
        f"{joins_sql}"
    )

    return sql, dim_columns


def _table_alias(table_name: str) -> str:
    """Generate a short alias for a dimension table.

    The initials form can collide with a SQL reserved word (e.g.
    ``accounts_source`` → ``as``); every use site quotes the alias (``AS
    "{alias}"``) so DuckDB accepts it as an identifier rather than failing to
    parse the join.
    """
    # Use first letter of each word, or first 3 chars
    parts = table_name.split("_")
    if len(parts) > 1:
        return "".join(p[0] for p in parts if p)
    return table_name[:3]
