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


def build_enriched_view_sql(
    fact_table_name: str,
    fact_duckdb_path: str,
    dimension_joins: list[DimensionJoin],
) -> tuple[str, str, list[str]]:
    """Build SQL for an enriched view joining fact + dimension tables.

    Creates a DuckDB CREATE OR REPLACE VIEW statement that LEFT JOINs
    the fact table with qualifying dimension tables.

    Column naming:
    - Fact columns: f.* (all columns from fact table)
    - Dimension columns: {dim_table}__{column} (excluding PK/FK columns)

    Args:
        fact_table_name: Name of the fact table
        fact_duckdb_path: DuckDB path to the fact table
        dimension_joins: List of dimension joins to include

    Returns:
        Tuple of (view_name, create_view_sql, dimension_column_names)
    """
    view_name = f"enriched_{fact_table_name}"

    if not dimension_joins:
        # No joins — view is just the fact table
        sql = f'CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM "{fact_duckdb_path}"'
        return view_name, sql, []

    # Build SELECT clause
    select_parts = ["f.*"]
    dimension_column_names: list[str] = []

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

    for join, alias in zip(dimension_joins, join_aliases, strict=True):
        # Use fact FK column as prefix so repeated joins of the same dim table are distinct:
        # e.g. kontonummer_des_gegenkontos__beschriftung vs kontonummer_des_kontos__beschriftung
        col_prefix = join.fact_fk_column
        for col in join.include_columns:
            qualified_name = f"{col_prefix}__{col}"
            select_parts.append(f'{alias}."{col}" AS "{qualified_name}"')
            dimension_column_names.append(qualified_name)

    select_clause = ",\n    ".join(select_parts)

    # Build FROM + JOIN clauses
    join_clauses = []
    for join, alias in zip(dimension_joins, join_aliases, strict=True):
        join_clauses.append(
            f'LEFT JOIN "{join.dim_duckdb_path}" AS {alias} '
            f'ON f."{join.fact_fk_column}" = {alias}."{join.dim_pk_column}"'
        )

    joins_sql = "\n".join(join_clauses)

    sql = (
        f'CREATE OR REPLACE VIEW "{view_name}" AS\n'
        f"SELECT\n    {select_clause}\n"
        f'FROM "{fact_duckdb_path}" AS f\n'
        f"{joins_sql}"
    )

    return view_name, sql, dimension_column_names


def _table_alias(table_name: str) -> str:
    """Generate a short alias for a dimension table."""
    # Use first letter of each word, or first 3 chars
    parts = table_name.split("_")
    if len(parts) > 1:
        return "".join(p[0] for p in parts if p)
    return table_name[:3]
