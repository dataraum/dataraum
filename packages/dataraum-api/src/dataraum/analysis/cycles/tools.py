"""Tools for the business cycle detection agent.

These tools allow the LLM agent to query specific data on-demand
to validate hypotheses and gather detailed metrics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    import duckdb


class CycleDetectionTools:
    """Tools available to the business cycle detection agent.

    Each method is a tool the agent can call to get specific information.
    The tools operate on the existing metadata + can query raw data.
    """

    def __init__(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_id_to_name: dict[str, str],
    ) -> None:
        """Initialize tools.

        Args:
            session: SQLAlchemy session for metadata queries
            duckdb_conn: DuckDB connection for data queries
            table_id_to_name: Mapping of table_id to DuckDB table name
        """
        self._session = session
        self._duckdb = duckdb_conn
        self._table_map = table_id_to_name

    def get_column_value_distribution(
        self,
        table_name: str,
        column_name: str,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Get distinct values and their counts for a column.

        Useful for understanding status columns, category columns,
        and identifying possible cycle stages.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            limit: Max distinct values to return

        Returns:
            Distribution info with distinct values and counts
        """
        try:
            duckdb_table = f"typed_{table_name}"
            result = self._duckdb.execute(
                f'SELECT "{column_name}", COUNT(*) as cnt '
                f'FROM "{duckdb_table}" '
                f'WHERE "{column_name}" IS NOT NULL '
                f'GROUP BY "{column_name}" '
                f"ORDER BY cnt DESC "
                f"LIMIT {limit}"
            ).fetchall()

            total_row = self._duckdb.execute(f'SELECT COUNT(*) FROM "{duckdb_table}"').fetchone()
            total = total_row[0] if total_row else 0

            null_row = self._duckdb.execute(
                f'SELECT COUNT(*) FROM "{duckdb_table}" WHERE "{column_name}" IS NULL'
            ).fetchone()
            null_count = null_row[0] if null_row else 0

            return {
                "table": table_name,
                "column": column_name,
                "total_rows": total,
                "null_count": null_count,
                "null_rate": null_count / total if total > 0 else 0,
                "distinct_count": len(result),
                "values": [
                    {"value": str(r[0]), "count": r[1], "percentage": r[1] / total * 100}
                    for r in result
                ],
            }
        except Exception as e:
            return {"error": str(e), "table": table_name, "column": column_name}

    def get_cycle_completion_metrics(
        self,
        table_name: str,
        entity_column: str,
        status_column: str,
        completion_value: str,
    ) -> dict[str, Any]:
        """Get metrics for a potential business cycle.

        Calculates completion rate, counts, and other metrics
        for a cycle identified by entity + status columns.

        Args:
            table_name: Table containing the transactions
            entity_column: Column identifying the entity (customer, vendor, etc.)
            status_column: Column indicating cycle status
            completion_value: Value that indicates cycle completion (e.g., "Paid")

        Returns:
            Cycle metrics including completion rate, counts
        """
        try:
            duckdb_table = f"typed_{table_name}"

            # Total entities
            row = self._duckdb.execute(
                f'SELECT COUNT(DISTINCT "{entity_column}") '
                f'FROM "{duckdb_table}" '
                f'WHERE "{entity_column}" IS NOT NULL'
            ).fetchone()
            total_entities = row[0] if row else 0

            # Total records
            row = self._duckdb.execute(f'SELECT COUNT(*) FROM "{duckdb_table}"').fetchone()
            total_records = row[0] if row else 0

            # Completed (status = completion_value)
            row = self._duckdb.execute(
                f'SELECT COUNT(*) FROM "{duckdb_table}" WHERE "{status_column}" = ?',
                [completion_value],
            ).fetchone()
            completed = row[0] if row else 0

            # Entities with completed cycles
            row = self._duckdb.execute(
                f'SELECT COUNT(DISTINCT "{entity_column}") '
                f'FROM "{duckdb_table}" '
                f'WHERE "{status_column}" = ?',
                [completion_value],
            ).fetchone()
            entities_completed = row[0] if row else 0

            return {
                "table": table_name,
                "entity_column": entity_column,
                "status_column": status_column,
                "completion_value": completion_value,
                "total_records": total_records,
                "total_entities": total_entities,
                "completed_records": completed,
                "completed_entities": entities_completed,
                "record_completion_rate": completed / total_records if total_records > 0 else 0,
                "entity_completion_rate": entities_completed / total_entities
                if total_entities > 0
                else 0,
            }
        except Exception as e:
            return {"error": str(e)}

    def get_entity_transaction_flow(
        self,
        table_name: str,
        entity_column: str,
        type_column: str,
        date_column: str | None = None,
        sample_size: int = 5,
    ) -> dict[str, Any]:
        """Get sample transaction flows for an entity.

        Shows how transactions progress for sample entities,
        useful for understanding the actual cycle flow.

        Args:
            table_name: Table containing transactions
            entity_column: Column identifying the entity
            type_column: Column indicating transaction type
            date_column: Optional date column for ordering
            sample_size: Number of entities to sample

        Returns:
            Sample transaction flows showing cycle progression
        """
        try:
            duckdb_table = f"typed_{table_name}"

            # Get sample entities
            entities = self._duckdb.execute(
                f'SELECT DISTINCT "{entity_column}" '
                f'FROM "{duckdb_table}" '
                f'WHERE "{entity_column}" IS NOT NULL '
                f"LIMIT {sample_size}"
            ).fetchall()

            flows = []
            for (entity,) in entities:
                order_by = f'"{date_column}"' if date_column else "1"
                txns = self._duckdb.execute(
                    f'SELECT "{type_column}", '
                    f"{f'''"{date_column}",''' if date_column else ''} "
                    f"COUNT(*) as cnt "
                    f'FROM "{duckdb_table}" '
                    f'WHERE "{entity_column}" = ? '
                    f'GROUP BY "{type_column}"'
                    f"{f''', "{date_column}" ''' if date_column else ''}"
                    f"ORDER BY {order_by} "
                    f"LIMIT 20",
                    [entity],
                ).fetchall()

                flows.append(
                    {
                        "entity": str(entity),
                        "transactions": [
                            {
                                "type": t[0],
                                "date": str(t[1]) if date_column else None,
                                "count": t[-1],
                            }
                            for t in txns
                        ],
                    }
                )

            return {
                "table": table_name,
                "entity_column": entity_column,
                "type_column": type_column,
                "sample_flows": flows,
            }
        except Exception as e:
            return {"error": str(e)}

    def get_correlation_between_columns(
        self,
        table_name: str,
        column1: str,
        column2: str,
    ) -> dict[str, Any]:
        """Check if two columns are correlated/related.

        Useful for validating if columns participate in the same cycle.

        Args:
            table_name: Table containing the columns
            column1: First column
            column2: Second column

        Returns:
            Correlation/association metrics
        """
        from dataraum.analysis.correlation.db_models import (
            CategoricalAssociation,
            ColumnCorrelation,
        )
        from dataraum.storage import Column, Table

        try:
            # Get column IDs
            table_stmt = select(Table).where(Table.table_name == table_name)
            table = self._session.execute(table_stmt).scalar_one_or_none()
            if not table:
                return {"error": f"Table {table_name} not found"}

            col1_stmt = select(Column).where(
                Column.table_id == table.table_id,
                Column.column_name == column1,
            )
            col2_stmt = select(Column).where(
                Column.table_id == table.table_id,
                Column.column_name == column2,
            )
            col1 = self._session.execute(col1_stmt).scalar_one_or_none()
            col2 = self._session.execute(col2_stmt).scalar_one_or_none()

            if not col1 or not col2:
                return {"error": "Column(s) not found"}

            # Check numeric correlation
            corr_stmt = select(ColumnCorrelation).where(
                ColumnCorrelation.table_id == table.table_id,
                ColumnCorrelation.column1_id == col1.column_id,
                ColumnCorrelation.column2_id == col2.column_id,
            )
            corr = self._session.execute(corr_stmt).scalar_one_or_none()

            # Check categorical association
            assoc_stmt = select(CategoricalAssociation).where(
                CategoricalAssociation.table_id == table.table_id,
                CategoricalAssociation.column1_id == col1.column_id,
                CategoricalAssociation.column2_id == col2.column_id,
            )
            assoc = self._session.execute(assoc_stmt).scalar_one_or_none()

            result: dict[str, Any] = {
                "table": table_name,
                "column1": column1,
                "column2": column2,
            }

            if corr:
                result["numeric_correlation"] = {
                    "pearson_r": corr.pearson_r,
                    "spearman_rho": corr.spearman_rho,
                    "strength": corr.correlation_strength,
                    "is_significant": corr.is_significant,
                }

            if assoc:
                result["categorical_association"] = {
                    "cramers_v": assoc.cramers_v,
                    "chi_square": assoc.chi_square,
                    "strength": assoc.association_strength,
                    "is_significant": assoc.is_significant,
                }

            if not corr and not assoc:
                result["message"] = "No pre-computed correlation/association found"

            return result
        except Exception as e:
            return {"error": str(e)}

    def get_functional_dependencies(
        self,
        table_name: str,
    ) -> dict[str, Any]:
        """Get functional dependencies for a table.

        Shows which columns determine other columns,
        useful for understanding data structure.

        Args:
            table_name: Table to analyze

        Returns:
            List of functional dependencies
        """
        from dataraum.analysis.correlation.db_models import FunctionalDependency
        from dataraum.storage import Column, Table

        try:
            # Filter by layer='typed' to avoid multiple results (raw, typed, quarantine)
            table_stmt = select(Table).where(
                Table.table_name == table_name,
                Table.layer == "typed",
            )
            table = self._session.execute(table_stmt).scalar_one_or_none()
            if not table:
                return {"error": f"Table {table_name} not found"}

            deps_stmt = select(FunctionalDependency).where(
                FunctionalDependency.table_id == table.table_id,
                FunctionalDependency.confidence > 0.9,
            )
            deps = self._session.execute(deps_stmt).scalars().all()

            # Get column names
            cols_stmt = select(Column).where(Column.table_id == table.table_id)
            cols = self._session.execute(cols_stmt).scalars().all()
            col_map = {c.column_id: c.column_name for c in cols}

            return {
                "table": table_name,
                "dependencies": [
                    {
                        "determinant": [
                            col_map.get(cid, cid) for cid in dep.determinant_column_ids
                        ],
                        "dependent": col_map.get(dep.dependent_column_id, dep.dependent_column_id),
                        "confidence": dep.confidence,
                        "violations": dep.violation_count,
                    }
                    for dep in deps
                ],
            }
        except Exception as e:
            return {"error": str(e)}


def get_tool_definitions() -> list[dict[str, Any]]:
    """Get JSON schema definitions for the tools.

    Returns tool definitions in the format expected by Claude's tool use.
    Includes both exploration tools and the submit_analysis tool for final output.
    """
    from dataraum.analysis.cycles.models import BusinessCycleAnalysisOutput

    return [
        # Submit analysis tool - for structured final output
        {
            "name": "submit_analysis",
            "description": (
                "Submit your final business cycle analysis. Call this tool ONLY when you have "
                "gathered enough information and are ready to present your findings. "
                "This is a REQUIRED final step - you must call this tool to complete the analysis."
            ),
            "input_schema": BusinessCycleAnalysisOutput.model_json_schema(),
        },
        # Exploration tools
        {
            "name": "get_column_value_distribution",
            "description": "Get distinct values and their counts for a column. Useful for understanding status columns, category columns, and identifying possible cycle stages.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (without 'typed_' prefix)",
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Name of the column to analyze",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max distinct values to return (default 20)",
                        "default": 20,
                    },
                },
                "required": ["table_name", "column_name"],
            },
        },
        {
            "name": "get_cycle_completion_metrics",
            "description": "Get metrics for a potential business cycle. Calculates completion rate based on entity column and status column.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table containing the transactions",
                    },
                    "entity_column": {
                        "type": "string",
                        "description": "Column identifying the entity (customer, vendor, etc.)",
                    },
                    "status_column": {
                        "type": "string",
                        "description": "Column indicating cycle status",
                    },
                    "completion_value": {
                        "type": "string",
                        "description": "Value that indicates cycle completion (e.g., 'Paid')",
                    },
                },
                "required": ["table_name", "entity_column", "status_column", "completion_value"],
            },
        },
        {
            "name": "get_entity_transaction_flow",
            "description": "Get sample transaction flows for entities. Shows how transactions progress, useful for understanding the actual cycle flow.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table containing transactions",
                    },
                    "entity_column": {
                        "type": "string",
                        "description": "Column identifying the entity",
                    },
                    "type_column": {
                        "type": "string",
                        "description": "Column indicating transaction type",
                    },
                    "date_column": {
                        "type": "string",
                        "description": "Optional date column for ordering",
                    },
                    "sample_size": {
                        "type": "integer",
                        "description": "Number of entities to sample (default 5)",
                        "default": 5,
                    },
                },
                "required": ["table_name", "entity_column", "type_column"],
            },
        },
        {
            "name": "get_functional_dependencies",
            "description": "Get functional dependencies for a table. Shows which columns determine other columns.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table to analyze",
                    },
                },
                "required": ["table_name"],
            },
        },
    ]
