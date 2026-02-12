"""Tools for the business cycle detection agent.

These tools allow the LLM agent to query specific data on-demand
to validate hypotheses and gather detailed metrics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dataraum.analysis.cycles.models import BusinessCycleAnalysisOutput

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


def get_tool_definitions() -> list[dict[str, Any]]:
    """Get JSON schema definitions for the tools.

    Returns tool definitions in the format expected by Claude's tool use.
    Includes both exploration tools and the submit_analysis tool for final output.
    """
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
    ]
