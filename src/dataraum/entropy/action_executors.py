"""Concrete action executors for seed fix actions.

Each executor follows the signature:
    (session, duckdb_conn, target, parameters) -> dict[str, Any]

The return dict should contain:
- "improved": bool — whether the fix improved the situation
- "evidence": str — human-readable summary of what was done
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.entropy.fix_executor import ActionCategory, ActionDefinition


def execute_override_type(
    session: Session,
    duckdb_conn: Any,
    target: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Override a column's type by updating the TypeDecision.

    Parameters:
        target_type: The new type to set (e.g., "DECIMAL(10,2)")
        reason: Why this override was applied
    """
    from dataraum.analysis.typing.db_models import TypeDecision

    target_type = parameters.get("target_type", "")
    reason = parameters.get("reason", "Manual type override via gate fix")

    # Parse target: "column:table.column"
    _, ref = target.split(":", 1) if ":" in target else ("", target)
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return {"improved": False, "evidence": f"Invalid target format: {target}"}

    table_name, column_name = parts

    # Find the TypeDecision for this column
    from dataraum.storage import Column, Table

    table = session.execute(
        select(Table).where(Table.table_name == table_name, Table.layer == "typed")
    ).scalar_one_or_none()

    if not table:
        return {"improved": False, "evidence": f"Table not found: {table_name}"}

    column = session.execute(
        select(Column).where(Column.table_id == table.table_id, Column.column_name == column_name)
    ).scalar_one_or_none()

    if not column:
        return {"improved": False, "evidence": f"Column not found: {column_name}"}

    td = session.execute(
        select(TypeDecision).where(TypeDecision.column_id == column.column_id)
    ).scalar_one_or_none()

    if td:
        old_type = td.decided_type
        td.decided_type = target_type
        td.decision_source = "gate_fix"
        td.decision_reason = reason
    else:
        return {"improved": False, "evidence": f"No TypeDecision for {target}"}

    return {
        "improved": True,
        "evidence": f"Changed type from {old_type} to {target_type}: {reason}",
    }


def execute_declare_unit(
    session: Session,
    duckdb_conn: Any,
    target: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Declare or update a column's unit in SemanticAnnotation.

    Parameters:
        unit: The unit to declare (e.g., "EUR", "kg")
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column, Table

    unit = parameters.get("unit", "")
    _, ref = target.split(":", 1) if ":" in target else ("", target)
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return {"improved": False, "evidence": f"Invalid target: {target}"}

    table_name, column_name = parts

    table = session.execute(
        select(Table).where(Table.table_name == table_name, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return {"improved": False, "evidence": f"Table not found: {table_name}"}

    column = session.execute(
        select(Column).where(Column.table_id == table.table_id, Column.column_name == column_name)
    ).scalar_one_or_none()
    if not column:
        return {"improved": False, "evidence": f"Column not found: {column_name}"}

    ann = session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == column.column_id)
    ).scalar_one_or_none()

    if ann:
        # Store unit declaration in business_description as structured note
        # The actual unit is tracked via TypeCandidate.detected_unit
        from dataraum.analysis.typing.db_models import TypeCandidate

        tc = session.execute(
            select(TypeCandidate)
            .where(TypeCandidate.column_id == column.column_id)
            .order_by(TypeCandidate.confidence.desc())
            .limit(1)
        ).scalar_one_or_none()

        if tc:
            tc.detected_unit = unit
            tc.unit_confidence = 1.0

        return {"improved": True, "evidence": f"Declared unit '{unit}' on {target}"}
    else:
        return {"improved": False, "evidence": f"No SemanticAnnotation for {target}"}


def execute_add_business_name(
    session: Session,
    duckdb_conn: Any,
    target: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Add or update a column's business name.

    Parameters:
        business_name: The human-readable business name
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column, Table

    business_name = parameters.get("business_name", "")
    _, ref = target.split(":", 1) if ":" in target else ("", target)
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return {"improved": False, "evidence": f"Invalid target: {target}"}

    table_name, column_name = parts

    table = session.execute(
        select(Table).where(Table.table_name == table_name, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return {"improved": False, "evidence": f"Table not found: {table_name}"}

    column = session.execute(
        select(Column).where(Column.table_id == table.table_id, Column.column_name == column_name)
    ).scalar_one_or_none()
    if not column:
        return {"improved": False, "evidence": f"Column not found: {column_name}"}

    ann = session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == column.column_id)
    ).scalar_one_or_none()

    if ann:
        ann.business_name = business_name
        return {"improved": True, "evidence": f"Set business name '{business_name}' on {target}"}
    else:
        return {"improved": False, "evidence": f"No SemanticAnnotation for {target}"}


def execute_declare_null_meaning(
    session: Session,
    duckdb_conn: Any,
    target: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Document null semantics for a column.

    Parameters:
        meaning: What nulls mean (e.g., "not applicable", "unknown", "missing")
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column, Table

    meaning = parameters.get("meaning", "")
    _, ref = target.split(":", 1) if ":" in target else ("", target)
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return {"improved": False, "evidence": f"Invalid target: {target}"}

    table_name, column_name = parts

    table = session.execute(
        select(Table).where(Table.table_name == table_name, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return {"improved": False, "evidence": f"Table not found: {table_name}"}

    column = session.execute(
        select(Column).where(Column.table_id == table.table_id, Column.column_name == column_name)
    ).scalar_one_or_none()
    if not column:
        return {"improved": False, "evidence": f"Column not found: {column_name}"}

    ann = session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == column.column_id)
    ).scalar_one_or_none()

    if ann:
        # Append null semantics to business_description since there's no dedicated field
        null_note = f"[null_meaning: {meaning}]"
        if ann.business_description:
            if "[null_meaning:" not in ann.business_description:
                ann.business_description = f"{ann.business_description} {null_note}"
        else:
            ann.business_description = null_note
        return {"improved": True, "evidence": f"Documented null meaning '{meaning}' on {target}"}
    else:
        return {"improved": False, "evidence": f"No SemanticAnnotation for {target}"}


def execute_confirm_relationship(
    session: Session,
    duckdb_conn: Any,
    target: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Confirm an auto-detected relationship.

    Parameters:
        relationship_id: The relationship ID to confirm
    """
    from dataraum.analysis.relationships.db_models import Relationship

    relationship_id = parameters.get("relationship_id", "")
    if not relationship_id:
        return {"improved": False, "evidence": "No relationship_id provided"}

    rel = session.execute(
        select(Relationship).where(Relationship.relationship_id == relationship_id)
    ).scalar_one_or_none()

    if rel:
        rel.is_confirmed = True
        return {"improved": True, "evidence": f"Confirmed relationship {relationship_id}"}
    else:
        return {"improved": False, "evidence": f"Relationship not found: {relationship_id}"}


def execute_create_filtered_view(
    session: Session,
    duckdb_conn: Any,
    target: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Create a filtered view excluding problematic rows.

    Parameters:
        filter_sql: SQL WHERE clause for filtering
        view_name: Name for the filtered view
    """
    filter_sql = parameters.get("filter_sql", "")
    view_name = parameters.get("view_name", "")

    if not filter_sql or not view_name:
        return {"improved": False, "evidence": "filter_sql and view_name required"}

    if duckdb_conn is None:
        return {"improved": False, "evidence": "DuckDB connection required for views"}

    # Parse table from target
    _, ref = target.split(":", 1) if ":" in target else ("", target)
    table_name = ref.split(".")[0]

    try:
        duckdb_conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_name} WHERE {filter_sql}"
        )
        return {"improved": True, "evidence": f"Created view {view_name} with filter: {filter_sql}"}
    except Exception as e:
        return {"improved": False, "evidence": f"Failed to create view: {e}"}


def get_seed_actions() -> list[ActionDefinition]:
    """Return all seed action definitions."""
    return [
        ActionDefinition(
            action_type="override_type",
            category=ActionCategory.TRANSFORM,
            description="Override a column's detected type",
            hard_verifiable=True,
            parameters_schema={
                "target_type": "The new type (e.g., DECIMAL(10,2))",
                "reason": "Why this override was applied",
            },
            executor=execute_override_type,
        ),
        ActionDefinition(
            action_type="declare_unit",
            category=ActionCategory.ANNOTATE,
            description="Declare or update a column's unit",
            hard_verifiable=False,
            parameters_schema={"unit": "The unit (e.g., EUR, kg)"},
            executor=execute_declare_unit,
        ),
        ActionDefinition(
            action_type="add_business_name",
            category=ActionCategory.ANNOTATE,
            description="Add a human-readable business name to a column",
            hard_verifiable=False,
            parameters_schema={"business_name": "The business name"},
            executor=execute_add_business_name,
        ),
        ActionDefinition(
            action_type="declare_null_meaning",
            category=ActionCategory.ANNOTATE,
            description="Document what null values mean for a column",
            hard_verifiable=False,
            parameters_schema={"meaning": "What nulls mean"},
            executor=execute_declare_null_meaning,
        ),
        ActionDefinition(
            action_type="confirm_relationship",
            category=ActionCategory.ANNOTATE,
            description="Confirm an auto-detected relationship",
            hard_verifiable=False,
            parameters_schema={"relationship_id": "The relationship ID"},
            executor=execute_confirm_relationship,
        ),
        ActionDefinition(
            action_type="create_filtered_view",
            category=ActionCategory.TRANSFORM,
            description="Create a DuckDB view excluding problematic rows",
            hard_verifiable=True,
            parameters_schema={
                "filter_sql": "SQL WHERE clause",
                "view_name": "Name for the view",
            },
            executor=execute_create_filtered_view,
        ),
    ]
