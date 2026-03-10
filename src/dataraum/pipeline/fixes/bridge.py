"""Bridge — converts FixInput + FixSchema into FixDocuments.

This module replaces the per-action handler functions from fix_registry.py.
Instead of hardwired handler functions, it reads the FixSchema from the
detector and builds FixDocuments that the interpreters can apply.
"""

from __future__ import annotations

from dataraum.pipeline.fixes import FixInput
from dataraum.pipeline.fixes.models import FixDocument, FixSchema


def build_fix_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
) -> list[FixDocument]:
    """Build FixDocuments from a FixSchema and user input.

    Routes by operation type:
    - append: One FixDocument per affected_column, appending the column ref
    - merge/set with key_template=None: One FixDocument per affected_column
    - merge/set with key_template: One FixDocument using template-derived key

    Args:
        schema: The fix schema from the detector.
        fix_input: User's structured fix decision.
        table_name: Table this fix applies to.
        column_name: Column this fix applies to (may be None for table-scoped).
        dimension: Entropy dimension being addressed.

    Returns:
        List of FixDocuments ready for interpreter application.
    """
    if schema.target != "config":
        # Only config target is bridged in Phase 3
        return []

    config_path = schema.config_path or ""
    key_path = list(schema.key_path or [])
    operation = schema.operation or "set"

    if operation == "append":
        return _build_append_documents(
            schema, fix_input, table_name, column_name, dimension, config_path, key_path
        )

    if schema.key_template is not None:
        return _build_keyed_documents(
            schema, fix_input, table_name, column_name, dimension, config_path, key_path, operation
        )

    return _build_per_column_documents(
        schema, fix_input, table_name, column_name, dimension, config_path, key_path, operation
    )


def _build_append_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
    config_path: str,
    key_path: list[str],
) -> list[FixDocument]:
    """Build append documents — one per affected column."""
    docs: list[FixDocument] = []
    reason = fix_input.interpretation or f"{schema.action} for {table_name}"

    for i, col_ref in enumerate(fix_input.affected_columns):
        docs.append(
            FixDocument(
                target="config",
                action=schema.action,
                table_name=table_name,
                column_name=column_name,
                dimension=dimension,
                ordinal=i,
                description=f"{schema.action}: {col_ref}",
                payload={
                    "config_path": config_path,
                    "key_path": key_path,
                    "operation": "append",
                    "value": col_ref,
                    "reason": reason,
                },
            )
        )

    return docs


def _build_per_column_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
    config_path: str,
    key_path: list[str],
    operation: str,
) -> list[FixDocument]:
    """Build merge/set documents — one per affected column, column as key suffix."""
    docs: list[FixDocument] = []
    value = _extract_value(schema, fix_input)
    reason = fix_input.interpretation or f"{schema.action} for {table_name}"

    for i, col_ref in enumerate(fix_input.affected_columns):
        docs.append(
            FixDocument(
                target="config",
                action=schema.action,
                table_name=table_name,
                column_name=column_name,
                dimension=dimension,
                ordinal=i,
                description=f"{schema.action}: {col_ref}",
                payload={
                    "config_path": config_path,
                    "key_path": key_path + [col_ref],
                    "operation": operation,
                    "value": value,
                    "reason": reason,
                },
            )
        )

    return docs


def _build_keyed_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
    config_path: str,
    key_path: list[str],
    operation: str,
) -> list[FixDocument]:
    """Build merge/set document — one doc using key_template for key suffix."""
    assert schema.key_template is not None
    key_suffix = schema.key_template.format(**fix_input.parameters)

    # Exclude key_template fields from the value dict
    key_fields = _extract_template_fields(schema.key_template)
    value = {
        k: v
        for k, v in fix_input.parameters.items()
        if k in schema.fields and k not in key_fields
    }

    reason = fix_input.interpretation or f"{schema.action}: {key_suffix}"

    return [
        FixDocument(
            target="config",
            action=schema.action,
            table_name=table_name,
            column_name=column_name,
            dimension=dimension,
            ordinal=0,
            description=f"{schema.action}: {key_suffix}",
            payload={
                "config_path": config_path,
                "key_path": key_path + [key_suffix],
                "operation": operation,
                "value": value,
                "reason": reason,
            },
        )
    ]


def _extract_value(schema: FixSchema, fix_input: FixInput) -> dict[str, object]:
    """Extract value dict from fix_input.parameters, filtered by schema fields."""
    return {k: v for k, v in fix_input.parameters.items() if k in schema.fields}


def _extract_template_fields(template: str) -> set[str]:
    """Extract field names from a format string template.

    E.g. "{from_table}->{to_table}" -> {"from_table", "to_table"}
    """
    import string

    formatter = string.Formatter()
    return {
        field_name
        for _, field_name, _, _ in formatter.parse(template)
        if field_name is not None
    }
