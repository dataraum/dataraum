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

    Routes by target and operation type:
    - metadata: Model-based (ORM patch) or marker (DataFix record only)
    - config/append: One FixDocument per affected_column
    - config/merge or set with key_template=None: One per affected_column
    - config/merge or set with key_template: One using template-derived key

    Args:
        schema: The fix schema from the detector.
        fix_input: User's structured fix decision.
        table_name: Table this fix applies to.
        column_name: Column this fix applies to (may be None for table-scoped).
        dimension: Entropy dimension being addressed.

    Returns:
        List of FixDocuments ready for interpreter application.
    """
    if schema.target == "metadata":
        return _build_metadata_documents(schema, fix_input, table_name, column_name, dimension)

    if schema.target != "config":
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
    """Build append documents — one per affected column.

    When the schema has structured fields beyond just "reason" (e.g.,
    confirm_expected_pattern with table, columns, pattern_type), appends
    the parameters dict. Otherwise (e.g., document_accepted_outlier_rate
    with only an optional reason), appends the column reference string.
    """
    docs: list[FixDocument] = []
    reason = fix_input.interpretation or f"{schema.action} for {table_name}"

    # Structured append: schema has fields beyond just "reason"
    has_structured_fields = bool(schema.fields.keys() - {"reason"})
    if has_structured_fields:
        value: object = _extract_value(schema, fix_input)
    else:
        value = None  # set per-column below

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
                    "value": value if has_structured_fields else col_ref,
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
    """Build merge/set document(s) — using key_template for key suffix.

    Supports a ``patterns`` array parameter for multi-format columns:
    each entry in the array produces a separate FixDocument, allowing
    multiple patterns to be written in a single apply_fix call.
    """
    assert schema.key_template is not None

    # Multi-pattern mode: "patterns" array in parameters
    patterns_list = fix_input.parameters.get("patterns")
    if isinstance(patterns_list, list) and patterns_list:
        return _build_keyed_documents_multi(
            schema,
            fix_input,
            patterns_list,
            table_name,
            column_name,
            dimension,
            config_path,
            key_path,
            operation,
        )

    # Single-pattern mode (original)
    try:
        key_suffix = schema.key_template.format(**fix_input.parameters)
    except KeyError:
        # LLM didn't include required template fields — cannot build a valid config key.
        # Return empty so the caller logs "no documents" and moves on.
        import logging

        logging.getLogger(__name__).warning(
            "key_template %r requires fields missing from parameters %s — skipping fix",
            schema.key_template,
            sorted(fix_input.parameters),
        )
        return []

    # Exclude key_template fields from the value dict
    key_fields = _extract_template_fields(schema.key_template)
    value = {
        k: v for k, v in fix_input.parameters.items() if k in schema.fields and k not in key_fields
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


def _build_keyed_documents_multi(
    schema: FixSchema,
    fix_input: FixInput,
    patterns_list: list[object],
    table_name: str,
    column_name: str | None,
    dimension: str,
    config_path: str,
    key_path: list[str],
    operation: str,
) -> list[FixDocument]:
    """Build one FixDocument per entry in a patterns array."""
    assert schema.key_template is not None
    docs: list[FixDocument] = []
    key_fields = _extract_template_fields(schema.key_template)
    reason = fix_input.interpretation or f"{schema.action} for {table_name}"

    for i, entry in enumerate(patterns_list):
        if not isinstance(entry, dict):
            continue
        try:
            key_suffix = schema.key_template.format(**entry)
        except KeyError:
            continue

        value = {k: v for k, v in entry.items() if k not in key_fields}

        docs.append(
            FixDocument(
                target="config",
                action=schema.action,
                table_name=table_name,
                column_name=column_name,
                dimension=dimension,
                ordinal=i,
                description=f"{schema.action}: {key_suffix}",
                payload={
                    "config_path": config_path,
                    "key_path": key_path + [key_suffix],
                    "operation": operation,
                    "value": value,
                    "reason": reason,
                },
            )
        )

    return docs


def _build_metadata_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
) -> list[FixDocument]:
    """Build metadata fix documents.

    Three flavours:
    - **model-based**: Schema has ``model`` — builds payload with
      ``model`` + ``field_updates`` so MetadataInterpreter patches
      an ORM row directly (SemanticAnnotation, Relationship).
    - **marker**: No ``model`` but has structured fields — stores
      parameters in the DataFix payload for detector queries
      (e.g. document_join_path, confirm_expected_pattern).
    - **acceptance**: No ``model``, no structured fields — the
      DataFix record itself is the fix; the gate queries it.
    """
    reason = fix_input.interpretation or f"{schema.action} for {table_name}"

    if schema.model:
        return _build_metadata_model_documents(
            schema, fix_input, table_name, column_name, dimension, reason
        )
    return _build_metadata_marker_documents(
        schema, fix_input, table_name, column_name, dimension, reason
    )


def _build_metadata_model_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
    reason: str,
) -> list[FixDocument]:
    """Build metadata documents that patch an ORM model."""
    field_updates = _extract_value(schema, fix_input)
    docs: list[FixDocument] = []

    for i, col_ref in enumerate(fix_input.affected_columns):
        parts = col_ref.split(".", 1)
        col_name = parts[1] if len(parts) > 1 else col_ref

        docs.append(
            FixDocument(
                target="metadata",
                action=schema.action,
                table_name=table_name,
                column_name=col_name,
                dimension=dimension,
                ordinal=i,
                description=f"{schema.action}: {col_ref}",
                payload={
                    "model": schema.model,
                    "field_updates": dict(field_updates),
                    "reason": reason,
                },
            )
        )

    return docs


def _build_metadata_marker_documents(
    schema: FixSchema,
    fix_input: FixInput,
    table_name: str,
    column_name: str | None,
    dimension: str,
    reason: str,
) -> list[FixDocument]:
    """Build metadata marker documents — DataFix record is the fix.

    For acceptance markers the payload is just ``{reason}``.
    For DataFix-only fixes (e.g. document_join_path) the payload
    also includes ``parameters`` so detectors can query them later.
    """
    params = _extract_value(schema, fix_input) if schema.fields else {}
    docs: list[FixDocument] = []

    for i, col_ref in enumerate(fix_input.affected_columns):
        parts = col_ref.split(".", 1)
        col_name = parts[1] if len(parts) > 1 else col_ref

        payload: dict[str, object] = {"reason": reason}
        if params:
            payload["parameters"] = params

        docs.append(
            FixDocument(
                target="metadata",
                action=schema.action,
                table_name=table_name,
                column_name=col_name,
                dimension=dimension,
                ordinal=i,
                description=f"{schema.action}: {col_ref}",
                payload=payload,
            )
        )

    return docs


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
        field_name for _, field_name, _, _ in formatter.parse(template) if field_name is not None
    }
