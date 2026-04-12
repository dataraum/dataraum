"""Teach type registry — 8 teach types for the MCP teach tool.

Each teach type maps to a handler that builds a FixDocument and applies it
via apply_and_persist(). Config teaches write YAML (ConfigInterpreter),
metadata teaches patch DB models (MetadataInterpreter).

The set of registered types IS the Goodhart firewall — only these 8 types
can be dispatched, and each writes to a known, safe path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes.interpreters import apply_and_persist
from dataraum.pipeline.fixes.models import FixDocument

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Teach type literals
# ---------------------------------------------------------------------------

TeachType = Literal[
    "concept",
    "validation",
    "cycle",
    "type_pattern",
    "null_value",
    "concept_property",
    "relationship",
    "explanation",
]

VALID_TEACH_TYPES: set[str] = {
    "concept",
    "validation",
    "cycle",
    "type_pattern",
    "null_value",
    "concept_property",
    "relationship",
    "explanation",
}

# ---------------------------------------------------------------------------
# Parameter models (one per teach type)
# ---------------------------------------------------------------------------


class ConceptParams(BaseModel):
    """Parameters for teaching a business concept to the ontology."""

    name: str = Field(description="Concept name (e.g. 'revenue')")
    indicators: list[str] = Field(description="Column name patterns that suggest this concept")
    description: str | None = Field(default=None, description="Human-readable description")
    temporal_behavior: str | None = Field(
        default=None, description="How this concept behaves over time (e.g. 'cumulative')"
    )
    typical_role: str | None = Field(
        default=None, description="Typical semantic role (e.g. 'measure', 'dimension')"
    )
    typical_values: list[str] = Field(default_factory=list, description="Example values")
    exclude_patterns: list[str] = Field(
        default_factory=list, description="Column name patterns to exclude"
    )


class ValidationParams(BaseModel):
    """Parameters for teaching a validation rule."""

    validation_id: str = Field(description="Unique identifier (e.g. 'double_entry_balance')")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="What this validation checks")
    category: str = Field(default="custom", description="Category (e.g. 'financial', 'custom')")
    severity: str = Field(default="medium", description="Severity level")
    check_type: str = Field(default="custom", description="Check type (e.g. 'balance', 'custom')")
    sql_hints: str = Field(description="Hints for SQL generation")
    expected_outcome: str = Field(description="What a passing result looks like")
    parameters: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class CycleParams(BaseModel):
    """Parameters for teaching a business cycle definition."""

    cycle_id: str = Field(description="Cycle identifier (e.g. 'order_to_cash')")
    description: str = Field(description="What this cycle represents")
    business_value: str = Field(default="medium", description="Business importance")
    typical_stages: list[dict[str, Any]] = Field(description="Ordered list of cycle stages")
    participating_entities: list[str] = Field(default_factory=list)
    completion_indicators: list[str] = Field(default_factory=list)
    related_tables: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)


class TypePatternParams(BaseModel):
    """Parameters for teaching a new type inference pattern."""

    name: str = Field(description="Pattern name (e.g. 'custom_date')")
    pattern: str = Field(description="Regex pattern to match values")
    inferred_type: str = Field(description="DuckDB type to infer (e.g. 'DATE', 'DOUBLE')")
    pattern_section: str = Field(
        description="Which pattern list to add to: date_patterns, identifier_patterns, "
        "numeric_patterns, currency_patterns, boolean_patterns"
    )
    examples: list[str] = Field(default_factory=list, description="Example values")
    standardization_expr: str | None = Field(
        default=None, description="SQL expression for value standardization"
    )


class NullValueParams(BaseModel):
    """Parameters for teaching a domain-specific null value."""

    value: str = Field(description="String that represents null (e.g. 'TBD', 'PENDING')")
    case_sensitive: bool = Field(default=False, description="Whether matching is case-sensitive")
    description: str | None = Field(default=None, description="Why this is null in this domain")


class ConceptPropertyParams(BaseModel):
    """Parameters for patching a SemanticAnnotation field."""

    field_updates: dict[str, Any] = Field(
        description="Fields to update on SemanticAnnotation "
        "(e.g. {semantic_role: 'measure', business_concept: 'revenue'})"
    )


class RelationshipParams(BaseModel):
    """Parameters for confirming or declaring a relationship."""

    from_table: str = Field(description="Source table name")
    from_column: str = Field(description="Source column name")
    to_table: str = Field(description="Target table name")
    to_column: str = Field(description="Target column name")
    relationship_type: str | None = Field(default=None, description="Relationship type")
    cardinality: str | None = Field(default=None, description="Cardinality (e.g. 'one-to-many')")


class ExplanationParams(BaseModel):
    """Parameters for providing domain context for an entropy observation."""

    dimension: str = Field(description="Entropy dimension this explains")
    context: str = Field(description="Domain explanation")
    evidence_sql: str | None = Field(default=None, description="SQL that demonstrates the claim")


# ---------------------------------------------------------------------------
# Param model registry
# ---------------------------------------------------------------------------

PARAM_MODELS: dict[str, type[BaseModel]] = {
    "concept": ConceptParams,
    "validation": ValidationParams,
    "cycle": CycleParams,
    "type_pattern": TypePatternParams,
    "null_value": NullValueParams,
    "concept_property": ConceptPropertyParams,
    "relationship": RelationshipParams,
    "explanation": ExplanationParams,
}

# ---------------------------------------------------------------------------
# Rerun phase mapping (for measurement_hint in response)
# ---------------------------------------------------------------------------

_RERUN_PHASES: dict[str, str] = {
    "concept": "semantic",
    "validation": "validation",
    "cycle": "business_cycles",
    "type_pattern": "typing",
    "null_value": "import",
}

# ---------------------------------------------------------------------------
# List upsert helper
# ---------------------------------------------------------------------------


def _upsert_list(
    config_root: Path | None,
    config_path: str,
    key_path: list[str],
    match_field: str,
    entry: dict[str, Any],
) -> list[dict[str, Any]]:
    """Read a list from config YAML, replace or add an entry, return the new list.

    Finds the list at key_path in the YAML file. If an entry with
    entry[match_field] already exists, replaces it. Otherwise appends.
    Returns the updated list for use with ConfigInterpreter's "set" operation.
    """
    existing: list[dict[str, Any]] = []

    if config_root is not None:
        file_path = config_root / config_path
        if file_path.exists():
            import yaml

            with open(file_path) as f:
                data = yaml.safe_load(f) or {}
            current: Any = data
            for key in key_path:
                if isinstance(current, dict):
                    current = current.get(key)
                else:
                    current = None
                    break
            if isinstance(current, list):
                existing = current

    match_value = entry[match_field]
    # Remove existing entry with same key, then append new one
    filtered = [
        item
        for item in existing
        if not (isinstance(item, dict) and item.get(match_field) == match_value)
    ]
    filtered.append(entry)
    return filtered


# ---------------------------------------------------------------------------
# Handlers — each returns a FixDocument
# ---------------------------------------------------------------------------


def _handle_concept(
    params: ConceptParams, vertical: str, *, config_root: Path | None = None, **_kw: Any
) -> FixDocument:
    config_path = f"verticals/{vertical}/ontology.yaml"
    concept = params.model_dump(exclude_none=True)
    updated = _upsert_list(config_root, config_path, ["concepts"], "name", concept)

    return FixDocument(
        target="config",
        action="concept",
        table_name="*",
        column_name=None,
        dimension="schema",
        description=f"Teach concept: {params.name}",
        payload={
            "config_path": config_path,
            "key_path": ["concepts"],
            "operation": "set",
            "value": updated,
        },
    )


def _handle_validation(params: ValidationParams, vertical: str, **_kw: Any) -> FixDocument:
    """Handle validation teach — writes whole-file spec via ConfigInterpreter.

    Uses empty key_path with operation="set" to replace the entire file
    content. Each validation spec is its own file.
    """
    spec = params.model_dump(exclude_none=True)
    spec.setdefault("version", "1.0")

    return FixDocument(
        target="config",
        action="validation",
        table_name="*",
        column_name=None,
        dimension="validation",
        description=f"Teach validation: {params.name}",
        payload={
            "config_path": f"verticals/{vertical}/validations/{params.validation_id}.yaml",
            "key_path": [],
            "operation": "set",
            "value": spec,
        },
    )


def _handle_cycle(params: CycleParams, vertical: str, **_: Any) -> FixDocument:
    cycle_def = params.model_dump(exclude_none=True)
    cycle_id = cycle_def.pop("cycle_id")
    return FixDocument(
        target="config",
        action="cycle",
        table_name="*",
        column_name=None,
        dimension="cycles",
        description=f"Teach cycle: {cycle_id}",
        payload={
            "config_path": f"verticals/{vertical}/cycles.yaml",
            "key_path": ["cycle_types"],
            "operation": "merge",
            "value": {cycle_id: cycle_def},
        },
    )


def _handle_type_pattern(
    params: TypePatternParams, *, config_root: Path | None = None, **_kw: Any
) -> FixDocument:
    config_path = "phases/typing.yaml"
    pattern_entry = params.model_dump(exclude_none=True)
    section = pattern_entry.pop("pattern_section")
    updated = _upsert_list(config_root, config_path, [section], "name", pattern_entry)

    return FixDocument(
        target="config",
        action="type_pattern",
        table_name="*",
        column_name=None,
        dimension="types",
        description=f"Teach type pattern: {params.name}",
        payload={
            "config_path": config_path,
            "key_path": [section],
            "operation": "set",
            "value": updated,
        },
    )


def _handle_null_value(
    params: NullValueParams, *, config_root: Path | None = None, **_kw: Any
) -> FixDocument:
    config_path = "null_values.yaml"
    entry = params.model_dump(exclude_none=True)
    updated = _upsert_list(config_root, config_path, ["missing_indicators"], "value", entry)

    return FixDocument(
        target="config",
        action="null_value",
        table_name="*",
        column_name=None,
        dimension="null_semantics",
        description=f"Teach null value: {params.value!r}",
        payload={
            "config_path": config_path,
            "key_path": ["missing_indicators"],
            "operation": "set",
            "value": updated,
        },
    )


def _handle_concept_property(params: ConceptPropertyParams, target: str, **_kw: Any) -> FixDocument:
    table_name, _sep, column_name = target.partition(".")
    if not column_name:
        raise ValueError("concept_property requires target as 'table.column'")
    return FixDocument(
        target="metadata",
        action="concept_property",
        table_name=table_name,
        column_name=column_name,
        dimension="semantic",
        description=f"Teach concept property on {target}",
        payload={
            "model": "SemanticAnnotation",
            "field_updates": params.field_updates,
        },
    )


def _handle_relationship(params: RelationshipParams, **_: Any) -> FixDocument:
    field_updates: dict[str, Any] = {}
    if params.relationship_type:
        field_updates["relationship_type"] = params.relationship_type
    if params.cardinality:
        field_updates["cardinality"] = params.cardinality
    return FixDocument(
        target="metadata",
        action="relationship",
        table_name=params.from_table,
        column_name=params.from_column,
        dimension="relationships",
        description=f"Teach relationship: {params.from_table}.{params.from_column} → "
        f"{params.to_table}.{params.to_column}",
        payload={
            "model": "Relationship",
            "field_updates": field_updates,
            # Resolution hints — used by resolver and factory, not applied via setattr
            "hints": {
                "from_table": params.from_table,
                "to_table": params.to_table,
                "to_column": params.to_column,
            },
        },
    )


def _handle_explanation(params: ExplanationParams, target: str, **_kw: Any) -> FixDocument:
    table_name, _sep, column_name = target.partition(".")
    return FixDocument(
        target="metadata",
        action="explanation",
        table_name=table_name,
        column_name=column_name or None,
        dimension=params.dimension,
        description=f"Teach explanation on {target}: {params.context[:80]}",
        payload={
            "context": params.context,
            "evidence_sql": params.evidence_sql,
        },
    )


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

_HANDLERS: dict[str, Any] = {
    "concept": _handle_concept,
    "validation": _handle_validation,
    "cycle": _handle_cycle,
    "type_pattern": _handle_type_pattern,
    "null_value": _handle_null_value,
    "concept_property": _handle_concept_property,
    "relationship": _handle_relationship,
    "explanation": _handle_explanation,
}

# Types that require a target parameter
_REQUIRES_TARGET: set[str] = {"concept_property", "explanation"}

# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------


def handle_teach(
    teach_type: str,
    params: dict[str, Any],
    *,
    source_id: str,
    session: Any,
    vertical: str,
    config_root: Path | None = None,
    target: str | None = None,
) -> dict[str, Any]:
    """Dispatch a teach request to the appropriate handler.

    Args:
        teach_type: One of the 8 registered teach types.
        params: Type-specific parameters (validated by Pydantic model).
        source_id: Pipeline source ID for DataFix persistence.
        session: SQLAlchemy session.
        vertical: Active vertical name (e.g. 'finance', '_adhoc').
        config_root: Config root directory (required for config teaches).
        target: Optional target specifier (e.g. 'orders.amount').

    Returns:
        Response dict with status, type, teaching_id, and measurement_hint.
    """
    if teach_type not in VALID_TEACH_TYPES:
        return {
            "error": f"Unknown teach type: {teach_type!r}. "
            f"Valid types: {', '.join(sorted(VALID_TEACH_TYPES))}",
        }

    # Validate params against the Pydantic model
    param_model_cls = PARAM_MODELS[teach_type]
    try:
        validated_params = param_model_cls.model_validate(params)
    except Exception as e:
        return {"error": f"Invalid params for teach type {teach_type!r}: {e}"}

    # Validate target for types that require it
    if teach_type in _REQUIRES_TARGET and not target:
        return {"error": f"teach type {teach_type!r} requires a target (e.g. 'table.column')"}

    # Build handler kwargs
    handler = _HANDLERS[teach_type]
    kwargs: dict[str, Any] = {"vertical": vertical, "config_root": config_root}
    if target is not None:
        kwargs["target"] = target

    try:
        doc = handler(validated_params, **kwargs)
    except (ValueError, KeyError) as e:
        return {"error": str(e)}

    # Apply and persist
    try:
        records = apply_and_persist(
            source_id,
            [doc],
            session=session,
            config_root=config_root,
        )
    except Exception as e:
        logger.error("teach_apply_failed", teach_type=teach_type, error=str(e))
        return {"error": f"Failed to apply teach: {e}"}

    if not records:
        return {"error": "No records created"}

    record = records[0]
    if record.status == "failed":
        return {
            "error": f"Teach applied but failed: {record.error_message}",
            "teaching_id": record.fix_id,
        }

    # Build response
    response: dict[str, Any] = {
        "status": "applied",
        "type": teach_type,
        "target": target or doc.table_name,
        "teaching_id": record.fix_id,
    }

    # Add measurement hint for config teaches
    rerun_phase = _RERUN_PHASES.get(teach_type)
    if rerun_phase:
        response["measurement_hint"] = (
            f"Config updated. Call measure(target_phase='{rerun_phase}') "
            f"to rerun the {rerun_phase} phase and see the effect."
        )

    return response
