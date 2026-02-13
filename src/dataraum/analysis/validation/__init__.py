"""Generic Validation Module.

LLM-powered validation checks that interpret table schemas
to identify relevant columns and generate appropriate SQL.
"""

from dataraum.analysis.validation.agent import ValidationAgent
from dataraum.analysis.validation.config import (
    format_validation_specs_for_context,
    get_validation_spec,
    get_validation_specs_by_category,
    get_validation_specs_by_tags,
    load_all_validation_specs,
)
from dataraum.analysis.validation.db_models import (
    ValidationResultRecord,
)
from dataraum.analysis.validation.models import (
    GeneratedSQL,
    ValidationResult,
    ValidationRunResult,
    ValidationSeverity,
    ValidationSpec,
    ValidationStatus,
)
from dataraum.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
    get_multi_table_schema_for_llm,
)

__all__ = [
    # Agent
    "ValidationAgent",
    # Config
    "load_all_validation_specs",
    "get_validation_spec",
    "get_validation_specs_by_category",
    "get_validation_specs_by_tags",
    "format_validation_specs_for_context",
    # Resolver
    "get_multi_table_schema_for_llm",
    "format_multi_table_schema_for_prompt",
    # Models
    "ValidationSpec",
    "GeneratedSQL",
    "ValidationResult",
    "ValidationRunResult",
    "ValidationSeverity",
    "ValidationStatus",
    # DB Models
    "ValidationResultRecord",
]
