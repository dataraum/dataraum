"""Generic Validation Module.

LLM-powered validation checks that interpret table schemas
to identify relevant columns and generate appropriate SQL.
"""

from dataraum.analysis.validation.agent import ValidationAgent
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
