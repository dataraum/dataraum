"""Typed enums for entropy dimensions, analysis keys, and fix actions.

Provides compile-time safety for the string keys that couple pipeline
phases to entropy detectors.  All enums inherit from ``str`` so they
are drop-in replacements for bare strings in dict keys, comparisons,
and serialisation.
"""

from __future__ import annotations

from enum import Enum


class _StrValueMixin(str, Enum):
    """Mixin that ensures str() returns the value, not the enum repr.

    Python 3.12+ changed str(StrEnum) to return 'ClassName.MEMBER'.
    We override __str__ so these enums remain drop-in for bare strings.
    """

    def __str__(self) -> str:
        return str(self.value)


class Layer(_StrValueMixin):
    """Entropy hierarchy layers."""

    STRUCTURAL = "structural"
    VALUE = "value"
    SEMANTIC = "semantic"
    COMPUTATIONAL = "computational"


class Dimension(_StrValueMixin):
    """Entropy hierarchy dimensions within layers."""

    TYPES = "types"
    RELATIONS = "relations"
    NULLS = "nulls"
    OUTLIERS = "outliers"
    DISTRIBUTION = "distribution"
    TEMPORAL = "temporal"
    BUSINESS_MEANING = "business_meaning"
    UNITS = "units"
    DIMENSIONAL = "dimensional"
    COVERAGE = "coverage"
    DERIVED_VALUES = "derived_values"


class AnalysisKey(_StrValueMixin):
    """Analysis outputs that phases produce and detectors consume."""

    TYPING = "typing"
    STATISTICS = "statistics"
    SEMANTIC = "semantic"
    RELATIONSHIPS = "relationships"
    CORRELATION = "correlation"
    DRIFT_SUMMARIES = "drift_summaries"
    SLICE_VARIANCE = "slice_variance"
    COLUMN_QUALITY_REPORTS = "column_quality_reports"
    ENRICHED_VIEW = "enriched_view"


class SubDimension(_StrValueMixin):
    """Entropy sub-dimensions measured by detectors."""

    TYPE_FIDELITY = "type_fidelity"
    JOIN_PATH_DETERMINISM = "join_path_determinism"
    RELATIONSHIP_QUALITY = "relationship_quality"
    NULL_RATIO = "null_ratio"
    OUTLIER_RATE = "outlier_rate"
    BENFORD_COMPLIANCE = "benford_compliance"
    TEMPORAL_DRIFT = "temporal_drift"
    FORMULA_MATCH = "formula_match"
    NAMING_CLARITY = "naming_clarity"
    UNIT_DECLARATION = "unit_declaration"
    TIME_ROLE = "time_role"
    CROSS_COLUMN_PATTERNS = "cross_column_patterns"
    COLUMN_QUALITY = "column_quality"
    DIMENSION_COVERAGE = "dimension_coverage"


class FixAction(_StrValueMixin):
    """Actions that detectors can flag as fixable.

    Each action here has a registered handler in FixRegistry that produces
    ConfigPatch objects describing what to change.  Document-only resolution
    options (no handler) stay as plain strings in ResolutionOption.action.
    """

    ACCEPT_FINDING = "accept_finding"
    DOCUMENT_BUSINESS_MEANING = "document_business_meaning"
    DECLARE_UNIT = "declare_unit"
    SET_TIMESTAMP_ROLE = "set_timestamp_role"
    ADD_TYPE_PATTERN = "add_type_pattern"
    CONFIRM_RELATIONSHIP = "confirm_relationship"
    RESOLVE_JOIN_AMBIGUITY = "resolve_join_ambiguity"
