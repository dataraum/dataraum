"""Context builder for graph execution.

Collects context from all analysis modules to provide the LLM
with the information needed to generate SQL for graph execution.

This module replaces the quality/context.py functionality for graph-specific
use cases, with support for slice-based filtering.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    import duckdb

    from dataraum_context.graphs.field_mapping import FieldMappings

logger = logging.getLogger(__name__)


# =============================================================================
# Context Models
# =============================================================================


@dataclass
class ColumnContext:
    """Context for a single column."""

    column_id: str
    column_name: str
    table_name: str

    # Type info
    data_type: str | None = None
    semantic_role: str | None = None  # key, measure, dimension, timestamp, etc.
    entity_type: str | None = None  # customer, product, transaction, etc.

    # Business concept mapping (from ontology, for metric calculations)
    business_concept: str | None = None  # e.g., 'revenue', 'accounts_receivable'

    # Statistical metrics
    null_ratio: float | None = None
    cardinality_ratio: float | None = None
    outlier_ratio: float | None = None

    # Temporal metrics
    is_stale: bool | None = None
    detected_granularity: str | None = None

    # Quality grade from quality_summary module
    quality_grade: str | None = None  # A, B, C, D, F
    quality_score: float | None = None  # 0.0 - 1.0

    # Derived column info from correlation analysis
    is_derived: bool = False
    derived_formula: str | None = None  # e.g., "quantity * unit_price"

    # Quality flags
    flags: list[str] = field(default_factory=list)

    # Entropy scores (from entropy layer)
    entropy_scores: dict[str, Any] | None = None  # Layer scores and composite
    resolution_hints: list[dict[str, Any]] = field(default_factory=list)  # Top fixes


@dataclass
class TableContext:
    """Context for a single table."""

    table_id: str
    table_name: str
    row_count: int | None = None
    column_count: int = 0

    # Classification
    is_fact_table: bool | None = None
    is_dimension_table: bool | None = None
    entity_type: str | None = None

    # Columns
    columns: list[ColumnContext] = field(default_factory=list)

    # Quality flags
    flags: list[str] = field(default_factory=list)

    # Entropy (from entropy layer)
    table_entropy: dict[str, Any] | None = None  # Aggregated entropy scores
    readiness_for_use: str | None = None  # ready, investigate, blocked


@dataclass
class RelationshipContext:
    """Context for a table relationship."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str
    cardinality: str | None = None
    confidence: float = 0.0

    # Entropy (from entropy layer)
    relationship_entropy: dict[str, Any] | None = None  # Join path entropy


@dataclass
class SliceContext:
    """Available slice dimension for filtering/grouping."""

    column_name: str
    table_name: str
    priority: int = 0  # Higher = more recommended for slicing
    value_count: int = 0  # Number of distinct values
    business_context: str | None = None  # e.g., "Regional breakdown"


@dataclass
class BusinessCycleContext:
    """Detected business cycle/process."""

    cycle_name: str
    cycle_type: str  # e.g., "order_to_cash", "procure_to_pay"
    tables_involved: list[str] = field(default_factory=list)
    completion_rate: float | None = None  # What % of cycles complete


@dataclass
class GraphExecutionContext:
    """Complete context for graph execution.

    Provides the LLM with all information needed to generate SQL
    for business or quality metric calculations.
    """

    # Tables and their metadata
    tables: list[TableContext] = field(default_factory=list)

    # Relationships between tables
    relationships: list[RelationshipContext] = field(default_factory=list)

    # Graph topology
    graph_pattern: str | None = None  # star_schema, mesh, chain, etc.
    hub_tables: list[str] = field(default_factory=list)
    leaf_tables: list[str] = field(default_factory=list)

    # Aggregate statistics
    total_tables: int = 0
    total_columns: int = 0
    total_relationships: int = 0

    # Quality summary (aggregated from analysis modules)
    quality_issues_by_severity: dict[str, int] = field(default_factory=dict)
    quality_flags: list[str] = field(default_factory=list)

    # Entropy summary (from entropy layer)
    entropy_summary: dict[str, Any] | None = None  # Overall entropy and readiness

    # Slice context (if filtering by dimension)
    slice_column: str | None = None
    slice_value: str | None = None

    # Available slice dimensions (from slicing analysis)
    available_slices: list[SliceContext] = field(default_factory=list)

    # Business cycles (from cycles analysis)
    business_cycles: list[BusinessCycleContext] = field(default_factory=list)

    # Field mappings (business_concept → column mappings for metrics)
    field_mappings: FieldMappings | None = None

    # Metadata
    built_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# =============================================================================
# Context Builder
# =============================================================================


def build_execution_context(
    session: Session,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    *,
    slice_column: str | None = None,
    slice_value: str | None = None,
) -> GraphExecutionContext:
    """Build execution context from all analysis modules.

    Aggregates metadata from:
    - Statistical profiles (null ratios, cardinality, outliers)
    - Semantic annotations (roles, entity types)
    - Temporal analysis (staleness, granularity)
    - Relationship graph topology
    - Quality issues from each pillar

    Args:
        session: SQLAlchemy session
        table_ids: Tables to include in context
        duckdb_conn: Optional DuckDB connection for row counts
        slice_column: Optional column to filter by (for slice metrics)
        slice_value: Optional value to filter on (for slice metrics)

    Returns:
        GraphExecutionContext with all relevant metadata
    """
    # Lazy imports to avoid circular dependencies
    from dataraum_context.analysis.correlation.db_models import DerivedColumn
    from dataraum_context.analysis.cycles.db_models import DetectedBusinessCycle
    from dataraum_context.analysis.quality_summary.db_models import ColumnQualityReport
    from dataraum_context.analysis.relationships.db_models import Relationship
    from dataraum_context.analysis.relationships.graph_topology import (
        analyze_graph_topology,
    )
    from dataraum_context.analysis.semantic.db_models import SemanticAnnotation, TableEntity
    from dataraum_context.analysis.slicing.db_models import SliceDefinition
    from dataraum_context.analysis.statistics.db_models import (
        StatisticalProfile,
        StatisticalQualityMetrics,
    )
    from dataraum_context.analysis.temporal import TemporalColumnProfile
    from dataraum_context.analysis.typing.db_models import TypeDecision
    from dataraum_context.graphs.field_mapping import load_semantic_mappings
    from dataraum_context.storage import Column, Table

    if not table_ids:
        return GraphExecutionContext()

    # 1. Load tables
    tables_stmt = select(Table).where(Table.table_id.in_(table_ids))
    tables = session.execute(tables_stmt).scalars().all()
    table_map = {t.table_id: t for t in tables}

    # 2. Load all columns for these tables
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns = session.execute(columns_stmt).scalars().all()
    columns_by_table: dict[str, list[Column]] = {}
    for col in columns:
        if col.table_id not in columns_by_table:
            columns_by_table[col.table_id] = []
        columns_by_table[col.table_id].append(col)

    # 3. Load statistical profiles
    column_ids = [col.column_id for col in columns]
    stat_profiles: dict[str, StatisticalProfile] = {}
    if column_ids:
        stat_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id.in_(column_ids))
        for profile in session.execute(stat_stmt).scalars().all():
            stat_profiles[profile.column_id] = profile

    # 4. Load statistical quality metrics
    stat_quality: dict[str, StatisticalQualityMetrics] = {}
    if column_ids:
        qual_stmt = select(StatisticalQualityMetrics).where(
            StatisticalQualityMetrics.column_id.in_(column_ids)
        )
        for metrics in session.execute(qual_stmt).scalars().all():
            stat_quality[metrics.column_id] = metrics

    # 5. Load semantic annotations
    semantic: dict[str, SemanticAnnotation] = {}
    if column_ids:
        sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        for ann in session.execute(sem_stmt).scalars().all():
            semantic[ann.column_id] = ann

    # 6. Load temporal profiles
    temporal: dict[str, TemporalColumnProfile] = {}
    if column_ids:
        temp_stmt = select(TemporalColumnProfile).where(
            TemporalColumnProfile.column_id.in_(column_ids)
        )
        for temp_prof in session.execute(temp_stmt).scalars().all():
            temporal[temp_prof.column_id] = temp_prof

    # 7. Load type decisions
    type_decisions: dict[str, TypeDecision] = {}
    if column_ids:
        type_stmt = select(TypeDecision).where(TypeDecision.column_id.in_(column_ids))
        for decision in session.execute(type_stmt).scalars().all():
            type_decisions[decision.column_id] = decision

    # 8. Load table entities (fact/dimension classification)
    table_entities: dict[str, TableEntity] = {}
    entity_stmt = select(TableEntity).where(TableEntity.table_id.in_(table_ids))
    for entity in session.execute(entity_stmt).scalars().all():
        table_entities[entity.table_id] = entity

    # 9. Load relationships
    rel_stmt = select(Relationship).where(
        (Relationship.from_table_id.in_(table_ids))
        & (Relationship.to_table_id.in_(table_ids))
        & ((Relationship.detection_method == "llm") | (Relationship.confidence > 0.7))
    )
    relationships_db = session.execute(rel_stmt).scalars().all()

    # Build relationship contexts
    relationships: list[RelationshipContext] = []
    rel_list_for_topology: list[dict[str, Any]] = []

    for rel in relationships_db:
        from_table = table_map.get(rel.from_table_id)
        to_table = table_map.get(rel.to_table_id)

        if from_table and to_table:
            # Get column names
            from_col = next((c for c in columns if c.column_id == rel.from_column_id), None)
            to_col = next((c for c in columns if c.column_id == rel.to_column_id), None)

            if from_col and to_col:
                relationships.append(
                    RelationshipContext(
                        from_table=from_table.table_name,
                        from_column=from_col.column_name,
                        to_table=to_table.table_name,
                        to_column=to_col.column_name,
                        relationship_type=rel.relationship_type or "unknown",
                        cardinality=rel.cardinality,
                        confidence=rel.confidence,
                    )
                )
                rel_list_for_topology.append(
                    {
                        "table1": from_table.table_name,
                        "table2": to_table.table_name,
                    }
                )

    # 10. Load slice definitions
    slice_contexts: list[SliceContext] = []
    slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
    for slice_def in session.execute(slice_stmt).scalars().all():
        slice_col = next((c for c in columns if c.column_id == slice_def.column_id), None)
        slice_tbl = table_map.get(slice_def.table_id)
        if slice_col and slice_tbl:
            slice_contexts.append(
                SliceContext(
                    column_name=slice_col.column_name,
                    table_name=slice_tbl.table_name,
                    priority=slice_def.slice_priority,
                    value_count=slice_def.value_count or 0,
                    business_context=slice_def.business_context,
                )
            )
    # Sort by priority descending
    slice_contexts.sort(key=lambda s: s.priority, reverse=True)

    # 11. Load quality grades from quality_summary
    quality_grades: dict[str, tuple[str, float]] = {}  # column_id -> (grade, score)
    if column_ids:
        grade_stmt = select(ColumnQualityReport).where(
            ColumnQualityReport.source_column_id.in_(column_ids)
        )
        for report in session.execute(grade_stmt).scalars().all():
            quality_grades[report.source_column_id] = (
                report.quality_grade,
                report.overall_quality_score,
            )

    # 12. Load derived columns from correlation analysis
    derived_columns: dict[str, str] = {}  # column_id -> formula
    if column_ids:
        derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
        for derived in session.execute(derived_stmt).scalars().all():
            derived_columns[derived.derived_column_id] = derived.formula

    # 13. Load business cycles
    business_cycle_contexts: list[BusinessCycleContext] = []
    # Get the most recent analysis run for these tables
    from dataraum_context.analysis.cycles.db_models import BusinessCycleAnalysisRun

    # Find analysis runs that include any of our tables
    cycle_run_stmt = (
        select(BusinessCycleAnalysisRun)
        .where(BusinessCycleAnalysisRun.completed_at.isnot(None))
        .order_by(BusinessCycleAnalysisRun.completed_at.desc())
        .limit(1)
    )
    latest_run = session.execute(cycle_run_stmt).scalar_one_or_none()
    if latest_run:
        cycles_stmt = select(DetectedBusinessCycle).where(
            DetectedBusinessCycle.analysis_id == latest_run.analysis_id
        )
        for cycle in session.execute(cycles_stmt).scalars().all():
            business_cycle_contexts.append(
                BusinessCycleContext(
                    cycle_name=cycle.cycle_name,
                    cycle_type=cycle.canonical_type or cycle.cycle_type,
                    tables_involved=cycle.tables_involved,
                    completion_rate=cycle.completion_rate,
                )
            )

    # 14. Load field mappings
    field_mappings = load_semantic_mappings(session, table_ids)

    # 15. Compute graph topology
    table_names = [t.table_name for t in tables]
    graph_structure = analyze_graph_topology(
        table_ids=table_names,
        relationships=rel_list_for_topology,
    )

    # 16. Build entropy context
    from dataraum_context.entropy.context import (
        build_entropy_context as build_entropy_ctx,
    )
    from dataraum_context.entropy.context import (
        get_column_entropy_summary,
        get_table_entropy_summary,
    )

    entropy_context = build_entropy_ctx(session, table_ids)

    # Build column-level entropy lookup
    column_entropy_lookup: dict[str, dict[str, Any]] = {}
    for col_key, col_entropy_profile in entropy_context.column_profiles.items():
        column_entropy_lookup[col_key] = get_column_entropy_summary(col_entropy_profile)

    # Build table-level entropy lookup
    table_entropy_lookup: dict[str, dict[str, Any]] = {}
    for tbl_name, tbl_entropy_profile in entropy_context.table_profiles.items():
        table_entropy_lookup[tbl_name] = get_table_entropy_summary(tbl_entropy_profile)

    # Update relationship contexts with entropy data
    for rel_ctx in relationships:
        rel_key = (
            f"{rel_ctx.from_table}.{rel_ctx.from_column}->{rel_ctx.to_table}.{rel_ctx.to_column}"
        )
        rel_profile = entropy_context.relationship_profiles.get(rel_key)
        if rel_profile:
            rel_ctx.relationship_entropy = {
                "composite_score": rel_profile.composite_score,
                "cardinality_entropy": rel_profile.cardinality_entropy,
                "join_path_entropy": rel_profile.join_path_entropy,
                "is_deterministic": rel_profile.is_deterministic,
                "join_warning": rel_profile.join_warning,
            }

    # Build entropy summary
    entropy_summary_dict: dict[str, Any] = {
        "overall_readiness": entropy_context.overall_readiness,
        "high_entropy_count": entropy_context.high_entropy_count,
        "critical_entropy_count": entropy_context.critical_entropy_count,
        "compound_risk_count": entropy_context.compound_risk_count,
        "readiness_blockers": entropy_context.readiness_blockers,
    }

    # 17. Build table contexts
    table_contexts: list[TableContext] = []
    quality_issues_by_severity: dict[str, int] = {}
    quality_flags: list[str] = []

    for table_id in table_ids:
        table = table_map.get(table_id)
        if not table:
            continue

        # Get row count from DuckDB if available
        row_count = None
        if duckdb_conn and table.duckdb_path:
            try:
                # Apply slice filter if provided
                if slice_column and slice_value:
                    query = f"""
                        SELECT COUNT(*) FROM "{table.duckdb_path}"
                        WHERE "{slice_column}" = ?
                    """
                    result = duckdb_conn.execute(query, [slice_value]).fetchone()
                else:
                    query = f'SELECT COUNT(*) FROM "{table.duckdb_path}"'
                    result = duckdb_conn.execute(query).fetchone()
                if result:
                    row_count = result[0]
            except Exception as e:
                logger.warning(f"Could not get row count for {table.duckdb_path}: {e}")

        # Build column contexts
        table_columns = columns_by_table.get(table_id, [])
        column_contexts: list[ColumnContext] = []

        for col in table_columns:
            stat_prof = stat_profiles.get(col.column_id)
            quality = stat_quality.get(col.column_id)
            sem_ann = semantic.get(col.column_id)
            temp_profile = temporal.get(col.column_id)
            type_dec = type_decisions.get(col.column_id)

            # Extract metrics
            null_ratio = stat_prof.null_ratio if stat_prof else None
            cardinality_ratio = stat_prof.cardinality_ratio if stat_prof else None
            outlier_ratio = None
            if quality:
                outlier_ratio = quality.iqr_outlier_ratio or quality.isolation_forest_anomaly_ratio

            # Generate column flags
            flags = _generate_column_flags(
                null_ratio=null_ratio,
                outlier_ratio=outlier_ratio,
                benford_compliant=quality.benford_compliant if quality else None,
                is_stale=temp_profile.is_stale if temp_profile else None,
                cardinality_ratio=cardinality_ratio,
            )

            # Add derived column flag
            is_derived = col.column_id in derived_columns
            if is_derived:
                flags.append("derived_column")

            # Aggregate issue counts
            if quality and quality.quality_data:
                issues = quality.quality_data.get("quality_issues", [])
                for issue in issues:
                    sev = issue.get("severity", "warning") if isinstance(issue, dict) else "warning"
                    quality_issues_by_severity[sev] = quality_issues_by_severity.get(sev, 0) + 1

            if flags:
                quality_flags.extend(flags)

            # Get quality grade if available
            col_quality = quality_grades.get(col.column_id)
            quality_grade = col_quality[0] if col_quality else None
            quality_score = col_quality[1] if col_quality else None

            # Get entropy data for this column
            entropy_key = f"{table.table_name}.{col.column_name}"
            col_entropy = column_entropy_lookup.get(entropy_key)

            column_contexts.append(
                ColumnContext(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    table_name=table.table_name,
                    data_type=type_dec.decided_type if type_dec else None,
                    semantic_role=sem_ann.semantic_role if sem_ann else None,
                    entity_type=sem_ann.entity_type if sem_ann else None,
                    business_concept=sem_ann.business_concept if sem_ann else None,
                    null_ratio=null_ratio,
                    cardinality_ratio=cardinality_ratio,
                    outlier_ratio=outlier_ratio,
                    is_stale=temp_profile.is_stale if temp_profile else None,
                    detected_granularity=temp_profile.detected_granularity
                    if temp_profile
                    else None,
                    quality_grade=quality_grade,
                    quality_score=quality_score,
                    is_derived=is_derived,
                    derived_formula=derived_columns.get(col.column_id),
                    flags=flags,
                    entropy_scores=col_entropy,
                    resolution_hints=col_entropy.get("resolution_hints", []) if col_entropy else [],
                )
            )

        # Get table entity info
        table_entity = table_entities.get(table_id)

        # Generate table flags
        table_flags: list[str] = []
        if table_entity:
            if table_entity.is_fact_table:
                table_flags.append("fact_table")
            if table_entity.is_dimension_table:
                table_flags.append("dimension_table")

        # Get table entropy data
        tbl_entropy = table_entropy_lookup.get(table.table_name)

        table_contexts.append(
            TableContext(
                table_id=table_id,
                table_name=table.table_name,
                row_count=row_count,
                column_count=len(column_contexts),
                is_fact_table=table_entity.is_fact_table if table_entity else None,
                is_dimension_table=table_entity.is_dimension_table if table_entity else None,
                entity_type=table_entity.detected_entity_type if table_entity else None,
                columns=column_contexts,
                flags=table_flags,
                table_entropy=tbl_entropy,
                readiness_for_use=tbl_entropy.get("readiness") if tbl_entropy else None,
            )
        )

    return GraphExecutionContext(
        tables=table_contexts,
        relationships=relationships,
        graph_pattern=graph_structure.pattern,
        hub_tables=graph_structure.hub_tables,
        leaf_tables=graph_structure.leaf_tables,
        total_tables=len(table_contexts),
        total_columns=sum(t.column_count for t in table_contexts),
        total_relationships=len(relationships),
        quality_issues_by_severity=quality_issues_by_severity,
        quality_flags=list(set(quality_flags)),  # Deduplicate
        entropy_summary=entropy_summary_dict,
        slice_column=slice_column,
        slice_value=slice_value,
        available_slices=slice_contexts,
        business_cycles=business_cycle_contexts,
        field_mappings=field_mappings,
    )


# =============================================================================
# Flag Generation (inlined from quality/context.py)
# =============================================================================


def _generate_column_flags(
    null_ratio: float | None,
    outlier_ratio: float | None,
    benford_compliant: bool | None,
    is_stale: bool | None,
    cardinality_ratio: float | None,
) -> list[str]:
    """Generate actionable flags from column metrics."""
    flags = []

    if null_ratio is not None and null_ratio > 0.5:
        flags.append("high_nulls")
    elif null_ratio is not None and null_ratio > 0.1:
        flags.append("moderate_nulls")

    if outlier_ratio is not None and outlier_ratio > 0.1:
        flags.append("high_outliers")
    elif outlier_ratio is not None and outlier_ratio > 0.05:
        flags.append("moderate_outliers")

    if benford_compliant is False:
        flags.append("benford_violation")

    if is_stale is True:
        flags.append("stale_data")

    if cardinality_ratio is not None:
        if cardinality_ratio > 0.99:
            flags.append("near_unique")
        elif cardinality_ratio < 0.01:
            flags.append("low_cardinality")

    return flags


# =============================================================================
# Context Formatting for LLM
# =============================================================================


def format_entropy_for_prompt(context: GraphExecutionContext) -> str:
    """Format entropy information for LLM prompts.

    Creates a concise entropy summary that helps the LLM understand:
    - Overall data readiness
    - High-entropy columns that need assumptions
    - Dangerous combinations (compound risks)
    - Columns that may block reliable answers

    Args:
        context: GraphExecutionContext with entropy data

    Returns:
        Formatted string for entropy section, or empty string if no entropy data
    """
    if not context.entropy_summary:
        return ""

    summary = context.entropy_summary
    lines = []

    # Overall readiness header
    readiness = summary.get("overall_readiness", "unknown")
    if readiness == "ready":
        lines.append("## DATA READINESS: ✓ READY")
        lines.append("Data quality is sufficient for reliable answers.")
    elif readiness == "investigate":
        lines.append("## DATA READINESS: ⚠ INVESTIGATE")
        lines.append("Some columns have elevated uncertainty - state assumptions when using them.")
    else:  # blocked
        lines.append("## DATA READINESS: ✗ BLOCKED")
        lines.append(
            "Critical data quality issues exist - consider refusing or asking clarification."
        )

    lines.append("")

    # Stats summary
    high_count = summary.get("high_entropy_count", 0)
    critical_count = summary.get("critical_entropy_count", 0)
    compound_count = summary.get("compound_risk_count", 0)

    if high_count > 0 or critical_count > 0:
        lines.append(f"- High entropy columns: {high_count}")
        if critical_count > 0:
            lines.append(f"- Critical entropy columns: {critical_count}")
        if compound_count > 0:
            lines.append(f"- Compound risks: {compound_count}")
        lines.append("")

    # Blocked columns (critical)
    blockers = summary.get("readiness_blockers", [])
    if blockers:
        lines.append("### BLOCKING ISSUES")
        lines.append("These columns have critical uncertainty and should be clarified before use:")
        for blocker in blockers[:5]:  # Limit to 5
            lines.append(f"  - {blocker}")
        lines.append("")

    # High entropy columns with details
    high_entropy_cols = _collect_high_entropy_columns(context)
    if high_entropy_cols:
        lines.append("### HIGH UNCERTAINTY COLUMNS")
        lines.append("State assumptions when using these columns:")
        for col_info in high_entropy_cols[:10]:  # Limit to 10
            dims = ", ".join(col_info["dimensions"][:2])  # Show first 2 dimensions
            lines.append(f"  - {col_info['name']} (entropy: {col_info['score']:.2f}) - {dims}")
        lines.append("")

    # Compound risks
    compound_warnings = _format_compound_risks(context)
    if compound_warnings:
        lines.append("### DANGEROUS COMBINATIONS")
        lines.append("These dimension combinations create multiplicative risk:")
        lines.extend(compound_warnings)
        lines.append("")

    return "\n".join(lines)


def _collect_high_entropy_columns(context: GraphExecutionContext) -> list[dict[str, Any]]:
    """Collect columns with high entropy from the context.

    Args:
        context: GraphExecutionContext

    Returns:
        List of dicts with column name, score, and high entropy dimensions
    """
    high_cols: list[dict[str, Any]] = []

    for table in context.tables:
        for col in table.columns:
            if col.entropy_scores:
                score = col.entropy_scores.get("composite_score", 0.0)
                if score >= 0.5:  # High entropy threshold
                    high_cols.append(
                        {
                            "name": f"{table.table_name}.{col.column_name}",
                            "score": score,
                            "dimensions": col.entropy_scores.get("high_entropy_dimensions", []),
                            "readiness": col.entropy_scores.get("readiness", "unknown"),
                        }
                    )

    # Sort by score descending
    high_cols.sort(key=lambda x: x["score"], reverse=True)
    return high_cols


def _format_compound_risks(context: GraphExecutionContext) -> list[str]:
    """Format compound risk warnings.

    Args:
        context: GraphExecutionContext

    Returns:
        List of warning lines
    """
    warnings: list[str] = []

    # Check table-level compound risks
    for table in context.tables:
        if table.table_entropy:
            risk_count = table.table_entropy.get("compound_risk_count", 0)
            if risk_count > 0:
                blocked = table.table_entropy.get("blocked_columns", [])
                if blocked:
                    warnings.append(
                        f"  - Table '{table.table_name}': {risk_count} compound risks "
                        f"affecting columns: {', '.join(blocked[:3])}"
                    )

    return warnings


def _format_column_entropy_inline(col: ColumnContext) -> str:
    """Format inline entropy indicator for a column.

    Args:
        col: ColumnContext with entropy data

    Returns:
        Short entropy indicator string or empty string
    """
    if not col.entropy_scores:
        return ""

    score = col.entropy_scores.get("composite_score", 0.0)
    readiness = col.entropy_scores.get("readiness", "ready")

    if readiness == "blocked":
        return " ⛔"
    elif readiness == "investigate" or score >= 0.5:
        return " ⚠"
    return ""


def format_context_for_prompt(context: GraphExecutionContext) -> str:
    """Format execution context as a readable string for LLM prompts.

    Args:
        context: GraphExecutionContext from build_execution_context()

    Returns:
        Formatted string suitable for LLM prompt
    """
    lines = []

    # Summary
    lines.append("## DATASET CONTEXT")
    lines.append("")
    lines.append(f"- Tables: {context.total_tables}")
    lines.append(f"- Columns: {context.total_columns}")
    lines.append(f"- Relationships: {context.total_relationships}")
    lines.append(f"- Graph pattern: {context.graph_pattern or 'unknown'}")

    if context.hub_tables:
        lines.append(f"- Hub tables: {', '.join(context.hub_tables)}")
    if context.leaf_tables:
        lines.append(f"- Leaf/dimension tables: {', '.join(context.leaf_tables)}")

    if context.slice_column:
        lines.append("")
        lines.append(f"- **Slice filter**: {context.slice_column} = '{context.slice_value}'")

    lines.append("")

    # Quality summary
    if context.quality_issues_by_severity:
        lines.append("## QUALITY SUMMARY")
        for sev, count in sorted(context.quality_issues_by_severity.items()):
            lines.append(f"- {sev}: {count} issues")
        lines.append("")

    # Entropy/Data readiness section
    entropy_section = format_entropy_for_prompt(context)
    if entropy_section:
        lines.append(entropy_section)

    # Tables
    lines.append("## TABLES")
    for table in context.tables:
        table_type = ""
        if table.is_fact_table:
            table_type = " (FACT)"
        elif table.is_dimension_table:
            table_type = " (DIMENSION)"

        lines.append(f"\n### {table.table_name}{table_type}")
        if table.row_count:
            lines.append(f"Rows: {table.row_count:,}")
        if table.entity_type:
            lines.append(f"Entity type: {table.entity_type}")

        lines.append("")
        lines.append("Columns:")
        for col in table.columns:
            role = f" [{col.semantic_role}]" if col.semantic_role else ""
            dtype = f" ({col.data_type})" if col.data_type else ""
            concept = f" → {col.business_concept}" if col.business_concept else ""
            grade = f" [Grade: {col.quality_grade}]" if col.quality_grade else ""
            derived = f" (derived: {col.derived_formula})" if col.is_derived else ""
            flags_str = f" - FLAGS: {', '.join(col.flags)}" if col.flags else ""
            entropy_indicator = _format_column_entropy_inline(col)
            lines.append(
                f"  - {col.column_name}{entropy_indicator}{dtype}{role}"
                f"{concept}{grade}{derived}{flags_str}"
            )

    # Relationships
    if context.relationships:
        lines.append("")
        lines.append("## RELATIONSHIPS")
        for rel in context.relationships:
            # Add entropy warning for non-deterministic joins
            rel_warning = ""
            if rel.relationship_entropy:
                if not rel.relationship_entropy.get("is_deterministic", True):
                    rel_warning = " ⚠"
            lines.append(
                f"- {rel.from_table}.{rel.from_column} → "
                f"{rel.to_table}.{rel.to_column}{rel_warning} "
                f"({rel.cardinality or '?'}, conf={rel.confidence:.2f})"
            )

    # Available slices
    if context.available_slices:
        lines.append("")
        lines.append("## AVAILABLE SLICES")
        lines.append("Recommended dimensions for filtering/grouping:")
        for slice_ctx in context.available_slices[:5]:  # Show top 5
            context_str = f" - {slice_ctx.business_context}" if slice_ctx.business_context else ""
            lines.append(
                f"  - {slice_ctx.table_name}.{slice_ctx.column_name} "
                f"(priority: {slice_ctx.priority}, values: {slice_ctx.value_count}){context_str}"
            )

    # Business cycles
    if context.business_cycles:
        lines.append("")
        lines.append("## DETECTED BUSINESS CYCLES")
        for cycle in context.business_cycles:
            tables_str = ", ".join(cycle.tables_involved[:3])
            if len(cycle.tables_involved) > 3:
                tables_str += f" +{len(cycle.tables_involved) - 3} more"
            completion = f" ({cycle.completion_rate:.0%} complete)" if cycle.completion_rate else ""
            lines.append(f"  - {cycle.cycle_name} ({cycle.cycle_type}): {tables_str}{completion}")

    return "\n".join(lines)


__all__ = [
    "ColumnContext",
    "TableContext",
    "RelationshipContext",
    "SliceContext",
    "BusinessCycleContext",
    "GraphExecutionContext",
    "build_execution_context",
    "format_context_for_prompt",
    "format_entropy_for_prompt",
]
