"""Context builder for graph execution.

Collects context from all analysis modules to provide the LLM
with the information needed to generate SQL for graph execution.

This module replaces the quality/context.py functionality for graph-specific
use cases, with support for slice-based filtering.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb

    from dataraum.analysis.cycles.health import HealthReport
    from dataraum.graphs.field_mapping import FieldMappings

logger = get_logger(__name__)


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
    temporal_behavior: str | None = None  # 'additive' or 'point_in_time'

    # Statistical metrics
    null_ratio: float | None = None
    cardinality_ratio: float | None = None
    outlier_ratio: float | None = None

    # Temporal metrics
    is_stale: bool | None = None
    detected_granularity: str | None = None

    # Business metadata (from SemanticAnnotation)
    business_name: str | None = None
    business_description: str | None = None
    unit_source_column: str | None = None

    # Temporal bounds (from TemporalColumnProfile)
    min_timestamp: str | None = None
    max_timestamp: str | None = None
    completeness_ratio: float | None = None

    # Quality grade from quality_summary module
    quality_grade: str | None = None  # A, B, C, D, F
    quality_score: float | None = None  # 0.0 - 1.0

    # Quality narrative (from ColumnQualityReport)
    quality_summary: str | None = None  # LLM narrative
    quality_findings: list[str] = field(default_factory=list)  # key_findings from report_data
    quality_recommendations: list[str] = field(default_factory=list)

    # Derived column info from correlation analysis
    is_derived: bool = False
    derived_formula: str | None = None  # e.g., "quantity * unit_price"

    # Quality flags
    flags: list[str] = field(default_factory=list)

    # Entropy scores (from entropy layer)
    entropy_scores: dict[str, Any] | None = None  # Layer scores and composite
    resolution_hints: list[dict[str, Any]] = field(default_factory=list)  # Top fixes

    # Entropy interpretation (from EntropyInterpretationRecord)
    entropy_explanation: str | None = None
    entropy_assumptions: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TableContext:
    """Context for a single table."""

    table_id: str
    table_name: str
    duckdb_name: str | None = None  # Actual DuckDB table name (e.g., "typed_sales")
    row_count: int | None = None
    column_count: int = 0

    # Classification
    is_fact_table: bool | None = None
    is_dimension_table: bool | None = None
    entity_type: str | None = None

    # From TableEntity
    table_description: str | None = None
    grain_columns: list[str] = field(default_factory=list)
    time_column: str | None = None

    # Columns
    columns: list[ColumnContext] = field(default_factory=list)

    # Quality flags
    flags: list[str] = field(default_factory=list)

    # Entropy (from entropy layer)
    table_entropy: dict[str, Any] | None = None  # Aggregated entropy scores
    readiness_for_use: str | None = None  # ready, investigate, blocked

    # Table-level entropy interpretation
    table_entropy_explanation: str | None = None
    table_entropy_assumptions: list[dict[str, Any]] = field(default_factory=list)


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
    distinct_values: list[str] = field(default_factory=list)  # Actual categorical values


@dataclass
class CycleStageContext:
    """A stage within a business cycle."""

    stage_name: str
    stage_order: int
    indicator_column: str | None = None
    indicator_values: list[str] = field(default_factory=list)
    completion_rate: float | None = None


@dataclass
class EntityFlowContext:
    """An entity flowing through a business cycle."""

    entity_type: str  # "customer", "vendor"
    entity_column: str  # "customer_id"
    entity_table: str  # "customers"
    fact_table: str | None = None
    relationship_type: str | None = None


@dataclass
class BusinessCycleContext:
    """Detected business cycle with full metadata."""

    cycle_name: str
    cycle_type: str  # e.g., "order_to_cash", "procure_to_pay"
    tables_involved: list[str] = field(default_factory=list)
    completion_rate: float | None = None  # What % of cycles complete
    description: str | None = None
    business_value: str = "medium"
    confidence: float = 0.0
    stages: list[CycleStageContext] = field(default_factory=list)
    entity_flows: list[EntityFlowContext] = field(default_factory=list)
    status_column: str | None = None  # "invoices.status"
    completion_value: str | None = None  # "paid"

    # Volume metrics (from DetectedBusinessCycle)
    total_records: int | None = None
    completed_cycles: int | None = None
    evidence: list[str] = field(default_factory=list)


@dataclass
class ValidationContext:
    """Result of a validation check."""

    validation_id: str
    status: str  # passed, failed, skipped, error
    severity: str  # info, warning, error, critical
    passed: bool
    message: str
    details: dict[str, Any] | None = None  # From ValidationResultRecord.details


@dataclass
class EnrichedViewContext:
    """A pre-built enriched view joining fact + dimension tables."""

    view_name: str
    fact_table: str
    dimension_columns: list[str] = field(default_factory=list)
    is_grain_verified: bool = False


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

    # Column summaries for contract evaluation (from entropy network)
    column_summaries: dict[str, Any] = field(default_factory=dict)

    # Overall entropy score (average from snapshot)
    overall_entropy_score: float | None = None

    # Slice context (if filtering by dimension)
    slice_column: str | None = None
    slice_value: str | None = None

    # Available slice dimensions (from slicing analysis)
    available_slices: list[SliceContext] = field(default_factory=list)

    # Business cycles (from cycles analysis)
    business_cycles: list[BusinessCycleContext] = field(default_factory=list)

    # Cycle health (from cycles health computation)
    cycle_health: HealthReport | None = None

    # Validation results (from validation analysis)
    validations: list[ValidationContext] = field(default_factory=list)

    # Enriched views (pre-joined fact + dimension tables)
    enriched_views: list[EnrichedViewContext] = field(default_factory=list)

    # Field mappings (business_concept → column mappings for metrics)
    field_mappings: FieldMappings | None = None

    # Aggregated assumptions across all columns (for top-level section)
    active_assumptions: list[dict[str, Any]] = field(default_factory=list)

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
    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
    from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.relationships.graph_topology import (
        analyze_graph_topology,
    )
    from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.statistics.db_models import (
        StatisticalProfile,
    )
    from dataraum.analysis.statistics.quality_db_models import (
        StatisticalQualityMetrics,
    )
    from dataraum.analysis.temporal import TemporalColumnProfile
    from dataraum.analysis.typing.db_models import TypeDecision
    from dataraum.graphs.field_mapping import load_semantic_mappings
    from dataraum.storage import Column, Table

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
        & (Relationship.detection_method == "llm")
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
                    column_name=slice_def.column_name or slice_col.column_name,
                    table_name=slice_tbl.table_name,
                    priority=slice_def.slice_priority,
                    value_count=slice_def.value_count or 0,
                    business_context=slice_def.business_context,
                    distinct_values=slice_def.distinct_values or [],
                )
            )
    # Sort by priority descending
    slice_contexts.sort(key=lambda s: s.priority, reverse=True)

    # 11. Load quality reports from quality_summary
    quality_reports: dict[str, ColumnQualityReport] = {}
    if column_ids:
        grade_stmt = select(ColumnQualityReport).where(
            ColumnQualityReport.source_column_id.in_(column_ids)
        )
        for report in session.execute(grade_stmt).scalars().all():
            quality_reports[report.source_column_id] = report

    # 12. Load derived columns from correlation analysis
    derived_columns: dict[str, str] = {}  # column_id -> formula
    if column_ids:
        derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
        for derived in session.execute(derived_stmt).scalars().all():
            derived_columns[derived.derived_column_id] = derived.formula

    # 13. Load business cycles (filtered to current source)
    #     Derive source_id from table_ids — reused later for entropy snapshot
    source_id_result = session.execute(
        select(Table.source_id).where(Table.table_id.in_(table_ids)).limit(1)
    )
    _source_id = source_id_result.scalar()

    business_cycle_contexts: list[BusinessCycleContext] = []
    if _source_id:
        cycles_stmt = (
            select(DetectedBusinessCycle)
            .where(DetectedBusinessCycle.source_id == _source_id)
            .order_by(DetectedBusinessCycle.detected_at.desc())
        )
    else:
        cycles_stmt = select(DetectedBusinessCycle).order_by(
            DetectedBusinessCycle.detected_at.desc()
        )
    for cycle in session.execute(cycles_stmt).scalars().all():
        stages = [
            CycleStageContext(
                stage_name=s.get("stage_name", ""),
                stage_order=s.get("stage_order", 0),
                indicator_column=s.get("indicator_column"),
                indicator_values=s.get("indicator_values", []),
                completion_rate=s.get("completion_rate"),
            )
            for s in (cycle.stages or [])
        ]
        entity_flows = [
            EntityFlowContext(
                entity_type=ef.get("entity_type", ""),
                entity_column=ef.get("entity_column", ""),
                entity_table=ef.get("entity_table", ""),
                fact_table=ef.get("fact_table"),
                relationship_type=ef.get("relationship_type"),
            )
            for ef in (cycle.entity_flows or [])
        ]
        # Combine status_table + status_column for concise reference
        status_col = None
        if cycle.status_table and cycle.status_column:
            status_col = f"{cycle.status_table}.{cycle.status_column}"
        elif cycle.status_column:
            status_col = cycle.status_column

        business_cycle_contexts.append(
            BusinessCycleContext(
                cycle_name=cycle.cycle_name,
                cycle_type=cycle.canonical_type or cycle.cycle_type,
                tables_involved=cycle.tables_involved,
                completion_rate=cycle.completion_rate,
                description=cycle.description,
                business_value=cycle.business_value,
                confidence=cycle.confidence,
                stages=stages,
                entity_flows=entity_flows,
                status_column=status_col,
                completion_value=cycle.completion_value,
                total_records=cycle.total_records,
                completed_cycles=cycle.completed_cycles,
                evidence=cycle.evidence or [],
            )
        )

    # 13b. Load validation results
    from dataraum.analysis.validation.db_models import ValidationResultRecord

    validation_contexts: list[ValidationContext] = []
    # ValidationResultRecord has no source_id — filter post-hoc by table_id overlap.
    # This is correct because callers pre-filter table_ids to a single source.
    val_stmt = select(ValidationResultRecord).order_by(ValidationResultRecord.executed_at.desc())
    table_id_set = set(table_ids)
    seen_validation_ids: set[str] = set()
    for val_rec in session.execute(val_stmt).scalars().all():
        if not (table_id_set & set(val_rec.table_ids)):
            continue
        # Deduplicate: keep only the most recent run per validation_id
        if val_rec.validation_id in seen_validation_ids:
            continue
        seen_validation_ids.add(val_rec.validation_id)
        validation_contexts.append(
            ValidationContext(
                validation_id=val_rec.validation_id,
                status=val_rec.status,
                severity=val_rec.severity,
                passed=val_rec.passed,
                message=val_rec.message or "",
                details=val_rec.details,
            )
        )

    # 13c. Load enriched views
    from dataraum.analysis.views.db_models import EnrichedView

    enriched_view_contexts: list[EnrichedViewContext] = []
    ev_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
    for ev in session.execute(ev_stmt).scalars().all():
        fact_table = table_map.get(ev.fact_table_id)
        if fact_table:
            enriched_view_contexts.append(
                EnrichedViewContext(
                    view_name=ev.view_name,
                    fact_table=fact_table.table_name,
                    dimension_columns=ev.dimension_columns or [],
                    is_grain_verified=ev.is_grain_verified,
                )
            )

    # 13d. Compute cycle health
    from dataraum.analysis.cycles.health import compute_cycle_health
    from dataraum.core.config import load_phase_config

    cycle_health_report: HealthReport | None = None
    if _source_id:
        bc_config = load_phase_config("business_cycles")
        _vertical = bc_config.get("vertical")
        if _vertical:
            try:
                cycle_health_report = compute_cycle_health(session, _source_id, vertical=_vertical)
            except Exception as e:
                logger.warning("cycle_health_failed", error=str(e))

    # 14. Load field mappings
    field_mappings = load_semantic_mappings(session, table_ids)

    # 15. Compute graph topology
    table_names = [t.table_name for t in tables]
    graph_structure = analyze_graph_topology(
        table_ids=table_names,
        relationships=rel_list_for_topology,
    )

    # 16. Build entropy context using Bayesian network (typed tables enforced internally)
    from dataraum.entropy.views.network_context import (
        ColumnNetworkResult,
        build_for_network,
    )

    network_ctx = build_for_network(session, table_ids)

    # Build column-level entropy lookup from network results
    column_entropy_lookup: dict[str, dict[str, Any]] = {}
    for target, col_result in network_ctx.columns.items():
        # target is "column:table.col", extract "table.col"
        col_key = target.removeprefix("column:")
        column_entropy_lookup[col_key] = _column_network_to_dict(col_result)

    # Build table-level entropy lookup aggregated from per-column network results
    table_entropy_lookup: dict[str, dict[str, Any]] = {}
    _table_columns: dict[str, list[ColumnNetworkResult]] = {}
    for target, col_result in network_ctx.columns.items():
        col_key = target.removeprefix("column:")
        tbl_name = col_key.split(".")[0] if "." in col_key else col_key
        _table_columns.setdefault(tbl_name, []).append(col_result)
    for tbl_name, col_results in _table_columns.items():
        table_entropy_lookup[tbl_name] = _table_network_to_dict(tbl_name, col_results)

    # Build entropy summary from the network context
    entropy_summary_dict: dict[str, Any] = {
        "overall_readiness": network_ctx.overall_readiness,
        "high_entropy_count": network_ctx.columns_blocked + network_ctx.columns_investigate,
        "critical_entropy_count": network_ctx.columns_blocked,
        "columns_blocked": network_ctx.columns_blocked,
        "columns_investigate": network_ctx.columns_investigate,
        "columns_ready": network_ctx.columns_ready,
        "readiness_blockers": [
            t.removeprefix("column:")
            for t, c in network_ctx.columns.items()
            if c.readiness == "blocked"
        ],
    }

    # 16b. Build column summaries for contract evaluation (reuses network_ctx)
    from dataraum.entropy.views.query_context import network_to_column_summaries

    column_summaries = network_to_column_summaries(network_ctx)

    # 16c. Read overall entropy score from snapshot (_source_id resolved in step 13)
    from dataraum.entropy.db_models import EntropySnapshotRecord

    overall_entropy_score: float | None = None
    if _source_id:
        snapshot_result = session.execute(
            select(EntropySnapshotRecord.avg_entropy_score)
            .where(EntropySnapshotRecord.source_id == _source_id)
            .order_by(EntropySnapshotRecord.snapshot_at.desc())
            .limit(1)
        )
        overall_entropy_score = snapshot_result.scalar()

    # 18. Load entropy interpretations
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord

    entropy_interps: dict[str, EntropyInterpretationRecord] = {}
    table_interps: dict[str, EntropyInterpretationRecord] = {}

    if _source_id:
        interp_stmt = select(EntropyInterpretationRecord).where(
            EntropyInterpretationRecord.source_id == _source_id
        )
        for interp in session.execute(interp_stmt).scalars().all():
            if interp.column_id:
                entropy_interps[interp.column_id] = interp
            elif interp.table_id:
                table_interps[interp.table_id] = interp

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
                logger.warning("row_count_query_failed", table=table.duckdb_path, error=str(e))

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
                outlier_ratio = quality.iqr_outlier_ratio or quality.zscore_outlier_ratio

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

            # Get quality report if available
            col_report = quality_reports.get(col.column_id)

            # Get entropy data for this column
            entropy_key = f"{table.table_name}.{col.column_name}"
            col_entropy = column_entropy_lookup.get(entropy_key)

            # Get entropy interpretation for this column
            col_interp = entropy_interps.get(col.column_id)

            column_contexts.append(
                ColumnContext(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    table_name=table.table_name,
                    data_type=type_dec.decided_type if type_dec else None,
                    semantic_role=sem_ann.semantic_role if sem_ann else None,
                    entity_type=sem_ann.entity_type if sem_ann else None,
                    business_concept=sem_ann.business_concept if sem_ann else None,
                    temporal_behavior=sem_ann.temporal_behavior if sem_ann else None,
                    business_name=sem_ann.business_name if sem_ann else None,
                    business_description=sem_ann.business_description if sem_ann else None,
                    unit_source_column=sem_ann.unit_source_column if sem_ann else None,
                    null_ratio=null_ratio,
                    cardinality_ratio=cardinality_ratio,
                    outlier_ratio=outlier_ratio,
                    is_stale=temp_profile.is_stale if temp_profile else None,
                    detected_granularity=temp_profile.detected_granularity
                    if temp_profile
                    else None,
                    min_timestamp=str(temp_profile.min_timestamp)
                    if temp_profile and temp_profile.min_timestamp
                    else None,
                    max_timestamp=str(temp_profile.max_timestamp)
                    if temp_profile and temp_profile.max_timestamp
                    else None,
                    completeness_ratio=temp_profile.completeness_ratio if temp_profile else None,
                    quality_grade=col_report.quality_grade if col_report else None,
                    quality_score=col_report.overall_quality_score if col_report else None,
                    quality_summary=col_report.summary if col_report else None,
                    quality_findings=col_report.report_data.get("key_findings", [])
                    if col_report and col_report.report_data
                    else [],
                    quality_recommendations=col_report.report_data.get("recommendations", [])
                    if col_report and col_report.report_data
                    else [],
                    is_derived=is_derived,
                    derived_formula=derived_columns.get(col.column_id),
                    flags=flags,
                    entropy_scores=col_entropy,
                    resolution_hints=col_entropy.get("resolution_hints", []) if col_entropy else [],
                    entropy_explanation=col_interp.explanation if col_interp else None,
                    entropy_assumptions=col_interp.assumptions_json or [] if col_interp else [],
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

        # Get table-level entropy interpretation
        table_interp = table_interps.get(table_id)

        # Extract grain_columns: stored as JSON (may be list of column IDs or names)
        grain_cols: list[str] = []
        if table_entity and table_entity.grain_columns:
            raw_grain = table_entity.grain_columns
            if isinstance(raw_grain, list):
                grain_cols = list(raw_grain)
            elif isinstance(raw_grain, dict):
                # Some formats store as {"columns": [...]}
                grain_cols = list(raw_grain.get("columns", []))

        table_contexts.append(
            TableContext(
                table_id=table_id,
                table_name=table.table_name,
                duckdb_name=table.duckdb_path,
                row_count=row_count,
                column_count=len(column_contexts),
                is_fact_table=table_entity.is_fact_table if table_entity else None,
                is_dimension_table=table_entity.is_dimension_table if table_entity else None,
                entity_type=table_entity.detected_entity_type if table_entity else None,
                table_description=table_entity.description if table_entity else None,
                grain_columns=grain_cols,
                time_column=table_entity.time_column if table_entity else None,
                columns=column_contexts,
                flags=table_flags,
                table_entropy=tbl_entropy,
                readiness_for_use=tbl_entropy.get("readiness") if tbl_entropy else None,
                table_entropy_explanation=table_interp.explanation if table_interp else None,
                table_entropy_assumptions=table_interp.assumptions_json or []
                if table_interp
                else [],
            )
        )

    # Aggregate active assumptions across all columns
    active_assumptions: list[dict[str, Any]] = []
    for tctx in table_contexts:
        for cctx in tctx.columns:
            for assumption in cctx.entropy_assumptions:
                active_assumptions.append(
                    {
                        "table": tctx.table_name,
                        "column": cctx.column_name,
                        **assumption,
                    }
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
        column_summaries=column_summaries,
        overall_entropy_score=overall_entropy_score,
        slice_column=slice_column,
        slice_value=slice_value,
        available_slices=slice_contexts,
        business_cycles=business_cycle_contexts,
        cycle_health=cycle_health_report,
        validations=validation_contexts,
        enriched_views=enriched_view_contexts,
        field_mappings=field_mappings,
        active_assumptions=active_assumptions,
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
# Network-to-dict converters
# =============================================================================


def _column_network_to_dict(result: Any) -> dict[str, Any]:
    """Convert ColumnNetworkResult to dict for ColumnContext.entropy_scores.

    Args:
        result: ColumnNetworkResult from network inference

    Returns:
        Dict compatible with existing entropy_scores consumers
    """
    high_dims = [ne.dimension_path for ne in result.node_evidence if ne.state != "low"]
    resolution_hints = []
    for ne in sorted(result.node_evidence, key=lambda x: x.impact_delta, reverse=True):
        if ne.resolution_options and ne.state != "low":
            best = ne.resolution_options[0]
            resolution_hints.append(
                {
                    "action": best.get("action", ""),
                    "description": best.get("description", ""),
                    "effort": best.get("effort", ""),
                }
            )
    return {
        "worst_intent_p_high": result.worst_intent_p_high,
        "readiness": result.readiness,
        "top_priority_node": result.top_priority_node,
        "top_priority_impact": result.top_priority_impact,
        "high_entropy_dimensions": high_dims,
        "intents": [
            {"name": i.intent_name, "p_high": i.p_high, "readiness": i.readiness}
            for i in result.intents
        ],
        "resolution_hints": resolution_hints,
    }


def _table_network_to_dict(
    table_name: str,
    col_results: list[Any],
) -> dict[str, Any]:
    """Aggregate per-column network results into a table-level dict.

    Args:
        table_name: Table name
        col_results: List of ColumnNetworkResult for this table

    Returns:
        Dict compatible with existing table_entropy consumers
    """
    if not col_results:
        return {"readiness": "ready"}

    blocked = [r for r in col_results if r.readiness == "blocked"]
    investigate = [r for r in col_results if r.readiness == "investigate"]
    p_highs = [r.worst_intent_p_high for r in col_results]

    if blocked:
        readiness = "blocked"
    elif investigate:
        readiness = "investigate"
    else:
        readiness = "ready"

    return {
        "readiness": readiness,
        "columns_blocked": len(blocked),
        "columns_investigate": len(investigate),
        "avg_worst_intent_p_high": sum(p_highs) / len(p_highs),
        "max_worst_intent_p_high": max(p_highs),
        "blocked_columns": [r.target.removeprefix("column:").split(".", 1)[-1] for r in blocked],
    }


# =============================================================================
# Metadata Document Formatter
# =============================================================================


def format_metadata_document(
    context: GraphExecutionContext,
    source_name: str = "dataset",
) -> str:
    """Format execution context as a structured metadata document for LLM prompts.

    Produces a rich, pre-digested document with business names, descriptions,
    quality narratives, entropy assumptions, and actionable notes. Replaces
    both format_context_for_prompt() and format_entropy_for_prompt().

    Args:
        context: GraphExecutionContext from build_execution_context()
        source_name: Human-readable name for the data source

    Returns:
        Formatted markdown metadata document
    """
    lines: list[str] = []

    # --- Overview ---
    lines.append(f"# Data Catalog: {source_name}")
    lines.append("")
    lines.append("## Overview")
    lines.append("")

    overview_parts = [f"{context.total_tables} tables, {context.total_columns} columns."]
    if context.graph_pattern:
        overview_parts.append(f"Schema: {context.graph_pattern}.")
    if context.hub_tables:
        overview_parts.append(f"Hub: {', '.join(context.hub_tables)}.")
    if context.leaf_tables:
        overview_parts.append(f"Leaves: {', '.join(context.leaf_tables)}.")
    lines.append(" ".join(overview_parts))

    # Temporal coverage from column profiles
    temporal_info = _build_temporal_summary(context)
    if temporal_info:
        lines.append(temporal_info)

    # Data readiness from entropy summary
    readiness_info = _build_readiness_summary(context)
    if readiness_info:
        lines.append(readiness_info)

    if context.slice_column:
        lines.append(f"Active filter: {context.slice_column} = '{context.slice_value}'")

    lines.append("")

    # --- Tables ---
    lines.append("## Tables")

    for table in context.tables:
        table_type = ""
        if table.is_fact_table:
            table_type = "FACT"
        elif table.is_dimension_table:
            table_type = "DIMENSION"

        display_name = table.duckdb_name or table.table_name
        type_label = f" ({table_type})" if table_type else ""
        lines.append(f"\n### {display_name}{type_label}")

        # Entity description
        if table.entity_type:
            desc = f"**Entity**: {table.entity_type}"
            if table.table_description:
                desc += f" — {table.table_description}"
            lines.append(desc)

        # Grain, rows, time column
        meta_parts = []
        if table.grain_columns:
            meta_parts.append(f"**Grain**: {', '.join(table.grain_columns)}.")
        if table.row_count:
            meta_parts.append(f"**Rows**: {table.row_count:,}.")
        if table.time_column:
            # Find matching temporal column for time range
            time_col = next((c for c in table.columns if c.column_name == table.time_column), None)
            time_info = f"**Time column**: {table.time_column}"
            if time_col:
                time_parts = []
                if time_col.detected_granularity:
                    time_parts.append(time_col.detected_granularity)
                if time_col.min_timestamp and time_col.max_timestamp:
                    time_parts.append(f"{time_col.min_timestamp} to {time_col.max_timestamp}")
                if time_parts:
                    time_info += f" ({', '.join(time_parts)})"
            meta_parts.append(time_info + ".")
        if meta_parts:
            lines.append(" ".join(meta_parts))

        # Column table
        lines.append("")
        lines.append("| Column | Type | Role | Description | Notes |")
        lines.append("|--------|------|------|-------------|-------|")
        for col in table.columns:
            col_type = col.data_type or ""
            col_role = col.semantic_role or ""
            col_desc = _build_column_description(col)
            col_notes = _build_column_notes(col)
            lines.append(
                f"| {col.column_name} | {col_type} | {col_role} | {col_desc} | {col_notes} |"
            )

        # Quality section (per-table)
        _append_table_quality(lines, table)

        # Data quality notes (entropy interpretations for non-ready columns)
        _append_data_quality_notes(lines, table)

    # --- Relationships ---
    if context.relationships:
        lines.append("")
        lines.append("## Relationships")
        lines.append("")
        lines.append("| From | To | Cardinality | Confidence |")
        lines.append("|------|----|-------------|------------|")
        for rel in context.relationships:
            warning = ""
            if rel.relationship_entropy and not rel.relationship_entropy.get(
                "is_deterministic", True
            ):
                warning = " ⚠ non-deterministic"
            lines.append(
                f"| {rel.from_table}.{rel.from_column} | {rel.to_table}.{rel.to_column} "
                f"| {rel.cardinality or '?'} | {rel.confidence:.2f}{warning} |"
            )

    # --- Enriched Views ---
    if context.enriched_views:
        lines.append("")
        lines.append("## Enriched Views")

        slices_by_table: dict[str, list[SliceContext]] = {}
        for s in context.available_slices:
            slices_by_table.setdefault(s.table_name, []).append(s)

        for ev in context.enriched_views:
            verified = " (grain verified)" if ev.is_grain_verified else ""
            lines.append(f"\n### {ev.view_name}{verified}")
            lines.append(f"Fact table: {ev.fact_table}.")
            dims = ", ".join(ev.dimension_columns) if ev.dimension_columns else "none"
            lines.append(f"Joined columns: {dims}.")

            view_slices = slices_by_table.get(ev.fact_table, [])
            if view_slices:
                lines.append("Slice dimensions:")
                for s in view_slices:
                    vals_str = ""
                    if s.distinct_values:
                        vals = ", ".join(s.distinct_values[:10])
                        if len(s.distinct_values) > 10:
                            vals += f", +{len(s.distinct_values) - 10} more"
                        vals_str = f": [{vals}]"
                    lines.append(f"  - **{s.column_name}** ({s.value_count} values){vals_str}")

    # --- Business Processes ---
    if context.business_cycles:
        lines.append("")
        lines.append("## Business Processes")
        _append_business_processes(lines, context)

    # --- Assumptions in Effect ---
    if context.active_assumptions:
        lines.append("")
        lines.append("## Assumptions in Effect")
        lines.append("")
        lines.append("Pre-computed from entropy analysis. Apply unless user specifies otherwise:")
        lines.append("")
        for assumption in context.active_assumptions:
            text = assumption.get("assumption_text", "")
            conf = assumption.get("confidence", "")
            basis = assumption.get("basis", "")
            impact = assumption.get("impact", "")
            tbl = assumption.get("table", "")
            col_name = assumption.get("column", "")
            lines.append(f"- **{tbl}.{col_name}**: {text} (confidence: {conf})")
            if basis:
                lines.append(f"  Basis: {basis}. Impact: {impact}.")

    # --- Validation Results ---
    if context.validations:
        lines.append("")
        lines.append("## Validation Results")
        lines.append("")
        failed = [v for v in context.validations if not v.passed]
        passed = [v for v in context.validations if v.passed]
        lines.append(f"PASSED: {len(passed)} | FAILED: {len(failed)}")
        if failed:
            lines.append("")
            lines.append("Failed:")
            for v in failed:
                lines.append(f"- [{v.severity.upper()}] {v.validation_id}: {v.message}")
                if v.details:
                    summary = v.details.get("summary", "")
                    if summary:
                        lines.append(f"  Details: {summary}")

    return "\n".join(lines)


def _build_temporal_summary(context: GraphExecutionContext) -> str | None:
    """Build temporal coverage line from column profiles."""
    earliest = None
    latest = None
    granularity = None
    completeness_values: list[float] = []

    for table in context.tables:
        for col in table.columns:
            if col.min_timestamp:
                if earliest is None or col.min_timestamp < earliest:
                    earliest = col.min_timestamp
            if col.max_timestamp:
                if latest is None or col.max_timestamp > latest:
                    latest = col.max_timestamp
            if col.detected_granularity and not granularity:
                granularity = col.detected_granularity
            if col.completeness_ratio is not None:
                completeness_values.append(col.completeness_ratio)

    if not earliest:
        return None

    parts = [f"Temporal coverage: {earliest} to {latest}"]
    if granularity:
        parts.append(f" ({granularity}")
        if completeness_values:
            avg = sum(completeness_values) / len(completeness_values)
            parts.append(f", {avg:.0%} complete")
        parts.append(")")
    elif completeness_values:
        avg = sum(completeness_values) / len(completeness_values)
        parts.append(f" ({avg:.0%} complete)")

    return "".join(parts) + "."


def _build_readiness_summary(context: GraphExecutionContext) -> str | None:
    """Build data readiness line from entropy summary."""
    if not context.entropy_summary:
        return None

    summary = context.entropy_summary
    readiness = summary.get("overall_readiness", "unknown")
    columns_with_assumptions = len({(a["table"], a["column"]) for a in context.active_assumptions})
    blocked_count = summary.get("critical_entropy_count", 0)

    return (
        f"Data readiness: {readiness} "
        f"({columns_with_assumptions} columns need assumptions, {blocked_count} blocked)."
    )


def _build_column_description(col: ColumnContext) -> str:
    """Build column description from business metadata."""
    parts = []
    if col.business_name:
        parts.append(col.business_name)
        if col.business_description:
            parts.append(f": {col.business_description}")
    elif col.business_concept:
        parts.append(col.business_concept)
        if col.temporal_behavior:
            parts.append(f" ({col.temporal_behavior})")
    return "".join(parts)


def _build_column_notes(col: ColumnContext) -> str:
    """Build column notes from derived, unit, quality, and entropy data."""
    notes = []

    if col.unit_source_column:
        notes.append(f"Unit source: {col.unit_source_column}.")

    if col.is_derived and col.derived_formula:
        notes.append(f"Derived: {col.derived_formula}.")

    if col.quality_grade:
        notes.append(f"Grade: {col.quality_grade}.")

    # Entropy readiness indicator
    if col.entropy_scores:
        readiness = col.entropy_scores.get("readiness", "ready")
        if readiness == "blocked":
            notes.append("⛔ blocked.")
        elif readiness == "investigate":
            notes.append("⚠ investigate.")

    if col.flags:
        notes.append(f"Flags: {', '.join(col.flags)}.")

    return " ".join(notes)


def _append_table_quality(lines: list[str], table: TableContext) -> None:
    """Append quality section for a table."""
    quality_cols = [col for col in table.columns if col.quality_grade and col.quality_summary]
    if not quality_cols:
        return

    lines.append("")
    lines.append("**Quality**:")
    for col in quality_cols:
        lines.append(f"- {col.column_name} (Grade {col.quality_grade}): {col.quality_summary}")
        for finding in col.quality_findings[:2]:
            lines.append(f"  - {finding}")
        for rec in col.quality_recommendations[:2]:
            lines.append(f"  - Recommendation: {rec}")


def _append_data_quality_notes(lines: list[str], table: TableContext) -> None:
    """Append data quality notes from entropy interpretations."""
    has_notes = False

    # Table-level entropy interpretation
    if table.table_entropy_explanation:
        lines.append("")
        lines.append("**Data Quality Notes**:")
        lines.append(f"- Table: {table.table_entropy_explanation}")
        for assumption in table.table_entropy_assumptions:
            text = assumption.get("assumption_text", "")
            conf = assumption.get("confidence", "")
            basis = assumption.get("basis", "")
            lines.append(f"  Assumption: {text} (confidence: {conf}, basis: {basis})")
        has_notes = True

    # Column-level entropy interpretations (non-ready columns)
    notes_cols = [
        col
        for col in table.columns
        if col.entropy_explanation
        and col.entropy_scores
        and col.entropy_scores.get("readiness", "ready") != "ready"
    ]
    if not notes_cols:
        return

    if not has_notes:
        lines.append("")
        lines.append("**Data Quality Notes**:")
    for col in notes_cols:
        lines.append(f"- {col.column_name}: {col.entropy_explanation}")
        for assumption in col.entropy_assumptions:
            text = assumption.get("assumption_text", "")
            conf = assumption.get("confidence", "")
            basis = assumption.get("basis", "")
            lines.append(f"  Assumption: {text} (confidence: {conf}, basis: {basis})")


def _append_business_processes(lines: list[str], context: GraphExecutionContext) -> None:
    """Append business processes section."""
    # Build health lookup
    health_lookup: dict[str, Any] = {}
    if context.cycle_health:
        for cs in context.cycle_health.cycle_scores:
            if cs.canonical_type:
                health_lookup[cs.canonical_type] = cs

    for cycle in context.business_cycles:
        # Determine verification status
        health_score = health_lookup.get(cycle.cycle_type)
        if health_score:
            score = health_score.composite_score
            if score is not None and score >= 0.8:
                status = "VERIFIED"
            elif score is not None and score >= 0.5:
                status = "PARTIAL"
            else:
                status = "UNVERIFIED"
            val_info = (
                f"({health_score.validations_passed}/{health_score.validations_run} validations)"
            )
        else:
            status = "UNVERIFIED"
            val_info = ""

        lines.append(f"\n### {cycle.cycle_name} ({cycle.cycle_type}) — {status} {val_info}")
        lines.append("")

        if cycle.description:
            lines.append(cycle.description)

        # Volume
        volume_parts = []
        if cycle.total_records is not None:
            volume_parts.append(f"{cycle.total_records:,} records")
        if cycle.completed_cycles is not None:
            volume_parts.append(f"{cycle.completed_cycles:,} completed")
        if cycle.completion_rate is not None:
            volume_parts.append(f"{cycle.completion_rate:.0%} completion rate")
        if volume_parts:
            lines.append(f"Volume: {', '.join(volume_parts)}.")

        # Evidence
        if cycle.evidence:
            evidence_str = "; ".join(cycle.evidence[:3])
            lines.append(f"Evidence: {evidence_str}")

        # Stages
        if cycle.stages:
            lines.append("")
            lines.append("Stages:")
            for stage in sorted(cycle.stages, key=lambda s: s.stage_order):
                vals = ", ".join(stage.indicator_values) if stage.indicator_values else ""
                ind_col = f" {stage.indicator_column}" if stage.indicator_column else ""
                indicator = f" →{ind_col} in [{vals}]" if vals else ""
                progress = (
                    f" ({stage.completion_rate:.0%})" if stage.completion_rate is not None else ""
                )
                lines.append(f"  {stage.stage_order}. {stage.stage_name}{indicator}{progress}")

        # Completion tracking
        if cycle.status_column and cycle.completion_value:
            lines.append(
                f'Completion: {cycle.status_column} = "{cycle.completion_value}"'
                + (
                    f", {cycle.completion_rate:.0%} complete"
                    if cycle.completion_rate is not None
                    else ""
                )
                + "."
            )

        # Entity flows
        if cycle.entity_flows:
            for ef in cycle.entity_flows:
                lines.append(
                    f"Entity flow: {ef.entity_type} "
                    f"({ef.entity_table}.{ef.entity_column}) → {ef.fact_table}."
                )

    return


__all__ = [
    "ColumnContext",
    "TableContext",
    "RelationshipContext",
    "SliceContext",
    "CycleStageContext",
    "EntityFlowContext",
    "BusinessCycleContext",
    "ValidationContext",
    "EnrichedViewContext",
    "GraphExecutionContext",
    "build_execution_context",
    "format_metadata_document",
]
