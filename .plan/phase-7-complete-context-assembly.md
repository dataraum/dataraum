# Phase 7: Complete Context Assembly Implementation

**Status:** Planning  
**Date:** 2025-11-28  
**Dependencies:** 
- Phase 1 (Statistical Quality) ✅
- Phase 2A (LLM Infrastructure) ✅
- Phase 3 (Enrichment) ✅ (partially)
- Quality Synthesis ✅

---

## Executive Summary

Phase 7 completes the context assembly layer by:

1. **Completing missing converters** (semantic, temporal, correlation)
2. **Implementing remaining assembly functions** (semantic, temporal, correlation, quality)
3. **Integrating LLM features** (summary, queries, quality rules)
4. **Assembling complete ContextDocument** with all 5 pillars
5. **Testing end-to-end context generation**

**Goal:** Produce complete, AI-ready `ContextDocument` that aggregates all metadata from the 5 analytical pillars.

---

## Current Status Analysis

### What's Already Implemented ✅

| Component | Status | Location |
|-----------|--------|----------|
| **Assembly Infrastructure** | ✅ Complete | `context/assembly.py` |
| - Statistical profiling assembly | ✅ | `_assemble_statistical_profiling()` |
| - Statistical quality assembly | ✅ | `_assemble_statistical_quality()` |
| - Topological summary assembly | ✅ | `_assemble_topological_summary()` |
| **Converters (Partial)** | ⚠️ Partial | `context/converters.py` |
| - Statistical converters | ✅ | `convert_statistical_profile()`, etc. |
| - Topological converters | ✅ | `convert_topological_metrics()` |
| **Quality Synthesis** | ✅ Complete | `quality/synthesis.py` |
| - Column quality assessment | ✅ | `assess_column_quality()` |
| - Table quality assessment | ✅ | `assess_table_quality()` |
| - Dimension scoring | ✅ | All 6 dimensions |
| - Issue aggregation | ✅ | From all pillars |
| **LLM Features** | ✅ Complete | `llm/features/` |
| - Semantic analysis | ✅ | `semantic.py` |
| - Summary generation | ✅ | `summary.py` |
| - Query generation | ✅ | `queries.py` |
| - Quality rule generation | ✅ | `quality.py` |
| **Enrichment** | ✅ Complete | `enrichment/` |
| - Semantic enrichment | ✅ | `semantic.py` |
| - Temporal enrichment | ✅ | `temporal.py` |
| - Topology enrichment | ✅ | `topology.py` |

### What's Missing ❌

| Component | Status | Required For |
|-----------|--------|--------------|
| **Converters** | ❌ Missing | Assembly |
| - Semantic converter | ❌ | Convert DB → Pydantic |
| - Temporal converter | ❌ | Convert DB → Pydantic |
| - Correlation converters | ❌ | Convert DB → Pydantic |
| **Assembly Functions** | ❌ Missing | ContextDocument |
| - `_assemble_correlation_analysis()` | ❌ | Pillar 1 |
| - `_assemble_semantic()` | ❌ | Pillar 3 |
| - `_assemble_temporal_summary()` | ❌ | Pillar 4 |
| - `_assemble_quality()` | ❌ | Pillar 5 |
| **LLM Integration** | ❌ Missing | AI features |
| - Summary generation in assembly | ❌ | ContextDocument.ai_summary |
| - Query generation in assembly | ❌ | ContextDocument.suggested_queries |
| - Quality rule integration | ❌ | Quality pillar |
| **Ontology Loading** | ❌ Missing | Domain concepts |
| - Load relevant metrics | ❌ | ContextDocument.relevant_metrics |
| - Load domain concepts | ❌ | ContextDocument.domain_concepts |

---

## Architecture Overview

### 5-Pillar Context Structure

```
ContextDocument
├─ Pillar 1: Statistical Context
│  ├─ statistical_profiling: StatisticalProfilingResult ✅
│  ├─ statistical_quality: StatisticalQualityResult ✅
│  └─ correlation_analysis: CorrelationAnalysisResult ❌
│
├─ Pillar 2: Topological Context
│  ├─ topology: TopologyEnrichmentResult ❌
│  └─ topological_summary: TopologicalSummary ✅
│
├─ Pillar 3: Semantic Context
│  └─ semantic: SemanticEnrichmentResult ❌
│
├─ Pillar 4: Temporal Context
│  └─ temporal_summary: TemporalQualitySummary ❌
│
├─ Pillar 5: Quality Context (Synthesized)
│  └─ quality: QualitySynthesisResult ❌
│
├─ Ontology Content
│  ├─ relevant_metrics: list[MetricDefinition] ❌
│  └─ domain_concepts: list[DomainConcept] ❌
│
└─ LLM-Generated Content
   ├─ suggested_queries: list[SuggestedQuery] ❌
   ├─ ai_summary: str ❌
   ├─ key_facts: list[str] ❌
   └─ warnings: list[str] ❌
```

---

## Implementation Plan

### Task 1: Complete Converters (context/converters.py)

**Status:** ❌ Not Started  
**Priority:** HIGH  
**Dependencies:** None

#### 1.1: Semantic Converters

**Add to `context/converters.py`:**

```python
def convert_semantic_annotation(
    db_annotation: SemanticAnnotationDB,
) -> SemanticAnnotation:
    """Convert SQLAlchemy SemanticAnnotation to Pydantic."""
    from dataraum_context.core.models import SemanticRole, DecisionSource
    
    return SemanticAnnotation(
        annotation_id=db_annotation.annotation_id,
        column_id=db_annotation.column_id,
        column_ref=ColumnRef(
            table_name="",  # Loaded from join
            column_name="",  # Loaded from join
        ),
        semantic_role=SemanticRole(db_annotation.semantic_role),
        entity_type=db_annotation.entity_type,
        business_name=db_annotation.business_name,
        business_description=db_annotation.business_description,
        annotation_source=DecisionSource(db_annotation.annotation_source),
        annotated_by=db_annotation.annotated_by,
        annotated_at=db_annotation.annotated_at,
        confidence=db_annotation.confidence or 0.8,
    )


def convert_table_entity(
    db_entity: TableEntityDB,
) -> EntityDetection:
    """Convert SQLAlchemy TableEntity to Pydantic EntityDetection."""
    return EntityDetection(
        table_id=db_entity.table_id,
        table_name="",  # Loaded from join
        entity_type=db_entity.detected_entity_type,
        description=db_entity.description,
        confidence=db_entity.confidence or 0.8,
        evidence=db_entity.evidence or {},
        grain_columns=db_entity.grain_columns.get("columns", []) if db_entity.grain_columns else [],
        is_fact_table=db_entity.is_fact_table,
        is_dimension_table=db_entity.is_dimension_table,
        time_column=db_entity.time_column,
    )


def convert_relationship(
    db_rel: RelationshipDB,
) -> Relationship:
    """Convert SQLAlchemy Relationship to Pydantic."""
    from dataraum_context.core.models import RelationshipType, Cardinality
    
    cardinality = None
    if db_rel.cardinality:
        cardinality = Cardinality(db_rel.cardinality)
    
    return Relationship(
        relationship_id=db_rel.relationship_id,
        from_table="",  # Loaded from join
        from_column="",  # Loaded from join
        to_table="",  # Loaded from join
        to_column="",  # Loaded from join
        relationship_type=RelationshipType(db_rel.relationship_type),
        cardinality=cardinality,
        confidence=db_rel.confidence or 0.8,
        detection_method=db_rel.detection_method,
        evidence=db_rel.evidence or {},
    )
```

#### 1.2: Temporal Converters

**Add to `context/converters.py`:**

```python
def convert_temporal_quality_metrics(
    db_metrics: TemporalQualityMetricsDB,
) -> TemporalQualityResult:
    """Convert SQLAlchemy TemporalQualityMetrics to Pydantic."""
    from dataraum_context.core.models import (
        SeasonalDecompositionResult,
        ChangePointResult,
        DistributionShiftResult,
    )
    
    # Convert seasonal decomposition if present
    seasonal_decomp = None
    if db_metrics.seasonality_strength is not None:
        seasonal_decomp = SeasonalDecompositionResult(
            has_seasonality=db_metrics.has_seasonality,
            seasonality_strength=db_metrics.seasonality_strength,
            seasonal_period=db_metrics.seasonal_period,
            trend_strength=db_metrics.trend_strength or 0.0,
            residual_variance=0.0,  # Not stored in DB yet
        )
    
    # Convert change points if present
    change_points = []
    if db_metrics.change_points:
        for cp_data in db_metrics.change_points.get("change_points", []):
            change_points.append(
                ChangePointResult(
                    timestamp=cp_data["timestamp"],
                    confidence=cp_data.get("confidence", 0.8),
                    change_type=cp_data.get("change_type", "unknown"),
                    magnitude=cp_data.get("magnitude", 0.0),
                )
            )
    
    # Convert distribution shifts
    distribution_shifts = []
    if db_metrics.distribution_shifts:
        for shift_data in db_metrics.distribution_shifts.get("shifts", []):
            distribution_shifts.append(
                DistributionShiftResult(
                    period_start=shift_data["period_start"],
                    period_end=shift_data["period_end"],
                    ks_statistic=shift_data.get("ks_statistic", 0.0),
                    p_value=shift_data.get("p_value", 1.0),
                    is_significant=shift_data.get("is_significant", False),
                )
            )
    
    return TemporalQualityResult(
        completeness_ratio=db_metrics.completeness_ratio or 1.0,
        gap_count=db_metrics.gap_count or 0,
        largest_gap_days=db_metrics.largest_gap_days,
        seasonal_decomposition=seasonal_decomp,
        change_points=change_points,
        distribution_shifts=distribution_shifts,
        data_freshness_days=db_metrics.data_freshness_days,
        is_stale=db_metrics.is_stale,
        quality_issues=db_metrics.quality_issues or [],
    )
```

#### 1.3: Correlation Converters

**Add to `context/converters.py`:**

```python
def convert_column_correlation(
    db_corr: ColumnCorrelationDB,
) -> ColumnCorrelationPair:
    """Convert SQLAlchemy ColumnCorrelation to Pydantic."""
    from dataraum_context.core.models import CorrelationType
    
    correlation_types = []
    if db_corr.pearson_r is not None:
        correlation_types.append(CorrelationType.PEARSON)
    if db_corr.spearman_rho is not None:
        correlation_types.append(CorrelationType.SPEARMAN)
    if db_corr.kendall_tau is not None:
        correlation_types.append(CorrelationType.KENDALL)
    
    return ColumnCorrelationPair(
        column1_id=db_corr.column1_id,
        column2_id=db_corr.column2_id,
        correlation_types=correlation_types,
        pearson_r=db_corr.pearson_r,
        spearman_rho=db_corr.spearman_rho,
        kendall_tau=db_corr.kendall_tau,
        p_value=db_corr.p_value,
        is_significant=db_corr.is_significant,
        strength=db_corr.correlation_strength or "unknown",
    )


def convert_functional_dependency(
    db_fd: FunctionalDependencyDB,
) -> FunctionalDependencyResult:
    """Convert SQLAlchemy FunctionalDependency to Pydantic."""
    return FunctionalDependencyResult(
        determinant_columns=db_fd.determinant_column_ids,
        dependent_column=db_fd.dependent_column_id,
        confidence=db_fd.confidence or 0.0,
        violation_count=db_fd.violation_count or 0,
        is_approximate=db_fd.is_approximate,
    )


def convert_categorical_association(
    db_assoc: CategoricalAssociationDB,
) -> CategoricalAssociationResult:
    """Convert SQLAlchemy CategoricalAssociation to Pydantic."""
    return CategoricalAssociationResult(
        column1_id=db_assoc.column1_id,
        column2_id=db_assoc.column2_id,
        cramers_v=db_assoc.cramers_v or 0.0,
        chi_square=db_assoc.chi_square,
        p_value=db_assoc.p_value,
        degrees_of_freedom=db_assoc.degrees_of_freedom or 0,
        is_significant=db_assoc.is_significant,
        strength=db_assoc.association_strength or "unknown",
    )
```

**Deliverables:**
- [ ] Semantic converters implemented
- [ ] Temporal converters implemented
- [ ] Correlation converters implemented
- [ ] Unit tests for all converters

---

### Task 2: Implement Missing Assembly Functions

**Status:** ❌ Not Started  
**Priority:** HIGH  
**Dependencies:** Task 1

#### 2.1: Correlation Analysis Assembly

**Add to `context/assembly.py`:**

```python
async def _assemble_correlation_analysis(
    tables: list[Table],
    session: AsyncSession,
) -> CorrelationAnalysisResult | None:
    """Assemble correlation analysis for all tables.
    
    Aggregates:
    - ColumnCorrelation (Pearson, Spearman, Kendall)
    - FunctionalDependency
    - CategoricalAssociation
    """
    from dataraum_context.storage.models_v2.correlation_context import (
        ColumnCorrelation,
        FunctionalDependency,
        CategoricalAssociation,
    )
    
    all_correlations = []
    all_functional_deps = []
    all_categorical_assocs = []
    
    for table in tables:
        # Get column IDs for this table
        await session.refresh(table, ["columns"])
        column_ids = [col.column_id for col in table.columns]
        
        # Query correlations
        stmt = select(ColumnCorrelation).where(
            ColumnCorrelation.column1_id.in_(column_ids)
        ).order_by(ColumnCorrelation.computed_at.desc())
        
        result = await session.execute(stmt)
        correlations = result.scalars().all()
        
        for db_corr in correlations:
            pydantic_corr = convert_column_correlation(db_corr)
            all_correlations.append(pydantic_corr)
        
        # Query functional dependencies
        stmt = select(FunctionalDependency).where(
            FunctionalDependency.table_id == table.table_id
        ).order_by(FunctionalDependency.computed_at.desc())
        
        result = await session.execute(stmt)
        fds = result.scalars().all()
        
        for db_fd in fds:
            pydantic_fd = convert_functional_dependency(db_fd)
            all_functional_deps.append(pydantic_fd)
        
        # Query categorical associations
        stmt = select(CategoricalAssociation).where(
            CategoricalAssociation.column1_id.in_(column_ids)
        ).order_by(CategoricalAssociation.computed_at.desc())
        
        result = await session.execute(stmt)
        assocs = result.scalars().all()
        
        for db_assoc in assocs:
            pydantic_assoc = convert_categorical_association(db_assoc)
            all_categorical_assocs.append(pydantic_assoc)
    
    if not all_correlations and not all_functional_deps and not all_categorical_assocs:
        return None
    
    return CorrelationAnalysisResult(
        correlations=all_correlations,
        functional_dependencies=all_functional_deps,
        categorical_associations=all_categorical_assocs,
        high_correlation_count=len([c for c in all_correlations 
                                     if c.pearson_r and abs(c.pearson_r) > 0.9]),
    )
```

#### 2.2: Semantic Assembly

**Add to `context/assembly.py`:**

```python
async def _assemble_semantic(
    tables: list[Table],
    session: AsyncSession,
) -> SemanticEnrichmentResult | None:
    """Assemble semantic enrichment results.
    
    Aggregates:
    - SemanticAnnotation (column-level)
    - TableEntity (table-level)
    - Relationship (cross-table)
    """
    from dataraum_context.storage.models_v2.semantic_context import (
        SemanticAnnotation,
        TableEntity,
        Relationship,
    )
    
    all_annotations = []
    all_entities = []
    all_relationships = []
    
    # Build column name lookup
    column_lookup = {}  # column_id -> (table_name, column_name)
    table_lookup = {}   # table_id -> table_name
    
    for table in tables:
        await session.refresh(table, ["columns"])
        table_lookup[table.table_id] = table.table_name
        
        for col in table.columns:
            column_lookup[col.column_id] = (table.table_name, col.column_name)
    
    # Query annotations
    for table in tables:
        stmt = (
            select(SemanticAnnotation)
            .join(Column)
            .where(Column.table_id == table.table_id)
            .order_by(SemanticAnnotation.annotated_at.desc())
        )
        
        result = await session.execute(stmt)
        annotations = result.scalars().all()
        
        for db_annotation in annotations:
            pydantic_annotation = convert_semantic_annotation(db_annotation)
            
            # Fill in table/column names
            if db_annotation.column_id in column_lookup:
                table_name, col_name = column_lookup[db_annotation.column_id]
                pydantic_annotation.column_ref = ColumnRef(
                    table_name=table_name,
                    column_name=col_name,
                )
            
            all_annotations.append(pydantic_annotation)
    
    # Query table entities
    table_ids = [t.table_id for t in tables]
    stmt = select(TableEntity).where(TableEntity.table_id.in_(table_ids))
    result = await session.execute(stmt)
    entities = result.scalars().all()
    
    for db_entity in entities:
        pydantic_entity = convert_table_entity(db_entity)
        
        # Fill in table name
        if db_entity.table_id in table_lookup:
            pydantic_entity.table_name = table_lookup[db_entity.table_id]
        
        all_entities.append(pydantic_entity)
    
    # Query relationships
    stmt = select(Relationship).where(
        Relationship.from_table_id.in_(table_ids)
        | Relationship.to_table_id.in_(table_ids)
    )
    result = await session.execute(stmt)
    relationships = result.scalars().all()
    
    for db_rel in relationships:
        pydantic_rel = convert_relationship(db_rel)
        
        # Fill in table/column names
        if db_rel.from_table_id in table_lookup:
            pydantic_rel.from_table = table_lookup[db_rel.from_table_id]
        if db_rel.to_table_id in table_lookup:
            pydantic_rel.to_table = table_lookup[db_rel.to_table_id]
        if db_rel.from_column_id in column_lookup:
            _, col_name = column_lookup[db_rel.from_column_id]
            pydantic_rel.from_column = col_name
        if db_rel.to_column_id in column_lookup:
            _, col_name = column_lookup[db_rel.to_column_id]
            pydantic_rel.to_column = col_name
        
        all_relationships.append(pydantic_rel)
    
    if not all_annotations and not all_entities and not all_relationships:
        return None
    
    return SemanticEnrichmentResult(
        annotations=all_annotations,
        entity_detections=all_entities,
        relationships=all_relationships,
        source="database",
    )
```

#### 2.3: Temporal Summary Assembly

**Add to `context/assembly.py`:**

```python
async def _assemble_temporal_summary(
    tables: list[Table],
    session: AsyncSession,
) -> TemporalQualitySummary | None:
    """Assemble temporal quality summary across all tables."""
    from dataraum_context.storage.models_v2.temporal_context import (
        TemporalQualityMetrics,
    )
    
    temporal_results = []
    
    for table in tables:
        await session.refresh(table, ["columns"])
        
        for column in table.columns:
            # Get most recent temporal quality metrics
            stmt = (
                select(TemporalQualityMetrics)
                .where(TemporalQualityMetrics.column_id == column.column_id)
                .order_by(TemporalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            db_metrics = result.scalar_one_or_none()
            
            if db_metrics:
                pydantic_result = convert_temporal_quality_metrics(db_metrics)
                temporal_results.append(pydantic_result)
    
    if not temporal_results:
        return None
    
    # Aggregate into summary
    total_completeness = sum(r.completeness_ratio for r in temporal_results)
    avg_completeness = total_completeness / len(temporal_results)
    
    total_gaps = sum(r.gap_count for r in temporal_results)
    
    has_seasonality = any(
        r.seasonal_decomposition and r.seasonal_decomposition.has_seasonality
        for r in temporal_results
    )
    
    has_change_points = any(
        r.change_points and len(r.change_points) > 0
        for r in temporal_results
    )
    
    stale_columns = sum(1 for r in temporal_results if r.is_stale)
    
    return TemporalQualitySummary(
        temporal_columns_analyzed=len(temporal_results),
        average_completeness=avg_completeness,
        total_gaps=total_gaps,
        has_seasonality=has_seasonality,
        has_change_points=has_change_points,
        stale_columns=stale_columns,
    )
```

#### 2.4: Quality Synthesis Assembly

**Add to `context/assembly.py`:**

```python
async def _assemble_quality(
    source_id: str,
    tables: list[Table],
    session: AsyncSession,
) -> QualitySynthesisResult | None:
    """Assemble quality synthesis for the source.
    
    Uses the quality synthesis module to aggregate quality from all pillars.
    """
    from dataraum_context.quality.synthesis import assess_table_quality
    
    # For now, assess quality for the first table
    # In production, you'd aggregate across all tables
    if not tables:
        return None
    
    # Use the first typed table
    table = tables[0]
    
    result = await assess_table_quality(table.table_id, session)
    
    if result.success:
        return result.value
    
    return None
```

**Deliverables:**
- [ ] Correlation assembly implemented
- [ ] Semantic assembly implemented
- [ ] Temporal summary assembly implemented
- [ ] Quality assembly implemented
- [ ] Unit tests for each function

---

### Task 3: LLM Feature Integration

**Status:** ❌ Not Started  
**Priority:** MEDIUM  
**Dependencies:** Task 2

#### 3.1: Context Summary Generation

**Add to `context/assembly.py`:**

```python
async def _generate_llm_summary(
    session: AsyncSession,
    llm_service: LLMService,
    semantic_result: SemanticEnrichmentResult | None,
    quality_result: QualitySynthesisResult | None,
) -> tuple[str | None, list[str], list[str]]:
    """Generate LLM-powered context summary.
    
    Returns:
        Tuple of (ai_summary, key_facts, warnings)
    """
    if not semantic_result:
        return (None, [], [])
    
    from dataraum_context.core.models import QualitySummary
    
    # Build quality summary
    quality_summary = None
    if quality_result:
        quality_summary = QualitySummary(
            overall_score=quality_result.table_assessment.overall_score,
            tables_assessed=1,
            issues_found=quality_result.total_issues,
            critical_issues=quality_result.critical_issues,
        )
    
    # Call LLM summary feature
    result = await llm_service.generate_summary(
        session=session,
        semantic_result=semantic_result,
        quality_summary=quality_summary,
    )
    
    if result.success and result.value:
        summary = result.value
        return (summary.summary, summary.key_facts, summary.warnings)
    
    return (None, [], [])
```

#### 3.2: Suggested Queries Generation

**Add to `context/assembly.py`:**

```python
async def _generate_suggested_queries(
    session: AsyncSession,
    llm_service: LLMService,
    semantic_result: SemanticEnrichmentResult | None,
    ontology: str,
    ontology_metrics: list[dict] | None,
) -> list[SuggestedQuery]:
    """Generate LLM-powered suggested queries."""
    if not semantic_result:
        return []
    
    result = await llm_service.generate_queries(
        session=session,
        semantic_result=semantic_result,
        ontology=ontology,
        ontology_metrics=ontology_metrics or [],
    )
    
    if result.success and result.value:
        return result.value
    
    return []
```

#### 3.3: Quality Rule Generation

**Add to `context/assembly.py`:**

```python
async def _generate_quality_rules(
    session: AsyncSession,
    llm_service: LLMService,
    semantic_result: SemanticEnrichmentResult | None,
    ontology: str,
) -> list[QualityRule]:
    """Generate LLM-powered quality rules."""
    if not semantic_result:
        return []
    
    # Load ontology rules
    from dataraum_context.storage.models_v2 import Ontology as OntologyModel
    
    stmt = select(OntologyModel).where(OntologyModel.name == ontology)
    result = await session.execute(stmt)
    ontology_obj = result.scalar_one_or_none()
    
    ontology_rules = {}
    if ontology_obj and ontology_obj.quality_rules:
        ontology_rules = ontology_obj.quality_rules
    
    result = await llm_service.generate_quality_rules(
        session=session,
        semantic_result=semantic_result,
        ontology=ontology,
        ontology_rules=ontology_rules,
    )
    
    if result.success and result.value:
        return result.value
    
    return []
```

**Deliverables:**
- [ ] Summary generation integrated
- [ ] Query generation integrated
- [ ] Quality rule generation integrated
- [ ] Tests for LLM integration

---

### Task 4: Ontology Loading

**Status:** ❌ Not Started  
**Priority:** MEDIUM  
**Dependencies:** None

#### 4.1: Ontology Metrics Loading

**Add to `context/assembly.py`:**

```python
async def _load_ontology_metrics(
    session: AsyncSession,
    ontology: str,
) -> list[dict]:
    """Load relevant metrics from ontology."""
    from dataraum_context.storage.models_v2 import Ontology as OntologyModel
    
    stmt = select(OntologyModel).where(OntologyModel.name == ontology)
    result = await session.execute(stmt)
    ontology_obj = result.scalar_one_or_none()
    
    if not ontology_obj or not ontology_obj.metrics:
        return []
    
    # Convert metrics dict to list
    metrics_list = []
    for metric_name, metric_spec in ontology_obj.metrics.items():
        metrics_list.append({
            "name": metric_name,
            **metric_spec
        })
    
    return metrics_list
```

#### 4.2: Ontology Concepts Loading

**Add to `context/assembly.py`:**

```python
async def _load_ontology_concepts(
    session: AsyncSession,
    ontology: str,
) -> list[dict]:
    """Load domain concepts from ontology."""
    from dataraum_context.storage.models_v2 import Ontology as OntologyModel
    
    stmt = select(OntologyModel).where(OntologyModel.name == ontology)
    result = await session.execute(stmt)
    ontology_obj = result.scalar_one_or_none()
    
    if not ontology_obj or not ontology_obj.concepts:
        return []
    
    # Convert concepts dict to list
    concepts_list = []
    for concept_name, concept_spec in ontology_obj.concepts.items():
        concepts_list.append({
            "name": concept_name,
            **concept_spec
        })
    
    return concepts_list
```

**Deliverables:**
- [ ] Ontology metrics loading implemented
- [ ] Ontology concepts loading implemented
- [ ] Tests for ontology loading

---

### Task 5: Update Main Assembly Function

**Status:** ❌ Not Started  
**Priority:** HIGH  
**Dependencies:** Tasks 1-4

#### 5.1: Complete `assemble_context_document()`

**Update `context/assembly.py`:**

```python
async def assemble_context_document(
    source_id: str,
    ontology: str,
    session: AsyncSession,
    llm_service: LLMService | None = None,
    include_llm_features: bool = True,
) -> Result[ContextDocument]:
    """Assemble complete context document for a data source.
    
    This is the main entry point for context assembly. It:
    1. Loads source and tables from database
    2. Fetches and converts each pillar's metadata
    3. Aggregates into ContextDocument
    4. Optionally generates LLM features
    5. Returns ready for AI consumption
    
    Args:
        source_id: The source to assemble context for
        ontology: Ontology to apply (e.g., 'financial_reporting')
        session: Database session
        llm_service: Optional LLM service for AI features
        include_llm_features: Whether to generate LLM features
    
    Returns:
        Result containing ContextDocument or error
    """
    start_time = datetime.now()
    
    # Load source
    result = await session.execute(
        select(Source).where(Source.source_id == source_id).options(selectinload(Source.tables))
    )
    source = result.scalar_one_or_none()
    
    if not source:
        return Result.fail(f"Source not found: {source_id}")
    
    # Filter to only typed tables
    typed_tables = [t for t in source.tables if t.layer == "typed"]
    
    if not typed_tables:
        return Result.fail(f"No typed tables found for source: {source_id}")
    
    # ========================================================================
    # Pillar 1: Statistical Context
    # ========================================================================
    statistical_profiling = await _assemble_statistical_profiling(typed_tables, session)
    statistical_quality = await _assemble_statistical_quality(typed_tables, session)
    correlation_analysis = await _assemble_correlation_analysis(typed_tables, session)
    
    # ========================================================================
    # Pillar 2: Topological Context
    # ========================================================================
    topology = None  # TODO: Load from relationships/topology cache
    topological_summary = await _assemble_topological_summary(typed_tables, session)
    
    # ========================================================================
    # Pillar 3: Semantic Context
    # ========================================================================
    semantic = await _assemble_semantic(typed_tables, session)
    
    # ========================================================================
    # Pillar 4: Temporal Context
    # ========================================================================
    temporal_summary = await _assemble_temporal_summary(typed_tables, session)
    
    # ========================================================================
    # Pillar 5: Quality Context (Synthesized)
    # ========================================================================
    quality = await _assemble_quality(source_id, typed_tables, session)
    
    # ========================================================================
    # Ontology Content
    # ========================================================================
    relevant_metrics = await _load_ontology_metrics(session, ontology)
    domain_concepts = await _load_ontology_concepts(session, ontology)
    
    # ========================================================================
    # LLM-Generated Content (Optional)
    # ========================================================================
    ai_summary = None
    key_facts = []
    warnings = []
    suggested_queries = []
    llm_features_used = []
    
    if include_llm_features and llm_service and semantic:
        # Generate summary
        ai_summary, key_facts, warnings = await _generate_llm_summary(
            session, llm_service, semantic, quality
        )
        if ai_summary:
            llm_features_used.append("context_summary")
        
        # Generate suggested queries
        suggested_queries = await _generate_suggested_queries(
            session, llm_service, semantic, ontology, relevant_metrics
        )
        if suggested_queries:
            llm_features_used.append("suggested_queries")
    
    # ========================================================================
    # Assemble Final Document
    # ========================================================================
    duration = (datetime.now() - start_time).total_seconds()
    
    document = ContextDocument(
        source_id=source_id,
        source_name=source.name,
        generated_at=datetime.now(),
        ontology=ontology,
        # Pillar 1: Statistical
        statistical_profiling=statistical_profiling,
        statistical_quality=statistical_quality,
        correlation_analysis=correlation_analysis,
        # Pillar 2: Topological
        topology=topology,
        topological_summary=topological_summary,
        # Pillar 3: Semantic
        semantic=semantic,
        # Pillar 4: Temporal
        temporal_summary=temporal_summary,
        # Pillar 5: Quality
        quality=quality,
        # Ontology content
        relevant_metrics=relevant_metrics,
        domain_concepts=domain_concepts,
        # LLM content
        suggested_queries=suggested_queries,
        ai_summary=ai_summary,
        key_facts=key_facts,
        warnings=warnings,
        llm_features_used=llm_features_used,
        assembly_duration_seconds=duration,
    )
    
    return Result.ok(document)
```

**Deliverables:**
- [ ] Main assembly function completed
- [ ] All pillars assembled
- [ ] LLM features integrated
- [ ] Integration test passes

---

## Testing Strategy

### Unit Tests

**Create `tests/context/test_converters.py`:**

```python
@pytest.mark.asyncio
async def test_convert_semantic_annotation():
    """Test semantic annotation converter."""
    # Create mock DB annotation
    db_annotation = SemanticAnnotationDB(...)
    
    # Convert
    pydantic_annotation = convert_semantic_annotation(db_annotation)
    
    # Assert
    assert pydantic_annotation.semantic_role == SemanticRole.MEASURE
    assert pydantic_annotation.confidence == 0.9

@pytest.mark.asyncio
async def test_convert_temporal_quality_metrics():
    """Test temporal quality metrics converter."""
    # Create mock DB metrics
    db_metrics = TemporalQualityMetricsDB(...)
    
    # Convert
    pydantic_result = convert_temporal_quality_metrics(db_metrics)
    
    # Assert
    assert pydantic_result.completeness_ratio == 0.95
    assert len(pydantic_result.change_points) == 2
```

### Integration Tests

**Create `tests/context/test_assembly_integration.py`:**

```python
@pytest.mark.asyncio
async def test_assemble_complete_context_document(
    db_session,
    sample_source_id,
    llm_service_mock,
):
    """Test complete context document assembly."""
    # Assemble
    result = await assemble_context_document(
        source_id=sample_source_id,
        ontology="financial_reporting",
        session=db_session,
        llm_service=llm_service_mock,
        include_llm_features=True,
    )
    
    # Assert
    assert result.success
    document = result.value
    
    # Verify all pillars present
    assert document.statistical_profiling is not None
    assert document.semantic is not None
    assert document.quality is not None
    
    # Verify LLM features
    assert document.ai_summary is not None
    assert len(document.suggested_queries) > 0
    assert "context_summary" in document.llm_features_used

@pytest.mark.asyncio
async def test_assemble_without_llm_features(
    db_session,
    sample_source_id,
):
    """Test context assembly without LLM features."""
    # Assemble without LLM
    result = await assemble_context_document(
        source_id=sample_source_id,
        ontology="general",
        session=db_session,
        llm_service=None,
        include_llm_features=False,
    )
    
    # Assert
    assert result.success
    document = result.value
    
    # Verify no LLM features
    assert document.ai_summary is None
    assert len(document.suggested_queries) == 0
    assert len(document.llm_features_used) == 0
```

### End-to-End Tests

**Create `tests/e2e/test_context_generation.py`:**

```python
@pytest.mark.asyncio
async def test_e2e_context_generation_finance_example(
    finance_example_source,
    db_session,
    llm_service,
):
    """Test end-to-end context generation with finance example data."""
    # Assemble context
    result = await assemble_context_document(
        source_id=finance_example_source.source_id,
        ontology="financial_reporting",
        session=db_session,
        llm_service=llm_service,
        include_llm_features=True,
    )
    
    assert result.success
    document = result.value
    
    # Verify financial-specific content
    assert document.ontology == "financial_reporting"
    assert len(document.relevant_metrics) > 0
    
    # Verify quality assessment
    assert document.quality is not None
    assert document.quality.table_assessment.overall_score > 0
    
    # Verify LLM summary mentions financial concepts
    assert document.ai_summary is not None
    assert any(term in document.ai_summary.lower() 
               for term in ["financial", "accounting", "ledger"])
```

---

## Deliverables Checklist

### Code Implementation
- [ ] Complete converters (semantic, temporal, correlation)
- [ ] Implement correlation assembly
- [ ] Implement semantic assembly
- [ ] Implement temporal summary assembly
- [ ] Implement quality assembly
- [ ] Integrate LLM summary generation
- [ ] Integrate suggested queries generation
- [ ] Integrate quality rule generation
- [ ] Load ontology metrics and concepts
- [ ] Update main assembly function
- [ ] Add error handling and fallbacks

### Testing
- [ ] Unit tests for converters (8+ tests)
- [ ] Unit tests for assembly functions (6+ tests)
- [ ] Integration test for complete assembly
- [ ] Integration test without LLM features
- [ ] E2E test with finance example
- [ ] All tests passing

### Documentation
- [ ] Update `docs/INTERFACES.md` - Context assembly API
- [ ] Update `docs/ARCHITECTURE.md` - 5-pillar assembly flow
- [ ] Add docstrings to all new functions
- [ ] Create usage examples in README

### Configuration
- [ ] Verify ontology YAML structure
- [ ] Verify prompt templates work
- [ ] Test with different ontologies

---

## Success Criteria

Phase 7 is complete when:

1. ✅ All converters implemented and tested
2. ✅ All assembly functions implemented
3. ✅ Complete `ContextDocument` assembled with all 5 pillars
4. ✅ LLM features integrated (summary, queries, rules)
5. ✅ Ontology content loaded (metrics, concepts)
6. ✅ Integration test passes with real data
7. ✅ E2E test with finance example passes
8. ✅ All unit tests pass
9. ✅ Documentation updated
10. ✅ No TODOs remaining in assembly.py

---

## Implementation Order

### Week 1: Core Assembly
1. **Task 1** - Complete converters (2 days)
   - Semantic converters
   - Temporal converters
   - Correlation converters
   - Unit tests

2. **Task 2** - Assembly functions (2 days)
   - Correlation assembly
   - Semantic assembly
   - Temporal summary assembly
   - Quality assembly
   - Unit tests

3. **Task 5** - Update main function (1 day)
   - Complete `assemble_context_document()`
   - Integration test

### Week 2: LLM Integration & Testing
4. **Task 3** - LLM integration (2 days)
   - Summary generation
   - Query generation
   - Quality rules
   - Tests

5. **Task 4** - Ontology loading (1 day)
   - Metrics loading
   - Concepts loading
   - Tests

6. **Final Testing** (2 days)
   - E2E tests
   - Performance testing
   - Documentation
   - Bug fixes

---

## Estimated Effort

**Total:** 10 days (~2 weeks)

| Task | Effort | Priority |
|------|--------|----------|
| Task 1: Converters | 2 days | HIGH |
| Task 2: Assembly functions | 2 days | HIGH |
| Task 3: LLM integration | 2 days | MEDIUM |
| Task 4: Ontology loading | 1 day | MEDIUM |
| Task 5: Main function update | 1 day | HIGH |
| Testing & documentation | 2 days | HIGH |

---

## Risk Mitigation

### Risk: Missing Data in Database

**Mitigation:**
- All assembly functions return `None` if no data found
- ContextDocument accepts `None` for all pillars
- Graceful degradation if pillars missing

### Risk: LLM Service Unavailable

**Mitigation:**
- `llm_service` is optional parameter
- `include_llm_features` flag allows disabling
- Assembly works without LLM features

### Risk: Ontology Not Found

**Mitigation:**
- Return empty lists for metrics/concepts
- Use "general" ontology as fallback
- Don't fail assembly if ontology missing

### Risk: Performance Issues

**Mitigation:**
- Use eager loading (`selectinload`) for relationships
- Limit queries to typed tables only
- Consider caching assembled documents

---

## Dependencies on Other Phases

### Required (Must Be Complete)
- ✅ Phase 1: Statistical quality models exist
- ✅ Phase 2A: LLM infrastructure exists
- ✅ Quality synthesis: Works and tested

### Nice to Have (Can Be Added Later)
- ⚠️ Phase 2B: Correlation analysis (placeholder for now)
- ⚠️ Phase 3: Full topology enrichment (basic exists)
- ⚠️ Phase 4: Complete temporal analysis (basic exists)

### Can Proceed Without
- ❌ Phase 5: Domain quality rules (can add later)
- ❌ Phase 6: MCP tools (uses assembled context)

---

## Future Enhancements (Post-Phase 7)

### Caching Strategy
- Cache assembled ContextDocument per source
- Invalidate on data changes
- TTL-based expiry

### Incremental Assembly
- Only re-assemble changed pillars
- Merge with cached document
- Faster updates

### Parallel Assembly
- Assemble pillars in parallel (asyncio.gather)
- Faster overall assembly
- Better resource utilization

### Assembly Hooks
- Pre-assembly hooks (validation)
- Post-assembly hooks (notifications)
- Custom assembly logic per ontology

---

## Questions for Review

1. **Converters**: Should we add validation in converters or trust DB data?
2. **LLM Integration**: Should LLM features be required or always optional?
3. **Ontology Loading**: Should we validate ontology structure on load?
4. **Error Handling**: Fail fast or continue with warnings?
5. **Performance**: Should we add query optimization now or later?

---

## Ready to Start?

This plan provides:
- ✅ Clear task breakdown with dependencies
- ✅ Detailed implementation guidance
- ✅ Comprehensive testing strategy
- ✅ Success criteria
- ✅ Risk mitigation
- ✅ Realistic timeline

**Next Step:** Review this plan and start with Task 1 (Converters)!
