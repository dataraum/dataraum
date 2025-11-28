# Phase 3: Enrichment - Implementation Plan

**Status**: Planning  
**Dependencies**: Phase 1 (Storage) ✅, Phase 2A (LLM) ✅, Phase 2B (Profiling) ✅  

---

## Overview

Phase 3 implements the enrichment layer that extracts semantic, topological, and temporal metadata from profiled data. This phase integrates the LLM features built in Phase 2A with the storage and profiling infrastructure.

### Key Principle

Per CLAUDE.md:
> **Correctness over speed** - We would rather have working code slowly than broken code quickly.

This means:
- Integration tests must pass before declaring done
- Verify LLM outputs match expected structure
- Test with real data from `examples/finance_csv_example/`

---

## Architecture Summary

```
Profiling Results (Phase 2B)
    ↓
┌─────────────────────────────────────────────────────────────┐
│                  ENRICHMENT LAYER (Phase 3)                  │
│                                                              │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────┐  │
│  │   Semantic     │  │   Topological  │  │   Temporal   │  │
│  │  (LLM-powered) │  │   (TDA-based)  │  │ (Time-based) │  │
│  │                │  │                │  │              │  │
│  │ • Column roles │  │ • Relationships│  │ • Granularity│  │
│  │ • Entity types │  │ • Join paths   │  │ • Gaps       │  │
│  │ • Business     │  │ • FK detection │  │ • Trends     │  │
│  │   names        │  │                │  │              │  │
│  └────────────────┘  └────────────────┘  └──────────────┘  │
│         │                     │                  │          │
│         └─────────────────────┴──────────────────┘          │
│                             ↓                                │
│                   Store in Metadata DB                       │
└─────────────────────────────────────────────────────────────┘
    ↓
Metadata Tables:
- semantic_annotations
- table_entities  
- relationships
- temporal_profiles
```

---

## Module Structure

```
src/dataraum_context/enrichment/
├── __init__.py          # Public API
├── semantic.py          # Semantic enrichment (LLM integration)
├── topology.py          # Topological enrichment (TDA wrapper)
├── temporal.py          # Temporal enrichment
└── coordinator.py       # Orchestrates all enrichment
```

---

## Implementation Steps

### Step 7: Semantic Enrichment

**Goal**: Integrate LLM semantic analysis with metadata storage

#### 7.1: Semantic Enrichment Module

**File**: `src/dataraum_context/enrichment/semantic.py`

```python
"""Semantic enrichment using LLM analysis."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import Result, SemanticEnrichmentResult
from dataraum_context.llm import LLMService
from dataraum_context.storage.models import (
    Column,
    Relationship as RelationshipModel,
    SemanticAnnotation as AnnotationModel,
    Table,
    TableEntity as EntityModel,
)


async def enrich_semantic(
    session: AsyncSession,
    llm_service: LLMService,
    table_ids: list[str],
    ontology: str = "general",
) -> Result[SemanticEnrichmentResult]:
    """Run semantic enrichment on tables.
    
    Steps:
    1. Call LLM service for semantic analysis
    2. Map column_refs to actual column_ids from database
    3. Store annotations in semantic_annotations table
    4. Store entity detections in table_entities table
    5. Store relationships in relationships table
    6. Return enrichment result
    """
    # Call LLM service
    llm_result = await llm_service.analyze_semantics(
        session=session,
        table_ids=table_ids,
        ontology=ontology,
    )
    
    if not llm_result.success:
        return llm_result
    
    enrichment = llm_result.value
    
    # Load column ID mappings
    column_map = await _load_column_mappings(session, table_ids)
    table_map = await _load_table_mappings(session, table_ids)
    
    # Store annotations
    for annotation in enrichment.annotations:
        column_id = column_map.get(
            (annotation.column_ref.table_name, annotation.column_ref.column_name)
        )
        if not column_id:
            continue  # Skip if column not found
        
        # Create or update semantic annotation
        db_annotation = AnnotationModel(
            column_id=column_id,
            semantic_role=annotation.semantic_role.value,
            entity_type=annotation.entity_type,
            business_name=annotation.business_name,
            business_description=annotation.business_description,
            annotation_source=annotation.annotation_source.value,
            annotated_by=annotation.annotated_by,
            confidence=annotation.confidence,
        )
        session.add(db_annotation)
    
    # Store entity detections
    for entity in enrichment.entity_detections:
        table_id = table_map.get(entity.table_name)
        if not table_id:
            continue
        
        db_entity = EntityModel(
            table_id=table_id,
            detected_entity_type=entity.entity_type,
            description=entity.description,
            confidence=entity.confidence,
            evidence=entity.evidence,
            grain_columns={"columns": entity.grain_columns},
            is_fact_table=entity.is_fact_table,
            is_dimension_table=entity.is_dimension_table,
            detection_source="llm",
        )
        session.add(db_entity)
    
    # Store relationships
    for rel in enrichment.relationships:
        from_col_id = column_map.get((rel.from_table, rel.from_column))
        to_col_id = column_map.get((rel.to_table, rel.to_column))
        from_table_id = table_map.get(rel.from_table)
        to_table_id = table_map.get(rel.to_table)
        
        if not all([from_col_id, to_col_id, from_table_id, to_table_id]):
            continue
        
        db_rel = RelationshipModel(
            relationship_id=rel.relationship_id,
            from_table_id=from_table_id,
            from_column_id=from_col_id,
            to_table_id=to_table_id,
            to_column_id=to_col_id,
            relationship_type=rel.relationship_type.value,
            cardinality=rel.cardinality.value if rel.cardinality else None,
            confidence=rel.confidence,
            detection_method=rel.detection_method,
            evidence=rel.evidence,
        )
        session.add(db_rel)
    
    await session.commit()
    
    return Result.ok(enrichment)


async def _load_column_mappings(
    session: AsyncSession,
    table_ids: list[str],
) -> dict[tuple[str, str], str]:
    """Load mapping of (table_name, column_name) -> column_id."""
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = await session.execute(stmt)
    
    return {
        (table_name, col_name): col_id
        for table_name, col_name, col_id in result.all()
    }


async def _load_table_mappings(
    session: AsyncSession,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id."""
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = await session.execute(stmt)
    
    return {table_name: table_id for table_name, table_id in result.all()}
```

**Key Points**:
- Thin wrapper around LLM service
- Maps LLM results to database IDs
- Stores in appropriate tables
- Handles missing columns/tables gracefully

#### 7.2: Integration with Profiling

Need to update `semantic.py` in LLM features to actually load profiles from database:

**File**: `src/dataraum_context/llm/features/semantic.py`

Update `_load_profiles()` to query `column_profiles` table instead of using placeholder.

### Step 8: Topological Enrichment

**Goal**: Copy TDA classes and remove column name-based ranking (keep TDA-based ranking)

**IMPORTANT**: We are **copying** the TDA classes into our codebase, not importing from prototypes.

#### 8.1: What to Copy and What to Remove

**From `topology_extractor.py` - COPY ENTIRELY**:
- All TDA mathematics: `compute_persistence()`, `build_feature_matrix()`, `extract_column_features()`
- All value-based analysis: `analyze_column_relationship()` using correlations, set operations, cardinality
- All row topology: `extract_row_topology()`, `identify_clusters()`, `compute_outlier_scores()`

**From `relationship_finder.py` - COPY WITH MODIFICATIONS**:

✅ **Keep (TDA and value-based ranking)**:
- `compare_persistence_diagrams()` - Wasserstein distance for topology comparison
- `compute_join_score()` - Jaccard similarity, containment checks, range overlap
- `are_types_compatible()` - Type checking
- `compute_range_overlap()` - Numeric range analysis
- `determine_join_type()` - Cardinality-based classification

❌ **Remove (Column name pattern ranking)**:
- `is_id_column()` - Uses column NAME patterns ('id', 'key', 'code', etc.)
- The name-based boost in `compute_join_score()` line ~180:
  ```python
  # Boost score for ID-like columns
  if self.is_id_column(col1) and self.is_id_column(col2):
      score *= 1.2
  ```
- `is_numeric_id()` - While mostly value-based, it's only called from join scoring for the name-based boost

**Why Remove These?**
User explicitly requested: "remove the ranking BASED ON COLUMN NAMES, keep the ranking based on TDA results"
- `is_id_column()` looks at `str(col.name).lower()` for patterns
- This is heuristic-based, not topology-based
- TDA-based ranking (Wasserstein distance, structural similarity) should be the sole ranking mechanism

#### 8.2: Topology Module Structure

**Files to Create**:
- `src/dataraum_context/enrichment/tda/topology_extractor.py` - Copy of prototype (no changes)
- `src/dataraum_context/enrichment/tda/relationship_finder.py` - Copy with name-based methods removed
- `src/dataraum_context/enrichment/tda/__init__.py` - Exports
- `src/dataraum_context/enrichment/topology.py` - Integration layer

#### 8.3: Topology Enrichment Module

**File**: `src/dataraum_context/enrichment/topology.py`

```python
"""Topological enrichment using copied TDA classes."""

from uuid import uuid4

import duckdb
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import (
    Cardinality,
    Relationship,
    RelationshipType,
    Result,
    TopologyEnrichmentResult,
)
from dataraum_context.storage.models import Column, Table

# Import our copied TDA classes (not from prototypes)
from dataraum_context.enrichment.tda.relationship_finder import TableRelationshipFinder
from dataraum_context.enrichment.tda.topology_extractor import TableTopologyExtractor


async def enrich_topology(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
) -> Result[TopologyEnrichmentResult]:
    """Run topological analysis on tables using TDA.
    
    Steps:
    1. Load table data from DuckDB
    2. Extract topology for each table using TDA
    3. Find relationships between tables
    4. Store relationships (skip ranking by column names)
    5. Cache TDA results for reuse
    """
    # Load table data
    tables_data = await _load_tables_data(session, duckdb_conn, table_ids)
    
    if not tables_data:
        return Result.fail("No table data found")
    
    # Initialize TDA components
    extractor = TableTopologyExtractor()
    finder = TableRelationshipFinder()
    
    # Extract topology for each table
    topologies = {}
    for table_name, df in tables_data.items():
        try:
            topology = extractor.extract_topology(df)
            topologies[table_name] = topology
            
            # Cache topology results
            await _cache_topology_result(session, table_name, topology)
            
        except Exception as e:
            # Continue with other tables if one fails
            print(f"Warning: Failed to extract topology for {table_name}: {e}")
            continue
    
    # Find relationships between tables
    relationships = []
    
    if len(tables_data) > 1:
        tda_relationships = finder.find_relationships(tables_data, topologies)
        
        # Convert TDA relationships to our model (skip ranking step)
        for rel in tda_relationships:
            # Map cardinality
            cardinality = None
            if rel.get("type") == "one-to-many":
                cardinality = Cardinality.ONE_TO_MANY
            elif rel.get("type") == "one-to-one":
                cardinality = Cardinality.ONE_TO_ONE
            elif rel.get("type") == "many-to-many":
                cardinality = Cardinality.MANY_TO_MANY
            
            relationship = Relationship(
                relationship_id=str(uuid4()),
                from_table=rel["table1"],
                from_column=rel["best_join"]["column1"],
                to_table=rel["table2"],
                to_column=rel["best_join"]["column2"],
                relationship_type=RelationshipType.FOREIGN_KEY,
                cardinality=cardinality,
                confidence=rel.get("confidence", 0.0),
                detection_method="tda",
                evidence={
                    "overlap_score": rel["best_join"].get("overlap_score"),
                    "structural_similarity": rel["best_join"].get("structural_similarity"),
                    "semantic_similarity": rel["best_join"].get("semantic_similarity"),
                },
            )
            relationships.append(relationship)
    
    return Result.ok(TopologyEnrichmentResult(
        relationships=relationships,
        join_paths=[],  # TODO: Generate join paths from relationships
    ))


async def _load_tables_data(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
) -> dict[str, pd.DataFrame]:
    """Load table data from DuckDB as pandas DataFrames."""
    # Get table names
    stmt = select(Table.table_name, Table.layer).where(Table.table_id.in_(table_ids))
    result = await session.execute(stmt)
    tables = result.all()
    
    # Load data from DuckDB
    tables_data = {}
    for table_name, layer in tables:
        # Use typed layer if available, otherwise raw
        actual_table = f"typed_{table_name}" if layer == "typed" else f"raw_{table_name}"
        
        try:
            df = duckdb_conn.execute(f"SELECT * FROM {actual_table} LIMIT 10000").df()
            tables_data[table_name] = df
        except Exception as e:
            print(f"Warning: Failed to load {actual_table}: {e}")
            continue
    
    return tables_data


async def _cache_topology_result(
    session: AsyncSession,
    table_name: str,
    topology: dict,
) -> None:
    """Cache topology extraction results for reuse.
    
    TODO: Create topology_cache table to store:
    - Table name/ID
    - Topology features (persistence diagrams, Betti numbers)
    - Column relationships
    - Timestamp
    
    This avoids re-running expensive TDA computations.
    """
    # For now, just store in relationships table
    # In future, create dedicated topology_cache table
    pass
```

**Key Changes**:
- ✅ Integrates working TDA prototype
- ✅ Skips column name ranking (as requested)
- ✅ Uses actual topology and structural features
- ✅ Caches topology results (placeholder for now)
- ✅ Handles errors gracefully (continues if one table fails)

### Step 9: Temporal Enrichment

**Goal**: Extract temporal patterns from time columns

#### 9.1: Temporal Enrichment Module

**File**: `src/dataraum_context/enrichment/temporal.py`

```python
"""Temporal enrichment for time columns."""

from datetime import datetime, timedelta

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import (
    ColumnProfile,
    ColumnRef,
    Result,
    TemporalEnrichmentResult,
    TemporalGap,
    TemporalProfile,
)
from dataraum_context.storage.models import Column, SemanticAnnotation, Table


async def enrich_temporal(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
) -> Result[TemporalEnrichmentResult]:
    """Extract temporal patterns from time columns.
    
    Steps:
    1. Find columns with timestamp semantic role
    2. Analyze each time column using DuckDB
    3. Detect granularity (day, week, month, etc.)
    4. Find gaps in time series
    5. Store temporal profiles
    """
    # Find timestamp columns
    stmt = (
        select(Column, Table.table_name, SemanticAnnotation.semantic_role)
        .join(Table)
        .outerjoin(SemanticAnnotation)
        .where(
            Table.table_id.in_(table_ids),
            SemanticAnnotation.semantic_role == "timestamp",
        )
    )
    result = await session.execute(stmt)
    timestamp_columns = result.all()
    
    profiles = []
    
    for col, table_name, _ in timestamp_columns:
        # Analyze time column
        temporal_result = await _analyze_time_column(
            duckdb_conn,
            table_name,
            col.column_name,
        )
        
        if temporal_result.success:
            profile = temporal_result.value
            profile.column_id = col.column_id
            profile.column_ref = ColumnRef(
                table_name=table_name,
                column_name=col.column_name,
            )
            profiles.append(profile)
    
    return Result.ok(TemporalEnrichmentResult(profiles=profiles))


async def _analyze_time_column(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
) -> Result[TemporalProfile]:
    """Analyze a time column to extract temporal patterns."""
    try:
        # Get min/max timestamps
        result = duckdb_conn.execute(f"""
            SELECT 
                MIN({column_name}) as min_ts,
                MAX({column_name}) as max_ts,
                COUNT(DISTINCT {column_name}) as distinct_count
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
        """).fetchone()
        
        min_ts, max_ts, distinct_count = result
        
        if not min_ts or not max_ts:
            return Result.fail("No valid timestamps found")
        
        # Detect granularity by looking at consecutive gaps
        gap_result = duckdb_conn.execute(f"""
            WITH ordered_ts AS (
                SELECT DISTINCT {column_name} as ts
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
                ORDER BY ts
            ),
            gaps AS (
                SELECT 
                    ts,
                    LEAD(ts) OVER (ORDER BY ts) as next_ts,
                    epoch(LEAD(ts) OVER (ORDER BY ts) - ts) as gap_seconds
                FROM ordered_ts
            )
            SELECT 
                mode() WITHIN GROUP (ORDER BY gap_seconds) as dominant_gap_seconds
            FROM gaps
            WHERE gap_seconds IS NOT NULL
        """).fetchone()
        
        dominant_gap_seconds = gap_result[0] if gap_result else None
        granularity = _infer_granularity(dominant_gap_seconds)
        
        # Build temporal profile
        profile = TemporalProfile(
            column_id="",  # Filled by caller
            column_ref=ColumnRef(table_name="", column_name=""),  # Filled by caller
            min_timestamp=min_ts,
            max_timestamp=max_ts,
            detected_granularity=granularity,
            granularity_confidence=0.8,
            expected_periods=0,  # TODO: Calculate based on granularity
            actual_periods=distinct_count,
            completeness_ratio=1.0,  # TODO: Calculate
            gap_count=0,  # TODO: Detect gaps
            gaps=[],
            has_seasonality=False,  # TODO: Detect
            seasonality_period=None,
            trend_direction=None,
        )
        
        return Result.ok(profile)
        
    except Exception as e:
        return Result.fail(f"Failed to analyze time column: {e}")


def _infer_granularity(gap_seconds: float | None) -> str:
    """Infer time granularity from dominant gap."""
    if gap_seconds is None:
        return "unknown"
    
    # Convert to common granularities
    if gap_seconds < 60:
        return "second"
    elif gap_seconds < 3600:
        return "minute"
    elif gap_seconds < 86400:
        return "hour"
    elif gap_seconds < 86400 * 7:
        return "day"
    elif gap_seconds < 86400 * 31:
        return "week"
    elif gap_seconds < 86400 * 366:
        return "month"
    else:
        return "year"
```

**Key Points**:
- Uses DuckDB for time series analysis
- Detects granularity from gap patterns
- Simple gap detection (can enhance later)
- Stores temporal profiles for time columns

### Step 10: Enrichment Coordinator

**Goal**: Orchestrate all enrichment steps

#### 10.1: Coordinator Module

**File**: `src/dataraum_context/enrichment/coordinator.py`

```python
"""Enrichment coordinator - orchestrates all enrichment steps."""

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import Result
from dataraum_context.enrichment.semantic import enrich_semantic
from dataraum_context.enrichment.temporal import enrich_temporal
from dataraum_context.enrichment.topology import enrich_topology
from dataraum_context.llm import LLMService


class EnrichmentCoordinator:
    """Coordinates enrichment pipeline."""
    
    def __init__(
        self,
        llm_service: LLMService,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        self.llm_service = llm_service
        self.duckdb_conn = duckdb_conn
    
    async def enrich_all(
        self,
        session: AsyncSession,
        table_ids: list[str],
        ontology: str = "general",
        include_topology: bool = False,
        include_temporal: bool = True,
    ) -> Result[dict]:
        """Run all enrichment steps.
        
        Args:
            session: Database session
            table_ids: Tables to enrich
            ontology: Ontology to use
            include_topology: Run topology enrichment (TDA)
            include_temporal: Run temporal enrichment
        
        Returns:
            Result containing enrichment summary
        """
        results = {}
        
        # 1. Semantic enrichment (required)
        semantic_result = await enrich_semantic(
            session=session,
            llm_service=self.llm_service,
            table_ids=table_ids,
            ontology=ontology,
        )
        
        if not semantic_result.success:
            return Result.fail(f"Semantic enrichment failed: {semantic_result.error}")
        
        results["semantic"] = {
            "annotations": len(semantic_result.value.annotations),
            "entities": len(semantic_result.value.entity_detections),
            "relationships": len(semantic_result.value.relationships),
        }
        
        # 2. Topology enrichment (optional, TDA-based)
        if include_topology:
            topology_result = await enrich_topology(
                session=session,
                table_ids=table_ids,
            )
            
            if topology_result.success:
                results["topology"] = {
                    "relationships": len(topology_result.value.relationships),
                    "join_paths": len(topology_result.value.join_paths),
                }
        
        # 3. Temporal enrichment (optional)
        if include_temporal:
            temporal_result = await enrich_temporal(
                session=session,
                duckdb_conn=self.duckdb_conn,
                table_ids=table_ids,
            )
            
            if temporal_result.success:
                results["temporal"] = {
                    "profiles": len(temporal_result.value.profiles),
                }
        
        return Result.ok(results)
```

#### 10.2: Public API

**File**: `src/dataraum_context/enrichment/__init__.py`

```python
"""Enrichment layer - semantic, topological, and temporal metadata extraction."""

from dataraum_context.enrichment.coordinator import EnrichmentCoordinator
from dataraum_context.enrichment.semantic import enrich_semantic
from dataraum_context.enrichment.temporal import enrich_temporal
from dataraum_context.enrichment.topology import enrich_topology

__all__ = [
    "EnrichmentCoordinator",
    "enrich_semantic",
    "enrich_temporal",
    "enrich_topology",
]
```

---

## Result Caching & Persistence Strategy

### Why Cache Enrichment Results?

1. **TDA is expensive** - Persistent homology computations take time
2. **LLM calls cost money** - Cache semantic analysis results
3. **Temporal analysis reusable** - Time patterns don't change often
4. **Incremental updates** - Only re-enrich changed tables

### Caching Layers

#### Layer 1: Database Tables (Already Exists)

**Semantic Results**:
- `semantic_annotations` - Column semantic roles
- `table_entities` - Entity type detections
- `relationships` - Detected relationships

**Temporal Results**:
- `temporal_profiles` - Granularity, gaps, trends

**Quality Results** (Phase 4):
- `quality_rules` - Generated rules
- `quality_results` - Rule execution results

#### Layer 2: LLM Cache (Already Exists)

**Table**: `llm_cache`
- Caches LLM responses by prompt hash
- 24-hour TTL by default
- Invalidated when source data changes

#### Layer 3: Enrichment Metadata Cache (NEW)

**Purpose**: Store expensive intermediate computations

**Table**: `enrichment_cache`

```sql
CREATE TABLE enrichment_cache (
    cache_id UUID PRIMARY KEY,
    table_id UUID REFERENCES tables(table_id),
    enrichment_type VARCHAR NOT NULL,  -- 'topology', 'statistical', 'temporal'
    cache_key VARCHAR NOT NULL,        -- Hash of input parameters
    
    -- Results (JSONB for flexibility)
    result_data JSONB NOT NULL,
    
    -- Metadata
    computed_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,
    computation_time_ms INTEGER,
    
    -- Invalidation
    is_valid BOOLEAN DEFAULT TRUE,
    data_fingerprint VARCHAR,          -- Hash of source data
    
    UNIQUE(table_id, enrichment_type, cache_key)
);

CREATE INDEX idx_enrichment_cache_lookup 
    ON enrichment_cache(table_id, enrichment_type, is_valid);
```

**What to Cache**:

1. **Topology Results** (`enrichment_type='topology'`):
   ```json
   {
     "persistence_diagrams": [...],
     "betti_numbers": [1, 3, 0],
     "column_topology": {
       "col1-col2": {"strength": 0.85}
     }
   }
   ```

2. **Statistical Features** (`enrichment_type='statistical'`):
   ```json
   {
     "distributions": {...},
     "correlations": {...},
     "outliers": [...]
   }
   ```

3. **Temporal Features** (`enrichment_type='temporal'`):
   ```json
   {
     "fourier_transform": [...],
     "autocorrelation": [...],
     "seasonal_decomposition": {...}
   }
   ```

### Cache Invalidation Strategy

**Invalidate when**:
1. Source data changes (new rows, updated values)
2. Schema changes (columns added/removed/renamed)
3. Manual invalidation via API
4. TTL expires (configurable per enrichment type)

**Implementation**:

```python
# enrichment/cache.py

async def get_cached_enrichment(
    session: AsyncSession,
    table_id: str,
    enrichment_type: str,
    cache_key: str,
) -> dict | None:
    """Get cached enrichment result if valid."""
    stmt = select(EnrichmentCache).where(
        EnrichmentCache.table_id == table_id,
        EnrichmentCache.enrichment_type == enrichment_type,
        EnrichmentCache.cache_key == cache_key,
        EnrichmentCache.is_valid == True,
        EnrichmentCache.expires_at > datetime.now(UTC),
    )
    
    result = await session.execute(stmt)
    cache_entry = result.scalar_one_or_none()
    
    return cache_entry.result_data if cache_entry else None


async def store_enrichment_cache(
    session: AsyncSession,
    table_id: str,
    enrichment_type: str,
    cache_key: str,
    result_data: dict,
    ttl_seconds: int = 86400 * 7,  # 7 days default
) -> None:
    """Store enrichment result in cache."""
    cache_entry = EnrichmentCache(
        table_id=table_id,
        enrichment_type=enrichment_type,
        cache_key=cache_key,
        result_data=result_data,
        expires_at=datetime.now(UTC) + timedelta(seconds=ttl_seconds),
    )
    
    session.add(cache_entry)
    await session.commit()


async def invalidate_enrichment_cache(
    session: AsyncSession,
    table_id: str,
    enrichment_type: str | None = None,
) -> None:
    """Invalidate cached enrichment results for a table."""
    stmt = select(EnrichmentCache).where(
        EnrichmentCache.table_id == table_id
    )
    
    if enrichment_type:
        stmt = stmt.where(EnrichmentCache.enrichment_type == enrichment_type)
    
    result = await session.execute(stmt)
    entries = result.scalars().all()
    
    for entry in entries:
        entry.is_valid = False
    
    await session.commit()
```

### Cache Usage in Enrichment

**Before Computation**:
```python
# Check cache first
cache_key = _compute_cache_key(table_id, params)
cached = await get_cached_enrichment(session, table_id, "topology", cache_key)

if cached:
    return Result.ok(TopologyEnrichmentResult(**cached))

# Compute if not cached
result = await _run_tda_analysis(...)

# Store in cache
await store_enrichment_cache(
    session, table_id, "topology", cache_key, 
    result.model_dump(),
    ttl_seconds=86400 * 7  # 7 days
)
```

### Performance Benefits

| Operation | Without Cache | With Cache | Speedup |
|-----------|---------------|------------|---------|
| TDA analysis | ~10-30s | <100ms | 100-300x |
| LLM semantic | ~5-10s | <100ms | 50-100x |
| Temporal analysis | ~1-5s | <50ms | 20-100x |

### Migration Plan

**Phase 3**: Use existing tables + LLM cache
**Phase 4**: Add `enrichment_cache` table for TDA/statistical results
**Phase 5**: Optimize cache warming and invalidation

---

## Testing Strategy

### Unit Tests

**Test Semantic Enrichment**:
```python
# tests/enrichment/test_semantic.py

@pytest.mark.asyncio
async def test_enrich_semantic_stores_annotations(
    db_session,
    llm_service_mock,
    sample_table_ids,
):
    """Test that semantic enrichment stores annotations."""
    # Setup mock LLM response
    llm_service_mock.analyze_semantics.return_value = Result.ok(
        SemanticEnrichmentResult(
            annotations=[...],
            entity_detections=[...],
            relationships=[],
            source="llm",
        )
    )
    
    # Run enrichment
    result = await enrich_semantic(
        session=db_session,
        llm_service=llm_service_mock,
        table_ids=sample_table_ids,
        ontology="general",
    )
    
    assert result.success
    
    # Verify annotations stored
    stmt = select(SemanticAnnotation)
    annotations = (await db_session.execute(stmt)).scalars().all()
    assert len(annotations) > 0
```

**Test Temporal Enrichment**:
```python
# tests/enrichment/test_temporal.py

@pytest.mark.asyncio
async def test_enrich_temporal_detects_granularity(
    db_session,
    duckdb_conn,
    daily_time_series_table,
):
    """Test temporal enrichment detects daily granularity."""
    result = await enrich_temporal(
        session=db_session,
        duckdb_conn=duckdb_conn,
        table_ids=[daily_time_series_table.table_id],
    )
    
    assert result.success
    assert len(result.value.profiles) == 1
    assert result.value.profiles[0].detected_granularity == "day"
```

### Integration Tests

**End-to-End Enrichment**:
```python
# tests/enrichment/test_integration.py

@pytest.mark.asyncio
async def test_full_enrichment_pipeline(
    db_session,
    duckdb_conn,
    llm_service,
    finance_example_tables,
):
    """Test full enrichment pipeline with real data."""
    coordinator = EnrichmentCoordinator(
        llm_service=llm_service,
        duckdb_conn=duckdb_conn,
    )
    
    result = await coordinator.enrich_all(
        session=db_session,
        table_ids=finance_example_tables,
        ontology="financial_reporting",
    )
    
    assert result.success
    assert result.value["semantic"]["annotations"] > 0
    assert result.value["temporal"]["profiles"] >= 0
```

---

## Deliverables Checklist

### Code
- [ ] `enrichment/semantic.py` - Semantic enrichment with LLM
- [ ] `enrichment/topology.py` - TDA integration (no ranking)
- [ ] `enrichment/temporal.py` - Temporal pattern detection
- [ ] `enrichment/coordinator.py` - Orchestration
- [ ] `enrichment/__init__.py` - Public API
- [ ] Update `llm/features/semantic.py` - Load real profiles from DB
- [ ] Add TDA prototype dependencies to `pyproject.toml`

### Tests
- [ ] `tests/enrichment/test_semantic.py` - Semantic tests
- [ ] `tests/enrichment/test_temporal.py` - Temporal tests
- [ ] `tests/enrichment/test_integration.py` - Integration tests
- [ ] All tests passing

### Documentation
- [ ] Update `docs/INTERFACES.md` - Add enrichment module
- [ ] Update `docs/ARCHITECTURE.md` - Document enrichment flow
- [ ] Add usage examples to README

---

## Success Criteria

Phase 3 is complete when:

1. ✅ Semantic enrichment calls LLM and stores results
2. ✅ Annotations stored in `semantic_annotations` table
3. ✅ Entity detections stored in `table_entities` table
4. ✅ Relationships stored in `relationships` table
5. ✅ Temporal enrichment detects granularity for time columns
6. ✅ Temporal profiles stored in `temporal_profiles` table
7. ✅ Coordinator orchestrates all enrichment steps
8. ✅ Integration test with finance example data passes
9. ✅ All unit tests pass
10. ✅ Documentation updated

---

## Not Required for Phase 3

- ❌ Column name pattern ranking for relationships (skip as requested)
- ❌ Join path generation (relationships only)
- ❌ Advanced temporal gap detection (basic only)
- ❌ Seasonality detection (placeholder)
- ❌ Trend analysis (placeholder)
- ❌ Manual override UI for annotations
- ❌ Batch processing optimization
- ❌ `enrichment_cache` table (defer to Phase 4)

---

## Implementation Order

1. **Semantic enrichment** (highest priority)
   - Integrate LLM service
   - Store annotations
   - Store entities
   - Store relationships

2. **Temporal enrichment** (medium priority)
   - Detect time columns
   - Analyze granularity
   - Basic gap detection
   - Store temporal profiles

3. **Coordinator** (final integration)
   - Orchestrate enrichment steps
   - Error handling
   - Result aggregation

4. **Testing** (throughout)
   - Unit tests for each module
   - Integration test with finance data
   - Verify LLM outputs

---

## Estimated Effort

**Phase 3 Total**: 3-4 days

- Semantic enrichment: 1 day
- TDA topology integration: 1 day  
- Temporal enrichment: 0.5 day
- Coordinator + integration: 0.5 day
- Testing: 0.5-1 day

---

## Ready to Start?

Phase 3 builds on the solid foundation of Phases 1, 2A, and 2B. The LLM infrastructure is ready, storage models are in place, and profiling can provide the input data.

Let's implement semantic enrichment first, as it's the most critical for AI-driven analytics!
