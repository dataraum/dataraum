# Relationships

## Reasoning & Summary

`analysis/relationships/` detects joinable column pairs between tables using value overlap (Jaccard/containment similarity). It runs after statistical profiling and produces candidates that the semantic agent confirms or rejects.

The module uses a three-phase approach: pre-compute column stats, filter by type compatibility, then compute Jaccard scores using adaptive algorithms (exact for <10K distinct, sampling for 10K-1M, MinHash for >1M). Candidates are enriched with referential integrity metrics and graph topology analysis.

## Data Model

### SQLAlchemy Model

```
Relationship (relationships)
├── relationship_id: String (PK, auto-generated)
├── from_table_id: FK → tables (NOT NULL)
├── from_column_id: FK → columns (NOT NULL)
├── to_table_id: FK → tables (NOT NULL)
├── to_column_id: FK → columns (NOT NULL)
├── relationship_type: String (candidate, foreign_key, semantic_reference, derived)
├── cardinality: String (one-to-one, one-to-many, many-to-one, many-to-many)
├── confidence: Float
├── detection_method: String (candidate, llm, manual)
├── evidence: JSON (join metrics, RI scores, algorithm used)
├── is_confirmed: Boolean (default False)
├── confirmed_at: DateTime
├── confirmed_by: String
└── detected_at: DateTime
```

UniqueConstraint on `(from_column_id, to_column_id, detection_method)` — allows same column pair with different detection methods.

### Pydantic Models

- **JoinCandidate** — single column pair with overlap score, cardinality, uniqueness ratios, RI metrics
- **RelationshipCandidate** — table pair with list of JoinCandidates, join success rate, duplicate detection
- **RelationshipDetectionResult** — detection run summary with timing

### Graph Topology Models

- **TableRole** — table classification (hub, dimension, bridge, isolated) with connection count
- **SchemaCycle** — circular reference between tables (distinct from business cycles)
- **GraphStructure** — full topology: table roles, pattern classification, cycles, component count

## Metrics

### Join Detection

| Metric | Formula | Purpose |
|--------|---------|---------|
| Jaccard similarity | \|A∩B\| / \|A∪B\| | Value overlap between columns |
| Containment | 1.0 if A⊆B or B⊆A | Full inclusion detection |
| Statistical confidence | 1 - 1/√x (sampled), 1 - √(J(1-J)/k) (MinHash) | Confidence in estimate |
| Left uniqueness | distinct(col) / count(col) | Column cardinality ratio |

### Evaluation (Referential Integrity)

| Metric | Formula | Purpose |
|--------|---------|---------|
| Left RI | matched_fk / total_fk × 100 | % of FK values with matching PK |
| Right RI | referenced_pk / total_pk × 100 | % of PK values that are referenced |
| Orphan count | total_fk - matched_fk | FK values without matching PK |
| Join success rate | Left RI of best join | Overall relationship quality |
| Introduces duplicates | join_count > table1_count | Fan trap detection |

### Graph Topology

| Pattern | Criteria |
|---------|----------|
| star_schema | 1 hub + 2+ leaves, no cycles |
| hub_and_spoke | 1+ hubs + 1+ leaves, no cycles |
| chain | No hubs, relationships == tables - 1 |
| mesh | Interconnected, no clear star |
| cyclic | Has cycles, no hubs |
| mesh_with_cycles | Has cycles + hubs |
| disconnected | Multiple components or no relationships |
| sparse | >50% isolated tables |

## Algorithms

### Adaptive Algorithm Selection

| Cardinality | Algorithm | Complexity | Confidence |
|------------|-----------|------------|------------|
| <10K distinct | Exact Jaccard | O(n) | 1.0 |
| 10K-1M distinct | Sampling (reservoir) | O(m) where m = sample | 1 - 1/√x |
| >1M distinct | MinHash (128 hashes) | O(n) | ~91% at k=128 |

### Type Compatibility Groups

Column pairs are only compared if their resolved types belong to the same group:
- **numeric**: INT*, FLOAT*, DOUBLE, DECIMAL, etc.
- **string**: VARCHAR, TEXT, CHAR
- **temporal**: DATE, TIMESTAMP (cast to TIMESTAMP for comparison)
- **boolean**: BOOL
- **uuid**: UUID

## Configuration

### `config/system/relationships.yaml`

```yaml
min_confidence: 0.5        # Minimum Jaccard/containment score threshold
sample_percent: 10.0       # Percentage of rows to sample for uniqueness calculation
```

## Roadmap / Planned Features

- **Context assembly for semantic agent** — Function to assemble text context combining column statistics, intra-table correlations/dependencies, and join candidates for the semantic analysis LLM. `graphs/context.py` has `RelationshipContext` dataclass but lacks the assembly logic that combines relationship evidence with statistics and correlations for richer semantic annotation.
- **Composite key detection & denormalized query model** — Full design in `docs/archive/composite-keys-design.md`. Summary: add `relationship_group_id` + `key_position` to Relationship model, detect composite keys via uniqueness testing on column combinations, update enriched views for multi-column ON clauses, update downstream consumers (query agent, semantic agent, relationship entropy). Implementation in 4 phases: (1) data model + migration, (2) detection logic in finder.py, (3) views + downstream, (4) flat query model. Also includes denormalized query model (canonical nested + flat exploded representations). Evidence from example data: no single column in any dimension table is unique — composite keys required for grain-safe joins.
- **Name-based hints** — boost confidence for columns with similar names (e.g., `customer_id` in both tables)
- **Cross-source relationships** — detect joins across different source types (Parquet, SQLite, PostgreSQL, APIs). Currently limited to tables within a single DuckDB instance. Multi-source would require a federation layer: DuckDB can attach Parquet files and PostgreSQL databases natively, so the Jaccard/RI queries could work across sources if tables are registered in the same DuckDB catalog. Key challenges: type normalization across source dialects, sampling strategies for remote sources (API pagination, PostgreSQL cursors), and handling schema drift when sources are updated independently.
