# Topology Module

## Reasoning & Summary

The topology module answers: **"What is the structural shape of this data?"**

It uses Topological Data Analysis (TDA) via ripser to compute persistence diagrams from numeric data, then extracts:
- **Betti numbers**: Connected components (b0), cycles (b1), voids (b2)
- **Persistent entropy**: Information complexity from persistence diagrams
- **Cycle detection**: Persistent topological cycles indicating recurring patterns
- **Stability analysis**: Bottleneck distance between diagrams for structural change detection
- **Anomaly detection**: Fragmentation, unexpected topology

This is a **compute-only module** — no DB models, no pipeline phase. It is consumed by other modules (temporal_slicing for temporal topology drift, entropy for structural complexity scoring).

For cross-table schema topology (hub/leaf/bridge), see `relationships/graph_topology.py`.

## Architecture

```
topology/
├── __init__.py       # Public API (4 exports)
├── analyzer.py       # analyze_topological_quality(): single-table analysis
├── models.py         # Pydantic models (8 models, no DB models)
├── extraction.py     # Betti numbers, persistence diagrams, entropy, cycle detection
├── stability.py      # Bottleneck distance computation
└── tda/
    ├── __init__.py
    └── extractor.py  # TableTopologyExtractor: ripser-based TDA
```

**~1,200 LOC** across 7 files.

### Data Flow

```
analyze_topological_quality(table_id, duckdb_conn, session)
  │
  ├── Load table data (up to table_sample_limit rows)
  │
  ├── TableTopologyExtractor.extract_topology(df)
  │     ├── build_feature_matrix()     → numeric feature space
  │     ├── compute_persistence()       → ripser persistence diagrams
  │     ├── extract_column_topology()   → per-column features + relationships
  │     └── extract_row_topology()      → row-level structural features
  │
  ├── extract_betti_numbers()           → BettiNumbers
  ├── process_persistence_diagrams()    → list[PersistenceDiagram]
  ├── compute_persistent_entropy()      → float
  ├── detect_persistent_cycles()        → list[CycleDetection]
  │
  ├── Anomaly detection (fragmentation check)
  └── _generate_topology_description()  → human-readable summary
```

### TableTopologyExtractor (tda/extractor.py)

The core TDA extraction class. Takes a DataFrame, converts it to a feature space, runs ripser for persistence diagrams, and extracts column-level and row-level topological features:

| Method | Purpose |
|--------|---------|
| `build_feature_matrix()` | Converts DataFrame to numeric array (handles numeric, categorical, datetime) |
| `compute_persistence()` | Runs ripser on distance matrix → persistence diagrams |
| `extract_column_topology()` | Per-column entropy, uniqueness, correlations, relationships |
| `extract_row_topology()` | Row-level point cloud features, distance matrix, outlier scoring |

## Data Model

### Pydantic Models (models.py) — No DB Models

| Model | Purpose |
|-------|---------|
| `BettiNumbers` | b0, b1, b2, total_complexity, is_connected, has_cycles |
| `PersistencePoint` | Single point in persistence diagram: birth, death, persistence |
| `PersistenceDiagram` | Per-dimension diagram: points, max_persistence, entropy |
| `CycleDetection` | Persistent cycle: dimension, persistence, involved columns, type |
| `StabilityAnalysis` | Bottleneck distance, stability level, component/cycle changes |
| `TopologicalAnomaly` | Anomaly type, severity, description, evidence |
| `TopologicalQualityResult` | Complete analysis: Betti numbers, diagrams, cycles, anomalies, description |

## Configuration

### `config/system/topology.yaml`

```yaml
anomaly_detection:
  fragmentation_threshold: 3            # betti_0 > this = fragmented
  fragmentation_high_threshold: 5       # betti_0 > this = high severity
  complexity_low: 2                     # total_complexity <= this = low
  complexity_moderate: 5                # total_complexity <= this = moderate
  table_sample_limit: 10000             # max rows loaded for TDA

extraction:
  histogram_bins: 20                    # bins for distribution entropy
  relationship_strength_threshold: 0.1  # min strength for column relationship
  min_valid_samples: 10                 # min non-null pairs for correlation
  categorical_overlap_threshold: 0.1    # min overlap for partial relationship
  row_sample_size: 1000                 # max rows for row-level topology
  min_rows_for_topology: 3              # min rows needed for TDA
  min_features_for_outliers: 10         # min features for outlier scoring
```

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `temporal_slicing/analyzer.py` | `TableTopologyExtractor`, `compute_persistent_entropy`, `compute_bottleneck_distance` |
| `entropy_phase` | `analyze_topological_quality()` (topology complexity for entropy scoring) |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Reduced exports from 11 to 4 | Internal models not used externally |
| Added `compute_persistent_entropy` to exports | Used by temporal_slicing but was missing |
| Created `config/system/topology.yaml` | Extracted 12 Python constants to YAML |
| Added structured logging | Silent operations now observable |

## Roadmap

- **DB persistence**: Topology results are computed but not stored; could add a `TopologicalAnalysis` DB model
- **Column-level topology exposure**: Column relationships detected by TDA could feed into relationship detection
- **Dependency audit**: ripser + persim are heavy dependencies; evaluate if topology is providing enough value
