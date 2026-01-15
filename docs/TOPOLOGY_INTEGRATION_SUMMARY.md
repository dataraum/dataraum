# Topology Integration Summary

## Overview

Integrated topological data analysis (TDA) into the slicing and temporal analysis pipelines. The slicing module now delegates to the existing `topology` module for full TDA capabilities.

## Architecture

```
slicing/          →  orchestrates WHEN to run topology on slices
topology/         →  owns HOW topology is computed (single source of truth)
temporal_slicing/ →  orchestrates WHEN to run temporal topology
```

## What Was Implemented

### 1. Per-Slice Topology Analysis (`run_topology_on_slices`)

**Location:** `src/dataraum_context/analysis/slicing/slice_runner.py`

Delegates to `analyze_topological_quality()` from the topology module for each slice:

- **Betti Numbers:** β₀ (components), β₁ (cycles), β₂ (voids)
- **Persistence Diagrams:** Full TDA persistence analysis
- **Persistent Entropy:** Information-theoretic measure of topological complexity
- **Homological Stability:** Comparison with previous analysis runs
- **Anomaly Detection:** Built-in topological anomaly classification
- **Cross-Slice Drift:** Detects slices with unusual topology vs. average

**Key Change:** Removed duplicated inline Betti computation in favor of using the existing TDA extractor.

### 2. Temporal Topology Analysis (`analyze_temporal_topology`)

**Location:** `src/dataraum_context/analysis/temporal_slicing/analyzer.py`

Tracks how data structure changes over time periods:

- **Period Topologies:** Betti numbers computed for each time period
- **Topology Drift:** Detects significant changes between consecutive periods
- **Trend Analysis:** Identifies increasing/decreasing/stable/volatile complexity
- **Anomaly Detection:** Flags periods with unusual structure

### 3. Phase 8 Script Integration

**Location:** `scripts/run_phase8_slice_analysis.py`

```bash
# Run topology only
uv run python ./scripts/run_phase8_slice_analysis.py --skip-semantic --topology

# Run temporal + topology
uv run python ./scripts/run_phase8_slice_analysis.py --skip-semantic --temporal --time-column "Belegdatum der Buchung" --topology
```

## Results from Test Run

```
4. Running topology analysis on slice tables...
   Slices analyzed: 11
   Slices with anomalies: 11

   Average topology across slices:
      Betti-0 (components): 28.0
      Betti-1 (cycles): 0.0
      Complexity: 28.0

   Structural drift detected (9 deviations):
      - SV: persistent_entropy = 0.0 (avg: 0.4, 100% deviation)
      - RE: persistent_entropy = 0.1 (avg: 0.4, 70% deviation)
      - WK: persistent_entropy = 0.1 (avg: 0.4, 74% deviation)
      - ZV: persistent_entropy = 1.1 (avg: 0.4, 166% deviation)
      ...
```

## Module Structure

| Module | Purpose |
|--------|---------|
| `topology/analyzer.py` | Core TDA analysis (`analyze_topological_quality`) |
| `topology/tda/extractor.py` | `TableTopologyExtractor` for persistence diagrams |
| `topology/extraction.py` | Betti number extraction, entropy computation |
| `topology/stability.py` | Homological stability assessment |
| `topology/db_models.py` | `TopologicalQualityMetrics` persistence |
| `slicing/slice_runner.py` | `run_topology_on_slices()` - orchestration |
| `temporal_slicing/analyzer.py` | `analyze_temporal_topology()` - time-based |

## Metrics Captured Per Slice

| Metric | Description | Source |
|--------|-------------|--------|
| `betti_0` | Connected components | TDA extractor |
| `betti_1` | Cycles/loops | TDA extractor |
| `betti_2` | Voids | TDA extractor |
| `complexity` | Total Betti sum | TDA extractor |
| `persistent_entropy` | Information content | TDA extractor |
| `num_cycles` | Persistent cycles count | TDA extractor |
| `has_anomalies` | Anomaly flag | TDA analyzer |
| `anomalies` | Anomaly descriptions | TDA analyzer |
| `stability` | Homological stability | TDA analyzer |

## Benefits of Integration

1. **Single Source of Truth:** No duplicated Betti computation
2. **Full TDA:** Persistence diagrams, not just simple connectivity
3. **Persistent Entropy:** Detects information complexity variations
4. **Historical Stability:** Compare to previous runs
5. **Rich Anomalies:** Multiple anomaly types (fragmentation, cycles, etc.)
6. **DB Persistence:** Uses existing `TopologicalQualityMetrics` model
