# Topology Integration Summary

## Overview

Implemented topological data analysis (TDA) into the slicing and temporal analysis pipelines. This enables detection of structural patterns and drift across business segments and time periods.

## What Was Implemented

### 1. Per-Slice Topology Analysis (`run_topology_on_slices`)

**Location:** `src/dataraum_context/analysis/slicing/slice_runner.py`

Computes topological features for each slice table:

- **Betti-0 (β₀):** Connected components in the correlation graph
- **Betti-1 (β₁):** Cycles/loops in the correlation structure
- **Structural Complexity:** Combined topology score (β₀ + β₁ + β₂)
- **Cross-Slice Drift:** Detects slices with unusual topology vs. average

**Algorithm:**
1. Build correlation graph from numeric columns (edge if |corr| ≥ 0.5)
2. Compute connected components using Union-Find
3. Count triangles for cycle detection
4. Compare each slice to the average topology

**New Components:**
| Component | Type | Description |
|-----------|------|-------------|
| `TopologySlicesResult` | Dataclass | Result container with topology metrics per slice |
| `run_topology_on_slices()` | Function | Main entry point for slice topology analysis |

### 2. Temporal Topology Analysis (`analyze_temporal_topology`)

**Location:** `src/dataraum_context/analysis/temporal_slicing/analyzer.py`

Tracks how data structure changes over time periods:

- **Period Topologies:** Betti numbers computed for each time period
- **Topology Drift:** Detects significant changes between consecutive periods (>20% threshold)
- **Trend Analysis:** Identifies increasing/decreasing/stable/volatile complexity trends
- **Anomaly Detection:** Flags periods with unusual structure (>2 std dev from mean)

**New Components:**
| Component | Type | Location |
|-----------|------|----------|
| `PeriodTopology` | Dataclass | `temporal_slicing/models.py` |
| `TopologyDrift` | Dataclass | `temporal_slicing/models.py` |
| `TemporalTopologyResult` | Dataclass | `temporal_slicing/models.py` |
| `TemporalTopologyAnalysis` | DB Model | `temporal_slicing/db_models.py` |
| `analyze_temporal_topology()` | Function | `temporal_slicing/analyzer.py` |

### 3. Phase 8 Script Integration

**Location:** `scripts/run_phase8_slice_analysis.py`

Added `--topology` flag to enable topology analysis:

```bash
# Run topology only
uv run python ./scripts/run_phase8_slice_analysis.py --skip-semantic --topology

# Run temporal + topology (enables temporal topology automatically)
uv run python ./scripts/run_phase8_slice_analysis.py --skip-semantic --temporal --time-column "Belegdatum der Buchung" --topology
```

## Results from Test Run

```
4. Running temporal analysis on slice tables...
   Temporal analysis complete!
   Slices analyzed: 11
   Periods analyzed: 3
   Incomplete periods: 25
   Volume anomalies: 6
   Drift detected in: 26 slices

   Running temporal topology analysis...
   Temporal topology analyzed: 3 slices
      - slice_herkunftskennzeichen_sv: 3 periods, trend=stable, 2 drifts, 0 anomalies
      - slice_herkunftskennzeichen_re: 1 periods, trend=stable, 0 drifts, 0 anomalies
      - slice_herkunftskennzeichen_an: 3 periods, trend=stable, 1 drifts, 0 anomalies

5. Running topology analysis on slice tables...
   Slices analyzed: 11
   Slices with anomalies: 11

   Average topology across slices:
      Betti-0 (components): 24.0
      Betti-1 (cycles): 5.2
      Complexity: 29.2
```

## Technical Details

### Correlation-Based Graph Construction

- Edges created between column pairs where |correlation| ≥ threshold (default 0.5)
- Uses DuckDB's `CORR()` function for efficient computation
- Handles NULL values by filtering

### Betti Number Computation

| Betti | Meaning | Algorithm |
|-------|---------|-----------|
| β₀ | Connected components | Union-Find with path compression |
| β₁ | Independent cycles | Triangle counting in edge set |
| β₂ | Voids | Set to 0 (rarely relevant for tabular data) |

### Drift Detection Thresholds

| Metric | Threshold | Significance |
|--------|-----------|--------------|
| Betti-0 change | >20% | >50% is significant |
| Complexity change | >20% | >50% is significant |
| Correlation density | >30% | Edge count change |

## Module Exports Updated

**`slicing/__init__.py`:**
```python
from .slice_runner import (
    run_topology_on_slices,
    TopologySlicesResult,
)
```

**`temporal_slicing/__init__.py`:**
```python
from .analyzer import analyze_temporal_topology
from .models import PeriodTopology, TopologyDrift, TemporalTopologyResult
from .db_models import TemporalTopologyAnalysis
```

## Use Cases

1. **Quality Assurance:** Detect slices with unusual correlation structure
2. **Data Drift Monitoring:** Track structural changes over time
3. **Segment Comparison:** Compare topology across business segments
4. **Anomaly Detection:** Flag periods with abnormal data relationships

## Future Enhancements

- Persist temporal topology results to database
- Add visualization of topology evolution
- Implement higher-order Betti numbers for complex datasets
- Add configurable thresholds via YAML config
