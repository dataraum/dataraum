# Topology & Cycle Detection: Data and Information Concept

## Executive Summary

This document outlines how topological data analysis (TDA) metrics become valuable quality context for downstream AI agents. The key insight: **Betti numbers alone are meaningless; their value emerges through domain interpretation and change detection.**

### Important Notes

1. **Existing Infrastructure**: The `quality/formatting/` module contains formatters with configurable thresholds (`ThresholdConfig`, `InterpretationTemplate`). The topology formatter will be the first migrated to the new module structure.

2. **LLM Context Approach**: Cycle classification uses `config/domains/financial.yaml` as **vocabulary for LLM context**, NOT for deterministic pattern matching. The LLM receives cycle definitions as context and classifies—this avoids brittle regex while keeping LLM grounded in domain knowledge.

3. **Complexity Warning**: Previous implementation attempts have failed partway through. This plan emphasizes incremental, testable steps with clear checkpoints.

---

## Existing Implementation Inventory

**CRITICAL**: A substantial implementation already exists. This plan is about **restructuring and cleanup**, NOT building from scratch.

### Current State (What Already Exists)

| Location | Lines | What It Does |
|----------|-------|--------------|
| `quality/topological.py` | ~877 | Full single-table and multi-table TDA analysis |
| `quality/domains/financial_orchestrator.py` | ~1195 | Complete orchestration: metrics + domain rules + LLM |
| `quality/domains/financial_llm.py` | ~374 | LLM cycle classification and interpretation |
| `quality/formatting/topological.py` | ~758 | Topology formatter with threshold configs |
| `enrichment/tda/topology_extractor.py` | ~326 | TDA extraction using ripser |
| `enrichment/tda/relationship_finder.py` | ~??? | Cross-table relationship detection |
| `enrichment/db_models.py` | ~223 | TopologicalQualityMetrics, MultiTableTopologyMetrics, BusinessCycleClassification |
| `enrichment/topology.py` | ~244 | High-level topology enrichment entry point |
| `config/domains/financial.yaml` | ~434 | Cycle vocabulary, thresholds, fiscal calendar |

### Functions That Already Exist

**In `quality/topological.py`**:
- `extract_betti_numbers()` - Extract β₀, β₁, β₂ from persistence diagrams
- `process_persistence_diagrams()` - Convert to structured PersistenceDiagram objects
- `compute_persistent_entropy()` - Compute entropy measure of complexity
- `detect_persistent_cycles()` - Find significant H1 features
- `assess_homological_stability()` - Compare with previous period (bottleneck distance)
- `compute_historical_complexity()` - Trend analysis over lookback window
- `analyze_topological_quality()` - **Main entry point** for single-table analysis
- `analyze_topological_quality_multi_table()` - Cross-table with graph analysis

**In `quality/domains/financial_orchestrator.py`**:
- `assess_fiscal_stability()` - Domain rule: fiscal period awareness
- `detect_financial_anomalies()` - Domain rule: anomaly detection
- `analyze_complete_financial_quality()` - Single-table: metrics + domain rules + LLM
- `classify_cross_table_cycle_with_llm()` - Multi-label LLM classification
- `analyze_complete_financial_dataset_quality()` - **Full dataset analysis**

**In `quality/domains/financial_llm.py`**:
- `classify_financial_cycle_with_llm()` - Classify single cycle with config vocabulary
- `interpret_financial_quality_with_llm()` - Holistic quality interpretation

---

## Concept vs Reality: Gap Analysis

### What Chapters 1-6 Propose vs What Already Exists

| Concept (Ch 1-6) | Already Exists? | Where | Gap/Opportunity |
|------------------|-----------------|-------|-----------------|
| **Betti numbers extraction** | ✅ Yes | `quality/topological.py:extract_betti_numbers()` | None |
| **Persistence diagrams** | ✅ Yes | `PersistenceDiagram` model, `process_persistence_diagrams()` | None |
| **Persistent entropy** | ✅ Yes | `compute_persistent_entropy()` | None |
| **Cycle detection** | ✅ Yes | `CycleDetection` model, `detect_persistent_cycles()` | None |
| **Bottleneck distance stability** | ✅ Yes | `StabilityAnalysis` model, `assess_homological_stability()` | None |
| **Historical complexity tracking** | ✅ Yes | `complexity_mean`, `complexity_std`, `complexity_z_score` in model | None |
| **Multi-table topology** | ✅ Yes | `analyze_topological_quality_multi_table()` | None |
| **LLM cycle classification** | ✅ Yes | `classify_financial_cycle_with_llm()` | None |
| **Domain vocabulary (config)** | ✅ Yes | `config/domains/financial.yaml` | None |
| **Fiscal period rules** | ✅ Yes | `assess_fiscal_stability()` | None |
| **Anomaly detection** | ✅ Yes | `detect_financial_anomalies()` | None |
| **Hybrid DB storage** | ✅ Yes | `TopologicalQualityMetrics` has structured + JSONB | None |
| **Threshold-based formatting** | ✅ Yes | `quality/formatting/topological.py` | 758 lines - could simplify |
| **Layered information value** | ⚠️ Implicit | Scattered across functions | Could be more explicit |
| **Future domain extensibility** | ⚠️ Partial | Financial only | Architecture supports it |

### Key Finding: The Concept Documents What Exists

**The concept (chapters 1-6) is essentially a description of the current implementation.** The main gaps are:

1. **Organization** - Code is scattered, not architectural gaps
2. **Clarity** - The layering is implicit in code flow, not explicit in structure
3. **Simplification** - Some files are large (758 lines for formatter)

### What's Actually New in the Concept?

| New Idea | Value | Should We Add? |
|----------|-------|----------------|
| **Explicit 4-layer architecture** | Makes mental model clear | Yes - via module structure |
| **Queryable computed columns** | `GENERATED ALWAYS AS` in DB | Maybe - depends on query patterns |
| **Externalized thresholds to YAML** | Easier tuning without code changes | Yes - during consolidation |
| **Healthcare/marketing domain examples** | Shows extensibility pattern | No - wait for actual need |

### Recommendations: What to Change During Move

1. **Keep algorithms unchanged** - They work, don't fix what isn't broken

2. **Simplify the formatter** (758 → ~300 lines)
   - Extract threshold configs to YAML
   - Reduce redundant severity mapping code
   - Keep core formatting logic

3. **Make layering explicit** via module structure:
   ```
   analysis/topology/
   ├── extraction.py    # Layer 1: Raw computation
   ├── stability.py     # Layer 2: Historical context
   └── analyzer.py      # Orchestrates layers 1-2

   domains/financial/cycles/
   ├── rules.py         # Layer 3: Domain rules
   ├── classifier.py    # Layer 4: LLM interpretation
   └── interpreter.py   # Layer 4: LLM interpretation
   ```

4. **Single config loader** - Remove duplicate `_load_financial_config()`

5. **Consider externalizing thresholds** to `config/thresholds/topology.yaml`:
   ```yaml
   betti_0:
     thresholds: {none: 1, moderate: 2, high: 3}
     default_severity: severe
   bottleneck:
     thresholds: {stable: 0.1, minor_changes: 0.2, significant_changes: 0.5}
     default_severity: unstable
   ```

### Freedom to Change (No External Dependencies)

**Everything can be changed** - there are no external consumers:

| Item | Can Change? | Notes |
|------|-------------|-------|
| TDA algorithms | ✅ Yes | Keep if working, improve if needed |
| LLM prompts | ✅ Yes | Not tuned yet, can redesign |
| DB table names | ✅ Yes | No migration needed, rename freely |
| Config file locations | ✅ Yes | Move to better location if useful |
| Pydantic model fields | ✅ Yes | Not used outside TDA, can restructure |

This means during consolidation we can:
- Rename tables/models for clarity
- Restructure Pydantic models if current design is awkward
- Rewrite LLM prompts if they're not well designed
- Move configs to more logical locations
- Simplify interfaces

---

## 1. The Three Use Cases

### A. General Topology Metrics (Domain-Agnostic)
**Purpose**: Detect structural anomalies in ANY dataset

| Metric | What It Measures | Quality Signal |
|--------|------------------|----------------|
| β₀ (Betti-0) | Connected components | Fragmentation - isolated data clusters |
| β₁ (Betti-1) | Cycles/loops | Circular relationships - redundancy or flows |
| β₂ (Betti-2) | Voids/cavities | Higher-order holes - rare, usually noise |
| Persistence | Feature lifetime | Significance - short-lived = noise |
| Entropy | Complexity distribution | Richness - uniform vs hierarchical |
| Stability | Topology change over time | Consistency - structural drift |

**Example Quality Signals**:
- β₀ > 1 in a single table → Missing relationships or orphaned records
- High β₁ with low persistence → Noise, not meaningful cycles
- Stability drop (bottleneck distance spike) → Data quality degradation

### B. Financial Cycle Detection (Domain-Specific)
**Purpose**: Identify and validate business process cycles

| Cycle Type | Expected Tables | Topology Signature |
|------------|-----------------|-------------------|
| Accounts Receivable | customers, invoices, payments | β₁=1, high persistence |
| Accounts Payable | vendors, POs, invoices, payments | β₁=1, high persistence |
| Revenue Cycle | orders, fulfillment, billing, cash | β₁=1-2 |
| Intercompany | entities, transfers, eliminations | β₁ varies by structure |

**Classification Approach** (from `config/domains/financial.yaml`):
- Cycle vocabulary (AR, AP, Inventory, Payroll, etc.) is passed to LLM as context
- LLM classifies detected cycles using this vocabulary
- NOT pattern matching—LLM interprets based on column names, semantic roles, relationships
- Configuration defines `expected_cycles` for completeness checks

**Domain Rules** (deterministic, auditable):
- If period-end + instability → EXPECTED (fiscal close)
- If mid-period + instability → ALERT (data quality issue)
- If β₀ > 2 in GL → WARN (cost centers may be disconnected)
- If expected cycle missing → WARN (incomplete data flow)

### C. LLM Context Approach for Cycle Classification

**Why NOT Pattern Matching**:
- Column names are inconsistent across organizations
- Regex patterns become brittle and unmaintainable
- False positives/negatives erode trust

**Why LLM Classification Works**:
- LLM can interpret semantic meaning, not just string patterns
- Domain vocabulary provides grounding (prevents hallucination)
- Human-readable explanations for audit trails

**How It Works** (from `config/domains/financial.yaml`):

```yaml
# This is VOCABULARY, not pattern matching rules
cross_table_cycle_patterns:
  accounts_receivable_cycle:
    description: "AR collection cycle spanning customer and transaction tables"
    table_patterns:  # Hints for LLM, not strict matches
      - ["*transaction*", "*customer*"]
      - ["*invoice*", "*customer*", "*payment*"]
    column_indicators:
      required:
        - pattern: "A/R|receivable|ar_"
          description: "AR-specific columns"
      supporting:
        - pattern: "customer|client"
          description: "Customer identifiers"
    business_value: "high"
```

**LLM Prompt Structure**:
```
Context: You are classifying detected topology cycles in financial data.

Available cycle types (from domain config):
- accounts_receivable_cycle: AR collection cycle...
- expense_cycle: AP/Expense cycle...
- revenue_cycle: Revenue recognition...

Detected cycle:
- Tables involved: [customers, invoices, payments]
- Key columns: [customer_id, invoice_number, payment_date, ar_balance]
- Relationship cardinality: many-to-one (invoices → customers)

Classify this cycle. Respond with:
1. Cycle type (from list above, or "unknown")
2. Confidence (0-1)
3. Reasoning (1-2 sentences)
```

**Key Principle**: The YAML config provides **vocabulary and structure** for the LLM. The LLM does the actual classification based on semantic understanding. This keeps the LLM grounded while avoiding brittle pattern matching.

### D. Future Domain Extensibility
**Purpose**: Enable other domains (healthcare, marketing, supply chain) to define their own cycle interpretations

**Pattern**: Domain analyzers register cycle vocabularies and rules:
```yaml
# config/domains/healthcare.yaml
cycles:
  patient_journey:
    expected_tables: [patients, encounters, diagnoses, treatments, outcomes]
    expected_betti_1: 1-2
    interpretation: "Patient care pathway"

  claim_cycle:
    expected_tables: [encounters, claims, adjudication, payments]
    expected_betti_1: 1
    interpretation: "Revenue cycle management"

rules:
  fragmentation_threshold: 3  # β₀ > 3 triggers warning
  missing_cycle_severity: moderate
```

---

## 2. Information Value: What Makes Metrics Meaningful

### The Raw Numbers Are Meaningless
```
β₀=2, β₁=5, β₂=0
```
This tells us nothing without context.

### Context Makes Them Valuable

**Layer 1: Structural Context**
```
Table: general_ledger (500k rows)
Columns: account_code, amount, date, cost_center, entity
β₀=2 → Two disconnected component clusters
β₁=5 → Five independent cycles detected
Persistence: [0.8, 1.2, 0.3, 2.1, 0.5] → Only 2 significant (>1.0)
Entropy: 1.4 → Moderately complex topology
```

**Layer 2: Domain Context**
```
Domain: Financial (GL)
Expected: β₀=1 (single connected ledger), β₁=3-5 (AR, AP, payroll, etc.)
Observed: β₀=2 (ALERT: disconnected), β₁=5 (OK)
Fiscal Context: Quarter-end close
Interpretation: Second component may be closing adjustments (expected)
```

**Layer 3: Historical Context**
```
Previous Period: β₀=1, β₁=5, bottleneck_distance=0.0
Current Period: β₀=2, β₁=5, bottleneck_distance=0.45
Stability: "significant_changes" (threshold: 0.2)
Pattern: Recurring at period-end (last 4 quarters)
Assessment: Normal fiscal period effect, not data quality issue
```

**Layer 4: Quality Assessment (for AI Agents)**
```json
{
  "topology_quality": {
    "overall_health": "good",
    "structural_issues": [
      {
        "type": "temporary_fragmentation",
        "severity": "info",
        "description": "Two components detected during period-end close",
        "is_expected": true,
        "recommendation": null
      }
    ],
    "cycle_health": {
      "expected_cycles_present": ["ar_cycle", "ap_cycle", "payroll_cycle"],
      "missing_cycles": [],
      "anomalous_cycles": []
    },
    "trend": "stable_with_periodic_variation"
  }
}
```

---

## 3. Data Model Design

### Core Principle: Hybrid Storage
Structured columns for querying + JSONB for full reconstruction

### Table: `topological_metrics` (renamed from `TopologicalQualityMetrics`)

```sql
CREATE TABLE topological_metrics (
    -- Identity
    metric_id UUID PRIMARY KEY,
    table_id UUID REFERENCES tables(table_id),
    computed_at TIMESTAMP NOT NULL,

    -- Queryable Betti Numbers
    betti_0 INTEGER NOT NULL,           -- Connected components
    betti_1 INTEGER NOT NULL,           -- Cycles
    betti_2 INTEGER NOT NULL DEFAULT 0, -- Voids (rare)
    structural_complexity INTEGER GENERATED ALWAYS AS (betti_0 + betti_1 + betti_2) STORED,

    -- Queryable Persistence Summary
    max_persistence_h0 FLOAT,
    max_persistence_h1 FLOAT,
    persistent_entropy FLOAT,

    -- Queryable Stability
    bottleneck_distance FLOAT,
    is_stable BOOLEAN,
    stability_level VARCHAR(20),  -- 'stable', 'minor_changes', 'significant_changes', 'unstable'

    -- Queryable Flags
    has_cycles BOOLEAN GENERATED ALWAYS AS (betti_1 > 0) STORED,
    has_anomalies BOOLEAN DEFAULT FALSE,
    is_fragmented BOOLEAN GENERATED ALWAYS AS (betti_0 > 1) STORED,

    -- Full Data (JSONB for reconstruction)
    persistence_diagrams JSONB NOT NULL,  -- Full diagrams for all dimensions
    detected_cycles JSONB,                 -- CycleDetection objects
    stability_details JSONB,               -- Full StabilityAnalysis
    anomalies JSONB,                       -- Detected anomalies

    -- Audit
    previous_metric_id UUID REFERENCES topological_metrics(metric_id)
);

-- Indexes for common queries
CREATE INDEX idx_topo_table ON topological_metrics(table_id);
CREATE INDEX idx_topo_stability ON topological_metrics(is_stable, stability_level);
CREATE INDEX idx_topo_fragmented ON topological_metrics(is_fragmented) WHERE is_fragmented;
CREATE INDEX idx_topo_cycles ON topological_metrics(betti_1) WHERE betti_1 > 0;
```

### Table: `cross_table_topology` (renamed from `MultiTableTopologyMetrics`)

```sql
CREATE TABLE cross_table_topology (
    metric_id UUID PRIMARY KEY,
    table_ids JSONB NOT NULL,           -- Array of table UUIDs
    computed_at TIMESTAMP NOT NULL,

    -- Graph-Level Betti
    graph_betti_0 INTEGER NOT NULL,     -- Connected table groups
    graph_cycle_count INTEGER NOT NULL, -- Table-level cycles
    relationship_count INTEGER NOT NULL,

    -- Flags
    is_connected BOOLEAN GENERATED ALWAYS AS (graph_betti_0 = 1) STORED,
    has_table_cycles BOOLEAN GENERATED ALWAYS AS (graph_cycle_count > 0) STORED,

    -- Full Data
    per_table_topology JSONB,           -- TopologicalMetrics per table
    detected_cycles JSONB,              -- Cross-table cycles (table paths)
    relationship_evidence JSONB         -- Join evidence
);
```

### Table: `domain_cycle_classifications` (domain-specific)

```sql
CREATE TABLE domain_cycle_classifications (
    classification_id UUID PRIMARY KEY,
    metric_id UUID REFERENCES topological_metrics(metric_id),
    domain VARCHAR(50) NOT NULL,        -- 'financial', 'healthcare', etc.

    -- Classification Result
    cycle_type VARCHAR(100),            -- 'accounts_receivable_cycle'
    confidence FLOAT,
    business_value VARCHAR(20),         -- 'high', 'medium', 'low'
    is_expected BOOLEAN,

    -- Context
    involved_tables JSONB,
    involved_columns JSONB,
    classification_evidence JSONB,

    -- LLM Attribution
    classified_by VARCHAR(50),          -- 'rules', 'llm:claude-3-5-sonnet'
    classified_at TIMESTAMP NOT NULL
);
```

---

## 4. Existing Formatter Infrastructure

### Current State (`quality/formatting/`)

The project has an existing formatter pattern that should be leveraged:

```python
# quality/formatting/base.py - Core patterns to reuse

@dataclass
class ThresholdConfig:
    """Maps numeric values to severity levels."""
    thresholds: dict[str, float]  # {"none": 1.0, "moderate": 5.0, "high": 10.0}
    default_severity: str = "severe"
    ascending: bool = True

    def get_severity(self, value: float) -> str:
        # Higher values = more severe (or reverse if ascending=False)

@dataclass
class InterpretationTemplate:
    """Templates for natural language interpretations."""
    templates: dict[str, str]  # {"none": "No issues", "high": "Elevated {metric}"}

    def format(self, severity: str, **kwargs) -> str:
        # Generate human-readable interpretation

class CommonThresholds:
    """Reusable threshold configurations."""
    VIF: ThresholdConfig  # 1.0 → 5.0 → 10.0
    CORRELATION: ThresholdConfig
    COMPLETENESS: ThresholdConfig  # Descending (lower is worse)
    NULL_RATIO: ThresholdConfig
```

### Topology Formatter Migration

The existing `quality/formatting/topological.py` (758 lines) will be the **first formatter migrated** to the new module structure. It contains:

- `format_structure_group()` - Betti numbers interpretation
- `format_cycles_group()` - Cycle detection formatting
- `format_complexity_group()` - Entropy and complexity
- `format_topological_stability_group()` - Stability analysis

**Migration approach**:
1. Extract threshold configs to `analysis/topology/thresholds.py`
2. Move formatting logic to `analysis/topology/formatter.py`
3. Keep using `ThresholdConfig` pattern from `quality/formatting/base.py`
4. Deprecate old file with re-exports during transition

### Topology Thresholds (to be extracted)

```python
# analysis/topology/thresholds.py

from dataclasses import dataclass, field
from quality.formatting.base import ThresholdConfig

@dataclass
class TopologyThresholds:
    """Threshold configurations for topology metrics."""

    # Betti-0 (fragmentation)
    BETTI_0: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 1, "moderate": 2, "high": 3},
            default_severity="severe",
        )
    )

    # Betti-1 (cycles)
    BETTI_1: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 3, "low": 5, "moderate": 10, "high": 15},
            default_severity="severe",
        )
    )

    # Persistence significance
    PERSISTENCE: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"noise": 0.2, "weak": 0.5, "significant": 1.0},
            default_severity="strong",
        )
    )

    # Bottleneck distance (stability)
    BOTTLENECK: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"stable": 0.1, "minor_changes": 0.2, "significant_changes": 0.5},
            default_severity="unstable",
        )
    )

TOPOLOGY_THRESHOLDS = TopologyThresholds()
```

---

## 5. Module Architecture

### Proposed Structure

```
analysis/
└── topology/
    ├── __init__.py
    ├── db_models.py              # TopologicalMetrics, CrossTableTopology
    ├── models.py                 # Pydantic: BettiNumbers, PersistenceDiagram, etc.
    │
    ├── extraction/
    │   ├── __init__.py
    │   ├── feature_matrix.py     # Table → feature vectors
    │   ├── persistence.py        # Feature vectors → persistence diagrams
    │   └── betti.py              # Diagrams → Betti numbers
    │
    ├── analysis/
    │   ├── __init__.py
    │   ├── single_table.py       # analyze_topology(table_id) → TopologicalMetrics
    │   ├── multi_table.py        # analyze_cross_table_topology(table_ids)
    │   ├── stability.py          # compare_topology(prev, curr) → StabilityAnalysis
    │   └── cycles.py             # extract_cycles(diagrams) → CycleDetection[]
    │
    ├── processor.py              # Main entry: profile_topology()
    └── formatter.py              # Format for LLM context

domains/
├── base.py                       # DomainAnalyzer ABC (existing)
├── registry.py                   # Domain registration (existing)
├── db_models.py                  # DomainCycleClassification
│
└── financial/
    ├── __init__.py
    ├── analyzer.py               # FinancialDomainAnalyzer (existing)
    ├── checks.py                 # Standard checks (existing)
    ├── models.py                 # (existing)
    ├── db_models.py              # (existing)
    │
    ├── cycles/                   # NEW: Financial cycle analysis
    │   ├── __init__.py
    │   ├── detector.py           # detect_financial_cycles(topology) → Cycles
    │   ├── rules.py              # Deterministic fiscal rules
    │   ├── classifier.py         # LLM cycle classification
    │   └── config.py             # Load config/domains/financial.yaml
    │
    └── orchestrator.py           # Combines checks + topology + cycles
```

### Dependency Flow

```
analysis/topology (domain-agnostic)
    ↓ exports: TopologicalMetrics, analyze_topology(), compare_topology()

domains/financial/cycles (domain-specific)
    ↓ imports: TopologicalMetrics
    ↓ applies: fiscal rules, cycle vocabularies
    ↓ exports: FinancialCycleAnalysis

domains/financial/orchestrator
    ↓ imports: standard checks + topology + cycles
    ↓ exports: complete_financial_quality_analysis()
```

---

## 5. Information Flow for AI Agents

### What Agents Receive

**Minimal Context** (for simple queries):
```json
{
  "topology_summary": {
    "connected_components": 1,
    "cycle_count": 3,
    "is_stable": true,
    "complexity": "moderate"
  }
}
```

**Standard Context** (for quality assessment):
```json
{
  "topology": {
    "betti_numbers": {"b0": 1, "b1": 3, "b2": 0},
    "significant_cycles": 2,
    "stability": "stable",
    "entropy": 1.2,
    "interpretation": "Well-connected dataset with expected business process cycles"
  },
  "quality_signals": [
    {"type": "cycle_health", "status": "ok", "detail": "All expected cycles present"}
  ]
}
```

**Rich Context** (for deep analysis):
```json
{
  "topology": {
    "betti_numbers": {"b0": 1, "b1": 3, "b2": 0},
    "persistence_diagrams": {
      "H0": [{"birth": 0, "death": "inf"}],
      "H1": [
        {"birth": 0.1, "death": 2.3, "persistence": 2.2},
        {"birth": 0.2, "death": 1.8, "persistence": 1.6},
        {"birth": 0.5, "death": 0.7, "persistence": 0.2}
      ]
    },
    "significant_features": [
      {"dimension": 1, "persistence": 2.2, "interpretation": "Primary business cycle"},
      {"dimension": 1, "persistence": 1.6, "interpretation": "Secondary process flow"}
    ],
    "stability": {
      "bottleneck_distance": 0.15,
      "is_stable": true,
      "components_changed": 0,
      "cycles_changed": 0
    },
    "entropy": 1.2,
    "historical_trend": "stable_4_periods"
  },
  "domain_context": {
    "domain": "financial",
    "classified_cycles": [
      {"type": "accounts_receivable", "confidence": 0.92, "tables": ["customers", "invoices", "payments"]},
      {"type": "accounts_payable", "confidence": 0.88, "tables": ["vendors", "bills", "payments"]}
    ],
    "fiscal_context": {
      "current_period": "2024-Q4",
      "is_period_end": false,
      "expected_instability": false
    },
    "anomalies": []
  }
}
```

---

## 6. Quality Signals from Topology

### Universal Signals (Any Domain)

| Signal | Condition | Severity | Meaning |
|--------|-----------|----------|---------|
| `fragmented_data` | β₀ > 1 | Warning | Disconnected data clusters |
| `missing_relationships` | Expected joins not found | Moderate | Incomplete schema |
| `noisy_topology` | High β₁ with low persistence | Info | Many weak/spurious cycles |
| `structural_drift` | Stability dropping over time | Warning | Data quality degradation |
| `anomalous_complexity` | Entropy z-score > 2 | Warning | Unusual structural pattern |

### Financial-Specific Signals

| Signal | Condition | Severity | Meaning |
|--------|-----------|----------|---------|
| `missing_ar_cycle` | AR cycle not detected | Moderate | Incomplete receivables flow |
| `missing_ap_cycle` | AP cycle not detected | Moderate | Incomplete payables flow |
| `excessive_cycles` | β₁ > 10 | Warning | Overly complex/circular data |
| `fiscal_instability` | Instability outside period-end | Critical | Unexpected structural change |
| `orphaned_entities` | β₀ > 2 in consolidated data | Warning | Intercompany gaps |

---

## 7. Migration Plan: Consolidation & Cleanup

**Goal**: One clear home for each concept. No duplication. Easier to understand and maintain.

**No backward compatibility needed** - We update all imports directly. This is a cleanup, not a library release.

### Current Problems

| Problem | Example |
|---------|---------|
| **Topology scattered** | `enrichment/tda/`, `enrichment/topology.py`, `quality/topological.py` |
| **Unclear boundaries** | Is topology "enrichment" or "quality" or "analysis"? |
| **Financial cycles split** | Orchestrator in `quality/domains/`, checks in `domains/financial/` |
| **Duplicate config loading** | `_load_financial_config()` in 2+ files |
| **DB models scattered** | Topology in `enrichment/db_models.py`, BusinessCycle same place |

### Target Architecture

```
analysis/
└── topology/                    # ALL topology lives here - ONE place
    ├── __init__.py              # Public API: analyze_topology()
    ├── tda/
    │   └── extractor.py         # TableTopologyExtractor (from enrichment/tda/)
    ├── extraction.py            # Betti numbers, persistence, entropy
    ├── stability.py             # Bottleneck distance, historical trends
    ├── analyzer.py              # Main: analyze_topology(), analyze_topology_multi_table()
    ├── models.py                # Pydantic: BettiNumbers, TopologicalResult, etc.
    ├── db_models.py             # TopologicalMetrics, MultiTableTopology
    └── formatter.py             # LLM context formatting with thresholds

domains/
└── financial/                   # ALL financial domain logic here - ONE place
    ├── __init__.py              # Public API
    ├── checks.py                # Accounting checks (exists, keep)
    ├── config.py                # Single loader for financial.yaml
    ├── cycles/
    │   ├── detector.py          # Cycle detection using topology
    │   ├── classifier.py        # LLM classification
    │   ├── rules.py             # Fiscal stability, anomaly detection
    │   └── interpreter.py       # LLM holistic interpretation
    ├── orchestrator.py          # Main: analyze_financial_quality()
    ├── models.py                # Pydantic models
    └── db_models.py             # BusinessCycleClassification + metrics
```

### Phase 8A: Consolidate `analysis/topology/`

**Step 1: Create structure + move TDA core**
```
enrichment/tda/topology_extractor.py → analysis/topology/tda/extractor.py
enrichment/tda/relationship_finder.py → (check if used, may delete)
```

**Step 2: Move quality/topological.py functions**
```
quality/topological.py → analysis/topology/
├── extraction.py    # extract_betti_numbers, process_persistence_diagrams, compute_persistent_entropy
├── stability.py     # assess_homological_stability, compute_historical_complexity
└── analyzer.py      # analyze_topological_quality, analyze_topological_quality_multi_table
```

**Step 3: Move models**
```
quality/models.py (topology parts) → analysis/topology/models.py
enrichment/db_models.py (topology parts) → analysis/topology/db_models.py
```

**Step 4: Move formatter**
```
quality/formatting/topological.py → analysis/topology/formatter.py
```

**Step 5: Delete old files**
- `enrichment/tda/` - emptied
- `enrichment/topology.py` - delete (redundant wrapper)
- `quality/topological.py` - delete
- `quality/formatting/topological.py` - delete

**Step 6: Update all imports** (no re-exports, direct fixes)

### Phase 8B: Consolidate `domains/financial/`

**Step 1: Create cycles/ submodule**
```
domains/financial/cycles/
├── __init__.py
├── detector.py      # detect_financial_anomalies (from orchestrator)
├── rules.py         # assess_fiscal_stability (from orchestrator)
├── classifier.py    # classify_*_cycle_with_llm (from financial_llm.py)
└── interpreter.py   # interpret_financial_quality_with_llm (from financial_llm.py)
```

**Step 2: Create single config loader**
```
domains/financial/config.py  # _load_financial_config() - ONE place
```

**Step 3: Consolidate orchestrator**
```
quality/domains/financial_orchestrator.py → domains/financial/orchestrator.py
(Main functions: analyze_complete_financial_quality, analyze_complete_financial_dataset_quality)
```

**Step 4: Move DB model**
```
enrichment/db_models.py (BusinessCycleClassification) → domains/financial/db_models.py
```

**Step 5: Delete old files**
- `quality/domains/financial_orchestrator.py` - delete
- `quality/domains/financial_llm.py` - delete

**Step 6: Update all imports**

### Phase 10: Final Cleanup

1. **Run full test suite** - fix all import errors
2. **Verify scripts work** - `scripts/run_phase9_financial_domain.py`
3. **Update `scripts/infra.py`** model imports
4. **Clean up empty directories**
5. **Update this document** with final structure

### Simplification Opportunities (Do During Move)

| What | Why |
|------|-----|
| Remove duplicate `_load_financial_config()` | Currently in 2 files |
| Consolidate `enrichment/topology.py` wrapper | Thin layer adds no value |
| Check if `relationship_finder.py` is used | May be dead code |
| Simplify formatter thresholds | May have unused severity levels |

### What Changes, What Stays

| Changes | Stays |
|---------|-------|
| File locations | Algorithm logic |
| Import paths | DB table names |
| Module organization | Config file location |
| (clean up duplication) | LLM prompts |

---

## 8. Execution Approach

### Why Previous Attempts Failed

1. **Trying to do too much at once** - Moving + refactoring + adding features simultaneously
2. **No working checkpoint** - Breaking tests and pushing forward anyway
3. **Unclear target** - Not knowing what "done" looks like

### How We Avoid This

**Principle: Move one file at a time. Tests must pass after each move.**

### Execution Order

```
Phase 8A: topology consolidation
├── Step 1: Create analysis/topology/ skeleton
├── Step 2: Move enrichment/tda/topology_extractor.py
│   └── Update imports → Run tests → Commit
├── Step 3: Move quality/topological.py (split into files)
│   └── Update imports → Run tests → Commit
├── Step 4: Move topology models from quality/models.py
│   └── Update imports → Run tests → Commit
├── Step 5: Move topology DB models from enrichment/db_models.py
│   └── Update imports → Run tests → Commit
├── Step 6: Move formatter from quality/formatting/topological.py
│   └── Update imports → Run tests → Commit
└── Step 7: Delete emptied files, clean up

Phase 8B: financial cycles consolidation
├── Step 1: Create domains/financial/cycles/ skeleton
├── Step 2: Create config.py (single config loader)
├── Step 3: Move rules.py (assess_fiscal_stability)
├── Step 4: Move detector.py (detect_financial_anomalies)
├── Step 5: Move classifier.py (LLM classification functions)
├── Step 6: Move interpreter.py (LLM interpretation)
├── Step 7: Move orchestrator.py
├── Step 8: Move BusinessCycleClassification DB model
└── Step 9: Delete old files, clean up

Phase 10: verification
├── Run full test suite
├── Run scripts/run_phase9_financial_domain.py
├── Verify DB migrations
└── Update documentation
```

### Checkpoints

After EACH file move:
1. `pytest tests/` - must pass
2. Check for import errors in dependent files
3. Commit the change

If tests fail:
- Fix the imports
- If fix takes > 10 minutes, reconsider the approach
- Don't proceed to next file until green

### Red Flags

Stop and reassess if:
- More than 3 import errors from one move
- Circular import (A imports B imports A)
- Need to modify algorithm logic to make move work
- Tests fail for reasons other than import paths

---

## 9. Success Criteria

### Functional
- [ ] `analyze_topology(table_id)` returns complete TopologicalMetrics
- [ ] Cross-table topology detects relationship cycles
- [ ] Financial cycle detection works with domain rules
- [ ] LLM classification integrates cleanly
- [ ] Formatter produces useful LLM context at all detail levels

### Architectural
- [ ] `analysis/topology/` has no domain-specific code
- [ ] `domains/*/cycles/` contains only domain-specific logic
- [ ] No circular dependencies
- [ ] Clean separation: computation → rules → LLM

### Quality
- [ ] All existing tests pass
- [ ] New tests for topology extraction
- [ ] Integration test: full financial quality pipeline
- [ ] Performance: topology analysis < 30s for 100k rows

---

## 10. Open Questions

1. **Persistence threshold**: What persistence value distinguishes signal from noise?
   - Current: No threshold, all features included
   - Proposal: Configurable per-domain (default: 0.5)

2. **Historical comparison window**: How many periods to track for stability?
   - Current: Only previous period
   - Proposal: Configurable (default: 4 periods)

3. **Multi-table cycle semantics**: Should table-level cycles map to business processes?
   - Current: Yes, via LLM classification
   - Alternative: Rule-based mapping from config

4. **Cross-domain cycles**: Can cycles span domains (e.g., financial + supply chain)?
   - Current: Not supported
   - Future: Consider after single-domain patterns are solid

---

## 11. Summary

**The Key Insight**: Topology metrics become valuable through layered interpretation:

1. **Computation** (domain-agnostic): Betti numbers, persistence, entropy
2. **Context** (domain-agnostic): Stability analysis, historical trends
3. **Domain Rules** (domain-specific): Fiscal calendar, expected patterns
4. **LLM Interpretation** (domain-specific): Cycle classification, quality assessment

**The Migration Strategy**:
- Phase 8A: Extract pure topology to `analysis/topology/`
- Phase 8B: Move domain cycles to `domains/*/cycles/`
- Phase 10: Integrate and clean up

**The Value Proposition**:
- AI agents receive rich, contextualized topology information
- Quality signals are actionable (not just numbers)
- Future domains can plug in their own cycle vocabularies
