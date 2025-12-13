# Multi-Table Business Cycle Analysis Plan

**Status**: Implemented (Phases 1-5 complete, Phase 6 optional cleanup)
**Created**: 2025-12-13
**Last Updated**: 2025-12-13

## Executive Summary

This document explores the architecture for detecting and classifying **business process cycles** (AR, AP, Revenue, Expense) in multi-table financial datasets. The key insight is that business cycles are **cross-table phenomena** that require relationship graph analysis, not single-table TDA.

---

## Problem Statement

### Current Gap

| What We're Detecting | What Business Cycles Actually Are |
|---------------------|-----------------------------------|
| **Single-table TDA**: Column correlation cycles (Amount ↔ Credit ↔ Debit) | **Cross-table flows**: Customer → Invoice → Payment → Account |
| Numerical/structural patterns within one table | Business process patterns spanning multiple tables |
| Topology of one data matrix | Topology of table relationship graph |

### The Finance Example Schema

```
                         ┌─────────────────┐
                         │  Master_txn     │ (central hub)
                         │  - Customer name│──────────┐
                         │  - Vendor name  │          │
                         │  - Account      │          │
                         │  - Product_Svc  │          │
                         │  - payment method          │
                         └─────────────────┘          │
                                │                     │
          ┌─────────────────────┼─────────────────────┼───────────────┐
          ▼                     ▼                     ▼               ▼
     ┌─────────┐          ┌─────────┐          ┌──────────┐   ┌────────────┐
     │customers│          │ vendors │          │ products │   │chart_of_acc│
     └─────────┘          └─────────┘          └──────────┘   └────────────┘
          │                     │
          └──────────┬──────────┘
                     ▼
              ┌──────────────┐
              │payment_methods│
              └──────────────┘
```

### Business Cycles in This Data

1. **AR Cycle (Accounts Receivable)**:
   - `customers` → `transactions` (Sale=Yes, A/R) → `payment_methods`
   - Graph cycle: customers ↔ transactions (via Customer name)

2. **AP Cycle (Accounts Payable)**:
   - `vendors` → `transactions` (Purchase=Yes, A/P) → `payment_methods`
   - Graph cycle: vendors ↔ transactions (via Vendor name)

3. **Revenue Recognition Cycle**:
   - `customers` → `transactions` → `products` → `chart_of_accounts` (Income)

4. **Expense Cycle**:
   - `vendors` → `transactions` → `products` → `chart_of_accounts` (Expense)

---

## Existing Infrastructure

### 1. Relationship Gathering (`cross_table_multicollinearity.py`)

```python
async def gather_relationships(table_ids, session) -> list[EnrichedRelationship]:
    """
    - Queries Relationship table for all pairs of input tables
    - Applies differentiated confidence thresholds:
        - foreign_key: 0.7
        - semantic: 0.6
        - correlation: 0.5
    - Deduplicates bidirectional relationships
    - Enriches with column/table metadata
    """
```

**EnrichedRelationship Model:**
- `from_table`, `to_table`, `from_column`, `to_column`
- `relationship_type` (FK, semantic, correlation, hierarchy)
- `cardinality` (one-to-one, one-to-many, many-to-one, many-to-many)
- `confidence`, `detection_method`, `evidence`

### 2. Relationship Detection (`tda/relationship_finder.py`)

```python
class TableRelationshipFinder:
    """
    - Compares persistence diagrams (Wasserstein distance)
    - Finds join columns (Jaccard similarity, containment)
    - Classifies relationships using Betti numbers
    - Builds join graph (NetworkX)
    """
```

### 3. Graph Cycle Detection (`topological.py`)

```python
def analyze_relationship_graph(table_ids, relationships):
    """
    - Builds directed graph with NetworkX
    - Detects simple cycles: nx.simple_cycles(G)
    - Returns: cycles (list of table_id lists), betti_0, cycle_count
    """
```

### 4. Financial Domain Config (`config/domains/financial.yaml`)

Already defines cycle patterns with:
- `column_patterns`: Keywords to look for
- `semantic_roles`: Expected column roles
- `expected_relationships`: Table relationship patterns
- `business_value`: Importance classification

---

## Proposed Architecture

### Clear Separation: Single-Table vs Cross-Table

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SINGLE-TABLE ANALYSIS                            │
│  Purpose: Data quality within each table                                │
├─────────────────────────────────────────────────────────────────────────┤
│  analyze_topological_quality()                                          │
│      │                                                                  │
│      ├── Betti numbers (connectivity, cycles, voids)                    │
│      ├── Persistence diagrams (structural features)                     │
│      ├── Complexity metrics (entropy, stability)                        │
│      └── Quality warnings (fragmentation, anomalies)                    │
│                                                                         │
│  Output: TopologicalQualityResult per table                             │
│  Use case: "Is this table's internal structure healthy?"                │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                       CROSS-TABLE ANALYSIS                              │
│  Purpose: Business process cycle detection                              │
├─────────────────────────────────────────────────────────────────────────┤
│  analyze_complete_financial_dataset_quality()                           │
│      │                                                                  │
│      ├── Layer 1: Per-table accounting checks                           │
│      │       └── Double-entry, trial balance, sign conventions          │
│      │                                                                  │
│      ├── Layer 2: Relationship graph analysis                           │
│      │       ├── gather_relationships() - aggregate from all sources    │
│      │       └── analyze_relationship_graph() - detect cycles           │
│      │                                                                  │
│      ├── Layer 3: LLM business cycle classification                     │
│      │       └── classify_cross_table_cycle_with_llm()                  │
│      │                                                                  │
│      └── Layer 4: LLM holistic interpretation                           │
│              └── interpret_financial_dataset_quality_with_llm()         │
│                                                                         │
│  Output: DatasetFinancialQualityResult                                  │
│  Use case: "What business processes exist and are they complete?"       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Open Questions: LLM Classification Scope

### Question 1: What should LLM classify?

| Option | Single-Table Cycles | Cross-Table Cycles | Recommendation |
|--------|--------------------|--------------------|----------------|
| **A: Cross-table only** | Skip classification | Classify as AR/AP/Revenue/etc. | **Recommended** |
| **B: Both separately** | Classify as "data patterns" | Classify as "business processes" | Complex |
| **C: Unified** | Combine both inputs | Single classification | Confusing |

**Reasoning for Option A:**
- Single-table "cycles" are really column correlations (Amount ↔ Credit)
- These are **data quality indicators**, not business processes
- LLM classifying them as "AR cycle" would be misleading
- Cross-table cycles ARE the business processes we care about

### Question 2: What context does LLM need for cross-table classification?

**Minimum context:**
```yaml
cycle:
  tables: ["transactions", "customers"]
  relationships:
    - from: transactions.Customer name
      to: customers.Customer name
      type: foreign_key
      cardinality: many_to_one
```

**Rich context (recommended):**
```yaml
cycle:
  tables: ["transactions", "customers"]
  relationships:
    - from: transactions.Customer name
      to: customers.Customer name
      type: foreign_key
      cardinality: many_to_one

  table_semantics:
    transactions:
      role: "central_transaction_table"
      key_columns:
        - "Customer name" (customer_identifier)
        - "A/R paid" (payment_status)
        - "Transaction type" (transaction_classifier)
    customers:
      role: "master_data"
      key_columns:
        - "Customer name" (primary_identifier)
        - "Balance" (financial_metric)

  transaction_characteristics:
    - has_ar_columns: true
    - has_sale_indicator: true
    - links_to_payment_methods: true
```

### Question 3: How to handle ambiguous cycles?

**Scenario:** Cycle `[transactions, customers, transactions]` could be:
- AR cycle (if A/R columns involved)
- Revenue cycle (if Sale columns involved)
- Both (most likely)

**Options:**
1. **Single classification**: Pick most likely based on evidence
2. **Multi-label**: Return `["accounts_receivable_cycle", "revenue_cycle"]` with confidences
3. **Hierarchical**: "revenue_cycle" contains "accounts_receivable_cycle"

**Recommendation:** Multi-label with confidence scores

---

## Proposed Config Extension

### `config/domains/financial.yaml` additions

```yaml
# ===================================================================
# CROSS-TABLE CYCLE PATTERNS (NEW)
# ===================================================================

cross_table_cycle_patterns:
  accounts_receivable_cycle:
    description: "AR collection cycle spanning customer and transaction tables"

    # Table patterns (order matters for cycle matching)
    table_patterns:
      - ["*transaction*", "*customer*"]
      - ["*invoice*", "*customer*", "*payment*"]
      - ["*order*", "*customer*", "*receipt*"]

    # Column indicators (any table in cycle)
    column_indicators:
      required:
        - pattern: "A/R|receivable|ar_"
          weight: 0.4
      supporting:
        - pattern: "customer|client"
          weight: 0.2
        - pattern: "invoice|bill"
          weight: 0.2
        - pattern: "payment|receipt"
          weight: 0.2

    # Relationship characteristics
    relationship_hints:
      - cardinality: "many_to_one"  # transactions → customers
      - from_table_pattern: "*transaction*"
      - to_table_pattern: "*customer*"

    business_value: "high"

  expense_cycle:
    description: "AP/Expense cycle spanning vendor and transaction tables"

    table_patterns:
      - ["*transaction*", "*vendor*"]
      - ["*purchase*", "*vendor*", "*payment*"]
      - ["*bill*", "*vendor*", "*disbursement*"]

    column_indicators:
      required:
        - pattern: "A/P|payable|ap_|vendor"
          weight: 0.4
      supporting:
        - pattern: "purchase|expense"
          weight: 0.2
        - pattern: "bill|invoice"
          weight: 0.2
        - pattern: "payment|disbursement"
          weight: 0.2

    relationship_hints:
      - cardinality: "many_to_one"
      - from_table_pattern: "*transaction*"
      - to_table_pattern: "*vendor*"

    business_value: "high"

  inventory_cycle:
    description: "Inventory flow from purchase through sale"

    table_patterns:
      - ["*transaction*", "*product*"]
      - ["*inventory*", "*product*", "*sale*"]

    column_indicators:
      required:
        - pattern: "product|item|sku|inventory"
          weight: 0.4
      supporting:
        - pattern: "quantity|qty"
          weight: 0.2
        - pattern: "cost|cogs"
          weight: 0.2

    business_value: "medium"
```

---

## Testing Strategy

### Test Scenario 1: Finance CSV Example

```python
@pytest.mark.asyncio
async def test_business_cycle_detection_finance_example():
    """Test that AR and AP cycles are detected in finance_csv_example data."""

    # Setup: Load tables, create relationships
    table_ids = ["transactions", "customers", "vendors", "products", "chart_of_accounts"]

    # Create relationships (these would normally come from TDA/semantic enrichment)
    relationships = [
        # transactions → customers (FK via Customer name)
        Relationship(from_table="transactions", to_table="customers",
                    from_column="Customer name", to_column="Customer name",
                    relationship_type="foreign_key", cardinality="many_to_one"),

        # transactions → vendors (FK via Vendor name)
        Relationship(from_table="transactions", to_table="vendors",
                    from_column="Vendor name", to_column="Vendor name",
                    relationship_type="foreign_key", cardinality="many_to_one"),

        # Reverse relationships to create cycles
        Relationship(from_table="customers", to_table="transactions",
                    from_column="Customer name", to_column="Customer name",
                    relationship_type="foreign_key", cardinality="one_to_many"),

        Relationship(from_table="vendors", to_table="transactions",
                    from_column="Vendor name", to_column="Vendor name",
                    relationship_type="foreign_key", cardinality="one_to_many"),
    ]

    # Act: Run analysis
    result = await analyze_complete_financial_dataset_quality(
        table_ids=table_ids,
        duckdb_conn=conn,
        session=session,
        llm_service=mock_llm_service,
    )

    # Assert: Cycles detected
    assert result.success
    data = result.value

    # Should detect at least 2 cross-table cycles
    cross_table_cycles = data["cross_table_cycles"]
    assert len(cross_table_cycles) >= 2

    # Should classify AR cycle
    ar_cycles = [c for c in data["classified_cycles"]
                 if c["cycle_type"] == "accounts_receivable_cycle"]
    assert len(ar_cycles) >= 1
    assert ar_cycles[0]["confidence"] > 0.7

    # Should classify AP/Expense cycle
    ap_cycles = [c for c in data["classified_cycles"]
                 if c["cycle_type"] == "expense_cycle"]
    assert len(ap_cycles) >= 1
```

### Test Scenario 2: No Cycles (Star Schema)

```python
@pytest.mark.asyncio
async def test_no_cycles_star_schema():
    """Test that pure star schema (no cycles) is handled gracefully."""

    # Setup: Fact table with dimension tables, no reverse relationships
    # fact → dim1, fact → dim2, fact → dim3 (no cycles)

    result = await analyze_complete_financial_dataset_quality(...)

    assert result.success
    data = result.value

    # No cross-table cycles
    assert len(data["cross_table_cycles"]) == 0

    # LLM interpretation should note missing expected cycles
    assert "no business cycles detected" in data["interpretation"]["summary"].lower()
```

### Test Scenario 3: Complex Multi-Hop Cycle

```python
@pytest.mark.asyncio
async def test_complex_revenue_cycle():
    """Test detection of complex cycle: customer → order → invoice → payment → customer."""

    # Setup: 4-table cycle
    tables = ["customers", "orders", "invoices", "payments"]
    relationships = [
        ("customers", "orders"),
        ("orders", "invoices"),
        ("invoices", "payments"),
        ("payments", "customers"),  # Closes the cycle
    ]

    result = await analyze_complete_financial_dataset_quality(...)

    # Should detect 4-table cycle
    cycles = data["cross_table_cycles"]
    complex_cycle = [c for c in cycles if len(c["tables"]) == 4]
    assert len(complex_cycle) >= 1

    # Should classify as revenue_cycle
    classified = data["classified_cycles"]
    revenue = [c for c in classified if c["cycle_type"] == "revenue_cycle"]
    assert len(revenue) >= 1
```

---

## Implementation Phases

### Phase 1: Multi-Table Orchestrator Foundation

**File:** `quality/domains/financial_orchestrator.py`

**New function:** `analyze_complete_financial_dataset_quality()`

```python
async def analyze_complete_financial_dataset_quality(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_service: LLMService | None = None,
) -> Result[DatasetFinancialQualityResult]:
    """
    Complete financial quality analysis for multi-table dataset.

    Layers:
    1. Per-table accounting checks (existing)
    2. Relationship gathering (reuse gather_relationships)
    3. Cross-table cycle detection (reuse analyze_relationship_graph)
    4. LLM business cycle classification (new)
    5. LLM holistic interpretation (new)
    """
```

### Phase 2: Cross-Table Cycle Classification

**File:** `quality/domains/financial_llm.py`

**New function:** `classify_cross_table_cycle_with_llm()`

```python
async def classify_cross_table_cycle_with_llm(
    cycle_tables: list[str],
    relationships: list[EnrichedRelationship],
    table_semantics: dict[str, dict],  # From semantic enrichment
    column_samples: dict[str, list],   # Sample values for context
    llm_service: LLMService,
) -> Result[CrossTableCycleClassification]:
    """
    Classify a cross-table cycle as a business process.

    LLM receives:
    - Tables involved in cycle
    - Relationships connecting them
    - Semantic roles of key columns
    - Sample data for context
    - Config patterns as vocabulary

    Returns:
    - cycle_type: "accounts_receivable_cycle", "expense_cycle", etc.
    - confidence: 0-1
    - explanation: Why this classification
    - business_value: high/medium/low
    - completeness: Is the cycle complete or missing tables?
    """
```

### Phase 3: Config Extension

**File:** `config/domains/financial.yaml`

Add `cross_table_cycle_patterns` section (as shown above).

### Phase 4: Integration with Synthesis

**File:** `quality/synthesis.py`

Update `assess_dataset_quality()` to use new multi-table orchestrator.

### Phase 5: Relationship Package Extraction (Future)

**New package:** `dataraum_context.relationships` or separate repo

Extract and consolidate:
- `enrichment/tda/relationship_finder.py`
- `enrichment/cross_table_multicollinearity.py:gather_relationships()`
- `quality/topological.py:load_table_relationships()`
- `quality/topological.py:analyze_relationship_graph()`

---

## Decisions (Finalized 2025-12-13)

1. **LLM Classification Scope**: **Option A (cross-table only)**
   - Single-table cycles = data quality indicators (not classified as business processes)
   - Cross-table cycles = business processes (AR, AP, Revenue, Expense, etc.)

2. **Multi-Label Classification**: **Yes**
   - A cycle can be both "accounts_receivable_cycle" AND "revenue_cycle"
   - Return list of classifications with individual confidence scores

3. **Config vs LLM Balance**: **Config as vocabulary**
   - LLM always makes the final classification decision
   - Config provides context/vocabulary (pattern descriptions, expected relationships)
   - No hard-coded pattern matching rules

4. **Relationship Package Priority**: **Extract in last phase**
   - First: Implement multi-table analysis using existing code locations
   - Last: Extract relationship utilities to dedicated package for reuse

---

## Success Criteria

1. **Detection**: Correctly identify AR and AP cycles in finance_csv_example
2. **Classification**: LLM assigns correct business cycle types with >0.7 confidence
3. **Completeness**: Detect missing expected cycles (e.g., no payroll cycle)
4. **Performance**: Analysis completes in <30s for 7 tables, <100 relationships
5. **Separation**: Clear distinction between single-table quality and cross-table cycles

---

## Next Steps

### Pre-requisite: Fix Existing Integration
- [x] Fix `pipeline.py` to pass `llm_service` to `assess_dataset_quality()`

### Implementation Order
1. [x] Review and approve this plan
2. [x] Decide on open questions (LLM scope, multi-label, config balance)
3. [x] **Phase 1**: Multi-table orchestrator foundation (2025-12-13)
   - New function: `analyze_complete_financial_dataset_quality()`
   - Reuse: `gather_relationships()`, `analyze_relationship_graph()`
   - File: `quality/domains/financial_orchestrator.py`
4. [x] **Phase 2**: Cross-table cycle classification (2025-12-13)
   - New function: `classify_cross_table_cycle_with_llm()`
   - Multi-label support with confidence scores
   - File: `quality/domains/financial_orchestrator.py`
5. [x] **Phase 3**: Config extension (2025-12-13)
   - Added `cross_table_cycle_patterns` section to `financial.yaml`
   - 6 cross-table patterns: AR, AP, Revenue, Inventory, Payroll, Chart of Accounts
6. [x] **Phase 4**: Test fixtures for finance_csv_example (2025-12-13)
   - Created `tests/quality/test_multi_table_business_cycles.py`
   - 12 tests covering: graph cycle detection, star schema, 3-table cycles, LLM classification
   - Tests pass with realistic fixtures (star schema = no cycles, 3-table cycle = detected)
7. [x] **Phase 5**: Integration with synthesis (2025-12-13)
   - Updated `assess_dataset_quality()` in `synthesis.py` to call `analyze_complete_financial_dataset_quality()`
   - Cross-table cycles added to dataset issues
   - Business process interpretation added to dataset summary
   - All 146 quality tests pass
8. [ ] **Phase 6**: Relationship package extraction (future cleanup)
   - Optional: Extract relationship utilities to dedicated package for reuse
   - Candidates: `gather_relationships()`, `analyze_relationship_graph()`, `load_table_relationships()`
