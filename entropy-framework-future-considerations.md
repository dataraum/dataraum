# Entropy Framework: Future Considerations

> Reference document for UX, workflow, and API design decisions. These concepts should inform the user experience layer built on top of the core entropy calculation modules.

---

## 1. Entropy Lifecycle & Workflow

Entropy is not a one-time measurement but a continuous process.

### Measurement Timing by Dimension

| Timing | Dimensions |
|--------|------------|
| **Ingestion time** | Source (physical, provenance), Structural (schema, types) |
| **Transformation time** | Structural (relations), some Semantic |
| **Configuration time** | Semantic (definitions, units), Computational (formulas, rules) |
| **Query time** | Query (intent, linguistics), dynamic Semantic disambiguation |
| **Continuous/scheduled** | Value (drift), Source (freshness), all (periodic re-scan) |

### State Transitions

```
Unknown → Detected → Acknowledged → Resolved
                  ↓
            Deferred (accepted uncertainty)
```

### Re-evaluation Triggers

- Schema change detected
- Value distribution shift (statistical drift)
- Configuration update by user
- Time-based refresh (freshness SLA)
- Query failure or low-confidence response
- User feedback (thumbs down, correction)

---

## 2. Dimension Dependencies

Entropy dimensions are not independent. Some must be resolved before others become meaningful.

### Prerequisite Chain

```
Physical → Structural → Semantic → Computational → Query
   ↓           ↓            ↓            ↓
(can't read) (can't map) (can't interpret) (can't derive)
```

### Dangerous Combinations (Compounding Risk)

| Combination | Risk Level | Impact |
|-------------|------------|--------|
| High Semantic.Units + High Computational.Aggregations | Critical | Summing unknown currencies with unknown rollup rules |
| High Structural.Relations + High Computational.Filters | High | Wrong joins with implicit exclusions |
| High Value.Nulls + High Computational.Aggregations | High | Silent exclusion or inclusion errors |
| High Semantic.Temporal + High Query.Intent | Medium | Wrong time periods applied silently |

### Resolution Cascades

Some fixes resolve multiple entropy dimensions:

- Renaming `c_amt` → `credit_amount_eur`: fixes Schema, Units, Business Meaning
- Adding foreign key constraint: fixes Relations, Cardinality, Referential Integrity
- Creating semantic view: fixes Join Paths, Filters, potentially Aggregations

**UX Implication**: Show users the cascade effect of resolutions ("This fix will also improve X and Y").

---

## 3. Data Readiness Contracts

Different use cases tolerate different entropy levels.

### Suggested Profiles

| Use Case | Source | Structural | Semantic | Value | Computational | Query |
|----------|--------|------------|----------|-------|---------------|-------|
| **Regulatory reporting** | <0.1 | <0.1 | <0.1 | <0.1 | <0.1 | <0.2 |
| **Executive dashboard** | <0.2 | <0.2 | <0.2 | <0.3 | <0.2 | <0.4 |
| **Operational analytics** | <0.3 | <0.2 | <0.3 | <0.4 | <0.3 | <0.5 |
| **Ad-hoc exploration** | <0.4 | <0.3 | <0.5 | <0.5 | <0.5 | <0.6 |
| **Data science/ML** | <0.3 | <0.2 | <0.5 | <0.3 | <0.4 | N/A |

### Contract Workflow

1. User selects use case or defines custom profile
2. System evaluates current entropy against contract
3. Shows compliance status per dimension
4. Prioritizes resolution hints based on contract gaps

**UX Implication**: "Readiness dashboard" showing compliance by use case.

---

## 4. Resolution Timing & Authority

### Resolution Timing Classification

| Category | When | Example Dimensions |
|----------|------|-------------------|
| **Must resolve in config** | Before any queries | Structural, Source.Physical, Computational |
| **Can resolve at query time** | During user interaction | Query.Intent, Query.Linguistics, some Semantic |
| **Cannot fully resolve** | Must surface uncertainty | Value.Outliers, Source.Provenance, ambiguous rules |

### Resolution Authority Matrix

| Resolution Type | Authority | Persistence |
|-----------------|-----------|-------------|
| Add column alias | Data Engineer | Permanent |
| Define business term | Business Owner + Data Steward | Permanent |
| Set aggregation rules | Domain Owner (Finance, etc.) | Permanent |
| Declare canonical join | Data Architect | Permanent |
| Accept assumption for query | Query User | Per-query |
| Promote assumption to rule | Data Steward | Permanent |

### Audit Trail Requirements

Every resolution should track:
- Who made the decision
- When
- What evidence supported it
- What was the previous state
- Approval chain (if applicable)

**UX Implication**: Resolution workflow with approval steps for permanent changes; lightweight acceptance for per-query assumptions.

---

## 5. Query-Time Behavior

How should the query agent handle different entropy states?

### Decision Matrix

| Entropy Level | Deterministic | Agent Behavior |
|---------------|---------------|----------------|
| Low (<0.3) | Yes | Answer confidently |
| Medium (0.3-0.6) | Partial | Answer with stated assumptions |
| High (0.6-0.8) | No | Ask clarifying question OR answer with strong caveat |
| Critical (>0.8) | No | Refuse to answer; explain what needs resolution |

### Assumption Handling

```
Query: "Show me revenue by region"

If Semantic.Units entropy = 0.5:
  "Revenue by region (assuming EUR based on system default—
   some entities may report in local currency)"

If Query.Intent entropy = 0.6:
  "For what time period? Options: YTD, Last 12 months, All time"
```

**UX Implication**: Configurable behavior per user/role—some users want assumptions, others want questions.

---

## 6. Feedback & Learning

The system should improve over time.

### Feedback Signals

| Signal | What It Indicates | Action |
|--------|-------------------|--------|
| Query success rate | Overall system health | Prioritize low-success areas |
| User clarification patterns | Missing synonyms/defaults | Suggest new mappings |
| Assumption acceptance rate | Inference quality | Validate or revise heuristics |
| Resolution effectiveness | Hint quality | Improve prioritization |
| Thumbs down on response | Specific failure | Flag for review |

### Learning Opportunities

- **Synonym discovery**: User types "turnover" → system asks "did you mean revenue?" → user confirms → add to synonyms
- **Default inference**: 80% of users specify "last 12 months" → suggest as default
- **Pattern recognition**: Similar column names across tables → suggest consistent treatment

**UX Implication**: "Suggestions" panel showing learnings that need human approval.

---

## 7. API & MCP Server Design Considerations

### API Layers

```
Layer 1: Core Entropy (foundation - build first)
├── Detector modules (one per sub-dimension)
├── Entropy object serialization
├── Score calculation
└── Storage interface

Layer 2: Analysis & Aggregation
├── Cross-dimension analysis
├── Dependency checking
├── Contract evaluation
└── Resolution hint generation

Layer 3: Query Integration (MCP Server)
├── Context generation for query agents
├── Real-time entropy lookup
├── Assumption tracking
└── Clarification flow

Layer 4: Workflow & UX
├── Resolution workflow API
├── Approval flows
├── Feedback collection
├── Learning/suggestion engine
```

### MCP Server Endpoints (Future)

```
Tools:
- get_entropy_context(tables, columns, query?) → EntropyContext
- get_resolution_hints(target, max_hints?) → ResolutionHint[]
- accept_assumption(entropy_id, assumption) → Confirmation
- submit_feedback(query_id, feedback_type) → void

Resources:
- entropy://table/{table_name} → Full entropy profile
- entropy://column/{table}.{column} → Column entropy
- entropy://contract/{use_case} → Readiness status
```

---

## 8. Open Questions for UX Design

1. **How much entropy detail to surface to end users vs. analysts vs. admins?**
   - End users: Just confidence level + caveats
   - Analysts: Dimension breakdown + assumptions
   - Admins: Full entropy objects + resolution hints

2. **Push vs. pull for resolution hints?**
   - Push: Notify when entropy exceeds threshold
   - Pull: On-demand when user requests

3. **How to handle conflicting resolutions?**
   - User A defines "margin" as gross, User B as net
   - Need conflict detection + resolution workflow

4. **Versioning of entropy state?**
   - Track entropy over time for trend analysis
   - "Data quality improved 15% this quarter"

5. **Integration with existing data catalogs?**
   - Import definitions from existing glossaries
   - Export entropy metadata to external systems

---

## Summary: Build Sequence

### Phase 1: Foundation (Current Focus)
- [ ] Core detector modules (per dimension)
- [ ] Entropy object model
- [ ] YAML configuration schema
- [ ] Score calculation
- [ ] Storage interface
- [ ] Manual testing harness

### Phase 2: Analysis Layer
- [ ] Aggregation and rollups
- [ ] Dependency analysis
- [ ] Contract evaluation
- [ ] Resolution hint generation

### Phase 3: Query Integration
- [ ] Context generator for LLM agents
- [ ] MCP Server implementation
- [ ] Assumption tracking

### Phase 4: Workflow & UX
- [ ] Resolution workflows
- [ ] Approval flows
- [ ] Feedback loops
- [ ] Learning engine

---

*This document should be revisited when beginning Phase 3 and Phase 4 work.*
