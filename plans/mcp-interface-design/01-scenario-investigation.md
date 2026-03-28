# Scenario A: Investigate Stripe Invoices

**Tools exercised:** look, measure (polling), why (agent-powered), run_sql, teach, hypothesize
**Data:** zone1 testdata (Stripe invoices CSV, 31 rows, 22 columns)
**Pre-condition:** `add_source(name="stripe", path="/data/stripe_invoices.csv")` already done.

---

## Step 1: What am I working with?

```
CALLS: look()
```

**System:** Reads registered sources. For each table: TypeProfile, StatisticsResult,
SemanticAnnotation, RelationshipResult. No pipeline trigger — reads whatever has been
computed. If nothing computed yet, returns raw staging info (column names from CSV header,
row count from staging table).

**Response:**

```yaml
sources:
  - name: stripe
    tables:
      - name: stripe_invoices
        row_count: 31
        columns:
          - name: amount_due
            raw_type: VARCHAR
            inferred_type: BIGINT
            null_rate: 0.0
            cardinality: 22
            semantic:
              role: monetary_amount
              business_name: "Invoice Amount"
              unit: null
          - name: currency
            raw_type: VARCHAR
            inferred_type: VARCHAR
            null_rate: 0.0
            cardinality: 3
            semantic:
              role: currency_code
              business_name: "Currency"
          - name: description
            raw_type: VARCHAR
            inferred_type: VARCHAR
            null_rate: 0.06
            cardinality: 28
            semantic:
              role: description
              business_name: null
          - name: billing_reason
            raw_type: VARCHAR
            inferred_type: VARCHAR
            null_rate: 0.0
            cardinality: 5
            semantic:
              role: null
              business_name: null
          - name: status
            raw_type: VARCHAR
            inferred_type: VARCHAR
            null_rate: 0.0
            cardinality: 4
            semantic:
              role: status_flag
              business_name: "Invoice Status"
          - name: created
            raw_type: VARCHAR
            inferred_type: BIGINT
            null_rate: 0.0
            cardinality: 31
            semantic:
              role: timestamp
              business_name: "Created Date"
          # ... (16 more columns)
        relationships:
          - from: subscription
            to: stripe_subscriptions.id
            type: many_to_one
            confidence: 0.92
```

**Does NOT return:** entropy scores, readiness, quality signals.

**Agent reads:** 31 rows, 22 columns. amount_due is BIGINT with role "monetary_amount"
but no unit. description has nulls and no business_name. billing_reason has no semantic
role despite only 5 values. Relationship to stripe_subscriptions exists.

**Agent decides:** I need entropy scores. Call measure to trigger the pipeline.

---

## Step 2a: Trigger measurement (pipeline starts)

```
CALLS: measure(target="stripe_invoices")
```

**System:** DagResolver checks what's been computed. Nothing yet. Triggers full pipeline.
Blocks up to ~25s. Typing and statistics complete within that window.

**Response:**

```yaml
target: stripe_invoices
status: running
phases_completed: [staging, typing, statistics]
phases_remaining: [relationships, semantic, temporal, slicing, correlations, entropy]
points:
  - { target: "column:amount_due", dimension: "structural.types.type_fidelity", score: 0.02 }
  - { target: "column:created", dimension: "structural.types.type_fidelity", score: 0.05 }
  - { target: "column:description", dimension: "structural.types.type_fidelity", score: 0.0 }
readiness: null
```

**Agent reads:** Pipeline running. Structural scores available — type_fidelity is low
(good). Semantic, value, computational not yet available.

**Agent decides:** Structural looks fine. While waiting, inspect the data.

---

## Step 2b: Explore while waiting

```
CALLS: run_sql(
  sql="SELECT billing_reason, COUNT(*) as n
       FROM stripe_invoices GROUP BY 1 ORDER BY 2 DESC"
)
```

```yaml
columns: [billing_reason, n]
rows:
  - [subscription_create, 12]
  - [subscription_cycle, 10]
  - [subscription_update, 5]
  - [manual, 3]
  - [upcoming, 1]
```

```
CALLS: run_sql(
  sql="SELECT amount_due, currency FROM stripe_invoices ORDER BY amount_due DESC LIMIT 5"
)
```

```yaml
columns: [amount_due, currency]
rows:
  - [2500000, EUR]
  - [2400000, EUR]
  - [1200000, USD]
  - [850000, EUR]
  - [500000, CHF]
```

**Agent reads:** billing_reason has clear event types. Amounts are large integers —
definitely cents (2500000 = 25,000.00).

**Agent decides:** Check if measure has more results.

---

## Step 2c: Poll for complete results

```
CALLS: measure(target="stripe_invoices")
```

**Response:**

```yaml
target: stripe_invoices
status: complete
points:
  - { target: "column:amount_due", dimension: "structural.types.type_fidelity", score: 0.02 }
  - { target: "column:created", dimension: "structural.types.type_fidelity", score: 0.05 }
  - { target: "column:description", dimension: "semantic.business_meaning.naming_clarity", score: 0.41 }
  - { target: "column:billing_reason", dimension: "semantic.business_meaning.naming_clarity", score: 0.22 }
  - { target: "column:amount_due", dimension: "semantic.units.unit_declaration", score: 0.13 }
  - { target: "column:amount_due", dimension: "value.outliers.outlier_rate", score: 0.08 }
  - { target: "column:tax", dimension: "computational.consistency.arithmetic", score: 0.22 }
  - { target: "table:stripe_invoices", dimension: "computational.reconciliation.cross_table", score: 0.09 }
scores:
  structural: 0.08
  semantic: 0.31
  value: 0.12
  temporal: 0.04
  computational: 0.15
  source: 0.03
readiness:
  amount_due: investigate
  currency: ready
  description: investigate
  billing_reason: investigate
  status: ready
  created: ready
```

**Agent reads:** semantic (0.31) highest. Three columns flagged "investigate":
amount_due (unit), description (no business name), billing_reason (no role).
Tax has arithmetic issues (0.22).

**Agent decides:** Get analysis from why on the semantic dimension.

---

## Step 3: Why is semantic entropy high? (agent-powered)

```
CALLS: why(target="stripe_invoices", dimension="semantic")
```

**System:** LLM agent reads evidence, BBN state, and the **teach type vocabulary**.
Synthesizes narrative with resolution options that reference specific teach types.

**Response:**

```yaml
target: stripe_invoices
dimension: semantic
score: 0.31
analysis: |
  Semantic entropy is driven by three columns:

  1. **description** (score: 0.41) — Free text with 28 unique values
     in 31 rows. No concept mapping possible. Document as a
     description field — won't eliminate entropy (free text is
     inherently ambiguous) but records the semantic role.

  2. **billing_reason** (score: 0.22) — Categorical with 5 values:
     subscription_create (12), subscription_cycle (10),
     subscription_update (5), manual (3), upcoming (1). Three of five
     map to billing event type concepts. Teaching the system what
     these values mean reduces ambiguity for both measurement and
     future queries.

  3. **amount_due** (score: 0.13) — BIGINT with no unit declaration.
     Values consistent with cents (Stripe convention). Declaring
     the unit resolves this entirely.

  Recommended order: amount_due (quick, high confidence) →
  billing_reason (high impact) → description (accept residual entropy).
evidence:
  - target: column:amount_due
    dimension: semantic.units.unit_declaration
    detector: unit_entropy
    score: 0.13
    resolution_options:
      - teach_type: concept_property
        params: { unit: "cents" }
        description: "Declare unit. Detectors re-evaluate on next measure."
        bbn_impact: -0.08
  - target: column:billing_reason
    dimension: semantic.business_meaning.naming_clarity
    detector: business_meaning
    score: 0.22
    resolution_options:
      - teach_type: assumption
        params:
          content: "billing_reason is a billing event type:
            subscription_create = initial charge,
            subscription_cycle = recurring charge,
            subscription_update = proration,
            manual = one-time, upcoming = future estimate"
        description: "Teach value semantics. Reduces ambiguity for
          measurement and enriches context for query agent."
        bbn_impact: -0.15
  - target: column:description
    dimension: semantic.business_meaning.naming_clarity
    detector: business_meaning
    score: 0.41
    resolution_options:
      - teach_type: concept_property
        params: { business_name: "Invoice Description" }
        description: "Document semantic role. Free text retains
          residual entropy but the role is recorded."
        bbn_impact: -0.18
```

**Agent reads:** Three contributors with clear resolution paths — all are `teach`
calls with different types. amount_due is quick (hard-layer unit declaration),
billing_reason is high impact (soft-layer assumption), description is documentation
(hard-layer business name). Each resolution option specifies the exact teach type
and params.

**Agent decides:** Follow the recommended order. Start with amount_due.

---

## Step 4: Teach the unit

```
CALLS: teach(
  type="concept_property",
  target="stripe_invoices.amount_due",
  params={ unit: "cents" },
  confidence=0.95,
  evidence=["stripe_api_convention", "bigint_values_no_decimal"]
)
```

**System:** Validates params against concept_property schema — `unit` is a known
property. Routes to config interpreter. Writes to typing config overlay.
BBN predicts impact.

```yaml
status: taught
type: concept_property
target: column:stripe_invoices.amount_due
teaching_id: teach-001
measurement_hint: "Unit declared. Call measure() to see impact on semantic.units."
```

**Agent reads:** Teaching applied. Measurement hint says to check semantic.units.

**Agent decides:** Teach billing_reason semantics next. Will measure after batching.

---

## Step 5: Teach billing_reason semantics

```
CALLS: teach(
  type="assumption",
  target="stripe_invoices.billing_reason",
  params={
    content: "billing_reason is a billing event type enum:
      subscription_create = new subscription initial charge,
      subscription_cycle = recurring billing cycle charge,
      subscription_update = mid-cycle proration or plan change,
      manual = one-time manual charge (non-recurring),
      upcoming = scheduled future charge (not yet collected)"
  },
  confidence=0.90,
  evidence=["5_distinct_values_inspected", "naming_matches_billing_events"]
)
```

```yaml
status: taught
type: assumption
target: column:stripe_invoices.billing_reason
teaching_id: teach-002
measurement_hint: "Assumption taught. Detectors will use this context on next measure()."
```

**Agent decides:** Document description's business name. Then measure once for all three.

---

## Step 6: Teach description's business name

```
CALLS: teach(
  type="concept_property",
  target="stripe_invoices.description",
  params={ business_name: "Invoice Description" },
  confidence=0.90,
  evidence=["free_text_28_unique_values", "description_role_inferred"]
)
```

```yaml
status: taught
type: concept_property
target: column:stripe_invoices.description
teaching_id: teach-003
measurement_hint: "Business name set. Call measure() to see impact on semantic.business_meaning."
```

**Agent reads:** Three teachings applied (teach-001, 002, 003). Time to measure once.

---

## Step 7: Ground truth after teachings

```
CALLS: measure(target="stripe_invoices")
```

**System:** Detects teachings newer than last PipelineRun. Hard-layer teachings
(teach-001, teach-003) trigger re-run of affected phases. Soft-layer teaching
(teach-002) enriches context but doesn't require phase re-run.

```yaml
target: stripe_invoices
status: complete
points:
  - { target: "column:description", dimension: "semantic.business_meaning.naming_clarity", score: 0.22 }
  - { target: "column:billing_reason", dimension: "semantic.business_meaning.naming_clarity", score: 0.06 }
  - { target: "column:amount_due", dimension: "semantic.units.unit_declaration", score: 0.01 }
  - { target: "column:tax", dimension: "computational.consistency.arithmetic", score: 0.22 }
scores:
  structural: 0.08
  semantic: 0.13
  value: 0.12
  temporal: 0.04
  computational: 0.15
  source: 0.03
readiness:
  amount_due: ready
  billing_reason: ready
  description: investigate
```

**Agent reads:** Semantic 0.31 → 0.13. billing_reason 0.22 → 0.06 (soft-layer
assumption improved measurement context). amount_due 0.13 → 0.01 (hard-layer
unit declaration). description 0.41 → 0.22 (business name helped, residual from
free text). Computational still 0.15.

**Agent decides:** Check computational entropy.

---

## Step 8: Why is computational entropy elevated?

```
CALLS: why(target="stripe_invoices", dimension="computational")
```

**Response:**

```yaml
target: stripe_invoices
dimension: computational
score: 0.15
analysis: |
  Two issues:

  1. **tax arithmetic** (score: 0.22) — For 3 of 31 invoices,
     tax + amount_excluding_tax ≠ amount_due. Deltas are 0.01–0.03
     cents, consistent with Stripe's cent-level rounding.

     You can accept this as expected behavior. Note: acceptance
     records understanding but doesn't change the score.

  2. **cross-table reconciliation** (score: 0.09) — Invoice amounts
     not validated against subscription plan amounts.

     Suggested verification:
     ```sql
     SELECT i.subscription, i.amount_due, s.plan_amount,
            i.amount_due - s.plan_amount AS delta
     FROM stripe_invoices i
     JOIN stripe_subscriptions s ON i.subscription = s.id
     WHERE i.billing_reason = 'subscription_cycle'
     ```
     If validated, teach it as a reusable validation snippet.
evidence:
  - target: column:tax
    dimension: computational.consistency.arithmetic
    detector: computational_consistency
    score: 0.22
    resolution_options:
      - teach_type: acceptance
        params: { reason: "Stripe cent-level rounding, max delta 0.03" }
        layer: neutral
        description: "Accept as expected. Score unchanged — records understanding."
        bbn_impact: 0.0
  - target: table:stripe_invoices
    dimension: computational.reconciliation.cross_table
    detector: reconciliation
    score: 0.09
    resolution_options:
      - tool: run_sql
        sql: "SELECT i.subscription, i.amount_due, s.plan_amount ..."
        description: "Verify cross-table consistency first."
      - teach_type: validation
        params:
          content: "Cycle invoices match subscription plan amounts"
          sql: "SELECT ... HAVING delta != 0"
        layer: soft
        description: "After verification, teach as reusable validation.
          SQL promoted to snippet library."
```

**Agent reads:** Tax is rounding (accept it). Cross-table needs SQL verification
then can be taught as a validation.

**Agent decides:** Accept rounding, verify cross-table, teach the validation.

---

## Step 9: Accept rounding + verify cross-table

```
CALLS: teach(
  type="acceptance",
  target="stripe_invoices.tax",
  params={ reason: "Stripe cent-level rounding, max delta 0.03 cents" },
  confidence=0.95,
  evidence=["3_of_31_rows", "max_delta_003_cents"]
)
```

```yaml
status: taught
type: acceptance
target: column:stripe_invoices.tax
teaching_id: teach-004
measurement_hint: "Acceptance recorded. Score unchanged — issue documented as expected."
```

```
CALLS: run_sql(
  sql="SELECT i.subscription, i.amount_due, s.plan_amount,
       i.amount_due - s.plan_amount AS delta
       FROM stripe_invoices i
       JOIN stripe_subscriptions s ON i.subscription = s.id
       WHERE i.billing_reason = 'subscription_cycle'
       ORDER BY ABS(delta) DESC"
)
```

```yaml
columns: [subscription, amount_due, plan_amount, delta]
rows:
  - [sub_001, 2500000, 2500000, 0]
  - [sub_002, 1200000, 1200000, 0]
  - [sub_003, 850000, 850000, 0]
  # ... (all deltas are 0)
```

**Agent reads:** Acceptance recorded. Cross-table: perfect match on cycle invoices.

**Agent decides:** Validation passed. Teach it for reuse.

---

## Step 10: Teach the validation

```
CALLS: teach(
  type="validation",
  target="stripe_invoices",
  params={
    content: "Cycle invoices (billing_reason='subscription_cycle') match
      subscription plan amounts exactly on subscription_id join.",
    sql: "SELECT i.subscription, i.amount_due, s.plan_amount,
          i.amount_due - s.plan_amount AS delta
          FROM stripe_invoices i
          JOIN stripe_subscriptions s ON i.subscription = s.id
          WHERE i.billing_reason = 'subscription_cycle'
          HAVING delta != 0"
  },
  confidence=0.95,
  evidence=["10_of_10_cycle_invoices_match", "run_sql_step_9"]
)
```

```yaml
status: taught
type: validation
target: table:stripe_invoices
teaching_id: teach-005
snippet_promoted: true
measurement_hint: "Validation taught. SQL promoted to snippet library."
```

**Agent reads:** Validation taught, SQL promoted to snippet library. Future
investigations discover and re-run this validation.

---

## Step 11: Final measurement

```
CALLS: measure(target="stripe_invoices")
```

```yaml
target: stripe_invoices
status: complete
scores:
  structural: 0.08
  semantic: 0.13
  value: 0.12
  temporal: 0.04
  computational: 0.12
  source: 0.03
readiness:
  amount_due: ready
  billing_reason: ready
  description: investigate
  tax: ready
```

---

## End state

| Metric | Before | After |
|--------|--------|-------|
| semantic | 0.31 | 0.13 |
| computational | 0.15 | 0.12 |

| Teaching | Type | Effect |
|----------|------|--------|
| teach-001: amount_due unit=cents | concept_property | Detectors re-evaluated, score dropped |
| teach-002: billing_reason semantics | assumption | Enriched query and detector context |
| teach-003: description business name | concept_property | Detectors re-evaluated, score dropped |
| teach-004: tax rounding accepted | acceptance | Understanding recorded, score unchanged |
| teach-005: cross-table validation | validation | SQL promoted to snippet library |

## Tool usage summary

| Tool | Calls | Purpose |
|------|-------|---------|
| look | 1 | Entry point — schema overview |
| measure | 4 | Trigger pipeline (2a), poll (2c), post-teach (7, 11) |
| why | 2 | Semantic analysis (3), computational analysis (8) |
| run_sql | 3 | Inspect values (2b×2), verify cross-table (9) |
| teach | 5 | concept_property ×2, assumption, acceptance, validation |
| hypothesize | 0 | Not needed — why provided enough confidence to act |

## Observations

### One tool for all knowledge

Every resolution action is `teach` with a type. The agent never decides between
fix and teach — there's only teach. `concept_property` covers column facts (unit,
business name). `assumption` covers domain knowledge. `acceptance` records
understanding. `validation` creates reusable SQL checks.

### "Teach three, measure once"

Steps 4-6: three teach calls. Step 7: one measure call. The agent batches knowledge
provision and verifies in one shot — more efficient than the old fix-then-measure
cycle.

### Detectors verify teachings against data

Step 7: billing_reason drops from 0.22 to 0.06 after teach-002 (assumption). The
detector re-evaluates with enriched context — the assumption provides information
the detector uses to reduce ambiguity. But it checks against data: if the assumption
contradicted the observed values, the score wouldn't improve. This is the Goodhart
firewall: agents provide knowledge, detectors verify it.

### why suggests teach for simple cases, hypothesize for complex ones

Steps 4-6: why suggested teach calls directly — the decisions were simple enough.
For concept mappings with cascading effects (metric activation, related columns,
identities), why suggests hypothesize first. See Scenario C.

### Snippets as compare/validate infrastructure

Step 10: verify with run_sql → teach as validation → SQL in snippet library →
future reuse. Compare/validate emerge from usage patterns.

### Open questions

- **teach params validation**: Each type has a schema. The agent gets valid params
  from why's suggestions. What if the agent constructs a teach call without why?
  teach should validate and return clear errors.
- **Snippet discovery in why**: Could why check the snippet library and suggest
  existing validation snippets? "A validation for this exists: run it via run_sql."
- **concept_property scope**: `concept_property` covers many different properties
  (unit, business_name, maps_to, role, type). Should params validation differ by
  property name? E.g., `unit` must be a string, `maps_to` must reference a known
  concept in the ontology.
