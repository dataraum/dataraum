# Scenario C: Concept Mapping with hypothesize

**Tools exercised:** why, hypothesize, run_sql, teach, measure
**Data:** zone1 Stripe data, after Scenario A (5 teachings applied)
**Pre-condition:** The agent has resolved the easy wins (unit, business_name, billing_reason
semantics). Now it wants to go deeper — map amount_due to a canonical financial concept
to activate metric computation.

**Why this scenario exists:** Scenarios A and B skipped hypothesize because why gave
enough confidence for simple teachings. This scenario shows when hypothesize is essential:
the agent needs to preview what a concept mapping unlocks before committing.

---

## Context: Where the investigation stands

After Scenario A:
- amount_due: unit=cents taught, semantic score resolved
- billing_reason: semantics taught as assumption
- description: business_name taught
- Scores: semantic 0.13, computational 0.12

The agent now asks: "amount_due has a unit, but what IS it? Is it gross revenue?
Net revenue? Invoice total? The answer determines which metrics activate and
what cross-table validations become possible."

---

## Step 1: Why suggests a concept mapping

```
CALLS: why(target="stripe_invoices.amount_due", dimension="semantic")
```

**System:** LLM reads evidence for this specific column. Sees: unit is declared (cents),
role is monetary_amount, but no canonical concept mapping. Checks ontology for
matching concepts.

**Response:**

```yaml
target: stripe_invoices.amount_due
dimension: semantic
score: 0.01                         # unit was resolved, but concept is not mapped
analysis: |
  amount_due has a declared unit (cents) and type (BIGINT) but is not mapped
  to a canonical financial concept. The column name and semantic role suggest
  it's an invoice-level monetary amount. Three candidate concepts from the
  finance ontology:

  - **invoice_total** — gross amount including tax. Additive within a period.
  - **revenue_net** — revenue excluding tax. Requires tax subtraction.
  - **amount_receivable** — amount owed, may include non-revenue items.

  The column's relationship to tax and amount_excluding_tax columns may help
  disambiguate. Recommend hypothesizing each candidate to see what it unlocks.
evidence:
  - target: column:amount_due
    dimension: semantic.business_meaning.concept_mapping
    detector: business_meaning
    score: 0.01
    resolution_options:
      - tool: hypothesize
        concept: invoice_total
        description: "Preview mapping to invoice_total. Will show related
          metrics, expected relationships, and validation opportunities."
      - tool: hypothesize
        concept: revenue_net
        description: "Preview mapping to revenue_net. Requires tax subtraction
          — will show whether amount_excluding_tax resolves this."
```

**Key difference from Scenario A:** why doesn't suggest a teach directly. It suggests
**hypothesize** — because concept mapping is complex enough to preview first. The
confidence is `must_validate`.

**Agent reads:** Three candidate concepts. why recommends hypothesizing to see what
each unlocks. Let me try invoice_total first — it matches the column name.

---

## Step 2: hypothesize — what if amount_due is invoice_total?

```
CALLS: hypothesize(
  target="stripe_invoices.amount_due",
  concept="invoice_total"
)
```

**System:** Dispatches on `concept` field. Looks up `invoice_total` in finance ontology.
Finds BBN nodes affected by this mapping. Runs `what_if_analysis`. Loads concept
properties, related metrics, related columns from ontology + snippet library.

**Response:**

```yaml
target: stripe_invoices.amount_due
affected_nodes: [naming_clarity, formula_match, aggregation_safety]
intent_deltas:
  aggregation_intent: +0.18        # "how reliable are aggregations" improves
  reporting_intent: +0.12          # "how suitable for reports" improves
  query_intent: +0.15              # "how well can queries use this" improves
readiness_before: investigate
readiness_after: ready
confidence: must_validate

concept_properties:
  name: invoice_total
  temporal_behavior: additive      # can be summed across periods
  role: measure
  unit_from_concept: currency      # unit comes from a currency column
  includes_tax: true               # this is the gross amount
  typical_relationships:
    - tax_amount: "tax column, same row"
    - net_amount: "amount_excluding_tax column, same row"
    - expected_identity: "net_amount + tax_amount = invoice_total"

related_metrics:
  - id: gross_revenue
    formula: "SUM(invoice_total) WHERE billing_reason = 'subscription_cycle'"
    resolved_inputs:
      invoice_total: stripe_invoices.amount_due   # this mapping
      billing_reason: stripe_invoices.billing_reason  # already taught
    status: fully_resolved           # all inputs available
  - id: net_revenue
    formula: "SUM(invoice_total - tax_amount)"
    resolved_inputs:
      invoice_total: stripe_invoices.amount_due
      tax_amount: null               # not yet mapped
    status: partially_resolved       # needs tax mapping

related_columns:
  - column: stripe_invoices.tax
    expected_concept: tax_amount
    current_status: unmapped
    note: "If tax maps to tax_amount, the identity check activates"
  - column: stripe_invoices.amount_excluding_tax
    expected_concept: net_amount
    current_status: unmapped

snippets: []                         # no prior snippets for this concept

detector_evidence:
  naming_clarity:
    current_score: 0.01
    predicted_after: 0.0
  formula_match:
    current_score: null              # no formula to check yet
    predicted_after: 0.0             # identity check becomes available
```

**Agent reads:** This is rich. If amount_due is invoice_total:
- It's gross (includes tax). There's an expected identity: net + tax = total.
- gross_revenue metric is fully resolved (all inputs mapped).
- net_revenue metric needs tax_amount mapping.
- The tax and amount_excluding_tax columns are expected related concepts.
- Confidence is `must_validate` — I should check the identity before committing.

**Agent decides:** Check the identity: amount_excluding_tax + tax = amount_due.

---

## Step 3: Validate the identity

```
CALLS: run_sql(
  sql="SELECT
         COUNT(*) AS total_rows,
         COUNT(*) FILTER (
           WHERE amount_excluding_tax + tax = amount_due
         ) AS identity_holds,
         COUNT(*) FILTER (
           WHERE ABS(amount_excluding_tax + tax - amount_due) <= 3
         ) AS identity_within_rounding
       FROM stripe_invoices
       WHERE amount_excluding_tax IS NOT NULL AND tax IS NOT NULL"
)
```

```yaml
columns: [total_rows, identity_holds, identity_within_rounding]
rows:
  - [29, 26, 29]
```

**Agent reads:** 29 rows have both columns. The exact identity holds for 26 (90%).
All 29 hold within rounding tolerance of 3 cents. The 3 mismatches are the same
rounding issue from Scenario A (already accepted as teach-004).

**Agent decides:** Identity confirmed within rounding. amount_due IS invoice_total.
The rounding tolerance is already documented. Let me also verify the gross_revenue
metric resolves correctly.

---

## Step 4: Verify the metric

```
CALLS: run_sql(
  sql="SELECT currency,
       ROUND(SUM(amount_due) / 100.0, 2) AS gross_revenue
       FROM stripe_invoices
       WHERE billing_reason = 'subscription_cycle'
       GROUP BY currency
       ORDER BY gross_revenue DESC"
)
```

```yaml
columns: [currency, gross_revenue]
rows:
  - [EUR, 39500.00]
  - [USD, 8500.00]
  - [CHF, 3500.00]
```

**Agent reads:** gross_revenue metric resolves cleanly. The numbers make sense
relative to the MRR figures from Scenario B (MRR was EUR 43,850 which included
normalized yearly contracts — gross_revenue of subscription_cycle only is lower
as expected).

**Agent decides:** Validated. Teach the concept mapping. Also teach the related
columns while I'm at it — the identity confirms their roles.

---

## Step 5: Teach the concept mapping (batch)

The agent teaches three things, then measures once (per Session Protocol pattern).

```
CALLS: teach(
  type="concept_property",
  target="stripe_invoices.amount_due",
  params={
    maps_to: "invoice_total",
    temporal_behavior: "additive",
    includes_tax: true
  },
  confidence=0.93,
  evidence=["hypothesize-001", "run_sql-identity-check", "run_sql-metric-verify"]
)
```

```yaml
status: taught
type: concept_property
target: column:stripe_invoices.amount_due
teaching_id: teach-009
measurement_hint: "Concept mapped. Call measure() to see impact on semantic.business_meaning and computational.formula_match."
```

```
CALLS: teach(
  type="concept_property",
  target="stripe_invoices.tax",
  params={
    maps_to: "tax_amount",
    relates_to: "stripe_invoices.amount_due"
  },
  confidence=0.93,
  evidence=["hypothesize-001", "run_sql-identity-check"]
)
```

```yaml
status: taught
type: concept_property
target: column:stripe_invoices.tax
teaching_id: teach-010
```

```
CALLS: teach(
  type="concept_property",
  target="stripe_invoices.amount_excluding_tax",
  params={
    maps_to: "net_amount",
    relates_to: "stripe_invoices.amount_due"
  },
  confidence=0.93,
  evidence=["hypothesize-001", "run_sql-identity-check"]
)
```

```yaml
status: taught
type: concept_property
target: column:stripe_invoices.amount_excluding_tax
teaching_id: teach-011
```

**Agent reads:** Three concept mappings taught. All share the same evidence chain
(the hypothesize result + identity check + metric verification).

---

## Step 6: Measure once

```
CALLS: measure(target="stripe_invoices")
```

**System:** Detects 3 new teachings. Re-runs affected phases. Detectors verify
the teachings against actual data: is the identity consistent? Does the concept
mapping match the observed distribution, type, and cardinality?

```yaml
target: stripe_invoices
status: complete
points:
  - { target: "column:amount_due", dimension: "semantic.business_meaning.naming_clarity", score: 0.0 }
  - { target: "column:amount_due", dimension: "semantic.business_meaning.concept_mapping", score: 0.0 }
  - { target: "column:tax", dimension: "semantic.business_meaning.concept_mapping", score: 0.0 }
  - { target: "column:amount_excluding_tax", dimension: "semantic.business_meaning.concept_mapping", score: 0.0 }
  - { target: "table:stripe_invoices", dimension: "computational.consistency.formula_match", score: 0.03 }
  # ...
scores:
  structural: 0.08
  semantic: 0.05                    # was 0.13 — three concepts resolved
  value: 0.12
  temporal: 0.04
  computational: 0.05              # was 0.12 — formula identity verified by detector
  source: 0.03
readiness:
  amount_due: ready
  tax: ready                       # was not flagged but now confirmed
  amount_excluding_tax: ready
  billing_reason: ready
  description: investigate          # still unresolved (free text)
```

**Agent reads:** Semantic 0.13 → 0.05. Computational 0.12 → 0.05. The concept
mappings resolved three columns at once. The formula_match detector verified
the net + tax = total identity and scored it at 0.03 (near-perfect, slight
rounding variance). The detector didn't just take the agent's word — it checked.

---

## End state

| Metric | Before Scenario C | After |
|--------|------------------|-------|
| semantic | 0.13 | 0.05 |
| computational | 0.12 | 0.05 |
| Columns with concept mapping | 0 | 3 (amount_due, tax, amount_excluding_tax) |
| Metrics activated | 0 | 1 (gross_revenue fully resolved) |

## The hypothesize → teach flow

```
why                 → "here's what's wrong, consider these concepts"
hypothesize         → "if you map to invoice_total, here's what changes:
                       these intents improve, these metrics activate,
                       these columns are expected, this identity should hold"
run_sql (validate)  → "the identity holds, the metric resolves"
teach (batch)       → three concept mappings in one go
measure (once)      → detectors verify against data, scores drop
```

**hypothesize is the pivotal step.** Without it, the agent would need to:
1. Manually look up the ontology to find invoice_total's properties
2. Manually figure out which metrics would activate
3. Manually identify related columns (tax, amount_excluding_tax)
4. Manually construct the validation SQL

hypothesize packages all of that into one call: concept properties, related metrics
with resolution status, related columns, expected relationships, and confidence level.
The agent gets a complete picture and knows exactly what to validate.

## Observations

### hypothesize is essential for concept mappings

For simple teachings (unit=cents, business_name="Invoice Description"), why gives
enough context. But concept mappings unlock cascading effects:
- Metric graphs activate
- Related columns become discoverable
- Identity relationships emerge
- Cross-table validations become possible

Only hypothesize previews these cascading effects before the agent commits.

### The `must_validate` signal drives investigation depth

hypothesize returns `confidence: must_validate` for concept mappings. This tells the
agent: "the BBN prediction is directionally correct but you MUST check against data."
The agent responds by running validation SQL. For high-confidence predictions
(e.g., `hypothesize(teach_type="concept_property")` for a unit declaration),
the agent could skip validation.

### "Teach three, measure once" is more efficient

Steps 5: three teach calls. Step 6: one measure call. The agent batches knowledge
provision and then verifies in one shot. This is cheaper than the old fix-then-measure
pattern (which measured after each fix).

### Detectors verify teachings

Step 6: the formula_match detector independently checks the net + tax = total
identity. It scores 0.03 (not 0.0) because of the rounding variance. The detector
doesn't trust the agent's claim — it verifies against data. This is the Goodhart
firewall in action: agents provide knowledge, detectors check it.

### The learning flywheel starts

hypothesize returned no snippets (empty library for this concept). After the agent
ran validation SQL in steps 3 and 4, those queries are in the session trace. If
promoted to snippets, the NEXT investigation of Stripe data would see them in
hypothesize's response:

```yaml
snippets:
  - sql: "SELECT ... WHERE amount_excluding_tax + tax = amount_due"
    description: "Invoice total identity check"
    source: "session:sess-001"
  - sql: "SELECT currency, SUM(amount_due)/100.0 FROM ... GROUP BY currency"
    description: "Gross revenue by currency"
    source: "session:sess-001"
```

### Open questions

- **Multiple concept candidates:** Step 1 suggests three candidates (invoice_total,
  revenue_net, amount_receivable). The agent picked one and hypothesized it. Should
  it hypothesize multiple candidates and compare? The hypothesize design doc's
  open question #1 asks about chained hypotheses.
- **Concept mapping vs assumption:** Scenario A taught billing_reason semantics as
  an assumption. Scenario C teaches amount_due → invoice_total as a concept_property.
  The distinction: assumptions are free-text knowledge, concept_property mappings
  connect to the ontology and activate metric graphs. Is this clear enough for agents?
- **teach type alignment:** The Session Protocol uses `concept_property` as the unified
  teach type for column properties (unit, maps_to, temporal_behavior). The overview
  has separate types (unit, business_name, etc.). These should align — `concept_property`
  as the type, with the specific property as a field.
