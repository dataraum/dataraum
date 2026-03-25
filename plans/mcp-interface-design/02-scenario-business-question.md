# Scenario B: "What's our MRR?" (Job 1 → Job 2 bridge)

**Tools exercised:** begin_session, query, teach (decision + assumption), run_sql, measure, why
**Data:** Same zone1 Stripe data, after Scenario A's investigation
**Pre-condition:** Scenario A completed — 5 teachings applied, semantic at 0.13

---

## Step 1: Start a session

```
CALLS: begin_session(
  contract="executive_dashboard",
  intent="Monthly recurring revenue analysis from Stripe data"
)
```

**System:** Checks registered sources, cached scores, known issues.

```yaml
session_id: sess-001
sources: [stripe]
cached_scores:
  stripe_invoices:
    structural: 0.08
    semantic: 0.13
    value: 0.12
    temporal: 0.04
    computational: 0.12
    source: 0.03
known_issues:
  - target: column:description
    dimension: semantic
    status: unresolved
feasibility: feasible
```

**Does NOT return:** full schema (that's look), evidence (that's why).

**Agent reads:** Scores exist from previous investigation. Feasibility is good.
One unresolved column (description) — doesn't block MRR calculation.

**Agent decides:** Proceed with the business question.

---

## Step 2: Ask the business question

```
CALLS: query(question="What is our monthly recurring revenue?")
```

**System:** query is an LLM agent. It:
1. Parses the question, identifies relevant columns (amount_due, currency,
   billing_reason, status, created)
2. Pulls context: teachings (teach-001: unit=cents, teach-002: billing_reason
   semantics), semantic annotations, BBN readiness
3. Generates SQL incorporating what it knows (divides by 100, filters to
   subscription_cycle based on teach-002's assumption)
4. Executes SQL, evaluates result
5. Classifies into decisions_made vs open_questions
6. Detects teachable decisions (calculation rules that should generalize)

```yaml
answer:
  summary: "MRR by currency (October 2024)"
  data:
    - { currency: EUR, mrr: 41850.00, subscriptions: 18 }
    - { currency: USD, mrr: 8500.00, subscriptions: 2 }
    - { currency: CHF, mrr: 3500.00, subscriptions: 1 }
  sql: "SELECT currency, ROUND(SUM(amount_due) / 100.0, 2) AS mrr, ..."
decisions_made:
  - "Amounts divided by 100 (unit: cents, from teach-001) — 31 rows"
  - "Excluded billing_reason 'manual' and 'upcoming' (non-recurring, from teach-002) — 4 invoices"
  - "Excluded status='uncollectible' — 1 invoice"
  - "Used subscription_cycle invoices only for recurring (from teach-002)"
open_questions:
  - issue: "3 currencies present. No exchange rates available."
    options: ["Report per-currency (current)", "User provides rates"]
    impact: "Cannot produce unified MRR without FX rates"
  - issue: "1 yearly contract (Acme Corp, 24000/yr). Normalize to monthly?"
    options: ["Include as 2000/mo", "Exclude from MRR"]
    impact: "2000 difference in EUR MRR"
confidence:
  level: medium
  factors:
    - "31 invoices, single billing cycle — may not represent steady state"
    - "description column has unresolved semantic entropy"
```

**Does NOT return:** entropy scores, column profiles, evidence details.

**Agent reads:** MRR answer with 4 auto-applied decisions and 2 open questions.
Decisions reference Scenario A's teachings — teach-001 (unit) and teach-002
(billing_reason semantics) feed directly into the query. The investigation improves
the query.

**Agent presents to user:** "Your MRR is EUR 41,850, USD 8,500, and CHF 3,500.
I excluded 4 non-recurring invoices and 1 uncollectible. Two questions:
should I normalize the yearly Acme contract to monthly? And do you have
exchange rates?"

---

## Step 3: User resolves ambiguity

```
USER: "Yes, normalize yearly to monthly. Report per-currency, no need for unified."
```

```
CALLS: teach(
  type="decision",
  target="stripe_invoices",
  params={
    rule: "Normalize yearly contracts to monthly (divide by 12)",
    scope: "mrr_calculation"
  },
  confidence=1.0,
  evidence=["user_instruction"]
)
```

**System:** Persists as session overlay. Decision type is soft-layer — affects query
agent behavior, not detector scores. Future query calls find this decision and
auto-apply it.

```yaml
status: applied
type: decision
target: table:stripe_invoices
layer: soft
teaching_id: teach-006
```

**Agent reads:** Decision persisted.

**Agent decides:** Re-run query.

---

## Step 4: Query with resolved decision

```
CALLS: query(question="What is our MRR?")
```

**System:** query agent finds teach-006 (decision) in context. Auto-applies yearly
normalization (was open_question, now in decisions_made). Currency question dismissed
by user — not re-asked.

```yaml
answer:
  summary: "MRR by currency (October 2024)"
  data:
    - { currency: EUR, mrr: 43850.00, subscriptions: 19 }
    - { currency: USD, mrr: 8500.00, subscriptions: 2 }
    - { currency: CHF, mrr: 3500.00, subscriptions: 1 }
  sql: "..."
decisions_made:
  - "Amounts divided by 100 (unit: cents, from teach-001) — 31 rows"
  - "Excluded billing_reason 'manual' and 'upcoming' (from teach-002) — 4 invoices"
  - "Excluded status='uncollectible' — 1 invoice"
  - "Used subscription_cycle invoices only (from teach-002)"
  - "Yearly contracts normalized to monthly ÷12 (from teach-006) — 1 invoice (Acme Corp)"
open_questions: []
confidence:
  level: medium
  factors:
    - "31 invoices, single billing cycle"
teachable_decisions:
  - decision: "Yearly contracts normalized to monthly (÷12)"
    suggested_teaching:
      type: assumption
      target: stripe_invoices
      params: { content: "For MRR calculations, normalize yearly contracts to monthly by dividing by 12" }
    reason: "This is a general MRR calculation rule, not specific to this query"
```

**Agent reads:** EUR MRR up 2,000 (Acme normalized). No more open questions.
query flagged the yearly normalization as teachable — it's a general rule.

**Agent presents:** "Updated MRR: EUR 43,850 (includes Acme Corp at EUR 2,000/mo).
USD and CHF unchanged."

**Agent decides:** The teachable suggestion makes sense — generalize the rule.

---

## Step 5: User wants to explore

```
USER: "Break it down by plan type."
```

```
CALLS: run_sql(
  sql="SELECT plan_id, currency,
       ROUND(SUM(amount_due)/100.0, 2) AS revenue,
       COUNT(*) AS invoices
       FROM stripe_invoices
       WHERE billing_reason = 'subscription_cycle'
       GROUP BY 1, 2 ORDER BY 3 DESC"
)
```

```yaml
columns: [plan_id, currency, revenue, invoices]
rows:
  - [plan_enterprise, EUR, 28500.00, 5]
  - [plan_professional, EUR, 12350.00, 8]
  - [plan_starter, EUR, 3000.00, 5]
  - [plan_enterprise, USD, 8500.00, 2]
  - [plan_starter, CHF, 3500.00, 1]
```

Just rows. The agent writes correct SQL because it knows cents and billing_reason
filtering from the prior teachings and query decisions.

---

## Step 6: Teach generalizable rules

Two teachings: one prompted by query's teachable_decisions (step 4), one from
the agent's observation about MRR calculation patterns.

```
CALLS: teach(
  type="assumption",
  target="stripe_invoices",
  params={
    content: "For MRR calculations, normalize yearly contracts to monthly
      by dividing by 12"
  },
  confidence=1.0,
  evidence=["teach-006", "user_confirmed_mrr_normalization"]
)
```

```yaml
status: applied
type: assumption
target: table:stripe_invoices
layer: soft
teaching_id: teach-007
```

```
CALLS: teach(
  type="assumption",
  target="stripe_invoices.billing_reason",
  params={
    content: "For recurring revenue calculations, only 'subscription_cycle'
      invoices count as MRR. 'subscription_create' are initial charges,
      'subscription_update' are prorations, 'manual' are one-time,
      'upcoming' are future estimates. This refines teach-002's general
      semantics into a specific calculation rule."
  },
  confidence=1.0,
  evidence=["teach-002", "teach-006", "query_decisions_made"]
)
```

```yaml
status: applied
type: assumption
target: column:stripe_invoices.billing_reason
layer: soft
teaching_id: teach-008
```

**Agent reads:** Both teachings persisted. teach-007 generalizes the yearly
normalization rule. teach-008 refines the billing_reason semantics into a
specific MRR calculation rule. Future queries inherit both.

---

## Step 7: Verify improvement

```
CALLS: measure(target="stripe_invoices")
```

```yaml
target: stripe_invoices
status: complete
scores:
  structural: 0.08
  semantic: 0.10
  value: 0.12
  temporal: 0.04
  computational: 0.12
  source: 0.03
readiness:
  amount_due: ready
  billing_reason: ready
  description: investigate
```

**Agent reads:** Semantic down to 0.10 (from 0.13). Soft-layer assumptions
enriched the semantic analysis context.

---

## End state

| Metric | Start of Scenario B | End |
|--------|-------------------|-----|
| semantic | 0.13 | 0.10 |
| computational | 0.12 | 0.12 |

| Teaching | Type | Layer |
|----------|------|-------|
| teach-006: yearly normalization (decision) | decision | soft |
| teach-007: yearly normalization (assumption, generalized) | assumption | soft |
| teach-008: billing_reason MRR filtering rule | assumption | soft |

## Observations

### Unified teach simplifies the scenario

Every action is a `teach` call. The agent doesn't deliberate about fix vs teach.
Different types serve different purposes:
- `decision` (step 3): persists a user choice for this query scope
- `assumption` (step 6): generalizes the decision into reusable knowledge

### Job 2 → Job 1 bridge via teachings

Scenario A's teachings feed Scenario B's query directly:
- teach-001 (unit=cents) → "Amounts divided by 100" in decisions_made
- teach-002 (billing_reason semantics) → informs exclusion and filtering logic

The chain: investigation → teach → query → user answers → teach → future queries.
Everything flows through teach.

### decision → assumption promotion is natural

Step 3: `teach(type="decision")` persists a specific user choice.
Step 4: query detects it's teachable (general rule, not query-specific).
Step 6: `teach(type="assumption")` generalizes it.

The promotion happens in the agent's flow, not as a system mechanism.
query's `teachable_decisions` field is the prompt.

### query carries the most intelligence

query does: parse NL, pull all teachings as context, generate SQL accounting for
domain knowledge, execute, classify assumptions, detect teachable patterns.
Everything else is simpler.

### Open questions

- **query context scaling**: query's agent needs all teachings + annotations for
  relevant columns. Pre-agent (haiku-class) for relevance filtering is the mitigation.
  Build telemetry from day one.
- **decision durability**: teach-006 (decision) persists in the session overlay.
  New session next month — does it carry over? Probably: teachings persist in the
  vertical config overlay, decisions persist in the audit ledger. Both available
  to future sessions.
- **teachable_decisions generation**: query's LLM decides what's teachable.
  Heuristic: if the decision is a calculation rule (not data-specific), it's
  teachable. This is an LLM judgment within the query agent.
- **Teaching accumulation**: 8 teachings after 2 scenarios. At scale, the teaching
  overlay grows. Need a mechanism to consolidate/prune (future: report tool could
  summarize teachings, deliver tool could snapshot the overlay).
