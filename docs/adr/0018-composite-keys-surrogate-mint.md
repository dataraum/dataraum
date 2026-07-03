# ADR-0018 — Composite keys are cured at the source by a surrogate-key mint

- **Status:** Accepted
- **Date:** 2026-07-03
- **Ticket:** DAT-277 (epic DAT-652)
- **Reference:** dbt's surrogate-key pattern (hash of the key columns); plan + validation record on the DAT-277 ticket

## Context

The relationship model is a single column pair (`from_column_id → to_column_id`). Real
datasets carry **composite** foreign keys — a referencing column plus one or more scoping
columns present on both sides, canonically a tenant key: BookSQL's
`master_txn.account → chart_of_accounts.account_name` is only meaningful together with
`business_id`. A single-column model cannot express this, with three measured consequences:

- The best single-column join is **many-to-many** — aggregates over it silently
  over-count (the fan-trap).
- The scoping column links every table to every table on its own, so its standalone
  candidates are degenerate and the LLM elevates them **inconsistently** across runs
  (BookSQL: 20 `business_id ↔ business_id` pairs, a different survivor each run).
- The SQL agents learn to **avoid the join** and ground on weaker single-table
  discriminators.

A first cut (`refactor/dat-277-composite-key-rescue`, PR #394, parked) expressed the
composite downstream: relationship groups (`relationship_group_id`/`key_position`) and a
multi-column `ON` clause in the view builder. It worked, but every single-column consumer
needed gating or threading (anchor recovery, column-name collisions, suppression gaps),
and the complexity grew with each consumer. The parked branch's own conclusion: the
composite is a symptom to cure at the source, not to rescue at every reader.

## Decision

**A confirmed composite key is fused into ONE deterministic hash column on both typed
tables, and the catalog persists ONE ordinary single-column relationship on that pair.
No consumer ever sees a multi-column key.**

1. **Detection is evidence, the LLM is the judge.** A greedy pre-pass
   (`analysis/relationships/composite.py`) probes each fan-out candidate: anchor on the
   strongest pair, fuse the co-present pair that most reduces join multiplicity, accept
   only when the composite's measured cardinality collapses out of many-to-many — else
   abstain (a genuine bridge can never satisfy the criterion). A hit is attached to the
   `semantic_per_table` candidate feed as a `composite_key` hint; the LLM confirms via
   `RelationshipOutput.key_columns`. Nothing is auto-created from statistics.

2. **A confirmed composite never enters the catalog as a half-key.** It persists as a
   `surrogate_key_intents` row (run-versioned, digest-keyed for retry idempotency). The
   plain single-column persist path is byte-identical when no composite is confirmed.

3. **The `surrogate_mint` phase** (begin_session, after the teach overlays, before
   `enriched_views`) cures each intent at the source:
   - Both typed tables are re-materialized with the hash column by **wrapping the typing
     recipe's DDL** on the ADR-0010/DAT-414 substrate (emit → store → execute; steady
     state executes nothing). The expression is **NULL-propagating**
     (`md5(a::VARCHAR || '|' || b …)`): any NULL component yields a NULL surrogate, so a
     LEFT JOIN misses — FK semantics, deliberately not dbt's NULL placeholder, which
     would false-join NULL↔NULL.
   - The column name is deterministic in the component set (`_sk__<components>`, anchor
     first, scope sorted), and the `Column` row is upserted by `(table_id, column_name)`
     — so `column_id` is stable across runs and teach/keeper overlays keyed on it hold.
   - ONE relationship persists on the surrogate pair, **FK-side-first by measured
     cardinality** (a dim→fact confirmation flips to fact→dim many-to-one), with the
     natural→surrogate provenance in `evidence.surrogate`.
   - Reconcile owns the `_sk__*` namespace: a surrogate neither re-confirmed nor still
     referenced by the promoted/kept catalog (the keeper grace window) is dropped,
     physical and metadata.

4. **Guardrails make the worst case "no column minted", never a wrong join.** The mint
   abstains on: divergent component types (native `=` coerces, `'007' = 7`; the hash
   compares VARCHAR renderings, `'7' ≠ '007'` — a same-type pair is provably
   equivalent, a divergent pair is not), float components (equal DOUBLEs can render
   differently: `-0.0`/`0.0`), a composite whose measured cardinality does **not**
   collapse (the LLM confirmed against the data — the flagged single-column anchor
   persists instead), a vanished component (the pair aborts as a unit), and a missing
   typing recipe. A DuckLake commit race propagates as a retryable failure (DAT-641
   pattern) rather than a silent miss.

## Consequences

- The begin_session chain is 14 phases; typed tables can carry engine-minted `_sk__*`
  columns (VARCHAR, profiled, reconciled by the mint). The catalog invariant — every
  defined relationship is a single column pair — now also holds for composite-keyed data.
- `views/builder.py`, the enrichment agent, cycles, lineage, validation, and both SQL
  agents are unchanged: they consume the surrogate as an ordinary many-to-one FK.
- Validated on BookSQL (7 tables, 810k-row fact): four `(name, business_id)` composites
  mint stably every run, the enriched view joins them grain-verified, and
  `gl_invoice_match` gained a join path. The chart-of-accounts composite is **refused**
  — BookSQL's dual-role accounts (same name, both Income and Expenses within one
  business) make a row-grain FK impossible by construction; the honest outcome is the
  flagged fan-trap plus set-grain semi-joins at answer time.
- Deploying a workflow-chain change: an in-flight begin_session run fails Temporal replay
  determinism on the new code and needs a re-run (true of every phase addition).
- Search-quality follow-up (the greedy finds local optima; misses are safe but real) is
  DAT-679, evaluation-first.

## Alternatives rejected

- **Multi-column ON at the consumers** (the parked branch): every single-column consumer
  pays — group storage, anchor recovery, collision suffixing, suppression gating. The
  complexity compounds per consumer instead of being paid once at the source.
- **Hash expression inside the view SQL only:** the catalog still cannot express the key,
  so the answer agent still sees a many-to-many natural edge and the flaky elevation
  persists — the multi-column rescue in disguise.
- **Fixing direction handling in the enrichment agent** instead of orienting at the mint:
  changes behavior for every natural relationship (a consumer-side blast radius) to fix a
  producer-side convention.
- **Deriving a missing key component semantically** (e.g. inferring an account's side
  from credit/debit): an unreliable per-row heuristic pre-pass; where the fact does not
  carry the discriminator, the honest verdict is the fan-trap flag, and interpretation
  belongs to the agent at answer time.
