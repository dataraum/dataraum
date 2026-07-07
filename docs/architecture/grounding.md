# Grounding

How SQL that answers a business question is authored, trusted, and judged —
across the engine (metrics) and the cockpit (Q&A).

## The LLM authors the SQL; the evidence feed is deterministic

- Two agent surfaces author SQL: the **engine graph agent** (metric graphs,
  worker-side — `graphs/agent.py`) and the **cockpit answer agent**
  (natural-language Q&A — `src/tools/query.ts`, a nested sub-agent). Each has
  its own context-assembly path (`graphs/context.py` vs. the cockpit's prompt
  blocks); both follow the same discipline: deterministic evidence in, LLM SQL
  out.
- **There is no deterministic metric-SQL builder — do not build one.** The
  metric-SQL shape space is open-ended; deterministic builders are legitimate
  only for fixed-shape SQL. LLM authoring of extract SQL is the design, not a
  fallback.
- Within a metric graph, only **extract** steps are LLM-authored. **Formula**
  and **constant** steps compose deterministically from the metric DAG
  (`graphs/formula_composer.py`): a closed arithmetic grammar over declared
  dependency steps, unknown operands fail loud, division is NULLIF-guarded.
- The grounding feed carries: value distributions (`top_values` +
  `distinct_count` — a complete enumeration when the distinct count fits;
  numeric min/max so a signed measure is visible), **confirmed concept →
  value-set bindings** ("use as the filter, do not improvise"), the vertical's
  concept vocabulary and driver rankings, **sign/natural-balance conventions
  from the vertical YAML**, and column-reliability markers (per-column
  readiness bands + quality flags from the entropy layer).
- The agent never invents a filter: a predicate is grounded in an enumerated
  value set (bounded catalog value-searches included) or the metric is
  **inconclusive-with-reason** — abstaining beats a plausible wrong number.
- The engine stays domain-agnostic. Vertical vocabulary — convention
  statements, concept groups, indicators — reaches prompts only as opaque
  config strings (`OntologyConvention`): the engine validates the envelope,
  never interprets the content.

## Verification is a sanity floor; confidence flags, never blocks

- The post-execution verifier (`graphs/verifier.py`) judges **support, not
  magnitude**: a NULL aggregate (nothing measured) makes the metric
  inconclusive; a genuine 0 passes. It is structurally blind to
  wrong-but-non-empty SQL — it is a floor, never the correctness gate. The
  gate is the feed.
- Declared value bounds and low grounding confidence **flag the executed
  metric, never refuse it**: the agent records per-concept grounding
  confidence, the weakest input's confidence rides every composed snippet, and
  a low value lands as a visible `state_reason` on a still-executed artifact
  (the cockpit renders it as a caveat badge). Nothing in this path blocks.

## Durable knowledge is SQL, never values

- **Executed metric values are ephemeral.** The durable artifact is the SQL
  snippet (`sql_snippets`), reused across runs and re-run on demand; the
  engine discards the number.
- **Validation verdicts are computed on demand, never stored.**
  `validation_results` is a pure run-versioned SQL store; tolerance and
  severity live in the vertical config, read at every consumer — never
  denormalized onto the row.
- The validation SQL output is **contracted**: one row with a non-negative
  numeric `deviation` (0 = satisfied) and a `magnitude` (the reference scale).
  The verdict is `deviation <= tolerance`; a non-conforming output (no row, no
  numeric `deviation`) is inconclusive — never a fail.
- That two-line rule runs identically in Python
  (`analysis/validation/evaluate.py`) and TS (`src/tools/validation-verdict.ts`),
  pinned by one shared truth table
  (`engine/tests/fixtures/validation_verdict_vectors.json`) asserted in both
  pytest and vitest — drift is a test failure, not a production surprise.

## One SQL parser

- Canonical SQL comparison runs on DuckDB's `json_serialize_sql` in both
  packages (engine `core/sql_normalize.py`, cockpit `src/lib/sql-canonical.ts`)
  — the same parser that executes the SQL. **Do not add another SQL parser.**
