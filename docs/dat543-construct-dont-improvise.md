# Feed the agents — deterministic grounding + LLM-authored SQL (DAT-616 reframe)

**Spine.** This is a context engine; the metric path is wrong because we **starve** the SQL agents of metadata we already compute — not because we lack a checker, and **not because we lack a deterministic SQL builder.** The shape space of metric SQL (sum, ratio, end-of-period, window, multi-column, signed measures, CTEs) is open-ended; every fixed builder has died on the tail before. So the LLM authors the SQL — that is the design, not a fallback. The fix is to **feed it the deterministic grounding** it's currently denied, and make that grounding **teach-confirmed and durable**. Deterministic *evidence* + LLM *authoring* + human *teach* — never deterministic authoring.

All file:line verified on this branch.

> **Ticket note:** the "grounding heart" below is **DAT-620** (new, under DAT-543). The real **DAT-581 is an unrelated cross-column dependency sniffer** — earlier notes mislabeled the grounding work as 581.

## 1. The idiom — and its honest boundary

The engine already runs "deterministic stats → narrow LLM decides → emit SQL":
- **enriched_views** — `build_enriched_view_sql` (`analysis/views/builder.py`) is a deterministic builder, but it works **only because its shape is fixed** (one grain-preserving LEFT JOIN). The LLM (`enriched_views_phase.py:_get_llm_recommendations`) only picks dims. sqlglot-gated.
- **slice agent** (`SlicingAgent(LLMFeature)`) — picks dims/values from `top_values`; doesn't author SQL.
- **drivers** (`DriverRankingArtifact`, `analysis/drivers/`) — statistical (variance-reduction, min_support, p-value; precision/recall-calibrated).

**Boundary (the lesson):** a deterministic builder is legitimate for *fixed* shapes (enriched_views) and nothing else. Metrics are not fixed-shape → **the LLM writes the metric SQL.** Do not build `build_extract_sql`. The deterministic part is the *grounding it consumes*, not the SQL it emits.

## 2. The grounding (DAT-620) — the heart, one decision with two faces

Grounding = `concept → (measure expression, predicate/value-set)`. **Both faces are one job (DAT-620), and it is an LLM+stats+teach job, not deterministic:**
- **measure expression** — usually `SUM(amount)`, but may be `SUM(debit) − SUM(credit)`, a sign convention, a ratio (`Σnum/Σden`). Drivers already model this: `Measure{target_type: flow|stock|ratio, column|numerator/denominator}` (`drivers/models.py`), read from the catalog's `temporal_behavior`.
- **predicate/value-set** — which `account_type` values *are* the concept.

Inputs (deterministic evidence, fed to the grounding step):
- `top_values` + `distinct_count` — the **complete enumeration** of the partition column (exhaustive iff `distinct_count ≤ K`).
- drivers `ranked_dimensions` (which dim partitions the measure) + `interesting_slices` (`{dimension,value,effect,support}` — high-signal **hint**, recall<1, NOT the partition) + `measure_column_id` + `target_type`.
- ontology `indicators`/`exclude_patterns` lexicon (`verticals/*/ontology.yaml`).

The grounding is **proposed** by a narrow agent over those inputs, then **teach-confirmed**. (`interesting_slices` is a labeling hint; the labeling itself is the agent+teach step.)

## 3. The binding table — the durable artifact

A standalone per-workspace, run-versioned table: per `concept` a **predicate spec** — a conjunction of per-column labeled value-sets (single-column is the 1-element case) + measure expression + provenance (`lexicon|driver|teach`) + support. Multi-column is native (drivers' `driver_paths`/`DriverNode.children` already give multi-dimension drill vectors; the LLM authors the conjunction).

**Fed by both, at different layers (the "true question", resolved):** the partition *structure* — dimension, values, support, effect — is deterministic from **drivers (DAT-546 today / DAT-573 on-demand) + `top_values`**; the concept *labels* over that structure are **DAT-620 (proposer + teach)**. So **DAT-620 owns the WRITE; drivers feed the evidence each row carries AND the freshness trigger** (a re-run surfacing a new value → re-label). 581 writes, 573 feeds + invalidates.

Properties:
- **Complete:** *every* live value of the partition column carries a label, including explicit `unmapped` — so a concept's `IN`-list can't silently miss a value (no `Direct Materials` undercount).
- **Fall-loud on drift:** a value present in the live column but absent from the confirmed set (re-import) → the dependent metric is **inconclusive-with-reason until labeled**, never computed on a stale partition. (The graph cornerstone steer.)
- The shared grounding artifact for DAT-620/591/611/619/617; "clear what it is for later reference."

## 4. Feed the SQL agents (the actual DAT-616 fix)

Serve the evidence + the confirmed binding into both authors so they **stop improvising the predicate**:
- **Engine GraphAgent** — today starved: `_describe_table` sends `SELECT DISTINCT … LIMIT 5`, no counts (`agent.py:666`); `context.py:810-811` lifts only null/cardinality and **drops `top_values`/`distinct_count`** though the profile is in scope (`:803`); `graphs/context.py` loads **no drivers**. Serve: `top_values` (low-card dims) + `ranked_dimensions` + the binding.
- **Cockpit answer agent** — better fed (`<drivers>` already serves driver slices, `query-context.ts:643`), but `<schema>`/`<dimensions>` strip values. Serve the binding + dimension values the same way.

**How the agent authors — the template, moved into the LLM world (not a deterministic filler, not free improvisation):**
- **Named-context contract.** The prompt names exactly what is available and grounded — measure expression (incl. signed/ratio), the predicate-spec (per-column labeled value-sets), dimension columns, aggregation, the catalog extract intent. The agent fills *these* slots; it never invents a filter.
- **Blueprint library.** The prompt describes the SQL *shape* per pattern class and carries a few-shot exemplar for each: simple aggregate, end-of-period (latest-period CTE), window, multi-column conjunction, ratio (two extracts composed). **Shape coverage lives here** — CTE/window/multi-column are *taught as blueprints*, not hoped for. This is `graph_sql_generation.yaml`'s new job: a named-context contract + a covered blueprint library, replacing the improvise-the-filter instruction.

This keeps the neat part of "fill a template" (a known shape, named slots) while putting the composition where it belongs — the LLM — so the open-ended shape tail doesn't break it. The agent writes SQL **from** the binding; it no longer guesses which rows.

## 5. The gatekeeper (your fork) — deterministic routing on confidence, not SQL-shape

Since the LLM always authors, the gate is not builder-vs-agent. It is a deterministic AND of trust signals deciding **auto-accept vs teach/editor**:
- **grounding confidence** — binding teach-confirmed (auto) vs agent-proposed-only (review);
- **execution** — runs vs `BinderException` (DAT-611's deterministic escalate→editable trigger);
- **structural check** — the emitted predicate's column+literals match the confirmed binding; referenced columns exist (sqlglot, already in-tree: `core/sql_normalize.py`, `entropy/measurements/derived_value.py`).

Confirmed binding + structural-match + executes → accept. Otherwise → `SqlEditor` / teach. The editor overwrite *is* a teaching.

## 6. Verification — reframed, mostly gone

- **context-free extract** (`concept+statement+aggregation`): structural check vs ontology (concept known, aggregation matches stock/flow). Deterministic, no data — the only legitimate "verify".
- **grounding**: the trust boundary — teach-confirmed + drivers-evidenced + fall-loud. Not verified as an LLM output.
- **emitted SQL**: structural match to the binding (§5), not a value-space judge.
- **Keep the metric `validation:` checks as a cheap post-execution sanity floor (KEPT).** They're a one-number comparison on the step's already-aggregated scalar — `StepResult.value` = `result[0]` (`execution.py:155`, the step's `… AS value`) — evaluated by a Python-ast safe-evaluator (`verifier.py`); **SQL-untouched, in-memory-trivial, orthogonal to the grounding fix.** These are the metric-graph's own per-extract bounds (`loader.py` → `GraphStep.validations`), NOT the unrelated operating_model `ValidationAgent`. What was wrong in #369 was *framing* them (+ the NULL-support gate) as the honest-fail gate — they're blind to wrong-non-empty. So **keep `verifier.py` (value-bound + NULL floor) as sanity; it is NOT the fix.**
- **Scalar LLM judge: out** (re-certifies the generator's own blind spot). **Execution self-consistency: deferred, not rejected — it is the *eventual* best validation for this problem.** Pre-feed it entrenches the modal-wrong binding (votes on a starved guess); *post-feed*, K well-grounded samples agreeing on the executed result is real confidence and disagreement flags residual grounding ambiguity. A later phase **after** the feed+binding lands — not now. Salvage from #369 = the de-financed prompt **+ validations-as-sanity**, minus the honest-fail-gate framing.

## 7. Re-scope

**Build order: DAT-620 FIRST (eval-driven) → 616 (feed + blueprint prompt) → 573/591/611/619/617.** The grounding is the heart and the risk, so it leads and is proven by eval before the rest builds on it.

- **DAT-620 (first, eval-driven)** = the grounding (§2) — concept→(measure expression, predicate/value-set), both faces; the drivers+lexicon **proposer** agent + teach; writes the predicate-spec binding table. **Built from the eval side: a long-format fixture with ground-truth concept→value labels; the proposer's precision/recall against teach is the gate, not an afterthought** (cross-repo: dataraum-eval owns the fixture + oracle labels). Keystone.
- **DAT-616** = feed the agents (§4): serve `top_values`/`ranked_dimensions`/binding into the GraphAgent; rewrite `graph_sql_generation.yaml` into the named-context contract + blueprint library; **keep `verifier.py` as a cheap value-bound + NULL-support sanity floor** (re-framed from #369's "honest-fail gate" — it's sanity, not the fix); keep de-financed prompt. **Salvage from #369 = de-financed prompt + the verifier-as-sanity (+ its loader/model wiring).** The metric stops improvising because it's *fed*, not because of the verifier.
- **DAT-573** = on-demand drivers — the proven stats source feeding §2; serve into the GraphAgent (loads none today).
- **DAT-591/611** = cockpit drill — deterministic metric→column edge + AST GROUP-BY rewrite + Haiku-on-`BinderException` + `SqlEditor`. Consumes the binding; downstream end of the same pipe. `json_serialize_sql` is the right tool *here* (manipulate existing SQL), not in the engine.
- **DAT-619** = snippets = verified-skill store; entry gated on teach-confirmed-or-structural-pass, not blind `failure_count==0`.
- **DAT-617** = consumer; rides the above.

## 8. Honest unproven / open forks

- **Unproven:** DAT-620 grounding quality (it's the hard heart — past deterministic attempts failed here); `top_values` truncation at `distinct_count > K` (full-DISTINCT pull or refuse-to-ground); the completeness/fall-loud invariant is per-column-clean for **conjunctions** but murky for arbitrary **OR-combinations across columns** (cross-product can't be enumerated) — that boolean-shape edge is the real one to watch, NOT column count.
- **Settled:** labeling uses the drivers+lexicon **proposer** agent (proposes, teach confirms) — not teach-only. Binding = `concept → predicate spec` (conjunction of per-column value-sets), multi-column native, no single-dim limitation.
- **Forks for the lead:** (1) binding persistence schema — exact predicate-spec shape + how the measure expression (incl. signed/ratio) is stored. (2) structural-check lives once (engine) or twice (engine+cockpit). (3) channel precedence on disagreement: teach(human) > drivers(signal) > top_values(enumeration); union vs confirmed-set as the structural-check oracle.
