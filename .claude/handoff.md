# Calibration Handoff

Changes in dataraum that need attention in other repos.

Updated by `/implement` in this repo. Read by `/accept` in dataraum-eval.

## 2026-06-11 (DAT-504): lake convergence — db_recipe raw-schema write + eligibility quarantine idempotency

No detector or recall impact — substrate-placement and idempotency fixes only.
`extract_backend` now writes recipe tables into `lake.raw` via
`CREATE OR REPLACE` (they used to land in `lake.main` in production while
metadata claimed layer="raw") and restores the connection's (catalog, schema)
pair after extraction. Eligibility's column drop is now the convergent
rebuild-from-recipe → one-shot `lake.quarantine."quarantine_columns_<bare>"`
replace → drop sequence; lake failures fail the activity instead of degrading
to warnings. Shipped `lake.main` strays are NOT cleaned up (workspaces are
disposable per DD/34045953) — a wiped stack is the clean baseline.

- **Status**: no action needed in eval/testdata

## 2026-06-11 (live smoke): first full clean-corpus journey on main — numbers SUSPECT until DAT-511

The deferred live operating_model smoke ran end-to-end on a wiped stack (main =
DAT-442 #284 + DAT-509 #285): 5 clean finance CSVs → add_source ×5 →
begin_session → operating_model, real LLM throughout. The spine works; the
TB↔GL watcher executed; columns_used fan-out reached the exact TB columns
(cross_table_consistency 1.0 on account_id/period/debit_balance/credit_balance
under the operating_model head); debit_balance/credit_balance resolved
point_in_time UNCONTESTED (witnesses agree on clean data — the contested tag
correctly stays off).

**DO NOT ingest the validation outcomes as calibration signal yet.** The run
surfaced a sequencing bug (DAT-511): the operating model was started ~90s
BEFORE begin_session's detect head promoted (caused by the cockpit narrating
completions one stage early, DAT-510), so validation GROUNDING ran against
pre-promote session state. The clean-leg outcomes — TB↔GL 91.7% match,
GL↔invoice 98.5%, 1 sign-convention violation, 2 date-ordering violations,
and 5/9 metrics failing execution (the income-statement family: Net Income,
EBITDA Margin, Net/Operating Margin, Operating Income) — may be artifacts of
that early grounding rather than engine behavior. Re-run the clean leg after
DAT-511's guard lands; THEN compare against the clean bands. If the noise
reproduces post-fix, the date-ordering count (2) and the metric grounding
failures become real precision findings (the A4 due-date fix was supposed to
zero the former).

**DAT-511 guard landed**: `resolve_operating_model_scope` now fails born-loud
(non-retryable `PhaseFailed`) when the session has linked tables but no promoted
begin_session head — operating_model can no longer ground over a partial
workspace. Eval drivers must await `beginSessionWorkflow` completion before
starting `operatingModelWorkflow` (the sequential runner already does); a driver
that pipelines them will now fail fast instead of producing quiet noise.

- **Status**: pending (DAT-511 merged; smoke rerun owes eval the clean-leg verdict)

## 2026-06-11 (pre-merge sweep): relationship_discovery gaps preserved from LANE-NOTES

LANE-NOTES.md (lane scratch, deleted at merge) was the only record of three
relationship_discovery remainders — preserved here so they don't vanish:

- **Resolve write-back (ADR-0009 piece 5) NOT built — by design, for now.** No
  consumer field exists for the genuineness verdict: `Relationship.is_confirmed`
  is the human flag, not a resolve target; `SemanticAnnotation` is column-grain.
  Candidate surface: a contested/genuineness field on the Relationship row (or
  the cockpit reading the entropy object + `claim_witnesses` directly). Needs a
  ticket when the consumer is designed.
- **Pure-decline focal pairs are structurally unmeasurable.** engine.py
  enumerates `detection_method != 'candidate'` pairs only (the DAT-405
  defined-catalog contract), so "data says strong overlap, the selector said no"
  never becomes a focal pair; on keeper/manual pairs the llm witness ABSTAINS on
  declines (a decline's strength is uncalibrated). Measuring it = focal-pair
  enumeration change + rig calibration of the decline witness.
- **Confirm-overlay human witness:** an explicit `confirm` teach only flips
  `is_confirmed` evidence — it never materializes a row, so a confirm-taught
  pair's human witness is silent (also noted in the eval state-of-the-union).

Ticket hygiene: the join_path two-distinct-FK fixture gap is cited against
DAT-419, which is **Cancelled** — it has no live tracker. LANE-NOTES' two
shared-change requests (relationship-grain witness persistence in engine.py;
`_REL_METHOD_PRECEDENCE` keeper alignment in snapshot.py) were both DONE on
this branch before merge.

- **Status**: pending (ticket filing only — no code change expected)

## 2026-06-11 (wave 2): derived_value score = max(mismatch, identity conflict)

The detection-derived-cal-v1 corpus exposed a silent false negative: under
WHOLESALE divergence (all rows follow formula B while the NAME advertises A)
the best graded formula matches perfectly → the scalar was 0.0 and the column
banded ready, while the pooled name-vs-data conflict (C ≈ 0.8 on the named
claim) rode in evidence only. `obj.score` is now the WORSE of the two honest
statistics — best-graded mismatch rate and the identity conflict — with the
conflict leg behind the same hypothesis-hygiene gate as the scalar (review
wave-1 blocker stays closed). Eval implications: wholesale recall flips green;
corroborated clean columns score the residual pooled conflict (~0.01-0.05,
below every floor); ORDERING_DETECTORS semantics unchanged. Also:
stage_date_ordering sql_hints no longer present due_date as a process stage
(A4 sweep caught the validation LLM flagging the 65% of clean payments that
arrive EARLY on 2 of 4 seeds).

## 2026-06-11: calibration program wave 1 + the TB↔GL watcher (DAT-432/442/444, lanes L2/L3/L9 + L7)

**Four witnessed measurements now** (was two): `relationship_discovery` (lane L2 —
genuineness pool over existing rows: value_overlap data witness + llm/manual/keeper;
also FIXED the silent recall=0: teach-materialized rows shadowed measured RI evidence
in `load_relationship_for_pair`, so the orphan measurement died exactly on taught
pairs) and `derived_value`'s `llm_hypothesis` witness (lane L3 — the
`column_annotation` prompt gains a `derived_formula_hypothesis` field pair;
hypothesis graded over `lake.typed` by the discovery match-rate statistic; `obj.score`
semantics unchanged). Lane L9: `rebind` teach applier (appends the column to the
target concept's `indicators` — steers the grounding LLM's input, never writes
`business_concept`) + an AST guard: every `teach_suggestion` must name an appliable
teach type. ADR-0009 records the 7-piece measurement pack; ALL new numbers are
uncalibrated placeholders in `loss.yaml`/`reliabilities.yaml` pending the batch rig.

**L7 — cross_table_consistency is live end-to-end:** `OperatingModelWorkflow` gains a
terminal `operating_model_detect` right after `validation` (pure scoring, zero LLM);
the dual-grain read views accept the `(session:{id}, "operating_model")` head
(`via_operating_model_head`); `ValidationResultRecord.columns_used` is persisted and
failed checks fan out to COLUMN-grain entropy objects, so bands reach the GL columns
deliverable metrics flow through. NEW finance spec `tb_gl_reconciliation.yaml`
(critical): per-(account, period) TB vs SUM(journal_lines) — none of the nine specs
reconciled TB↔GL before. Score shape: failed CRITICAL = categorical 1.0 (rates put the
injected 10% break at risk 0.08, invisible, while 8/8 GL-derived deliverable numbers
were wrong — the scoreboard finding); ERROR/inconclusive = 0.0 + `validation_unassessed`
warning (was 0.5 = clean-leg false alarms from LLM SQL nondeterminism).

**Calibrate:** the eval runner now drives `operatingModelWorkflow` third;
`cross_table_consistency` is in `CURRENT_SLICE_DETECTORS` + `ORDERING_DETECTORS`;
DAT-444 remap done. The first full batch (clean + detection-v1) validates recall,
clean-leg precision (watch `validation_unassessed` + relationship_discovery quiet),
and whether the outcomes scoreboard moves off 0 right / 0 prevented / 8
wrong-delivered. Deferred: generative families for the three new witness sets
(relationship, formula-divergence, events-backed stock/flow), confirm-overlay human
witness, pure-decline focal pairs.

- **Status**: pending



## 2026-06-10: run-resolved entropy load actually wired — detect path was inert (DAT-491)

**The review-C2 fix shipped in name only and is now real.** `build_for_readiness`
accepted `current_run_id`/`session_id` but dropped both at the `_load_entropy_objects`
call — the run-resolution `load_for_tables` gained for DAT-491 never executed, so
every readiness rollup since that commit still blind-loaded (stale add_source rows
coexisting with session re-adjudications, max-score dedup deciding). Fixed +
regression-pinned, and the QUERY-TIME half is now threaded too:

- `storage.py load_for_tables`: resolution now also triggers on `session_id` alone
  (query time has no in-flight run): session detect head > table heads/legacy.
  Rank 0 is None-guarded — legacy unstamped rows (`run_id IS NULL`) no longer match
  a vacant in-flight slot (`None == None`) and outrank the session head.
- `build_for_readiness` forwards the ids (the inert hop); `build_column_evidence`
  gained `session_id=None` kwarg; `build_for_query` exposes it as public API;
  `graphs/context.py build_execution_context` passes its existing `session_id`
  through — the metrics/agent path now sees the session-head verdict for
  re-adjudicated detectors (temporal_behavior is the first).
- Omitting the ids keeps the legacy blind load everywhere (single-path callers,
  harnesses that skip promote).

**Calibrate:** any eval reading evidence through `build_column_evidence`/
`build_for_query` on a session that ran begin_session should now see the
re-adjudicated temporal_behavior conflict (e.g. debit_balance C≈0.36), not the
stale add_source pair. CAUTION: with `session_id` passed, stamped non-head rows
are DROPPED, not blind-loaded — probes that call `persist_readiness` without
`promote_run` must keep omitting `session_id` (or promote first).
5 new tests in `tests/unit/entropy/test_persist_readiness_scope.py::TestRunResolvedLoad`.

**Review wave-1 fix pass (2026-06-11, fc7a549b) — cockpit-facing:** the dual-grain
read views gained `via_operating_model_head` AND `current_entropy_readiness` now has
latest-promoted-wins precedence between the session-grain heads (detect vs
operating_model) — regenerate the drizzle mirror (`bun run db:pull:metadata`) and
audit unpinned readiness reads (why-table.ts) which previously picked one of two
'current' rows nondeterministically. `validation_results.columns_used` is a new
NOT-NULL JSON column — existing workspaces need the column added before the next
validation run writes.

- **Status**: pending

## 2026-06-09: unit_consistency — measurement #2 on the witness template (DAT-428)

**The generalization test passed at the measurement layer.** `entropy/measurements/
unit_consistency.py` adds the second pooled measurement on the SAME pooling engine —
a new claim space `{consistent, mixed}` + two witness extractors, zero engine-core
change (the DAT-457 promise: "a measurement is a claim space + witness extractors").

- **magnitude_modality** witness: log10|v| bimodality via Pearson's coefficient
  `(skew²+1)/kurtosis` (grounded, the uniform reference 0.555 is the pivot; no boost
  curve). Reads a SCALE mix (kEUR among EUR) as `mixed`, a single scale as `consistent`.
- **declared_unit** witness: the column's claim to one unit (Pint/LLM confidence);
  abstains when none. Conflict is born when magnitude reads MIXED but the unit insists
  SINGLE. 6 unit tests green.

**Scope decision (grounded in mix_units):** the old `mix_units` injector does a ×1.1
CURRENCY mix — a 10% shift is undetectable from values (the unit_entropy misalignment).
So unit_consistency targets SCALE mixes only; the DAT-450 mixed-units family must
inject ×1000-ish scale corruption, not currency.

**Open precision risk to validate:** a clean financial column with both small fees and
large invoices is naturally multimodal in log-magnitude → magnitude witness may read
`mixed` → false conflict. The clean-baseline run is the precision check (must stay
quiet); do NOT pre-tune — measure it. This is the same wall outlier_rate/temporal_drift
hit; the disagreement framing (vs the declared unit) helps but doesn't fully resolve it.

**Next phase (not yet done — the plumbing, needs its own e2e run):** column-scoped
detector (layer SEMANTIC, dimension UNITS, new sub_dimension UNIT_CONSISTENCY) reading
typed values + `load_typing` unit_confidence → measure_unit_consistency → witnessed
EntropyObject; register it and CLEAN-CUT the old single-LLM `unit_entropy` (+ its
loss.yaml entry + eval intent_readiness expectation, per ADR-0009's
declaration[U]/consistency[C] split); the mixed-units scale family (testdata) + a rig
block + a recall test (DAT-450).

**SUPERSEDED (2026-06-11, design v6 kill gate):** the bimodality measurement was
FALSIFIED and cut (149fb379) — the "next phase" above is dead, do NOT build it.
At tip there is no `measurements/unit_consistency.py`, no detector, no config
rows; `unit_entropy` stays the single-witness scalar until a data-grounded
second witness passes the OQ6 entry criterion (every pooled measurement needs a
witness whose input is the data, not the name). DAT-450's mixed-units SCALE
family is obsolete in that form.

## 2026-06-09: witness reliabilities are a CALIBRATED ARTIFACT, not inline constants (DAT-450)

**The placeholder `DEFAULT_RELIABILITIES` are no longer the shipped values.** Per
ADR-0009 reliabilities are "estimated quantities with provenance, never inline
constants." New engine pieces:

- **`dataraum-config/entropy/reliabilities.yaml`** — the shipped artifact:
  `witnesses[measurement_id][witness_id] = r` + a `provenance` block (calibrated
  flag, corpus version, sample size, seed range, held-out Brier, date). Currently
  carries the **measured** null_semantics values from the eval rig.
- **`entropy/reliabilities.py`** — loader (sibling of `loss.py`):
  `get_reliability_config()` / `ReliabilityConfig.for_measurement(id)` /
  `reset_reliability_config_cache()`.
- **`null_token_adjudication.py`** — `load_data` loads the artifact and threads
  `reliabilities=` into `measure_null_semantics`; absent → the measurement's
  neutral fallback. `DEFAULT_RELIABILITIES` reframed as that fallback only.

**Measured values (corpus null_tokens-v2, Laplace accuracy on opinionated votes):**
`quarantine_clustering 0.868`, `null_vocabulary 0.944`, **`type_claim 0.266`**.
Held-out pooled Brier: measured **0.115** < placeholder 0.189 < uniform 0.191 — the
measured weights resolve strictly better than the constants they replace (DAT-450
AC4). The estimator is plain accuracy, chosen by held-out proper scoring (a balanced
variant pooled worse and laundered the next finding — the rig MEASURES, it does not
flatter a witness). v2 adds a disclosed ~20% clustered-decoy minority so
quarantine's false-positive rate enters the estimate; provenance now ships
`per_class_accuracy` (sensitivity/specificity) per witness.

**LIVE-PROVEN end-to-end (2026-06-09).** A real `addSourceWorkflow` over
detection-null-v1 persisted `claim_witnesses` whose `reliability` equals the
shipped values (NOT the `0.8/0.7/0.6` fallback) — the artifact is consumed by the
live pipeline. The adjudication-recall test passes (injected C > clean C on both
injected columns). The live run also surfaced a calibration bug the isolated rig
structurally couldn't: the family's combined marker+decoy ratio must keep
`parse_success ≥ min_confidence` (0.85, `phases/typing.yaml`) — at 16% corruption
`journal_lines.debit` fell to VARCHAR, never quarantined, and the detector was
skipped. The family ratio is now ENFORCED (combined upper bound ≤0.12 raises at
construction; defaults ≤0.10 → parse ≥0.90); both columns resolve DOUBLE and fire.
Verify with `scripts/check_reliability_consumption.py`.

**Findings for witness design (DAT-457)** — two witnesses have **0% specificity**,
now MEASURED (provenance `per_class_accuracy`), not hidden:
- **`type_claim`** votes is-null on everything in `failed_examples[:5]` (it can only
  return 0.5 or `0.5+0.5·psr`, never argues is-value) → it cannot tell a sentinel
  from a genuine-but-unparseable value. `r≈0.27`, per-witness Brier worse than
  always-abstain. If its signal should count, it needs a real is-value path.
- **`quarantine_clustering`** votes is-null on any CLUSTER → on the v2 corpus's
  clustered-decoy minority it mistakes a recurring genuine value for a sentinel
  (specificity 0). Its `r` dropped 0.999→0.868 once that exposure entered the
  estimate (the decoy-clustering stress family, DAT-450 follow-up). It is a
  clustering detector, not a marker/genuine discriminator.

Both are correctly down-weighted by the pool, and conflict C stays weight-robust so
a contested genuine value still fires `investigate` via the vocabulary witness's
dissent. Fixing either is a witness-design change, not a calibration one.

**Re-run:** `python scripts/calibrate_reliabilities.py` in dataraum-eval rewrites
the artifact. Add a new measurement's witnesses to `reliabilities.yaml` + the rig
when its witnesses land (unit/temporal_behavior/concept are still single-LLM).

## 2026-06-08: metric pre-gate REMOVED — metrics ground agentically (DAT-456 fix)

**Supersedes the `can_execute_metric` born-loud-gate claim further below.** A live
operating_model smoke over the 8-table finance set exposed it: **0/12 metrics
grounded, ALL string-match-blocked.** The DAT-456 "born-loud gate" was a
deterministic dict-key check (`FieldMappings.has_mapping`) of each metric's
`standard_field` against the `business_concept` annotation keys, run BEFORE the
graph agent — so a metric needing `revenue` was declared "ungroundable" unless a
column was literally annotated `business_concept == "revenue"`, and the LLM that
would derive revenue from the GL (chart_of_accounts × journal_lines) never ran.
That contradicts the agentic platform.

**Fix:** the pre-gate is deleted (`metrics_phase` no longer calls
`can_execute_metric`). Every parseable metric is composed by the agent; born-loud
lives at the agent (stays `grounded` with the reason when it cannot materialize
runnable SQL) and at snippet materialization (gated on a clean execute, AFTER the
prompt). The only legitimate pre-gate is the parse check (malformed graph →
`declared`). `can_execute_metric` / `resolve_metric_fields` deleted from
`graphs/field_mapping.py`.

**Calibration impact:** the set of metrics reaching execution GROWS back to all
parseable graphs (the opposite of the gate's shrink). `formula_match` (DAT-442)
should calibrate against metrics the AGENT grounds, not a mapping pre-filter.

## 2026-06-08: cockpit cycle teach loop — `teach_cycle` / `look_cycle` / `why_cycle` are live (DAT-465)

**Cockpit-only; NO calibration impact, NO engine change, NO schema change.** Logged
only so a full-loop smoke uses the right tool names. The CYCLE family now has the
same cockpit teach loop validation got in DAT-440/441: `teach_cycle` writes a
`cycle`-typed `config_overlay` row (declare a new cycle by free-form name, or
override a shipped one by name — `_apply_cycle` upsert-replaces into the vertical's
`cycle_types` mapping, DAT-455); the next `operating_model` run grounds + measures
it; `look_cycle` / `why_cycle` read the promoted `current_lifecycle_artifacts`
(`artifact_type='cycle'`) joined with `current_detected_business_cycles` on
`canonical_type`. The declared→grounded→executed states + the visibly-impossible
"not detected in this workspace" reason come straight from `business_cycles_phase`
(DAT-455) — this slice only reads + writes them, it does not change detection or
the `business_cycle_health` inputs DAT-455 already described.

## 2026-06-08: metrics revived through the operating_model lifecycle — `can_execute_metric` is now a born-loud gate (DAT-456)

Branch `feat/dat-456-metrics-lifecycle`. The dormant graph-execution phase is
revived source-free through the DAT-438 typed-artifact lifecycle (the **third**
family after validation + cycles): `graph_execution_phase` → `metrics_phase`,
registered phase/activity name **`metrics`**, slotted into
`OperatingModelWorkflow` after `business_cycles`, before promote. Each declared
metric graph flows declare → **compose** → execute. **`formula_match` calibration
is DAT-442's lane** and lands after this — note (don't wire) that executed
metrics feed the computational `formula_match` detector via derived-formula
metadata; band wiring stays with the eval epic.

What changed that calibration must account for:

- **`can_execute_metric` is now a hard grounding gate, not an advisory.** Before,
  a metric with unresolvable required `standard_field` mappings logged
  `metric_missing_direct_mappings` (INFO) and **still executed** — the graph
  agent inferred SQL over enriched views (a silent best-effort number). Now an
  unmappable required input leaves the metric **`declared` with a `state_reason`**
  (visibly impossible), and it never reaches the LLM. So the set of metrics that
  *execute* shrinks to those whose inputs genuinely ground in the workspace. A
  metric that previously produced a (possibly wrong) value may now be
  blocked-with-reason. This is the born-loud principle applied to the graph.
- **The no-tool-call → JSON-parse fallback in the graph agent is deleted.** When
  the composition LLM returns no tool call, that is a bind ERROR (the metric stays
  `grounded` with the reason) — never a guessed SQL parsed from free text. Mirrors
  DAT-439's validation cut.
- **Field mappings are run-pinned.** `load_semantic_mappings(..., semantic_runs=)`
  pins each table's annotations to its begin_session run (the operating_model
  compose gate threads the pinned base-run map) — multi-run isolation, fail-closed.
- **No schema change.** Unlike validation (`ValidationResultRecord`) and cycles
  (`DetectedBusinessCycle`), the metrics family persists **no new run-versioned
  table**: the only run-versioned metric artifact is the existing
  `LifecycleArtifact` (state + reason), and the durable executable knowledge is
  the **cross-run SQL snippet base** (`sql_snippets`, shared with the query
  agent — NOT run-versioned). The metric value is re-derived by re-running the
  promoted snippet, never snapshotted. `schema.sql` is unchanged.
- **Induction dropped.** `MetricInductionAgent` / `save_metrics_config`
  (`graphs/induction.py`) are DELETED — declares come from the vertical's
  `metrics/` graphs ⊕ `metric` overlay teach rows; no cold-start induction
  (agent-tier boundary).

### dataraum-testdata

The 8-table finance set (invoices, payments, journal_entries/lines,
chart_of_accounts, trial_balance, bank_transactions, fx_rates) is the workspace
where the finance metrics actually compose (DSO, DPO, cash_conversion_cycle,
current_ratio); a thin 2-table workspace leaves most *visibly impossible* (the
adversarial proof). No new ground truth needed for this slice — `formula_match`
recall/precision against the existing derived-formula injections is DAT-442.

## 2026-06-08: cycles revived through the operating_model lifecycle — `business_cycle_health` inputs changed (DAT-455)

Branch `feat/dat-455-cycles-lifecycle`. The dormant business-cycles phase is
revived source-free through the DAT-438 typed-artifact lifecycle (the second
family after validation). **DAT-442's `business_cycle_health` calibration must
land AFTER this** — both *when* cycles exist and *how* they are scoped changed:

- **`DetectedBusinessCycle` is now run-versioned + session-scoped, NOT
  source-scoped.** The `source_id` column (+ its FK and `idx_detected_cycles_source`)
  is GONE; the row now carries `run_id` (NOT NULL) and a
  `uq_detected_cycle_run` UNIQUE on `(session_id, canonical_type, run_id)`.
  `canonical_type` is now NOT NULL (it IS the declared-artifact identity). Any
  eval code that read cycles by `source_id` must read by `(session_id, run_id)`
  at the promoted `operating_model` head instead. Schema changed → `down -v` for
  a fresh workspace.
- **Cycles are now written in the `operating_model` stage, NOT the
  begin_session/add_source `detect` pass.** The `business_cycle_health` detector
  (`entropy/detectors/semantic/business_cycle_health.py`) still runs during the
  *detect* pass, but now resolves the session's PROMOTED `operating_model` head
  and reads cycles at that run — so on a workspace where operating_model has not
  run yet (the common case at detect time) it reads NOTHING and scores 0.0
  (`no_cycles_involving_table`). The detector's `load_data` inputs changed from
  "all cycles for this table's source" to "cycles at the promoted operating_model
  run for this session"; `context.session_id` is now required (returns early
  without it). This is the substrate-generality finding (see PR): the detector's
  cross-stage read is the seam the lifecycle did NOT make seamless.
- **`compute_cycle_health` signature changed**: `compute_cycle_health(session,
  session_id, *, vertical, run_id)` (was `source_id`). `run_id=None` now reads
  NOTHING (both cycles and validation results are run-versioned) and returns an
  empty report — fail-closed, never a cross-run read. `HealthReport.source_id` →
  `HealthReport.session_id`.
- **Cycle detection is now declared, not induced.** `CycleInductionAgent` and
  `induce_adhoc`/`save_cycles_config` are DELETED. The declared set is the
  vertical's `cycles.yaml` `cycle_types` ⊕ `cycle` overlay teach rows — each
  canonical cycle type becomes one `cycle` lifecycle artifact (declare → bind →
  execute). Ungroundable declared cycles stay `declared` with `state_reason`
  ("not detected in this workspace"); detected-but-unmeasured cycles stay
  `grounded`. The `BusinessCycleAgent.analyze()` all-in-one method is replaced
  by `ground_cycles()` (returns a `BusinessCycleAnalysis`; the phase persists).
- **Calibrate**: `business_cycle_health` recall/precision against `clean_eval` —
  but only AFTER an `operating_model` run has detected + promoted cycles for the
  session. Key scenarios:
  1. A finance workspace where order_to_cash / accounts_receivable / period_close
     ground and measure → the detector scores their tables off the promoted
     operating_model run.
  2. A workspace where operating_model has NOT run → the detector reads no cycles
     and scores 0.0 everywhere (no false alarms from a stale source-wide read).
  3. Multi-run: re-running operating_model supersedes cycles under a fresh
     `run_id`; the detector must read only the promoted run's cycles, never the
     union (the run-versioned consumer contract).
- **Status**: pending

## 2026-06-07: validation agent-tier honesty pass — `status=failed` semantics changed (DAT-439)

Branch `feat/dat-439-fail-loud-sweep`. **DAT-442's cross_table_consistency
calibration must land AFTER this** — what `status=failed` MEANS in
`validation_results` changed:

- **`failed` now means a JUDGED data failure only.** An evaluation that could
  not judge the data (no recognizable result columns, zero rows from a
  balance/comparison/aggregate summary query, unrecognized `check_type`) is
  now `status=error` — previously these surfaced as `failed` (live-proven:
  `three_way_match` "Comparison check inconclusive" was reported FAILED).
  The artifact stays `grounded` with the reason in
  `lifecycle_artifacts.state_reason`; the result row is `error`, never
  `failed`. Any eval assertion counting failures must not expect
  inconclusives among them.
- **`_score_validation_result` branches** (`entropy/detectors/computational/
  cross_table_consistency.py`): `skipped` now scores 0.0 explicitly
  (previously fell into check-type scoring and a skip carrying table_ids
  could score 1.0); `error` keeps 0.5 (covers execution errors AND
  inconclusive evaluations). Calibration of these constants = DAT-442.
- **No-tool-call LLM responses are no longer rescued** by JSON-parsing the
  text content — they are bind ERRORs (artifact stays `declared` + reason
  "no structured output"). A mocked-LLM eval scenario that emitted plain-text
  JSON instead of a tool call now yields an ungroundable validation.
- `can_validate=true` with no SQL is a bind ERROR now, not `skipped`.
- Unrecognized `check_type` (e.g. `referential`) no longer passes on
  row_count>0 — it is `error` (inconclusive). Only `balance` / `comparison` /
  `constraint` / `aggregate` are judgeable.

## 2026-06-07: operating_model stage — validation lifecycle revival (DAT-438)

Branch `feat/dat-438-operating-model`. The validation family moved from a
dormant source-scoped phase into a new `operatingModelWorkflow`
(resolve → validation → promote, stage head `(session:{id}, "operating_model")`).
What eval needs to know:

- **`ValidationResultRecord` is run-versioned now**: new `run_id` column (NOT
  NULL) + UNIQUE `(session_id, validation_id, run_id)`. Any eval read of
  `validation_results` must scope to the promoted operating_model head
  (`head_run_id(session, session_head_target(sid), "operating_model")`) or
  this-run's id — an unscoped read double-counts superseded runs. Schema
  change ⇒ `down -v` on any persistent stack.
- **Validation is no longer in any begin_session/add_source chain** — it runs
  ONLY via `operatingModelWorkflow` (input: SessionIdentity only; the table
  set comes from `session_tables`). Driving it: start the workflow after a
  begin_session run of the same session.
- **Validation induction is DELETED** (`ValidationInductionAgent`,
  `save_validation_specs`). `_adhoc` sessions now yield ZERO declared
  validations (explicit `no_declared_validations` outcome) — any eval
  scenario that relied on cold-start induced validations is obsolete.
  Declared set = vertical YAML ⊕ `validation` config_overlay rows (new teach
  applier — a teach scenario can now ADD/REPLACE a validation by
  `validation_id`).
- **New `lifecycle_artifacts` table**: per `(session, "validation",
  validation_id, run)` row with state declared/grounded/executed +
  `state_reason`. Recall-style assertions can use it: an ungroundable spec is
  `declared` + reason (never silently absent); an execution error is
  `grounded` + reason; PASSED/FAILED results have `executed` artifacts.
- **`cross_table_consistency` detector** now filters `run_id == ctx.run_id`
  on the detect path when set (band wiring + eval gate = DAT-432, unchanged
  here).
- Calibration impact expected: NONE on existing detectors (validation wasn't
  in any driven chain). The cross_table recall assertions stay
  `OUT_OF_SLICE_REASON` until DAT-432.

## 2026-06-07: value artifacts run-versioned + promoted-read surface (DAT-448 + DAT-453)

Branch `feat/dat-448-453-promoted-read-surface`. Two coupled changes.

### DAT-448 — the value-layer artifacts joined the version axis

- **`SliceDefinition`, `ColumnDriftSummary`, `TemporalSliceAnalysis`,
  `DerivedColumn` now carry `run_id`** (nullable; new writes stamp `ctx.run_id`;
  `uq_drift_slice_column_run` guards the drift grain). The drift/period persists
  switched from append-only to run-scoped delete-then-insert — re-runs no longer
  duplicate rows.
- **`slicing` + `correlations` `should_skip` are run-scoped**: a fresh
  begin_session run ALWAYS re-derives slice definitions (and their
  `sql_template` DDL) + derived columns. The "skips itself on stale definitions"
  replay hazard from the 2026-06-05 entry is FIXED — eval no longer needs to
  wipe PG to avoid cross-run definition leakage (wiping is still fine).
- **Slice-table profiles/quality are stamped with the run** — they upserted on
  `(column_id, run_id=NULL)` before, which never conflicts, so re-runs
  duplicated them and slice_variance read the pile unscoped.
- **Pinned base-run map**: `run_detectors` resolves the promoted
  `(table:{id}, stage)` heads ONCE (`loaders.resolve_base_runs`) and threads
  them onto `DetectorContext.base_runs`; `load_semantic`/`load_statistics`
  consult the pin instead of re-resolving the moving head per call (the
  DAT-405 fallback semantics, now torn-read-proof). `_table_head_run` is gone.
- **Detector input reads are run-scoped**: `load_drift_summaries` /
  `load_correlation` take `run_id`; dimensional_entropy + slice_variance filter
  their direct reads. Test callers passing `run_id=None` match unstamped rows —
  existing unit-test fixtures keep working.

**Eval impact**: recall/precision semantics unchanged on fresh runs (single
run per session reads its own rows + the pinned add_source base). Run BOTH
strategies' full sweeps; expect baseline-identical results. The teach re-run
path (DAT-447) is now viable: second runs re-derive cleanly, no
`slice_view_unbindable` / `drift_analysis_failed` stale leakage.

**Testdata**: no changes needed.

### DAT-453 — promoted reads enforced by the database (ADR-0008)

- `storage/read_views.py` generates `current_*` head-joined views (+ pass-
  throughs) into `ws_<id>_read`; `schema_read.sql` is the checked-in artifact
  (CI drift gate covers it). Bootstrap materializes the views and provisions
  `cockpit_reader` (SELECT on read schema; INSERT/UPDATE only on the three
  control tables: sources, investigation_sessions, config_overlay).
- The cockpit's Drizzle mirror narrowed to the read schema; its tools dropped
  hand-rolled head joins.
- Engine in-process readers deliberately stay on the `head_run_id()` helper
  seam (grants can't bind the engine; view SQL would cost typed ORM reads).

**Eval impact**: none on the calibration path (eval connects as the engine
role and reads via `measure`-style queries). NOTE for DAT-447 step 0: the
eval conftest score read can now alternatively go through
`ws_<id>_read.current_entropy_objects` instead of hand-resolving heads.

## 2026-06-05: value-layer fixes from DAT-405 calibration (eval findings)

Two fixes on `fix/dat-405-value-layer-eval-findings`, found by the first
post-DAT-403 calibration runs in dataraum-eval (findings list on DAT-405):

- **Enriched dim-column profiling repaired** — `enriched_views_phase` passed the
  pre-quoted `view_fqn` as the profiler's `table_duckdb_path`, which the profiler
  re-quotes as ONE identifier → DuckDB `zero-length delimited identifier` → every
  enriched join-column profile failed (`dim_columns_profiled … profiles: 0` on
  every run). Now passes the bare view name (== the persisted `Table.duckdb_path`).
  Enriched join columns (`account_id__account_type`, …) now HAVE statistics —
  dimension_coverage / slice detector inputs change accordingly.
- **`slice_analysis` no longer dies on stale slice definitions** — a slice VIEW
  whose definition references a column the current slicing view doesn't carry
  (e.g. an enriched join column on a run whose enriched view is a passthrough)
  binder-failed at COUNT and killed the whole begin_session run (both
  detection-typing-v1 runs). `register_slice_tables` now probes the COUNT, warns
  `slice_view_unbindable`, and skips that slice. The underlying replay hazard —
  `slicing` skips itself when "all fact tables already have slice definitions",
  freezing stale definitions across runs — is documented on DAT-405, not yet fixed.
- **Loaders head-fallback (second commit)** — `load_semantic`/`load_statistics`
  fall back to the promoted `(table:{id}, stage)` snapshot head when the current
  run has no row. Session detects carried the SESSION run's run_id while the
  per-column rows were written by the add_source run, so strict this-run reads
  silently broke `temporal_drift` (0 records ever — fail closed) and
  `slice_variance`'s role gate (1.000 on ID columns on clean data — fail open).
- **temporal_drift gates on `temporal_behavior` (third commit)** — point-in-time
  measures (period balances: clean trial_balance debit/credit_balance scored
  0.45–0.54) drift by data-model design; the detector now skips them next to the
  existing role gate. Additive measures (transaction amounts) keep drift
  detection. Decision: DAT-405 hybrid (accept slice_variance's clean scores as
  accurate heterogeneity; gate drift on periodic snapshots).
- **network.yaml edge calibration (DAT-405, two edges)** —
  `temporal_drift → query_intent` 0.3→0.45 (at 0.3 even a maximal drift score
  rolled to ≤ the 0.3 clean-band floor — query_intent could never leave "ready"
  on drift evidence); `relationship_quality → reporting_intent` 0.5→0.75 (at
  relationship grain it is the only observed parent, and the sqrt-boosted
  20%-orphan fixture scores ≈0.45 — 0.5 could never band past "ready").
  Eval floor expectations now assert relationship problems at RELATIONSHIP grain
  (per-endpoint), per the DAT-405 decision: the column's own band stays blind to
  relationship problems by design.
- **Systemic (not yet fixed): slice definitions are table-scoped + immortal**
  while enriched/slicing views are run-versioned and LLM-shaped. Third
  manifestation observed same day: `drift_analysis_failed` GROUP BY on a stale
  dim column after a re-run's enriched view picked different dims. Content-keyed
  sources (DAT-422) widen this to CROSS-strategy leakage in eval (same bytes →
  same table_id). Proposed: run-version slice definitions (stamp run_id, promote
  via MetadataSnapshotHead, re-derive instead of skip on column-set change).
  Full mechanics on DAT-405.

### dataraum-eval
- Re-run value-layer calibration; detection-typing-v1 completes begin_session
  end-to-end now (verified 2026-06-05, recall 2/2).
- Known residue: temporal slice profiling still warns `column_profile_failed`
  (Binder) for stale-definition columns on period slices — non-fatal, same
  stale-definitions family.

## 2026-06-05: DAT-403 value layer revived + wired into begin_session

The 5 dormant value-layer phases (`slicing` → `slicing_view` → `slice_analysis` →
`temporal_slice_analysis` → `correlations`) are revived source-free and wired into
`beginSessionWorkflow` after `enriched_views`, and their 4 detectors now run in the
terminal session detect. **First time these detectors execute in a real run — they
need recall calibration (DAT-405).** Branch `feat/dat-403-slices`.

### dataraum-eval
- **Newly executing detectors** (declared by the now-wired value phases, added to
  `SESSION_DETECTOR_PHASES` in `worker/activity.py`): `slice_variance` (slice_analysis),
  `temporal_drift` + `dimensional_entropy` (temporal_slice_analysis), `derived_value`
  (correlations). They feed `column:`/`table:` readiness bands in begin_session.
- **New network node** `cross_column_patterns` (`semantic.dimensional.cross_column_patterns`)
  in `dataraum-config/entropy/network.yaml` so `dimensional_entropy` contributes to bands —
  edges `→ query_intent 0.3`, `→ aggregation_intent 0.4`. **These strengths are initial
  guesses; calibrate them.** The other three detectors reuse existing nodes
  (`slice_stability`, `temporal_drift`, `formula_match`).
- **Calibrate:** value-layer detector recall on a begin_session run — known slice variance,
  temporal drift across periods, and derived-column (formula) injections must surface as
  non-ready bands; clean data must stay ready (precision). This is the DAT-405 value-layer
  gate. No response-shape change; the readiness record schema is unchanged.
- **Substrate note (DAT-415):** slicing views are now run-versioned on the
  `MaterializationRecipe` substrate (`layer="slicing"`), like enriched views — relevant only
  if a strategy resets/rebuilds views (`rebuild_session_views`).

### dataraum-testdata
- Injection hints for value-layer recall: (a) a categorical slice dimension whose per-slice
  null-rate / distinct-count varies sharply across slices (slice_variance); (b) a column whose
  category distribution drifts month-over-month (temporal_drift); (c) a derived column with a
  known formula that holds only ~part of the time, e.g. `total = qty * price` at match_rate
  ~0.5 (derived_value); (d) two columns with a conditional dependency / mutual exclusivity
  across slices (dimensional_entropy / cross_column_patterns). Keep directional.

## 2026-06-05: typing replay-poison + STRPTIME-throw + eligibility key-abort fixes

Three add_source typing/eligibility bug fixes from a user report (German DD.MM.YYYY
data; branch `fix/typing-replay-poison`). **Typing outcomes change — re-run the
add_source recall suite.** Expected movement: date-typed coverage UP.

### dataraum-eval
- **Changed:** `analysis/typing/{patterns.py, inference.py, resolution.py}`,
  `analysis/eligibility/{evaluator.py, config.py}`, `pipeline/phases/column_eligibility_phase.py`,
  `storage/models.py` (`Column.type_decision` → `type_decisions` list), `graphs/context.py`,
  `dataraum-config/phases/column_eligibility.yaml` (`key_patterns` removed).
- **Behavior changes:**
  1. **One malformed value no longer zeroes a date pattern.** Standardization exprs are
     TRY_-normalized (`STRPTIME`→`TRY_STRPTIME`, inner `CAST`→`TRY_CAST`) at Pattern load.
     A 99%-clean DD.MM.YYYY column now types DATE with the bad rows quarantined instead of
     falling back to VARCHAR with `success_rate=0.0` and no failed examples. Columns that
     previously stayed VARCHAR because of a single dirty value will now be DATE — ground
     truths that encoded the buggy VARCHAR outcome need updating.
  2. **Re-runs/replays re-decide types.** Resolution honors only `decision_source='manual'`
     TypeDecisions (latest), and a manual pin keeps the standardization expr; candidates are
     run-scoped. Previously ANY second run froze the first run's outcome (taught patterns
     never applied) or re-applied DATE without its parse expr (100%-NULL column → all rows
     quarantined → eligibility dropped it). Any calibration that re-runs typing on the same
     workspace exercises this path.
  3. **Eligibility no longer aborts on all-null key-named columns.** `is_likely_key` /
     `key_patterns` deleted; an all-null `*_id` column drops + records INELIGIBLE like any
     other and the run continues. Scenarios that asserted a pipeline FAILURE for null key
     columns now expect a completed run with `dropped >= 1`.
- **Calibrate:** add_source recall suite + any teach/replay strategy. No new response
  fields; no workflow contract change.

### dataraum-testdata
- Injection hints: (a) a date column with exactly one unparseable-but-regex-matching value
  (e.g. `29.02.2023`) — should type DATE, quarantine 1 row; (b) a 100%-null `*_id` column —
  should drop, not abort; (c) a DD.MM.YYYY column re-typed across two runs — second run must
  still parse (the replay-poison regression).

## 2026-06-05: DAT-422 — add_source runs over a SET of sources (workflow input contract change)

Epic DAT-420 (source model). **The `addSourceWorkflow` input changed shape — BREAKING for the
eval add_source driver (DAT-425).** No detector, threshold, or table-naming change; harness
adaptation only. (The DAT-421 entry below predates this — its "no contract change" claim is
about the semantic scoping ticket, not the epic.)

- **New input:** `AddSourceInput = { identity, source_ids }` (`worker/contracts.py`). The caller
  passes the run's source set as `source_ids` (≥1) and leaves `identity.source_id` unset — the
  workflow scopes each per-source `import` itself and runs source-free past import
  (session-scoped fan-out/reduce/detect).
- **Workflow id is session-keyed:** `addsource-{workspace_id}-{session_id}`
  (`add_source_workflow_id`), not source-keyed. The caller MUST seed the
  `investigation_sessions` row BEFORE starting — `typing` writes `session_tables` with a
  NOT-NULL FK to it (the cockpit trigger does this; the eval driver must too).
- **File datasets (the cockpit model):** one content-keyed source per file — name
  `src_<digest>`, `connection_config.file_uris = [the one uri]`; identical bytes upsert one
  source and import skips the re-load. A db_recipe connection stays ONE user-named source.
  Minimal driver adaptation: keep seeding however the harness does and wrap the id(s) in
  `source_ids` — but DAT-425's pinned shape is per-object content-sources.
- **Result shape unchanged** (`AddSourceResult = { raw_table_ids, tables }`); table naming
  unchanged (`<source_name>__<stem>` — an upload's tables read `src_<digest>__<stem>`, so
  resolve tables via the run's `session_tables` or the returned ids, not a hand-built name).
- **Calibrate:** nothing — re-run the add_source recall suite after bumping the engine
  submodule; expect no movement (same detectors, same data).
- **Status:** pending

## 2026-06-04: DAT-421 — add_source `semantic_per_column` scopes by session, not source

Epic DAT-420 (source model). The add_source source-level reduce was the last spine
phase still selecting its tables by `Table.source_id == ctx.source_id`; it now uses the
session anchor `tables_for_session(session_id)` — the same key `detect`/readiness already
use (DAT-410), populated by `typing` via `session_tables` (DAT-407).

- **Behavior-preserving for single-source calibration.** For an add_source run `typing`
  links exactly that source's freshly-typed tables to the run's session, so the reduce's
  table set is identical to the old source filter. The `[semantic]` producer feeds the
  same columns to `business_meaning`, `unit_entropy`, `temporal`, `null_ratio`,
  `type_fidelity`, `benford` — **no recall/precision movement expected** from this ticket.
- **What changed for eval:** the scoping KEY is now session-id, not source-id. If a future
  calibration exercises a multi-(per-object-)source run, the reduce selects the run's
  session-linked tables across sources — intended, not a regression.
- **No schema / contract / workflow change**, no new tables, no detector changes.
- **Calibrate:** nothing new required; re-run the add_source recall suite after bumping the
  engine submodule → expect no movement.

## 2026-06-04: DAT-415 + DAT-402 — enriched_views revival + table-grain readiness + view-DDL versioning

begin_session Slice 2.1 (DAT-402) bundled with view-DDL versioning (DAT-415, Phase 2 Slice B
of the versioned-metadata epic). Revives the dormant `enriched_views` phase as a run-versioned
begin_session step and gives begin_session its first **table-grain readiness** signal.

### dataraum-eval
- **Changed:** `pipeline/phases/enriched_views_phase.py` (source-free re-seam, run-versioned,
  recipe-backed DDL), `analysis/views/{recipe.py (new), db_models.py, builder.py}`,
  `entropy/detectors/semantic/dimension_coverage.py` (scope view→table),
  `entropy/views/readiness_context.py` + `entropy/readiness.py` (table-grain rollup + persist),
  `worker/{workflows.py, activities.py, activity.py, main.py}` (new `enriched_views` activity in
  the begin_session spine + `SESSION_DETECTOR_PHASES`), `core/sql_normalize.py` (new),
  `analysis/typing/recipe.py` (`order_recipes_by_dependency` made public).
- **Affects:** the **begin_session** path only — add_source is untouched (no `table:`-scoped
  detector runs on `_DETECTOR_PHASES`; `enriched_views` is not in it). begin_session now runs
  `relationships → semantic_per_table → session_materialize_overlays → enriched_views →
  session_detect → …`, and `session_detect` runs `dimension_coverage` (newly wired) in addition
  to the relationship detectors.
- **Calibrate (the real gate — DAT-405):** `dimension_coverage` recall/precision at **table
  grain**. It builds a grain-preserving LEFT JOIN view per fact table over its LLM-confirmed
  dimension relationships, then scores the mean NULL rate across the joined dimension columns
  (sqrt-boosted). Ground truth = the finance `break_referential_integrity` injection on
  `payments.invoice_id` (~20% orphans): the enriched view's joined `invoices.*` columns are NULL
  for orphaned rows → `dimension_coverage ≈ sqrt(0.2) ≈ 0.45` on **`table:payments`** → a
  non-`ready` band (rolls into `query_intent` 0.5 / `reporting_intent` 0.6). Validate that a clean
  join (no orphans) scores ~0 and the 20%-orphan case lands `investigate`. This is a NEW readiness
  surface — there was no `table:` band before.
- **Notes:**
  - **New readiness rows:** `entropy_readiness` now carries `target = "table:{table_name}"` rows
    (table FK set, **column_id NULL**) alongside the existing `column:` / `relationship:` rows.
    Resolve the current run via the per-session head (`session:{id}`, stage `detect`) — same as
    relationship readiness, NOT the per-table head. Engine reader: `load_table_readiness(session,
    session_id)`.
  - **Schema changes (need a fresh ws schema):** `enriched_views.view_sql` is **dropped** and
    `enriched_views.run_id` **added**; `materialization_recipes` now also stores `layer="enriched"`
    rows. `create_all` is additive — it will NOT drop `view_sql`, so a stale workspace whose
    `enriched_views.view_sql` is still `NOT NULL` will **fail inserts**. Re-pull a clean schema
    (`docker compose down -v`) before driving begin_session. (The eval's vendored engine submodule
    is pinned pre-DAT-408 and uninitialized — bump it to current `main` first; see the eval
    teach→keeper foundation notes.)
  - **View determinism:** the enrichment LLM runs at temperature 0 and view DDL is sqlglot-canonical
    + collision-free-named (`enriched_{source}__{table}`), so a re-run with the same confirmed joins
    produces a byte-identical recipe (no spurious new version). Eval can diff recipe DDL to detect a
    genuine view change across runs.
  - **No add_source recall movement expected** from this ticket — if the add_source detector-recall
    suite moves, that's a regression, not this change.
- **Status:** pending

## 2026-06-04: DAT-414 — versioned typed/quarantine materialization DDL

Phase 2 of the versioned-metadata epic (DAT-412), Slice A typed/quarantine only.

- **No behavior change to calibrate.** The typed/quarantine `CREATE OR REPLACE TABLE … AS
  SELECT` that typing executes is **byte-identical** to before — the only additions are
  (a) persisting that DDL string as a new `MaterializationRecipe` row stamped with the
  run's `run_id`, and (b) a new `rebuild_from_recipe` / `reset_to_run` API that
  re-executes a stored DDL. Detector inputs, decided types, quarantine contents, and
  readiness are all unchanged. Recall/precision suites should be unaffected.
- **New table:** `materialization_recipes` (grain `(table_id, layer, run_id)`, columns
  `target_fqn`, `ddl`, `depends_on`). Auto-created via `create_all`; nullable `run_id`
  mirrors `type_decisions`. No eval read of it is required.
- **One non-determinism note** if eval ever round-trips a quarantine table: the quarantine
  DDL stamps `_quarantined_at` via `CURRENT_TIMESTAMP`, so re-executing the *same* recipe
  produces a fresh audit timestamp — the **data** rows round-trip identically, the clock
  advances. Compare quarantine on data columns, not `_quarantined_at`.
- **Calibrate:** nothing new required. If the existing add_source detector-recall suite is
  re-run after bumping the engine submodule, expect no movement from this ticket.

## 2026-06-04: DAT-409 — relationship teach write-loop + overlay-contract unification

Slice 2.0c. The relationship-overlay write path (teach → materialize → keeper) plus a
clean-up of the overlay contract DAT-408 left half-migrated.

- **Two relationship-calibration changes to re-validate (DAT-405):**
  1. `join_path_determinism` "resolved" is now **per-column-pair, not per-table-pair**. A
     `confirm` resolves ambiguity only for the exact focal column path it names; other
     paths between the same two tables stay ambiguous. (Was: any confirmed join between
     the two tables marked every focal pair resolved.) Re-check join_path on multi-path
     schemas.
  2. `relationship_entropy` confirmation now reads `load_confirmed_relationship_pairs`
     (`action='confirm'`, column-pair key) instead of the deleted table-name
     `load_preferred_join_overlays`. Same source overlay, stricter key.
- **New cross-run persistence (keeper):** `write_relationship_keepers` lifts a promoted
  `llm` the current run didn't reproduce (and the user didn't reject) into a `keep`
  ConfigOverlay; it materializes as `detection_method='keeper'` from the **next** run on.
  The lifted relationship is **absent from the run that detected its absence** (one-run
  gap, by spec) — eval comparing run N's catalog should expect the keeper only in N+1.
- **Single overlay payload shape:** `ConfigOverlay(type='relationship')` is now uniformly
  `{action, from_column_id, to_column_id}`, `action ∈ {confirm, reject, add, keep}`
  (`keep` engine-written). The old `{table, target_table}` confirm shape is gone.
- **Calibrate:** exercise a teach→re-run cycle (confirm/reject/add) and a two-run
  silent-accept (drop an llm between runs → assert `keeper` in the later run).
- **Status:** pending

## 2026-06-03: DAT-408 — relationship-granularity readiness + begin_session on the substrate

Slice 2.0b + the begin_session-substrate core of DAT-415. Relationship readiness is now
first-class (per directional column pair), produced by the two relationship detectors
**reshaped to a new `relationship` detector scope**, persisted + promoted on the
versioned snapshot substrate at begin_session's terminal `detect`.

### dataraum-eval
- **Detector behavior CHANGED (recalibrate recall/precision vs ground truth):**
  - **`join_path_determinism`** — was column-scoped + proportional (orphan / star-schema /
    ambiguity-ratio over a column's relationships). Now **relationship-scoped**: for the
    focal directional pair it scores `score_ambiguous` iff there is **>1 distinct
    column-pair join path between the two tables** (and no preferred-join overlay), else
    `score_deterministic`. The `orphan` (0.9) and proportional bands are **gone**. One
    object per relationship, target `relationship:{from_col}::{to_col}`.
  - **`relationship_entropy`** — was column-scoped (one object per relationship a column
    touched). Now **relationship-scoped**: one object per directional pair (representative
    row `manual > llm > candidate`). RI/cardinality/semantic math unchanged, BUT
    confirmation now comes from **`ConfigOverlay(type='relationship')`** (multi-source),
    **not** `Relationship.is_confirmed` (DAT-372). So "confirmed" fires only when the user
    has actually teach-confirmed via an overlay — expect more relationships scoring
    "unconfirmed" until teaches exist. `join_path_determinism` reads the SAME overlays, so
    the two agree on "confirmed."
- **New readiness rows**: `entropy_readiness` now contains `relationship:` target rows
  (band/intents/drivers), `column_id`/`table_id`/`source_id` NULL — identity is in the new
  **`target`** column. The rollup (`assemble_readiness_context`) routes `relationship:`
  targets through the same network rollup as columns (so `join_path_determinism` →
  `query_intent` etc. drive the band; `relationship_quality` is a DirectSignal unless a
  network node exists for it — check `network.yaml` if relationship bands look flat).
- **New reader**: `load_relationship_readiness(session, session_id)` — head-resolved (per
  `(relationship:{...}, "detect")`) and **gated on a live, non-suppressed `Relationship`**
  (dropped/vanished relationships keep prior-run rows for audit but don't surface).
- **Relationship catalog is RUN-VERSIONED** (the add_source contract — affects replay):
  every `Relationship` carries a `run_id`, rows **coexist across runs** (non-destructive),
  and **deletes are run_id-scoped, retry-only** (a re-run never touches a prior run's rows).
  `detection_method` taxonomy: **`candidate`** = ephemeral per-run structural; the **defined**
  set = `not candidate` (`llm` this-run + `manual`/`keeper` materialized from overlays). The
  unique key is `(session_id, run_id, from_col, to_col, method)`. **All catalog reads scope to
  the current run** — durable `manual`/`keeper` are re-materialized into each run from
  `ConfigOverlay`, so a single current-run read sees the whole catalog. The overlay **writers**
  (confirm/reject/add + the silent-accept→`keeper` lift-up + materialize-from-overlay) are
  **DAT-409**, not here — so this slice's re-runs reproduce `candidate`/`llm` only.
  **Dormant defined-relationship consumers** (`enriched_views`/`cycles`/`validation`/`graphs`)
  still read `method=='llm'` unscoped — they are in no live workflow; their slices (DAT-402/3/4)
  must adopt the run-scoped `not candidate` read when reactivated.
- **begin_session is versioned + sealed PER SESSION**: `BeginSessionWorkflow` mints a `run_id`,
  ends with `session_detect` → `session_promote_to_latest`, which sets one head
  `(session:{id}, "detect")` → the current run (atomic whole-session re-run, no per-target head).
  Re-runs are non-destructive; readers resolve the session's current run via that head.
  `should_skip`'s "already detected/classified → skip" idempotency branch was removed on
  `relationships` + `semantic_per_table` (it would make a replay a no-op); preconditions kept.
- **Schema** (fresh DB / re-pull): `EntropyReadinessRecord.target` (NOT NULL);
  **`source_id` DROPPED from `entropy_objects` + `entropy_readiness`** (write-only; source via
  `table_id`) + their two indexes; `MetadataSnapshotHead.table_id` → **`target`** string
  (`table:{id}` add_source, `relationship:{a}::{b}` readiness, `session:{id}` the begin_session
  seal); **`Relationship.run_id`** + unique key `(session_id, run_id, from_col, to_col, method)`;
  `SessionIdentity.run_id`. If an eval path filtered entropy/readiness/relationships by
  `source_id`, switch to `session_id`; relationship reads should scope to the current `run_id`.

### dataraum-testdata
- Hints: ground truth for **relationship-level** quality would help calibrate the reshaped
  detectors — multi-FK fixtures with (a) a single clean join path, (b) two distinct
  column-pair paths between the same two tables (ambiguous), and (c) a confirmed vs
  unconfirmed relationship (to exercise the overlay-confirmation path).

### Status: pending

## 2026-06-03: DAT-413 — versioned metadata substrate + non-destructive replay (Slice A)

### dataraum-eval
- **Changed**: add_source metadata is now versioned by a `run_id` (the snapshot axis, minted per workflow run). Replay is a **full add_source re-run under a fresh `run_id`** — the partial-replay machinery (`ReplayScope` / `from_phase` / `replay_cleanup`) is gone. New `metadata_snapshot_head (table_id, stage) → run_id` table + a terminal `promote_to_latest` activity that flips the head. `run_id` columns added to `TypeCandidate`, `TypeDecision`, `StatisticalProfile`, `StatisticalQualityMetrics`, `ColumnEligibilityRecord`, `SemanticAnnotation`, `TemporalColumnProfile`, `TableEntity`, `EntropyObjectRecord`, `EntropyReadinessRecord`.
- **Affects**: any eval path that reads persisted readiness or re-runs add_source.
  1. **`load_persisted_readiness` now head-resolves** — it returns readiness ONLY for the promoted `(table_id, stage='detect')` run. A run that wrote readiness but did NOT call `promote_to_latest` returns EMPTY. The normal `AddSourceWorkflow` runs `detect` then `promote_to_latest`, so the pipeline path is fine; but a harness that drives `persist_readiness`/`detect` directly without promoting will now read empty.
  2. **Single-row-per-column is gone** — `TypeDecision`/`SemanticAnnotation`/`StatisticalProfile`/`StatisticalQualityMetrics`/`ColumnEligibilityRecord`/`TemporalColumnProfile` are now keyed `(column_id, run_id)`; multiple runs coexist. Reads must resolve the current run via the head (the `detect` loaders already filter by the current `run_id`).
  3. **Replay path reads versioned snapshots** — on a teach + re-run the detectors see THIS run's freshly-derived metadata (prior runs intact). Recalibrate the replay path: a teach + re-run should reflect the teach, non-destructively.
- **Schema / setup**: requires a **fresh DB**. `create_all` adds the new `metadata_snapshot_head` table but will NOT add the new `run_id` columns or widened unique constraints to existing tables on a reused volume — drop the Postgres/workspace volume before running the worker / `bun run db:pull:metadata` against this schema.
- **Notes**:
  - Idempotency (Temporal at-least-once): one-row-per-column models use unique `(column_id, run_id)` + upsert; `TypeCandidate` + the two entropy records use `run_id`-scoped delete-before-insert.
  - `should_skip`'s "outputs already exist → skip" bail was removed on the 6 add_source metadata phases (a re-run always re-derives under the new `run_id`); `import`'s re-load guard kept.
  - **Deferred (NOT this slice)**: begin_session versioning (Slice B / DAT-415); relationship-head granularity + the `Relationship` constraint (DAT-408); DDL/materialization versioning (DAT-414); the cockpit Drizzle mirror regen + `look_table`/`why_column` head-join (bucket 2, pending).

### dataraum-testdata
- No new injections. Existing add_source + teach-replay scenarios still apply; a re-run now appends a new `run_id`'s rows rather than mutating in place.

### Status: pending

## 2026-06-02: DAT-401 — begin_session spine revives `relationships` + `semantic_per_table` (session-scoped)

Slice 2.0a. A new `BeginSessionWorkflow` composes a user-selected set of typed tables
(may span sources) into an analytical session and revives the two dormant cross-table
phases over that selection. **Both phases' internal logic is unchanged — only their
*scope* moved** from `Table.source_id == ctx.source_id` to `ctx.table_ids` (the
session's selection). No terminal `detect` runs in begin_session (relationship-/table-
granularity readiness is DAT-408 / 2.0b), so **no `entropy_readiness` rows are produced
by this stage yet**.

What changed in the engine:
- **`relationships` phase** (`pipeline/phases/relationships_phase.py`): now scopes by
  `ctx.table_ids`; still persists `Relationship` rows with `detection_method='candidate'`.
  Added `replay_cleanup` (drops its own candidate rows for the scoped tables).
- **`semantic_per_table` phase** (`pipeline/phases/semantic_per_table_phase.py`): now
  scopes by `ctx.table_ids`; still classifies tables (`TableEntity`) + confirms a subset
  of candidates as `detection_method='llm'`, reasoning over the per-column annotations.
  **LLM behavior preserved verbatim** — only the table-set selection changed. Added
  `replay_cleanup` (drops its `TableEntity` + `'llm'` rels for the scoped tables).
- New source-free runner `run_session_phase` + `begin_session_select` (writes
  `session_tables`); `PhaseContext.source_id` is now `str | None` (begin_session passes
  `None`; add_source lineage uses `require_source_id()`).

### dataraum-eval
- **Expectation: `semantic_per_table` recall is unchanged by the revival.** The phase is
  preserved verbatim except for scoping by the session's selected tables instead of a
  source. For a selection equal to a source's typed tables, the input set is identical, so
  the table classifications + LLM-confirmed relationships must match the retired monolithic
  `semantic` phase's table half. **This is the Slice-2 eval gate (DAT-405).** Verify the
  golden output (recorded fixture) does not move.
- **Relationship *detector* calibration does NOT move here** — `relationships` only persists
  structural `candidate` rows (unchanged logic); the relationship-granularity readiness +
  detectors (`join_path_determinism`, `relationship_entropy`) land in DAT-408.
- **No readiness snapshot impact** — begin_session 2.0a writes no `entropy_readiness` rows.
- **Status**: pending

### dataraum-testdata
- Needs **multi-table, multi-source** fixtures with a known join structure + ground-truth
  relationships (which `from_table.col -> to_table.col` pairs are real, expected
  cardinality) so DAT-408's relationship detectors can be calibrated against a cross-source
  selection. Directional, not prescriptive — testdata owns the injection design.

### For DAT-408 (relationship-granularity readiness) — known schema tension
- Both phases' `should_skip` + `replay_cleanup` are **session-scoped** (filter by
  `Relationship.session_id` / `TableEntity.session_id`) so a session only checks/clears its
  own rows. But `Relationship`'s unique constraint is `(from_column_id, to_column_id,
  detection_method)` — **global, no `session_id`** — and `_store_candidates` does a plain
  `session.add` (no conflict handling). So two *different* sessions cannot both hold a
  candidate for the same column pair; a cross-session re-detect would raise a unique
  violation (loud, not silent corruption). 2.0a is single-active-session per workspace, so
  this is unreachable now — but DAT-408 should decide whether relationships are
  session-scoped or workspace-global and align the unique constraint accordingly.

## 2026-06-02: DAT-410 — detect/readiness scope by the session's tables, not `source_id`

Behavior-preserving runtime refactor: the terminal `detect` step (`run_detectors`) +
`persist_readiness` now scope by the run-session's `session_tables` (DAT-407's rail)
instead of `Table.source_id`. **No detector logic, threshold, rollup, or readiness
band/driver computation changed** — only the *table-set the same detectors run over*
and the *delete-before-insert scope* of `entropy_readiness`.

### dataraum-eval
- **Expectation: readiness output is byte-identical for add_source.** A single-source
  add_source run's `session_tables` is exactly that source's freshly-typed tables (linked
  in the `typing` phase, same transaction as the `Table` row), so `run_detectors` +
  `persist_readiness` see the identical table set as the prior `source_id`-scoped code.
- **Calibrate / guard**: re-run the readiness snapshot calibration and confirm
  `entropy_readiness` rows (band, `worst_intent_risk`, intents, drivers) and detector
  recall do **not** move. This is the no-regression gate for the change; any drift is a bug.
- **Signature change** (engine-internal, not an eval surface): `persist_readiness(session, source_id, session_id)` → `persist_readiness(session, session_id, table_ids)`. New helper `tables_for_session(session, session_id) -> list[str]`.
- **Improved (not a regression)**: a per-table teach replay now clears only that table's
  `entropy_readiness` rows (delete scoped by `table_id`), not the whole source's.
- **Multi-source note**: the change makes the layer multi-source-ready but add_source is
  still single-source; multi-source detect (begin_session) is wired in DAT-408. The
  `run_detector_post_step` `source_id` anchor is intentionally kept here (harmless for
  single-source); DAT-408 drops it.
- **Status**: pending

## 2026-06-02: DAT-406 — add_source progress via `@workflow.query get_progress` (parent-level)

Adds a read-only parent-level progress surface to `addSourceWorkflow` and reshapes
the per-table fan-out so progress can advance as children resolve. **No detector,
schema, score, or rollup behavior changed** — this is orchestration/observability
only. The one calibration-adjacent point is a workflow-contract order change in
`AddSourceResult.tables` (see below), called out so the eval harness does not treat
that ordering as stable.

What changed in the engine (`packages/engine/src/dataraum/worker/`):
- **Fan-out swap: `asyncio.gather` → `workflow.as_completed`.** The parent previously
  collected children with `tables = list(await asyncio.gather(*children))`, which
  preserves **input/index order** — i.e. `AddSourceResult.tables` came back in
  `target_raw_ids` order. It now consumes them with the deterministic
  `workflow.as_completed(children)` (so `tables_completed` can tick up as each child
  lands), which yields in **child-completion order**. **Behavioral consequence:
  `AddSourceResult.tables` ordering is now non-deterministic** (completion-order, not
  input-order). The approved DAT-406 spec explicitly says `tables` order need not be
  preserved — the field is a set of raw→typed mappings the reduce/`detect` read from
  substrate, not by position — so this is intended, not a regression. Flagged here
  because it is a workflow-return-shape change: **any eval assertion that depends on
  `AddSourceResult.tables[i]` lining up with `target_raw_ids[i]` (or on a stable
  ordering of `tables`) will now flake — compare as an unordered set / key by the
  raw or typed id, not by position.** The fan-out width and the set of children are
  unchanged; only collection order moved.
- **New `get_progress` `@workflow.query` handler + `ProgressSnapshot` contract.**
  `AddSourceWorkflow` now carries a `ProgressSnapshot` (plain stdlib `@dataclass` in
  `worker/contracts.py`: `{phase: str, tables_total: int, tables_completed: int}`)
  in `self._progress`, advances `phase` before each stage
  (`import` → `processing_tables` → `semantic_per_column` → `detect` → `done`), sets
  `tables_total` once the fan-out width is known, and bumps `tables_completed` after
  each awaited (history-recorded) child completion. The read-only `get_progress`
  query returns it; the cockpit Client polls it by workflow/run id while the parent
  is blocked in the fan-out. Query is non-mutating and every mutation sits behind an
  awaited recorded-history event, so replay reconstructs the identical snapshot —
  determinism preserved. **No calibration impact** — this is a brand-new observation
  surface, but the eval harness should know the query name `get_progress` and the
  `ProgressSnapshot` shape now exist on `addSourceWorkflow` (e.g. if it inspects
  Temporal history or the workflow's query/return surface).
- **No schema / Drizzle-mirror change, no detector/threshold/rollup touch.**
  `entropy_objects`, all detector scores, the readiness rollup, the bands, and
  `entropy_readiness` are untouched. Only `worker/workflows.py`, `worker/contracts.py`,
  `worker/__init__.py`, and two worker tests changed.

### dataraum-eval
- **Eval action: confirm detector-score parity stays unchanged (it should be — no
  scoring code was touched).** Drive a run the same way (`addSourceWorkflow`).
- **Treat `AddSourceResult.tables` as unordered.** If any harness keyed on the
  `tables` list position (input/`gather` order), switch to set/membership comparison
  or key by raw/typed id. The order is now child-completion order and is
  non-deterministic by design.
- **New surface, no calibration consumption:** the `get_progress` query +
  `ProgressSnapshot` (`{phase, tables_total, tables_completed}`) are observability
  only — nothing for calibration to baseline, just noted so history/contract
  introspection isn't surprised by the new query.

### dataraum-testdata (hints)
- None.

## 2026-06-01: DAT-399 (D) — persisted readiness as the single source of truth

Make the engine's query-time consumers READ the persisted `entropy_readiness` band
instead of recomputing the noisy-OR rollup at query time. The rollup now runs exactly
once, at the terminal `detect` step. **Behavior-preserving — no calibration impact expected.**

What changed in the engine:
- **Query-time consumers stop recomputing the rollup.** `entropy/views/query_context.py::build_for_query` (the contract gate) and `graphs/context.py` (ContextDocument assembly) no longer call `build_for_readiness`. They now read `load_persisted_readiness` (reconstructs the banded view from the `entropy_readiness` rows) for the band/counts/overall_readiness, and `build_column_evidence` (rollup-free raw evidence) for the contract `dimension_scores` + `avg_entropy_score`. `build_for_readiness` (the full noisy-OR) survives ONLY as the `detect` step's computation via `persist_readiness`.
- **Why scores are unchanged:** the contract gate's `dimension_scores` only ever read raw per-node `score`/`dimension_path` + direct signals — they never went through the noisy-OR. `build_column_evidence` is the same score-assembly code with the rollup skipped, so `dimension_scores` are byte-identical. Contracts also read `ColumnSummary.readiness` (a blocked column blocks every contract); that band is threaded in via the new `network_to_column_summaries(..., band_by_target=...)` override, sourced from the persisted rows.
- **No schema / Drizzle-mirror change** — slice D is read-only against the existing `entropy_readiness` table.

### dataraum-eval
- **Eval action: re-verify contract-gate parity (should be unchanged).** The contract path is calibration-sensitive, but this is a behavior-preserving read-path swap: `entropy_objects`, all detector scores, the rollup, the bands, and contract `dimension_scores` are identical. An engine integration parity test (`test_persisted_readiness_is_single_source_of_truth`) asserts persisted-band == live-rollup AND contract `dimension_scores`/readiness identical, on real data. If any eval harness imported `build_for_readiness` for query-time contract summaries, switch to `build_column_evidence` + `band_by_target` (mirrors `_build_column_summaries` in `tests/integration/test_contracts.py`).

### dataraum-testdata (hints)
- None.

## 2026-06-01: DAT-399 (A+B+C) — retire BBN-era scaffolding, self-describing drivers, readiness-vocabulary rename

Cleanup + extend + rename on top of DAT-394. **Behavior-preserving for detector scores** — no calibration impact expected.

What changed in the engine:
- **Retired dead BBN-era code** (no live consumer; only the dead `reference/mcp/` path or unwired phases reached it): `entropy/engine.py::compute_network`, `views/network_context.py::format_network_context`, the cross-column aggregation (`AggregateIntentReadiness`/`_aggregate_intents`/`CrossColumnFix`/`_compute_cross_column_fix` + `EntropyForNetwork.intents`/`top_fix`), `IntentReadiness.posterior`/`dominant_state`, `ColumnNetworkResult.needs_attention`, `dimensional_entropy`'s dead `build_for_network` read, `network/model.py::get_parents`/`get_children`, and the whole `entropy/measurement.py` module. The rollup (`rollup.py`, `network.yaml`) and all detector logic are UNTOUCHED.
- **Self-describing readiness drivers (extend):** `entropy_readiness.intents[].drivers[]` and `.top_drivers[]` now carry `dimension_path` + `label` per driver (humanized node name) so the cockpit needs no node→label dictionary. **Additive inside the opaque JSONB payload → NO Drizzle mirror change, NO schema change.**

- **Readiness view-layer rename (C):** the BBN-inference vocabulary is gone — `entropy/views/network_context.py` → `readiness_context.py`, `EntropyForNetwork` → `EntropyForReadiness`, `build_for_network` → `build_for_readiness`, `ColumnNetworkResult` → `ColumnReadinessResult`, and the probability term `p_high`/`worst_intent_p_high` → `risk`/`worst_intent_risk` (now consistent with the `entropy_readiness.worst_intent_risk` column). The `entropy/network/` package (the weighted DAG / rollup) keeps its name — only the inference is gone, the DAG is real. Pure rename; no behavior change.

### dataraum-eval
- **Eval action: re-verify detector-score parity (should be unchanged).** This is pure dead-code retirement + a JSONB payload enrichment + an internal view-layer rename; no detector, threshold, or rollup change. `entropy_objects` and all scores are identical. If any eval harness imported `build_for_network`/`EntropyForNetwork` directly (unlikely — eval reads via DB/workflow), update to the `*_readiness` names.

### dataraum-testdata (hints)
- None.

## 2026-06-01: DAT-394 — one terminal `detect` step + persisted per-intent readiness

Collapses detector execution into a single source-wide `detect` activity and persists
the readiness-v2 rollup. The BBN was already retired (DAT-393); this lands its
persistence + simplifies where detectors run.

What changed in the engine (calibration-relevant):
- **Activity rename: `detect_table` + `detect_source` → a single `detect`.** Detectors no
  longer run per-table at the child tail (`detect_table`) or in a separate parent step
  (`detect_source`). One `detect` activity in `addSourceWorkflow`, after
  `semantic_per_column`, runs the **union of all wired detectors source-wide**
  (`run_detectors`, `table_ids=None`). The set is unchanged — `type_fidelity`, `null_ratio`
  (was detect_table) + `business_meaning`, `unit_entropy`, `temporal_entropy`,
  `outlier_rate`, `benford` (was detect_source). **Detector scores are byte-for-byte
  unchanged** — only the execution site/timing moved (rationale: nothing reads entropy
  mid-run; the split bought no parallelism). The per-table analytics fan-out
  (`typing→…→temporal`) is unchanged.
- **New `entropy_readiness` table** (one row per analyzed column, written by `detect`):
  collapsed `band` (ready/investigate/blocked — the contract-gate signal) + `worst_intent_risk`
  + `intents` JSONB (`[{intent, band, risk, drivers:[{node,state,impact_delta}]}]`, the
  query/aggregation/reporting split) + `top_drivers` JSONB + FKs. Delete-before-insert scoped
  to `source_id` → self-refreshing on any replay.

### dataraum-eval

- **Eval action: confirm detector-score PARITY (the bar for this PR).** The execution site
  moved, the scoring did not — `test_detector_precision` baselines must be unchanged. Any
  delta is a bug in the move, not expected drift. Drive a run the same way (`addSourceWorkflow`);
  the new `detect` step is internal to the workflow.
- **Harness/fixture update: activity names changed.** Anything that invokes activities by name
  or inspects Temporal history for `detect_table` / `detect_source` must switch to the single
  `detect`. A replay still always re-runs the reduce + `detect` at the parent tail (unchanged
  semantics; `detect` is never a `from_phase` entry point).
- **New end-to-end surface to verify:** `entropy_readiness` now lets eval assert the readiness
  shape directly (per-column band + per-intent breakdown). On clean below-floor data `intents`
  is legitimately empty (band `ready`); richness appears as detector scores cross the 0.3 floor.
- **Deploy note (dev): drain in-flight `addSourceWorkflow` runs before deploying** — the
  removed activity names would otherwise non-determinism-fail mid-history. No `patched()` guard
  added (dev-acceptable).

### Cockpit (cross-PACKAGE — DONE in this branch)

- **Drizzle metadata mirror re-pulled** (`src/db/metadata/{schema,relations}.ts` now expose
  `entropy_readiness`) so the compose-smoke Drizzle drift check stays green and the cockpit
  `why`/`look` tools (DAT-353) can read it. Regenerated via `bun run db:pull:metadata` from a
  fresh isolated schema built by the branch's `create_all` — diff is exactly the new table +
  relations (no schema-name churn). The engine schema is the source; the mirror is generated,
  never hand-edited.

### dataraum-testdata (hints)

- None. No detector or fixture surface changed.

## 2026-06-01: DAT-364 (tail) — temporal `analyze_update_frequency` NaN guard

Bug fix found while building the DAT-364 isolation test (the workflow-ID change itself is
**not** calibration-relevant — workflow-ID naming + workspace guard only, no detector/schema/phase
change).

What changed in the engine (calibration-relevant):
- **`analyze_update_frequency` now coerces a NaN `interval_std` to `0.0`** (`analysis/temporal/patterns.py`).
  A date column with exactly one interval (a 2-row table) has no sample std → pandas returns NaN →
  the JSON `profile_data` insert crashed (`invalid input syntax for type json … Token "NaN"`). A lone
  interval is trivially regular, so `0.0` is the correct reading. `interval_cv` (derived) is now
  finite too.

### dataraum-eval

- **Eval action: re-verify only if testdata has single-interval (2-row) date columns.** Previously
  such a column crashed the `temporal` phase; now it profiles cleanly with `interval_std=0.0`. No
  change to multi-interval columns or to detector recall/precision on healthy data — the coercion
  only fires on the degenerate single-interval case.

## 2026-05-31: DAT-378 — file source = explicit `file_uris` list (multi-file ingest, atomic)

Makes the engine import contract correct end-to-end for the cockpit `connect → select →
add_source` journey, and unifies the file-source connection contract.

What changed in the engine (calibration-relevant):
- **File-source contract unified on `connection_config['file_uris']` (a list).** A single-file
  source (`add_file_source`, one uploaded object) stores a one-element list; a multi-file source
  (the cockpit `select` stage enumerating a bucket prefix) stores many. The scalar
  `connection_config['path']` key and the dead CLI `source_path` fallback are **retired** — the
  worker path carried neither. `import` reads `file_uris` only; `db_recipe` sources still use the
  DISTINCT `connection_config['tables']` recipe-query key (unchanged). `ImportPhase._resolve_file_uris`,
  `SourceManager.add_file_source`, `SourceManager.list_sources`, and the cockpit seed
  (`drive-add-source.ts`) all moved to `file_uris`.
- **One raw table per URI.** `import` validates EVERY URI (`validate_source_uri` — the engine never
  globs) then loads each in turn, so one import activity yields N raw tables; `addSourceWorkflow`
  fans out one `processTableWorkflow` per raw table (DAT-370) — **no Temporal-contract change**.
- **Multi-URI import is now atomic.** Raw loaders (CSV/JSON/Parquet) use `CREATE OR REPLACE TABLE`,
  and a per-URI failure mid-list drops this run's DuckDB tables + rolls back the session, so a failed
  import commits nothing. Previously a partial failure committed the earlier URIs (failure is a
  RETURN, so `session_scope` committed on clean exit) and the next run's `should_skip` silently
  dropped the rest — a data-corruption wedge, now fixed.
- **Extension routing reconciled with the cockpit.** Engine suffix→loader now matches `connect.ts`
  FILE_READERS / `upload/policy.ts` ALLOWED_EXTENSIONS exactly: **csv/tsv/txt → CSV, parquet/pq →
  Parquet, json/jsonl/ndjson → JSON**. `.ndjson` previously fell through to the CSV loader (misparse);
  `.txt`/`.pq` were accepted by the cockpit but rejected at engine registration. Both fixed.

### dataraum-eval

- **Eval action: behavior-preserving for single-file sources — re-verify, don't expect a shift.** A
  single CSV/Parquet/JSON source produces the same raw table as before; only the connection key
  (`path` → one-element `file_uris`) and the raw `CREATE` (now `OR REPLACE`) changed. The multi-file
  path is **new capability** (a source can now be several files → several raw tables), exercised the
  same way (`addSourceWorkflow`); detector logic is untouched.
- **How to drive a run / seed a source**: a file source's `connection_config` is now
  `{"file_uris": ["s3://<lake-bucket>/<key>", ...]}` (NOT `{"path": ...}`). Any eval/harness fixture
  or seed that wrote `{"path": ...}` for a file source must switch to `{"file_uris": [...]}`. db_recipe
  sources are unchanged (`{"tables": [{name, sql}], "backend": ...}`).
- **`.ndjson` now lands in the JSON loader** — a fixture that relied on the old (wrong) CSV routing
  for an `.ndjson` file would change shape; none expected.

### dataraum-testdata (hints)

- No new injection types. A multi-file fixture (a bucket prefix with ≥2 loadable files that should
  ingest as ≥2 raw tables) would exercise the new fan-out + the atomic-failure path directly. Optional.

## 2026-05-31: DAT-382 — ontology induction LEAVES the engine for the cockpit agent tier

Lands the ADR-0004 cut: `_adhoc` ontology induction is no longer the engine's
job. The cockpit `frame` stage (TS, TanStack AI + `@tanstack/ai-anthropic`) now
induces concepts from the connect schema and writes them as `concept`
`config_overlay` rows; the engine grounds against those rows. Folds in DAT-377 as
the grounding-only frozen-artifact contract (ADR-0007).

What changed in the engine (calibration-relevant):
- **Deleted** `analysis/semantic/induction.py` (the `OntologyInductionAgent` +
  `induce_adhoc_concepts`), its `__init__` exports, and the
  `dataraum-config/llm/prompts/ontology_induction.yaml` prompt. The DAT-376 split
  stays — only induction's *home* moved (to the cockpit). The cycles / validation /
  graphs induction agents are **untouched**.
- **`semantic_per_column` is grounding-only.** The `if ontology == "_adhoc":
  induce_adhoc_concepts(...)` branch is replaced by a **fail-loud** guard: a cold-start
  `_adhoc` workspace with **zero** concept overlay rows now FAILS the phase with a clear
  error instead of grounding against an empty concept set. `ground_columns` is otherwise
  unchanged.
- The cold-start concept set is now produced by the **TS frame agent**, not the engine.
  Its prompt is the engine `ontology_induction.yaml` re-homed verbatim to
  `packages/cockpit/src/prompts/frame.ts`. Concept payload contract is unchanged
  (`OntologyConcept` field set; `core/overlay._apply_concept` consumes it as before).

Calibration impact: cold-start induction quality is now a cockpit (TS) concern. The
engine no longer makes the induction LLM call; evaluate induction against the TS frame
agent. Grounding (column→concept mapping) recall/precision is unaffected by this PR —
it still runs in the engine against the same concept rows. Recall coverage for the
relocated induction is handed to DAT-379/383.

## 2026-05-29: DAT-373 — stable typed Column ids + owner-scoped per-phase replay_cleanup (Option A)

Fixes the cross-stage data-loss hazard DAT-343 flagged: a type-teach replay used
to (a) drop the typed `Table` and cascade-wipe **every** per-Column row of **every**
stage, and (b) re-mint fresh `uuid4` typed Column ids on each re-type (orphaning
any other stage's per-Column rows even if cleanup were scoped). Both are fixed so
a future `begin_session` (DAT-356) / frame-ground (DAT-377) per-Column finding
survives an `add_source` teach. **No schema migration** (the `owner_stage`
discriminator, Option B, is a deferred fast-follow — not done here).

What changed in the engine:
- **Stable typed identity.** `resolve_types` + `TypingPhase._promote_strongly_typed`
  now RECONCILE the typed/quarantine `Table` + `Column` rows by
  `(source_id, table_name, layer)` / `(table_id, column_name)` — reuse + UPDATE in
  place, delete columns no longer present, insert genuinely new ones — instead of
  drop+recreate. Typed Table id AND typed Column ids are **unchanged across a
  re-type**. New shared helpers `reconcile_typed_table` / `reconcile_typed_columns`
  in `analysis/typing/resolution.py`.
- **`typing.replay_cleanup` is now in-place + owner-scoped.** It KEEPS the typed
  `Table`/`Column` rows; clears only typing-owned `TypeCandidate`/`TypeDecision`
  (raw + typed copies) and drops the DuckDB typed/quarantine tables (rebuilt by
  `_run`'s `CREATE OR REPLACE`). It NO LONGER deletes the typed `Table`, so it no
  longer cascade-wipes `StatisticalProfile` / `SemanticAnnotation` / temporal /
  quality / eligibility rows.
- **Per-phase owner-scoped `replay_cleanup`** added to `statistics`,
  `column_eligibility`, `statistical_quality`, `temporal` — each deletes only its
  OWN per-Column rows scoped to the replay's typed `table_ids`. The workflow now
  invokes `replay_cleanup_for_phase` for **every** phase that re-runs under a
  replay (`_maybe_replay_cleanup` gated by the new `_phase_reruns_on_replay`), not
  just `from_phase`; the source-level reduce always self-cleans.
- **`typing.should_skip`** now treats a typed table as "done" only if its columns
  still carry a `TypeDecision` (the row cleanup clears) — the surviving typed
  `Table` row alone is no longer the signal.
- **`BasePhase.replay_cleanup` docstring** now states the ownership contract:
  delete ONLY your own rows scoped to `table_ids`; NEVER delete a parent `Table`
  you don't exclusively own; the Table-delete cascade is reserved for
  `import`/source teardown.

### dataraum-eval

- **Eval action: NO recalibration needed.** No detector, prompt, threshold, or
  annotation-content change. Recall is unaffected: the re-type produces the same
  typed data + the same `TypeDecision`/`TypeCandidate` content as before; only the
  row identity (reuse vs. fresh uuid4) and the cleanup scope changed.
- **Eval-fixture flag:** any fixture or assertion that relied on a re-type
  **minting new typed `column_id`s** (or a new typed `table_id`) is now WRONG —
  ids are stable across replays. The cockpit integration smoke
  (`packages/cockpit/src/temporal/drive-add-source.ts`) asserted "every
  typed_table_id CHANGED" as proof `replay_cleanup` fired; that assertion must
  flip to assert ids are STABLE and that a seeded foreign per-Column row survives.
  Not changed in this lane (cross-PACKAGE, TS, not run here).

### Tests

- RED→GREEN hazard test + in-place semantics in
  `tests/unit/pipeline/test_phase_replay_cleanup.py`.
- Stable-id + downstream-skip update in `tests/unit/pipeline/test_typing_phase.py`.
- `_phase_reruns_on_replay` predicate in `tests/unit/worker/test_replay_scope.py`.
- New `tests/integration/pipeline/test_replay_cross_stage.py`: re-type keeps typed
  ids stable AND a simulated begin_session `SemanticAnnotation` on a typed column
  survives a re-type + statistics rebuild (real DuckLake substrate).

## 2026-05-29: DAT-376 — split induction ↔ grounding in `semantic_per_column` (structure-only)

Detached the two LLM steps inside `semantic_per_column` into independently
callable module-level functions, **in place** — no new pipeline stage, and
the `add_source` surface (workflow names, activity names, phase order,
`pipeline.yaml`, `contracts.py`) is byte-for-byte unchanged. This is a pure
extract-then-rewire; the phase `_run` is now a thin composer over the two
functions.

### dataraum-eval

- **Eval action: NO recalibration needed.** Recall is safe by construction —
  nothing that produces detector/annotation content changed:
  - The **ontology induction agent**, its prompt, and its tool schema are
    untouched (the extracted `induce_adhoc_concepts` wraps the *same*
    `OntologyInductionAgent.induce` call and the *same* per-concept
    `ConfigOverlay(type="concept", payload={"vertical":"_adhoc", ...})`
    insert + `session.commit()` as DAT-371's `_ensure_adhoc_ontology`).
  - The **`ColumnAnnotationAgent`** (the grounding step's worker), its prompt,
    its tool schema, and the `required_standard_fields` it receives from
    `GraphLoader(vertical=ontology).get_all_abstract_fields()` are unchanged.
  - **`persist_column_annotations`** row shapes are unchanged (reused verbatim).
  - All five `semantic_per_column` detectors are unchanged.
- **`replay_cleanup` is unchanged** — still drops `SemanticAnnotation` only and
  NEVER the induced `concept` `ConfigOverlay` rows. A new regression test pins
  this (`test_semantic_split_phases.py::TestPerColumnReplayCleanup`).

### The new seam (for DAT-377 / DAT-378)

`semantic_per_column` now composes two functions, both in
`dataraum.analysis.semantic` (and re-exported from its `__init__`):

- `induction.induce_adhoc_concepts(*, session, config, provider, renderer, table_ids) -> Result[int]`
  — cold-start `_adhoc` ontology induction. Short-circuits (returns `Result.ok(0)`)
  when concepts already exist; otherwise induces and inserts one `concept`
  overlay row per concept, then commits. The `if ontology == "_adhoc":` gate
  stays at the call site.
- `processor.ground_columns(*, session, config, provider, renderer, table_ids, ontology, session_id) -> Result[int]`
  — per-column annotation + `persist_column_annotations`, returns the row count.

This is the seam DAT-377/378 act on: the **connect/frame relocation moves the
induction CALL upstream** (induction belongs in `frame`, where the user declares
concepts before data — see the `project_frame_stage_ontology` memory), leaving
`add_source` / `semantic_per_column` calling **only** `ground_columns`. No
content change to either step is implied by that move — purely *where* the
induction call lives.

- **Status**: pending

## 2026-05-28: DAT-371 — `_adhoc` ontology induction moves to `concept` overlay rows

Follow-up to DAT-343 that unblocks DAT-339 user testing. The baked-in
config root is bind-mounted `:ro`, so `semantic_per_column`'s cold-start
`_adhoc` path (which used to `OntologyLoader.save()` back to
`verticals/_adhoc/ontology.yaml`) crashed with `OSError: Read-only file
system`. Induced concepts now persist as `config_overlay` rows.

### dataraum-eval

- **Eval action: no recalibration needed.** No detector logic changed;
  concept-content is still produced by the same LLM induction agent
  with the same prompt and tool schema. What changed is *where* the
  induced concepts live (Postgres overlay rows, not a YAML file) and
  the layered-read path that materializes them.
- **New `concept` overlay applier** in `dataraum.core.overlay`:
  ``verticals/<v>/ontology.yaml`` reads now merge concept rows
  (upsert-replace by `name`) **before** `concept_property` patches. If
  any eval fixture inserts both for the same vertical, the order matters
  — a `concept` row replaces a concept wholesale; subsequent
  `concept_property` rows for that concept patch on top.
- **`OntologyLoader.load` now routes through `load_yaml_config`** so the
  overlay applies. The in-loader cache is removed (live reads must
  reflect freshly-inserted rows). Eval fixtures that pass
  `verticals_dir=...` still bypass the overlay and are deterministic.
- **`OntologyLoader.save` is deleted.** Any eval helper that wrote a
  vertical YAML via the loader must switch to inserting `ConfigOverlay`
  rows (one per concept; `type='concept'`; payload includes `vertical`).
- **New `_adhoc` baseline ships at
  `packages/dataraum-config/verticals/_adhoc/ontology.yaml`** with
  `concepts: []`. The induction-on-cold-start path inserts overlay rows
  on top of this empty baseline.
- **Cockpit `concept` payload is now typed** (`ConceptPayload` in
  `teach.validation.ts`) mirroring `OntologyConcept` — required:
  `vertical` + `name`; everything else optional with passthrough.

### dataraum-testdata

- No testdata change required. Adhoc induction still happens on the
  same data shape; the only difference is the persistence substrate.

## 2026-05-28: DAT-343 — teach via Postgres `config_overlay` + remove-and-replay (E3)

DAT-343 retires the DAT-358 filesystem teach overlay and replaces it with a
per-workspace `ws_<id>.config_overlay` Postgres table. Teach edits flow
through that single seam; layered reads merge active rows over the
baked-in YAML via per-type appliers in `dataraum.core.overlay`. The
`addSourceWorkflow` grows an optional `replay: ReplayScope` input so the
cockpit can re-run the affected portion of the chain after a teach.

### dataraum-eval

- **Eval action: re-baseline.** This PR doesn't change detector logic, but it
  changes the substrate detectors observe AND the trigger surface that
  invalidates their inputs.
- **`relationship` detector now reads `ConfigOverlay`, not `DataFix`.**
  `entropy/detectors/structural/relations.py:_get_preferred_joins` queries
  rows of `type='relationship'` with `superseded_at IS NULL`. Payload shape
  changed from nested `{parameters: {table, target_table, ...}}` to flat
  `{source_id, table, target_table, ...}`. Any eval fixture writing the
  legacy shape needs updating. The detector lives in `semantic_per_table`
  which isn't in the slice-1 chain — no calibration impact in slice 1; flag
  for slice 2 when that phase joins.
- **`Relationship.is_confirmed` no longer gets stamped by user teaches.**
  `MetadataInterpreter._create_relationship` was the only writer; deleted in
  P3. `relationship_entropy` still reads `is_confirmed` and gives confirmed
  joins a lower entropy. Same slice-2+ latency — neither detector runs
  today, but when they do, user teaches will affect `join_path_determinism`
  scoring (cuts ambiguity) but NOT `relationship_entropy` scoring (the
  "confirmed" branch). Tracked as **DAT-372** (`Relationship.is_confirmed
  signal lost from relationship_entropy post-DAT-343`).
- **Per-Column cleanup is FK-cascade-driven, not per-phase-owned.** Critical
  for slice 2: `typing.replay_cleanup` deletes the typed `Table` row,
  SQLAlchemy cascade wipes its `Column` rows, and every per-Column row
  cascades from there. Works in slice 1 because `add_source` is the only
  stage writing per-Column. The moment `begin_session` lands and attaches
  findings to those same Columns, an `add_source` teach replay silently
  wipes them. Tracked as **DAT-373** (`Per-phase replay_cleanup ownership
  — required before begin_session writes per-Column data`); marked
  `Blocks DAT-356` (slice 2). Re-design needed: per-stage tables, or
  per-stage column identity, or scoped cascade declarations.
- **Replay paths re-run detectors.** A teach + `replay(from_phase="typing",
  raw_table_ids=[t])` re-runs typing + analytics + `detect_table` for that
  table → `type_fidelity`, `null_ratio` regenerate. A teach +
  `replay(from_phase="import", raw_table_ids=None)` re-runs the full source
  → all per-table detectors + `detect_source` (`business_meaning`,
  `unit_entropy`, `temporal_entropy`, `outlier_rate`, `benford`). On any
  replay the source-level reduce (`semantic_per_column` + `detect_source`)
  always re-runs — eval should expect detector outputs to refresh on every
  replay invocation, not just on initial `add_source` runs.
- **How to drive a teach round-trip**:
  1. `teach({type, payload})` → inserts a row in `ws_<id>.config_overlay`;
     returns `{overlay_id, type}`.
  2. (optional) batch more teaches.
  3. `replay({source_id, scope: ReplayScope, vertical?})` → starts
     `addSourceWorkflow` with `ReplayScope` carrying the from_phase + the
     raw_table_ids to narrow the fan-out. `workflow_id` is reused as
     `addsource-<source_id>` with `ALLOW_DUPLICATE` policy — Temporal UI
     shows iterations grouped per source. Returns the run_id; await via
     `client.workflow.getHandle(...).result()`.
  4. (undo) `undoTeach(overlay_id)` → sets `superseded_at = now()`. The
     row is still readable by audit queries but no longer participates in
     layered reads. Idempotent.
- **Cold-start regression — DAT-371 follow-up:**
  `semantic_per_column._ensure_adhoc_ontology` still writes
  `verticals/_adhoc/ontology.yaml` to the bind-mounted (read-only)
  baked-in config dir — OSError on every initial `add_source` run with the
  default `_adhoc` vertical. Workaround for now: pass an explicit
  `vertical` (e.g. `"finance"`) in the `SourceIdentity`. **DAT-371 blocks
  DAT-339 user testing**; the fix moves induced concepts to `concept`
  overlay rows via a new per-type applier.
- **Container-restart persistence is architecturally guaranteed**
  (Postgres-backed; survives engine + cockpit restarts). Spec asked for
  explicit verification — not added as a test. If you want it, a single
  `docker compose restart engine-worker` between a teach and a
  `getPendingOverlays` assertion is the minimum.
- **`DATARAUM_HOME` env + `dataraum_workspace` Docker volume retired.**
  Local dev setups holding stale data in that volume should
  `docker compose down -v` once before next bring-up.

### dataraum-testdata (hints)

- No new injection types needed — the substrate change doesn't introduce
  new detection surface.
- A teach-aware fixture set would be useful for slice-2 calibration: data
  with known mis-typing that a `type_pattern` teach should fix on replay.
  Not a slice-1 ask.

## 2026-05-27: DAT-370 follow-up — restore the source-level detectors (eval-caught regression)

Eval found that DAT-370 orphaned `semantic_per_column`'s detectors. When detectors
moved off the per-phase path, only `detect_table` (the table-local phases) was
wired; `semantic_per_column` runs as the source-level reduce but nothing ran its
declared detectors — `business_meaning`, `unit_entropy`, `temporal_entropy`,
`outlier_rate`, `benford` — so they were dead from DAT-370 until now.

Fix: added a source-level `detect_source` activity that runs after the reduce in
`addSourceWorkflow`, executing the `_SOURCE_LEVEL_PHASES` (= `semantic_per_column`)
detectors **source-wide** (`run_detector_post_step(table_ids=None)`; single
sequential step in the parent, no concurrency). Mirrors `detect_table`. A unit
guard (`test_no_chain_phase_detector_is_orphaned`) now fails if any chain phase
declares a detector that no detect step runs.

### dataraum-eval

- **Action: re-run the semantic detectors — they now produce scores.**
  `business_meaning`, `unit_entropy`, `temporal_entropy`, `outlier_rate`, `benford`
  execute once after the reduce, source-wide (same scope as the pre-DAT-370 coarse
  run). No detector logic changed — purely the missing execution path restored.
- Drive a run the same way (`addSourceWorkflow`); the new `detect_source` step is
  internal to the workflow.
- `relationship_entropy` / `join_path_determinism` (semantic_per_table) and the
  other Zone-2/3 detectors remain unwired — their phases aren't in the chain yet.

### dataraum-testdata (hints)

- None.

## 2026-05-27: DAT-370 — per-table fan-out for add_source (E4b-2)

The table is now the unit of work. `addSourceWorkflow` imports the source,
**fans out one `processTableWorkflow` child per raw table** (`asyncio.gather`),
then runs `semantic_per_column` once as the source-level reduce. Each child runs
the table-local chain scoped to its one table: `typing` (mints a typed id) →
`statistics` → `column_eligibility` → `statistical_quality` → `temporal` →
`detect_table`. This replaces DAT-368's coarse single pass over the whole source.

Two structural changes ride along:
- **Detectors moved off the per-phase path to a stage-level step.** They no
  longer run as a post-step after each phase; instead one `detect_table` step at
  the tail of each child runs the table-local detectors (`type_fidelity`,
  `null_ratio`) **scoped to that child's typed table**. `run_detector_post_step`
  gained a `table_ids` scope (delete-before-insert + scan restricted to the
  table) so parallel children never clobber each other's
  `(source_id, detector_id)` rows.
- **Message contract redesigned per-boundary.** The uniform
  `PhaseActivityInput`/`PhaseActivityResult` envelope is gone; activities take
  typed inputs (a `SourceIdentity` header + their real args) and the workflow
  returns `AddSourceResult { raw_table_ids, tables:[{raw_table_id, typed_table_id}] }`.

### dataraum-eval

- **Eval action: behavior-preserving — re-verify, don't expect a shift.** Same
  detectors, same per-table/per-column analysis; only granularity (per-table) and
  detector *timing* (once per table at stage end vs. once per source-wide phase)
  changed. The union of per-table detector records equals the old single
  source-wide run. **This is the per-table execution the eval gate was waiting
  on** — calibration can now run against the stabilized pattern.
- **How to drive a run**: start `addSourceWorkflow` (task queue
  `dataraum-pipeline`) with `AddSourceInput` = `{ identity: { workspace_id,
  source_id, session_id, vertical? } }`. It fans out per table and stops after
  `semantic_per_column`; `relationships` + `semantic_per_table` (slice-2) and
  teach (DAT-343) are still not in the chain.
- **If recall moves**: suspect the per-table detector scoping (`table_ids` in
  `run_detector_post_step`) or the per-table `should_skip` rewrites in the four
  analytics phases — those are the only behavioral touches.
- **Status**: per-table execution stabilized; eval unblocked to run in parallel.

### dataraum-testdata (hints)

- None. No detector or fixture surface changed; output is preserved.

## 2026-05-27: DAT-369 — de-monolith (retire the hand-rolled scheduler + monitoring)

Pure-cleanup follow-up to DAT-368. Now that the engine is a Temporal activity
worker, the hand-rolled orchestration is dead and gone: deleted the
scheduler/runner/setup/event-system, the `PipelineRun`/`PhaseLog` monitoring
tables + `pipeline/status.py`, the YAML dependency-DAG machinery (per-phase
`dependencies`/`produces`, `YAMLAwarePhase`, the transitive-dep helpers), the
MCP-only `investigation/recorder.py`, and `ConnectionManager.bind_session_id`.
The dead MCP surface moved out of the package to `reference/mcp/`. `TEMPORAL_*`
settings are now required/fail-loud.

### dataraum-eval

- **Eval action: none.** No detector, pipeline-phase behavior, response-shape,
  or Temporal-contract change. `pipeline.yaml` kept every phase's `description`
  + `detectors` (the worker still runs detectors as post-steps via
  `PhaseDeclaration.detectors`); only the unused DAG metadata was removed. The
  one behavioral touch — `enriched_views` `should_skip` now checks for an
  `EnrichedView` row instead of a `PhaseLog` "completed" row — is on a slice-2
  phase that calibration doesn't exercise yet.
- **Status**: no calibration impact; informational only.

### dataraum-testdata (hints)

- None. No detector or fixture surface changed.

## 2026-05-27: DAT-368 — slice-1 run surface lands (addSourceWorkflow)

The engine run surface that DAT-362 + DAT-341 calibration were **blocked on**
now exists. The engine is a Temporal worker; all seven slice-1 table-local
phases are registered as activities (`import`, `typing`, `statistics`,
`column_eligibility`, `statistical_quality`, `temporal`, `semantic_per_column`)
and the `addSourceWorkflow` workflow drives them in dependency order over a
source, then completes.

### dataraum-eval

- **What changed**: no detector or response-shape change — this is purely the
  *execution surface*. Phases now run through `dataraum.worker.run_phase_activity`
  (scoped Postgres session + a per-activity DuckDB cursor) and are orchestrated
  by `addSourceWorkflow`, instead of the in-process scheduler / `PipelineTestHarness`.
- **How to drive a run**: trigger `addSourceWorkflow` via the Temporal Client
  (task queue `dataraum-pipeline`) with `{workspace_id, source_id, session_id,
  vertical?, table_ids?}`. It runs **once over all the source's tables** (coarse;
  per-table fan-out + column batching is E4b-2 / DAT-370). It stops at
  `semantic_per_column` — `relationships` + `semantic_per_table` (slice-2) and
  teach (DAT-343) are **not** in the chain yet.
- **Calibrate**: the DAT-362 semantic-split calibration (business_meaning /
  unit_entropy recall vs. the pre-split baseline) can now actually run end-to-end
  through this surface. `semantic_per_table` detectors (`join_path_determinism`,
  `relationship_entropy`) remain un-runnable here until slice-2.
- **Status**: run surface ready; DAT-362 calibration unblocked.

### dataraum-testdata (hints)

- None. Same fixtures; this is an orchestration change, not a detector change.

## 2026-05-26: DAT-362 — semantic phase split (per-column + per-table)

The monolithic `semantic` phase is split into two pipeline phases (Option B):
`semantic_per_column` (annotates + **persists** columns on the balanced model)
and `semantic_per_table` (classifies tables + confirms relationships, reasoning
over the persisted annotations). The old single `analyze_schema` LLM call is gone.

### dataraum-eval

- **What changed**: the semantic detectors' *inputs* are produced differently,
  even though the detectors themselves are untouched:
  - **Column annotations now come from a column-only LLM call** that runs
    **before** relationships (table-local), instead of the old capable-model
    pass that saw relationship context. The deliberate trade (DAT-362 Option B):
    the LLM cross-table column-upgrade pass is **dropped**; human/agent teach
    between the phases is meant to replace it. This is the change most likely
    to move `business_meaning` recall.
  - **Unit detection moved**: the table-level `unit_relationships` backfill is
    removed. `unit_source_column` is now set **directly per column** by the
    per-column model (prompt `<unit_detection>`). Watch `unit_entropy`.
  - The per-column model tier changed `fast → balanced` (was a throwaway
    pre-pass; now authoritative). Net annotation quality should hold or improve.
  - `temporal_entropy`, `outlier_rate`, `benford` read the same persisted
    annotations — should be unaffected. `join_path_determinism`,
    `relationship_entropy` read relationships from `semantic_per_table` —
    same data, later phase.
- **Affected phases/detectors**: `semantic_per_column` produces `[semantic]`
  + detectors `business_meaning, unit_entropy, temporal_entropy, outlier_rate,
  benford`; `semantic_per_table` runs `join_path_determinism,
  relationship_entropy`. Downstream (`enriched_views`, `business_cycles`,
  `validation`, `data_fixes`) now depend on `semantic_per_table`.
- **Expected calibration outcome**: recall on `business_meaning` / `unit_entropy`
  is the open question — this is the first run of the next-gen split, and the
  user accepted that quality is validated here, in eval, not in-repo. If recall
  regresses, fix the per-column prompt (`column_annotation.yaml`) /
  `semantic_per_table.yaml`, not the detectors.
- **Calibrate**: full suite once the engine run surface lands (blocked on
  DAT-344 / E4, same as DAT-341). Compare `business_meaning` + `unit_entropy`
  recall against the pre-split baseline specifically.
- **Status**: pending (blocked on DAT-344)

### dataraum-testdata (hints)

- No new injection types required. If `unit_entropy` regresses, a targeted
  fixture with cross-column unit dimensions (e.g. a `currency_code` column
  defining units for several measures in one table) would exercise the new
  per-column unit-detection path directly.

## 2026-05-21: DAT-341 — workspace-typed substrate (slice 1 E1)

Substrate change: typed tables move from `lake.session_<id>` (per-session,
ephemeral) to `lake.{raw,typed,quarantine}.<source>__<table>`
(workspace-stable). `Table.workspace_id` and `EntropyObjectRecord.workspace_id`
FKs added (NOT NULL). `EntropyObjectRecord.session_id` stays NOT NULL but
is no longer the load-bearing scope.

### dataraum-eval

- **What changed (and what didn't)**: substrate-only refactor. Detector
  logic is unchanged; data reaching detectors is identical. The schema
  rename (`lake.session_<id>.typed_<x>` → `lake.typed."<x>"`) is the only
  surface-level shift, and it shows up in detector evidence strings as
  `<name>` instead of `typed_<name>` — cosmetic, not score-affecting.
- **Expected calibration outcome**: identical recall to pre-DAT-341.
  Eval's known-injection tests are deterministic; any drop in recall
  is a **bug** (a missed read site where some detector or analysis
  module still does `FROM "typed_<name>"` and now resolves to an empty
  schema slot), not "drift" or "expected variation". Investigate the
  failing detector's SQL — grep for hardcoded `typed_*` / `raw_*`
  prefixes that the substrate migration missed.
- **Calibrate**: run the full calibration suite as soon as the API
  surface lands (`dataraum-eval` calls into the engine via REST —
  blocked on DAT-344 / E4). Per the CLAUDE.md "calibration is the
  definition of done" rule, recall must not regress.
- **Notes**: workspace.db schema gained a `workspace_id` FK on `tables`
  and `entropy_objects`. Existing eval state on disk needs
  `rm -rf ${DATARAUM_HOME}` before the first calibration run.
- **Status**: pending (blocked on DAT-344)

### dataraum-testdata (hints)

- No new injection types required for this migration. The substrate change
  is structural and detector-agnostic.
- One directional hint: now that raw/typed/quarantine share a bare table
  name across layers, an injection that produces noisy raw data + clean
  typed data (e.g. "values DO TRY_CAST to numeric but the original
  strings have suspicious whitespace patterns") becomes easier to test —
  raw and typed are siblings in the catalog rather than schema-mates.
  Optional, not blocking.

## 2026-05-19: Open vendor bugs surfaced by eval tools-test port (NOT in PR #118)

While porting `calibration/tools/test_tool_chain.py` and friends to drive the
control plane over HTTP MCP, three real upstream bugs in `begin_session` /
`resume_session` / `look` / `run_sql` came out. These are **not fixed in
PR #118** — they need their own ticket(s) and an architectural call.

### Root cause: per-session lake schema + workspace-scoped entropy + resume that doesn't resume

Post-DAT-323 each `begin_session` creates a brand-new
`lake.session_<id>` schema. Pipeline writes (raw/typed/quarantine tables)
go to that schema. But entropy scores live in workspace Postgres
(`EntropyObjectRecord` keyed by `source_id`), so `_measure` sees scores
from the FIRST session that ran the pipeline and reports `status:complete`
regardless of which session is currently active.

Net effect when a user begins a second session on the same source:
- `measure()` returns the existing (workspace) scores — no pipeline trigger
- `look()` and `look(target=tbl)` work because they go through SQLAlchemy
  against workspace tables
- **`look(target=tbl, sample=N)` fails** — it executes
  `SELECT * FROM "typed_<src>__<tbl>" LIMIT N` on the per-session DuckDB
  cursor, which USEs an empty `lake.session_<new id>` schema
- **`run_sql` fails for raw-SQL paths that reference typed tables** — same
  reason; LLM repair masks this nondeterministically (sometimes patches
  the SQL with the schema prefix, sometimes doesn't, so the same test
  flips between PASS and XPASS)

DuckDB's error message even hints at the right schema:

```
Catalog Error: Table with name typed_detection_v1__invoices does not exist!
Did you mean "session_d71492d0_8e89_481d_8e4d_bfa49a284be1.typed_detection_v1__invoices"?
```

### The intended escape hatch (`resume_session`) is broken

`_restore_archived_session` in `src/dataraum/mcp/server.py:1481-1641` is
documented (and intended) to rebind the manager to the *existing*
`lake.session_<archived id>` schema — that's where the populated tables
live. The implementation instead calls `begin_session(...)` to mint a
**new** `InvestigationSession` id and binds the manager to that:

```python
# server.py:1619-1631
inv = begin_session(
    session,
    anchor_source_id,
    resume_intent,
    contract=archived_contract,
    vertical=archived_vertical,
)
new_session_id = inv.session_id
session_mgr.bind_session_id(new_session_id)   # ← wrong id; should be the archived session_id
```

So restoring an archive lands you in *another* empty lake schema. The
"data reused as-is" promise in the docstring (`# Pipeline data, snippets,
and teach overlays are reused as-is`) is false post-DAT-323 because the
schema isn't reused.

### Reproduction

```python
# Two fresh begin_sessions against the same source on a populated workspace
async with mcp_session(handle) as s:
    await call_tool(s, "add_source", {"name": "detection_v1", "path": "/var/lib/dataraum/sources/detection-v1"})
    await call_tool(s, "begin_session", {"source": "detection_v1", "intent": "first"})
    await call_tool(s, "measure", {})                      # triggers pipeline → populates lake.session_<id_A>
    await call_tool(s, "end_session", {"outcome": "delivered"})
    # Resume the archived session — supposedly attaches to id_A's schema
    archives = await call_tool(s, "resume_session", {})
    target = next(a["session_id"] for a in archives["archived_sessions"] if a["source"] == "detection_v1")
    await call_tool(s, "resume_session", {"session_id": target, "intent": "second"})
    # Should see typed data via raw SQL — fails because manager is bound to a NEW empty schema
    r = await call_tool(s, "run_sql", {"sql": "SELECT COUNT(*) FROM typed_detection_v1__invoices"})
    print(r)  # → "Catalog Error: Table ... does not exist! Did you mean session_<id_A>.typed_..."
```

### Design question (not just a one-line fix)

The architectural tension is: per-session lake schemas (DAT-323) make
session isolation clean, but the "resume" UX needs the resumed session
to see the prior session's data. Three plausible directions:

1. **Make `_restore_archived_session` pass the archived session_id to
   `bind_session_id` instead of a new one.** Loses the audit-trail
   benefit of a new `InvestigationSession` record per resume, but the
   schema reuse works. Probably 5-line patch.
2. **Pipeline data lives in a per-source schema (not per-session)** —
   `lake.source_<id>` instead of `lake.session_<id>`. Session schemas
   become a layer of overlays (teach, snippets, …) on top of shared
   pipeline data. Bigger refactor, cleaner UX.
3. **Resume copies the prior schema to the new session's schema.**
   Duplicates data on every resume; probably worst option.

### Where the bug bites in eval

Two ported tests live as `xfail(strict=True)` in
`calibration/tools/test_tool_chain.py` linked to this writeup:
`TestLookSample.test_sample_rows` and `TestRunSql.test_columns_metadata`.
Remove the `xfail` markers once the vendor fix lands.

### Status

- **PR #118** ships the seven other bugs we found end-to-end. This one is
  **not in it** — a fix would either be a 5-line patch with stronger
  semantic claims to make (option 1), or a real architectural change
  (option 2).
- **No urgency for the detector-recall eval** — that flow only uses
  `look` (short-name target) and `measure`, both of which work today.
- **Blocks the practitioner tools-test surface** — `look(sample)` and
  `run_sql` against typed tables can't be exercised reliably until this
  is fixed.

## 2026-05-19: DAT-325 — L6 Cutover (HTTP MCP is the only entrypoint; CLI + stdio + rich gone)

### dataraum-eval
- **Changed**: `pyproject.toml` (dropped `dataraum-mcp` script entry, dropped `typer` + `rich` deps), `src/dataraum/server/app.py` (mounts `/mcp/` Starlette sub-app behind bearer middleware; chained lifespans; `DATARAUM_MCP_TOKEN` refuse-to-start), `src/dataraum/mcp/server.py` (deleted `main()`, `run_server()`, `run_http_server()`, `_build_http_app()`, `_health()`, `_StreamableHTTPASGIApp`, `BearerAuthMiddleware`, `_TOKEN_ENV_VAR`, plus `hmac`/`stdio_server`/`StreamableHTTPSessionManager`/`sys` imports), `src/dataraum/mcp/__init__.py` (`run_server` re-export dropped), `src/dataraum/cli/` (entire tree deleted), `tests/unit/cli/` (deleted), `docs/cli.md` (deleted), `src/dataraum/core/logging.py` (Rich rendering path stripped — `LogBuffer`, `activate_console`/`deactivate_console`, `_build_text`, `_active_console`/`_active_log_buffer` globals gone; `_ProxyLogger.msg` always routes through stderr).
- **Affects**: **the calibration harness in dataraum-eval that currently shells out to `dataraum-mcp` over stdio is broken.** The script entry no longer exists; stdio is unreachable; the only transport is HTTP at `POST /mcp/` behind `Authorization: Bearer $DATARAUM_MCP_TOKEN`. **Per user (2026-05-19): do not block on this — eval gets adapted after L7.**
- **Adaptation path (post-L7)**:
  - **Option A (preferred):** spin up the control plane via `docker compose up -d --wait` (or `uvicorn dataraum.server.app:app` in-process for hermetic runs); set `DATARAUM_MCP_TOKEN` in the harness's env; talk to it over HTTP MCP (`mcp.client.streamable_http.streamablehttp_client(url, headers={"Authorization": f"Bearer {token}"})`). Most realistic — matches what shipping clients (Claude Code via `claude mcp add --transport http`) do.
  - **Option B (in-process, no transport):** import `from dataraum.mcp.server import create_server` and drive the MCP `Server` instance directly. Bypasses HTTP entirely; useful for unit-style calibration that doesn't need transport in the loop.
  - **Do NOT** try to reanimate stdio. The runner functions are gone; the import paths the eval harness used (`dataraum.mcp.run_server`, `dataraum.mcp.server.main`) raise `ImportError`.
- **No detector change. No tool surface change. No response shape change.** Same 12 MCP tools, same arguments, same outputs — only the transport that delivers them changed.
- **Env vars affecting eval**: `DATARAUM_MCP_TOKEN` (required) is the only addition. The DAT-323 set (`DUCKLAKE_CATALOG_URL`, `DUCKLAKE_DATA_PATH`, `DATABASE_URL`, `DUCKLAKE_PG_POOL_MAX`, `DUCKLAKE_SKIP_INSTALL`) still applies — see the DAT-323 handoff entry below.
- **Status**: pending — gated on L7 (DAT-326) merging first so eval has a stable integration smoke story to anchor against.

## 2026-05-19: DAT-323 — L4 DuckLake substrate (per-session DuckDB files → DuckLake)

### dataraum-eval
- **Changed**: `src/dataraum/server/storage.py` (new — process-wide DuckLake anchor on a named in-memory DuckDB; `bootstrap_lake` / `get_anchor` / `connect_session` / `teardown_lake` / `health_probe`), `src/dataraum/server/app.py` (FastAPI lifespan calls bootstrap + /health probes postgres + ducklake), `src/dataraum/core/connections.py` (`_init_duckdb` swap; new `_LakeScopedConnection` wrapper that intercepts `.cursor()` and `__enter__/__exit__` so cursors and cursor-of-cursors stay scoped to `lake.session_<id>`; new `bind_session_id()` method; `ConnectionConfig.duckdb_path` dropped), `src/dataraum/mcp/server.py` (three sites use `bind_session_id`), `src/dataraum/sources/{csv,json}/loader.py` (inline comment on the ephemeral `:memory:` schema-sniff carve-out), `src/dataraum/analysis/{statistics/profiler.py,statistics/quality.py,temporal/processor.py,correlation/within_table/derived_columns.py,relationships/joins.py,relationships/evaluator.py}` (8 `.cursor()` call sites converted from `cursor = X.cursor(); try: ...; finally: cursor.close()` to `with X.cursor() as cursor:` so they actually receive USE-scoped cursors via the recursive wrapper).
- **Affects**: the runtime substrate for **all** per-session pipeline data. v0.2.x's `~/.dataraum/sessions/{fp}/data.duckdb` files are gone — every per-session DuckDB connection is now opened against the named in-memory DB `:memory:dataraum_lake`, with the DuckLake catalog ATTACHed as `lake` and a per-session schema `lake.session_<id_clean>`. Pipeline writes (`raw_*`, `typed_*`, `quarantine_*`) and all analysis cursors resolve unqualified table refs against the session schema. No MCP tool surface change, no detector logic change, no response-shape change.
- **Eval setup that must change**: `tests/integration` and any calibration harness that constructs a `ConnectionManager` (directly or via `create_server`) now requires the DuckLake anchor to be bootstrapped first. Mirror the pattern in `tests/conftest.py` (worktree at `tests/conftest.py`): session-scoped `lake_catalog_url` + `lake_data_path` + `lake_anchor` fixtures, and an autouse `lake_clean` between tests to drop per-session schemas (CASCADE). MCP-flow tests need an autouse `lake_anchor` + `lake_clean` (see `tests/{unit,integration}/mcp/conftest.py` for the shape).
- **Calibrate**: no detector regressions expected (no detector code changed). Re-run cold-start `clean_eval` end-to-end to confirm the full pipeline runs against DuckLake: import → typing → semantic → relationships → correlations → temporal → graph_execution → entropy. Watch for: (a) any DDL pattern the lane smoke didn't cover (`TEMP TABLE` semantics, schema-qualified DROPs); (b) `CHECKPOINT` requirements — DuckLake buffers writes in memory until `CHECKPOINT`, so parquet files only appear under DATA_PATH after explicit flush; (c) pool ceiling under heavy parallel-phase load (`DUCKLAKE_PG_POOL_MAX` env, default 64).
- **Env vars introduced**: `DUCKLAKE_CATALOG_URL` (required, e.g. `postgresql://user:pw@host:5432/dataraum_lake_catalog`), `DUCKLAKE_DATA_PATH` (required, filesystem dir for parquet output), `DUCKLAKE_PG_POOL_MAX` (optional, default 64), `DUCKLAKE_SKIP_INSTALL` (optional — set to skip the cold-start `INSTALL ducklake` network round trip; container images should pre-install at build time).
- **Notes**:
  - **Archive design (Option A)**: DuckDB does not support `ALTER SCHEMA RENAME` (probed; "Altering schemas is not yet supported"). `end_session` no longer touches the lake schema — active vs archived is a workspace-DB flag (`ArchivedSession` row); `resume_session` rebinds via `bind_session_id(sid)`, USEing the existing `lake.session_<id>`. Schemas accumulate; lake-side GC deferred post-spine.
  - **Coverage gap (acknowledged, deferred)**: pipeline-phase integration tests under `tests/integration/{pipeline,analysis,...}` use the harness fixture `integration_duckdb` which is plain `duckdb.connect(':memory:')`. They validate phase logic in isolation from substrate, **not** against DuckLake. Substrate validation lives in `tests/platform/smoke_dat323.py` (12 lane-smoke tests) + MCP unit+integration tests. Per the user, deferred until after platform stabilization.
  - **Postgres pool config**: `SET GLOBAL pg_pool_max_connections` MUST run before the `ATTACH` (not via `postgres_configure_pool` post-attach, which doesn't propagate to DuckLake's catalog pool). `SET` without `GLOBAL` only affects the local connection.
- **Status**: pending

## 2026-05-14: DAT-299 — Concurrent per-metric LLM dispatch in graph_execution

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/phases/graph_execution_phase.py` (per-metric loop refactored: prep → execute (parallel/serial) → post), `src/dataraum/graphs/agent.py` (lock around `_code_cache`), `src/dataraum/core/connections.py` (docstring tightening only), `tests/unit/pipeline/test_graph_execution_dispatch.py` (new, 9 tests).
- **Affects**: `measure` / `_run_pipeline` wall clock during cold-start runs. Per-metric `agent.execute()` calls now dispatch concurrently via `asyncio.to_thread` + `asyncio.gather` with a semaphore cap of 5. **No MCP response shape or schema changes.** Per-metric results (snippets written, snippet promotion via inspiration_snippet_id delete) are functionally unchanged.
- **Calibrate**: graph-agent metric set wall-clock check on cold-start `clean_eval`. Expected: `graph_execution` phase drops from ~4-5 min sequential to ~60-90s on the same metric count. Snippets produced and metric correctness should be identical to pre-DAT-299 (the LLM is called the same number of times, just concurrently).
- **Notes**:
  - **Per-call resource isolation**: each parallel `agent.execute()` opens its own `manager.session_scope()` (auto-commit) and its own `manager.duckdb_cursor()`. The main `ctx.session` is untouched during parallel execution.
  - **Snippet promotion** (deleting the inspiration snippet after metric success) stays sequential on the main session, post-gather.
  - **Concurrency cap = 5** (hardcoded `_MAX_CONCURRENT_METRICS`). Sonnet 4.6 tier-3+ workspaces handle this easily; bump in the constant if profiling shows underutilization.
  - **Free-threading note**: `GraphAgent._code_cache` is now guarded by a `threading.Lock` because the same agent instance is shared across N concurrent workers; under PYTHON_GIL=0 the check-then-set was a race.
  - **Exception handling**: unexpected exceptions inside the parallel path (e.g. `session_scope` failing) are captured per-worker as `Result.fail(...)` — they no longer abort sibling workers via `asyncio.gather` propagation. The phase's failure semantics (`metrics_executed` / `metrics_failed` in `PhaseResult.outputs`, hard-fail when all failed) are unchanged.
  - **Serial fallback**: when `ctx.manager is None` (unit tests with no real connection manager), the phase falls back to the previous sequential loop with shared session/cursor. No behavior change for that path.
  - **Out of scope (deferred)**: cold-start induction parallelism across phases, AsyncAnthropic provider rewrite, configurable concurrency cap.
- **Status**: pending

## 2026-05-13: DAT-273 — Post-DAT-266 audit (dead symbols + db column + re-exports)

### dataraum-eval
- **Changed**: `src/dataraum/graphs/{models.py, __init__.py, induction.py, agent.py}`, `src/dataraum/entropy/db_models.py`, `src/dataraum/query/__init__.py`, `tests/integration/graphs/test_agent.py`
- **Affects**: nothing the eval harness consumes — pure code hygiene. No MCP tool, detector, pipeline phase, response shape, or behavior changes.
- **Calibrate**: nothing.
- **Notes**:
  - `entropy_objects.expires_at` column deleted. SQLAlchemy `create_all` is idempotent; existing workspaces keep the orphan column harmlessly. No wipe needed.
  - Deleted symbols (any eval-side reference would already be broken — none expected): `dataraum.graphs.StepValidation`, `dataraum.graphs.MetricScope`, `TransformationGraph.{scope, slice_dimension}`, `GeneratedCode.{graph_version, schema_mapping_id}`.
  - `dataraum.query.QueryAgent` no longer re-exported at package level — import via `dataraum.query.agent.QueryAgent`. Same for `QueryAnalysisOutput`, `QueryExecutionRecord`, `SQLSnippetRecord`, `SnippetGraph`, `SnippetLibrary`, `SnippetMatch`, `SnippetUsageRecord` — use the deeper `dataraum.query.{models, db_models, snippet_library, snippet_models}` paths. `QueryResult` + `answer_question` remain available from `dataraum.query`.
  - `induction.py` LLM tool schema no longer asks the model for a `validation` array — only affects metric induction prompt output.
- **Status**: pending

## 2026-05-13: DAT-284 — Quick wins (Sonnet 4.6 + graph prompt enrichment + has_trend)

### dataraum-eval
- **Changed**: `config/llm/config.yaml` (Sonnet 4.5 → 4.6 on `default_model` + `balanced`), `src/dataraum/graphs/context.py` (`ColumnContext.has_trend` field + populate + emit), `config/llm/prompts/graph_sql_generation.yaml` (new `<temporal_signals>` section).
- **Affects**: every LLM call routed through the `balanced` or `default` tier (semantic / column / validation / cycle / metric induction, graph SQL generation, enrichment, `why`). Graph SQL generation prompt now includes explicit `temporal_behavior` → aggregation guidance.
- **Calibrate**: graph-agent metric set smoke. Key scenarios:
  1. Existing finance metrics (DSO, gross_profit, current_ratio, etc.) still compute against `clean_eval` — no regression from added prompt context.
  2. Metrics on tables with `temporal_behavior: point_in_time` annotated columns (e.g. balance-sheet items) should pick the `end_of_period` aggregation pattern more reliably.
  3. Metric YAMLs whose declared `aggregation` conflicts with the column's `temporal_behavior` annotation — the LLM now explicitly trusts the column annotation and notes the override in assumptions.
- **Notes**:
  - **Model swap**: `claude-sonnet-4-5` → `claude-sonnet-4-6`. Sonnet 4.6 is the current generation; the short-form ID is canonical (no date suffix, matches existing Haiku pattern). Output format unchanged; structured-output prompts should remain stable but eval should validate.
  - **`has_trend` surface**: added as `bool | None` on `ColumnContext`, populated from `TemporalColumnProfile.has_trend` (only set for DATE/TIMESTAMP/TIMESTAMPTZ columns by construction). Emitted in the metadata-document's per-column Notes column as `"Trending over time."` when truthy. No DB schema change — `has_trend` was already persisted.
  - **`<temporal_signals>` prompt section**: bridges existing `temporal_behavior` semantic annotation to existing `<aggregation_types>` block. Includes conflict-resolution rule (trust the column annotation over a misaligned step aggregation). Explicitly notes that the `Trending over time.` note appears on the time-axis column and should be paired with the measure column's `temporal_behavior`.
  - **`detected_granularity` (AC7 second half)**: already emitted at `src/dataraum/graphs/context.py:1008-1009` for `table.time_column`. No code change in this PR.
  - **DAT-284 descope**: cold-start baseline + parallelism investigation (originally ACs 1, 3, 4, 5) split to **DAT-299** in v0.2.3. This PR is the quick-wins half (ACs 2, 6, 7, 8).
- **Status**: pending

## 2026-05-12: DAT-290 — Single source per session, multi_source pattern retired

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` (begin_session signature; new list_sources tool; multi_source filters purged; _orient_to_active_session shape fix), `src/dataraum/mcp/db_models.py` (ArchivedSession schema), `src/dataraum/pipeline/setup.py` (single-source resolution; fingerprint-of-set deleted), `src/dataraum/pipeline/phases/import_phase.py` (single-source dispatch; _load_registered_sources gone)
- **Affects**: every MCP call that goes through `begin_session`. The session-bound source must be selected explicitly. `_run_pipeline` semantics unchanged — still runs the pipeline against the active session's source.
- **Calibrate**: re-run MCP smoke / harness tests. Key adaptations the eval harness must make:
  1. `begin_session(source="<name>", intent="...", contract=...)` — `source` is required. Calling without it returns a schema-level error (`isError=True`). Calling with an unknown name returns a tool-level error that includes the list of available source names.
  2. `add_source(name="X", ...)` — calling twice with the same name now errors (`"Source 'X' already exists."`). The registry is append-only via `add_source`; use `SourceManager.remove_source` for archival (no MCP surface yet).
  3. New `list_sources` MCP tool — returns `{"sources": [{name, type, status, path, backend, recipe_tables}], "count": int}`. No URLs, no credentials. Use to discover what's registered before `begin_session`.
  4. Response shape change: `begin_session` and `resume_session` now return `source: "name"` (scalar). The previous `sources: [list]` field is gone — every session has exactly one source by construction. `resume_session()` archive listings have `source: "name"` per entry (was `sources: [list]`).
  5. `_orient_to_active_session` (idempotent-resume path) returns `source: "name"` to match.
  6. `multi_source` synthetic Source row no longer exists in session.db. Any eval code that filtered it out (`name != "multi_source"`) can be deleted.
- **Notes**:
  - **Workspace.db schema change**: `archived_sessions.source_names` (JSON list) → `archived_sessions.source_name` (scalar string). Existing workspaces with the old column require `rm -rf ~/.dataraum/` (consistent with DAT-192 / DAT-209 / DAT-286 precedent — v0.2.2 CHANGELOG documents this).
  - **What's deleted from the import phase**: `_load_registered_sources`, `_load_from_path`, `_detect_source_type`, `_get_or_create_source`, the `multi_source` row creation block, the silent per-source error swallowing that hid DAT-289's root causes.
  - **`setup_pipeline` runtime_config** changed shape — now carries `source_id`, `source_name`, `source_type`, `source_connection_config`, `source_backend`, `source_fingerprint` (single source). No `registered_sources` list, no `source_set_fingerprint`.
  - DAT-288 + DAT-289 close as superseded by this rework (no individual patches landed for them).
  - Cross-source analysis in a single session is **explicitly out of scope**. v0.4+ direction if it ever comes up: extend the recipe yaml to declare multiple connections (the recipe is already a multi-table aggregate), not reintroduce multi_source.
- **Status**: pending

---

*Older handoffs (2026-03 and earlier, v0.2.x packages — resolved) are archived in [archive/handoff-2026-03-and-earlier.md](./archive/handoff-2026-03-and-earlier.md).*
