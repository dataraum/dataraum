# Pipeline Redesign: Reactive Scheduling with Exit Quality Checks

**Status:** Draft
**Size:** XL
**Branch:** `refactor/stage-pipeline` (from `refactor/streamline`)

---

## The Fundamental Problem

The current orchestrator (~1000 lines) mixes five concerns in a single `while` loop:

1. **Scheduling** — which phases are ready to run
2. **Execution** — running phases in a thread pool
3. **Quality measurement** — post-verification detectors
4. **Gate handling** — blocking, presenting, resolving
5. **Display** — Rich Live, progress callbacks, event emission

This creates unsolvable problems: Live display conflicts with gate prompts, gate checking interleaves with scheduling, "not yet measured" confusion when producers and consumers are queued together.

Previous iterations tried to fix this with "stages" (grouping phases between predefined gate points). That papered over the real issue: the orchestrator has no clear model for when to pause, how events propagate, and how to resume. Stages are an artificial grouping — the phase contracts already encode everything the scheduler needs.

---

## Entry Criteria vs. Exit Criteria

A fundamental design choice that changes everything:

**Entry criteria (current model):** Block the CONSUMER if quality isn't good enough. The user learns about the problem when `statistics` can't start because `type_fidelity` is too high. But `typing` already ran — the problem existed the moment it finished. The user is told at the wrong time.

**Exit criteria (proposed model):** Check the PRODUCER on the way out. After `typing` completes, measure `type_fidelity`. If it's bad, present the issue immediately — with impact assessment ("this will block statistics, semantic, and eventually graph_execution"). The user decides: fix now, or defer and accept the risk.

In software quality gates, this is normal: you close a phase and verify its output before dependents proceed. We adopt the same model:

1. Phase completes
2. Post-verification measures quality
3. If issues found: present with downstream impact, let user fix or defer
4. If fixed: re-measure, close cleanly
5. If deferred: close with caveats, dependents proceed on whatever quality exists

**No blocked state. No deadlock detection.** Every phase runs when its dependencies are met. Quality is the producer's responsibility. The risk of deferral is the user's choice.

**What about cost protection?** If `type_fidelity` is 0.90 (90% of casts failed), running `semantic` (expensive LLM) is burning money. But the user already expressed their quality expectations through the selected contract. A `regulatory_reporting` contract (threshold 0.1) would flag this immediately. An `exploratory_analysis` contract (threshold 0.5) would too. If the user chose a contract that allows 0.90 — that's their explicit choice. For a safety net beyond contracts, a single global policy ("never run LLM phases if type_fidelity > 0.8") is sufficient — not per-phase magic numbers.

---

## Mental Model: Manufacturing Inspection Line

The pipeline is a manufacturing line. The workpiece is the metadata database. Each phase adds metadata. After each station, the inspector checks quality. If insufficient, the line stops for rework. The key difference from CI/CD: the workpiece is mutable and rework is in-place.

```
[Assembly]  →  [Exit Check]  →  [Rework?]  →  [Assembly]  →  [Exit Check]  →  ...
 (phase)       (detectors)      (fix/defer)    (phase)        (detectors)
```

- **The workpiece is mutable.** Unlike CI/CD (rebuild from scratch on failure), the metadata database can be fixed in-place. Re-running 20 phases is expensive; fixing one type annotation and resuming is cheap.
- **Inspection measures the process.** Entropy detectors measure uncertainty in the metadata extraction process ("how confident are we in these type inferences?"), not just whether values are correct. This is the SPC (Statistical Process Control) philosophy.
- **Passing defects downstream is more expensive than stopping now.** Running semantic analysis on garbage types wastes LLM tokens. The exit check warns the user before this happens.
- **Rework is targeted.** A fix addresses the specific issue. It doesn't restart the line.

### Prior art

| Domain | Pattern | What maps | What doesn't |
|--------|---------|-----------|--------------|
| **Manufacturing SPC** | Control charts, stop-the-line (andon cord) | Entropy scores = control charts. Gates = control limits. Fix-and-continue = andon. | SPC monitors continuous production; we run once per source. |
| **Cooper's Stage-Gate** | Go/Kill/Hold/Recycle decisions between stages | Go = proceed. Hold = pause for fix. Recycle = re-run after fix. | Stage-Gate is project management with human judgment; ours is mostly automated. |
| **CI/CD quality gates** | SonarQube blocks deployment if metrics fail | Threshold checking, pass/fail | CI/CD restarts from scratch. No in-place fix. |
| **Great Expectations** | Declarative data assertions, validation results | Detectors ≈ expectation suites | No pause/fix/resume lifecycle. |
| **Dagster asset checks** | `blocking=True` prevents downstream materialization | Blocking = our gate concept | No interactive fix loop. |
| **MLOps model gates** | Performance must exceed threshold before deployment | Validate artifacts before proceeding | Binary decision only; no structured remediation. |

**What makes our pattern distinct:** the combination of (1) automated measurement, (2) pipeline pause (not restart), (3) structured fix actions, and (4) resume from where you stopped. No single framework provides all four.

---

## What Is a Gate

A gate is an **exit quality check** after a phase (or batch of parallel phases) completes. It is not a blocking entry barrier — it's a producer-side verification with user-facing impact assessment.

A gate presents:

1. **What happened** — which dimensions changed, current scores
2. **Why it matters** — which contract thresholds are violated, with gap size
3. **What to do** — suggested fixes from the action registry, ranked by confidence

### Resolution options

| Action | What happens | When to use |
|--------|-------------|-------------|
| **Fix** | Apply a targeted action. Re-run detector. Re-check. | The issue is fixable and worth fixing now. |
| **Defer** | Record the issue as a caveat. Continue. Downstream phases run on whatever quality exists. | The issue should be tracked but not fixed now. |
| **Abort** | Stop the pipeline. Return partial results. | The data quality is fundamentally unusable. |

Note: "Skip" and "Defer" are the same thing — continue with a warning. The deferred issue appears in the final report.

### When gates fire

Gates fire at **natural pauses** — when a wave of parallel phases completes and no phases are currently running. The scheduler accumulates exit check issues from each completing phase and presents them as a batch at the natural pause. This means:

- If relationships, correlations, and temporal complete around the same time, the user sees ALL issues from that wave at once
- The user can prioritize: fix the one that violates the contract, defer the one that's within tolerance
- No per-phase interruption during parallel execution

### What gates are NOT

- Not entry barriers. Phases are not "blocked" — they run when deps are met.
- Not comprehensive quality reports. That happens in the entropy phase.
- Not artificial "stages." The pause points emerge from the DAG structure.

---

## How Fixes Work

A fix is a **targeted state change** that improves a specific quality dimension.

### The fix loop

```
Phase completes → post-verification detects quality issue → issue accumulated
  ↓
Natural pause (no phases running) → scheduler yields EXIT_CHECK with all issues
  ↓
Caller presents issues ranked by contract violation severity
  e.g., "type_fidelity: 0.65 — contract 'executive_dashboard' requires ≤ 0.20"
  ↓
Caller looks up available fixes from the action registry
  e.g., [1] override_type: cast to DECIMAL(10,2)  [2] defer  [3] abort
  ↓
User (or auto_fix policy) chooses
  ↓
Fix executor runs:
  1. Snapshot BEFORE — run detector, record score
  2. Apply change — update DB (e.g., TypeDecision.decided_type = DECIMAL)
  3. Snapshot AFTER — run detector, record new score
  4. Persist decision record (who, what, why, before→after)
  ↓
Caller sends resolution to scheduler
  ↓
Scheduler updates scores, resumes scheduling
  → If fixed: downstream phases proceed with improved quality
  → If deferred: downstream phases proceed anyway, issue recorded as caveat
```

### Fix actions — current and planned

**The fix registry should grow with the pipeline, not be limited to entropy dimensions.**

| Fix action | Category | When discovered | Blocking dimension |
|------------|----------|----------------|--------------------|
| `override_type` | TRANSFORM | After typing | type_fidelity |
| `confirm_relationship` | ANNOTATE | After relationships | join_path_determinism |
| `add_business_name` | ANNOTATE | After semantic | naming_clarity |
| `declare_unit` | ANNOTATE | After semantic | unit_declaration |
| `declare_null_meaning` | ANNOTATE | After statistics | null_ratio |
| `create_filtered_view` | TRANSFORM | After entropy | outlier_rate |
| `override_eligibility` | ANNOTATE | After column_eligibility | *(new)* |
| `override_temporal_column` | ANNOTATE | After temporal | *(new)* |
| `override_slice` | ANNOTATE | After slicing | *(new)* |
| `declare_constraint_exception` | ANNOTATE | After validation | *(new)* |
| `override_field_mapping` | ANNOTATE | After graph_execution | *(new)* |

Current: 6 actions. Planned: 11+. Each phase knows best what can go wrong and what the fix is.

### Deferred issues

Not every issue needs immediate attention. Issues that the user defers — or that the contract doesn't flag — appear in the final quality report and the context document as caveats. The consumer (query agent) can factor these into its confidence.

---

## Discover on the Way, Fix on the Way

### The problem with "entropy at the end"

The current pipeline runs 17 phases, then runs entropy detection as phase 18, then LLM interpretation as phase 19. Quality issues discovered at phase 18 are too late — all the expensive LLM phases (semantic, slicing, quality_summary, validation, business_cycles) already ran on potentially bad data.

### The solution: incremental measurement via post-verification

Each phase declares which entropy dimensions it affects. After a phase completes, the scheduler runs those specific detectors and updates the quality scores. Issues are discovered **at the point they occur**, not at the end.

### Detector availability — the true dependency map

All 11 detectors are pure computation on pre-assembled metadata (zero SQL queries). Their real dependencies:

| After this phase | Detectors that can run | Scores updated |
|---|---|---|
| typing | TypeFidelityDetector | type_fidelity |
| statistics | NullRatioDetector | null_ratio |
| relationships | JoinPathDeterminismDetector, RelationshipEntropyDetector | join_path_determinism, relationship_quality |
| semantic | OutlierRateDetector¹, BenfordDetector¹, BusinessMeaningDetector, UnitEntropyDetector, TemporalEntropyDetector | outlier_rate, benford_law, business_meaning, unit_entropy, temporal_entropy |
| temporal_slice_analysis | TemporalDriftDetector¹ | temporal_drift |
| quality_summary | DimensionalEntropyDetector | cross_column_patterns |

¹ Uses `semantic_role` to filter inapplicable columns (e.g., skip primary keys for outlier detection). Needs semantic for precision, not for core computation.

### Exit checks are contract-driven

After a phase completes and post-verification updates scores, the scheduler checks those scores against the **selected contract** — the same contract that evaluates the final pipeline output. No per-phase thresholds. No magic numbers in phase code.

Example: the user selected `executive_dashboard` (thresholds: `structural.types ≤ 0.20`, `structural.relations ≤ 0.20`, `semantic.business_meaning ≤ 0.20`, ...). After typing completes:

1. Post-verification runs `TypeFidelityDetector` → `type_fidelity = 0.65`
2. Scheduler checks `executive_dashboard.dimension_thresholds["structural.types"]` → 0.20
3. Violation: 0.65 > 0.20. Issue created with gap size (0.45), affected columns, suggested fix (`override_type`).
4. At the next natural pause, the issue is presented to the user.

This eliminates a second configuration layer. The contract is the single source of truth for "what quality level is acceptable." The same thresholds that evaluate the final report also drive incremental exit checks throughout the pipeline.

### The comprehensive entropy phase still runs

The entropy phase (all 12 detectors + Bayesian network) still runs as a normal phase. It provides:
- Cross-dimensional correlations (are type issues correlated with null issues?)
- Intent-level probabilities (is this data safe for aggregation?)
- Prioritized action recommendations
- The complete quality picture for the context document

But it's no longer the first time quality is measured — it's the final comprehensive assessment after incremental checks throughout.

---

## The Phase Contract

If you deleted the orchestrator and rebuilt it from just the Phase protocol, what do you need?

### Remove: `outputs`

The `outputs` property (`list[str]`) is declared on every phase but never read by the orchestrator. It's dead documentation. The only cross-phase data passing via `previous_outputs` is one case: import → typing passes `raw_tables`. This should be a DB query.

**Remove `outputs` from Phase protocol. Remove `previous_outputs` and `get_output` from PhaseContext.** Phases communicate through the database (SQLAlchemy models, DuckDB tables). This is already true for 19 of 20 phases.

### Remove: `entropy_preconditions`

The `entropy_preconditions` property (`dict[str, float]`) is a second configuration layer with hardcoded magic numbers that duplicates what contracts already provide. Only 3 of 20 phases declare it. The thresholds are opinions embedded in phase code (`{"type_fidelity": 0.3}`) that can't be adjusted without editing Python.

Contracts already define per-dimension thresholds, per-use-case, in YAML. The user selects a contract. The scheduler checks post-verification scores against that contract. One quality dial, not two.

**Remove `entropy_preconditions` from Phase protocol. Remove `check_preconditions` from `PipelineEntropyState`. Remove `_check_gate()` from the orchestrator.** The contract replaces all of this.

### The minimal Phase protocol

```python
class Phase(Protocol):
    name: str
    description: str
    dependencies: list[str]
    post_verification: list[str]               # dimensions to measure on exit
    run(ctx: PhaseContext) -> PhaseResult
    should_skip(ctx: PhaseContext) -> str | None
```

Five properties. One method. One conditional. That's the entire contract between a phase and the scheduler. The phase declares what it needs to run (`dependencies`), what quality it affects (`post_verification`), and how to execute (`run`). The scheduler handles scheduling, measurement, and quality evaluation — using the user-selected contract for thresholds.

## The Scheduler: Rebuilt from Phase Contracts

### Core loop: submit, complete, check, pause at natural breaks

```python
class PipelineScheduler:
    """Reactive scheduler. No stages. No blocking. Exit checks at natural pauses."""

    def __init__(self, phases: dict[str, Phase], contract: ContractProfile, max_workers: int = 4):
        self.phases = phases
        self.contract = contract
        self.max_workers = max_workers
        self.state: dict[str, PhaseStatus] = {n: PhaseStatus.PENDING for n in phases}
        self.scores: dict[str, float] = {}
        self.deferred: list[Issue] = []

    def run(self, ctx_factory) -> Generator[PipelineEvent, Resolution | None, PipelineResult]:
        pool = ThreadPoolExecutor(max_workers=self.max_workers)
        futures: dict[Future, str] = {}
        pending_issues: list[Issue] = []

        yield PipelineEvent(event_type=EventType.PIPELINE_STARTED)

        while True:
            # ── Find phases whose deps are met ──
            ready = [
                name for name, status in self.state.items()
                if status == PhaseStatus.PENDING and self._deps_met(name)
            ]

            # ── Submit ready phases ──
            for name in ready:
                if len(futures) >= self.max_workers:
                    break
                self.state[name] = PhaseStatus.RUNNING
                futures[pool.submit(self._execute, name, ctx_factory)] = name
                yield PipelineEvent(event_type=EventType.PHASE_STARTED, phase=name)

            # ── Wait for completions ──
            if futures:
                done, _ = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
                for f in done:
                    name = futures.pop(f)
                    result = f.result()
                    self.state[name] = result.status

                    # Post-verification: exit quality check
                    new_scores = self._post_verify(name, ctx_factory)
                    self.scores.update(new_scores)

                    # Assess downstream impact
                    issues = self._assess_impact(name, new_scores)
                    pending_issues.extend(issues)

                    yield PipelineEvent(
                        event_type=EventType.PHASE_COMPLETED,
                        phase=name, scores=dict(self.scores),
                    )

            # ── Natural pause: nothing running, issues accumulated ──
            if not futures and pending_issues:
                resolution = yield PipelineEvent(
                    event_type=EventType.EXIT_CHECK,
                    violations=self._format_issues(pending_issues),
                    scores=dict(self.scores),
                )
                self._apply_resolution(resolution, pending_issues)
                pending_issues.clear()
                continue

            # ── Done ──
            if not futures and not ready:
                break

        pool.shutdown(wait=False)
        return self._build_result()
```

### `_assess_impact`: check new scores against the contract

This is the key function. After a phase completes and post-verification runs, check which newly-measured dimensions violate the selected contract:

```python
def _assess_impact(self, completed: str, new_scores: dict[str, float]) -> list[Issue]:
    """Check if new scores violate the selected contract's thresholds."""
    issues = []
    for dim, score in new_scores.items():
        threshold = self.contract.dimension_thresholds.get(dim)
        if threshold is not None and score > threshold:
            issues.append(Issue(
                dimension=dim,
                score=score,
                threshold=threshold,
                caused_by=completed,
                contract=self.contract.name,
                suggested_fix=self._lookup_fix(dim),
            ))
    return issues
```

No iteration over phases. No per-phase thresholds. The contract is the single authority on what's acceptable.

### `_apply_resolution`: fix or defer

```python
def _apply_resolution(self, resolution: Resolution | None, issues: list[Issue]):
    if resolution is None or resolution.action == Action.DEFER:
        self.deferred.extend(issues)
    elif resolution.action == Action.FIX:
        # Fix executor already ran. Update scores from the fix result.
        self.scores.update(resolution.updated_scores)
        # Re-assess against contract: did the fix resolve all issues?
        remaining = [
            i for i in issues
            if self.scores.get(i.dimension, 1.0) > self.contract.dimension_thresholds.get(i.dimension, 0.0)
        ]
        self.deferred.extend(remaining)
    elif resolution.action == Action.ABORT:
        for name in self.state:
            if self.state[name] == PhaseStatus.PENDING:
                self.state[name] = PhaseStatus.SKIPPED
```

### That's ~100 lines of scheduling logic.

The rest is cleanly separated:

| Concern | Where | Est. lines |
|---------|-------|-----------|
| Scheduling + exit checks | `PipelineScheduler.run()` | ~100 |
| Phase execution + retry | `PipelineScheduler._execute()` | ~50 |
| Post-verification + contract check | `_post_verify()`, `_assess_impact()` | ~40 |
| Checkpoint save/restore | `CheckpointManager` (separate) | ~80 |
| CLI display + interaction | `cli/commands/run.py` | ~200 |
| Gate rendering + fix execution | `cli/gate_handler.py` | ~150 |
| MCP adapter | `mcp/server.py` | ~50 |

Total: ~670 lines across clean modules vs. ~1200 tangled today.

---

## The Caller: How CLI Drives the Scheduler

The generator IS the event system. No event queues, no callbacks. The generator yields events, the caller handles them. `yield`/`send` for gates.

```python
def run_pipeline_interactive(console, phases, contract, config):
    scheduler = PipelineScheduler(phases, contract=contract, max_workers=config.max_parallel)
    gen = scheduler.run(ctx_factory)

    event = next(gen)
    while True:
        try:
            if event.event_type == EventType.EXIT_CHECK:
                # Natural pause — nothing running, full terminal control
                render_exit_check(console, event)
                resolution = prompt_resolution(console, event)
                if resolution.action == Action.FIX:
                    execute_fix(resolution, config)
                event = gen.send(resolution)

            elif event.event_type == EventType.PHASE_STARTED:
                update_spinner(event.phase)
                event = next(gen)

            elif event.event_type == EventType.PHASE_COMPLETED:
                render_phase_result(console, event)
                event = next(gen)

            else:
                event = next(gen)

        except StopIteration as e:
            render_final_report(console, e.value)
            break
```

### The batch resolution UX

When parallel phases complete, their issues accumulate. At the natural pause, the user sees everything at once:

```
  ✓ relationships (1.8s)
  ✓ correlations (1.2s)
  ✓ temporal (0.9s)

  Exit check [executive_dashboard] — 2 issues found:

  1. structural.relations: 0.62 (contract requires ≤ 0.20, gap: 0.42)
     ↳ orders.customer_id → customers.id: 3 ambiguous candidates
     → [fix] confirm_relationship (87% confidence)

  2. semantic.temporal: 0.55 (contract requires ≤ 0.20, gap: 0.35)
     ↳ orders.created_at: role ambiguous (timestamp vs. date)
     → [fix] override_temporal_column

  Fix [1,2], defer all, or abort? _
```

The user sees issues **ranked by gap size** (how far from the contract threshold). Both violate the same contract — the user decides which to fix now vs. defer. If they'd chosen `exploratory_analysis` (threshold 0.50), issue 2 wouldn't even appear.

### Why this solves the terminal problem

At `EXIT_CHECK`, the generator is **suspended**. The thread pool is idle (no phases running, no futures pending). Nothing writes to stdout or stderr. The CLI has exclusive terminal ownership — render panels, prompt, execute fixes, re-render. Then `gen.send(resolution)` resumes the scheduler.

No `Live` display. No `set_live()`/`stop_live()`. No log capture during gates (nothing running = no logs).

During phase execution, the generator is inside `wait(futures, timeout=0.5)`. The CLI shows a spinner. Phase events arrive between `wait()` cycles.

### MCP caller: same generator, automatic resolution

```python
def run_pipeline_mcp(phases, contract, config, gate_mode, progress_callback):
    scheduler = PipelineScheduler(phases, contract=contract, max_workers=config.max_parallel)
    gen = scheduler.run(ctx_factory)

    event = next(gen)
    while True:
        try:
            if event.event_type == EventType.EXIT_CHECK:
                if gate_mode == "skip":
                    event = gen.send(Resolution(action=Action.DEFER))
                elif gate_mode == "fail":
                    event = gen.send(Resolution(action=Action.ABORT))
                elif gate_mode == "auto_fix":
                    event = gen.send(auto_resolve(event))
            else:
                if progress_callback:
                    progress_callback(event)
                event = next(gen)
        except StopIteration as e:
            return e.value
```

---

## What About Stages?

There are no stages. The display groups phases by the natural pauses (exit checks). If no issues are found, all phases run in one continuous stream. If issues are found after typing, the user sees a pause after "Foundation." If more issues after semantic, another pause.

The CLI can label these groups from a simple lookup (phase name → display label) or auto-generate from the phase that triggered the exit check. No `PhaseGroup` dataclass. No YAML stage config. The rhythm emerges from the data quality, not from configuration.

---

## Log Capture

During phase execution (between events), structlog output from worker threads can leak to the terminal. Two options:

1. **Buffer during execution, show after.** Redirect structlog to a StringIO buffer while phases run. After each phase completes, include captured warnings in the event. CLI shows them in the phase result display.

2. **Suppress in interactive mode.** In interactive mode, structlog goes to a file. In non-interactive mode (`--quiet` or piped), it goes to stderr as normal.

This is a CLI concern, not a scheduler concern. The scheduler never touches the terminal.

---

## The Final Report

After all phases complete, the pipeline produces a quality report:

1. **Per-phase results** — status, duration, metrics, warnings
2. **Entropy scores** — all dimensions, accumulated throughout the run via post-verification
3. **Deferred issues** — quality problems that were skipped at gates
4. **Gate decisions** — what was fixed, what was skipped, with before/after scores
5. **Contract evaluation** — the selected contract evaluated against final scores
6. **Context document** — the pre-computed metadata served to the query agent

The graph agent (graph_execution phase) prepares the computation foundation: business metrics mapped to concrete columns. The query agent then uses the full context document (including entropy scores, quality caveats, and computed metrics) to answer user questions.

---

## Migration: Clean Rewrite

No dual path. No backward compatibility shim. The phase implementations don't change. The storage layer doesn't change. The entropy detectors don't change. We rewrite the orchestration layer.

### What to delete

| File | Why |
|------|-----|
| `orchestrator.py` scheduling loop (~300 lines) | Replaced by `PipelineScheduler` |
| `orchestrator.py` gate checking (~200 lines) | Gates emerge from scheduling state |
| `orchestrator.py` progress callbacks (~100 lines) | Events are yields, not callbacks |
| `runner.py` (~150 lines) | Thin wrapper around scheduler |
| `cli/commands/run.py` Live display (~200 lines) | Replaced by event-driven rendering |
| `cli/gate_handler.py` Live management (~100 lines) | Handler always has full terminal |

### What to write

| File | Purpose | Lines (est.) |
|------|---------|-------------|
| `pipeline/scheduler.py` | `PipelineScheduler` with reactive loop | ~150 |
| `pipeline/runner.py` | Thin wrapper: build config → create scheduler → return result | ~80 |
| `cli/commands/run.py` | Event-driven rendering with gate prompts | ~250 |
| `cli/gate_handler.py` | Gate rendering + fix execution (simplified) | ~150 |

### What changes minimally

| File | Change |
|------|--------|
| `pipeline/base.py` | Remove `entropy_preconditions` from Phase protocol, remove `outputs`/`previous_outputs`/`get_output` |
| `pipeline/phases/base.py` | Remove `entropy_preconditions` property from BasePhase |
| `pipeline/phases/semantic_phase.py` | Remove `entropy_preconditions` override |
| `pipeline/phases/statistics_phase.py` | Remove `entropy_preconditions` override |
| `pipeline/phases/graph_execution_phase.py` | Remove `entropy_preconditions` override |
| `pipeline/phases/typing_phase.py` | Remove `get_output("import", "raw_tables")` → DB query |
| `pipeline/entropy_state.py` | Remove `check_preconditions()` (contract evaluation replaces it) |
| `pipeline/gates.py` | Simplify — gate violations come from contract, not from preconditions; replace `_DIMENSION_TO_ACTIONS` with `ActionRegistry.improves_dimensions` lookup |
| `pipeline/db_models.py` | Replace `PhaseCheckpoint` with `PhaseLog` (observability only), simplify `PipelineRun` |
| `pipeline/cleanup.py` | Remove checkpoint deletion (no longer exists); cleanup_phase stays for output deletion |
| `pipeline/status.py` | Derive status from `should_skip` + DB state, not from checkpoints |
| `entropy/fix_executor.py` | Add `improves_dimensions` to `ActionDefinition` |
| `mcp/server.py` | Update `_analyze()` to use new scheduler (~20 lines) |

### What stays unchanged

| File | Reason |
|------|--------|
| `pipeline/phases/*.py` (other 15) | Phase logic untouched — only remove dead property |
| `pipeline/events.py` | EventType, PipelineEvent |
| `entropy/*.py` | All detectors, processor, fix executor |
| `entropy/contracts.py` | Contract evaluation — now also used during pipeline, not just at end |
| `analysis/*.py` | All analysis modules |
| `storage/*.py` | All storage modules |

### Migration steps

1. **Write `PipelineScheduler`** — the reactive loop with contract-driven exit checks. Test with mock phases.
2. **Write CLI driver** — event-driven rendering with batch resolution UX. Test with mock scheduler.
3. **Add `Fix` model + replay** — fix persistence, `after_phase`, replay on re-run. Test with FixExecutor.
4. **Wire together** — replace `orchestrator.run()` call sites with scheduler.
5. **Delete old orchestrator code** — scheduling loop, gate interleaving, progress callbacks, PhaseCheckpoint.
6. **Migrate PhaseCheckpoint → PhaseLog** — DB migration, update status/CLI queries.

Each step: tests green.

---

## Dimension Name Mapping

The pipeline has three naming layers for entropy dimensions. The scheduler bridges them.

### The three layers

| Layer | Format | Example | Where used |
|-------|--------|---------|------------|
| **sub_dimension** (flat) | `snake_case` | `type_fidelity` | `post_verification`, `PipelineEntropyState`, detector `sub_dimension` |
| **dimension_path** (full) | `layer.dimension.sub_dimension` | `structural.types.type_fidelity` | `EntropyObject.dimension_path`, `ColumnSummary.dimension_scores` |
| **contract dimension** (2-part) | `layer.dimension` | `structural.types` | `ContractProfile.dimension_thresholds` |

### How the bridge works today

Each detector already declares all three parts:

```python
class TypeFidelityDetector(BaseDetector):
    layer = "structural"
    dimension = "types"
    sub_dimension = "type_fidelity"
    # → dimension_path = "structural.types.type_fidelity"
```

Contract evaluation in `_get_dimension_score()` uses **prefix matching**: contract dimension `structural.types` matches any `ColumnSummary` key starting with `structural.types.` — which finds `structural.types.type_fidelity`.

### What the scheduler needs

The scheduler receives flat names from `post_verification` (e.g., `type_fidelity`) and needs to check them against contract thresholds (keyed by `structural.types`). Two options:

**Option A: Build a lookup from detector registry.** At scheduler init, iterate all detectors and build `sub_dimension → contract_dimension` mapping:

```python
# Built once at startup from detector classes:
DIMENSION_MAP = {
    "type_fidelity": "structural.types",
    "null_ratio": "value.nulls",
    "outlier_rate": "value.outliers",
    "join_path_determinism": "structural.relations",
    "relationship_quality": "structural.relations",
    "business_meaning": "semantic.business_meaning",
    "unit_declaration": "semantic.units",
    "temporal_entropy": "semantic.temporal",
    "cross_column_patterns": "semantic.dimensional",
    "temporal_drift": "semantic.temporal",
    "benford_law": "value.outliers",
}
```

**Option B: Store scores by full path, let contract evaluation handle it.** After post-verification, store scores as `dimension_path` (3-part) instead of `sub_dimension` (flat). Then pass them directly to contract evaluation, which already does prefix matching.

**Recommendation: Option B.** It reuses the existing contract evaluation logic with zero new mapping code. The scheduler stores `{"structural.types.type_fidelity": 0.65}` instead of `{"type_fidelity": 0.65}`. The detector already knows its own `dimension_path`. This change is in `_post_verify()` — use `detector.dimension_path` as the score key instead of `detector.sub_dimension`.

The `post_verification` property on phases still declares flat names (for filtering which detectors to run). The dimension path is resolved at runtime from the detector that matches.

---

## How Fixes Are Proposed at Exit Checks

### The current gap

Today, gate-time fix proposals use a **static map** (`_DIMENSION_TO_ACTIONS` in `gates.py`):

```python
"type_fidelity": ["override_type"],
"naming_clarity": ["add_business_name"],
...
```

This is crude. Meanwhile, the `get_actions` MCP tool has a **rich 3-channel merge** that combines LLM interpretation, Bayesian network impact analysis, and contract violations into prioritized, per-column actions. But this only runs after the full entropy phase — far too late for incremental exit checks.

### The proposal: lightweight action lookup at exit checks, full merge at the end

Exit checks happen early in the pipeline (after typing, after relationships, after semantic). At that point, neither the LLM interpretation nor the Bayesian network has run yet. So the full `merge_actions()` pipeline isn't available. But we don't need it — the exit check is about a specific dimension violation on specific columns.

**At exit check time, the scheduler has:**
1. The violated contract dimension (e.g., `structural.types`)
2. The score and threshold (e.g., 0.65 vs. 0.20)
3. The producing phase (e.g., `typing`)
4. The affected columns (from the detector's per-column scores)

**What it needs:** which fix actions are available, with affected columns.

### Dimension-to-fix mapping via the ActionRegistry

The `ActionRegistry` already exists and maps action types to `ActionDefinition` objects. Extend it: each action declares which dimensions it can improve.

```python
@dataclass
class ActionDefinition:
    action_type: str
    category: ActionCategory          # TRANSFORM | ANNOTATE
    description: str
    hard_verifiable: bool
    parameters_schema: dict[str, Any]
    executor: Callable
    improves_dimensions: list[str]    # NEW: ["structural.types"]
```

At exit check time:

```python
def _lookup_fixes(self, dimension: str) -> list[ActionDefinition]:
    """Find actions that claim to improve this dimension."""
    return [
        action for action in self.registry.values()
        if dimension in action.improves_dimensions
    ]
```

This replaces the static `_DIMENSION_TO_ACTIONS` map. When new actions are added (via Python or YAML recipes), they self-declare which dimensions they improve. No central map to maintain.

### Affected columns come from the detector

Post-verification already runs detectors per-column. The exit check knows which columns violated the contract threshold. The fix proposal can be column-specific:

```
Exit check [executive_dashboard] — 1 issue:

  structural.types: 0.65 (contract requires ≤ 0.20, gap: 0.45)

  Affected columns:
    orders.amount       — 35% parse failures (VARCHAR → DECIMAL)
    orders.discount     — 12% parse failures (VARCHAR → FLOAT)
    payments.tax_rate   — 8% parse failures (VARCHAR → DECIMAL)

  Available fix: override_type
    [1] Fix orders.amount → DECIMAL(10,2)
    [2] Fix orders.discount → FLOAT
    [3] Fix payments.tax_rate → DECIMAL(10,4)
    [4] Fix all 3 columns
    [5] Defer

  Choose: _
```

### YAML recipes extend this naturally

From `docs_old/projects/fixes.md`: the long-term vision is declarative YAML recipes with `verify_with` declaring which dimensions they improve:

```yaml
declare_unit:
  category: annotate
  primitive: metadata_write
  model: TypeCandidate
  resolve_via: column_id
  writes:
    detected_unit: "{unit}"
    unit_confidence: 1.0
  parameters:
    unit: { type: str, description: "The unit (e.g., EUR, kg)" }
  verify_with: [unit_declaration]    # ← maps to semantic.units
```

The `verify_with` field is the same as `improves_dimensions` — it declares which detector dimensions this recipe improves. When recipes migrate from Python executors to YAML, the dimension mapping comes along for free.

### The full merge still runs at the end

After the comprehensive entropy phase, the full 3-channel merge (`merge_actions()`) runs as before:
- LLM interpretation generates contextual, per-column actions
- Bayesian network ranks by causal impact
- Contract violations highlight remaining gaps

This produces the rich `get_actions` output for the MCP tool and the final report. The incremental exit checks handled the obvious structural issues early; the final merge handles nuanced semantic and cross-column patterns.

---

## Checkpoints, Invalidation, and Resumption

### The current checkpoint model

Today: `PipelineRun` (run-level record) + `PhaseCheckpoint` (one per phase per source). On resume, the orchestrator loads all "completed" checkpoints for a source_id and skips those phases. `--force` deletes a phase's checkpoint + outputs via `cleanup.py`, forcing re-execution.

### Why checkpoints are redundant in the new model

The pipeline's resumption state is already encoded in the **database itself**. Each phase writes to specific tables (TypeDecision, SemanticAnnotation, Relationship, etc.). Each phase already has `should_skip(ctx)` — it queries whether its outputs exist. Typing checks for typed tables. Statistics checks for StatisticalProfile rows. Semantic checks for SemanticAnnotation rows. This is the idempotency mechanism.

In the new model:
- **Resumption** = phase's `should_skip(ctx)` returns a reason → phase already ran, skip it
- **Force re-run** = `cleanup_phase(name)` deletes outputs → `should_skip` returns None → phase runs
- **Scheduler state** = derived at startup: run `should_skip` for all phases, mark those with outputs as COMPLETED

No `PhaseCheckpoint` needed for scheduling. The DB outputs ARE the checkpoint.

### What checkpoints provided beyond resumption

| Feature | Checkpoint field | New home |
|---------|-----------------|----------|
| Timing/duration | `duration_seconds`, `started_at`, `completed_at` | `PhaseLog` — append-only observability record |
| Records processed | `records_processed`, `records_created` | `PhaseLog` |
| Gate status | `gate_status`, `gate_reason` | Removed — no gates in the new model |
| Entropy scores at completion | `entropy_hard_scores` | Scheduler's `self.scores`, persisted in `PipelineRun.final_entropy_state` |
| Phase outputs (JSON) | `outputs` | Removed — `outputs` removed from Phase protocol |
| Detailed metrics | `tables_processed`, `db_queries`, etc. | `PhaseLog` |

### The replacement: PhaseLog (observability only)

```python
class PhaseLog(Base):
    """Append-only log of phase executions. Does not drive scheduling."""
    log_id: str           # UUID
    run_id: str           # FK → PipelineRun
    source_id: str
    phase_name: str
    status: str           # completed, failed, skipped
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    records_processed: int
    records_created: int
    error: str | None
    warnings: JSON
    entropy_scores: JSON  # scores after post-verification
    fix_decisions: JSON   # fixes applied at exit check (if any)
```

Multiple logs per phase per source (one per run). Useful for: CLI status display, run history, performance tracking, debugging. Not used for scheduling decisions.

### `PipelineRun` stays, simplified

```python
class PipelineRun(Base):
    run_id: str
    source_id: str
    status: str              # running, completed, failed
    contract_name: str       # selected contract
    started_at: datetime
    completed_at: datetime
    final_entropy_state: JSON
    deferred_issues: JSON    # issues deferred at exit checks
    config: JSON             # for reproducibility
```

No `phases_completed`/`phases_failed` counters (derived from PhaseLog). No `gate_mode` (gates are now contract-driven). No `target_phase` (handled by scheduler, not stored).

---

## Invalidation: Delete and Go Back

### How `--force` works today

```
dataraum run ./data --phase statistics --force
  → cleanup_phase("statistics")  # deletes StatisticalProfile, QualityMetrics, etc.
  → delete PhaseCheckpoint for statistics
  → re-run statistics and its dependents
```

### How it works in the new model

Same mechanism, but simpler — no checkpoint to delete:

```
dataraum run ./data --phase statistics --force
  → cleanup_phase("statistics")  # deletes phase outputs from DB
  → statistics.should_skip(ctx) returns None  # no outputs found
  → scheduler runs statistics
```

### Fix-induced invalidation

When a fix modifies typing outputs (e.g., `override_type` changes a `TypeDecision`), downstream phases have stale data. The question: should the scheduler automatically invalidate and re-run them?

**Yes, but only for the directly affected chain.** Each phase's `dependencies` list already encodes the graph. When a fix targets a phase's outputs:

```python
def _invalidate_downstream(self, fixed_phase: str):
    """Delete outputs of phases that depend on the fixed phase."""
    to_invalidate = self._transitive_dependents(fixed_phase)
    for phase_name in to_invalidate:
        if self.state[phase_name] == PhaseStatus.COMPLETED:
            cleanup_phase(phase_name, self.source_id, session, cursor)
            self.state[phase_name] = PhaseStatus.PENDING
```

This is the same `cleanup_phase()` already used by `--force`. No new mechanism needed.

**Example:** User applies `override_type` fix at the exit check after typing. The scheduler:
1. Runs the fix → TypeDecision updated
2. Post-verifies → type_fidelity improves
3. Checks which completed phases depend on typing: `[statistics, column_eligibility, ...]`
4. Calls `cleanup_phase()` for each → their outputs deleted → `should_skip` returns None
5. Scheduler re-queues them as PENDING
6. Next scheduling cycle picks them up

### When NOT to invalidate

Not every fix warrants invalidation. An `add_business_name` fix after semantic only changes a `SemanticAnnotation.business_name` field. Downstream phases that depend on semantic (enriched_views, slicing, etc.) might not be affected — the business name is metadata for the context document, not an input to their computation.

**Heuristic:** Only auto-invalidate when the fix's `improves_dimensions` includes a dimension that a downstream phase's `post_verification` produces. This means the fix changes something that a downstream phase measured and reported on. Otherwise, just record the fix and let the final entropy phase re-evaluate.

**For v1:** Auto-invalidate all transitive dependents. It's aggressive but correct. Optimization (selective invalidation) comes later.

---

## Fix Application: Per-Phase, Not Global

### Why fixes are per-phase

All current fixes modify a specific phase's output:

| Fix | Modifies | Phase whose output changes |
|-----|----------|---------------------------|
| `override_type` | `TypeDecision.decided_type` | typing |
| `declare_unit` | `TypeCandidate.detected_unit` | typing |
| `add_business_name` | `SemanticAnnotation.business_name` | semantic |
| `declare_null_meaning` | `SemanticAnnotation.business_description` | semantic |
| `confirm_relationship` | `Relationship.is_confirmed` | relationships |
| `create_filtered_view` | DuckDB view | enriched_views |

If the producing phase re-runs, it overwrites the fixed value. So the fix must be replayed **after** the phase runs. This makes fixes inherently per-phase.

### The `Fix` model (from `docs_old/projects/fixes.md`)

```python
class Fix(Base):
    fix_id: str              # UUID
    source_id: str           # FK → Source
    action_type: str         # maps to ActionRegistry
    target: str              # "column:orders.amount"
    parameters: JSON         # {"target_type": "DECIMAL(10,2)"}
    after_phase: str         # "typing" — replay after this phase completes
    status: str              # active | applied | failed | superseded
    created_at: datetime
    last_applied_at: datetime | None
    last_applied_run_id: str | None
```

The key addition: **`after_phase`** — which phase this fix should be replayed after. Derived from the exit check where the fix was originally applied.

### Replay on re-run

When the scheduler completes a phase:

```python
def _replay_fixes(self, phase_name: str):
    """Replay active fixes that belong to this phase."""
    fixes = load_fixes(source_id=self.source_id, after_phase=phase_name, status="active")
    for fix in fixes:
        result = self.fix_executor.execute(fix)
        if result.success:
            fix.status = "applied"
            fix.last_applied_at = now()
        else:
            fix.status = "failed"
            fix.error = result.error
```

This happens in the scheduler between phase completion and post-verification:

```
Phase completes
  → Replay active fixes for this phase
  → Post-verify (detectors run on fixed state)
  → Check contract
  → If violations remain: present at next natural pause
  → If all pass: continue silently
```

If a replayed fix still resolves the issue (scores pass contract), no exit check fires. The user never sees it. If a fix fails (e.g., schema changed), it's marked failed and the exit check fires as if it's a new issue.

### Are there overarching fixes?

Today: no. All 6 fixes target one column in one phase's output. But the `Fix` model supports it:

**Table-level fixes** — e.g., "declare this table as a DIMENSION table" would modify `TableEntity.entity_type` (semantic phase output). Target: `"table:journal_entries"`. Still per-phase (`after_phase: "semantic"`).

**Cross-phase fixes** — e.g., "exclude this column from all analysis" would need to modify column_eligibility output AND potentially invalidate statistics, semantic, etc. This is really two fixes: one `override_eligibility` (after column_eligibility) + auto-invalidation of dependents.

**Global policy fixes** — e.g., "all currency columns should be DECIMAL(10,2)" would apply to multiple targets across the same phase. This is a **recipe** (from `docs_old/projects/fixes.md`) that expands to N individual fixes, one per matching column, all with `after_phase: "typing"`.

So: the execution unit is always per-phase. But the **specification** can be broader (a recipe that generates multiple per-phase fixes). The `Fix` model holds individual, per-phase fix instances. The recipe layer (future) generates them.

---

## `should_skip` Audit

For DB-derived resumption to work, every phase must reliably detect whether its outputs exist. Audit result:

**20/20 phases implement `should_skip`.** All use DB queries. 19/20 are reliable.

### Phases that need migration (currently check PhaseCheckpoint)

These 3 phases use `PhaseCheckpoint` existence as their skip signal. They need to check their own output tables instead:

| Phase | Currently checks | Should check | Model to query |
|-------|-----------------|--------------|----------------|
| `business_cycles` | `PhaseCheckpoint.status = "completed"` | `DetectedBusinessCycle` records for source_id | `COUNT(*) FROM detected_business_cycles WHERE source_id = ?` |
| `validation` | `PhaseCheckpoint.status = "completed"` | `ValidationResultRecord` records for source's tables | `COUNT(*) FROM validation_results` joined through table_ids |
| `quality_summary` | `PhaseCheckpoint.status = "completed"` | `ColumnQualityReport` records for source's slices | `COUNT(*) FROM column_quality_reports` joined through slice definitions |

All three write their own DB records — migration is straightforward.

### One weak implementation

`slice_analysis` compares a count heuristic (sum of `distinct_values` across slice definitions) against existing `Table` records with `layer="slice"`. This doesn't verify tables were actually created in DuckDB. Should be tightened to check actual table existence.

### All other 16 phases: reliable

They query their own output tables (TypeDecision, SemanticAnnotation, StatisticalProfile, etc.) and compare counts against expected totals. No changes needed.

---

## Concurrent Source Safety

### How it works today

`PipelineRun` has `status: str` (running/completed/failed). Before starting a run, the orchestrator creates a `PipelineRun` record with `status = "running"`. If another run starts on the same source, it can check for an existing running record.

### The new model keeps this

`PipelineRun` stays. It still has `status`. The lock mechanism is unchanged:

```python
# At scheduler startup:
existing = session.query(PipelineRun).filter(
    PipelineRun.source_id == source_id,
    PipelineRun.status == "running",
).first()
if existing:
    raise PipelineAlreadyRunning(f"Run {existing.run_id} is in progress")

# Create new run:
run = PipelineRun(run_id=..., source_id=..., status="running", contract_name=...)
session.add(run)
session.commit()  # lock acquired
```

On completion: `run.status = "completed"`. On failure: `run.status = "failed"`. On crash: stale "running" record — the CLI can detect this (`started_at` too old) and offer cleanup.

What changed is only that `PipelineRun` no longer has `PhaseCheckpoint` children driving resumption. Resumption comes from `should_skip`. But `PipelineRun` itself — the run record, the status field, the concurrency guard — stays exactly as it is.
