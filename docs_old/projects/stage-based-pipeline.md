# Stage-Based Pipeline Redesign

**Status:** Proposal
**Size:** XL (8+ files, 500+ lines changed)
**Branch:** `refactor/stage-pipeline` (from `refactor/streamline`)

## Problem Statement

The current pipeline orchestrator interleaves entropy gate checking with phase scheduling in a single `while work_queue or active_futures` loop. This creates three unsolvable problems:

1. **No natural pause points.** The Rich `Live` display owns the terminal via a background refresh thread. When a gate fires, the orchestrator calls `gate_handler.resolve()` from inside the scheduling loop — but `Live` is still running, so `Prompt.ask()` output gets overwritten. Stopping/starting `Live` externally is fragile and breaks its internal state.

2. **Two conflicting dependency systems.** Metadata dependencies (`dependencies: list[str]`) control execution order within the ThreadPoolExecutor. Entropy preconditions (`entropy_preconditions: dict[str, float]`) control quality gates. Both are evaluated in the same loop, creating "not yet measured" confusion when a dimension's producer phase hasn't run yet because it's queued alongside the phase that needs its score.

3. **Log pollution.** structlog writes to stderr, Rich writes to stdout. In interactive mode, structlog `[warning]` lines from phase execution leak through and corrupt the Live display. Suppressing them requires either silencing all warnings (losing useful diagnostics) or capturing stderr (complex and fragile).

### What the user sees today

```
$ dataraum run ./testdata --gate-mode pause
2024-01-15 10:23:45 [warning  ] low_cardinality_detected   column=region ...
Pipeline [3/19]  Running: statistics, correlations
  entropy: type_fidelity=0.234
2024-01-15 10:23:46 [warning  ] null_ratio_high            column=amount ...
  gate: semantic — BLOCKED
```

Mixed structlog + Rich output. No prompt. No interaction. Pipeline exits.

### What the user should see

```
$ dataraum run ./testdata --gate-mode pause
Stage 1/5: Foundation ✓ (2.1s)
  import ✓  typing ✓
  type_fidelity: 0.234

Stage 2/5: Profiling ✓ (4.3s)
  statistics ✓  column_eligibility ✓  relationships ✓  correlations ✓  temporal ✓
  join_path_determinism: 0.612

━━━ GATE: Semantic Readiness ━━━━━━━━━━━━━━━━━━━━━━
  join_path_determinism: 0.612 (max 0.500)
    ↳ orders.customer_id → customers.id: ambiguous (3 candidates)

  [1] fix: deduplicate join path orders→customers (87%)
  [2] skip: continue with warnings
  [3] inspect: show affected rows

  Or ask a question about this gate: _
```

Clean output. Natural pause. Full interaction.

---

## Design Principles

1. **Stages own the terminal.** The CLI runs a sequential `for stage in stages` loop. Between stages, the CLI owns stdout — no Live, no threads, just Rich panels and Prompt.ask().
2. **Within a stage, phases run in parallel.** ThreadPoolExecutor + metadata dependencies, exactly as today. No entropy checks inside a stage.
3. **Gates fire between stages, not between phases.** Each stage declares an exit gate (entropy thresholds). The gate is checked after all phases in the stage complete.
4. **structlog captured during execution.** Phase threads write to a StringIO buffer. After the stage completes, warnings are summarized in the CLI output (not streamed live).
5. **The orchestrator doesn't know about the terminal.** It runs stages, returns results. The CLI decides how to display them.

---

## Stage Model

### Stage Definition

```python
@dataclass(frozen=True)
class Stage:
    name: str                              # e.g., "foundation"
    display_name: str                      # e.g., "Foundation"
    phases: list[str]                      # Phase names in this stage
    exit_gate: dict[str, float] | None     # Entropy thresholds to check after stage
                                           # e.g., {"type_fidelity": 0.5}
    order: int                             # Execution order (1-based)
```

### Stage Configuration (pipeline.yaml)

```yaml
stages:
  - name: foundation
    display_name: Foundation
    order: 1
    phases: [import, typing]
    exit_gate:
      type_fidelity: 0.50

  - name: profiling
    display_name: Profiling & Structure
    order: 2
    phases: [statistics, column_eligibility, relationships, correlations, temporal, statistical_quality]
    exit_gate:
      join_path_determinism: 0.50
      type_fidelity: 0.30

  - name: semantic
    display_name: Semantic Analysis
    order: 3
    phases: [semantic]
    exit_gate:
      naming_clarity: 0.40

  - name: views
    display_name: Views & Quality
    order: 4
    phases: [enriched_views, slicing, slice_analysis, temporal_slice_analysis, quality_summary, entropy, entropy_interpretation, validation, business_cycles]
    exit_gate: null  # No gate — proceed to final stage

  - name: computation
    display_name: Business Intelligence
    order: 5
    phases: [graph_execution]
    exit_gate: null  # Final stage, no exit gate
```

### Why these 5 stages

| Stage | Phases | Gate After | Rationale |
|-------|--------|------------|-----------|
| **Foundation** | import, typing | type_fidelity ≤ 0.50 | Types must be clean before statistics. `typing` produces `type_fidelity` via post-verification. |
| **Profiling & Structure** | statistics, column_eligibility, relationships, correlations, temporal, statistical_quality | join_path_determinism ≤ 0.50, type_fidelity ≤ 0.30 | Structural analysis must be clean before semantic interpretation. `relationships` produces `join_path_determinism`. Tighter type_fidelity gate (0.30 vs 0.50) catches regressions. |
| **Semantic** | semantic | naming_clarity ≤ 0.40 | Naming must be clear before graph execution. `semantic` produces `naming_clarity`. |
| **Views & Quality** | enriched_views through business_cycles (9 phases) | none | These phases build on clean semantic foundation. No new entropy dimensions gated. |
| **Computation** | graph_execution | none | Final aggregation. Entry was gated by Stage 3's exit gate on naming_clarity. |

### Mapping from current `entropy_preconditions`

The current phase-level preconditions map directly to stage exit gates:

- `statistics.entropy_preconditions = {"type_fidelity": 0.5}` → Stage 1 exit gate
- `semantic.entropy_preconditions = {"type_fidelity": 0.3, "join_path_determinism": 0.5}` → Stage 2 exit gate
- `graph_execution.entropy_preconditions = {"type_fidelity": 0.3, "naming_clarity": 0.4}` → Stage 3 exit gate

The `entropy_preconditions` property on phases becomes documentation-only — the stage config is the source of truth for gating.

---

## New Orchestrator Design

### Current: Single Loop

```
Pipeline.run():
    while work_queue or active_futures:
        pop phase → check deps → check gate → submit to pool
        collect futures → post-verify → update entropy state
        if gate_blocked and handler: resolve gate
```

### New: Two-Level Loop

```
Pipeline.run_stages(stages):
    for stage in stages:
        result = self._run_stage(stage)     # ThreadPoolExecutor inside
        if result.failed:
            return PipelineResult(failed)

        if stage.exit_gate:
            gate_result = self._check_exit_gate(stage)
            yield StageResult(stage, result, gate_result)
            # Caller decides what to do with gate_result
```

### Key change: `run_stages()` is a generator

The orchestrator **yields** after each stage, giving the caller (CLI or MCP) control. The caller inspects the `StageResult`, handles any gate, then calls `next()` to continue.

```python
class Pipeline:
    def run_stages(
        self,
        stages: list[Stage],
        manager: ConnectionManager,
        source_id: str,
        phase_configs: dict[str, dict],
        runtime_config: dict[str, Any],
    ) -> Generator[StageResult, GateResolution | None, PipelineResult]:
        """Run pipeline stage by stage, yielding after each for gate handling.

        The caller sends a GateResolution back (or None to continue).
        Returns the final PipelineResult when all stages complete.
        """
        for stage in stages:
            # Run all phases in this stage (parallel, metadata deps only)
            stage_result = self._run_stage(stage, manager, source_id, phase_configs, runtime_config)

            # Check exit gate
            gate = None
            if stage.exit_gate and stage_result.all_completed:
                violations = self._entropy_state.check_preconditions(stage.exit_gate)
                if violations:
                    gate = build_gate(
                        gate_id=f"stage_{stage.name}",
                        gate_type="stage",
                        blocked_phase=stage.name,  # Stage name, not phase
                        violations=violations,
                        entropy_state=self._entropy_state.to_dict(),
                        manager=manager,
                        source_id=source_id,
                    )

            # Yield to caller with stage results + optional gate
            resolution = yield StageResult(
                stage=stage,
                phase_results=stage_result.phase_results,
                gate=gate,
                entropy_scores=self._entropy_state.to_dict(),
                duration_seconds=stage_result.duration_seconds,
            )

            # Process resolution if gate was fired
            if gate and resolution:
                if resolution.action_taken == GateActionType.FIX:
                    # Fix was applied, re-verify
                    new_violations = self._entropy_state.check_preconditions(stage.exit_gate)
                    if new_violations:
                        # Fix didn't fully resolve — caller can retry or skip
                        pass
                elif resolution.action_taken == GateActionType.SKIP:
                    pass  # Continue to next stage
                # FIX_ALL, INSPECT, QUESTION handled similarly

        # All stages complete
        return self._build_pipeline_result()
```

### `_run_stage()` — Parallel phase execution within a stage

This is the existing scheduling loop, simplified by removing gate checks:

```python
def _run_stage(self, stage: Stage, manager, source_id, phase_configs, runtime_config) -> _StageExecResult:
    """Run all phases in a stage using ThreadPoolExecutor.

    Phases are ordered by metadata dependencies only.
    No entropy gate checks inside this method.
    """
    phases_to_run = [self.phases[name] for name in stage.phases if name not in self._completed]
    work_queue = deque(sorted(phases_to_run, key=lambda p: self._phase_priority.get(p.name, 0), reverse=True))
    active_futures: dict[Future, str] = {}
    phase_results = []

    with ThreadPoolExecutor(max_workers=self.config.max_parallel) as pool:
        while work_queue or active_futures:
            # Schedule ready phases
            not_ready = deque()
            while work_queue:
                phase = work_queue.popleft()
                deps_met = all(
                    d in self._completed or d in self._skipped
                    for d in phase.dependencies
                    if d in {p.name for p in phases_to_run}  # Only check intra-stage deps
                )
                if deps_met:
                    future = pool.submit(self._run_phase, phase.name, manager, source_id, phase_configs, runtime_config)
                    active_futures[future] = phase.name
                    self._running.add(phase.name)
                else:
                    not_ready.append(phase)
            work_queue = not_ready

            # Collect completed futures
            if active_futures:
                done, _ = wait(active_futures.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    phase_name = active_futures.pop(future)
                    result = future.result()
                    self._running.discard(phase_name)
                    # ... handle result, post-verify, update entropy state
                    phase_results.append(result)

    return _StageExecResult(phase_results=phase_results, ...)
```

### What's removed from the orchestrator

- `_check_gate()` calls inside the scheduling loop
- `_gate_blocked` / `_gate_attempts` tracking
- Gate handler invocation
- `_notify_progress()` / `ProgressCallback` (already removed)
- `progress_callback` parameter
- Gate resolution retry logic
- The `while True` outer loop that re-queues gate-blocked phases

### What stays

- `_run_phase()` with retry/backoff for SQLite contention
- `_execute_phase()` with PhaseContext construction
- `_run_post_verification()` after phase completion
- `PipelineEntropyState` updates
- `_compute_phase_priority()` for scheduling order
- Checkpoint saving/loading
- Event emission (`_emit_event`)
- All phase implementations unchanged

---

## New CLI Runner

### Current flow (run.py)

```python
def run(...):
    # 50 lines of arg validation
    # 30 lines of contract selection
    # 70 lines of Live display setup (closures, mutable state)
    # 10 lines of RunConfig
    with Live(...) as live:
        result = run_pipeline(config)   # Blocks until done. No interaction.
    # 160 lines of result display
```

### New flow

```python
def run(...):
    setup_logging(verbosity=verbose, log_format=log_format)
    validate_args(...)
    config = build_run_config(...)

    # Contract selection (before pipeline starts)
    if is_interactive and contract is None:
        contract = prompt_contract_selection(console)

    # Create pipeline generator
    pipeline_gen = create_and_run_pipeline(config)

    # Stage-by-stage execution with CLI control
    for stage_result in pipeline_gen:
        # Display stage summary (no Live needed — stage is done)
        render_stage_result(console, stage_result)

        # Handle gate if fired
        if stage_result.gate:
            resolution = gate_handler.resolve(stage_result.gate)
            pipeline_gen.send(resolution)

    # Final summary
    pipeline_result = pipeline_gen.value  # Generator return value
    render_pipeline_summary(console, pipeline_result)
```

### Key insight: No `Live` display needed

The stage-based model eliminates the need for Rich `Live`:

1. **During stage execution** (2-10 seconds): Show a simple spinner with `console.status()` — this is non-interactive and doesn't conflict with anything.
2. **Between stages**: Print completed results. This is plain `console.print()` — no refresh thread, no terminal ownership conflict.
3. **At gate prompts**: Full terminal control. Rich panels + `Prompt.ask()` work perfectly because nothing else is writing to the terminal.

```python
def run_interactive(console, pipeline_gen, gate_handler):
    """Run pipeline interactively, stage by stage."""
    for stage_result in pipeline_gen:
        # Stage header
        console.print(f"\n[bold]Stage {stage_result.stage.order}/{total}: {stage_result.stage.display_name}[/bold]")

        # Phase results (compact)
        for pr in stage_result.phase_results:
            icon = {"completed": "✓", "failed": "✗", "skipped": "○"}[pr.status]
            console.print(f"  {icon} {pr.phase_name} ({pr.duration:.1f}s)")

        # Entropy scores produced
        if stage_result.entropy_scores:
            for dim, score in stage_result.entropy_scores.items():
                bar = render_entropy_bar(score)
                console.print(f"  {dim}: {score:.3f} {bar}")

        # Gate resolution
        if stage_result.gate:
            resolution = gate_handler.resolve(stage_result.gate)
            pipeline_gen.send(resolution)
```

### Spinner during stage execution

For the period while phases are running (typically 2-10s per stage), use `console.status()`:

```python
def create_and_run_pipeline(config) -> Generator[StageResult, ...]:
    """Wrapper that shows a spinner during each stage."""
    pipeline = create_pipeline(config)
    stages = load_stages(config)

    gen = pipeline.run_stages(stages, ...)
    for stage_result in gen:
        yield stage_result
```

The spinner runs in the CLI layer, not inside the orchestrator. The orchestrator is a pure generator — no terminal awareness.

### Event callbacks still work

The orchestrator still emits `PipelineEvent`s via `_emit_event()`. The CLI can optionally subscribe to update the spinner text:

```python
with console.status("Running...") as status:
    def on_event(event):
        if event.event_type == EventType.PHASE_STARTED:
            status.update(f"Running {event.phase}...")

    pipeline.event_callback = on_event
    stage_result = next(gen)  # Blocks until stage completes
```

This is safe because `console.status()` is designed for concurrent updates (unlike `Live`).

---

## Log Capture Strategy

### Problem

structlog output from phase threads leaks into the terminal during interactive mode. Currently:
- Phase code calls `logger.warning("null_ratio_high", column="amount")`
- structlog renders to stderr immediately
- Rich renders to stdout
- User sees interleaved garbage

### Solution: Per-stage log buffer

```python
import io
import logging

class LogCapture:
    """Redirect structlog output to a buffer during stage execution."""

    def __init__(self):
        self._buffer = io.StringIO()
        self._handler = logging.StreamHandler(self._buffer)
        self._original_handlers = []

    def __enter__(self):
        root = logging.getLogger()
        self._original_handlers = root.handlers[:]
        root.handlers = [self._handler]
        return self

    def __exit__(self, *exc):
        root = logging.getLogger()
        root.handlers = self._original_handlers

    def get_warnings(self) -> list[str]:
        """Extract warning-level messages from buffer."""
        return [line for line in self._buffer.getvalue().splitlines() if line.strip()]
```

Usage in CLI:

```python
for stage_result in pipeline_gen:
    render_stage_result(console, stage_result)

    # Show captured warnings (summarized, not raw)
    if stage_result.captured_warnings:
        console.print(f"  [yellow]{len(stage_result.captured_warnings)} warnings[/yellow]")
        if verbose >= 1:
            for w in stage_result.captured_warnings:
                console.print(f"    {w}")
```

### Alternative: Leave logging as-is for non-interactive

- **Interactive mode** (`is_interactive=True`): Capture logs, show summary after stage
- **Non-interactive** (`--quiet` or piped): Let structlog write normally to stderr
- **JSON mode** (`--log-format json`): No Rich output at all, pure structlog JSON to stderr

This means the log capture is CLI-only, not an orchestrator concern.

---

## StageResult and PipelineResult

```python
@dataclass(frozen=True)
class StageResult:
    """Result of a single stage execution, yielded to the caller."""
    stage: Stage
    phase_results: list[PhaseRunResult]
    gate: Gate | None                     # None = no gate or gate passed
    entropy_scores: dict[str, float]      # Current entropy state after stage
    duration_seconds: float
    captured_warnings: list[str]          # structlog warnings from this stage

    @property
    def all_completed(self) -> bool:
        return all(pr.status == "completed" for pr in self.phase_results)

    @property
    def has_failures(self) -> bool:
        return any(pr.status == "failed" for pr in self.phase_results)

@dataclass
class PipelineResult:
    """Final result after all stages complete."""
    success: bool
    stages: list[StageResult]
    final_entropy_scores: dict[str, float]
    total_duration_seconds: float
    source_id: str
    output_dir: Path
    # ... existing RunResult fields
```

---

## MCP Integration

The MCP server uses the same generator, but doesn't need interactive gate resolution:

```python
async def _analyze(output_dir, path, name, event_callback, gate_mode):
    config = RunConfig(source_path=path, gate_mode=gate_mode, ...)

    gen = create_and_run_pipeline(config)
    for stage_result in gen:
        if event_callback:
            event_callback(PipelineEvent(
                event_type=EventType.STAGE_COMPLETED,
                phase=stage_result.stage.name,
                step=stage_result.stage.order,
                total=len(stages),
            ))

        if stage_result.gate:
            if gate_mode == "skip":
                gen.send(GateResolution(action_taken=GateActionType.SKIP))
            elif gate_mode == "auto_fix":
                # Auto-fix logic
                gen.send(resolution)
            elif gate_mode == "fail":
                return format_gate_failure(stage_result)

    return format_pipeline_result(gen.value)
```

The MCP handler doesn't use `console.status()` or any Rich rendering — it just iterates the generator and sends resolutions.

---

## Migration Path

### Phase A: Stage model + YAML config

**Files changed:**
- `src/dataraum/pipeline/stages.py` — NEW: `Stage` dataclass, `load_stages()` from YAML
- `config/pipeline.yaml` — ADD: `stages:` section alongside existing `pipeline:` section

**Acceptance:** `load_stages()` returns 5 Stage objects with correct phase lists. Existing pipeline still works unchanged.

### Phase B: Generator orchestrator

**Files changed:**
- `src/dataraum/pipeline/orchestrator.py` — ADD: `run_stages()` generator method, `_run_stage()` method. Keep existing `run()` method working (dual path).

**Acceptance:** `pipeline.run_stages(stages, ...)` yields StageResults with correct phase results. Existing `pipeline.run()` still works for backward compatibility.

### Phase C: New CLI runner

**Files changed:**
- `src/dataraum/cli/commands/run.py` — REWRITE: Replace Live-based execution with stage-by-stage loop using `console.status()` for spinners and `console.print()` for results.
- `src/dataraum/cli/gate_handler.py` — SIMPLIFY: Remove `set_live()`, `_live` field. The handler no longer needs to manage Live lifecycle — it always has full terminal control when called.

**Acceptance:** `dataraum run ./testdata --gate-mode pause` shows stage-by-stage output with interactive gate prompts. No Rich Live used.

### Phase D: Log capture

**Files changed:**
- `src/dataraum/cli/log_capture.py` — NEW: `LogCapture` context manager
- `src/dataraum/cli/commands/run.py` — USE: Wrap stage execution with LogCapture in interactive mode

**Acceptance:** No structlog output visible during interactive pipeline runs. Warnings shown as summary after each stage.

### Phase E: Remove old code path

**Files changed:**
- `src/dataraum/pipeline/orchestrator.py` — REMOVE: old `run()` method, gate-checking inside scheduling loop
- `src/dataraum/pipeline/runner.py` — UPDATE: Use `run_stages()` instead of `run()`

**Acceptance:** `grep -r "def run(" src/dataraum/pipeline/orchestrator.py` shows only `run_stages()`. All tests pass.

### Phase F: MCP adapter

**Files changed:**
- `src/dataraum/mcp/server.py` — UPDATE: `_analyze()` uses stage generator

**Acceptance:** MCP `analyze` tool works with stage-based pipeline. Task progress shows stage-level updates.

---

## Files

### DO change

| File | Change |
|------|--------|
| `src/dataraum/pipeline/orchestrator.py` | Add `run_stages()` generator, `_run_stage()`. Eventually remove old `run()`. |
| `src/dataraum/pipeline/runner.py` | Update `run()` to use stages. Update `RunConfig` if needed. |
| `src/dataraum/cli/commands/run.py` | Rewrite: stage-by-stage loop, no Live, console.status() spinner. |
| `src/dataraum/cli/gate_handler.py` | Simplify: remove `set_live()`, `_live`. Handler always has full terminal. |
| `src/dataraum/mcp/server.py` | Update `_analyze()` to iterate stage generator. |
| `config/pipeline.yaml` | Add `stages:` configuration section. |
| `tests/unit/pipeline/test_orchestrator.py` | Add tests for `run_stages()` generator. |
| `tests/unit/pipeline/test_gate_checking.py` | Update for stage-level gate checking. |
| `tests/unit/cli/test_gate_handler.py` | Remove Live-related tests. |
| `tests/unit/cli/test_interactive_run.py` | Update for stage-based CLI. |
| `tests/unit/mcp/test_progress.py` | Update for stage-level events. |

### DO create

| File | Purpose |
|------|---------|
| `src/dataraum/pipeline/stages.py` | `Stage` dataclass, `load_stages()`, stage YAML schema |
| `src/dataraum/cli/log_capture.py` | `LogCapture` context manager for buffering structlog |
| `tests/unit/pipeline/test_stages.py` | Tests for stage loading and validation |

### DO NOT change

| File | Reason |
|------|--------|
| `src/dataraum/pipeline/phases/*.py` | All phase implementations stay exactly as-is |
| `src/dataraum/pipeline/base.py` | PhaseContext, PhaseResult unchanged |
| `src/dataraum/pipeline/events.py` | EventType, PipelineEvent unchanged (may add STAGE_COMPLETED) |
| `src/dataraum/pipeline/gates.py` | Gate, GateHandler protocol unchanged |
| `src/dataraum/pipeline/entropy_state.py` | PipelineEntropyState unchanged |
| `src/dataraum/pipeline/registry.py` | Phase registry unchanged |
| `src/dataraum/entropy/*.py` | All entropy modules unchanged |
| `src/dataraum/analysis/*.py` | All analysis modules unchanged |
| `src/dataraum/storage/*.py` | All storage modules unchanged |

---

## Verification

### Unit tests

```bash
# After each phase:
uv run pytest --testmon tests/unit -q

# Stage-specific:
uv run pytest tests/unit/pipeline/test_stages.py -v
uv run pytest tests/unit/pipeline/test_orchestrator.py -v -k "stage"
uv run pytest tests/unit/cli/test_interactive_run.py -v
```

### Manual interactive test

```bash
# Happy path — all gates pass:
dataraum run .e2e/medium/testdata/ -o /tmp/stage-test --gate-mode pause

# Expected: 5 stages, no gate fires, clean output

# Gate trigger — force bad data:
dataraum run .e2e/bad_types/ -o /tmp/stage-test --gate-mode pause

# Expected: Gate fires after Stage 1, Rich panel with options, user can fix/skip

# Non-interactive:
dataraum run .e2e/medium/testdata/ -o /tmp/stage-test | cat

# Expected: No Rich formatting, no prompts, clean text output
```

### What "done" looks like

1. `dataraum run` shows stage-by-stage progress with spinners
2. Gates fire between stages, not during phase execution
3. Gate prompts work without terminal corruption
4. No structlog output visible in interactive mode (unless `-v`)
5. MCP `analyze` tool reports stage-level progress
6. All existing tests pass
7. All phase implementations unchanged

---

## Open Questions

1. **Should `entropy_preconditions` on phases be removed or kept as documentation?** Keeping them preserves the phase's self-description of what it needs. Removing them eliminates the dual-system confusion. Recommendation: Keep as `@property` on phases but don't evaluate them in the orchestrator — the stage config is authoritative.

2. **Should stages be configurable per run?** e.g., `--stage semantic` to run only up to the semantic stage. This maps to the existing `--phase` flag but at stage granularity. Recommendation: Yes, add `--stage` flag. Keep `--phase` for backward compatibility.

3. **What happens when a fix changes entropy scores mid-stage?** If a fix is applied at a gate and the user wants to re-verify, should the stage re-run? Recommendation: The gate handler can call `_run_post_verification()` after a fix and yield an updated StageResult. No need to re-run the full stage.

4. **Generator vs. callback for MCP?** The generator model is natural for CLI but awkward for async MCP. Alternative: Convert generator to async iterator for MCP, or use a simple loop wrapper that auto-resolves gates. Recommendation: Simple wrapper function that iterates the generator and applies gate_mode policy.
