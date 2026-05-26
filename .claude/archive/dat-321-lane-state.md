# DAT-321 Lane State (post-compact handoff)

**Branch:** `feat/dat-321-sqlalchemy-postgres`
**Worktree:** `/Users/philipp/Code/dataraum/dataraum-context/.worktrees/DAT-321/`
**Scope (Plan rev 4):** All SQLAlchemy → single workspace Postgres. Workspace tables + per-session tables in one Postgres DB. Per-session tables get `session_id` FK to `investigation_sessions.session_id`. Per-session DuckDB stays per-session (L4 owns DuckLake swap).

## Decisions locked
- **Hybrid plumbing**: `manager.session_id` set at MCP entry; phases read `ctx.manager.session_id`; deep agents get `session_id` as explicit kwarg. **No ContextVar** (thread-pool unsafe). **No magic SQLAlchemy event listener** (user rejected). **Explicit at every row-construction site.**
- **FK target**: `investigation_sessions.session_id` (the InvestigationSession PK).
- **`nullable=False`** — we do NOT relax. User explicitly rejected `nullable=True` partial-ship.
- **No new tables** (preserves Plan hard rule).
- **`testcontainers[postgres]>=4.14`** as dev dep. Session-scoped `pg_url` fixture, function-scoped `pg_url_clean` (TRUNCATE CASCADE).
- **CLI break is acceptable** ("we can remove dev command later, it is unused").
- **TRUNCATE CASCADE per test**, one Postgres container per pytest invocation.

## Phases committed (Phases 1, 2, 3)
- `a698bba1` phase 1: port 5 sqlite-dialect JSON imports → portable `sqlalchemy.JSON`
- `6db4a88b` phase 2: testcontainers dev dep + session-scoped pg fixtures
- `333c9d3c` phase 3: postgres-only SQLAlchemy + archive flow rewrite

Phase 3 also surfaced + fixed a real production bug (resume_session's cross-DB `delete(Source); add(Source)` workspace-sync), and rewrote `_read_archive_summary` from sqlite3 → SQLAlchemy.

## Phase 4 work in progress (UNCOMMITTED)

### Phase 4a — model files (DONE, 26 columns added across 19 files)
All per-session SQLAlchemy models have a new column:
```python
session_id: Mapped[str] = mapped_column(
    ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
)
```

Files touched (all in worktree, uncommitted):
- `pipeline/db_models.py` (PipelineRun, PhaseLog)
- `pipeline/fixes/models.py` (DataFix)
- `documentation/db_models.py` (FixLedgerEntry)
- `query/db_models.py` (QueryExecutionRecord)
- `query/snippet_models.py` (SQLSnippetRecord, SnippetUsageRecord)
- `entropy/db_models.py` (EntropyObjectRecord)
- `analysis/correlation/db_models.py` (DerivedColumn)
- `analysis/cycles/db_models.py` (DetectedBusinessCycle)
- `analysis/eligibility/db_models.py` (ColumnEligibilityRecord)
- `analysis/relationships/db_models.py` (Relationship)
- `analysis/semantic/db_models.py` (SemanticAnnotation, TableEntity)
- `analysis/slicing/db_models.py` (SliceDefinition, ColumnSliceProfile)
- `analysis/statistics/db_models.py` (StatisticalProfile)
- `analysis/statistics/quality_db_models.py` (StatisticalQualityMetrics)
- `analysis/temporal/db_models.py` (TemporalColumnProfile)
- `analysis/temporal_slicing/db_models.py` (ColumnDriftSummary, TemporalSliceAnalysis — also added `ForeignKey` to imports)
- `analysis/typing/db_models.py` (TypeCandidate, TypeDecision)
- `analysis/validation/db_models.py` (ValidationResultRecord — also added `ForeignKey` to imports)
- `analysis/views/db_models.py` (EnrichedView, SlicingView)

### Phase 4b — plumbing skeleton (DONE)
- `core/connections.py::ConnectionManager.session_id: str | None` field added.
- `pipeline/runner.py::RunConfig.session_id: str | None` field added.
- `pipeline/runner.py::run(config)` passes `session_id=config.session_id` to `setup_pipeline`.
- `pipeline/setup.py::setup_pipeline(*, ..., session_id: str | None = None)`:
  - Raises `RuntimeError` if `session_id is None` (fail-loud — CLI/dev path is dead per L5).
  - `ConnectionManager(conn_config, session_id=session_id)` — wires session_id onto the new manager.
  - `PipelineRun(run_id=..., session_id=session_id, ...)` — populates the FK.
- `pipeline/scheduler.py::PipelineScheduler._require_session_id()` helper added (reads `self.manager.session_id`, raises if None).
- `pipeline/scheduler.py::_write_phase_log` constructs `PhaseLog(session_id=self._require_session_id(), ...)`.
- `mcp/server.py::_get_session_manager(fingerprint, session_id: str | None = None)`:
  - Accepts session_id; sets on cached manager (refresh) or new manager.
  - `None` permitted for bootstrap paths (begin_session, _restore_archived_session) that create InvestigationSession AFTER opening; caller assigns `manager.session_id` once new id is in hand.
- `mcp/server.py::_run_pipeline(output_dir, session_id, ...)` and `_run_pipeline_background(output_dir, session_id, ...)` accept session_id.
- `mcp/server.py` call sites updated to pass `active_session_id` to `_get_session_manager`, `_run_pipeline`, `_run_pipeline_background`.
- `mcp/server.py` `open_session_manager: Callable[..., ConnectionManager]` (was `Callable[[str], ...]`) in the two callee signatures.

### Phase 4b — entropy engine (DONE)
- `entropy/engine.py::run_detector_post_step(*, session_id: str)` — kwarg-only.
- `entropy/engine.py::_make_record(*, session_id: str, ...)` — kwarg-only, populates `EntropyObjectRecord(session_id=session_id, ...)`.
- `pipeline/scheduler.py::_run_post_step_detectors` reads `sid = self._require_session_id()` and threads to both branches of the `if self.session_factory and self.manager` split.

### Phase 4b — test fixture baseline (DONE)
- `tests/conftest.py::session` fixture now seeds:
  - `Source(source_id=_TEST_SOURCE_ID, name="test_baseline", source_type="csv")` (flushed first)
  - `InvestigationSession(session_id=_TEST_SESSION_ID, source_id=_TEST_SOURCE_ID, intent="conftest baseline", status="active", started_at=now())`
- Helper `test_session_id() -> str` exposed for tests to use at construction sites.

### Phase 4b — STILL TODO

**Production construction sites** (each needs `session_id=...` populated; source of session_id noted):
- `pipeline/setup.py:170` — PipelineRun ✓ DONE (from setup_pipeline param)
- `pipeline/scheduler.py:371` — PhaseLog ✓ DONE (via `_require_session_id`)
- `entropy/engine.py:242` — EntropyObjectRecord ✓ DONE
- `pipeline/phases/column_eligibility_phase.py:154` — ColumnEligibilityRecord (use `ctx.manager.session_id`)
- `pipeline/phases/slicing_phase.py:208` — SliceDefinition (use `ctx.manager.session_id`)
- `pipeline/phases/slicing_view_phase.py:246` — SlicingView (use `ctx.manager.session_id`)
- `pipeline/phases/enriched_views_phase.py:309` — EnrichedView (use `ctx.manager.session_id`)
- `pipeline/phases/enriched_views_phase.py:416` — StatisticalProfile (use `ctx.manager.session_id`)
- `analysis/typing/resolution.py:281` — TypeDecision (thread session_id param from caller)
- `analysis/typing/resolution.py:423` — TypeDecision (same)
- `analysis/typing/resolution.py:437` — TypeCandidate (same)
- `analysis/typing/inference.py:120` — DBTypeCandidate (thread session_id)
- `analysis/correlation/within_table/derived_columns.py:255` — DBDerivedColumn (thread)
- `analysis/correlation/within_table/derived_columns.py:433` — DBDerivedColumn (thread)
- `analysis/cycles/agent.py:326` — DetectedBusinessCycle (thread)
- `analysis/validation/agent.py:699` — ValidationResultRecord (thread)
- `analysis/slicing/profiling.py:191` — ColumnSliceProfile (thread)
- `analysis/temporal/processor.py:286` — TemporalColumnProfile (thread)
- `analysis/temporal_slicing/analyzer.py:491` — ColumnDriftSummary (thread)
- `analysis/temporal_slicing/analyzer.py:805` — TemporalSliceAnalysis (thread)
- `analysis/statistics/quality.py:515` — DBStatisticalQualityMetrics (thread)
- `query/snippet_library.py:579` — SQLSnippetRecord (MCP-direct; pull from session_mgr.session_id)
- `query/snippet_library.py:638` — SnippetUsageRecord (same)
- `query/agent.py:1139` — QueryExecutionRecord (MCP-direct; same)
- `documentation/ledger.py:57` — FixLedgerEntry (MCP-direct; same)
- `pipeline/fixes/interpreters.py:315` — Relationship (DB persistence in teach flow; thread)

For "thread session_id" sites: the function signature gets a new `session_id: str` kwarg; caller (usually a Phase) passes `ctx.manager.session_id`.

**Test patches** (each failing test needs `session_id=test_session_id()` at construction):
Last testmon run showed 47 failures. Files touched:
- `tests/unit/pipeline/test_cleanup.py` (PhaseLog, StatisticalProfile constructions)
- `tests/unit/mcp/test_search_snippets.py` (snippet constructions)
- `tests/unit/analysis/correlation/test_enriched_derived.py` (DerivedColumn)
- `tests/unit/mcp/test_measure.py` (PhaseLog / EntropyObjectRecord constructions)
- `tests/unit/mcp/test_look.py` (TableEntity, SemanticAnnotation, Relationship, StatisticalQualityMetrics, TypeCandidate, EntropyObjectRecord, SQLSnippetRecord)
- `tests/unit/mcp/test_why.py` (FixLedgerEntry)
- `tests/unit/mcp/test_teach.py` (SemanticAnnotation, Relationship)
- `tests/unit/mcp/test_run_sql.py` (EntropyObjectRecord)
- `tests/unit/mcp/test_progress.py` (likely PhaseLog or RunConfig session_id)
- `tests/unit/analysis/validation/test_resolver.py` (1 ERROR — needs investigation)

After patching each test, re-run `uv run pytest --testmon tests/unit -q` to find next batch.

### Phase 5 — STILL TODO
Lane smoke `tests/platform/smoke_dat321.py`:
- Postgres workspace tables present (all ~30 tables)
- All previously-existing indexes present (named via `inspector.get_indexes`)
- session_id FK present on each per-session table (verify FK constraint exists)
- Round-trip insert/read with JSON `connection_config`, `discovered_schema` round-trip
- Two-sessions-no-leak: create 2 InvestigationSession rows, insert PhaseLog rows for each, query by session_id → no cross-contamination
- Grep assertion: `rg 'from sqlalchemy.dialects.sqlite' src/` returns zero
- `os.listdir` assertion: no `.db` file appears on disk at any session run

### Review gate (mandatory)
After phases land + lane smoke green, launch senior-code-reviewer + spec-compliance-reviewer. Both must approve before PR opens.

### Open PR
Title: `DAT-321: L2 / Minimum-port — All SQLAlchemy → single workspace Postgres`
Body must include:
- 5-bullet "Read first" summary (per ticket gate)
- Contract reference: Minimum-Port Plan rev 4 (Confluence 20971523)
- Other lanes in flight (DAT-324 is already merged via PR #111 likely — check `gh pr list --search "DAT-294 in:body"`)
- Lane smoke result
- Add row to `.claude/platform-status.md`

## Reviewer-trap quirks (already addressed)
- `manager.session_id = session_id` on cached manager reopens with refreshed id (resume_session reuses fingerprint with new session_id)
- `_get_session_manager(fp, None)` allowed for bootstrap; caller assigns id after creation
- `setup_pipeline` raises if session_id is None — fail-loud per L5
- `_require_session_id()` on scheduler protects PhaseLog writes
- testcontainers `pg_url_clean` truncates per test (filter by `inspect(engine).get_table_names()` to skip absent tables in early phases)

## Memory check before continuing
1. Confirm worktree path still valid: `git worktree list | grep DAT-321`
2. Confirm tasks #13 + #14 still open in TaskList
3. Confirm DATABASE_URL fixture works: `uv run pytest tests/unit/core/test_pg_fixture.py -q`
4. Re-run testmon to confirm the 47 failures are still the right count: `uv run pytest --testmon tests/unit -q`
5. Resume by patching `pipeline/phases/column_eligibility_phase.py:154` (simplest — phase has `ctx.manager.session_id` directly).
