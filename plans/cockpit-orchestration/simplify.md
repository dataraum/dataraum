# Cockpit orchestration is over-engineered — collapse to one workflow (BUG)

> One Jira task. Bug, not a feature. Amends DAT-529 / DAT-530 / ADR-0014.

## Problem

DAT-529/530 shipped a long-lived **singleton** per-workspace `JourneyWorkflow`: signal-fed, holds circuit-breaker state, continues-as-new every ~500 events, and gates every change behind `patched()` (ADR-0014). It bundles serialization + breaker + cascade + the grounding loop into one resident execution.

The only thing that *needs* a resident actor is **per-workspace cross-stage serialization** (run one stage at a time per workspace). But the versioned-metadata model already makes concurrent stages safe — `promote_to_latest` flips the catalog head **atomically**, and `begin_session` is **born-loud** if nothing is promoted. A concurrent stage reads the fully-old or fully-new catalog, never a torn one. **So serialization isn't needed**, and the singleton's whole determinism tax (patched ordering, continue-as-new, replay fixtures, cross-run in-memory state) buys nothing. It also made the DAT-569 post-import narration "risky" for no reason.

## Fix

1. **One cockpit workflow = the grounding loop.** `add_source` (awaited — the loop must have the result to assess) → assess + auto-apply mechanical teaches (LLM activity) → if teaches applied and `attempt < 3`, **call itself** via continue-as-new with `attempt+1` (re-runs `add_source` to pick up the teaches) → else (clean / exhausted / judgement gap) park `awaiting_input` + **narrate** → done. State on the payload (`attempt`, `table_ids`, `conversationId`, `verticals`); readiness re-read from the DB each attempt so the payload stays tiny. It's a workflow only for crash-durability mid-loop. Short-lived per attempt → the determinism surface is trivial; no `patched()`/continue-as-new-at-500 machinery.

2. **begin_session triggers operating_model on clean completion — and stops.** A fire-and-forget Temporal **start** (no child, no await; Temporal runs OM independently), gated by a `cascade_to_operating_model` flag the cockpit passes on the begin_session trigger. The originating `conversationId` rides through to OM so the completion-watcher narrates it like any run. This is an **engine** change (Python `begin_session`).

3. **Cockpit is a Temporal client for everything else** — trigger by name, watch Temporal (it owns the execution db), render progress + narrate via the existing completion-watcher.

### Delete
`JourneyWorkflow`, the signal-queue serialization, `breaker.ts` + `pauseAutoMode`/`resumeAutoMode`, continue-as-new-at-500, the journey `patched()` machinery (`CASCADE_PATCH`/`GROUNDING_PATCH`), and the cross-run state carry.

### Dropped (never wanted)
- **Pause-autonomy switch** — no persistent flag, no DB table. To change teachings the user teaches in the Connect chat + replays (DAT-597).
- **Auto-trip-after-N-failures breaker** — the 3-attempt cap (on the payload) is the only runaway safety needed.

## Where DAT-569 lands
The narration is the **last line** of the grounding-loop workflow — it already holds the verdict + `conversationId`, and it's a fresh short-lived execution, so it just calls a `narrate` activity. No `patched()`, no replay fixture, no risk. The thing that was "risky" on the singleton is trivial here.

## Why this is safe (the one premise)
Atomic `promote_to_latest` head-flip + `begin_session` born-loud guard ⇒ concurrent per-workspace stages can't read a torn catalog ⇒ cross-stage serialization is unnecessary ⇒ the resident singleton has no remaining job.

## Cross-package
Cockpit (delete the journey + rewire to client) + engine (begin_session cascade trigger) + the cockpit↔worker contract mirror. No analysis change.

## Refs
DAT-529, DAT-530, ADR-0014; the grounding loop = `worker/workflows/journey.ts` (`runGroundingLoop`); the safety premise = the versioned-metadata head (`promote_to_latest`).
