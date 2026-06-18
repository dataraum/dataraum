// Determinism gate (DAT-529, hardened in DAT-568): replay recorded
// JourneyWorkflow histories OFFLINE through the Replayer — no Temporal server
// (the sanctioned offline check; the test-server stalls CI, so it's deliberately
// avoided). If the workflow code ever drifts non-deterministically against a
// committed history, replay throws and this fails. Histories are captured from
// real runs via `temporal workflow show --output json` (see RE-CAPTURE below).
//
// COVERAGE — two committed fixtures, by design:
//
//   journey-history.json — the begin_session START path only: signal → recordRun
//     → startChild → attachRunId → parked awaiting child.result. No markers, no
//     completed child. A thin smoke that the start path stays deterministic.
//
//   journey-cascade-grounding-history.json (DAT-568) — a RICH, real journey that
//     exercises the orchestration the reducers' unit tests can't: 4 completed
//     children (onboarding add_source → grounding REPLAY add_source → begin_session
//     → operating_model CASCADE) and BOTH `patched()` markers (GROUNDING_PATCH +
//     CASCADE_PATCH). Decoding the recorded activity results confirms the paths it
//     covers: the grounding-teach loop applies mechanical teaches and REPLAYS
//     (assess #1 → appliedCount=3 → re-run add_source), then re-measures and exits
//     CLEAN (assess #2 → appliedCount=0, needsJudgement=false → "done", DAT-551);
//     the clean begin_session → operating_model auto-cascade (DAT-530); and the
//     breaker fold around the child results. So a refactor that breaks replay of the
//     cascade or the grounding apply→replay→done loop now fails in unit CI, not just
//     at smoke.
//
// GAP (acceptable, deferred): the grounding loop's `awaiting_input` PARK path
// (assess → judgement gap with nothing mechanical left or attempts exhausted →
// `markRunAwaitingInput` → surface + return) is NOT in this fixture — the captured
// run cleared on its second pass instead of parking. That branch is covered only by
// the `decideGroundingStep` unit tests, not end-to-end replay. Capture a parking run
// (a judgement gap the agent can't auto-resolve) opportunistically and add it here.
//
// RE-CAPTURE (when journey.ts legitimately changes such that a committed history no
// longer replays — i.e. the determinism check SHOULD be refreshed, not the code
// fixed): run a real journey on the compose smoke stack, then dump its history with
//   docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
//     --entrypoint temporal temporal-admin-tools \
//     workflow show --namespace default --address temporal:7233 \
//     --workflow-id journey-<workspace-id> --output json > <fixture>.json
// and replace the fixture. The workflow id passed to runReplayHistory below must
// match the captured execution's id (the smoke uses the bootstrap workspace
// `…0001`). A history may be OPEN (continue-as-new not yet reached); the Replayer
// replays up to the last recorded event, which is enough to catch drift.

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "@temporalio/worker";
import { describe, expect, it } from "vitest";

const workflowsPath = fileURLToPath(
	new URL("./workflows/index.ts", import.meta.url),
);

// The bootstrap workspace id both fixtures were captured under — the Replayer
// matches the recorded execution's workflow id.
const WORKFLOW_ID = "journey-00000000-0000-0000-0000-000000000001";

const fixturePath = (name: string): string =>
	fileURLToPath(new URL(`./__fixtures__/${name}`, import.meta.url));

async function replay(fixture: string): Promise<void> {
	const history = JSON.parse(await readFile(fixturePath(fixture), "utf8"));
	// Resolves on a clean replay; throws DeterminismViolationError on drift.
	await Worker.runReplayHistory({ workflowsPath }, history, WORKFLOW_ID);
}

describe("JourneyWorkflow determinism (DAT-529 / DAT-568)", () => {
	// first run bundles the workflow (webpack) — allow headroom
	it("replays the begin_session START history with no non-determinism error", async () => {
		await replay("journey-history.json");
		expect(true).toBe(true);
	}, 60_000);

	it("replays the cascade + grounding-loop history with no non-determinism error", async () => {
		await replay("journey-cascade-grounding-history.json");
		expect(true).toBe(true);
	}, 60_000);
});
