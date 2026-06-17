// Determinism gate (DAT-529): replay a recorded JourneyWorkflow history OFFLINE
// through the Replayer — no Temporal server (the sanctioned offline check; the
// test-server stalls CI, so it's deliberately avoided). If the workflow code
// ever drifts non-deterministically against this committed history, replay
// throws and this fails. Fixture captured from a real run via
// `temporal workflow show --output json` (signal → recordRun → startChild →
// attachRunId → parked awaiting child.result).
//
// COVERAGE NOTE (DAT-530 P3b.2): this fixture exercises only the begin_session
// START path — it parks before the child completes, so the auto-cascade
// (begin_session done → patched() → operating_model child) and the breaker fold
// are NOT replay-covered here. The cascade is `patched()`-gated, so this
// marker-less history correctly replays the OLD (no-cascade) path. Capturing a
// cascade fixture needs a begin_session child that actually completes = a real
// engine + LLM run (the gated compose-smoke); regenerate this fixture from such a
// run when one is available. Until then the cascade's determinism rests on the
// breaker unit tests + review.

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "@temporalio/worker";
import { describe, expect, it } from "vitest";

const fixture = fileURLToPath(
	new URL("./__fixtures__/journey-history.json", import.meta.url),
);
const workflowsPath = fileURLToPath(
	new URL("./workflows/index.ts", import.meta.url),
);

describe("JourneyWorkflow determinism (DAT-529)", () => {
	it("replays a recorded history with no non-determinism error", async () => {
		const history = JSON.parse(await readFile(fixture, "utf8"));
		// Resolves on a clean replay; throws DeterminismViolationError on drift.
		await Worker.runReplayHistory(
			{ workflowsPath },
			history,
			"journey-00000000-0000-0000-0000-000000000001",
		);
		expect(true).toBe(true);
	}, 60_000); // first run bundles the workflow (webpack) — allow headroom
});
