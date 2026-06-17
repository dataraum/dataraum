// Determinism gate (DAT-529): replay a recorded JourneyWorkflow history OFFLINE
// through the Replayer — no Temporal server (the sanctioned offline check; the
// test-server stalls CI, so it's deliberately avoided). If the workflow code
// ever drifts non-deterministically against this committed history, replay
// throws and this fails. Fixture captured from a real run via
// `temporal workflow show --output json` (signal → activity → parked).

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
