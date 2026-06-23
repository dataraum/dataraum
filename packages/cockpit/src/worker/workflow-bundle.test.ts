// Sandbox-bundle guard (DAT-609) — replaces the bundle step the deleted
// journey-replay.test.ts used to give us. The orchestration workflows run in the
// worker's deterministic vm sandbox; a disallowed import (node IO, the cockpit_db
// client, config — anything non-pure) breaks the bundle. This bundles
// workflows/index.ts OFFLINE (no Temporal server, no captured history) and asserts it
// produces code containing both registered workflow types — so a sandbox-safety
// regression fails in unit CI, not at deploy. (Per-workflow determinism Replayer
// fixtures still come from the DAT-579 compose-smoke; the project bans the test-server.)

import { fileURLToPath } from "node:url";
import { bundleWorkflowCode } from "@temporalio/worker";
import { describe, expect, it } from "vitest";

const workflowsPath = fileURLToPath(
	new URL("./workflows/index.ts", import.meta.url),
);

describe("orchestration workflow bundle (DAT-609)", () => {
	// First run webpacks the workflow code — allow headroom (mirrors the old
	// journey-replay.test budget).
	it("bundles the sandbox workflows with no disallowed imports", async () => {
		const { code } = await bundleWorkflowCode({ workflowsPath });
		expect(code.length).toBeGreaterThan(0);
		// Both registered workflow types must be present in the bundle.
		expect(code).toContain("groundingLoopWorkflow");
		expect(code).toContain("sessionCascadeWorkflow");
	}, 60_000);
});
