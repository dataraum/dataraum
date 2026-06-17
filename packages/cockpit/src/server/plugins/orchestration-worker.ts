// Boot seam (DAT-529): start the co-located orchestration worker ONCE when the
// cockpit server process boots. A Nitro plugin runs a single time at startup
// (before serving), which is exactly the "module-level singleton started at
// server boot" the design calls for — and it makes the worker tab-independent
// (it polls for the life of the process, no browser subscription needed).
//
// Registered via `nitro({ plugins: [...] })` in vite.config.ts. Fire-and-forget:
// we never block boot on the Temporal connection, and a start failure is logged,
// not fatal (the UI still serves; the singleton clears so a retry can happen).

import { definePlugin } from "nitro";
import { config } from "#/config";
import { startOrchestrationWorker } from "#/worker/worker";

export default definePlugin(() => {
	if (!config.temporalHost || !config.temporalNamespace) {
		console.warn(
			"[orchestration-worker] TEMPORAL_HOST/TEMPORAL_NAMESPACE unset — " +
				"orchestration worker not started (no Temporal in this env)",
		);
		return;
	}
	startOrchestrationWorker({
		address: config.temporalHost,
		namespace: config.temporalNamespace,
		taskQueue: config.cockpitOrchestrationTaskQueue,
	}).catch((err) => {
		console.error("[orchestration-worker] failed to start:", err);
	});
});
