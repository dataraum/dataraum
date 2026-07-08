// Boot seam (DAT-529): start the co-located ACTIVITY-ONLY worker (DAT-708) ONCE
// when the cockpit server process boots. A Nitro plugin runs a single time at
// startup (before serving), which is exactly the "module-level singleton started
// at server boot" the design calls for — and it makes the worker tab-independent
// (it polls for the life of the process, no browser subscription needed).
//
// Registered via `nitro({ plugins: [...] })` in vite.config.ts. Fire-and-forget:
// we never block boot on the Temporal connection, and a start failure is logged,
// not fatal (the UI still serves; the singleton clears so a retry can happen).

import { definePlugin } from "nitro";
import { config } from "#/config";
import { getOtel } from "#/otel";
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
		// The worker stays pure of `config` — the telemetry gate is resolved
		// here (the otel plugin ran first, so this is a cache hit, never a
		// second bootstrap).
		traced: getOtel() !== null,
	}).catch((err) => {
		console.error("[orchestration-worker] failed to start:", err);
	});
});
