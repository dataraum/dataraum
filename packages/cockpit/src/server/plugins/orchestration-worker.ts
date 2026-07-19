// Boot seam (DAT-529): start the co-located ACTIVITY-ONLY worker (DAT-708) ONCE
// when the cockpit server process boots. A Nitro plugin runs a single time at
// startup (before serving), which is exactly the "module-level singleton started
// at server boot" the design calls for — and it makes the worker tab-independent
// (it polls for the life of the process, no browser subscription needed).
//
// Registered via `nitro({ plugins: [...] })` in vite.config.ts. Fire-and-forget:
// we never block boot on the Temporal connection, and a start failure is logged,
// not fatal (the UI still serves; the singleton clears so a retry can happen).
//
// PORTAL MODE (DAT-819): the portal container is not a workspace — it has no
// boot workspace identity and MUST NOT poll a `cockpit-<ws>` queue (a second
// poller would steal that workspace's callbacks). The worker wiring below sits
// behind a dynamic import so portal-mode boot never evaluates the workspace
// config (#/config throws there, born-loud).

import { definePlugin } from "nitro";
import { baseConfig } from "#/config.base";

export default definePlugin(async () => {
	if (baseConfig.portalMode) {
		return;
	}
	const [
		{ config },
		{ getOtel },
		{ cockpitTaskQueueFor },
		{ startOrchestrationWorker },
	] = await Promise.all([
		import("#/config"),
		import("#/otel"),
		import("#/temporal/task-queue"),
		import("#/worker/worker"),
	]);
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
		// The queue is the boot identity (DAT-818): one cockpit per workspace
		// polls `cockpit-<ws>`, so the engine-hosted orchestration workflows —
		// which derive the same name from their input workspace_id — reach THIS
		// workspace's cockpit and no other.
		taskQueue: cockpitTaskQueueFor(config.dataraumWorkspaceId),
		// The worker stays pure of `config` — the telemetry gate is resolved
		// here (the otel plugin ran first, so this is a cache hit, never a
		// second bootstrap).
		traced: getOtel() !== null,
	}).catch((err) => {
		console.error("[orchestration-worker] failed to start:", err);
	});
});
