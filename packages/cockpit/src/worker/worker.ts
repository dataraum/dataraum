// The co-located ACTIVITY-ONLY worker singleton (DAT-529; slimmed in DAT-708).
//
// Lifecycle: created once when the cockpit server process boots (the Nitro
// plugin in src/server/plugins) and polls for the life of the process — so a
// run advances with NO browser tab open (the pain-#2 / tab-independence fix).
// It connects OUT to Temporal (no HTTP surface) and polls the cockpit's own
// activity queue, distinct from the engine's per-workspace queues.
//
// ACTIVITY-ONLY (ADR-0020): the orchestration WORKFLOWS run in Python on the
// engine worker — Temporal strongly discourages workflow workers outside
// authentic Node.js, and this process runs under Bun (DAT-705 proved the
// workflow vm sandbox silently drops interceptor headers there). Activities
// run on the main isolate — the part proven working under Bun since DAT-529 —
// so this worker registers ONLY the cockpit-bound activities (the cockpit_db
// run writers + the DAT-551 grounding-teach agent), which the engine-hosted
// workflows schedule by name on this queue. No workflow bundle, no vm sandbox,
// no build-time bundling step.
//
// Pure of `config`: it takes explicit Temporal params so the runtime can be
// smoke-tested with no full cockpit env; the boot plugin reads `config` and
// passes them in.

import {
	OpenTelemetryActivityInboundInterceptor,
	OpenTelemetryActivityOutboundInterceptor,
} from "@temporalio/interceptors-opentelemetry-v2";
import { NativeConnection, Worker } from "@temporalio/worker";
import * as activities from "./activities";

export interface OrchestrationWorkerOptions {
	/** Temporal frontend address, host:port (config.temporalHost). */
	address: string;
	/** Temporal namespace (config.temporalNamespace). */
	namespace: string;
	/** The cockpit activity task queue (config.cockpitOrchestrationTaskQueue). */
	taskQueue: string;
	/** Attach the OTel activity interceptors (ADR-0019/DAT-705). The boot
	 * plugin passes `getOtel() !== null` — an explicit flag rather than a
	 * config read keeps this module pure of `config` (smoke-testable with no
	 * full cockpit env). Off = the worker is byte-identical to before. */
	traced: boolean;
}

export interface RunningOrchestrationWorker {
	readonly worker: Worker;
	/** Stop polling and close the connection; clears the singleton so a later
	 * start re-creates it. */
	shutdown(): Promise<void>;
}

// HMR / double-import guard. In dev (`vite dev`) modules are re-evaluated on hot
// reload; without a process-wide pin each reload would spawn ANOTHER poller
// against the same queue. Cache the in-flight start PROMISE on globalThis so a
// re-import reuses the running worker (mirrors the temporal CLIENT singleton in
// temporal/progress.ts, lifted to globalThis for HMR).
const SINGLETON = Symbol.for("dataraum.orchestrationWorker");
type Holder = { [SINGLETON]?: Promise<RunningOrchestrationWorker> };

/**
 * Start the activity-only worker, or return the already-running one. Idempotent
 * across HMR re-evaluation and concurrent callers (the start promise is shared);
 * a failed start clears the cache so a later boot can retry.
 */
export function startOrchestrationWorker(
	opts: OrchestrationWorkerOptions,
): Promise<RunningOrchestrationWorker> {
	const holder = globalThis as Holder;
	const existing = holder[SINGLETON];
	if (existing) return existing;

	const starting = (async (): Promise<RunningOrchestrationWorker> => {
		const connection = await NativeConnection.connect({
			address: opts.address,
		});
		const worker = await Worker.create({
			connection,
			namespace: opts.namespace,
			taskQueue: opts.taskQueue,
			activities,
			// Tracing (ADR-0019/DAT-705): the engine-hosted orchestration
			// workflows schedule these activities with the OTel context on the
			// `_tracer-data` header; the inbound interceptor extracts it and wraps
			// each execution in a span parented to the workflow's — the cockpit's
			// run writers + teach agent join the run's ONE trace. The outbound
			// half stamps trace ids onto activity log/metric attributes. Plain
			// main-isolate worker options — no bundle, no vm sandbox (the reason
			// this worker is activity-only; see ADR-0020).
			...(opts.traced
				? {
						interceptors: {
							activity: [
								(ctx) => ({
									inbound: new OpenTelemetryActivityInboundInterceptor(ctx),
									outbound: new OpenTelemetryActivityOutboundInterceptor(ctx),
								}),
							],
						},
					}
				: {}),
		});

		// run() resolves only when the worker stops; let it poll in the background.
		// A crash clears the singleton so a future boot can re-create it.
		const runPromise = worker.run();
		runPromise.catch((err) => {
			console.error("[orchestration-worker] poll loop crashed:", err);
			holder[SINGLETON] = undefined;
		});

		console.log(
			`[orchestration-worker] RUNNING (activity-only) — queue=${opts.taskQueue} ns=${opts.namespace}`,
		);

		return {
			worker,
			async shutdown() {
				worker.shutdown();
				await runPromise.catch(() => {});
				await connection.close();
				holder[SINGLETON] = undefined;
			},
		};
	})().catch((err) => {
		holder[SINGLETON] = undefined;
		throw err;
	});

	holder[SINGLETON] = starting;
	return starting;
}
