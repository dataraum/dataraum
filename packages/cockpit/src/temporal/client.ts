// The process-shared Temporal client — the ONE factory every cockpit-side
// Temporal consumer goes through (unified in DAT-705: the progress poll, the
// completion watcher, and the orchestration/direct-run triggers previously
// built clients in two independent places, which is exactly how a cross-cutting
// client concern — today the OTel tracing interceptor — gets missed on one of
// them).
//
// SERVER-ONLY (imports `config`).

import { Client, Connection } from "@temporalio/client";
import { OpenTelemetryWorkflowClientInterceptor } from "@temporalio/interceptors-opentelemetry-v2";

import { config } from "#/config";
import { getOtel } from "#/otel";

/** The Temporal-unconfigured guard: Temporal config is OPTIONAL in config.ts,
 * so fail loud (not silent) when it isn't wired. */
function requireTemporalConfig(): { host: string; namespace: string } {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}
	return { host: config.temporalHost, namespace: config.temporalNamespace };
}

/**
 * A lazily-created, process-SHARED Temporal client. The connection is long-lived
 * (a gRPC channel that reconnects internally), so opening + closing one per call
 * — which the progress poll AND the completion watcher do every couple of seconds
 * per run — was pure churn. Cache the connect PROMISE so concurrent first-callers
 * share one connect; reset it only if the connect itself fails, so the next call
 * retries rather than reusing a rejected promise.
 */
let temporalClientPromise: Promise<Client> | null = null;

export function getTemporalClient(): Promise<Client> {
	if (!temporalClientPromise) {
		const { host, namespace } = requireTemporalConfig();
		temporalClientPromise = Connection.connect({ address: host })
			.then(
				(connection) =>
					new Client({
						connection,
						namespace,
						// Tracing (ADR-0019/DAT-705): inject the active W3C context into
						// Temporal's `_tracer-data` header so the engine's Python
						// TracingInterceptor (workflows + engine activities) and the
						// cockpit's activity-only worker continue ONE trace across the
						// seam. Attached only when telemetry is on — off keeps the
						// client byte-identical to before.
						...(getOtel()
							? {
									interceptors: {
										workflow: [new OpenTelemetryWorkflowClientInterceptor()],
									},
								}
							: {}),
					}),
			)
			.catch((err) => {
				temporalClientPromise = null;
				throw err;
			});
	}
	return temporalClientPromise;
}

/** Drop the shared Temporal client so the next call reconnects. Primarily for
 * tests (the module-level cache otherwise leaks across cases); also a hook if a
 * forced reconnect is ever needed. */
export function resetTemporalClient(): void {
	temporalClientPromise = null;
}
