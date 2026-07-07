// OpenTelemetry bootstrap (ADR-0019 / DAT-705). SERVER-ONLY.
//
// Tracing only — metrics and log shipping land with DAT-706/707. The single
// on/off switch is OTEL_EXPORTER_OTLP_ENDPOINT (surfaced via config.ts):
// unset/empty = telemetry off and nothing here is constructed, so the
// no-telemetry path stays byte-identical to before.
//
// Manual init, no auto-instrumentation: the runtime is Bun (dev AND prod), and
// the Node `--require .../register` auto-instrumentation path doesn't exist
// there — the Bun-blessed pattern is an explicit init module started at boot
// (the Nitro plugin in server/plugins/otel.ts) with manual OTLP/HTTP export.
// All our instrumentation is explicit anyway (Temporal client + activity
// interceptors; TanStack AI middleware in DAT-706), so no module patching and
// no import-order fragility.
//
// SDK line: the @opentelemetry/* SDK deps deliberately track the 1.x line
// (sdk-trace-node ^1.30, exporter 0.57.x) — @temporalio/interceptors-opentelemetry
// is built against sdk-trace-base/resources ^1.25, so mixing in the 2.x SDK
// splits the types and the runtime across that seam. Bump both together when
// Temporal's interceptors move to 2.x. (@opentelemetry/api is line-independent
// and shared.)

import "@tanstack/react-start/server-only";

import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { Resource } from "@opentelemetry/resources";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

import { config } from "#/config";

// HMR / double-import guard, mirroring the orchestration-worker singleton: the
// OTel api allows only ONE global provider registration per process, so a dev
// re-evaluation must reuse the first bootstrap, not re-register.
const SINGLETON = Symbol.for("dataraum.otel");
type Holder = { [SINGLETON]?: NodeTracerProvider | null };

/** OTLP is base-endpoint + per-signal path (spec); the exporter wants the full
 * traces URL when passed explicitly. */
export function tracesUrl(endpoint: string): string {
	return `${endpoint.replace(/\/$/, "")}/v1/traces`;
}

/**
 * Bootstrap (once) and return the tracer provider, or `null` when telemetry is
 * off. Registers the global provider with an AsyncLocalStorage context manager
 * and the default W3C TraceContext + Baggage propagators — the same
 * propagation the engine's Python `TracingInterceptor` uses, which is what
 * connects one trace across the TS→Python seam. Consumers (the Temporal client
 * interceptor, the worker's activity interceptors) read the GLOBAL tracer;
 * they only ever need this function for the on/off gate. The BatchSpanProcessor
 * buffers — a SIGTERM flush hook (registered once, below) drains it on
 * container stop so the last spans of a run aren't lost.
 */
export function getOtel(): NodeTracerProvider | null {
	const holder = globalThis as Holder;
	const existing = holder[SINGLETON];
	if (existing !== undefined) return existing;

	const endpoint = config.otelExporterOtlpEndpoint;
	if (!endpoint) {
		holder[SINGLETON] = null;
		return null;
	}

	const provider = new NodeTracerProvider({
		resource: new Resource({ "service.name": "dataraum-cockpit" }),
	});
	provider.addSpanProcessor(
		new BatchSpanProcessor(new OTLPTraceExporter({ url: tracesUrl(endpoint) })),
	);
	provider.register();

	// Flush parity with the engine worker (its `finally` calls
	// provider.shutdown()): drain buffered spans on container stop. Registered
	// once — guarded by the same singleton that guards registration.
	process.once("SIGTERM", () => {
		provider.shutdown().catch(() => {});
	});

	console.info("otel_enabled", { endpoint });
	holder[SINGLETON] = provider;
	return provider;
}
