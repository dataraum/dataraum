// OpenTelemetry bootstrap (ADR-0019 / DAT-705, metrics DAT-706). SERVER-ONLY.
//
// Traces + metrics — log shipping lands with DAT-707. The single on/off
// switch is OTEL_EXPORTER_OTLP_ENDPOINT (surfaced via config.ts):
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
// SDK line: deliberately the OTel JS 2.x line, paired with Temporal's official
// `@temporalio/interceptors-opentelemetry-v2` (EXPERIMENTAL by its README —
// accepted deliberately: we build on the current SDK line, not the frozen 1.x
// one the non-v2 interceptors package pins). The v2 package's peers pin the
// EXACT matching `@temporalio/*` version, so the interceptors package and the
// Temporal SDK move in lockstep — bump them together.

import "@tanstack/react-start/server-only";

import { metrics } from "@opentelemetry/api";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
	MeterProvider,
	PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

import { config } from "#/config";

// HMR / double-import guard, mirroring the orchestration-worker singleton: the
// OTel api allows only ONE global provider registration per process, so a dev
// re-evaluation must reuse the first bootstrap, not re-register.
const SINGLETON = Symbol.for("dataraum.otel");
type Holder = { [SINGLETON]?: NodeTracerProvider | null };

/** OTLP is base-endpoint + per-signal path (spec); the exporters want the full
 * per-signal URL when passed explicitly. */
export function tracesUrl(endpoint: string): string {
	return `${endpoint.replace(/\/$/, "")}/v1/traces`;
}

export function metricsUrl(endpoint: string): string {
	return `${endpoint.replace(/\/$/, "")}/v1/metrics`;
}

/**
 * Bootstrap (once) and return the tracer provider, or `null` when telemetry is
 * off. Registers the global provider with an AsyncLocalStorage context manager
 * and the default W3C TraceContext + Baggage propagators — the same
 * propagation the engine's Python `TracingInterceptor` uses, which is what
 * connects one trace across the TS→Python seam. Also registers the global
 * MeterProvider (DAT-706). Consumers (the Temporal client interceptor, the
 * worker's activity interceptors, lib/llm-otel.ts) read the GLOBAL tracer and
 * meter; they only ever need this function for the on/off gate. The
 * BatchSpanProcessor buffers — a SIGTERM flush hook (registered once, below)
 * drains it on container stop so the last spans of a run aren't lost.
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

	const resource = resourceFromAttributes({
		"service.name": "dataraum-cockpit",
	});
	const provider = new NodeTracerProvider({
		resource,
		// 2.x: processors ride the constructor (addSpanProcessor was removed).
		spanProcessors: [
			new BatchSpanProcessor(
				new OTLPTraceExporter({ url: tracesUrl(endpoint) }),
			),
		],
	});
	provider.register();

	// Metrics (DAT-706): the GenAI client histograms the TanStack AI
	// otelMiddleware records (`gen_ai.client.token.usage` / `.operation.duration`,
	// see lib/llm-otel.ts). Registered on the GLOBAL meter provider so
	// consumers resolve meters via `metrics.getMeter(...)` — same pattern as
	// the global tracer. Default export interval (60s) is fine: histograms
	// are aggregated cumulatively, nothing is lost between exports.
	const meterProvider = new MeterProvider({
		resource,
		readers: [
			new PeriodicExportingMetricReader({
				exporter: new OTLPMetricExporter({ url: metricsUrl(endpoint) }),
			}),
		],
	});
	metrics.setGlobalMeterProvider(meterProvider);

	// Flush parity with the engine worker (its `finally` calls
	// provider.shutdown()): drain buffered spans + the last metric export on
	// container stop. Registered once — guarded by the same singleton that
	// guards registration.
	process.once("SIGTERM", () => {
		provider.shutdown().catch(() => {});
		meterProvider.shutdown().catch(() => {});
	});

	console.info("otel_enabled", { endpoint });
	holder[SINGLETON] = provider;
	return provider;
}
