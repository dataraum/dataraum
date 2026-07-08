// Unit tests for the OTel bootstrap gate (ADR-0019 / DAT-705). Mock #/config +
// the @opentelemetry SDK modules at the seam: the assertions are about the GATE
// (off = nothing constructed; on = one provider, registered once) and the OTLP
// URL derivation — never about the SDK's own behavior.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	config: {} as Record<string, unknown>,
	providerRegister: vi.fn(),
	providerAddSpanProcessor: vi.fn(),
	providerCtor: vi.fn(),
	exporterCtor: vi.fn(),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));
// server-only marker: a no-op outside a real TanStack Start server context.
vi.mock("@tanstack/react-start/server-only", () => ({}));
vi.mock("@opentelemetry/sdk-trace-node", () => ({
	NodeTracerProvider: vi.fn(function NodeTracerProvider(
		this: Record<string, unknown>,
		opts: unknown,
	) {
		h.providerCtor(opts);
		this.register = h.providerRegister;
		this.addSpanProcessor = h.providerAddSpanProcessor;
		this.shutdown = vi.fn(async () => {});
	}),
}));
vi.mock("@opentelemetry/sdk-trace-base", () => ({
	BatchSpanProcessor: vi.fn(),
}));
vi.mock("@opentelemetry/exporter-trace-otlp-http", () => ({
	OTLPTraceExporter: vi.fn(function OTLPTraceExporter(opts: unknown) {
		h.exporterCtor(opts);
	}),
}));
vi.mock("@opentelemetry/resources", () => ({
	Resource: vi.fn(),
}));

// The module pins its singleton on globalThis (HMR guard) — drop it between
// cases so each test exercises a fresh bootstrap decision.
const SINGLETON = Symbol.for("dataraum.otel");

beforeEach(() => {
	vi.resetModules();
	delete (globalThis as Record<symbol, unknown>)[SINGLETON];
	h.config = {};
	h.providerRegister.mockClear();
	h.providerAddSpanProcessor.mockClear();
	h.providerCtor.mockClear();
	h.exporterCtor.mockClear();
});

afterEach(() => {
	delete (globalThis as Record<symbol, unknown>)[SINGLETON];
});

describe("getOtel (ADR-0019/DAT-705)", () => {
	it("returns null and constructs NOTHING when the endpoint is unset (telemetry off)", async () => {
		const { getOtel } = await import("./otel");
		expect(getOtel()).toBeNull();
		expect(h.providerCtor).not.toHaveBeenCalled();
		expect(h.exporterCtor).not.toHaveBeenCalled();
		// The off decision is cached too — a second call re-decides nothing.
		expect(getOtel()).toBeNull();
	});

	it("bootstraps ONCE and reuses the provider across calls (HMR/singleton guard)", async () => {
		h.config = { otelExporterOtlpEndpoint: "http://otel-lgtm:4318" };
		const { getOtel } = await import("./otel");
		const first = getOtel();
		const second = getOtel();
		expect(first).not.toBeNull();
		expect(second).toBe(first);
		expect(h.providerCtor).toHaveBeenCalledTimes(1);
		expect(h.providerRegister).toHaveBeenCalledTimes(1);
	});

	it("derives the OTLP traces URL from the base endpoint (trailing slash tolerated)", async () => {
		const { tracesUrl } = await import("./otel");
		expect(tracesUrl("http://otel-lgtm:4318")).toBe(
			"http://otel-lgtm:4318/v1/traces",
		);
		expect(tracesUrl("http://otel-lgtm:4318/")).toBe(
			"http://otel-lgtm:4318/v1/traces",
		);
	});

	it("hands the derived traces URL to the OTLP exporter", async () => {
		h.config = { otelExporterOtlpEndpoint: "http://localhost:4318" };
		const { getOtel } = await import("./otel");
		getOtel();
		expect(h.exporterCtor).toHaveBeenCalledWith({
			url: "http://localhost:4318/v1/traces",
		});
	});
});
