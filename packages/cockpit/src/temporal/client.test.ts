// Unit tests for the process-shared Temporal client factory (DAT-705). The
// factory is the ONE place cross-cutting client concerns attach — the test
// pins the telemetry gate: interceptor attached exactly when telemetry is on,
// byte-identical client options when it is off, one shared connection.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	config: {} as Record<string, unknown>,
	otel: null as unknown,
	connect: vi.fn(async () => ({ close: vi.fn() })),
	clientCtor: vi.fn(),
	interceptorCtor: vi.fn(),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));
vi.mock("#/otel", () => ({ getOtel: () => h.otel }));
vi.mock("@temporalio/client", () => ({
	Connection: { connect: h.connect },
	Client: vi.fn(function Client(opts: unknown) {
		h.clientCtor(opts);
		return { workflow: {} };
	}),
	WorkflowExecutionAlreadyStartedError: class extends Error {},
}));
vi.mock("@temporalio/interceptors-opentelemetry", () => ({
	OpenTelemetryWorkflowClientInterceptor: vi.fn(
		function OpenTelemetryWorkflowClientInterceptor() {
			h.interceptorCtor();
		},
	),
}));

import { getTemporalClient, resetTemporalClient } from "./client";

beforeEach(() => {
	resetTemporalClient();
	h.config = { temporalHost: "localhost:7233", temporalNamespace: "default" };
	h.otel = null;
	h.connect.mockClear();
	h.clientCtor.mockClear();
	h.interceptorCtor.mockClear();
});

describe("getTemporalClient (DAT-705)", () => {
	it("constructs the client WITHOUT interceptors when telemetry is off", async () => {
		await getTemporalClient();
		expect(h.clientCtor).toHaveBeenCalledTimes(1);
		const opts = h.clientCtor.mock.calls[0][0] as Record<string, unknown>;
		expect(opts.namespace).toBe("default");
		expect("interceptors" in opts).toBe(false);
		expect(h.interceptorCtor).not.toHaveBeenCalled();
	});

	it("attaches the OTel workflow-client interceptor when telemetry is on", async () => {
		h.otel = {}; // any non-null provider handle
		await getTemporalClient();
		const opts = h.clientCtor.mock.calls[0][0] as Record<string, unknown>;
		const interceptors = opts.interceptors as { workflow: unknown[] };
		expect(interceptors.workflow).toHaveLength(1);
		expect(h.interceptorCtor).toHaveBeenCalledTimes(1);
	});

	it("shares ONE connection across callers (the cached connect promise)", async () => {
		const [a, b] = await Promise.all([
			getTemporalClient(),
			getTemporalClient(),
		]);
		expect(a).toBe(b);
		expect(h.connect).toHaveBeenCalledTimes(1);
	});

	it("fails loud when Temporal isn't configured, and retries after a failure", async () => {
		h.config = {};
		// The config guard throws SYNCHRONOUSLY, before anything is cached.
		expect(() => getTemporalClient()).toThrow(/not configured/);
		// A later configured call therefore starts fresh and succeeds.
		h.config = { temporalHost: "localhost:7233", temporalNamespace: "default" };
		await expect(getTemporalClient()).resolves.toBeDefined();
	});
});
