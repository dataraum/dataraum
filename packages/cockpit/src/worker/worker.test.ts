// Unit tests for the activity-only worker's telemetry gate (DAT-705). Mock the
// Temporal SDK + interceptors package at the vendor boundary and assert OUR
// decision logic: `traced: false` builds the exact pre-OTel options (no
// `interceptors` key at all); `traced: true` attaches one activity-interceptor
// factory producing the OTel inbound + outbound pair. The worker lifecycle
// itself (poll loop, container deploys) stays smoke-covered, as before.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	connect: vi.fn(async () => ({ close: vi.fn(async () => {}) })),
	create: vi.fn(async (opts: unknown) => {
		h.createOpts = opts;
		return { run: vi.fn(() => new Promise(() => {})), shutdown: vi.fn() };
	}),
	createOpts: undefined as unknown,
	inboundCtor: vi.fn(),
	outboundCtor: vi.fn(),
}));

vi.mock("@temporalio/worker", () => ({
	NativeConnection: { connect: h.connect },
	Worker: { create: h.create },
}));
vi.mock("@temporalio/interceptors-opentelemetry-v2", () => ({
	OpenTelemetryActivityInboundInterceptor: vi.fn(
		function OpenTelemetryActivityInboundInterceptor(ctx: unknown) {
			h.inboundCtor(ctx);
		},
	),
	OpenTelemetryActivityOutboundInterceptor: vi.fn(
		function OpenTelemetryActivityOutboundInterceptor(ctx: unknown) {
			h.outboundCtor(ctx);
		},
	),
}));
// The activities barrel drags in the cockpit_db client + the grounding agent
// (config, LLM); the gate under test never touches them.
vi.mock("#/worker/activities", () => ({}));

import { startOrchestrationWorker } from "./worker";

// The worker start promise is pinned on globalThis (HMR guard) — drop it so
// each case exercises a fresh Worker.create decision.
const SINGLETON = Symbol.for("dataraum.orchestrationWorker");

beforeEach(() => {
	delete (globalThis as Record<symbol, unknown>)[SINGLETON];
	h.connect.mockClear();
	h.create.mockClear();
	h.createOpts = undefined;
	h.inboundCtor.mockClear();
	h.outboundCtor.mockClear();
});

const OPTS = {
	address: "localhost:7233",
	namespace: "default",
	taskQueue: "cockpit-orchestration",
};

describe("startOrchestrationWorker telemetry gate (DAT-705)", () => {
	it("builds the exact pre-OTel worker options when tracing is off", async () => {
		await startOrchestrationWorker({ ...OPTS, traced: false });
		const opts = h.createOpts as Record<string, unknown>;
		expect(opts.taskQueue).toBe("cockpit-orchestration");
		expect("interceptors" in opts).toBe(false);
	});

	it("attaches ONE activity-interceptor factory producing the OTel inbound+outbound pair when traced", async () => {
		await startOrchestrationWorker({ ...OPTS, traced: true });
		const opts = h.createOpts as {
			interceptors: { activity: Array<(ctx: unknown) => unknown> };
		};
		expect(opts.interceptors.activity).toHaveLength(1);

		// The factory runs per-activity-context at execution time — invoke it
		// with a marker ctx and pin that both interceptors receive THAT ctx.
		const ctx = { marker: true };
		const pair = opts.interceptors.activity[0](ctx) as {
			inbound: unknown;
			outbound: unknown;
		};
		expect(pair.inbound).toBeDefined();
		expect(pair.outbound).toBeDefined();
		expect(h.inboundCtor).toHaveBeenCalledWith(ctx);
		expect(h.outboundCtor).toHaveBeenCalledWith(ctx);
	});

	it("is a process singleton: a second start reuses the first worker", async () => {
		const a = await startOrchestrationWorker({ ...OPTS, traced: false });
		const b = await startOrchestrationWorker({ ...OPTS, traced: false });
		expect(b).toBe(a);
		expect(h.create).toHaveBeenCalledTimes(1);
	});
});
