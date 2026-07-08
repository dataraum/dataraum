// Turn-span test for streamAgentTurnToBus (DAT-706): the per-turn ACTIVE span
// is what stitches the orchestrator chat() run and every nested chat() (fired
// inside tool executions during the drain) into ONE trace — so it gets direct
// coverage: span created + ACTIVE while chat() runs, ended exactly once on
// every terminal path (success / failure with exception payload / cancelled /
// synchronous chat() throw). chat() itself is mocked (the real-engine
// telemetry contract lives in llm-otel.test.ts); the heavy import seams are
// mocked exactly like routes/api/chat.test.ts so no env/DB is touched.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	publish: vi.fn(),
	chat: vi.fn(),
	// The tracer under test writes here; the api mock below wires it up.
	getFinishedSpans: (() => []) as () => ReadonlyArray<{
		name: string;
		attributes: Record<string, unknown>;
		status: { code: number; message?: string };
		events: ReadonlyArray<{ name: string }>;
	}>,
	resetSpans: () => {},
	// Captured inside the chat() mock: the span that was ACTIVE at call time.
	activeSpanName: null as string | null,
}));

// Local in-memory tracer instead of the API globals: other test files in the
// same worker register their own global providers, and the OTel API keeps the
// FIRST registration — routing getTracer through a file-local provider keeps
// this test deterministic. An ALS context manager is registered so
// startActiveSpan's context propagation (the property under test) is real.
vi.mock("@opentelemetry/api", async (importOriginal) => {
	const actual = await importOriginal<typeof import("@opentelemetry/api")>();
	const { AsyncLocalStorageContextManager } = await import(
		"@opentelemetry/context-async-hooks"
	);
	const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } =
		await import("@opentelemetry/sdk-trace-base");
	actual.context.setGlobalContextManager(new AsyncLocalStorageContextManager());
	const exporter = new InMemorySpanExporter();
	const provider = new BasicTracerProvider({
		spanProcessors: [new SimpleSpanProcessor(exporter)],
	});
	h.getFinishedSpans = () =>
		exporter.getFinishedSpans() as unknown as ReturnType<
			typeof h.getFinishedSpans
		>;
	h.resetSpans = () => exporter.reset();
	return {
		...actual,
		trace: {
			...actual.trace,
			getTracer: (name: string) => provider.getTracer(name),
		},
	};
});

vi.mock("@tanstack/ai", async (importOriginal) => {
	const actual = await importOriginal<typeof import("@tanstack/ai")>();
	return { ...actual, chat: h.chat };
});

// Telemetry gate OFF for llmOtel — this test is about the turn span only.
vi.mock("#/otel", () => ({ getOtel: () => null }));

// The same import seams routes/api/chat.test.ts mocks — agent-turn pulls the
// tool registry + conversations seam transitively; none may touch env or DB.
vi.mock("#/config", () => ({ config: { anthropicApiKey: "sk-ant-test" } }));
vi.mock("#/lib/chat-bus", () => ({
	publish: h.publish,
	subscribe: () => () => {},
	hasSubscribers: () => false,
}));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
vi.mock("#/db/cockpit/client", () => ({ cockpitDb: {} }));
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspace: async () => "ws-test",
	resolveActiveWorkspaceRow: async () => ({
		id: "ws-test",
		taskQueue: "engine-ws-test",
		vertical: "_adhoc",
	}),
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: async () => {},
	hasRunningRun: async () => false,
}));
vi.mock("#/db/cockpit/conversations", () => ({
	appendMessages: async () => {},
	loadModelTranscript: async () => [],
	setConversationTitle: async () => {},
	getConversation: vi.fn(),
}));

import { context, SpanStatusCode, trace } from "@opentelemetry/api";

import { streamAgentTurnToBus } from "#/lib/agent-turn";

const MSG = [{ role: "user" as const, content: "hi" }];

function scriptedStream(chunks: unknown[], failWith?: Error) {
	return async function* stream() {
		// Record which span is ACTIVE when the model stream runs — the nesting
		// guarantee nested chat() runs rely on.
		h.activeSpanName =
			(
				trace.getSpan(context.active()) as unknown as
					| {
							name?: string;
					  }
					| undefined
			)?.name ?? null;
		for (const chunk of chunks) {
			yield chunk;
		}
		if (failWith) throw failWith;
	};
}

beforeEach(() => {
	h.publish.mockReset();
	h.chat.mockReset();
	h.resetSpans();
	h.activeSpanName = null;
});

describe("streamAgentTurnToBus turn span (DAT-706)", () => {
	it("wraps the drain in one ACTIVE 'turn {kind}' span and publishes the chunks", async () => {
		h.chat.mockImplementation(() => scriptedStream(["c1", "c2"])());

		await streamAgentTurnToBus("conv-1", MSG, {
			kind: "connect",
			persist: false,
		});

		expect(h.publish).toHaveBeenCalledTimes(2);
		const spans = h.getFinishedSpans();
		expect(spans).toHaveLength(1);
		expect(spans[0]?.name).toBe("turn connect");
		expect(spans[0]?.attributes["gen_ai.conversation.id"]).toBe("conv-1");
		expect(spans[0]?.status.code).not.toBe(SpanStatusCode.ERROR);
		// The span was ACTIVE while the model stream ran — what parents the
		// orchestrator's chat span and any nested sub-agent runs into the trace.
		expect(h.activeSpanName).toBe("turn connect");
	});

	it("records the exception + message on a non-abort stream failure and still ends the span", async () => {
		const consoleError = vi
			.spyOn(console, "error")
			.mockImplementation(() => {});
		h.chat.mockImplementation(() =>
			scriptedStream(["c1"], new Error("stream died"))(),
		);

		await streamAgentTurnToBus("conv-1", MSG, {
			kind: "connect",
			persist: false,
		});

		const spans = h.getFinishedSpans();
		expect(spans).toHaveLength(1);
		// The turn span is the first thing an operator opens on a red trace —
		// it must carry the diagnostic payload, not a bare ERROR.
		expect(spans[0]?.status.code).toBe(SpanStatusCode.ERROR);
		expect(spans[0]?.status.message).toBe("stream died");
		expect(spans[0]?.events.some((e) => e.name === "exception")).toBe(true);
		expect(consoleError).toHaveBeenCalledOnce();
		consoleError.mockRestore();
	});

	it("marks a deliberately aborted turn as cancelled — distinguishable from success AND from failure", async () => {
		const abortController = new AbortController();
		// A clean abort can end the drain WITHOUT throwing — the harder case.
		h.chat.mockImplementation(() => {
			abortController.abort();
			return scriptedStream(["c1"])();
		});

		await streamAgentTurnToBus("conv-1", MSG, {
			kind: "connect",
			persist: false,
			abortController,
		});

		const spans = h.getFinishedSpans();
		expect(spans).toHaveLength(1);
		// Mirrors the nested chat spans' abort shape (same key + ERROR
		// "cancelled"), so turn-outcome queries treat all levels alike.
		expect(spans[0]?.attributes["tanstack.ai.completion.reason"]).toBe(
			"cancelled",
		);
		expect(spans[0]?.status.code).toBe(SpanStatusCode.ERROR);
		expect(spans[0]?.status.message).toBe("cancelled");
		// No exception event — cancellation is not a failure.
		expect(spans[0]?.events.some((e) => e.name === "exception")).toBe(false);
	});

	it("ends the span when chat() construction itself throws synchronously", async () => {
		h.chat.mockImplementation(() => {
			throw new Error("bad middleware capability");
		});
		const consoleError = vi
			.spyOn(console, "error")
			.mockImplementation(() => {});

		await streamAgentTurnToBus("conv-1", MSG, {
			kind: "connect",
			persist: false,
		});

		const spans = h.getFinishedSpans();
		expect(spans).toHaveLength(1);
		expect(spans[0]?.status.code).toBe(SpanStatusCode.ERROR);
		expect(spans[0]?.status.message).toBe("bad middleware capability");
		consoleError.mockRestore();
	});
});
