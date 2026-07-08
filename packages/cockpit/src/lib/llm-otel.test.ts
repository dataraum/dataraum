// Contract test for llmOtel (DAT-706): drives the REAL chat() engine + the
// REAL @tanstack/ai otelMiddleware over a scripted two-iteration run (tool
// call → finish), with in-memory span/metric exporters. Deps float at
// `latest`, so this is the guard that a bump didn't change the emitted
// telemetry shape — same role tool-chip-state.contract.test.ts plays for the
// stream protocol.
//
// The scripted adapter mirrors @tanstack/ai-anthropic's usage semantics: each
// iteration's RUN_FINISHED carries that API call's OWN (incremental) usage —
// a stateless adapter invoked fresh per iteration cannot report run totals.

import { metrics, trace } from "@opentelemetry/api";
import {
	AggregationTemporality,
	InMemoryMetricExporter,
	MeterProvider,
	PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
	BasicTracerProvider,
	InMemorySpanExporter,
	SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { chat, toolDefinition } from "@tanstack/ai";
import { otelMiddleware } from "@tanstack/ai/middlewares/otel";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { llmOtel } from "#/lib/llm-otel";

// Gate control: `on` flips what the mocked getOtel returns. The factory only
// null-checks it; tracer/meter come from the API globals registered below.
const gate = { on: true };
vi.mock("#/otel", () => ({
	getOtel: () => (gate.on ? ({} as never) : null),
}));

// Register global in-memory providers ONCE for the file — the OTel API allows
// a single global registration per process.
const spanExporter = new InMemorySpanExporter();
trace.setGlobalTracerProvider(
	new BasicTracerProvider({
		spanProcessors: [new SimpleSpanProcessor(spanExporter)],
	}),
);
// DELTA + a drain in beforeEach: each test's flush reports only its own run —
// the meter is a process-global, its cumulative state would otherwise bleed
// across tests.
const metricExporter = new InMemoryMetricExporter(AggregationTemporality.DELTA);
const meterProvider = new MeterProvider({
	readers: [
		new PeriodicExportingMetricReader({
			exporter: metricExporter,
			// Manual forceFlush() only — no timer racing the assertions.
			exportIntervalMillis: 3_600_000,
		}),
	],
});
metrics.setGlobalMeterProvider(meterProvider);

// ---------------------------------------------------------------------------
// Scripted adapter — iteration 1 calls the tool and reports 500/50 (+cache
// split), iteration 2 closes with text and reports 700/80. Rollup truth:
// input 1200 / output 130 / cache_read 400 / cache_write 100.
// ---------------------------------------------------------------------------

const TOOL_CALL_ID = "tc-otel-1";
const TOOL_NAME = "workflow_status";
const TOOL_ARGS = '{"workflow_id":"wf-1"}';

const ITER1_USAGE = {
	promptTokens: 500,
	completionTokens: 50,
	totalTokens: 550,
	promptTokensDetails: { cachedTokens: 400, cacheWriteTokens: 100 },
};
const ITER2_USAGE = {
	promptTokens: 700,
	completionTokens: 80,
	totalTokens: 780,
};

function scriptedAdapter() {
	let call = 0;
	return {
		kind: "text" as const,
		name: "scripted",
		provider: "scripted",
		model: "scripted-model",
		async *chatStream(options: {
			messages: ReadonlyArray<{ role: string }>;
			threadId?: string;
		}) {
			call += 1;
			const runId = `run-${call}`;
			const threadId = options.threadId ?? "thread-otel";
			const base = { model: "scripted-model", timestamp: Date.now() };
			yield { type: "RUN_STARTED", runId, threadId, ...base };
			const toolResultSeen = options.messages.some((m) => m.role === "tool");
			if (!toolResultSeen) {
				yield {
					type: "TOOL_CALL_START",
					toolCallId: TOOL_CALL_ID,
					toolCallName: TOOL_NAME,
					toolName: TOOL_NAME,
					index: 0,
					...base,
				};
				yield {
					type: "TOOL_CALL_ARGS",
					toolCallId: TOOL_CALL_ID,
					delta: TOOL_ARGS,
					args: TOOL_ARGS,
					...base,
				};
				yield {
					type: "TOOL_CALL_END",
					toolCallId: TOOL_CALL_ID,
					toolCallName: TOOL_NAME,
					toolName: TOOL_NAME,
					input: JSON.parse(TOOL_ARGS),
					...base,
				};
				yield {
					type: "RUN_FINISHED",
					runId,
					threadId,
					finishReason: "tool_calls",
					usage: ITER1_USAGE,
					...base,
				};
			} else {
				const messageId = `msg-${call}`;
				yield {
					type: "TEXT_MESSAGE_START",
					messageId,
					role: "assistant",
					...base,
				};
				yield {
					type: "TEXT_MESSAGE_CONTENT",
					messageId,
					delta: "done",
					...base,
				};
				yield { type: "TEXT_MESSAGE_END", messageId, ...base };
				yield {
					type: "RUN_FINISHED",
					runId,
					threadId,
					finishReason: "stop",
					usage: ITER2_USAGE,
					...base,
				};
			}
		},
	};
}

const statusTool = toolDefinition({
	name: TOOL_NAME,
	description: "status of a workflow",
	inputSchema: z.object({ workflow_id: z.string() }),
}).server(async () => ({ ok: true }));

type ChatAdapter = Parameters<typeof chat>[0]["adapter"];

async function runScriptedChat(
	middleware: Parameters<typeof chat>[0]["middleware"],
) {
	const stream = chat({
		adapter: scriptedAdapter() as unknown as ChatAdapter,
		messages: [{ id: "u1", role: "user", content: "status of wf-1?" }],
		tools: [statusTool],
		middleware,
	});
	for await (const _chunk of stream) {
		// drain
	}
}

beforeEach(async () => {
	gate.on = true;
	spanExporter.reset();
	await meterProvider.forceFlush();
	metricExporter.reset();
});

describe("llmOtel", () => {
	it("returns no middleware when telemetry is off", () => {
		gate.on = false;
		expect(llmOtel("orchestrator")).toEqual([]);
	});

	it("stamps the call site and provider on every span and rolls up root usage", async () => {
		await runScriptedChat(llmOtel("answer_subagent"));

		const spans = spanExporter.getFinishedSpans();
		// tool + 2 iterations + root chat span.
		expect(spans).toHaveLength(4);
		for (const span of spans) {
			expect(span.attributes["dataraum.call_site"]).toBe("answer_subagent");
			expect(span.attributes["gen_ai.provider.name"]).toBe("scripted");
		}

		const root = spans.find((s) => s.name === "chat scripted-model");
		expect(root).toBeDefined();
		// The companion's rollup — NOT the last iteration's 700/80 (see the
		// canary below for the raw-middleware behavior this corrects).
		// total_tokens included: the middleware stamps it from the same stale
		// FinishInfo.usage, so an uncorrected span would carry 780 next to a
		// corrected 1200/130.
		expect(root?.attributes["gen_ai.usage.input_tokens"]).toBe(1200);
		expect(root?.attributes["gen_ai.usage.output_tokens"]).toBe(130);
		expect(root?.attributes["gen_ai.usage.total_tokens"]).toBe(1330);
		expect(root?.attributes["gen_ai.usage.cache_read.input_tokens"]).toBe(400);
		expect(root?.attributes["gen_ai.usage.cache_creation.input_tokens"]).toBe(
			100,
		);

		// Iteration spans keep their own incremental usage untouched.
		const iter0 = spans.find((s) => s.name === "chat scripted-model #0");
		expect(iter0?.attributes["gen_ai.usage.input_tokens"]).toBe(500);
		const iter1 = spans.find((s) => s.name === "chat scripted-model #1");
		expect(iter1?.attributes["gen_ai.usage.input_tokens"]).toBe(700);
	});

	it("records the two GenAI client histograms", async () => {
		await runScriptedChat(llmOtel("orchestrator"));
		await meterProvider.forceFlush();

		const recorded = metricExporter
			.getMetrics()
			.flatMap((rm) => rm.scopeMetrics)
			.flatMap((sm) => sm.metrics);
		const names = recorded.map((m) => m.descriptor.name);
		expect(names).toContain("gen_ai.client.token.usage");
		expect(names).toContain("gen_ai.client.operation.duration");

		const tokenUsage = recorded.find(
			(m) => m.descriptor.name === "gen_ai.client.token.usage",
		);
		const inputPoints = (tokenUsage?.dataPoints ?? []).filter(
			(dp) => dp.attributes["gen_ai.token.type"] === "input",
		);
		const inputSum = inputPoints.reduce(
			(acc, dp) => acc + ((dp.value as { sum?: number }).sum ?? 0),
			0,
		);
		expect(inputSum).toBe(1200);
	});

	// -------------------------------------------------------------------------
	// UPSTREAM CANARY (delete the usage-rollup companion when this fails):
	// @tanstack/ai's docs say the root span "rolls up usage across all
	// iterations", but 0.40.0's engine passes FinishInfo.usage = the LAST
	// iteration's usage only (https://github.com/TanStack/ai/issues/916).
	// When a bump makes this test fail, the engine started rolling up —
	// remove the companion from llmOtel and fold this assertion into the
	// contract test above.
	// -------------------------------------------------------------------------
	it("canary: raw otelMiddleware root span still under-reports multi-iteration usage", async () => {
		await runScriptedChat([
			otelMiddleware({ tracer: trace.getTracer("canary") }),
		]);

		const root = spanExporter
			.getFinishedSpans()
			.find((s) => s.name === "chat scripted-model");
		expect(root?.attributes["gen_ai.usage.input_tokens"]).toBe(
			ITER2_USAGE.promptTokens,
		);
		expect(root?.attributes["gen_ai.usage.output_tokens"]).toBe(
			ITER2_USAGE.completionTokens,
		);
	});
});
