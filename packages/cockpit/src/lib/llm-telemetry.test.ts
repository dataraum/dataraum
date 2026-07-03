// Unit test for the DAT-600 LLM telemetry middleware. Drives the hooks with
// synthetic SDK fixtures (no chat() / no network) and asserts the emitted
// `llm_call` line — snake_case keys, terminal status, iteration count, and the
// per-iteration usage ACCUMULATION (DAT-663) — mirrors the engine half.

import type {
	AbortInfo,
	ChatMiddlewareContext,
	ErrorInfo,
	FinishInfo,
	IterationInfo,
	UsageInfo,
} from "@tanstack/ai";
import { afterEach, describe, expect, it, vi } from "vitest";

import { llmTelemetryMiddleware } from "./llm-telemetry";

const ctx = (model: string) => ({ model }) as unknown as ChatMiddlewareContext;

const iter = (i: number) => ({ iteration: i }) as unknown as IterationInfo;

const finish = (over: Partial<FinishInfo>): FinishInfo =>
	({
		finishReason: "stop",
		duration: 1234,
		content: "",
		...over,
	}) as FinishInfo;

const usage = (over: Partial<UsageInfo>): UsageInfo =>
	({
		promptTokens: 0,
		completionTokens: 0,
		totalTokens: 0,
		...over,
	}) as UsageInfo;

afterEach(() => vi.restoreAllMocks());

describe("llmTelemetryMiddleware", () => {
	it("emits one finished llm_call with label, model, elapsed, iterations, and usage SUMMED over iterations", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = llmTelemetryMiddleware("answer_subagent");

		// Three agent-loop iterations, each reporting ITS OWN (incremental) usage —
		// the SDK fires onUsage once per usage-bearing RUN_FINISHED.
		mw.onIteration?.(ctx("claude-x"), iter(0));
		mw.onUsage?.(
			ctx("claude-x"),
			usage({
				promptTokens: 500,
				completionTokens: 50,
				promptTokensDetails: { cachedTokens: 400, cacheWriteTokens: 100 },
			}),
		);
		mw.onIteration?.(ctx("claude-x"), iter(1));
		mw.onUsage?.(
			ctx("claude-x"),
			usage({
				promptTokens: 700,
				completionTokens: 30,
				promptTokensDetails: { cachedTokens: 600 },
			}),
		);
		mw.onIteration?.(ctx("claude-x"), iter(2));
		const lastIterationUsage = usage({
			promptTokens: 800,
			completionTokens: 20,
			promptTokensDetails: { cachedTokens: 700, cacheWriteTokens: 10 },
		});
		mw.onUsage?.(ctx("claude-x"), lastIterationUsage);
		// FinishInfo.usage is the LAST iteration's RUN_FINISHED usage (the SDK never
		// sums) — the middleware must IGNORE it, or the final iteration double-counts.
		mw.onFinish?.(
			ctx("claude-x"),
			finish({ duration: 4200.7, usage: lastIterationUsage }),
		);

		expect(info).toHaveBeenCalledTimes(1);
		expect(info).toHaveBeenCalledWith("llm_call", {
			label: "answer_subagent",
			model: "claude-x",
			status: "finished",
			elapsed_ms: 4201, // rounded
			input_tokens: 2000,
			output_tokens: 100,
			cache_read_input_tokens: 1700,
			cache_creation_input_tokens: 110,
			iterations: 3,
		});
	});

	it("coerces missing usage / cache details to zero", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = llmTelemetryMiddleware("orchestrator");

		// No onUsage ever fired, and no iterations recorded.
		mw.onFinish?.(ctx("claude-y"), finish({ duration: 10, usage: undefined }));

		expect(info).toHaveBeenCalledWith("llm_call", {
			label: "orchestrator",
			model: "claude-y",
			status: "finished",
			elapsed_ms: 10,
			input_tokens: 0,
			output_tokens: 0,
			cache_read_input_tokens: 0,
			cache_creation_input_tokens: 0,
			iterations: 0,
		});
	});

	it("logs aborted runs with the usage accumulated up to the abort (DAT-663 drain-abort)", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = llmTelemetryMiddleware("frame_family");

		// Two iterations report usage, then the caller drain-aborts (the forced
		// tool fired) — the aborted row must carry the real accumulated spend.
		mw.onIteration?.(ctx("claude-x"), iter(0));
		mw.onUsage?.(
			ctx("claude-x"),
			usage({
				promptTokens: 1200,
				completionTokens: 40,
				promptTokensDetails: { cachedTokens: 1000, cacheWriteTokens: 200 },
			}),
		);
		mw.onIteration?.(ctx("claude-x"), iter(1));
		mw.onUsage?.(
			ctx("claude-x"),
			usage({ promptTokens: 1300, completionTokens: 60 }),
		);
		mw.onAbort?.(ctx("claude-x"), {
			reason: "captured",
			duration: 87.4,
		} as AbortInfo);

		expect(info).toHaveBeenCalledWith("llm_call", {
			label: "frame_family",
			model: "claude-x",
			status: "aborted",
			elapsed_ms: 87,
			input_tokens: 2500,
			output_tokens: 100,
			cache_read_input_tokens: 1000,
			cache_creation_input_tokens: 200,
			iterations: 2,
		});
	});

	it("logs errored runs with the usage accumulated before the failure", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = llmTelemetryMiddleware("grounding");

		mw.onIteration?.(ctx("claude-x"), iter(0));
		mw.onUsage?.(
			ctx("claude-x"),
			usage({ promptTokens: 300, completionTokens: 25 }),
		);
		mw.onError?.(ctx("claude-x"), {
			error: new Error("boom"),
			duration: 200,
		} as ErrorInfo);

		expect(info).toHaveBeenCalledWith("llm_call", {
			label: "grounding",
			model: "claude-x",
			status: "error",
			elapsed_ms: 200,
			input_tokens: 300,
			output_tokens: 25,
			cache_read_input_tokens: 0,
			cache_creation_input_tokens: 0,
			iterations: 1,
		});
	});

	it("keeps the iteration counter and usage accumulator private per middleware instance", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const a = llmTelemetryMiddleware("a");
		const b = llmTelemetryMiddleware("b");

		a.onIteration?.(ctx("m"), iter(0));
		a.onUsage?.(ctx("m"), usage({ promptTokens: 10, completionTokens: 1 }));
		a.onIteration?.(ctx("m"), iter(1));
		b.onIteration?.(ctx("m"), iter(0));
		b.onUsage?.(ctx("m"), usage({ promptTokens: 7, completionTokens: 2 }));

		a.onFinish?.(ctx("m"), finish({}));
		b.onFinish?.(ctx("m"), finish({}));

		expect(info).toHaveBeenNthCalledWith(
			1,
			"llm_call",
			expect.objectContaining({ label: "a", iterations: 2, input_tokens: 10 }),
		);
		expect(info).toHaveBeenNthCalledWith(
			2,
			"llm_call",
			expect.objectContaining({ label: "b", iterations: 1, input_tokens: 7 }),
		);
	});
});
