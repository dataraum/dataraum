// Unit test for the DAT-600 LLM telemetry middleware. Drives the hooks with
// synthetic SDK fixtures (no chat() / no network) and asserts the emitted
// `llm_call` line — snake_case keys + iteration count — mirrors the engine half.

import type {
	ChatMiddlewareContext,
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
	it("emits one llm_call with label, model, elapsed, mapped tokens, and iteration count", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = llmTelemetryMiddleware("answer_subagent");

		// Three agent-loop iterations before the run finishes.
		mw.onIteration?.(ctx("claude-x"), iter(0));
		mw.onIteration?.(ctx("claude-x"), iter(1));
		mw.onIteration?.(ctx("claude-x"), iter(2));
		mw.onFinish?.(
			ctx("claude-x"),
			finish({
				duration: 4200.7,
				usage: usage({
					promptTokens: 512,
					completionTokens: 64,
					promptTokensDetails: { cachedTokens: 480, cacheWriteTokens: 32 },
				}),
			}),
		);

		expect(info).toHaveBeenCalledTimes(1);
		expect(info).toHaveBeenCalledWith("llm_call", {
			label: "answer_subagent",
			model: "claude-x",
			elapsed_ms: 4201, // rounded
			input_tokens: 512,
			output_tokens: 64,
			cache_read_input_tokens: 480,
			cache_creation_input_tokens: 32,
			iterations: 3,
		});
	});

	it("coerces missing usage / cache details to zero", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = llmTelemetryMiddleware("orchestrator");

		// No usage on the finish info, and no iterations recorded.
		mw.onFinish?.(ctx("claude-y"), finish({ duration: 10, usage: undefined }));

		expect(info).toHaveBeenCalledWith("llm_call", {
			label: "orchestrator",
			model: "claude-y",
			elapsed_ms: 10,
			input_tokens: 0,
			output_tokens: 0,
			cache_read_input_tokens: 0,
			cache_creation_input_tokens: 0,
			iterations: 0,
		});
	});

	it("keeps the iteration counter private per middleware instance", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const a = llmTelemetryMiddleware("a");
		const b = llmTelemetryMiddleware("b");

		a.onIteration?.(ctx("m"), iter(0));
		a.onIteration?.(ctx("m"), iter(1));
		b.onIteration?.(ctx("m"), iter(0));

		a.onFinish?.(ctx("m"), finish({}));
		b.onFinish?.(ctx("m"), finish({}));

		expect(info).toHaveBeenNthCalledWith(
			1,
			"llm_call",
			expect.objectContaining({ label: "a", iterations: 2 }),
		);
		expect(info).toHaveBeenNthCalledWith(
			2,
			"llm_call",
			expect.objectContaining({ label: "b", iterations: 1 }),
		);
	});
});
