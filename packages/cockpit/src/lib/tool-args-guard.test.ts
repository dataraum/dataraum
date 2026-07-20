// Unit tests for the DAT-661 tool-args guard. Drives the pure coercion + the
// middleware hooks with synthetic SDK fixtures (no chat() / no network): the
// onConfig schema wrap (rescue + idempotency), the onToolPhaseComplete rejection
// counter, and the onError parse-failure counter.

import type {
	ChatMiddleware,
	ChatMiddlewareConfig,
	ChatMiddlewareContext,
	ErrorInfo,
	Tool,
	ToolPhaseCompleteInfo,
} from "@tanstack/ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import {
	coerceStringifiedToolArgs,
	readTopLevelProperties,
	type TopLevelProperties,
	toolArgsGuardMiddleware,
} from "./tool-args-guard";

const ctx = () => ({ model: "claude-x" }) as unknown as ChatMiddlewareContext;

const cfg = (tools: Tool[]): ChatMiddlewareConfig => ({
	messages: [],
	systemPrompts: [],
	tools,
});

const phase = (
	results: ToolPhaseCompleteInfo["results"],
): ToolPhaseCompleteInfo => ({
	toolCalls: [],
	results,
	needsApproval: [],
	needsClientExecution: [],
});

/** Resolve onConfig's transformed tools (the hooks are sync in practice). */
async function runOnConfig(mw: ChatMiddleware, config: ChatMiddlewareConfig) {
	return await mw.onConfig?.(ctx(), config);
}

afterEach(() => vi.restoreAllMocks());

describe("readTopLevelProperties", () => {
	it("collects every top-level key and the exactly-typed container fields", () => {
		const props = readTopLevelProperties(
			z.object({
				steps: z.array(z.object({ name: z.string() })),
				deps: z.record(z.string(), z.array(z.string())),
				final_sql: z.string(),
				// Nullable containers surface as anyOf (no top-level type) and are
				// deliberately NOT rescue candidates — engine parity.
				cfg: z.object({ a: z.string() }).nullable(),
			}),
		);
		expect(props).not.toBeNull();
		expect([...(props?.allKeys ?? [])].sort()).toEqual([
			"cfg",
			"deps",
			"final_sql",
			"steps",
		]);
		expect(Object.fromEntries(props?.containers ?? [])).toEqual({
			steps: "array",
			deps: "object",
		});
	});

	it("returns null for schemas without an object shape", () => {
		expect(readTopLevelProperties(z.string())).toBeNull();
	});
});

describe("coerceStringifiedToolArgs", () => {
	const props: TopLevelProperties = {
		allKeys: new Set(["steps", "final_sql"]),
		containers: new Map([["steps", "array"]]),
	};

	it("parse-rescues a stringified container field and reports the argument", () => {
		const coerced: string[] = [];
		const out = coerceStringifiedToolArgs(
			{ steps: '[{"name":"n","sql":"s"}]', final_sql: "select 1" },
			props,
			(a) => coerced.push(a),
		);
		expect(out).toEqual({
			steps: [{ name: "n", sql: "s" }],
			final_sql: "select 1",
		});
		expect(coerced).toEqual(["steps"]);
	});

	it("adopts a whole-payload string wholesale when its keys are the tool's own", () => {
		const coerced: string[] = [];
		const out = coerceStringifiedToolArgs(
			{ steps: '{"steps": [{"name":"n","sql":"s"}], "final_sql": "select 1"}' },
			props,
			(a) => coerced.push(a),
		);
		expect(out).toEqual({
			steps: [{ name: "n", sql: "s" }],
			final_sql: "select 1",
		});
		expect(coerced).toEqual(["steps"]);
	});

	it("leaves unparseable strings for zod to reject, without reporting", () => {
		const coerced: string[] = [];
		const input = { steps: "not json", final_sql: "x" };
		const out = coerceStringifiedToolArgs(input, props, (a) => coerced.push(a));
		expect(out).toBe(input); // untouched — same reference
		expect(coerced).toEqual([]);
	});

	it("touches neither string scalars on non-container fields nor well-formed input", () => {
		const coerced: string[] = [];
		const wellFormed = { steps: [{ name: "n" }], final_sql: "select 1" };
		expect(
			coerceStringifiedToolArgs(wellFormed, props, (a) => coerced.push(a)),
		).toBe(wellFormed);
		// A parsed scalar (JSON.parse("42") → number) is not a container — skip.
		const scalar = { steps: "42", final_sql: "x" };
		expect(
			coerceStringifiedToolArgs(scalar, props, (a) => coerced.push(a)),
		).toBe(scalar);
		expect(coerced).toEqual([]);
	});

	it("passes non-object raw values through (the SDK flattens those to {} anyway)", () => {
		const coerced: string[] = [];
		expect(
			coerceStringifiedToolArgs("raw", props, (a) => coerced.push(a)),
		).toBe("raw");
		expect(coerced).toEqual([]);
	});
});

describe("toolArgsGuardMiddleware", () => {
	const makeTool = (): Tool => ({
		name: "run_steps",
		description: "d",
		inputSchema: z.object({
			steps: z.array(z.object({ name: z.string(), sql: z.string() })),
			final_sql: z.string(),
		}),
		execute: async () => ({ ok: true }),
	});

	it("wraps zod tool schemas so a stringified container field validates after rescue, logging tool_args_coerced", async () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = toolArgsGuardMiddleware("answer_subagent");
		const tool = makeTool();

		const out = await runOnConfig(mw, cfg([tool]));
		expect(out?.tools).toHaveLength(1);
		const guarded = out?.tools?.[0];
		expect(guarded?.inputSchema).not.toBe(tool.inputSchema);
		// The handler-facing contract is preserved.
		expect(guarded?.name).toBe("run_steps");
		expect(guarded?.execute).toBe(tool.execute);

		// The SDK validates through the (wrapped) inputSchema — a stringified
		// `steps` now parses and validates instead of rejecting.
		const parsed = (guarded?.inputSchema as z.ZodType).safeParse({
			steps: '[{"name":"n","sql":"select 1"}]',
			final_sql: "select * from n",
		});
		expect(parsed.success).toBe(true);
		expect(parsed.data).toEqual({
			steps: [{ name: "n", sql: "select 1" }],
			final_sql: "select * from n",
		});
		expect(info).toHaveBeenCalledWith("tool_args_coerced", {
			label: "answer_subagent",
			tool: "run_steps",
			argument: "steps",
		});
	});

	it("still rejects genuinely invalid args after the wrap (the rescue admits nothing)", async () => {
		vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = toolArgsGuardMiddleware("answer_subagent");
		const out = await runOnConfig(mw, cfg([makeTool()]));
		const parsed = (out?.tools?.[0]?.inputSchema as z.ZodType).safeParse({
			steps: '["not-a-step"]',
			final_sql: "x",
		});
		expect(parsed.success).toBe(false);
	});

	it("is idempotent across onConfig re-fires (tools persist between iterations)", async () => {
		vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = toolArgsGuardMiddleware("answer_subagent");
		const first = await runOnConfig(mw, cfg([makeTool()]));
		expect(first?.tools).toBeDefined();
		// The engine feeds the TRANSFORMED tools back on the next iteration.
		const second = await runOnConfig(mw, cfg(first?.tools as Tool[]));
		expect(second).toBeUndefined(); // no re-wrap, no change
	});

	it("leaves tools without rescuable container fields untouched", async () => {
		const mw = toolArgsGuardMiddleware("nav");
		const scalarTool: Tool = {
			name: "pick",
			description: "d",
			inputSchema: z.object({ kind: z.string() }),
			execute: async () => ({ ok: true }),
		};
		expect(await runOnConfig(mw, cfg([scalarTool]))).toBeUndefined();
	});

	it("logs tool_args_rejected for the SDK's input-validation failures only", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = toolArgsGuardMiddleware("answer_subagent");

		mw.onToolPhaseComplete?.(
			ctx(),
			phase([
				{
					toolCallId: "t1",
					toolName: "emit_result",
					result: {
						error:
							"Input validation failed for tool emit_result: Validation failed: expected array, received string",
					},
				},
				// A handler-authored agent-error envelope — NOT a validation failure.
				{
					toolCallId: "t2",
					toolName: "run_steps",
					result: { error: "no such table: lake.raw.orders" },
				},
				// A plain success.
				{ toolCallId: "t3", toolName: "run_steps", result: { ok: true } },
			]),
		);

		expect(info).toHaveBeenCalledTimes(1);
		expect(info).toHaveBeenCalledWith("tool_args_rejected", {
			label: "answer_subagent",
			tool: "emit_result",
			error:
				"Input validation failed for tool emit_result: Validation failed: expected array, received string",
		});
	});

	it("counts the run-fatal unparseable-args path via onError, and ignores other errors", () => {
		const info = vi.spyOn(console, "info").mockImplementation(() => {});
		const mw = toolArgsGuardMiddleware("grounding");

		mw.onError?.(ctx(), {
			error: new Error('Failed to parse tool arguments as JSON: "{oops'),
			duration: 12,
		} as ErrorInfo);
		mw.onError?.(ctx(), {
			error: new Error("overloaded_error"),
			duration: 12,
		} as ErrorInfo);

		expect(info).toHaveBeenCalledTimes(1);
		expect(info).toHaveBeenCalledWith("tool_args_rejected", {
			label: "grounding",
			tool: null,
			error: 'Failed to parse tool arguments as JSON: "{oops',
		});
	});
});
