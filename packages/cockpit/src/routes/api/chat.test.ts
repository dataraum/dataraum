// Wiring test for the chat route's buildChatOptions (DAT-353). Pure: asserts the
// bug-prone glue — the system prompt is sent as a cached block and the full tool
// registry is attached — without calling the model or touching the network.
// The live agentic loop is acceptance-tested via the compose smoke (real LLM).
//
// Importing the route transitively pulls config.ts + the Postgres metadata
// client (via the tool registry). We MOCK both so the test needs no real env and
// opens no connection — and sets NO process.env, which would leak across files
// in a reused worker and un-skip the gated integration tests.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { anthropicApiKey: "sk-ant-test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { MAX_OUTPUT_TOKENS } from "../../llm";
import { buildChatOptions } from "./chat";

/** Narrow the SDK's `SystemPrompt` union (string | {content, metadata?}) to the
 * object form buildChatOptions always emits — the return type is pinned to
 * chat()'s own options type, so the union must be narrowed before asserting on
 * `.metadata`. */
function systemPromptObjects(opts: ReturnType<typeof buildChatOptions>) {
	return (opts.systemPrompts ?? []).map((p) =>
		typeof p === "string" ? { content: p, metadata: undefined } : p,
	);
}

describe("chat route wiring (DAT-353)", () => {
	it("sends the orchestrator instructions as a cached system block", () => {
		const opts = buildChatOptions([{ role: "user", content: "hi" }]);
		const prompts = systemPromptObjects(opts);
		expect(prompts).toHaveLength(1);
		const sys = prompts[0];
		expect(sys?.metadata?.cache_control).toEqual({ type: "ephemeral" });
		expect((sys?.content ?? "").length).toBeGreaterThan(0);
	});

	it("appends the workspace context as a SECOND, uncached system block (session-awareness)", () => {
		const ctx = "WORKSPACE CONTEXT — session abc";
		const opts = buildChatOptions(
			[{ role: "user", content: "hi" }],
			undefined,
			ctx,
		);
		const prompts = systemPromptObjects(opts);
		expect(prompts).toHaveLength(2);
		// The orchestrator stays the cached FIRST block (the cache breakpoint)…
		expect(prompts[0]?.metadata?.cache_control).toEqual({
			type: "ephemeral",
		});
		// …the session context is the SECOND block, past the breakpoint → no
		// cache_control, so it's never cached (a fresh suffix each turn).
		expect(prompts[1]?.content).toBe(ctx);
		expect(prompts[1]?.metadata).toBeUndefined();
	});

	it("omits the second block when there is no current session", () => {
		expect(
			buildChatOptions([{ role: "user", content: "hi" }], undefined, null)
				.systemPrompts,
		).toHaveLength(1);
	});

	it("sets the output budget via modelOptions.max_tokens — NOT a top-level maxTokens", () => {
		// THE DAT-436 root-cause pin: chat()'s TextActivityOptions has no
		// `maxTokens` field — a top-level one type-checks through an inferred
		// return while the anthropic adapter falls back to
		// `modelOptions?.max_tokens ?? 1024`, truncating every real turn
		// mid-tool-call (the severed-drain trigger behind the eternal spinners).
		const opts = buildChatOptions([{ role: "user", content: "hi" }]);
		expect(opts.modelOptions).toEqual({ max_tokens: MAX_OUTPUT_TOKENS });
		expect(opts).not.toHaveProperty("maxTokens");
	});

	it("attaches the full tool registry to the loop", () => {
		const opts = buildChatOptions([{ role: "user", content: "hi" }]);
		const names = (opts.tools ?? []).map((t: { name: string }) => t.name);
		expect(new Set(names)).toEqual(
			new Set([
				"list_sources",
				"list_tables",
				"list_verticals",
				"look_table",
				"why_column",
				"why_table",
				"look_relationships",
				"why_relationship",
				"run_sql",
				"probe",
				"connect",
				"frame",
				"select",
				"teach",
				"begin_session",
				"replay",
				"workflow_status",
				"upload",
			]),
		);
	});

	it("passes the conversation messages through unchanged", () => {
		const messages = [{ role: "user" as const, content: "hi" }];
		expect(buildChatOptions(messages).messages).toBe(messages);
	});

	it("threads the abort controller into the loop so a cancelled stream stops it", () => {
		const ac = new AbortController();
		expect(
			buildChatOptions([{ role: "user", content: "hi" }], ac).abortController,
		).toBe(ac);
		// Optional: omitting it is still valid (the param is optional).
		expect(
			buildChatOptions([{ role: "user", content: "hi" }]).abortController,
		).toBeUndefined();
	});
});
