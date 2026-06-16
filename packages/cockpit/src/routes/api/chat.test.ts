// Wiring test for buildChatOptions (DAT-353) — now in lib/agent-turn, shared by
// the chat route (user sends) and the completion watcher (Phase 2A). Pure:
// asserts the bug-prone glue — the system prompt is sent as a cached block and
// the full tool registry is attached — without calling the model or touching the
// network. The live agentic loop is acceptance-tested via the compose smoke.
//
// Importing agent-turn transitively pulls config.ts + the Postgres metadata
// client (via the tool registry) + the cockpit_db conversations seam. We MOCK
// them so the test needs no real env and opens no connection — and sets NO
// process.env, which would leak across files in a reused worker and un-skip the
// gated integration tests.

import { beforeEach, describe, expect, it, vi } from "vitest";

// Born-loud kind-resolution (DAT-532) seams: a controllable getConversation + a
// spy on the bus publish, so resolveTurnKind's two error paths are deterministic.
const h = vi.hoisted(() => ({
	getConversation: vi.fn(),
	publish: vi.fn(),
}));

vi.mock("#/config", () => ({ config: { anthropicApiKey: "sk-ant-test" } }));
vi.mock("#/lib/chat-bus", () => ({
	publish: h.publish,
	subscribe: () => () => {},
	hasSubscribers: () => false,
}));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
// The driver tools (via the registry) + workspace-context import the cockpit
// control plane (DAT-461/506); mock the seams so the route import never loads the
// cockpit_db (bun:sql) client.
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
	attachRunId: async () => {},
	hasRunningRun: async () => false,
}));
// The server-owned chat loop (DAT-462) persists via the conversations seam —
// mock it so the route import never loads the cockpit_db (bun:sql) client.
vi.mock("#/db/cockpit/conversations", () => ({
	appendMessages: async () => {},
	loadModelTranscript: async () => [],
	setConversationTitle: async () => {},
	getConversation: h.getConversation,
}));

import { buildChatOptions } from "../../lib/agent-turn";
import { AGENT_LOOP_MAX_ITERATIONS, MAX_OUTPUT_TOKENS } from "../../llm";
import { resolveTurnKind } from "./chat";

/** Narrow the SDK's `SystemPrompt` union (string | {content, metadata?}) to the
 * object form buildChatOptions always emits — the return type is pinned to
 * chat()'s own options type, so the union must be narrowed before asserting on
 * `.metadata`. */
function systemPromptObjects(opts: ReturnType<typeof buildChatOptions>) {
	return (opts.systemPrompts ?? []).map((p) =>
		typeof p === "string" ? { content: p, metadata: undefined } : p,
	);
}

const MSG = [{ role: "user" as const, content: "hi" }];
const toolNames = (opts: ReturnType<typeof buildChatOptions>) =>
	(opts.tools ?? []).map((t: { name: string }) => t.name);

describe("chat route wiring (DAT-353, DAT-532)", () => {
	it("sends the kind's instructions as a cached system block", () => {
		const opts = buildChatOptions("connect", MSG);
		const prompts = systemPromptObjects(opts);
		expect(prompts).toHaveLength(1);
		const sys = prompts[0];
		expect(sys?.metadata?.cache_control).toEqual({ type: "ephemeral" });
		expect((sys?.content ?? "").length).toBeGreaterThan(0);
	});

	it("appends the workspace context as a SECOND, uncached system block (session-awareness)", () => {
		const ctx = "WORKSPACE CONTEXT — session abc";
		const opts = buildChatOptions("connect", MSG, undefined, ctx);
		const prompts = systemPromptObjects(opts);
		expect(prompts).toHaveLength(2);
		// The instructions stay the cached FIRST block (the cache breakpoint)…
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
			buildChatOptions("connect", MSG, undefined, null).systemPrompts,
		).toHaveLength(1);
	});

	it("sets the output budget via modelOptions.max_tokens — NOT a top-level maxTokens", () => {
		// THE DAT-436 root-cause pin: chat()'s TextActivityOptions has no
		// `maxTokens` field — a top-level one type-checks through an inferred
		// return while the anthropic adapter falls back to
		// `modelOptions?.max_tokens ?? 1024`, truncating every real turn
		// mid-tool-call (the severed-drain trigger behind the eternal spinners).
		const opts = buildChatOptions("connect", MSG);
		expect(opts.modelOptions).toEqual({ max_tokens: MAX_OUTPUT_TOKENS });
		expect(opts).not.toHaveProperty("maxTokens");
	});

	it("sets an explicit agent-loop budget — no silent maxIterations(5) default", () => {
		// THE DAT-449 pin, sibling of the max_tokens pin above: chat() defaults
		// agentLoopStrategy to maxIterations(5) when omitted, silently stopping a
		// multi-tool turn at iteration 5 with no error. The strategy is a pure
		// predicate over the loop state, so pin the exact budget behaviorally.
		const strategy = buildChatOptions("connect", MSG).agentLoopStrategy;
		expect(strategy).toBeDefined();
		const continues = (iterationCount: number) =>
			strategy?.({ iterationCount, messages: [], finishReason: null });
		expect(continues(AGENT_LOOP_MAX_ITERATIONS - 1)).toBe(true);
		expect(continues(AGENT_LOOP_MAX_ITERATIONS)).toBe(false);
		// And the budget itself is deliberately ABOVE the SDK default.
		expect(AGENT_LOOP_MAX_ITERATIONS).toBeGreaterThan(5);
	});

	it("fences the loop's toolstack to the chat's kind (DAT-532)", () => {
		// A Connect chat's options expose ONLY Connect's registry — a Stage-only
		// tool (begin_session) is absent; Analyse's answer is absent. Per kind.
		const connect = new Set(toolNames(buildChatOptions("connect", MSG)));
		expect(connect.has("select")).toBe(true);
		expect(connect.has("probe")).toBe(true);
		expect(connect.has("begin_session")).toBe(false);
		expect(connect.has("answer")).toBe(false);
		expect(connect.has("run_sql")).toBe(false);

		const stage = new Set(toolNames(buildChatOptions("stage", MSG)));
		expect(stage.has("begin_session")).toBe(true);
		expect(stage.has("run_sql")).toBe(true);
		expect(stage.has("answer")).toBe(false);
		expect(stage.has("select")).toBe(false);

		const analyse = new Set(toolNames(buildChatOptions("analyse", MSG)));
		expect(analyse.has("answer")).toBe(true);
		expect(analyse.has("look_table")).toBe(true);
		expect(analyse.has("run_sql")).toBe(false);
		expect(analyse.has("begin_session")).toBe(false);
	});

	it("gives each kind its own instructions (the toolstack + prompt move together)", () => {
		const connect = systemPromptObjects(buildChatOptions("connect", MSG))[0]
			?.content;
		const analyse = systemPromptObjects(buildChatOptions("analyse", MSG))[0]
			?.content;
		expect(connect).not.toBe(analyse);
	});

	it("passes the conversation messages through unchanged", () => {
		expect(buildChatOptions("connect", MSG).messages).toBe(MSG);
	});

	it("threads the abort controller into the loop so a cancelled stream stops it", () => {
		const ac = new AbortController();
		expect(buildChatOptions("connect", MSG, ac).abortController).toBe(ac);
		// Optional: omitting it is still valid (the param is optional).
		expect(buildChatOptions("connect", MSG).abortController).toBeUndefined();
	});
});

describe("resolveTurnKind — born-loud kind resolution (DAT-532)", () => {
	beforeEach(() => {
		h.getConversation.mockReset();
		h.publish.mockReset();
	});

	it("returns the conversation's kind when it resolves", async () => {
		h.getConversation.mockResolvedValue({
			id: "c1",
			workspaceId: "ws-test",
			kind: "stage",
			title: null,
		});
		expect(await resolveTurnKind("c1")).toBe("stage");
		// Happy path publishes nothing — the turn proceeds.
		expect(h.publish).not.toHaveBeenCalled();
	});

	it("surfaces a RUN_ERROR and returns null for an unknown conversation", async () => {
		h.getConversation.mockResolvedValue(null);
		expect(await resolveTurnKind("ghost")).toBeNull();
		expect(h.publish).toHaveBeenCalledTimes(1);
		const [conversationId, chunk] = h.publish.mock.calls[0];
		expect(conversationId).toBe("ghost");
		expect((chunk as { type: string }).type).toBe("RUN_ERROR");
	});

	it("surfaces a RUN_ERROR and returns null when the read throws (db down)", async () => {
		h.getConversation.mockRejectedValue(new Error("db down"));
		const warn = vi.spyOn(console, "error").mockImplementation(() => {});
		expect(await resolveTurnKind("c1")).toBeNull();
		expect(h.publish).toHaveBeenCalledTimes(1);
		expect((h.publish.mock.calls[0][1] as { type: string }).type).toBe(
			"RUN_ERROR",
		);
		warn.mockRestore();
	});
});
