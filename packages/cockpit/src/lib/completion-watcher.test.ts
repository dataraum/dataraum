// Run-routing proof (DAT-528) — THE load-bearing acceptance criterion: a run
// started in conversation A narrates into A, even when another conversation's
// watcher is live. Before DAT-528 the watcher tracked ALL of a workspace's runs
// and whichever stream's claim landed first narrated it (order-dependent). Now
// each watcher tracks only `listWatchableRuns(itsConversationId)`, so a run only
// surfaces in the watcher of the chat that started it.
//
// We drive `pollOnce` (the extracted poll tick) directly — no timer, no loop. The
// `listWatchableRuns` mock is CONVERSATION-SCOPED exactly as the real SQL is
// (returns runs only for the queried id), so the test exercises the real routing
// contract: the watcher passes its own conversationId, the query filters on it.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	// The one in-flight run — it belongs to conversation "A".
	runForA: {
		workflowId: "wf-A",
		runId: "r-A",
		stage: "begin_session" as const,
	},
	narrated: [] as string[],
}));

vi.mock("#/db/cockpit/conversations", () => ({
	appendMessages: vi.fn(async () => {}),
	loadModelTranscript: vi.fn(async () => []),
}));

vi.mock("#/db/cockpit/runs", () => ({
	// CONVERSATION-SCOPED, like the real query: only conversation "A" has the run.
	listWatchableRuns: vi.fn(async (conversationId: string) =>
		conversationId === "A" ? [h.runForA] : [],
	),
	listRunningStages: vi.fn(async () => []),
	markRunStatus: vi.fn(async () => {}),
	claimRunNarration: vi.fn(async () => true),
}));

vi.mock("#/lib/chat-bus", () => ({
	hasSubscribers: vi.fn(() => true),
	publish: vi.fn(),
}));

vi.mock("#/lib/agent-turn", () => ({
	// Record which conversation each narration turn targets — the routing assertion.
	streamAgentTurnToBus: vi.fn(async (conversationId: string) => {
		h.narrated.push(conversationId);
	}),
}));

vi.mock("#/lib/model-messages", () => ({ buildModelMessages: () => [] }));
vi.mock("#/lib/completion-note", () => ({
	completionNote: () => ({ id: "note", role: "user", parts: [] }),
}));
vi.mock("#/prompts/workspace-context", () => ({
	buildWorkspaceContext: async () => null,
}));
vi.mock("#/temporal/progress", () => ({
	getWorkflowProgress: vi.fn(async () => ({ done: true, status: "COMPLETED" })),
	terminalRunStatus: () => "completed",
}));

import type { WatchableRun } from "#/db/cockpit/runs";
import { streamAgentTurnToBus } from "#/lib/agent-turn";
import { pollOnce } from "./completion-watcher";

beforeEach(() => {
	h.narrated = [];
	vi.clearAllMocks();
});
afterEach(() => vi.restoreAllMocks());

describe("run-routing (DAT-528): a run narrates only into its own conversation", () => {
	it("conversation B's watcher does NOT narrate A's run", async () => {
		await pollOnce(
			"B",
			new Map<string, WatchableRun>(),
			new AbortController().signal,
		);
		expect(streamAgentTurnToBus).not.toHaveBeenCalled();
	});

	it("conversation A's watcher narrates A's run, into A", async () => {
		await pollOnce(
			"A",
			new Map<string, WatchableRun>(),
			new AbortController().signal,
		);
		expect(streamAgentTurnToBus).toHaveBeenCalledTimes(1);
		// The narration turn targets conversation A — the routing guarantee.
		expect(h.narrated).toEqual(["A"]);
	});

	it("A narrates into A even while B's watcher polls concurrently (the proof)", async () => {
		const trackedA = new Map<string, WatchableRun>();
		const trackedB = new Map<string, WatchableRun>();
		const signal = new AbortController().signal;
		// Both watchers tick; only A's tracks (and narrates) the run.
		await Promise.all([
			pollOnce("A", trackedA, signal),
			pollOnce("B", trackedB, signal),
		]);
		expect(h.narrated).toEqual(["A"]);
		expect(trackedB.size).toBe(0);
	});
});
