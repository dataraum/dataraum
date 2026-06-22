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
		kind: "begin_session" as const,
	},
	narrated: [] as string[],
}));

vi.mock("#/db/cockpit/conversations", () => ({
	// narrateCompletion resolves the chat's kind (DAT-532) before narrating; a
	// non-null row lets the narration proceed (kind drives its toolstack).
	getConversation: vi.fn(async (id: string) => ({
		id,
		workspaceId: "ws-1",
		kind: "stage",
		title: null,
	})),
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

import { listWatchableRuns, type WatchableRun } from "#/db/cockpit/runs";
import { streamAgentTurnToBus } from "#/lib/agent-turn";
import { getWorkflowProgress } from "#/temporal/progress";
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

describe("placeholder runs are skipped until attachRunId finalizes the real id (DAT-595)", () => {
	it("does NOT track, poll, or narrate a run whose runId still equals its workflowId", async () => {
		// A just-recorded run carries the workflowId PLACEHOLDER until attachRunId
		// rewrites it post-start. getWorkflowProgress can only PIN a real id, so the
		// watcher must skip the placeholder — otherwise a reused workflow id would
		// resolve a PRIOR run's terminal state and mark this one done off it (DAT-595).
		vi.mocked(listWatchableRuns).mockResolvedValueOnce([
			{
				workflowId: "addsource-ws",
				runId: "addsource-ws",
				stage: "add_source",
				kind: "onboarding",
			},
		]);
		const tracked = new Map<string, WatchableRun>();
		await pollOnce("A", tracked, new AbortController().signal);
		expect(tracked.size).toBe(0);
		expect(getWorkflowProgress).not.toHaveBeenCalled();
		expect(streamAgentTurnToBus).not.toHaveBeenCalled();
	});
});

describe("import vs teach→replay narration (DAT-597)", () => {
	it("does NOT narrate an onboarding add_source run — the hub owns import progress", async () => {
		// Real id (runId != workflowId) so it clears the DAT-595 placeholder skip;
		// the DAT-597 gate then suppresses the chat echo of an import.
		vi.mocked(listWatchableRuns).mockResolvedValueOnce([
			{
				workflowId: "addsource-ws",
				runId: "real-run",
				stage: "add_source",
				kind: "onboarding",
			},
		]);
		await pollOnce(
			"A",
			new Map<string, WatchableRun>(),
			new AbortController().signal,
		);
		expect(streamAgentTurnToBus).not.toHaveBeenCalled();
	});

	it("DOES narrate a replay add_source run — the teach→re-ground verification", async () => {
		vi.mocked(listWatchableRuns).mockResolvedValueOnce([
			{
				workflowId: "addsource-ws",
				runId: "real-run",
				stage: "add_source",
				kind: "replay",
			},
		]);
		await pollOnce(
			"A",
			new Map<string, WatchableRun>(),
			new AbortController().signal,
		);
		expect(streamAgentTurnToBus).toHaveBeenCalledTimes(1);
	});
});
