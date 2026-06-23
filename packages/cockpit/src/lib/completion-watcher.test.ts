// Run-completion watcher (DAT-528 run-routing + DAT-615 push-completion).
//
// Two surfaces, tested separately because completion is now fire-and-forget:
//   • `pollOnce` — the DISCOVERY tick: lists THIS conversation's in-flight runs
//     (conversation-scoped, DAT-528), spawns a `result()` awaiter per run, and pushes
//     the phase pills. It does NOT itself narrate (the awaiter does), so its tests
//     assert routing/discovery, not narration.
//   • `awaitCompletion` — the per-run completion path: `await handle.result()` (push,
//     DAT-615) → markRunStatus → narrate. Awaitable, so its tests assert the
//     narration contract (routing into its conversation, DAT-597 import-skip, failure).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => {
	// Stand-in for the SDK error `handle.result()` throws on a failed run — the module
	// under test does `err instanceof WorkflowFailedError`, so the mock exports THIS class.
	class WorkflowFailedError extends Error {}
	return {
		WorkflowFailedError,
		runForA: {
			workflowId: "wf-A",
			runId: "r-A",
			stage: "begin_session" as const,
			kind: "begin_session" as const,
		},
		narrated: [] as string[],
		markStatus: [] as Array<{
			workflowId: string;
			runId: string;
			status: string;
		}>,
		// handle.result() — resolves (completed) by default; a test can reject it.
		result: vi.fn(async () => ({})),
		getHandle: vi.fn(),
	};
});

vi.mock("@temporalio/client", () => ({
	WorkflowFailedError: h.WorkflowFailedError,
}));

vi.mock("#/db/cockpit/conversations", () => ({
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
	markRunStatus: vi.fn(
		async (workflowId: string, runId: string, status: string) => {
			h.markStatus.push({ workflowId, runId, status });
		},
	),
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
	// The completion-watcher awaits handle.result() via this client; getHandle returns
	// a handle whose result() is the controllable mock.
	getTemporalClient: vi.fn(async () => ({
		workflow: { getHandle: (...args: unknown[]) => h.getHandle(...args) },
	})),
	getWorkflowProgress: vi.fn(async () => ({
		done: true,
		status: "COMPLETED",
		failure: null,
	})),
}));

import { streamAgentTurnToBus } from "#/lib/agent-turn";
import { getTemporalClient, getWorkflowProgress } from "#/temporal/progress";
import { awaitCompletion, pollOnce } from "./completion-watcher";

beforeEach(() => {
	h.narrated = [];
	h.markStatus = [];
	vi.clearAllMocks();
	h.result.mockResolvedValue({});
	h.getHandle.mockReturnValue({ result: h.result });
});
afterEach(() => vi.restoreAllMocks());

const liveSignal = (): AbortSignal => new AbortController().signal;

describe("pollOnce — discovery + routing (DAT-528)", () => {
	it("conversation B's tick does NOT touch A's run (scoped by conversationId)", async () => {
		await pollOnce("B", new Set<string>(), liveSignal());
		// No run for B → no awaiter spawned (no client handle), no progress query.
		expect(getTemporalClient).not.toHaveBeenCalled();
		expect(getWorkflowProgress).not.toHaveBeenCalled();
	});

	it("conversation A's tick discovers A's run: pins the real id + spawns one awaiter", async () => {
		const tracked = new Set<string>();
		await pollOnce("A", tracked, liveSignal());
		// Pills query pins the exact (workflowId, runId) — the real id (DAT-595).
		expect(getWorkflowProgress).toHaveBeenCalledWith({
			workflow_id: "wf-A",
			run_id: "r-A",
		});
		// The run is now being awaited — a second tick must NOT spawn a 2nd awaiter.
		expect(tracked.has("wf-A:r-A")).toBe(true);
		const handleCalls = h.getHandle.mock.calls.length;
		await pollOnce("A", tracked, liveSignal());
		expect(h.getHandle.mock.calls.length).toBe(handleCalls); // no re-spawn
	});
});

describe("awaitCompletion — push completion via result() (DAT-615)", () => {
	it("awaits the run's result(), marks it completed, and narrates into ITS conversation", async () => {
		await awaitCompletion("A", h.runForA, liveSignal());
		// Pinned the exact execution + awaited result() (the push edge, no poll).
		expect(h.getHandle).toHaveBeenCalledWith("wf-A", "r-A");
		expect(h.result).toHaveBeenCalledTimes(1);
		// markRunStatus BEFORE narrate (once-only via the status filter).
		expect(h.markStatus).toEqual([
			{ workflowId: "wf-A", runId: "r-A", status: "completed" },
		]);
		expect(h.narrated).toEqual(["A"]); // routed into A
	});

	it("marks FAILED when result() throws WorkflowFailedError (still narrates)", async () => {
		h.result.mockRejectedValueOnce(new h.WorkflowFailedError("boom"));
		await awaitCompletion("A", h.runForA, liveSignal());
		expect(h.markStatus).toEqual([
			{ workflowId: "wf-A", runId: "r-A", status: "failed" },
		]);
		expect(h.narrated).toEqual(["A"]);
	});

	it("an INFRA error (not WorkflowFailedError) is swallowed — no mark, no narrate (re-discovered next tick)", async () => {
		h.result.mockRejectedValueOnce(new Error("temporal hiccup"));
		await awaitCompletion("A", h.runForA, liveSignal());
		expect(h.markStatus).toEqual([]);
		expect(streamAgentTurnToBus).not.toHaveBeenCalled();
	});

	it("does NOT narrate an onboarding add_source — the hub owns import progress (DAT-597)", async () => {
		await awaitCompletion(
			"A",
			{
				workflowId: "addsource-ws",
				runId: "r1",
				stage: "add_source",
				kind: "onboarding",
			},
			liveSignal(),
		);
		// Still marked terminal (the monitor/inbox need it), just not narrated into chat.
		expect(h.markStatus).toEqual([
			{ workflowId: "addsource-ws", runId: "r1", status: "completed" },
		]);
		expect(streamAgentTurnToBus).not.toHaveBeenCalled();
	});

	it("DOES narrate a replay add_source — the teach→re-ground verification (DAT-597)", async () => {
		await awaitCompletion(
			"A",
			{
				workflowId: "addsource-ws",
				runId: "r2",
				stage: "add_source",
				kind: "replay",
			},
			liveSignal(),
		);
		expect(h.narrated).toEqual(["A"]);
	});

	it("skips post-completion work once the stream has closed (signal aborted)", async () => {
		const ac = new AbortController();
		ac.abort();
		await awaitCompletion("A", h.runForA, ac.signal);
		expect(h.markStatus).toEqual([]);
		expect(streamAgentTurnToBus).not.toHaveBeenCalled();
	});
});
