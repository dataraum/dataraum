// Unit coverage for the pure call-to-action ladder (DAT-632).

import { describe, expect, it } from "vitest";

import { computeNextActions, stageToChat } from "./next-actions";
import type { BriefingAttention, BriefingProgress } from "./types";

const NO_ATTENTION: BriefingAttention = {
	columnsBlocked: 0,
	columnsInvestigate: 0,
	readinessBlockers: [],
	stuckArtifacts: { total: 0, byType: [] },
	pendingTeaches: { count: 0, needsReplay: false },
	awaitingInput: [],
};

function attention(over: Partial<BriefingAttention> = {}): BriefingAttention {
	return { ...NO_ATTENTION, ...over };
}

const PROGRESS: BriefingProgress = {
	connect: "ready",
	stage: "ready",
	analyse: "empty",
};

describe("stageToChat", () => {
	it("routes add_source to connect, the rest to stage", () => {
		expect(stageToChat("add_source")).toBe("connect");
		expect(stageToChat("begin_session")).toBe("stage");
		expect(stageToChat("operating_model")).toBe("stage");
	});
});

describe("computeNextActions", () => {
	it("ranks awaiting-input above everything, routed to the stage's chat", () => {
		const actions = computeNextActions(
			PROGRESS,
			attention({
				pendingTeaches: { count: 2, needsReplay: true },
				awaitingInput: [
					{ workflowId: "w", stage: "add_source", note: "needs you" },
				],
			}),
		);
		expect(actions[0]).toMatchObject({
			kind: "review_blocker",
			priority: 0,
			targetChat: "connect",
			label: "needs you",
		});
	});

	it("emits a begin_session action once imported but unstaged", () => {
		const actions = computeNextActions(
			{ connect: "ready", stage: "empty", analyse: "empty" },
			NO_ATTENTION,
		);
		expect(actions.map((a) => a.kind)).toContain("begin_session");
	});

	it("emits a teach action for stuck operating-model artifacts", () => {
		const actions = computeNextActions(
			PROGRESS,
			attention({
				stuckArtifacts: { total: 4, byType: [{ type: "metric", count: 4 }] },
			}),
		);
		const teach = actions.find((a) => a.label.includes("operating-model"));
		expect(teach).toMatchObject({ kind: "teach", targetChat: "stage" });
	});

	it("emits the honest declare nudge for nothing_declared, not the run-OM loop (DAT-845)", () => {
		const actions = computeNextActions(
			{ connect: "ready", stage: "ready", analyse: "nothing_declared" },
			NO_ATTENTION,
		);
		const declare = actions.find((a) => a.kind === "declare");
		expect(declare).toMatchObject({ targetChat: "stage", priority: 3 });
		expect(declare?.label).toContain("no validations, cycles, or metrics");
		// Never the "run the operating model" nudge (that would loop) …
		expect(actions.map((a) => a.kind)).not.toContain("operating_model");
		// … and never "Ready to answer questions" (analyse isn't ready).
		expect(actions.map((a) => a.kind)).not.toContain("answer");
	});

	it("emits an answer action when analyse is ready and unblocked", () => {
		const actions = computeNextActions(
			{ connect: "ready", stage: "ready", analyse: "ready" },
			NO_ATTENTION,
		);
		expect(actions.find((a) => a.kind === "answer")?.targetChat).toBe(
			"analyse",
		);
	});

	it("pluralizes labels by count", () => {
		const actions = computeNextActions(
			PROGRESS,
			attention({
				columnsBlocked: 1,
				pendingTeaches: { count: 1, needsReplay: true },
			}),
		);
		expect(actions.find((a) => a.kind === "replay")?.label).toContain(
			"1 teach pending",
		);
		expect(actions.find((a) => a.kind === "teach")?.label).toContain(
			"1 column blocked",
		);
	});

	it("returns actions sorted by priority", () => {
		const actions = computeNextActions(
			{ connect: "ready", stage: "empty", analyse: "empty" },
			attention({
				pendingTeaches: { count: 1, needsReplay: true },
				columnsBlocked: 2,
			}),
		);
		const priorities = actions.map((a) => a.priority);
		expect(priorities).toEqual([...priorities].sort((a, b) => a - b));
	});
});
