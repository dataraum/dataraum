// Unit coverage for the pure per-chat projection (DAT-632).

import { describe, expect, it } from "vitest";

import { projectBriefing } from "./project";
import type { BriefingAction, WorkspaceBriefing } from "./types";

function briefingWith(nextActions: BriefingAction[]): WorkspaceBriefing {
	return {
		workspace: { id: "ws", vertical: null },
		inventory: {
			sourceCount: 0,
			tableCount: 0,
			bandCounts: { ready: 0, investigate: 0, blocked: 0, unknown: 0 },
			tables: [],
		},
		progress: { connect: "ready", stage: "ready", analyse: "ready" },
		attention: {
			columnsBlocked: 0,
			columnsInvestigate: 0,
			readinessBlockers: [],
			stuckArtifacts: { total: 0, byType: [] },
			pendingTeaches: { count: 0, needsReplay: false },
			awaitingInput: [],
		},
		nextActions,
	};
}

// A stage-owned teach (priority 2) and an analyse-owned answer (priority 4).
const MIXED: BriefingAction[] = [
	{
		kind: "teach",
		label: "2 columns blocked — teach to unblock",
		targetChat: "stage",
		priority: 2,
	},
	{
		kind: "answer",
		label: "Ready to answer questions",
		targetChat: "analyse",
		priority: 4,
	},
];

describe("projectBriefing", () => {
	it("a Connect chat foregrounds none of the stage/analyse actions, points at them", () => {
		const p = projectBriefing(briefingWith(MIXED), "connect");
		expect(p.foreground).toHaveLength(0);
		expect(p.background.map((x) => x.chat).sort()).toEqual([
			"analyse",
			"stage",
		]);
	});

	it("a Stage chat foregrounds its own teach, points only at the other chats", () => {
		const p = projectBriefing(briefingWith(MIXED), "stage");
		expect(p.foreground.map((a) => a.kind)).toEqual(["teach"]);
		for (const ptr of p.background) {
			expect(ptr.chat).not.toBe("stage");
			expect(ptr.label.length).toBeGreaterThan(0);
		}
	});

	it("an Analyse chat foregrounds only analyse-owned actions", () => {
		const p = projectBriefing(briefingWith(MIXED), "analyse");
		expect(p.foreground.every((a) => a.targetChat === "analyse")).toBe(true);
		expect(p.foreground.some((a) => a.kind === "teach")).toBe(false);
	});

	it("background pointer uses the OTHER kind's top (lowest-priority) action label", () => {
		const p = projectBriefing(briefingWith(MIXED), "connect");
		const stagePtr = p.background.find((x) => x.chat === "stage");
		expect(stagePtr?.label).toBe("2 columns blocked — teach to unblock");
	});
});
