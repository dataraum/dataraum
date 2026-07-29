// Unit coverage for the per-turn agent digest (DAT-634) — pure. The key property:
// the digest is PROJECTED, so a Connect chat isn't told to ground metrics and an
// Analyse chat isn't told to fix typing — those are stage-owned actions.

import { describe, expect, it } from "vitest";

import { formatBriefingDigest } from "./digest";
import type { WorkspaceBriefing } from "./types";

// A briefing mid-pipeline: blocked columns (→ a stage "teach to unblock" action),
// stuck metrics (→ a stage "teach to fix" action), and analyse ready (→ an analyse
// "answer" action). nextActions are pre-ranked as assembleBriefing would emit them.
function briefing(): WorkspaceBriefing {
	return {
		workspace: { id: "ws", vertical: "finance" },
		inventory: {
			sourceCount: 1,
			tableCount: 5,
			bandCounts: { ready: 2, investigate: 1, blocked: 2, unknown: 0 },
			tables: [],
		},
		progress: { connect: "ready", stage: "needs_attention", analyse: "ready" },
		attention: {
			columnsBlocked: 3,
			columnsInvestigate: 1,
			readinessBlockers: [
				{
					target: "column:src_a__orders.amount",
					source: "src_a",
					label: "orders.amount",
					band: "blocked",
					topDriver: "Unit entropy",
				},
			],
			stuckArtifacts: { total: 8, byType: [{ type: "metric", count: 8 }] },
			pendingTeaches: { count: 0, needsReplay: false },
			awaitingInput: [],
		},
		nextActions: [
			{
				kind: "teach",
				label: "3 columns blocked — teach to unblock",
				targetChat: "stage",
				priority: 2,
			},
			{
				kind: "teach",
				label: "8 operating-model items need grounding — teach to fix",
				targetChat: "stage",
				priority: 2,
			},
			{
				kind: "answer",
				label: "Ready to answer questions",
				targetChat: "analyse",
				priority: 4,
			},
		],
	};
}

describe("formatBriefingDigest", () => {
	it("states the readiness facts so the agent needs no tool call", () => {
		const d = formatBriefingDigest(briefing(), "stage");
		expect(d).toContain("3 columns blocked");
		expect(d).toContain("8 operating-model items need grounding");
		expect(d).toContain("orders.amount"); // a named blocker
	});

	it("a Stage chat foregrounds the teach actions (it owns them)", () => {
		const d = formatBriefingDigest(briefing(), "stage") ?? "";
		const suggested = d.slice(d.indexOf("Suggested next here:"));
		expect(suggested).toContain("teach to unblock");
		expect(suggested).toContain("teach to fix");
	});

	it("an Analyse chat does NOT suggest typing/grounding (stage-owned) — only answer", () => {
		const d = formatBriefingDigest(briefing(), "analyse") ?? "";
		const suggested = d.slice(
			d.indexOf("Suggested next here:"),
			d.indexOf("Elsewhere:") >= 0 ? d.indexOf("Elsewhere:") : undefined,
		);
		expect(suggested).toContain("Ready to answer"); // analyse-owned
		expect(suggested).not.toContain("teach to unblock"); // typing — stage's job
		expect(suggested).not.toContain("teach to fix"); // grounding — stage's job
		// They surface only as an "elsewhere" pointer.
		expect(d).toContain("Elsewhere:");
		expect(d.slice(d.indexOf("Elsewhere:"))).toContain("stage");
	});

	it("a Connect chat does NOT suggest metric-grounding (stage-owned)", () => {
		const d = formatBriefingDigest(briefing(), "connect") ?? "";
		const suggested =
			d.indexOf("Suggested next here:") >= 0
				? d.slice(
						d.indexOf("Suggested next here:"),
						d.indexOf("Elsewhere:") >= 0 ? d.indexOf("Elsewhere:") : undefined,
					)
				: "";
		expect(suggested).not.toContain("teach to fix"); // metric grounding — stage's
	});

	// DAT-671 R5 — the digest names at most DIGEST_BLOCKER_CAP blockers, and the
	// model has no other way to learn there are more: `columnsBlocked` counts
	// COLUMNS, while readinessBlockers also carries relationship and table
	// targets, so the two numbers are not interchangeable. An unmarked list of
	// five reads as the whole set.
	it("says how many blockers it is NOT naming", () => {
		const many: WorkspaceBriefing = {
			...briefing(),
			attention: {
				...briefing().attention,
				readinessBlockers: Array.from({ length: 12 }, (_, i) => ({
					target: `column:src_a__orders.c${i}`,
					source: "src_a",
					label: `orders.c${i}`,
					band: "blocked" as const,
					topDriver: "Unit entropy",
				})),
			},
		};
		const digest = formatBriefingDigest(many, "connect");
		expect(digest).toContain("(showing 5 of 12)");
		// The five it DID name are still named — the disclosure adds, never
		// replaces.
		expect(digest).toContain("orders.c0");
	});

	it("stays silent about a cut it did not make", () => {
		// Exactly one blocker, all of it shown: a "showing 1 of 1" would be noise
		// that trains the reader to ignore the note when it matters.
		expect(formatBriefingDigest(briefing(), "connect")).not.toContain(
			"showing",
		);
	});

	it("returns null when there's nothing notable and nothing to do", () => {
		const calm: WorkspaceBriefing = {
			...briefing(),
			progress: { connect: "ready", stage: "ready", analyse: "ready" },
			attention: {
				columnsBlocked: 0,
				columnsInvestigate: 0,
				readinessBlockers: [],
				stuckArtifacts: { total: 0, byType: [] },
				pendingTeaches: { count: 0, needsReplay: false },
				awaitingInput: [],
			},
			nextActions: [],
		};
		expect(formatBriefingDigest(calm, "connect")).toBeNull();
	});
});
