// Unit coverage for the Workspace Briefing read-model's pure core (DAT-632): the
// `assembleBriefing` derivation, the `computeNextActions` ladder, and the per-chat
// `projectBriefing` split. The IO (`buildWorkspaceBriefing`) is smoke/integration-
// covered; here we pin the deterministic projection over fixture rows.

import { describe, expect, it, vi } from "vitest";

// The module imports the schema → client → #/config and the cockpit_db client
// (via registry/runs). The pure functions touch none of it; stub the boundary so
// the import graph loads without `postgres()`-at-import (the vitest hang trap).
vi.mock("#/config", () => ({ config: { dataraumWorkspaceId: "ws-test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
vi.mock("#/db/cockpit/client", () => ({ cockpitDb: {} }));

import type { AwaitingInputItem } from "#/db/cockpit/runs";
import {
	assembleBriefing,
	type BriefingArtifactRow,
	type BriefingInputs,
	type BriefingReadinessRow,
	type BriefingStageFlags,
	type BriefingTableRow,
	computeNextActions,
	projectBriefing,
	type WorkspaceBriefing,
} from "./workspace-briefing";

// ── Fixtures ────────────────────────────────────────────────────────────────

const ALL_EMPTY_FLAGS: BriefingStageFlags = {
	hasImportedTables: false,
	catalogPromoted: false,
	operatingModelPromoted: false,
	addSourceRunning: false,
	beginSessionRunning: false,
	operatingModelRunning: false,
};

function driver(label: string) {
	return {
		node: "n",
		dimension_path: "d",
		label,
		state: "s",
		impact_delta: 0.5,
	};
}

function readiness(
	over: Partial<BriefingReadinessRow> = {},
): BriefingReadinessRow {
	return {
		target: "column:t.c",
		tableId: "tbl-1",
		columnId: "col-1",
		band: "ready",
		worstIntentRisk: 0.1,
		topDrivers: [],
		...over,
	};
}

function inputs(over: Partial<BriefingInputs> = {}): BriefingInputs {
	return {
		workspace: { id: "ws-test", vertical: "finance" },
		tables: [],
		readiness: [],
		artifacts: [],
		pendingTeachCount: 0,
		awaitingInput: [],
		flags: ALL_EMPTY_FLAGS,
		...over,
	};
}

// ── assembleBriefing: inventory ─────────────────────────────────────────────

describe("assembleBriefing — inventory", () => {
	it("counts tables, columns, and the worst column band per table", () => {
		const tableRows: BriefingTableRow[] = [
			{
				tableId: "tbl-1",
				tableName: "orders",
				sourceName: null,
				columnCount: 3,
			},
			{
				tableId: "tbl-2",
				tableName: "items",
				sourceName: null,
				columnCount: 1,
			},
		];
		const b = assembleBriefing(
			inputs({
				tables: tableRows,
				readiness: [
					readiness({ tableId: "tbl-1", columnId: "c1", band: "investigate" }),
					readiness({ tableId: "tbl-1", columnId: "c2", band: "blocked" }),
					readiness({ tableId: "tbl-2", columnId: "c3", band: "ready" }),
				],
				flags: { ...ALL_EMPTY_FLAGS, hasImportedTables: true },
			}),
		);
		expect(b.inventory.tableCount).toBe(2);
		// Sorted by display name: "items" before "orders".
		expect(b.inventory.tables.map((t) => t.name)).toEqual(["items", "orders"]);
		const orders = b.inventory.tables.find((t) => t.name === "orders");
		expect(orders?.band).toBe("blocked"); // worst of investigate + blocked
		expect(orders?.columnCount).toBe(3);
	});

	it("leaves a table band null when readiness has not run", () => {
		const b = assembleBriefing(
			inputs({
				tables: [
					{
						tableId: "tbl-1",
						tableName: "orders",
						sourceName: null,
						columnCount: 2,
					},
				],
				flags: { ...ALL_EMPTY_FLAGS, hasImportedTables: true },
			}),
		);
		expect(b.inventory.tables[0].band).toBeNull();
	});
});

// ── assembleBriefing: attention ─────────────────────────────────────────────

describe("assembleBriefing — attention", () => {
	it("counts blocked/investigate columns and ranks blockers by risk with top driver", () => {
		const b = assembleBriefing(
			inputs({
				readiness: [
					readiness({
						target: "column:t.a",
						columnId: "a",
						band: "blocked",
						worstIntentRisk: 0.4,
						topDrivers: [driver("mixed units")],
					}),
					readiness({
						target: "column:t.b",
						columnId: "b",
						band: "blocked",
						worstIntentRisk: 0.9,
						topDrivers: [driver("opaque codes")],
					}),
					readiness({
						target: "column:t.c",
						columnId: "c",
						band: "investigate",
					}),
				],
			}),
		);
		expect(b.attention.columnsBlocked).toBe(2);
		expect(b.attention.columnsInvestigate).toBe(1);
		// Highest risk first.
		expect(b.attention.readinessBlockers.map((x) => x.target)).toEqual([
			"column:t.b",
			"column:t.a",
		]);
		expect(b.attention.readinessBlockers[0].topDriver).toBe("opaque codes");
	});

	it("counts a relationship-grain blocker but not as a blocked COLUMN", () => {
		const b = assembleBriefing(
			inputs({
				readiness: [
					readiness({
						target: "relationship:x::y",
						tableId: null,
						columnId: null,
						band: "blocked",
						worstIntentRisk: 0.7,
					}),
				],
			}),
		);
		expect(b.attention.columnsBlocked).toBe(0); // not a column
		expect(b.attention.readinessBlockers).toHaveLength(1); // still a blocker
		expect(b.attention.readinessBlockers[0].topDriver).toBeNull();
	});

	it("treats only not-executed artifacts WITH a reason as stuck", () => {
		const artifacts: BriefingArtifactRow[] = [
			{
				artifactType: "validation",
				artifactKey: "v1",
				state: "executed",
				stateReason: null,
			},
			{
				artifactType: "validation",
				artifactKey: "v2",
				state: "declared",
				stateReason: null,
			},
			{
				artifactType: "metric",
				artifactKey: "m1",
				state: "declared",
				stateReason: "Missing table: gl",
			},
			{
				artifactType: "business_cycle",
				artifactKey: "bc1",
				state: "grounded",
				stateReason: "exec error",
			},
		];
		const b = assembleBriefing(inputs({ artifacts }));
		// Sorted by type then key: business_cycle (bc1) before metric (m1).
		expect(b.attention.stuckArtifacts.map((s) => s.key)).toEqual(["bc1", "m1"]);
	});

	it("flags pending teaches as needing a replay", () => {
		const b = assembleBriefing(inputs({ pendingTeachCount: 3 }));
		expect(b.attention.pendingTeaches).toEqual({ count: 3, needsReplay: true });
		const none = assembleBriefing(inputs({ pendingTeachCount: 0 }));
		expect(none.attention.pendingTeaches.needsReplay).toBe(false);
	});

	it("passes through awaiting-input items with their note", () => {
		const awaiting: AwaitingInputItem[] = [
			{
				workflowId: "addsource-ws",
				stage: "add_source",
				awaitingNote: "Tell me what NULL looks like",
				startedAt: new Date(),
			},
		];
		const b = assembleBriefing(inputs({ awaitingInput: awaiting }));
		expect(b.attention.awaitingInput).toEqual([
			{
				workflowId: "addsource-ws",
				stage: "add_source",
				note: "Tell me what NULL looks like",
			},
		]);
	});
});

// ── assembleBriefing: progress (StageStatus) ────────────────────────────────

describe("assembleBriefing — progress", () => {
	it("is all-empty for a fresh workspace", () => {
		const b = assembleBriefing(inputs());
		expect(b.progress).toEqual({
			connect: "empty",
			stage: "empty",
			analyse: "empty",
		});
	});

	it("connect=ready, stage=empty once tables are imported but not staged", () => {
		const b = assembleBriefing(
			inputs({ flags: { ...ALL_EMPTY_FLAGS, hasImportedTables: true } }),
		);
		expect(b.progress.connect).toBe("ready");
		expect(b.progress.stage).toBe("empty");
	});

	it("stage=needs_attention when the catalog is promoted but columns are blocked", () => {
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
				},
				readiness: [readiness({ columnId: "c", band: "blocked" })],
			}),
		);
		expect(b.progress.stage).toBe("needs_attention");
	});

	it("analyse=ready when the operating model is promoted and nothing is blocked", () => {
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					operatingModelPromoted: true,
				},
			}),
		);
		expect(b.progress.stage).toBe("ready");
		expect(b.progress.analyse).toBe("ready");
	});

	it("in-flight runs win over promotion state", () => {
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					beginSessionRunning: true,
				},
			}),
		);
		expect(b.progress.stage).toBe("in_progress");
	});
});

// ── computeNextActions: the call-to-action ladder ───────────────────────────

describe("computeNextActions", () => {
	it("ranks awaiting-input above everything, routed to the stage's chat", () => {
		const b = assembleBriefing(
			inputs({
				pendingTeachCount: 2,
				awaitingInput: [
					{
						workflowId: "addsource-ws",
						stage: "add_source",
						awaitingNote: "needs you",
						startedAt: new Date(),
					},
				],
			}),
		);
		const first = b.nextActions[0];
		expect(first.kind).toBe("review_blocker");
		expect(first.priority).toBe(0);
		expect(first.targetChat).toBe("connect"); // add_source → connect
		expect(first.label).toBe("needs you");
	});

	it("emits a begin_session action once imported but unstaged", () => {
		const b = assembleBriefing(
			inputs({ flags: { ...ALL_EMPTY_FLAGS, hasImportedTables: true } }),
		);
		expect(b.nextActions.map((a) => a.kind)).toContain("begin_session");
	});

	it("emits a teach action for stuck operating-model artifacts", () => {
		const b = assembleBriefing(
			inputs({
				artifacts: [
					{
						artifactType: "metric",
						artifactKey: "m1",
						state: "declared",
						stateReason: "Missing table: gl",
					},
				],
			}),
		);
		const teach = b.nextActions.find((a) =>
			a.label.includes("operating-model"),
		);
		expect(teach?.kind).toBe("teach");
		expect(teach?.targetChat).toBe("stage");
	});

	it("emits an answer action when analyse is ready and unblocked", () => {
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					operatingModelPromoted: true,
				},
			}),
		);
		const answer = b.nextActions.find((a) => a.kind === "answer");
		expect(answer?.targetChat).toBe("analyse");
	});

	it("pluralizes labels by count", () => {
		const one = computeNextActions(
			{ connect: "ready", stage: "ready", analyse: "empty" },
			{
				columnsBlocked: 1,
				columnsInvestigate: 0,
				readinessBlockers: [],
				stuckArtifacts: [],
				pendingTeaches: { count: 1, needsReplay: true },
				awaitingInput: [],
			},
		);
		expect(one.find((a) => a.kind === "replay")?.label).toContain(
			"1 teach pending",
		);
		expect(one.find((a) => a.kind === "teach")?.label).toContain(
			"1 column blocked",
		);
	});
});

// ── projectBriefing: per-chat split ─────────────────────────────────────────

describe("projectBriefing", () => {
	// A briefing with a stage-targeted teach action AND an analyse-targeted answer.
	function mixedBriefing(): WorkspaceBriefing {
		return assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					operatingModelPromoted: true,
				},
				readiness: [readiness({ columnId: "c", band: "blocked" })],
			}),
		);
	}

	it("a Connect chat foregrounds none of the stage-owned teach/grounding actions", () => {
		const p = projectBriefing(mixedBriefing(), "connect");
		expect(p.foreground).toHaveLength(0);
		// They surface as a background pointer to Stage instead.
		expect(p.background.map((x) => x.chat)).toContain("stage");
	});

	it("a Connect chat foregrounds an add_source review_blocker waiting on it", () => {
		const b = assembleBriefing(
			inputs({
				awaitingInput: [
					{
						workflowId: "addsource-ws",
						stage: "add_source",
						awaitingNote: "needs you",
						startedAt: new Date(),
					},
				],
			}),
		);
		const p = projectBriefing(b, "connect");
		expect(p.foreground.map((a) => a.kind)).toEqual(["review_blocker"]);
	});

	it("an Analyse chat foregrounds only analyse-owned actions, not typing/teach", () => {
		const b = mixedBriefing();
		const p = projectBriefing(b, "analyse");
		expect(p.foreground.every((a) => a.targetChat === "analyse")).toBe(true);
		// Stage-owned teach actions are demoted to background, not in foreground.
		expect(p.foreground.some((a) => a.kind === "teach")).toBe(false);
	});

	it("a Stage chat foregrounds its own actions and points at other chats' top action", () => {
		const b = mixedBriefing();
		const p = projectBriefing(b, "stage");
		expect(p.foreground.some((a) => a.kind === "teach")).toBe(true);
		// background carries one pointer per OTHER kind that has an action.
		for (const ptr of p.background) {
			expect(ptr.chat).not.toBe("stage");
			expect(ptr.label.length).toBeGreaterThan(0);
		}
	});
});
