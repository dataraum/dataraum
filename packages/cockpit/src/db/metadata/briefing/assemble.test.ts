// Unit coverage for the pure briefing assembly (DAT-632): inventory (source-
// qualified, no collapse), attention (all blockers, no cap; stuck-by-type), and
// progress. No DB — `assemble` imports only zod + the pure ladder.

import { describe, expect, it } from "vitest";

import { assembleBriefing } from "./assemble";
import type {
	BriefingArtifactRow,
	BriefingInputs,
	BriefingReadinessRow,
	BriefingStageFlags,
	BriefingTableMeta,
} from "./types";

const ALL_EMPTY_FLAGS: BriefingStageFlags = {
	hasImportedTables: false,
	catalogPromoted: false,
	operatingModelPromoted: false,
	addSourceRunning: false,
	beginSessionRunning: false,
	operatingModelRunning: false,
	operatingModelNothingDeclared: false,
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

function meta(source: string, name: string): BriefingTableMeta {
	return { source, name };
}

function inputs(over: Partial<BriefingInputs> = {}): BriefingInputs {
	return {
		workspace: { id: "ws-test", vertical: "finance" },
		readiness: [],
		artifacts: [],
		tableMetaById: {},
		pendingTeachCount: 0,
		awaitingInput: [],
		flags: ALL_EMPTY_FLAGS,
		...over,
	};
}

describe("assembleBriefing — inventory (source-qualified)", () => {
	it("keeps same-named tables from different sources distinct (never collapses)", () => {
		const b = assembleBriefing(
			inputs({
				// Two sources, each with a table whose bare name is "orders".
				tableMetaById: {
					"s1-orders": meta("src_a", "orders"),
					"s2-orders": meta("src_b", "orders"),
				},
				readiness: [
					readiness({ tableId: "s1-orders", columnId: "c1", band: "blocked" }),
					readiness({ tableId: "s2-orders", columnId: "c2", band: "ready" }),
				],
				flags: { ...ALL_EMPTY_FLAGS, hasImportedTables: true },
			}),
		);
		expect(b.inventory.sourceCount).toBe(2);
		expect(b.inventory.tableCount).toBe(2);
		expect(b.inventory.bandCounts).toEqual({
			ready: 1,
			investigate: 0,
			blocked: 1,
			unknown: 0,
		});
		// Both kept, each carrying its source — the bare name "orders" is not merged.
		expect(b.inventory.tables).toEqual([
			{
				tableId: "s1-orders",
				source: "src_a",
				name: "orders",
				band: "blocked",
			},
			{ tableId: "s2-orders", source: "src_b", name: "orders", band: "ready" },
		]);
	});

	it("takes the WORST column band per table and buckets null as unknown", () => {
		const b = assembleBriefing(
			inputs({
				tableMetaById: {
					t: meta("src_a", "orders"),
					u: meta("src_a", "items"),
				},
				readiness: [
					readiness({ tableId: "t", columnId: "c1", band: "investigate" }),
					readiness({ tableId: "t", columnId: "c2", band: "blocked" }),
					readiness({ tableId: "u", columnId: "c3", band: null }),
				],
			}),
		);
		const orders = b.inventory.tables.find((x) => x.name === "orders");
		expect(orders?.band).toBe("blocked");
		expect(b.inventory.bandCounts.unknown).toBe(1);
		// The null-band table still appears in the inventory, carrying band null.
		expect(b.inventory.tables.find((x) => x.name === "items")?.band).toBeNull();
	});

	it("sorts by source, then worst band, then name", () => {
		const b = assembleBriefing(
			inputs({
				tableMetaById: {
					a: meta("src_a", "zeta"),
					b: meta("src_a", "alpha"),
					c: meta("src_b", "gamma"),
				},
				readiness: [
					readiness({ tableId: "a", columnId: "a1", band: "ready" }),
					readiness({ tableId: "b", columnId: "b1", band: "blocked" }),
					readiness({ tableId: "c", columnId: "c1", band: "investigate" }),
				],
			}),
		);
		expect(b.inventory.tables.map((t) => `${t.source}/${t.name}`)).toEqual([
			"src_a/alpha", // src_a, blocked
			"src_a/zeta", // src_a, ready
			"src_b/gamma", // src_b
		]);
	});
});

describe("assembleBriefing — attention", () => {
	it("returns ALL blocked columns (no cap), worst risk first, source-qualified", () => {
		// 51 — past any plausible "small default" cap a regression might add.
		const rows: BriefingReadinessRow[] = Array.from({ length: 51 }, (_, i) =>
			readiness({
				target: `column:src_a__orders.col${i}`,
				tableId: "t",
				columnId: `c${i}`,
				band: "blocked",
				worstIntentRisk: i / 100,
				topDrivers: [driver("Unit entropy")],
			}),
		);
		const b = assembleBriefing(
			inputs({
				tableMetaById: { t: meta("src_a", "orders") },
				readiness: rows,
			}),
		);
		// All 51 kept — not capped.
		expect(b.attention.readinessBlockers).toHaveLength(51);
		// Highest risk first; label de-prefixed; source carried.
		expect(b.attention.readinessBlockers[0].label).toBe("orders.col50");
		expect(b.attention.readinessBlockers[0].source).toBe("src_a");
		expect(b.attention.readinessBlockers[0].target).toBe(
			"column:src_a__orders.col50",
		);
		expect(b.attention.readinessBlockers[0].topDriver).toBe("Unit entropy");
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
		expect(b.attention.columnsBlocked).toBe(0);
		expect(b.attention.readinessBlockers).toHaveLength(1);
		expect(b.attention.readinessBlockers[0].label).toBe("relationship");
		expect(b.attention.readinessBlockers[0].source).toBe("");
	});

	it("summarizes stuck artifacts by type (uncapped); excludes executed / no-reason", () => {
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
				stateReason: "Missing",
			},
			{
				artifactType: "metric",
				artifactKey: "m2",
				state: "grounded",
				stateReason: "exec error",
			},
			{
				artifactType: "business_cycle",
				artifactKey: "bc1",
				state: "declared",
				stateReason: "not detected",
			},
		];
		const b = assembleBriefing(inputs({ artifacts }));
		expect(b.attention.stuckArtifacts.total).toBe(3);
		expect(b.attention.stuckArtifacts.byType).toEqual([
			{ type: "business_cycle", count: 1 },
			{ type: "metric", count: 2 },
		]);
	});

	it("flags pending teaches as needing a replay", () => {
		expect(
			assembleBriefing(inputs({ pendingTeachCount: 3 })).attention
				.pendingTeaches,
		).toEqual({
			count: 3,
			needsReplay: true,
		});
		expect(
			assembleBriefing(inputs({ pendingTeachCount: 0 })).attention
				.pendingTeaches.needsReplay,
		).toBe(false);
	});

	it("passes through awaiting-input items with their note", () => {
		const b = assembleBriefing(
			inputs({
				awaitingInput: [
					{
						workflowId: "addsource-ws",
						stage: "add_source",
						awaitingNote: "needs you",
					},
				],
			}),
		);
		expect(b.attention.awaitingInput).toEqual([
			{ workflowId: "addsource-ws", stage: "add_source", note: "needs you" },
		]);
	});
});

describe("assembleBriefing — progress", () => {
	it("is all-empty for a fresh workspace", () => {
		expect(assembleBriefing(inputs()).progress).toEqual({
			connect: "empty",
			stage: "empty",
			analyse: "empty",
		});
	});

	it("connect=ready, stage=empty once imported but unstaged", () => {
		const b = assembleBriefing(
			inputs({ flags: { ...ALL_EMPTY_FLAGS, hasImportedTables: true } }),
		);
		expect(b.progress.connect).toBe("ready");
		expect(b.progress.stage).toBe("empty");
	});

	it("stage=needs_attention when catalog is promoted but a column is blocked", () => {
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

	it("analyse=ready when operating model promoted and nothing blocked", () => {
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
		expect(b.progress).toMatchObject({ stage: "ready", analyse: "ready" });
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

	it("analyse=nothing_declared when the OM run completed but declared nothing (DAT-845)", () => {
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					// The run COMPLETED without flipping the head → not promoted, not empty.
					operatingModelPromoted: false,
					operatingModelNothingDeclared: true,
				},
			}),
		);
		expect(b.progress.analyse).toBe("nothing_declared");
		// The honest nudge is emitted; the "run the operating model" loop is NOT, and
		// "Ready to answer" is NOT (analyse isn't ready).
		const kinds = b.nextActions.map((a) => a.kind);
		expect(kinds).toContain("declare");
		expect(kinds).not.toContain("operating_model");
		expect(kinds).not.toContain("answer");
	});

	it("a promoted head still wins over a stale nothing_declared flag", () => {
		// Belt-and-braces: if both are somehow set, head-presence (a real operating
		// model exists) takes precedence — analyse reads ready, never nothing_declared.
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					operatingModelPromoted: true,
					operatingModelNothingDeclared: true,
				},
			}),
		);
		expect(b.progress.analyse).toBe("ready");
	});

	it("an in-flight OM run wins over a prior nothing_declared", () => {
		const b = assembleBriefing(
			inputs({
				flags: {
					...ALL_EMPTY_FLAGS,
					hasImportedTables: true,
					catalogPromoted: true,
					operatingModelRunning: true,
					operatingModelNothingDeclared: true,
				},
			}),
		);
		expect(b.progress.analyse).toBe("in_progress");
	});
});
