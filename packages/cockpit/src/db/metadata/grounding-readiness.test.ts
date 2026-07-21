// @vitest-environment node
//
// DAT-597 Bug 1: the grounding loop must read the add_source GRAIN of the
// multi-grain `current_entropy_readiness` view (the row sealed by the per-table
// generation head), not a stale catalog-grain row a replay-after-session would
// also surface. We mock the metadata client to CAPTURE the WHERE clause and render
// it with the real pg dialect, asserting the `via_table_head` pin is present — a
// regression guard that a future edit can't silently drop the grain filter.

import { PgDialect } from "drizzle-orm/pg-core";
import { describe, expect, it, vi } from "vitest";

const captured: { cond?: unknown; rows: unknown[] } = { rows: [] };

vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: () => ({
				where: (cond: unknown) => {
					captured.cond = cond;
					return Promise.resolve(captured.rows);
				},
			}),
		}),
	},
}));

import { readGroundingReadiness } from "./grounding-readiness";

describe("readGroundingReadiness — add_source grain pin (DAT-597)", () => {
	it("filters on via_table_head (the add_source generation grain)", async () => {
		await readGroundingReadiness(["t1", "t2"]);
		expect(captured.cond).toBeDefined();
		const { sql } = new PgDialect().sqlToQuery(captured.cond as never);
		expect(sql).toContain("via_table_head");
	});

	it("short-circuits to [] with no query when there are no tables", async () => {
		captured.cond = undefined;
		captured.rows = [];
		const rows = await readGroundingReadiness([]);
		expect(rows).toEqual([]);
		expect(captured.cond).toBeUndefined();
	});

	it("carries coverage + the abstention trace so the loop can flag unmeasured (DAT-853)", async () => {
		captured.rows = [
			{
				target: "column:payments.amount",
				tableId: "t1",
				columnId: "c1",
				band: "ready",
				worstIntentRisk: 0,
				coverage: "unmeasured",
				abstentions: [
					{
						detector: "unit_entropy",
						reason: "insufficient_data",
						intents: ["aggregation_intent"],
					},
				],
				topDrivers: [],
			},
		];
		const [g] = await readGroundingReadiness(["t1"]);
		expect(g.band).toBe("ready");
		expect(g.coverage).toBe("unmeasured");
		expect(g.abstentions).toEqual([
			{
				detector: "unit_entropy",
				reason: "insufficient_data",
				intents: ["aggregation_intent"],
			},
		]);
	});

	it("fails CLOSED on an (unreachable) null coverage — biases to 'unmeasured'", async () => {
		// coverage is NOT NULL underneath, so this never fires; but if it ever did,
		// defaulting to 'unmeasured' keeps the gap filter from exiting green on an
		// unmeasured target (the epic's core failure), at worst one wasted LLM look.
		captured.rows = [
			{
				target: "column:payments.amount",
				tableId: "t1",
				columnId: "c1",
				band: "ready",
				worstIntentRisk: 0,
				coverage: null,
				abstentions: null,
				topDrivers: [],
			},
		];
		const [g] = await readGroundingReadiness(["t1"]);
		expect(g.coverage).toBe("unmeasured");
	});
});
