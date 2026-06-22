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

const captured: { cond?: unknown } = {};

vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: () => ({
				where: (cond: unknown) => {
					captured.cond = cond;
					return Promise.resolve([]);
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
		const rows = await readGroundingReadiness([]);
		expect(rows).toEqual([]);
		expect(captured.cond).toBeUndefined();
	});
});
