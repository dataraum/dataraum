// Tier-A axis derivation against the REAL catalog, on real result-column
// shapes.
//
// drill-axes-adhoc.test.ts covers the pure fold with hand-built rows. What it
// cannot cover — and what actually broke — is the join to a real catalog:
// tier A matches a result's OWN column names against catalogued dimension
// names, so the answer depends entirely on the spellings the engine wrote.
//
// The live failure this pins (smoke discovery 2): the model authors an alias
// like `account_name`, while the enrichment catalogues `account_id__name`.
// tier A then reports "no axes" on most grouped answers — not because the
// data lacks a dimension, but because the projection renamed it. There is no
// type error anywhere in that path; only a real catalog shows it.
//
// Reads go through the head-joined read views, so this also pins that the
// promoted-head gating is wired: drop the head and every axis silently
// vanishes.
//
// SCOPE — this is NOT end-to-end. `resolveAdHocDrillAxes` takes
// `resultColumns` as an argument; in production those come from the route's
// live `DESCRIBE` of the base statement (routes/api/drill/axes.ts →
// describeColumns). Here they are hand-written, so what is covered is the
// catalog join and the refusal wording, NOT the DESCRIBE that produces the
// names. A DESCRIBE that renamed or re-cased columns would not be caught by
// this suite — that seam needs the lake, which this harness deliberately
// leaves out.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { ACCOUNT_NAME_COLUMN, REGION_NAME_COLUMN } from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(
	fx.describeName("tier-A axes against the real catalog (DAT-671)"),
	() => {
		let resolveAdHocDrillAxes: typeof import("./drill-axes-adhoc").resolveAdHocDrillAxes;

		beforeAll(async () => {
			// Dynamic import: config parses at module eval and must see the
			// fixture DSNs that attachFixtureWorkspace() installed above.
			({ resolveAdHocDrillAxes } = await import("./drill-axes-adhoc"));
		});

		it("derives axes when the result projects catalogued dimension spellings", async () => {
			const result = await resolveAdHocDrillAxes([
				REGION_NAME_COLUMN,
				ACCOUNT_NAME_COLUMN,
				"total_amount",
			]);

			const columns = result.axes.map((a) => a.column);
			expect(columns).toContain(REGION_NAME_COLUMN);
			expect(columns).toContain(ACCOUNT_NAME_COLUMN);
			expect(result.reason).toBeUndefined();

			// Ranking is catalogued, not invented: `primary` outranks `supporting`.
			expect(columns.indexOf(REGION_NAME_COLUMN)).toBeLessThan(
				columns.indexOf(ACCOUNT_NAME_COLUMN),
			);

			// Catalogued values ride along so the drill menu can show real counts.
			const region = result.axes.find((a) => a.column === REGION_NAME_COLUMN);
			expect(region?.values).toEqual(["EU", "US"]);
			expect(region?.valueCount).toBe(2);
			expect(region?.businessContext).toBe("Sales region");
		});

		it("loses the axis when the projection ALIASES the dimension away", async () => {
			// THE LIVE BUG, pinned. `SELECT region_id__name AS region` is ordinary
			// SQL and looks completely fine; tier A simply cannot match `region`
			// to any catalogued name, so the drill menu comes back empty.
			const result = await resolveAdHocDrillAxes([
				"region",
				"account_name",
				"total_amount",
			]);

			expect(result.axes).toEqual([]);
			// The refusal must EXPLAIN itself — an empty menu with no reason is
			// how this shipped silently in the first place.
			expect(result.reason).toMatch(/catalogued dimension/i);
		});

		it("matches case-insensitively but answers in the RESULT's spelling", async () => {
			// The tier-A drill quotes the column against the result, so the axis
			// must carry the spelling the result actually has, not the catalog's.
			const shouted = REGION_NAME_COLUMN.toUpperCase();
			const result = await resolveAdHocDrillAxes([shouted, "total_amount"]);

			const region = result.axes.find(
				(a) => a.column.toLowerCase() === REGION_NAME_COLUMN,
			);
			expect(region).toBeDefined();
			expect(region?.column).toBe(shouted);
		});

		it("offers only catalogued DIMENSIONS, not other catalogued columns", async () => {
			// `amount` is a real column of the catalogued fact table — it is in
			// `columns` with origin 'fact' — but it is NOT a slice_definition, so
			// it must not become an axis. This is the exclusion that matters:
			// asserting a bare `sum(amount)` is absent would pass trivially,
			// since that string appears in neither the slice rows nor the
			// substrate list and could never have been offered.
			const result = await resolveAdHocDrillAxes([
				REGION_NAME_COLUMN,
				"amount",
				"sum(amount)",
			]);
			const columns = result.axes.map((a) => a.column);
			expect(columns).toContain(REGION_NAME_COLUMN);
			expect(columns).not.toContain("amount");
			expect(columns).not.toContain("sum(amount)");
		});

		it("refuses with a reason when the result has no columns", async () => {
			const result = await resolveAdHocDrillAxes([]);
			expect(result.axes).toEqual([]);
			expect(result.reason).toBeTruthy();
		});

		it("never buckets time on tier A", async () => {
			// Tier A wraps the result in a GROUP BY on its own columns; a temporal
			// bucket would need a grain the result does not carry.
			const result = await resolveAdHocDrillAxes([
				REGION_NAME_COLUMN,
				"total_amount",
			]);
			for (const axis of result.axes) expect(axis.temporal).toBeNull();
		});

		// DAT-671, "we should not slice on already existing slices": a result
		// whose own SQL already groups by one of its catalogued dimensions offers
		// that axis GREYED (disabledReason set), not absent — the menu never
		// empties from this rule, and other catalogued axes on the same result
		// stay fully enabled.
		it("greys the axis a result's own GROUP BY already breaks out by, keeping others enabled", async () => {
			const resultSql =
				`SELECT ${REGION_NAME_COLUMN}, SUM(total_amount) AS total_amount ` +
				"FROM lake.typed.current_orders_enriched " +
				`GROUP BY ${REGION_NAME_COLUMN}`;
			const result = await resolveAdHocDrillAxes(
				[REGION_NAME_COLUMN, ACCOUNT_NAME_COLUMN, "total_amount"],
				resultSql,
			);

			const region = result.axes.find((a) => a.column === REGION_NAME_COLUMN);
			const account = result.axes.find((a) => a.column === ACCOUNT_NAME_COLUMN);
			expect(region?.disabledReason).toMatch(/already at this grain/i);
			// The item stays IN THE MENU — never removed.
			expect(result.axes.map((a) => a.column)).toContain(REGION_NAME_COLUMN);
			expect(account?.disabledReason).toBeNull();
		});

		it("greys nothing when resultSql is absent — the determination never runs without it", async () => {
			const result = await resolveAdHocDrillAxes([
				REGION_NAME_COLUMN,
				ACCOUNT_NAME_COLUMN,
				"total_amount",
			]);
			for (const axis of result.axes) expect(axis.disabledReason).toBeNull();
		});

		it("greys nothing on an UNGROUPED result — raw detail hasn't been sliced yet", async () => {
			const resultSql =
				`SELECT ${REGION_NAME_COLUMN}, ${ACCOUNT_NAME_COLUMN}, total_amount ` +
				"FROM lake.typed.current_orders_enriched";
			const result = await resolveAdHocDrillAxes(
				[REGION_NAME_COLUMN, ACCOUNT_NAME_COLUMN, "total_amount"],
				resultSql,
			);
			for (const axis of result.axes) expect(axis.disabledReason).toBeNull();
		});
	},
);
