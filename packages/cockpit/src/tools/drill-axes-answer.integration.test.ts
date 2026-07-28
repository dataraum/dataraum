// The ALIAS GAP and its designed cure, on one answer (DAT-671).
//
// The same answer, resolved for slicing two ways:
//
//   TIER A     matches the RESULT's own column names against catalogued
//              dimension names. A projection that aliases the dimension —
//              `region_id__name AS region`, which is ordinary SQL and looks
//              completely fine — leaves nothing to match, so a result that
//              visibly HAS a dimension in it reports "no axes".
//
//   AT SOURCE  resolves from the declared RELATION instead, then asks the
//              catalog what that relation exposes. The projection's spelling
//              never enters into it, so the alias cannot break it.
//
// That is why parts-at-source is the answer to the alias gap rather than a
// spelling-normalizer or an alias map: the second path does not have the
// problem, so nothing has to be taught to work around it. This suite pins the
// contrast — if a later change made tier A the fallback for a scalar answer,
// or broke the relation-grounded path, exactly one of these two assertions
// would flip and say which.
//
// It ALSO pins the shape that used to make the cure unreachable: the declared
// value expression is CASE-guarded (the house empty-aggregation rule), which
// is the normal form of a real scalar. The prompt used to ask for "ONE
// aggregate", so the model honestly abstained on every guarded scalar and no
// handle was ever declared — the cure existed and never ran.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import {
	ACCOUNT_NAME_COLUMN,
	ENRICHED_VIEW,
	REGION_NAME_COLUMN,
} from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

/** What the model actually writes: the house empty-aggregation guard. */
const GUARDED_SUM = "CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(amount) END";

/** The answer's own result columns — the dimension aliased away, which is what
 *  `SELECT region_id__name AS region, … GROUP BY 1` produces. */
const ALIASED_RESULT_COLUMNS = ["region", "account_name", "total_amount"];

describe.skipIf(!fx.available)(
	fx.describeName("the alias gap, and parts-at-source closing it (DAT-671)"),
	() => {
		let resolveAdHocDrillAxes: typeof import("./drill-axes-adhoc").resolveAdHocDrillAxes;
		let resolveAnswerDrillAxes: typeof import("./drill-axes").resolveAnswerDrillAxes;

		beforeAll(async () => {
			// Dynamic import: config parses at module eval and must see the fixture
			// DSNs that attachFixtureWorkspace() installed above.
			({ resolveAdHocDrillAxes } = await import("./drill-axes-adhoc"));
			({ resolveAnswerDrillAxes } = await import("./drill-axes"));
		});

		it("tier A loses the dimension the projection aliased away", async () => {
			const tierA = await resolveAdHocDrillAxes(ALIASED_RESULT_COLUMNS);

			expect(tierA.axes).toEqual([]);
			// An empty result is a CLAIM — assert the reason, not just the
			// emptiness. "No axes" and "the catalog is empty" render identically.
			expect(tierA.reason).toMatch(/catalogued dimension/i);
		});

		it("the PROVEN source recovers both catalogued dimensions from the same answer", async () => {
			const atSource = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: GUARDED_SUM },
			]);

			const columns = atSource.axes.map((a) => a.column);
			expect(columns).toContain(REGION_NAME_COLUMN);
			expect(columns).toContain(ACCOUNT_NAME_COLUMN);
			expect(atSource.reason).toBeUndefined();
		});

		it("resolves from the relation even when it arrives fully qualified", async () => {
			// The declaration path reduces `lake.<layer>.<name>` before it gets
			// here; this pins that the reduced spelling is the one the catalog
			// answers to, which is the whole reason the reduction exists.
			const atSource = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: "SUM(amount)" },
			]);
			expect(atSource.axes.map((a) => a.column)).toContain(REGION_NAME_COLUMN);

			const stale = await resolveAnswerDrillAxes([
				{ relation: `lake.typed.${ENRICHED_VIEW}`, selectExpr: "SUM(amount)" },
			]);
			expect(stale.axes).toEqual([]);
			expect(stale.reason).toMatch(/outside the current analysis/i);
		});

		it("recovers the CATALOGUED axis, not just its name", async () => {
			// Recovering a bare column name would be a hollow cure: the slice menu
			// ranks by the catalog's own interest/relevance and discloses the
			// business context, so an axis that arrives without them renders as
			// unjudged. This is what the fact-scoped slice read returns only when
			// the fixture places slice rows the way the engine writes them.
			const atSource = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: GUARDED_SUM },
			]);
			const region = atSource.axes.find((a) => a.column === REGION_NAME_COLUMN);
			expect(region?.businessContext).toBe("Sales region");
			expect(region?.values).toEqual(["EU", "US"]);
			expect(region?.sliceInterest).toBe("primary");
			// Catalogued ranking decides the order: primary before supporting.
			const columns = atSource.axes.map((a) => a.column);
			expect(columns.indexOf(REGION_NAME_COLUMN)).toBeLessThan(
				columns.indexOf(ACCOUNT_NAME_COLUMN),
			);
		});

		it("withholds the time grain rather than guessing at an ad-hoc concept", async () => {
			// An answer's concept has no persisted additivity verdict, so the gate
			// never offers a time bucket. (No date dimension is catalogued here, so
			// there is nothing to strip and no gate REASON is set — the assertion
			// that matters is that no axis carries a temporal kind.)
			const atSource = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: GUARDED_SUM },
			]);
			expect(atSource.axes.length).toBeGreaterThan(0);
			for (const axis of atSource.axes) expect(axis.temporal).toBeNull();
		});
	},
);
