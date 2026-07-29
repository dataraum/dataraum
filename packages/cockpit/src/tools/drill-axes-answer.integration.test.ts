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
 *  `SELECT region_id__name AS region, … GROUP BY 1` produces.
 *
 *  These are DERIVED from the seeded catalog, not independent names: `region` is
 *  REGION_NAME_COLUMN (`region_id__name`) with the `<fk>__` enrichment prefix
 *  aliased off, and `account_name` is ACCOUNT_NAME_COLUMN (`account_id__name`)
 *  the same way — the exact rename a model writes without thinking. That
 *  correspondence is the whole experiment, so it is stated here rather than
 *  left to be reconstructed from seed-catalog.ts. */
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

		it("resolves the SAME axes whichever spelling the relation arrives in", async () => {
			// The model is told to address tables as lake.<layer>.<name>, so that is
			// the spelling an answer's declared source carries. The catalog's
			// view_name is bare, and this resolver keys a plain string Map on it —
			// so before DAT-671 the qualified spelling missed and the miss was
			// reported as "reads relations outside the current analysis — likely a
			// stale snippet from an earlier run": a false accusation about lineage
			// for what is only a format mismatch. Both spellings must land alike.
			const bare = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: "SUM(amount)" },
			]);
			const qualified = await resolveAnswerDrillAxes([
				{ relation: `lake.typed.${ENRICHED_VIEW}`, selectExpr: "SUM(amount)" },
			]);

			expect(bare.axes.map((a) => a.column)).toContain(REGION_NAME_COLUMN);
			expect(qualified.axes.map((a) => a.column)).toEqual(
				bare.axes.map((a) => a.column),
			);
			expect(qualified.reason).toBeUndefined();
		});

		it("still blames a genuinely unknown relation, and only then", async () => {
			// The stale-snippet reason has to survive as a TRUE statement — reducing
			// the spelling must not turn every miss into silence.
			const unknown = await resolveAnswerDrillAxes([
				{ relation: "lake.typed.no_such_view", selectExpr: "SUM(amount)" },
			]);
			expect(unknown.axes).toEqual([]);
			expect(unknown.reason).toMatch(/outside the current analysis/i);
			// Reduced in the message too, not echoed back qualified.
			expect(unknown.reason).toContain("no_such_view");
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
			// These sources carry no snippetId, so `resolveAnswerTarget` resolves NO
			// identity and the gate never reaches a verdict to offer a bucket from.
			// (The fixture DOES persist verdicts for `revenue`/`cost` since DAT-671
			// R3 — reaching them requires the identity spine, which is exactly the
			// point.) No date dimension is catalogued here either, so there is
			// nothing to strip and no gate REASON is set — the assertion that
			// matters is that no axis carries a temporal kind.
			const atSource = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: GUARDED_SUM },
			]);
			expect(atSource.axes.length).toBeGreaterThan(0);
			for (const axis of atSource.axes) expect(axis.temporal).toBeNull();
		});

		// DAT-671, "we should not slice on already existing slices" — the
		// parts-at-source path: `resolveAnswerDrillAxes`'s `baseSql` param (what
		// `answer-result.tsx` sends as `state.sql`) already groups by one of the
		// catalog's resolved axes, so THAT axis renders greyed while the other
		// catalogued dimension on the same fact stays a live option.
		//
		// FIXTURE HONESTY (owner review): this GROUP-BY `baseSql` is NOT a shape
		// `state.sql` can actually take on the live answer canvas TODAY — a
		// drillSource only exists once `proveAnswerSource` proves the declared
		// parts reproduce the answer's value via a SCALAR subquery comparison,
		// so a drillSource-bearing answer's `state.sql` is always single-row and
		// essentially never carries a naming GROUP BY (see the "near-dead wire"
		// note on `resolveAnswerDrillAxes`'s own docstring). This test therefore
		// exercises the MECHANISM — the wiring from `baseSql` through the
		// structural read to the stamped axis — not a reachable production path;
		// it stays valuable because the wire is harmless to keep (a wider proof
		// shape covering row-set answers would make it fire for real) and the
		// plumbing itself needs coverage independent of today's narrow proof.
		it("[mechanism, not yet a reachable shape] greys the axis a baseSql already groups by, keeping the other axis enabled", async () => {
			const baseSql =
				`SELECT ${REGION_NAME_COLUMN}, ${GUARDED_SUM} AS revenue ` +
				`FROM lake.typed.${ENRICHED_VIEW} GROUP BY ${REGION_NAME_COLUMN}`;
			const atSource = await resolveAnswerDrillAxes(
				[{ relation: ENRICHED_VIEW, selectExpr: GUARDED_SUM }],
				baseSql,
			);

			const region = atSource.axes.find((a) => a.column === REGION_NAME_COLUMN);
			const account = atSource.axes.find(
				(a) => a.column === ACCOUNT_NAME_COLUMN,
			);
			expect(region?.disabledReason).toMatch(/already at this grain/i);
			// The item stays IN THE MENU — never removed.
			expect(atSource.axes.map((a) => a.column)).toContain(REGION_NAME_COLUMN);
			expect(account?.disabledReason).toBeNull();
		});

		it("greys nothing when baseSql is absent — unchanged from before DAT-671 (the current live-canvas reality)", async () => {
			const atSource = await resolveAnswerDrillAxes([
				{ relation: ENRICHED_VIEW, selectExpr: GUARDED_SUM },
			]);
			for (const axis of atSource.axes) expect(axis.disabledReason).toBeNull();
		});
	},
);
