// The drill's IDENTITY read (DAT-671 R2) against the real property graph:
// snippet id -> the concept it grounds, which IS the measure verdict's
// `target_key`. Integration-only by nature — the whole function is one
// GRAPH_TABLE MATCH, so a unit test could only assert a string.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";

const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(
	fx.describeName("resolveGroundedConcepts (DAT-671 R2)"),
	() => {
		let resolveGroundedConcepts: typeof import("./concept-target").resolveGroundedConcepts;

		beforeAll(async () => {
			({ resolveGroundedConcepts } = await import("./concept-target"));
		});

		it("resolves each grounding snippet to its concept", async () => {
			const byId = await resolveGroundedConcepts([
				"snip_revenue",
				"snip_cost",
			]);
			expect(Object.fromEntries(byId)).toEqual({
				snip_revenue: "revenue",
				snip_cost: "cost",
			});
		});

		it("resolves nothing for an id the graph does not carry", async () => {
			// A hallucinated id, and `snip_formula` — a real row, but a FORMULA, so
			// it is not a grounding at all (`og_grounding` selects extracts only).
			// Both must come back absent rather than partially matched: absence is
			// what makes the caller withhold instead of guessing a neighbour.
			const byId = await resolveGroundedConcepts([
				"snip_does_not_exist",
				"snip_formula",
			]);
			expect(byId.size).toBe(0);
		});

		it("excludes a RETAINED-FAILURE grounding", async () => {
			// `snip_shrinkage` has an active concept row AND a live `grounded_by`
			// edge (the R0 worked example enumerates it) — the only thing keeping it
			// out here is `failure_count > 0`. So this pins the filter itself, not
			// the absence of a row: a computation we know does not run must never
			// license a verdict.
			const byId = await resolveGroundedConcepts(["snip_shrinkage"]);
			expect(byId.size).toBe(0);
		});

		it("does not query at all for an empty id list", async () => {
			expect((await resolveGroundedConcepts([])).size).toBe(0);
		});
	},
);
