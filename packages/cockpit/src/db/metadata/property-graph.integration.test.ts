// Worked example (DAT-671 R0): a MATCH over `og_grounded_by` — concept ->
// grounding-snippet — run through `queryOperatingModelGraph` against the
// fixture's real Postgres 19 property graph. This is the executable proof
// that the "TS has no PGQ" framing this package carried for a long time was
// never true: GRAPH_TABLE runs IN POSTGRES, reached here with nothing more
// than Drizzle's `sql` template and the ordinary metadata reader connection —
// see property-graph.ts's header for the corrected framing.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";

const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(
	fx.describeName(
		"the operating-model property graph reaches the cockpit (DAT-671 R0)",
	),
	() => {
		let queryOperatingModelGraph: typeof import("./property-graph").queryOperatingModelGraph;
		let sqlTag: typeof import("drizzle-orm").sql;

		// The concepts these MATCHes resolve to are seeded in GLOBAL SETUP
		// (`seed-catalog.ts`'s `conceptSeedSql`, DAT-671 R2) rather than here: a
		// concept row is what makes an `og_grounded_by` EDGE exist at all, so
		// seeding it inside one suite would make every other suite's view of the
		// graph depend on file execution order.
		beforeAll(async () => {
			({ queryOperatingModelGraph } = await import("./property-graph"));
			({ sql: sqlTag } = await import("drizzle-orm"));
		});

		it("MATCHes concept -[grounded_by]-> grounding, returning concept -> snippet rows", async () => {
			const rows = await queryOperatingModelGraph<{
				concept_name: string;
				snippet_id: string;
			}>(
				sqlTag.raw(
					"MATCH (c IS concept_node)-[e IS grounded_by]->(g IS grounding_node) " +
						"COLUMNS (c.name AS concept_name, g.snippet_id AS snippet_id)",
				),
			);
			// An empty result here would be a claim in itself (README's standing
			// rule) — it would mean either seeding or the grant/graph wiring broke,
			// not "no groundings yet", since the shared fixture always seeds
			// snip_revenue/snip_cost.
			//
			// `snip_shrinkage` is the fixture's RETAINED-FAILURE grounding and it is
			// listed here on purpose: this MATCH asks only "which concepts are
			// grounded", so a failed grounding IS one of the edges. Whether a caller
			// may ACT on it is a different question, asked with a different filter
			// (`tools/concept-target.ts` excludes `g.failed`).
			expect(
				rows
					.map((r) => [r.concept_name, r.snippet_id] as const)
					.sort(([a], [b]) => a.localeCompare(b)),
			).toEqual([
				["cost", "snip_cost"],
				["revenue", "snip_revenue"],
				["shrinkage", "snip_shrinkage"],
			]);
		});

		it("binds a value through a NESTED sql fragment into the MATCH's WHERE — the idiom future callers MUST copy", async () => {
			// The precedent property-graph.ts's header warns about: `sql.raw()` is
			// for STATIC pattern/label syntax only. Any request-derived value (here
			// standing in for a snippet/concept id R2/R3 will thread through) rides
			// a nested `` sql`...${value}` `` fragment instead, composed into the
			// MATCH/COLUMNS argument, which is itself composed into the outer
			// GRAPH_TABLE template inside queryOperatingModelGraph — two levels of
			// composition, both binding the value as a real parameter rather than
			// interpolating it into raw text.
			const conceptName = "revenue";
			const nameFilter = sqlTag`c.name = ${conceptName}`;
			const rows = await queryOperatingModelGraph<{
				concept_name: string;
				snippet_id: string;
			}>(
				sqlTag`MATCH (c IS concept_node WHERE ${nameFilter})-[e IS grounded_by]->(g IS grounding_node)
				       COLUMNS (c.name AS concept_name, g.snippet_id AS snippet_id)`,
			);
			expect(rows.map((r) => [r.concept_name, r.snippet_id])).toEqual([
				["revenue", "snip_revenue"],
			]);
		});
	},
);
