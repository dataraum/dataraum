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

		beforeAll(async () => {
			const { SQL } = await import("bun");
			// Engine-emulation scaffolding (same pattern as
			// bus-matrix.integration.test.ts): the cockpit's own roles cannot write
			// engine.* rows, so this seeds them directly.
			const seedSql = new SQL(fx.metadataUrl as string);
			try {
				// Two concepts naming the two graph-authored extract groundings the
				// SHARED fixture seed already writes (test/seed-catalog.ts's
				// graphSnippetSeedSql: snip_revenue/snip_cost, standard_field
				// revenue/cost, source graph:gross_margin — the only two rows that
				// pass current_groundings' `snippet_type = 'extract' AND source LIKE
				// 'graph:%'` filter; snip_formula is a formula, not a grounding).
				// og_grounded_by resolves a grounding's `concept` to the ACTIVE
				// concept row of the same name (name, superseded_at IS NULL).
				// vertical='_adhoc': the fixture workspace is unbound (no
				// workspace_settings row), and the vertical-scoped concepts read view
				// falls back to that placeholder vertical
				// (read_views.py's `_vertical_scoped_view_sql`).
				await seedSql.unsafe(
					`INSERT INTO engine.concepts
					 (concept_id, vertical, name, kind, created_at)
					 VALUES
					   ($1, '_adhoc', 'revenue', 'measure', $3),
					   ($2, '_adhoc', 'cost', 'measure', $3)
					 ON CONFLICT DO NOTHING`,
					["cpt_revenue", "cpt_cost", "2026-07-28 00:00:00"],
				);
			} finally {
				await seedSql.close();
			}
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
			expect(
				rows
					.map((r) => [r.concept_name, r.snippet_id] as const)
					.sort(([a], [b]) => a.localeCompare(b)),
			).toEqual([
				["cost", "snip_cost"],
				["revenue", "snip_revenue"],
			]);
		});
	},
);
