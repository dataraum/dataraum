// Does the coverage map actually survive the trip from the engine's schema to the
// per-dimension read a practitioner reads?
//
// `coverage-map.ts` is unit-tested exhaustively over hand-built rows; what THIS
// suite proves is that the real Postgres round trip (config `metrics` row →
// `current_lifecycle_artifacts` → `sql_snippets` sourced `graph:%`) lands each row
// in the state the pure builder would predict from the same facts — including the
// HEAD-GATING TRAP: `current_lifecycle_artifacts`/`current_concept_reconciliation`
// are joined to the ('catalog','operating_model') snapshot head, so a row seeded
// without it is INVISIBLE, not an error, and a metric that looks "declared but never
// grounded" can just as easily mean "the promotion step was forgotten". This suite
// therefore asserts REASONS, never bare state alone, so a silently-dropped head
// join would show up as a reason mismatch instead of passing by accident.
//
// Three dedicated graph_ids (never `gross_margin`/`mtr_gm`, which several OTHER
// suites pin the exact shape of) prove: LIT end-to-end, PARTIAL with a failed
// grounding's reason surviving verbatim (INCLUDING the src-digest strip, so the
// leak-barrier discipline is proven over the real DB round trip, not just in the
// pure unit tests), and DARK's "declared but never grounded" wording. The three
// untouched facets (supply/capacity/throughput) prove the OTHER dark wording —
// "no X unit metric declared" — for free, since nothing in the shared fixture ever
// declares a metric against them.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { TEST_WORKSPACE_ID } from "#/test/integration-env";
import {
	coverageGroundingSeedSql,
	coverageMetricSeedSql,
} from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

const LIT_GRAPH_ID = "dat855_lit_metric";
const PARTIAL_GRAPH_ID = "dat855_partial_metric";
const DARK_GRAPH_ID = "dat855_dark_metric";
// A real 40-hex src digest — the exact shape `stripSrcDigests` targets
// (`lib/display-names.ts`'s SRC_DIGEST regex), built rather than hand-typed so its
// length can never silently drift off 40.
const FAKE_SRC_DIGEST = `src_${"a".repeat(40)}`;

describe.skipIf(!fx.available)(
	fx.describeName("the coverage map reaches the cockpit (DAT-855 B2)"),
	() => {
		let loadCoverageMap: typeof import("./coverage-map-load").loadCoverageMap;

		beforeAll(async () => {
			const { SQL } = await import("bun");
			const sql = new SQL(fx.metadataUrl as string);
			try {
				await sql.unsafe(
					coverageMetricSeedSql({
						graphId: LIT_GRAPH_ID,
						name: "DAT-855 Lit Metric",
						dimensionFacet: "capital",
						state: "executed",
					}),
				);
				await sql.unsafe(
					coverageGroundingSeedSql({
						snippetId: "snip_cov_lit",
						graphId: LIT_GRAPH_ID,
						workspaceId: TEST_WORKSPACE_ID,
						snippetType: "formula",
						failed: false,
					}),
				);

				await sql.unsafe(
					coverageMetricSeedSql({
						graphId: PARTIAL_GRAPH_ID,
						name: "DAT-855 Partial Metric",
						dimensionFacet: "offer",
						state: "executed",
					}),
				);
				await sql.unsafe(
					coverageGroundingSeedSql({
						snippetId: "snip_cov_partial",
						graphId: PARTIAL_GRAPH_ID,
						workspaceId: TEST_WORKSPACE_ID,
						snippetType: "extract",
						standardField: "dat855_measure",
						failed: true,
						failureReason: `cannot resolve field for source ${FAKE_SRC_DIGEST}`,
					}),
				);

				await sql.unsafe(
					coverageMetricSeedSql({
						graphId: DARK_GRAPH_ID,
						name: "DAT-855 Dark Metric",
						dimensionFacet: "demand",
						state: "declared",
					}),
				);
			} finally {
				await sql.close();
			}
			({ loadCoverageMap } = await import("./coverage-map-load"));
		});

		it("reports the workspace as analyzed once the operating-model head is promoted", async () => {
			// The shared fixture's global head (`metricArtifactSeedSql`, seeded once
			// for every integration suite) already promotes ('catalog',
			// 'operating_model') — without it this silently short-circuits to an
			// EMPTY map indistinguishable from "nothing declared anywhere".
			const { analyzed } = await loadCoverageMap();
			expect(analyzed).toBe(true);
		});

		it("marks the dimension lit once a real metric executed cleanly", async () => {
			const { map } = await loadCoverageMap();
			const row = map.rows.find((r) => r.dimension === "capital");
			expect(row?.state).toBe("lit");
			expect(row?.reason).toBeNull();
			const metric = row?.metrics.find((m) => m.graphId === LIT_GRAPH_ID);
			expect(metric?.lit).toBe(true);
			expect(metric?.state).toBe("executed");
		});

		it("marks the dimension partial on a failed grounding, reason text stripped of its src digest", async () => {
			const { map } = await loadCoverageMap();
			const row = map.rows.find((r) => r.dimension === "offer");
			expect(row?.state).toBe("partial");
			expect(row?.reason?.kind).toBe("failed_grounding");
			// The digest is gone — stripSrcDigests fired over the REAL round trip,
			// not just in the pure builder's own unit test.
			expect(row?.reason?.text).not.toContain(FAKE_SRC_DIGEST);
			expect(row?.reason?.text).toBe("cannot resolve field for source upload");
			const metric = row?.metrics.find((m) => m.graphId === PARTIAL_GRAPH_ID);
			expect(metric?.lit).toBe(false);
		});

		it("marks the dimension dark with 'declared but never grounded' for a stuck metric", async () => {
			const { map } = await loadCoverageMap();
			const row = map.rows.find((r) => r.dimension === "demand");
			expect(row?.state).toBe("dark");
			expect(row?.metrics).toEqual([]);
			expect(row?.reason?.text).toBe("declared but never grounded");
		});

		it("marks an untouched dimension dark with 'no X unit metric declared'", async () => {
			const { map } = await loadCoverageMap();
			for (const dimension of ["supply", "capacity", "throughput"] as const) {
				const row = map.rows.find((r) => r.dimension === dimension);
				expect(row?.state).toBe("dark");
				expect(row?.reason?.text).toBe(`no ${dimension} unit metric declared`);
			}
		});

		it("always renders all six dimensions regardless of what's seeded", async () => {
			const { map } = await loadCoverageMap();
			expect(map.rows.map((r) => r.dimension)).toEqual([
				"demand",
				"offer",
				"supply",
				"capacity",
				"throughput",
				"capital",
			]);
		});
	},
);
