// Does the coverage map actually survive the trip from the engine's schema to the
// per-dimension read a practitioner reads?
//
// `coverage-map.ts` is unit-tested exhaustively over hand-built rows; what THIS
// suite proves is that the real Postgres round trip (config `metrics` row →
// `current_lifecycle_artifacts` → `sql_snippets` sourced `graph:%` →
// `metric_derives_from` → `current_concept_reconciliation`) lands each row in the
// state the pure builder would predict from the same facts — including two traps
// that only show up against a real schema:
//
//  - HEAD-GATING: `current_lifecycle_artifacts`/`current_concept_reconciliation` are
//    joined to the ('catalog','operating_model') snapshot head, so a row seeded
//    without it is INVISIBLE, not an error, and a metric that looks "declared but
//    never grounded" can just as easily mean "the promotion step was forgotten".
//  - SUPERSESSION: `metrics`/`concepts` are supersession-versioned tables; the raw
//    Drizzle mirror the loader reads carries every row EVER written, not just the
//    active one. A spec-compliance review confirmed this LIVE — the shared fixture's
//    already-superseded `legacy_margin` concept was miscounted as unclassified
//    before the loader's `isNull(supersededAt)` filter was added.
//
// This suite therefore asserts REASONS and COUNTS, never bare state alone, so either
// trap shows up as a mismatch instead of passing by accident.
//
// Six dedicated graph_ids (never `gross_margin`/`mtr_gm`, which several OTHER suites
// pin the exact shape of), one per facet, so each scenario's row state is
// observable in isolation:
//   demand   — DARK, "declared but never grounded" (a metric stuck at `declared`)
//   offer    — PARTIAL, a failed grounding's reason survives verbatim, INCLUDING the
//              src-digest strip (so the leak-barrier discipline is proven over the
//              real DB round trip, not just the pure unit tests)
//   capital  — LIT, a metric executed cleanly
//   supply   — PARTIAL via reconciliation disagreement, joined through a REAL
//              `metric_derives_from` row to a concept name DIFFERENT from the
//              metric's own graph_id (the exact bug a spec-compliance review found:
//              the loader used to look reconciliation up by graph_id, a namespace
//              `current_concept_reconciliation` never uses)
//   capacity — DARK, "no capacity unit metric declared": a SUPERSEDED metric row
//              declared under this facet must not count
//   throughput — DARK, "declared but never grounded": that SAME metric's LIVE
//              successor, declared under THIS facet instead, must be the one that
//              counts — proving the superseded row is excluded, not merely ignored
//              by coincidence, and that the live row is attributed to its OWN facet

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { TEST_WORKSPACE_ID } from "#/test/integration-env";
import {
	coverageGroundingSeedSql,
	coverageMetricSeedSql,
	metricDerivesFromSeedSql,
	RUN_ID,
} from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

const LIT_GRAPH_ID = "dat855_lit_metric";
const PARTIAL_GRAPH_ID = "dat855_partial_metric";
const DARK_GRAPH_ID = "dat855_dark_metric";
const RECONCILED_GRAPH_ID = "dat855_reconciled_metric";
// Deliberately NOT equal to RECONCILED_GRAPH_ID — the regression the concept-join
// bug hid: `current_concept_reconciliation` is keyed by concept NAME, a namespace
// disjoint from a metric's own graph_id.
const RECONCILED_CONCEPT = "dat855_accounts_receivable";
const SUPERSEDED_GRAPH_ID = "dat855_superseded_metric";
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

				// supply — an otherwise-lit metric demoted by a REAL reconciliation join
				// through metric_derives_from, keyed on a concept name distinct from its
				// own graph_id.
				await sql.unsafe(
					coverageMetricSeedSql({
						graphId: RECONCILED_GRAPH_ID,
						name: "DAT-855 Reconciled Metric",
						dimensionFacet: "supply",
						state: "executed",
					}),
				);
				await sql.unsafe(
					coverageGroundingSeedSql({
						snippetId: "snip_cov_reconciled",
						graphId: RECONCILED_GRAPH_ID,
						workspaceId: TEST_WORKSPACE_ID,
						snippetType: "formula",
						failed: false,
					}),
				);
				await sql.unsafe(
					metricDerivesFromSeedSql(RECONCILED_GRAPH_ID, RECONCILED_CONCEPT),
				);
				// A self-loop reconciliation row (fromConcept === toConcept), the shape
				// `current_concept_reconciliation`'s loader filter passes through — same
				// shape as `conceptSeedSql`'s own `rec_1`/`rec_2` rows. A NON-'*' pair_key
				// requires both snippet ids AND (for status='evaluated') the full
				// left/right value + delta measurement — the engine's own
				// `pair_key_snippets`/`status_verdict_reason` CHECK constraints (no FK on
				// the snippet ids, so these need not resolve to a real sql_snippets row).
				// `tolerance` must be non-null: a graded verdict requires a declared band.
				await sql.unsafe(
					`SET search_path TO engine;
INSERT INTO concept_reconciliation (
  reconciliation_id, run_id, vertical, from_concept, to_concept, pair_key,
  left_snippet_id, right_snippet_id, left_value, right_value, delta,
  relative_delta, tolerance, status, verdict, created_at)
VALUES (
  'rec_cov_disagree', '${RUN_ID}', '_adhoc', '${RECONCILED_CONCEPT}',
  '${RECONCILED_CONCEPT}', 'snip_cov_reconciled_l|snip_cov_reconciled_r',
  'snip_cov_reconciled_l', 'snip_cov_reconciled_r', 1000, 900, 100, 0.1, 0.01,
  'evaluated', 'beyond_tolerance', '2026-07-28 00:00:00')
ON CONFLICT DO NOTHING;`,
				);

				// capacity/throughput — one graph_id, two `metrics` rows: a SUPERSEDED one
				// declared under "capacity", a LIVE one (its successor) declared under
				// "throughput". Neither carries a lifecycle row — the point of this pair is
				// purely to prove which FACET the row counts under, not grounding state.
				await sql.unsafe(
					coverageMetricSeedSql({
						graphId: SUPERSEDED_GRAPH_ID,
						name: "DAT-855 Superseded Metric (old)",
						dimensionFacet: "capacity",
						superseded: true,
					}),
				);
				await sql.unsafe(
					coverageMetricSeedSql({
						graphId: SUPERSEDED_GRAPH_ID,
						name: "DAT-855 Superseded Metric (live)",
						dimensionFacet: "throughput",
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

		it("demotes an otherwise-lit metric to partial via a REAL metric_derives_from → reconciliation join", async () => {
			// The regression: graph_id ("dat855_reconciled_metric") and concept name
			// ("dat855_accounts_receivable") are deliberately different strings. A
			// loader that (re-)introduces a lookup keyed on graph_id would find no
			// reconciliation row here and wrongly render this dimension lit.
			const { map } = await loadCoverageMap();
			const row = map.rows.find((r) => r.dimension === "supply");
			expect(row?.state).toBe("partial");
			expect(row?.reason?.kind).toBe("disagreement");
			const metric = row?.metrics.find(
				(m) => m.graphId === RECONCILED_GRAPH_ID,
			);
			expect(metric?.lit).toBe(false);
			expect(metric?.state).toBe("executed");
		});

		it("excludes a SUPERSEDED metric row from its old facet ('capacity')", async () => {
			const { map } = await loadCoverageMap();
			const row = map.rows.find((r) => r.dimension === "capacity");
			expect(row?.state).toBe("dark");
			expect(row?.metrics).toEqual([]);
			// NOT "declared but never grounded" — that would mean the superseded row
			// was counted as declared population under this facet.
			expect(row?.reason?.text).toBe("no capacity unit metric declared");
		});

		it("attributes the LIVE successor to its OWN current facet ('throughput')", async () => {
			const { map } = await loadCoverageMap();
			const row = map.rows.find((r) => r.dimension === "throughput");
			expect(row?.state).toBe("dark");
			expect(row?.reason?.text).toBe("declared but never grounded");
		});

		it("excludes the shared fixture's already-superseded concept from unclassified", async () => {
			// `conceptSeedSql` seeds NINE concept rows, every one of them NULL-facet;
			// ONE (`legacy_margin`, `cpt_retired`) is already superseded there. Before
			// the loader's `isNull(concepts.supersededAt)` filter this counted 9; a
			// spec-compliance review confirmed exactly this live.
			const { map } = await loadCoverageMap();
			expect(map.unclassified.concepts).toBe(8);
			// mtr_gm (`conceptSeedSql`'s own gross_margin metrics row) is the only
			// NULL-facet, ACTIVE metrics row in the shared fixture — every metric THIS
			// suite seeds carries a non-null facet, and the superseded one is excluded
			// outright.
			expect(map.unclassified.metrics).toBe(1);
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
