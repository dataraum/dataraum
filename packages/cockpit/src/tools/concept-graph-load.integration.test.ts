// The concept block, served FROM the property graph (DAT-671 R3) — against the
// real Postgres 19 fixture, because that is now the only honest place to pin
// it: what this suite asserts used to be an in-memory `buildConceptGraph`
// covered by unit fixtures, and moving it onto `og_concepts` /
// `og_concept_edges` / `og_grounded_by` / `og_has_additivity` /
// `og_derives_from` moved the behaviour into SQL the engine owns. A unit test
// over hand-built rows would now only prove the fixture.
//
// The EXCLUSIONS matter as much as the inclusions and get their own assertions:
// a superseded concept, an edge into one, an edge naming no concept, a
// superseded edge, a metric-target verdict, and a `derives_from` edge to a
// concept that does not exist are all seeded (see `conceptSeedSql`) precisely
// so a read that forgot a filter cannot pass here.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { DERIVED_METRIC, PART_OF_CHAIN } from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(
	fx.describeName("loadConceptGraph over the property graph (DAT-671 R3)"),
	() => {
		let loadConceptGraph: typeof import("./concept-graph-load").loadConceptGraph;
		let formatConceptContext: typeof import("./concept-graph").formatConceptContext;
		let AXIS_KEY_ALL: typeof import("./concept-graph").AXIS_KEY_ALL;
		let graph: Awaited<
			ReturnType<typeof import("./concept-graph-load").loadConceptGraph>
		>;

		beforeAll(async () => {
			({ loadConceptGraph } = await import("./concept-graph-load"));
			({ formatConceptContext, AXIS_KEY_ALL } = await import(
				"./concept-graph"
			));
			graph = await loadConceptGraph();
		});

		const nodeNamed = (name: string) => {
			const found = graph.nodes.find((n) => n.name === name);
			if (!found) throw new Error(`no node for concept "${name}"`);
			return found;
		};

		describe("the node set", () => {
			it("serves every ACTIVE concept, name-sorted, and no superseded one", () => {
				const names = graph.nodes.map((n) => n.name);
				expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b)));
				expect(names).toContain("revenue");
				expect(names).toContain("shrinkage");
				// Seeded with `superseded_at` set — `og_concepts` filters it out
				// server-side, which is the filter this read no longer restates.
				expect(names).not.toContain("legacy_margin");
			});

			it("carries the definition text the concept_node vertex does not project", () => {
				// description/indicators/exclude_patterns are NOT graph properties —
				// they ride the identity join back to the `concepts` row. If that join
				// is dropped the block silently loses its whole grounding vocabulary,
				// which nothing else here would notice.
				const revenue = nodeNamed("revenue");
				expect(revenue.description).toBe("Income from primary operations");
				expect(revenue.indicators).toEqual(["sales", "turnover"]);
				expect(revenue.excludePatterns).toEqual(["deferred"]);
				expect(revenue.conceptId).toBe("cpt_revenue");
			});

			it("keeps an ungrounded, unclassified concept as a node with empty lists", () => {
				const top = nodeNamed(PART_OF_CHAIN[PART_OF_CHAIN.length - 1]);
				expect(top.groundings).toEqual([]);
				expect(top.additivity).toEqual([]);
				expect(top.derivedMetrics).toEqual([]);
				expect(top.partOfParents).toEqual([]);
			});
		});

		describe("concept edges", () => {
			it("serves 1-hop parents and children from the edge view's own endpoint resolution", () => {
				expect(nodeNamed("revenue").partOfParents).toEqual([PART_OF_CHAIN[0]]);
				expect(nodeNamed(PART_OF_CHAIN[0]).partOfChildren).toEqual(["revenue"]);
			});

			it("walks the transitive ancestry nearest-first and STOPS at the depth cap", () => {
				// The seeded chain is one link LONGER than the walk's cap, so this
				// pins the boundary itself rather than "some ancestry came back" —
				// the cockpit's CTE depth and the engine's `_PART_OF_MAX_DEPTH` are
				// the same closure and must not drift.
				expect(nodeNamed("revenue").partOfAncestry).toEqual([
					PART_OF_CHAIN[1],
					PART_OF_CHAIN[2],
					PART_OF_CHAIN[3],
				]);
				expect(nodeNamed("revenue").partOfAncestry).not.toContain(
					PART_OF_CHAIN[4],
				);
			});

			it("reads disjoint_with directionally — the engine stores both directions, so neither endpoint doubles", () => {
				expect(nodeNamed("revenue").disjointWith).toEqual(["cost"]);
				expect(nodeNamed("cost").disjointWith).toEqual(["revenue"]);
			});

			it("drops an edge whose endpoint is superseded, names no concept, or is itself superseded", () => {
				// `cost` is the from-side of BOTH must-drop part_of edges (one into
				// the superseded `legacy_margin`, one into a concept that was never
				// seeded), and the from-side of a superseded disjoint edge to
				// `shrinkage`. All three vanish in the element view's own filters.
				expect(nodeNamed("cost").partOfParents).toEqual([]);
				expect(nodeNamed("revenue").disjointWith).not.toContain("shrinkage");
				expect(nodeNamed("shrinkage").disjointWith).toEqual([]);
			});
		});

		describe("groundings", () => {
			it("attaches each grounding through the grounded_by EDGE, healthy ones first", () => {
				const revenue = nodeNamed("revenue");
				expect(revenue.groundings.map((g) => g.snippetId)).toEqual([
					"snip_revenue",
				]);
				expect(revenue.groundings[0].relation).toBe("current_orders_enriched");
				expect(revenue.groundings[0].selectExpr).toBe("SUM(amount)");
				expect(revenue.groundings[0].failed).toBe(false);
			});

			it("keeps a RETAINED-FAILURE grounding, marked failed", () => {
				// Unlike the drill's identity read, which excludes it: this block
				// SHOWS failed attempts so the agent does not re-try one. Failure
				// detail is null here because the fixture snippet carries no
				// provenance blob — the honest fallback, not a lost field.
				const shrinkage = nodeNamed("shrinkage");
				expect(shrinkage.groundings.map((g) => g.snippetId)).toEqual([
					"snip_shrinkage",
				]);
				expect(shrinkage.groundings[0].failed).toBe(true);
				expect(shrinkage.groundings[0].failureMode).toBeNull();
			});

			it("never attaches a FORMULA snippet as a grounding", () => {
				// `snip_formula` is a real row for `gross_margin`, but og_grounding
				// selects extracts only — and `gross_margin` is not a concept at all.
				const all = graph.nodes.flatMap((n) =>
					n.groundings.map((g) => g.snippetId),
				);
				expect(all).not.toContain("snip_formula");
			});
		});

		describe("the verdicts the drill enforces (has_additivity)", () => {
			it("serves both axis classes for a classified measure, sorted", () => {
				const revenue = nodeNamed("revenue");
				expect(
					revenue.additivity.map((a) => [a.axisKind, a.status, a.verdict]),
				).toEqual([
					["categorical", "abstained", null],
					["time", "classified", "additive"],
				]);
				const time = revenue.additivity[1];
				// The class-row sentinel this package mirrors, checked against what
				// the ENGINE actually serves rather than against the copy in
				// `drill-axes.ts` — agreeing with a sibling mirror would prove
				// nothing if both had drifted from the engine together.
				expect(time.axisKey).toBe(AXIS_KEY_ALL);
				expect(time.bucketGrain).toBe("month");
				expect(revenue.additivity[0].abstainReason).toBe("unknown_aggregate");
			});

			it("carries the doctrine reason for a non-additive verdict", () => {
				const cost = nodeNamed("cost");
				expect(cost.additivity).toHaveLength(1);
				expect(cost.additivity[0].verdict).toBe("semi_additive");
				expect(cost.additivity[0].reason).toBe("stock");
			});

			it("serves NO verdict for a concept the engine classified nothing for", () => {
				// Distinct from "not additive", and the render says nothing at all
				// rather than inventing either answer.
				expect(nodeNamed("shrinkage").additivity).toEqual([]);
			});

			it("never picks up a METRIC-target verdict — a metric is not a concept", () => {
				// A `metric`/gross_margin row is seeded alongside the measure ones;
				// og_has_additivity filters target_kind='measure', and nothing here
				// may work around that.
				const all = graph.nodes.flatMap((n) => n.additivity);
				expect(all.map((a) => a.verdict)).not.toContain(
					"non_additive_recompute",
				);
			});
		});

		describe("the metric DAG (derives_from — this edge's first cockpit reader)", () => {
			it("names the metric each grounded concept feeds, with its own facts", () => {
				const revenue = nodeNamed("revenue");
				expect(revenue.derivedMetrics).toHaveLength(1);
				expect(revenue.derivedMetrics[0]).toEqual({
					graphId: DERIVED_METRIC,
					name: "Gross Margin",
					category: "profitability",
					unit: "percent",
					outputType: "ratio",
				});
				expect(nodeNamed("cost").derivedMetrics.map((m) => m.graphId)).toEqual([
					DERIVED_METRIC,
				]);
			});

			it("drops a derives_from edge naming no active concept", () => {
				// The seeded ghost edge would otherwise surface as a phantom concept
				// or attach to the wrong one; og_derives_from INNER JOINs it away.
				expect(nodeNamed("shrinkage").derivedMetrics).toEqual([]);
				expect(graph.nodes.map((n) => n.name)).not.toContain("ghost_concept");
			});
		});

		describe("the tie-out assertion and its observation", () => {
			it("folds the promoted run's per-pair rows onto the reconciles_with edge", () => {
				const revenue = nodeNamed("revenue");
				expect(revenue.reconcilesWith).toHaveLength(1);
				const rec = revenue.reconcilesWith[0];
				expect(rec.partner).toBe("revenue");
				expect(rec.tolerance).toBe(0.01);
				expect(rec.status).toBe("evaluated");
				expect(rec.verdict).toBe("within_tolerance");
				// TWO pairs seeded, one of them not comparable — a partial check
				// must never be servable as a whole one.
				expect(rec.pairs).toBe(2);
				expect(rec.evaluatedPairs).toBe(1);
				expect(rec.relativeDelta).toBe(0.004);
			});
		});

		describe("the block the answer agent actually receives", () => {
			it("carries the served verdicts and metric edges into the rendered text", () => {
				// The end of the wire: graph → model → prompt. Asserting the loader's
				// object alone would leave the render free to drop a section.
				const text = formatConceptContext(graph);
				expect(text).toContain(
					"- additivity: any time axis — additive, bucketable no finer than month",
				);
				expect(text).toContain(
					"- additivity: any categorical axis — NOT CLASSIFIED (unknown_aggregate)",
				);
				expect(text).toContain(
					"- additivity: any time axis — semi_additive (stock), bucketable no finer than month",
				);
				expect(text).toContain(
					`- feeds metric: ${DERIVED_METRIC} (profitability, ratio in percent)`,
				);
				expect(text).toContain(
					`- part of: ${PART_OF_CHAIN[0]} (→ ${PART_OF_CHAIN[1]} → ${PART_OF_CHAIN[2]} → ${PART_OF_CHAIN[3]})`,
				);
				expect(text).toContain("- revenue (measure): Income from primary");
			});

			it("is byte-identical across two INDEPENDENT reads of the same data", () => {
				// The block is the TAIL of a `cache_control: ephemeral` system block,
				// so a physical-row-order leak busts the whole cached prefix
				// silently. Re-reading is the only way to catch that — formatting one
				// already-sorted object twice would prove nothing.
				return expect(
					loadConceptGraph().then(formatConceptContext),
				).resolves.toBe(formatConceptContext(graph));
			});
		});
	},
);
