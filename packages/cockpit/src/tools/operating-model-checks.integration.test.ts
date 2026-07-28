// Do declared step CHECKS actually survive from the persisted DAG to the
// thing the canvas renders?
//
// This is the "inert production path" class in miniature. Every stage in
// between type-checks perfectly whether or not the checks make it: the DAG is
// `json` in Postgres, narrowed from `unknown`, flattened into a node field the
// UI reads. Drop the flatten and nothing fails to compile, no unit test
// notices, and the canvas simply stops showing check indicators — silently,
// because "no checks declared" and "checks lost in transit" render identically
// (both: nothing).
//
// So the assertion is deliberately end-to-end over the REAL persisted shape:
// seed a metric artifact whose steps declare validation, promote the
// operating-model head, run the real loader + composer, and assert the checks
// arrive on the node field `nodes.tsx#checksIndicator` reads — including from
// NON-output steps, which is the union rule the owner ruled on (DAT-840) and
// the easiest thing for a refactor to quietly narrow back to the output step.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { VIEW_TABLE_ID } from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(
	fx.describeName("metric checks reach the render path (DAT-671)"),
	() => {
		let loadOperatingModelGraph: typeof import("./operating-model-load").loadOperatingModelGraph;

		beforeAll(async () => {
			({ loadOperatingModelGraph } = await import("./operating-model-load"));
		});

		it("reports the workspace as analyzed once the operating-model head is promoted", async () => {
			const { analyzed } = await loadOperatingModelGraph();
			// Without the promoted head this silently returns an EMPTY graph that
			// is indistinguishable from "nothing analyzed yet".
			expect(analyzed).toBe(true);
		});

		it("builds a metric node from the persisted graph_definition", async () => {
			const { graph } = await loadOperatingModelGraph();
			const metric = graph.nodes.find((n) => n.data.kind === "metric");
			expect(metric).toBeDefined();
			expect(metric?.data.kind).toBe("metric");
			if (metric?.data.kind !== "metric") throw new Error("unreachable");
			expect(metric.data.hasDag).toBe(true);
			// NB: `state` is the lifecycle_artifacts column passed straight
			// through — a persistence round-trip, NOT computed grounding.
			// Grounding is asserted separately below, off the snippet rows.
			expect(metric.data.state).toBe("grounded");
			expect(metric.data.unit).toBe("percent");
			expect(metric.data.category).toBe("profitability");
			expect(metric.data.formula).toBe("(revenue - cost) / revenue");
		});

		it("carries every step's checks — including non-output steps", async () => {
			const { graph } = await loadOperatingModelGraph();
			const metric = graph.nodes.find((n) => n.data.kind === "metric");
			if (metric?.data.kind !== "metric") throw new Error("no metric node");

			const checks = metric.data.validation;
			// Three declared across three steps: two extracts and the formula.
			expect(checks).toHaveLength(3);

			const conditions = checks.map((c) => c.condition);
			expect(conditions).toContain("revenue >= 0"); // extract step
			expect(conditions).toContain("cost >= 0"); // extract step
			expect(conditions).toContain("margin <= 1"); // output step

			// Each check is TAGGED with its step, which is what lets the indicator
			// say WHICH part of the metric a violation came from.
			const byStep = new Map(checks.map((c) => [c.stepId, c]));
			expect([...byStep.keys()].sort()).toEqual(["cost", "margin", "revenue"]);

			// Severity + message survive narrowing; the indicator colours on
			// severity and shows the message on hover.
			expect(byStep.get("revenue")?.severity).toBe("error");
			expect(byStep.get("revenue")?.message).toBe("Revenue cannot be negative");
			expect(byStep.get("cost")?.severity).toBe("warning");
		});

		it("grounds measures to their enriched view through the SQL parser", async () => {
			// The largest stretch of this loader: snippet rows →
			// sqlRelations() (DuckDB's parse tree) → viewByName → the enriched
			// view + the base tables it derives from. With zero snippets seeded
			// none of it executes, and the metric node still looks fine.
			const { graph } = await loadOperatingModelGraph();

			const view = graph.nodes.find((n) => n.data.kind === "table");
			expect(view).toBeDefined();

			// The measure→view edges only exist when grounding resolved.
			const measures = graph.nodes.filter((n) => n.data.kind === "measure");
			expect(measures.length).toBeGreaterThanOrEqual(2);
			for (const m of measures) {
				if (m.data.kind !== "measure") throw new Error("unreachable");
				expect(m.data.grounded).toBe(true);
				expect(m.data.sql).toBeTruthy();
			}
		});

		it("grounds a QUALIFIED relation identically to a bare one", async () => {
			// The live-bug class, pinned on the path that survives it. `revenue`
			// names the view bare; `cost` names it `lake.typed.<view>`. Grounding
			// reads relations from DuckDB's parse tree (BASE_TABLE.table_name is
			// the bare last segment), so BOTH resolve to the same view.
			//
			// Contrast — and this is the point — the AXES path keys a plain
			// string Map on the declared relation, so the qualified spelling
			// misses there. Two lookup strategies over the same data; only the
			// parser-based one is qualification-immune.
			const { graph } = await loadOperatingModelGraph();
			const grounds = graph.edges.filter((e) => e.kind === "grounds");

			const target = (measure: string) =>
				grounds.find((e) => e.source === `measure:${measure}`)?.target;

			// `revenue`'s snippet reads `current_orders_enriched`;
			// `cost`'s reads `lake.typed.current_orders_enriched`.
			expect(target("revenue")).toBe(`table:${VIEW_TABLE_ID}`);
			expect(target("cost")).toBe(`table:${VIEW_TABLE_ID}`);
			expect(target("cost")).toBe(target("revenue"));
		});

		it("renders check indicators for exactly the check-bearing metrics", async () => {
			// The node-face contract: `checksIndicator` renders iff validation is
			// non-empty, so a metric with checks must arrive with a non-empty array
			// and every non-metric node must not carry one at all.
			const { graph } = await loadOperatingModelGraph();
			for (const node of graph.nodes) {
				if (node.data.kind === "metric") {
					expect(Array.isArray(node.data.validation)).toBe(true);
				} else {
					expect("validation" in node.data).toBe(false);
				}
			}
		});
	},
);
