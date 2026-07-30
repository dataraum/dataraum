import { describe, expect, it } from "vitest";

import {
	buildCoverageMap,
	COVERAGE_DIMENSIONS,
	type CoverageGroundingInput,
	type CoverageLifecycleInput,
	type CoverageMapInput,
	type CoverageMetricInput,
	type CoverageReconciliationInput,
} from "./coverage-map";

// FIXTURE PROVENANCE: shapes are taken from the engine's writers —
// `metrics`/`concepts.dimension_facet` (`analysis/semantic/db_models.py::
// DimensionFacet`), `lifecycle_artifacts.state` (`lifecycle/db_models.py::
// ArtifactState`: declared/grounded/executed/canonical), and `sql_snippets.
// provenance` (`graphs/models.py`: `{failure_mode, failure_reason}` on a failed row).

function metric(
	graphId: string,
	dimensionFacet: string | null,
	over: Partial<CoverageMetricInput> = {},
): CoverageMetricInput {
	return { graphId, name: graphId, dimensionFacet, ...over };
}

function lifecycle(
	graphId: string,
	state: string | null,
	stateReason: string | null = null,
): CoverageLifecycleInput {
	return { graphId, state, stateReason };
}

function grounding(
	graphId: string,
	over: Partial<CoverageGroundingInput> = {},
): CoverageGroundingInput {
	return {
		graphId,
		snippetType: "formula",
		failed: false,
		provenance: { column_mappings_basis: {}, assumptions: [] },
		resolvedPeriod: null,
		calendarSource: null,
		...over,
	};
}

const EMPTY: CoverageMapInput = {
	metrics: [],
	concepts: [],
	lifecycle: [],
	groundings: [],
	reconciliation: [],
};

function rowOf(map: ReturnType<typeof buildCoverageMap>, dimension: string) {
	const row = map.rows.find((r) => r.dimension === dimension);
	if (!row) throw new Error(`no row for ${dimension}`);
	return row;
}

describe("buildCoverageMap (DAT-855 B2)", () => {
	it("always renders exactly the six dimensions, in the engine's declared order", () => {
		const map = buildCoverageMap(EMPTY);
		expect(map.rows.map((r) => r.dimension)).toEqual([
			"demand",
			"offer",
			"supply",
			"capacity",
			"throughput",
			"capital",
		]);
		expect(map.rows).toHaveLength(COVERAGE_DIMENSIONS.length);
	});

	it("is dark with 'no X unit metric declared' when nothing is declared for a facet", () => {
		const map = buildCoverageMap(EMPTY);
		for (const row of map.rows) {
			expect(row.state).toBe("dark");
			expect(row.metrics).toEqual([]);
			expect(row.reason?.text).toBe(`no ${row.dimension} unit metric declared`);
		}
	});

	it("is dark with 'declared but never grounded' when a metric exists but never left declared", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "declared")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("dark");
		expect(row.metrics).toEqual([]);
		expect(row.reason?.text).toBe("declared but never grounded");
	});

	it("is dark the same way when the metric has no lifecycle row at all", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("dark");
		expect(row.reason?.text).toBe("declared but never grounded");
	});

	it("is lit when a grounded metric reached executed cleanly", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("lit");
		expect(row.reason).toBeNull();
		expect(row.metrics).toHaveLength(1);
		expect(row.metrics[0]).toMatchObject({
			graphId: "dso",
			lit: true,
			reason: null,
		});
	});

	it("is partial when stuck at grounded (not yet executed)", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "grounded")],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.metrics[0].lit).toBe(false);
		expect(row.reason?.kind).toBe("not_executed");
		expect(row.reason?.text).toBe("grounded but not yet executed");
	});

	it("is partial with the declared-expectation-violation text, verbatim, when executed carries a violation", () => {
		const violation =
			"declared expectation not met for 'margin': Margin cannot exceed 100% (value=1.4, severity=error)";
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("gross_margin", "offer")],
			lifecycle: [lifecycle("gross_margin", "executed", violation)],
			groundings: [grounding("gross_margin")],
		});
		const row = rowOf(map, "offer");
		expect(row.state).toBe("partial");
		expect(row.reason?.kind).toBe("expectation_violated");
		expect(row.reason?.text).toBe(violation);
	});

	it("is partial with the provenance failure_reason when a grounding failed", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [
				grounding("dso", {
					snippetType: "extract",
					failed: true,
					provenance: {
						failure_mode: "verifier_rejected",
						failure_reason:
							"Current assets cannot be negative (value=-70907.31)",
					},
				}),
			],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.reason?.kind).toBe("failed_grounding");
		expect(row.reason?.text).toBe(
			"Current assets cannot be negative (value=-70907.31)",
		);
	});

	it("falls back to a bare failed-grounding reason when the failure carries no text", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [
				grounding("dso", {
					snippetType: "extract",
					failed: true,
					provenance: {},
				}),
			],
		});
		const row = rowOf(map, "capital");
		expect(row.reason?.kind).toBe("failed_grounding");
		expect(row.reason?.text).toBe("a grounding attempt for this metric failed");
	});

	it("is partial when the concept's own groundings disagree on reconciliation", () => {
		const reconciliation: CoverageReconciliationInput[] = [
			{ concept: "dso", status: "evaluated", verdict: "beyond_tolerance" },
		];
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
			reconciliation,
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.reason?.kind).toBe("disagreement");
	});

	it("stays lit when reconciliation exists but agrees (within_tolerance)", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
			reconciliation: [
				{ concept: "dso", status: "evaluated", verdict: "within_tolerance" },
			],
		});
		expect(rowOf(map, "capital").state).toBe("lit");
	});

	it("ignores a partner-edge reconciliation naming a DIFFERENT concept", () => {
		// The loader is documented to only pass SELF-LOOP rows through — this proves
		// the builder does not need to re-derive that filter to stay correct.
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
			reconciliation: [
				{
					concept: "some_other_concept",
					status: "evaluated",
					verdict: "beyond_tolerance",
				},
			],
		});
		expect(rowOf(map, "capital").state).toBe("lit");
	});

	it("prioritizes state_reason over a failed grounding's provenance text", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [
				lifecycle("dso", "executed", "declared expectation not met for 'x': y"),
			],
			groundings: [
				grounding("dso", {
					snippetType: "extract",
					failed: true,
					provenance: {
						failure_mode: "execution_failed",
						failure_reason: "boom",
					},
				}),
			],
		});
		expect(rowOf(map, "capital").reason?.kind).toBe("expectation_violated");
	});

	it("is lit as soon as ONE metric of the facet is lit, even with a partial sibling", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital"), metric("dpo", "capital")],
			lifecycle: [lifecycle("dso", "executed"), lifecycle("dpo", "grounded")],
			groundings: [grounding("dso"), grounding("dpo")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("lit");
		// Both grounded metrics are still listed — full transparency of what grounds it.
		expect(row.metrics.map((m) => m.graphId).sort()).toEqual(["dpo", "dso"]);
		expect(row.metrics.find((m) => m.graphId === "dso")?.lit).toBe(true);
		expect(row.metrics.find((m) => m.graphId === "dpo")?.lit).toBe(false);
	});

	it("excludes an ungrounded sibling from the metrics list even when the row is lit", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital"), metric("dpo", "capital")],
			lifecycle: [lifecycle("dso", "executed"), lifecycle("dpo", "declared")],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.metrics.map((m) => m.graphId)).toEqual(["dso"]);
	});

	it("picks the most informative reason among several non-lit grounded metrics", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital"), metric("dpo", "capital")],
			lifecycle: [
				lifecycle("dpo", "grounded"), // generic "not executed" fallback
				lifecycle("dso", "executed", "declared expectation not met for 'x': y"), // specific — should win
			],
			groundings: [grounding("dso"), grounding("dpo")],
		});
		const row = rowOf(map, "capital");
		expect(row.reason?.kind).toBe("expectation_violated");
	});

	it("treats cross_cutting as no row at all — not demand/offer/etc, not unclassified", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("ebitda", "cross_cutting")],
			concepts: [
				{ name: "tax", kind: "measure", dimensionFacet: "cross_cutting" },
			],
		});
		for (const row of map.rows) {
			expect(row.metrics.map((m) => m.graphId)).not.toContain("ebitda");
		}
		expect(map.unclassified.metrics).toBe(0);
		expect(map.unclassified.concepts).toBe(0);
	});

	it("counts NULL-facet metrics and concepts as unclassified, never as a row", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("mystery_metric", null), metric("dso", "capital")],
			concepts: [
				{ name: "revenue", kind: "measure", dimensionFacet: null },
				{ name: "cogs", kind: "measure", dimensionFacet: "offer" },
			],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
		});
		expect(map.unclassified).toEqual({ metrics: 1, concepts: 1 });
		for (const row of map.rows) {
			expect(row.metrics.map((m) => m.graphId)).not.toContain("mystery_metric");
		}
	});

	it("sorts a row's metrics by graphId for a stable render order", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("zeta", "capital"), metric("alpha", "capital")],
			lifecycle: [
				lifecycle("zeta", "executed"),
				lifecycle("alpha", "executed"),
			],
			groundings: [grounding("zeta"), grounding("alpha")],
		});
		expect(rowOf(map, "capital").metrics.map((m) => m.graphId)).toEqual([
			"alpha",
			"zeta",
		]);
	});

	it("carries resolvedPeriod/calendarSource through for display, never as a state input", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [
				grounding("dso", {
					snippetType: "extract",
					resolvedPeriod: "2026-06-30",
					calendarSource: "fiscal_calendar",
				}),
			],
		});
		const m = rowOf(map, "capital").metrics[0];
		expect(m.resolvedPeriod).toBe("2026-06-30");
		expect(m.calendarSource).toBe("fiscal_calendar");
		expect(m.lit).toBe(true);
	});
});
