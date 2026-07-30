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
//
// DE-ALIASED ON PURPOSE: a metric's `graphId` (e.g. `dso`) and a concept's `name`
// (e.g. `accounts_receivable`) are DISJOINT namespaces in production —
// `current_concept_reconciliation` is keyed by concept name, never by graph_id (a
// review finding: an earlier draft of this suite used "dso" as BOTH, which made the
// reconciliation join look correct while testing nothing about the actual key it
// runs on in production). Every fixture below that touches reconciliation uses two
// visibly different strings and wires them together via `metric()`'s `concepts`
// override, exactly as `coverage-map-load.ts` populates it from `metric_derives_from`.

function metric(
	graphId: string,
	dimensionFacet: string | null,
	over: Partial<CoverageMetricInput> = {},
): CoverageMetricInput {
	return { graphId, name: graphId, dimensionFacet, concepts: [], ...over };
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

	it("is partial with the declared-expectation-violation text, verbatim, when executed carries one", () => {
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
		expect(row.reason?.kind).toBe("state_reason");
		expect(row.reason?.text).toBe(violation);
	});

	it("is partial with the low-confidence-grounding text, verbatim, when executed carries one", () => {
		// metrics_phase.py:501-519's OWN phrasing — a shape the marker-substring
		// approach this gate used to run on would have missed entirely (it only
		// matched "declared expectation not met"), rendering this metric falsely lit.
		const lowConfidence =
			"low-confidence grounding (0.30 < 0.50): the extracted concept's basis is weakly supported";
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed", lowConfidence)],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.reason?.kind).toBe("state_reason");
		expect(row.reason?.text).toBe(lowConfidence);
	});

	it("is partial with a malformed-validation-condition text, verbatim, when executed carries one", () => {
		// verifier.py:124-127's OWN phrasing — a THIRD state_reason shape, proving the
		// gate is "any non-null state_reason", not a list of known phrasings.
		const malformed =
			"declared expectation for 'margin' is malformed: condition 'margin <=' failed to parse";
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed", malformed)],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.reason?.kind).toBe("state_reason");
		expect(row.reason?.text).toBe(malformed);
	});

	it("is lit when executed carries a NULL state_reason (the whole gate, not a substring match)", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "executed", null)],
			groundings: [grounding("dso")],
		});
		expect(rowOf(map, "capital").state).toBe("lit");
	});

	it("is lit when a CANONICAL metric (past executed) has a NULL state_reason", () => {
		// declared → grounded → executed → canonical (an `endorse` transition,
		// dormant today — lifecycle/transitions.py). Gating lit on `state ===
		// "executed"` alone would demote a canonical metric to partial with the
		// nonsense reason "stuck at lifecycle state 'canonical'".
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "canonical", null)],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("lit");
		expect(row.metrics[0]).toMatchObject({ lit: true, reason: null });
	});

	it("still demotes a canonical metric to partial when it DOES carry a state_reason", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")],
			lifecycle: [lifecycle("dso", "canonical", "some flag")],
			groundings: [grounding("dso")],
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.reason?.text).toBe("some flag");
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

	it("is partial when a DERIVED CONCEPT's groundings disagree on reconciliation — graphId ≠ concept name", () => {
		// The regression case: the metric's graph_id ("dso") is NOT the concept name
		// reconciliation is keyed by ("accounts_receivable") — exactly the disjoint
		// namespaces `metric_derives_from` bridges in production. A lookup keyed on
		// `graphId` (the bug this fixture used to mask by aliasing the two strings)
		// would find nothing here and wrongly stay lit.
		const reconciliation: CoverageReconciliationInput[] = [
			{
				concept: "accounts_receivable",
				status: "evaluated",
				verdict: "beyond_tolerance",
			},
		];
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [
				metric("dso", "capital", { concepts: ["accounts_receivable"] }),
			],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
			reconciliation,
		});
		const row = rowOf(map, "capital");
		expect(row.state).toBe("partial");
		expect(row.reason?.kind).toBe("disagreement");
	});

	it("checks disagreement across EVERY concept a metric derives from", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [
				metric("cash_conversion_cycle", "capital", {
					concepts: ["accounts_receivable", "inventory", "accounts_payable"],
				}),
			],
			lifecycle: [lifecycle("cash_conversion_cycle", "executed")],
			groundings: [grounding("cash_conversion_cycle")],
			// Only the SECOND derived concept disagrees — proves every concept in the
			// array is checked, not just the first.
			reconciliation: [
				{
					concept: "accounts_receivable",
					status: "evaluated",
					verdict: "within_tolerance",
				},
				{
					concept: "inventory",
					status: "evaluated",
					verdict: "beyond_tolerance",
				},
			],
		});
		expect(rowOf(map, "capital").state).toBe("partial");
	});

	it("stays lit when reconciliation exists but agrees (within_tolerance)", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [
				metric("dso", "capital", { concepts: ["accounts_receivable"] }),
			],
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
			reconciliation: [
				{
					concept: "accounts_receivable",
					status: "evaluated",
					verdict: "within_tolerance",
				},
			],
		});
		expect(rowOf(map, "capital").state).toBe("lit");
	});

	it("ignores a reconciliation row for a concept the metric does NOT derive from", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [
				metric("dso", "capital", { concepts: ["accounts_receivable"] }),
			],
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

	it("finds nothing (never crashes) when a metric derives from no concept at all", () => {
		const map = buildCoverageMap({
			...EMPTY,
			metrics: [metric("dso", "capital")], // concepts: [] via the default
			lifecycle: [lifecycle("dso", "executed")],
			groundings: [grounding("dso")],
			reconciliation: [
				{
					concept: "accounts_receivable",
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
		expect(rowOf(map, "capital").reason?.kind).toBe("state_reason");
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
		expect(row.reason?.kind).toBe("state_reason");
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
