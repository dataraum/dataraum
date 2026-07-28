// Twin-narrow drift test (DAT-840) — the two DAG-rendering paths
// (`lib/metric-dag.ts`'s `narrowDag`, the shipped/override shadow DAG;
// `tools/operating-model-graph.ts`'s `parseMetricDag`, the live model-canvas
// DAG) each narrow their OWN step shape off the same persisted
// `graph_definition` json, independently, by design (see `metric-dag.ts`'s
// header). Comments saying "mirrors the other narrow" don't fail CI when one
// changes and the other doesn't — this test does: it feeds the SAME raw
// `dependencies` literal to both and asserts their per-step `validation`
// arrays agree, step for step.

import { describe, expect, it } from "vitest";

import { narrowDag } from "#/lib/metric-dag";
import { parseMetricDag } from "#/tools/operating-model-graph";

// One raw `dependencies` object, shaped exactly like a persisted
// `graph_definition` — checks on BOTH a leaf extract and the output formula,
// an unusual severity, and a step with no `validation` key at all.
const DEPENDENCIES = {
	revenue: {
		type: "extract",
		level: 1,
		source: { standard_field: "revenue", statement: "income_statement" },
		aggregation: "sum",
		validation: [{ condition: "value > 0", severity: "critical" }],
	},
	days_in_period: {
		type: "constant",
		level: 1,
		parameter: "days_in_period",
		default: 30,
		// No `validation` key — must narrow to [] on both sides.
	},
	dso: {
		type: "formula",
		level: 2,
		expression: "revenue * days_in_period",
		depends_on: ["revenue", "days_in_period"],
		output_step: true,
		validation: [
			{
				condition: "0 <= value <= 365",
				severity: "warning",
				message: "DSO outside typical range",
			},
			{ condition: "value != null" },
		],
	},
};

describe("validation narrowing parity between narrowDag and parseMetricDag", () => {
	it("narrows the SAME per-step validation arrays from the same raw dependencies", () => {
		const { steps: shadowSteps } = narrowDag({}, DEPENDENCIES);
		const liveDag = parseMetricDag({
			output: {},
			metadata: { name: "dso" },
			dependencies: DEPENDENCIES,
		});
		expect(liveDag).not.toBeNull();

		const shadowById = new Map(shadowSteps.map((s) => [s.id, s.validation]));
		const liveById = new Map(
			(liveDag?.steps ?? []).map((s) => [s.stepId, s.validation]),
		);

		// Same step ids on both sides.
		expect([...shadowById.keys()].sort()).toEqual([...liveById.keys()].sort());
		for (const stepId of shadowById.keys()) {
			expect(liveById.get(stepId)).toEqual(shadowById.get(stepId));
		}
		// Concretely: the leaf extract's check, the formula's TWO checks, and
		// the checkless constant all agree.
		expect(shadowById.get("revenue")).toEqual([
			{ condition: "value > 0", severity: "critical", message: null },
		]);
		expect(shadowById.get("days_in_period")).toEqual([]);
		expect(shadowById.get("dso")).toEqual([
			{
				condition: "0 <= value <= 365",
				severity: "warning",
				message: "DSO outside typical range",
			},
			{ condition: "value != null", severity: null, message: null },
		]);
	});
});
