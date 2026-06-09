import { describe, expect, it } from "vitest";

import { narrowDag, summarizeDag } from "#/lib/metric-dag";

// A shipped metric DAG as it comes off the YAML (untrusted shape — extra keys
// like `validation`/`decimal_places` the render view ignores).
const EBITDA_OUTPUT = {
	type: "scalar",
	metric_id: "ebitda",
	unit: "currency",
	decimal_places: 0,
};
const EBITDA_DEPS = {
	ebitda: {
		level: 3,
		type: "formula",
		expression: "operating_income + depreciation",
		depends_on: ["operating_income", "depreciation"],
		output_step: true,
	},
	revenue: {
		level: 1,
		type: "extract",
		source: { standard_field: "revenue", statement: "income_statement" },
		aggregation: "sum",
		validation: [{ condition: "value > 0" }],
	},
	operating_income: {
		level: 2,
		type: "formula",
		expression: "revenue - cost_of_goods_sold",
		depends_on: ["revenue", "cost_of_goods_sold"],
	},
};

describe("narrowDag", () => {
	it("narrows the output node", () => {
		const { output } = narrowDag(EBITDA_OUTPUT, {});
		expect(output).toEqual({
			type: "scalar",
			metricId: "ebitda",
			unit: "currency",
		});
	});

	it("narrows extract and formula steps, dropping unknown keys", () => {
		const { steps } = narrowDag(EBITDA_OUTPUT, EBITDA_DEPS);
		const revenue = steps.find((s) => s.id === "revenue");
		expect(revenue).toMatchObject({
			type: "extract",
			standardField: "revenue",
			statement: "income_statement",
			aggregation: "sum",
			expression: null,
			outputStep: false,
		});
		const ebitda = steps.find((s) => s.id === "ebitda");
		expect(ebitda).toMatchObject({
			type: "formula",
			expression: "operating_income + depreciation",
			dependsOn: ["operating_income", "depreciation"],
			outputStep: true,
		});
	});

	it("sorts steps by dependency level, leaves first and output last", () => {
		const { steps } = narrowDag(EBITDA_OUTPUT, EBITDA_DEPS);
		expect(steps.map((s) => s.id)).toEqual([
			"revenue",
			"operating_income",
			"ebitda",
		]);
	});

	it("is tolerant of missing / malformed input (never throws)", () => {
		expect(narrowDag(null, null)).toEqual({ output: null, steps: [] });
		expect(narrowDag(undefined, "nope")).toEqual({ output: null, steps: [] });
		// A junk step value is skipped, not rendered.
		const { steps } = narrowDag({}, { good: { type: "extract" }, bad: 42 });
		expect(steps.map((s) => s.id)).toEqual(["good"]);
	});
});

describe("summarizeDag", () => {
	it("counts steps and collects extract leaf concepts", () => {
		const { steps } = narrowDag(EBITDA_OUTPUT, EBITDA_DEPS);
		const { stepCount, leafConcepts } = summarizeDag(steps);
		expect(stepCount).toBe(3);
		expect(leafConcepts).toEqual(["revenue"]); // only the extract step's standard_field
	});
});
