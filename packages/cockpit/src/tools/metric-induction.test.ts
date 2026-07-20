// The metric induction -> overlay payload conversion (DAT-807).
//
// This is the boundary that lets the LLM-facing schema and the persisted shape
// differ: the model emits arrays (expressible under constrained decoding), the
// engine reads maps (`GraphLoader._parse_graph`). Everything downstream —
// frameFamily's declare path, the teach write, the ModelFrame round-trip — sees
// only the payload shape, so a bug here is invisible until a metric fails to
// compose inside the engine.
//
// The load-bearing assertion is the round-trip: every converted payload must
// re-parse as `ProposedMetric`, which IS the shape the engine's applier stores.

import { describe, expect, it } from "vitest";

import { type InducedMetric, toProposedMetric } from "./metric-induction";
import { MetricSpecSchema } from "./metric-spec";

// The persisted shape minus `vertical` — the same schema frame.ts exposes as
// `ProposedMetric`, taken from metric-spec.ts so this test stays config-free.
const ProposedMetric = MetricSpecSchema.omit({ vertical: true });

const REVENUE = {
	type: "extract",
	step_id: "revenue",
	standard_field: "revenue",
	statement: "income_statement",
	aggregation: "sum",
	checks: [],
} satisfies InducedMetric["steps"][number];

const COST = {
	type: "extract",
	step_id: "cost_of_goods_sold",
	standard_field: "cost_of_goods_sold",
	statement: "income_statement",
	aggregation: "sum",
	checks: [],
} satisfies InducedMetric["steps"][number];

/** A two-leaf margin metric — the common induced shape. */
function grossMargin(over: Partial<InducedMetric> = {}): InducedMetric {
	return {
		graph_id: "gross_margin",
		name: "Gross Margin",
		description: "Share of revenue left after direct costs",
		category: "profitability",
		tags: ["margin"],
		output_type: "scalar",
		unit: "ratio",
		decimal_places: 2,
		parameters: [],
		steps: [REVENUE, COST],
		output_step: {
			type: "formula",
			step_id: "gross_margin",
			expression: "(revenue - cost_of_goods_sold) / revenue",
			depends_on: ["revenue", "cost_of_goods_sold"],
			checks: [
				{
					condition: "value <= 1",
					severity: "warning",
					message: "Margin above 100%",
				},
			],
		},
		interpretation_bands: [],
		...over,
	} as InducedMetric;
}

/** Narrow the payload's `dependencies` for assertion (it is `unknown` by design). */
function deps(m: unknown): Record<string, Record<string, unknown>> {
	return (m as { dependencies: Record<string, Record<string, unknown>> })
		.dependencies;
}

describe("toProposedMetric — array shape -> overlay payload", () => {
	it("keys dependencies by step_id, folding the output step back in", () => {
		const d = deps(toProposedMetric(grossMargin()));

		expect(Object.keys(d).sort()).toEqual([
			"cost_of_goods_sold",
			"gross_margin",
			"revenue",
		]);
		// The output step is a map entry like any other — the engine finds it via
		// the flag, not by position.
		expect(d.gross_margin?.output_step).toBe(true);
		expect(d.revenue?.output_step).toBeUndefined();
	});

	it("writes step checks under the SINGULAR key `validation`", () => {
		// GraphLoader reads `data.get("validation")`. A `validations` key would be
		// silently dropped and every declared expectation would vanish — the exact
		// class of bug that makes a metric look fine and check nothing.
		const d = deps(toProposedMetric(grossMargin()));

		expect(d.gross_margin?.validation).toEqual([
			{
				condition: "value <= 1",
				severity: "warning",
				message: "Margin above 100%",
			},
		]);
		expect(d.gross_margin).not.toHaveProperty("validations");
		// A step with no checks carries no key at all, mirroring
		// model_dump(exclude_none=True) — no empty-list spray into the JSONB row.
		expect(d.revenue).not.toHaveProperty("validation");
	});

	it("nests an extract's concept + statement under `source`", () => {
		const d = deps(toProposedMetric(grossMargin()));

		expect(d.revenue).toMatchObject({
			type: "extract",
			source: { standard_field: "revenue", statement: "income_statement" },
			aggregation: "sum",
		});
		// `table` / `column` are absent by construction: 44/44 shipped extracts key
		// off standard_field, and frame-time leaves are concepts, never columns.
		expect(d.revenue?.source).toEqual({
			standard_field: "revenue",
			statement: "income_statement",
		});
	});

	it("computes each step's topological level instead of asking the model", () => {
		const d = deps(
			toProposedMetric(
				grossMargin({
					steps: [
						REVENUE,
						COST,
						{
							type: "formula",
							step_id: "gross_profit",
							expression: "revenue - cost_of_goods_sold",
							depends_on: ["revenue", "cost_of_goods_sold"],
							checks: [],
						},
					],
					output_step: {
						type: "formula",
						step_id: "gross_margin",
						expression: "gross_profit / revenue",
						depends_on: ["gross_profit", "revenue"],
						checks: [
							{
								condition: "value <= 1",
								severity: "warning",
								message: "over 100%",
							},
						],
					},
				}),
			),
		);

		expect(d.revenue?.level).toBe(1); // leaf extract
		expect(d.gross_profit?.level).toBe(2); // formula over leaves
		expect(d.gross_margin?.level).toBe(3); // formula over a formula
	});

	it("terminates on a self-referential or dangling depends_on", () => {
		// The model can emit a cycle; the converter must not hang on it. The engine
		// reports the real problem at compose time.
		const cyclic = grossMargin({
			steps: [
				{
					type: "formula",
					step_id: "a",
					expression: "b",
					depends_on: ["b"],
					checks: [],
				},
				{
					type: "formula",
					step_id: "b",
					expression: "a",
					depends_on: ["a"],
					checks: [],
				},
			],
			output_step: {
				type: "formula",
				step_id: "out",
				expression: "a + nonexistent",
				depends_on: ["a", "nonexistent"],
				checks: [
					{ condition: "value >= 0", severity: "error", message: "neg" },
				],
			},
		});

		const d = deps(toProposedMetric(cyclic));
		expect(Object.keys(d).sort()).toEqual(["a", "b", "out"]);
		expect(typeof d.out?.level).toBe("number");
	});

	it("maps the parameter list to the name-keyed map a constant resolves from", () => {
		const m = toProposedMetric(
			grossMargin({
				parameters: [
					{
						name: "days_in_period",
						param_type: "integer",
						default: 30,
						description: "Analysis period length",
					},
				],
				steps: [
					REVENUE,
					{
						type: "constant",
						step_id: "days_in_period",
						parameter: "days_in_period",
						checks: [],
					},
				],
			}),
		) as { parameters?: Record<string, unknown> };

		expect(m.parameters).toEqual({
			days_in_period: {
				type: "integer",
				default: 30,
				description: "Analysis period length",
			},
		});
		// The constant references the parameter BY NAME — that link is what
		// `compose_constant_sql` resolves through (there is no inline literal).
		expect(deps(m).days_in_period).toMatchObject({
			type: "constant",
			parameter: "days_in_period",
		});
	});

	it("derives version and output.metric_id rather than spending schema budget", () => {
		const m = toProposedMetric(grossMargin()) as {
			version: string;
			output: Record<string, unknown>;
		};

		expect(m.version).toBe("1.0");
		expect(m.output).toEqual({
			type: "scalar",
			metric_id: "gross_margin",
			unit: "ratio",
			decimal_places: 2,
		});
	});

	it("omits parameters and interpretation entirely when empty", () => {
		const m = toProposedMetric(grossMargin());

		expect(m).not.toHaveProperty("parameters");
		expect(m).not.toHaveProperty("interpretation");
	});

	it("nests interpretation bands under interpretation.ranges when present", () => {
		const m = toProposedMetric(
			grossMargin({
				interpretation_bands: [
					{ min: 0, max: 0.2, label: "LOW", description: "Thin margin" },
				],
			}),
		) as { interpretation?: { ranges: unknown[] } };

		expect(m.interpretation?.ranges).toEqual([
			{ min: 0, max: 0.2, label: "LOW", description: "Thin margin" },
		]);
	});

	it("handles a single-extract metric, where the output IS the only step", () => {
		// 3 of the 16 shipped metrics are exactly this (transaction_count et al),
		// which is why `steps: []` must be legal.
		const d = deps(
			toProposedMetric(
				grossMargin({
					graph_id: "transaction_count",
					steps: [],
					output_step: {
						type: "extract",
						step_id: "transaction_count",
						standard_field: "transaction_count",
						statement: "",
						aggregation: "count",
						checks: [
							{
								condition: "value >= 0",
								severity: "error",
								message: "negative count",
							},
						],
					},
				}),
			),
		);

		expect(Object.keys(d)).toEqual(["transaction_count"]);
		expect(d.transaction_count?.output_step).toBe(true);
		expect(d.transaction_count?.level).toBe(1);
	});

	it("produces a payload that re-parses as the persisted metric shape", () => {
		// The strongest assertion available without the engine: whatever the
		// converter emits must satisfy the SAME schema the teach path writes and
		// the ModelFrame widget round-trips.
		for (const induced of [
			grossMargin(),
			grossMargin({
				parameters: [
					{ name: "d", param_type: "float", default: 1.5, description: "d" },
				],
				interpretation_bands: [
					{ min: 0, max: 1, label: "OK", description: "fine" },
				],
			}),
		]) {
			expect(() =>
				ProposedMetric.parse(toProposedMetric(induced)),
			).not.toThrow();
		}
	});
});
