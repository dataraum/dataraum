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
		steps: [
			REVENUE,
			COST,
			{
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
		],
		output_step_id: "gross_margin",
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
						{
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
					],
				}),
			),
		);

		expect(d.revenue?.level).toBe(1); // leaf extract
		expect(d.gross_profit?.level).toBe(2); // formula over leaves
		expect(d.gross_margin?.level).toBe(3); // formula over a formula
	});

	it("REJECTS a depends_on cycle rather than passing it to the engine", () => {
		// A cycle is not this metric's problem alone: the engine's warm DAG is
		// cross-metric, so `build_warm_dag` raises, `metrics_phase` swallows it and
		// returns an EMPTY authoring map, and every metric in the vertical then
		// honest-fails. Rejecting one induced metric is strictly cheaper.
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
				{
					type: "formula",
					step_id: "out",
					expression: "a",
					depends_on: ["a"],
					checks: [
						{ condition: "value >= 0", severity: "error", message: "neg" },
					],
				},
			],
			output_step_id: "out",
		});

		expect(() => toProposedMetric(cyclic)).toThrow(/dependency cycle/);
	});

	it("REJECTS a duplicate step_id", () => {
		// The map-keyed payload made duplicates impossible at parse time; the array
		// shape reintroduces them, and last-write-wins would silently drop a step
		// and repoint whatever depended on it.
		const dup = grossMargin({
			steps: [REVENUE, { ...COST, step_id: "revenue" }],
		});

		expect(() => toProposedMetric(dup)).toThrow(/repeats step_id 'revenue'/);
	});

	it("tolerates a DANGLING depends_on — that is the engine's to report", () => {
		// Distinct from a cycle: a missing operand fails only this metric, loudly,
		// at compose time. No need to pre-empt it here.
		const dangling = grossMargin({
			steps: [
				REVENUE,
				{
					type: "formula",
					step_id: "gross_margin",
					expression: "revenue - nope",
					depends_on: ["revenue", "nope"],
					checks: [
						{ condition: "value >= 0", severity: "error", message: "neg" },
					],
				},
			],
		});

		expect(() => toProposedMetric(dangling)).not.toThrow();
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
					{
						type: "formula",
						step_id: "gross_margin",
						expression: "revenue * days_in_period",
						depends_on: ["revenue", "days_in_period"],
						checks: [
							{ condition: "value >= 0", severity: "error", message: "neg" },
						],
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
					steps: [
						{
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
					],
					output_step_id: "transaction_count",
				}),
			),
		);

		expect(Object.keys(d)).toEqual(["transaction_count"]);
		expect(d.transaction_count?.output_step).toBe(true);
		expect(d.transaction_count?.level).toBe(1);
	});

	it("REJECTS an output_step_id that names no step", () => {
		// The guarantee the retired `output_step` union held at the grammar level:
		// a dangling id leaves the graph with NO output, and the engine's
		// `get_output_step()` would return nothing at all.
		expect(() =>
			toProposedMetric(grossMargin({ output_step_id: "not_a_step" })),
		).toThrow(/not one of its steps/);
	});

	it("REJECTS a CONSTANT step as the metric's output", () => {
		// The other retired guarantee: `OutputStepInput` offered only extract and
		// formula branches. A metric whose value is a resolved parameter is a
		// literal, not a measurement — 0 of the 16 shipped metrics do it.
		expect(() =>
			toProposedMetric(
				grossMargin({
					steps: [
						REVENUE,
						{
							type: "constant",
							step_id: "days_in_period",
							parameter: "days_in_period",
							checks: [],
						},
					],
					output_step_id: "days_in_period",
				}),
			),
		).toThrow(/not a measurement/);
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

// A shipped metric is the only ground truth available without the engine: if
// the converter can reproduce one, the payload shape is right. `dso` is the
// richest — every step type, a parameter, an output-step check, and levels
// spanning two tiers.
describe("toProposedMetric — reproduces a SHIPPED metric graph", () => {
	const dso: InducedMetric = {
		graph_id: "dso",
		name: "Days Sales Outstanding",
		description: "Average days to collect payment after sale",
		category: "working_capital",
		tags: ["ar", "collection", "working-capital"],
		output_type: "scalar",
		unit: "days",
		decimal_places: 1,
		parameters: [
			{
				name: "days_in_period",
				param_type: "integer",
				default: 30,
				description: "Analysis period length",
			},
		],
		steps: [
			{
				type: "extract",
				step_id: "accounts_receivable",
				standard_field: "accounts_receivable",
				statement: "balance_sheet",
				aggregation: "sum",
				checks: [],
			},
			{
				type: "extract",
				step_id: "revenue",
				standard_field: "revenue",
				statement: "income_statement",
				aggregation: "sum",
				checks: [],
			},
			{
				type: "constant",
				step_id: "days_in_period",
				parameter: "days_in_period",
				checks: [],
			},
			{
				type: "formula",
				step_id: "dso",
				expression: "(accounts_receivable / revenue) * days_in_period",
				depends_on: ["accounts_receivable", "revenue", "days_in_period"],
				checks: [
					{
						condition: "0 <= value <= 365",
						severity: "warning",
						message: "DSO outside typical range",
					},
				],
			},
		],
		output_step_id: "dso",
		interpretation_bands: [
			{
				min: 0,
				max: 30,
				label: "EXCELLENT",
				description: "Very efficient collection",
			},
		],
	};

	it("matches verticals/finance/metrics/working_capital/dso.yaml key for key", () => {
		const payload = toProposedMetric(dso) as Record<string, unknown>;

		expect(payload.graph_id).toBe("dso");
		expect(payload.version).toBe("1.0");
		expect(payload.output).toEqual({
			type: "scalar",
			metric_id: "dso",
			unit: "days",
			decimal_places: 1,
		});
		expect(payload.parameters).toEqual({
			days_in_period: {
				type: "integer",
				default: 30,
				description: "Analysis period length",
			},
		});
		expect(deps(payload)).toEqual({
			accounts_receivable: {
				level: 1,
				type: "extract",
				source: {
					standard_field: "accounts_receivable",
					statement: "balance_sheet",
				},
				aggregation: "sum",
			},
			revenue: {
				level: 1,
				type: "extract",
				source: { standard_field: "revenue", statement: "income_statement" },
				aggregation: "sum",
			},
			days_in_period: {
				level: 1,
				type: "constant",
				parameter: "days_in_period",
			},
			dso: {
				level: 2,
				type: "formula",
				expression: "(accounts_receivable / revenue) * days_in_period",
				depends_on: ["accounts_receivable", "revenue", "days_in_period"],
				output_step: true,
				validation: [
					{
						condition: "0 <= value <= 365",
						severity: "warning",
						message: "DSO outside typical range",
					},
				],
			},
		});
		expect(() => ProposedMetric.parse(payload)).not.toThrow();
	});

	it("differs from the shipped file only in fields the loader defaults or ignores", () => {
		// Documented, deliberate omissions — each verified against loader.py:
		//   metadata.source       -> defaults to "system" (_parse_metadata)
		//   parameters.*.options  -> parsed into ParameterDef.options, zero readers
		//   constant step.default -> read into GraphStep.value, zero readers; a
		//                            constant resolves via `parameter` alone
		const payload = toProposedMetric(dso) as Record<string, unknown>;
		const metadata = payload.metadata as Record<string, unknown>;

		expect(metadata).not.toHaveProperty("source");
		expect(
			(payload.parameters as Record<string, Record<string, unknown>>)
				.days_in_period,
		).not.toHaveProperty("options");
		expect(deps(payload).days_in_period).not.toHaveProperty("default");
	});
});
