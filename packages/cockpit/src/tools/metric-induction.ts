// The LLM-FACING metric-induction schema + its conversion to the overlay
// payload (DAT-807).
//
// WHY THIS MODULE EXISTS — two different concerns, deliberately separated:
//
//   1. `MetricSpecSchema` (metric-spec.ts) IS the persisted payload. Its shape
//      is the on-disk `verticals/<v>/metrics/**/<graph_id>.yaml` shape that the
//      engine's `GraphLoader._parse_graph` reads: `dependencies` is a MAP keyed
//      by step id, `parameters` is a MAP keyed by parameter name. 16 shipped
//      metric YAMLs and the `teach_metric` tool contract depend on it.
//   2. Anthropic constrained decoding (`output_config.format`) requires
//      `additionalProperties: false` on every object, which makes an OPEN MAP
//      inexpressible. So the schema the MODEL fills cannot be shape (1).
//
// Conflating the two is what blocked this site on the forced-tool path. The fix
// is to stop conflating them: the model fills the ARRAY-shaped schema below,
// and `toProposedMetric` converts it to the map-shaped payload at ONE boundary
// (`induceMetrics` in frame.ts). The engine, `GraphLoader`, `core/overlay.py`,
// and every shipped YAML stay untouched.
//
// SCHEMA BUDGET (Anthropic constrained decoding): 24 optional properties, 16
// union-typed properties, no recursion. Every field below is REQUIRED — an
// optional renders as `type: [T, "null"]` and spends from BOTH budgets, so the
// rule (lead ruling, DAT-807) is: nothing consumes it -> CUT; something consumes
// it -> REQUIRED with a documented sentinel. Measured: 0 optional / 2 union.
//
// NO `.min()` / `.max()` ANYWHERE: `minLength` / `maxLength` / `minimum` are
// rejected by the grammar compiler. The ONE exception the API documents is
// array `minItems` with value 0 or 1 — which is exactly what the output step's
// `checks` uses.

import { z } from "zod";

import { type MetricSpecInput, OUTPUT_TYPES } from "./metric-spec";
import { SEVERITIES } from "./validation-spec";

// The conversion target: the persisted metric shape MINUS `vertical`, which
// `frame` fixes on write. Structurally identical to frame.ts's `ProposedMetric`
// (`MetricSpecSchema.omit({ vertical: true })`) but taken from metric-spec.ts
// so this module — like metric-spec.ts itself — stays importable without
// booting `config.ts`, and so there is no import cycle with frame.ts.
type ProposedMetric = Omit<MetricSpecInput, "vertical">;

// How an EXTRACT step aggregates. Closed on purpose: the engine treats
// `aggregation` as a free string (loader.py:226) but it keys the extract
// snippet cache (agent.py:1495), so a paraphrase silently forks the cache.
// `end_of_period` is deliberately ABSENT even though the engine accepts it —
// the shipped catalogue's own comment is "period axis (all vs latest) is
// data-reconciled via target_type — never hardcode end_of_period".
export const AGGREGATIONS = [
	"sum",
	"avg",
	"min",
	"max",
	"count",
	"count_distinct",
] as const;

// A declared post-execution check on a step's value — the cockpit mirror of the
// engine's `StepValidation` (graphs/models.py:88, DAT-616), which
// `graphs/verifier.py:117` evaluates against the executed value: execution-pass
// is not validation. Until now the cockpit had NO counterpart, so no induced or
// taught metric could declare a check while shipped ones (dpo/dso/dio/
// current_ratio) do. All three fields required — zero optionals, zero unions.
// `z.strictObject`, not `z.object`: the adapter's structured-output normalizer
// (`makeStructuredOutputCompatible`) only recurses into `type: "object"` and
// `type: "array"` nodes — it does NOT descend into union branches, so an object
// nested inside one never receives `additionalProperties: false` from the
// adapter and the API rejects it. Zod emits it directly for a strict object.
// Every object in this module is therefore strict.
const StepCheck = z.strictObject({
	condition: z
		.string()
		.describe(
			"A comparison over the bare name `value`, e.g. 'value >= 0' or " +
				"'0 <= value <= 365'. Python comparison syntax over numeric literals " +
				"only — no SQL, no function calls, no 'and'/'or'.",
		),
	severity: z
		.enum(SEVERITIES)
		.describe(
			"How bad a violation is: info | warning | error | critical. A violation " +
				"FLAGS the executed metric, it never suppresses the number.",
		),
	message: z
		.string()
		.describe(
			"What the violation means in business terms, e.g. 'DSO outside typical range'.",
		),
});

// The three step kinds, as a discriminated union nested INSIDE the steps array.
// Not a root union — the root schema must stay `type: object`; a nested `anyOf`
// is supported. Each variant requires EXACTLY what the engine's composer reads,
// which is why the union beats the old flattened shape: `GraphStepSchema` had 13
// nullable fields because every per-type field had to be optional.
const extractShape = {
	type: z.literal("extract"),
	step_id: z
		.string()
		.describe(
			"lowercase_snake_case id for this step, e.g. 'revenue'. Formula steps " +
				"reference it by this exact id.",
		),
	standard_field: z
		.string()
		.describe(
			"The CONCEPT this leaf pulls — a name from the framed concept vocabulary, " +
				"e.g. 'revenue'. NEVER a column name: the engine binds the concept to a " +
				"real column later, in the semantic phase.",
		),
	statement: z
		.string()
		.describe(
			"The grouping the field lives in, e.g. 'income_statement', " +
				"'balance_sheet'. Use \"\" when the vertical has no statement notion.",
		),
	aggregation: z
		.enum(AGGREGATIONS)
		.describe("How this leaf aggregates across the matched rows."),
};

const formulaShape = {
	type: z.literal("formula"),
	step_id: z
		.string()
		.describe("lowercase_snake_case id for this step, e.g. 'gross_margin'."),
	expression: z
		.string()
		.describe(
			"Arithmetic over the step ids in depends_on, e.g. " +
				"'(accounts_receivable / revenue) * days_in_period'. CLOSED grammar: " +
				"step ids, numeric literals, + - * /, unary minus, parentheses. " +
				"Anything else (a function call, an unknown name) fails to compose.",
		),
	depends_on: z
		.array(z.string())
		.describe(
			"The step ids this formula consumes. Every identifier in `expression` " +
				"MUST appear here, or the engine refuses to compose a fabricated operand.",
		),
};

const constantShape = {
	type: z.literal("constant"),
	step_id: z
		.string()
		.describe("lowercase_snake_case id for this step, e.g. 'days_in_period'."),
	parameter: z
		.string()
		.describe(
			"The name of an entry in this metric's `parameters` list. A constant is " +
				"resolved SOLELY from the graph parameter's default " +
				"(graphs/agent.py:601) — there is no inline literal.",
		),
};

const checksField = z
	.array(StepCheck)
	.describe(
		"Post-execution checks on this step's value. [] when the step needs none.",
	);

/** One dependency step. `checks` may be empty here — only the output step must
 * declare one (see `OutputStepInput`).
 *
 * `z.union`, NOT `z.discriminatedUnion`: Zod renders a discriminated union as
 * `oneOf`, and Anthropic's constrained decoding supports `anyOf` / `allOf` only
 * — `oneOf` is not in the accepted keyword set. `z.union` renders `anyOf`, and
 * the `type` literal in every branch keeps the discriminator the model needs
 * (it becomes a `const` in each branch). */
const GraphStepInput = z.union([
	z.strictObject({ ...extractShape, checks: checksField }),
	z.strictObject({ ...formulaShape, checks: checksField }),
	z.strictObject({ ...constantShape, checks: checksField }),
]);

// The output step is structurally separate from the dependency steps rather
// than a `output_step: true` flag inside the array. Two things fall out for
// free that a boolean cannot express: EXACTLY ONE output step (the engine's
// `get_output_step()` returns the first hit, so zero or two is a silent bug),
// and a MANDATORY check on it (`minItems: 1` — the one array constraint
// constrained decoding accepts).
//
// CONSTANT is absent by design: a metric whose output is a resolved parameter
// is a literal, not a measurement — 0 of the 16 shipped metrics do it. It stays
// fully available as a DEPENDENCY step (4 shipped uses).
const outputChecksField = z
	.array(StepCheck)
	.min(1)
	.describe(
		"At least ONE post-execution check on the metric's own value — the range " +
			"or sign the result must satisfy for the number to be believable.",
	);

const OutputStepInput = z.union([
	z.strictObject({ ...extractShape, checks: outputChecksField }),
	z.strictObject({ ...formulaShape, checks: outputChecksField }),
]);

// A user-configurable parameter CONSTANT steps read from. `default` is
// `z.number()` because `compose_constant_sql` raises on a non-numeric value,
// and all 4 shipped constants resolve to an integer (days_in_period: 30).
const MetricParameterInput = z.strictObject({
	name: z
		.string()
		.describe(
			"lowercase_snake_case parameter name, e.g. 'days_in_period'. A constant " +
				"step's `parameter` field must match this exactly.",
		),
	param_type: z
		.enum(["integer", "float"])
		.describe(
			"The value's numeric kind. Only numeric parameters exist: a constant " +
				"step's SQL is a numeric literal.",
		),
	default: z
		.number()
		.describe("The value used when the user overrides nothing."),
	description: z.string().describe("What the parameter controls, in one line."),
});

const InterpretationBandInput = z.strictObject({
	min: z.number().describe("Inclusive lower bound of this band."),
	max: z.number().describe("Inclusive upper bound of this band."),
	label: z.string().describe("The band label, e.g. 'HEALTHY', 'CRITICAL'."),
	description: z
		.string()
		.describe("What a value in this band means, in business terms."),
});

/** One induced metric, shaped for constrained decoding. Flatter than the
 * payload (no `metadata` / `output` sub-objects) because the converter re-nests
 * them — the model gets a flat field list, the engine gets its YAML shape. */
export const InducedMetric = z.strictObject({
	graph_id: z
		.string()
		.describe(
			"lowercase_snake_case metric identifier, e.g. 'ebitda', 'current_ratio'. " +
				"Reusing a shipped graph_id OVERRIDES that metric.",
		),
	name: z
		.string()
		.describe("Human-readable metric name, e.g. 'Days Sales Outstanding'."),
	description: z
		.string()
		.describe("What the metric measures, in business terms."),
	category: z
		.string()
		.describe("Free-form grouping, e.g. 'profitability', 'liquidity'."),
	tags: z.array(z.string()).describe("Free-form tags; [] when none apply."),
	output_type: z
		.enum(OUTPUT_TYPES)
		.describe(
			"What the metric produces — 'scalar' unless it truly is a series/table.",
		),
	unit: z
		.string()
		.describe("The value's unit, e.g. 'currency', 'days', 'ratio', 'percent'."),
	// `z.number()`, not `z.int()`: Zod renders an integer type with safe-integer
	// `minimum`/`maximum` bounds, and numerical constraints are rejected by the
	// grammar compiler. The whole-number expectation rides in the description;
	// the engine takes whatever it gets (`decimal_places` is display-only).
	decimal_places: z
		.number()
		.describe(
			"How many decimal places to display — a whole number, e.g. 1 for days, " +
				"2 for a ratio, 0 for a count.",
		),
	parameters: z
		.array(MetricParameterInput)
		.describe(
			"Named parameters CONSTANT steps read from. [] when the metric has no " +
				"constant step.",
		),
	steps: z
		.array(GraphStepInput)
		.describe(
			"The DEPENDENCY steps feeding the output — leaf 'extract' steps pull " +
				"concepts, 'formula' steps combine earlier steps, 'constant' steps " +
				"resolve a parameter. [] when the metric is a single extract.",
		),
	output_step: OutputStepInput.describe(
		"The step whose result IS the metric's value. Exactly one, and it must " +
			"declare at least one check.",
	),
	interpretation_bands: z
		.array(InterpretationBandInput)
		.describe(
			"Value bands classifying the result (the declared MEANING, not a current " +
				"value). [] when the metric has no well-known benchmarks.",
		),
});
export type InducedMetric = z.infer<typeof InducedMetric>;

/** The structured-output shape the metric induction returns. */
export const InducedMetrics = z.strictObject({
	metrics: z.array(InducedMetric),
});

type AnyStep = z.infer<typeof GraphStepInput> | z.infer<typeof OutputStepInput>;

/** One step -> its entry in the payload's `dependencies` map. The step's
 * `checks` land under the key `validation` (SINGULAR) because that is what
 * `GraphLoader._parse_step` reads (loader.py:210) — `validations` would be
 * silently dropped. `level` is the topological depth, computed here rather than
 * asked of the model: the engine ignores it (nothing in `_parse_step` reads it)
 * but the ModelFrame DAG view renders it. */
function stepPayload(
	step: AnyStep,
	level: number,
	outputStep: boolean,
): Record<string, unknown> {
	const base: Record<string, unknown> = {
		level,
		type: step.type,
		...(step.checks.length > 0 ? { validation: step.checks } : {}),
		...(outputStep ? { output_step: true } : {}),
	};
	if (step.type === "extract") {
		return {
			...base,
			source: {
				standard_field: step.standard_field,
				statement: step.statement,
			},
			aggregation: step.aggregation,
		};
	}
	if (step.type === "formula") {
		return {
			...base,
			expression: step.expression,
			depends_on: step.depends_on,
		};
	}
	return { ...base, parameter: step.parameter };
}

/** Topological depth per step id: a leaf (extract/constant) is 1, a formula is
 * 1 + the deepest step it consumes. Purely derived from `depends_on`, so a
 * cyclic or dangling reference cannot loop — an unresolvable step settles at 1
 * and the engine's composer reports the real problem at execution time. */
function stepLevels(steps: AnyStep[]): Map<string, number> {
	const byId = new Map(steps.map((s) => [s.step_id, s]));
	const levels = new Map<string, number>();
	const resolve = (id: string, seen: Set<string>): number => {
		const cached = levels.get(id);
		if (cached !== undefined) return cached;
		const step = byId.get(id);
		if (step === undefined || step.type !== "formula" || seen.has(id)) return 1;
		seen.add(id);
		const level =
			1 + Math.max(0, ...step.depends_on.map((d) => resolve(d, new Set(seen))));
		levels.set(id, level);
		return level;
	};
	for (const s of steps) levels.set(s.step_id, resolve(s.step_id, new Set()));
	return levels;
}

/**
 * THE CONVERSION BOUNDARY: induced (array) shape -> overlay payload (map) shape.
 *
 * This is the ONLY place the two shapes meet. Everything downstream —
 * `frameFamily`'s declare path, the `teach` write, the ModelFrame widget's
 * accept/edit round-trip, the engine's `_apply_metric` — sees only
 * `ProposedMetric`, exactly as it did before DAT-807.
 *
 * Derived rather than asked of the model (no LLM judgment, no schema cost):
 * `version` (the loader defaults it), `output.metric_id` (== graph_id), and
 * each step's `level`.
 */
export function toProposedMetric(induced: InducedMetric): ProposedMetric {
	const all: AnyStep[] = [...induced.steps, induced.output_step];
	const levels = stepLevels(all);
	const dependencies: Record<string, unknown> = {};
	for (const step of all) {
		dependencies[step.step_id] = stepPayload(
			step,
			levels.get(step.step_id) ?? 1,
			step.step_id === induced.output_step.step_id,
		);
	}

	const parameters: Record<string, unknown> = {};
	for (const p of induced.parameters) {
		parameters[p.name] = {
			type: p.param_type,
			default: p.default,
			description: p.description,
		};
	}

	return {
		graph_id: induced.graph_id,
		version: "1.0",
		metadata: {
			name: induced.name,
			description: induced.description,
			category: induced.category,
			tags: induced.tags,
		},
		output: {
			type: induced.output_type,
			metric_id: induced.graph_id,
			unit: induced.unit,
			decimal_places: induced.decimal_places,
		},
		dependencies,
		...(Object.keys(parameters).length > 0 ? { parameters } : {}),
		...(induced.interpretation_bands.length > 0
			? { interpretation: { ranges: induced.interpretation_bands } }
			: {}),
	} as ProposedMetric;
}
