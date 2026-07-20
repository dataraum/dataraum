// Metric (transformation-graph) spec schema + shipped-metric reader for
// `teach_metric` (DAT-466).
//
// Split from `teach-metric.ts` so the schema + the pure shadow-detection are
// importable without booting `config.ts` (the validation-spec / cycle-spec
// precedent). `teach-metric.ts` owns the DB-bound write + the config-tree read;
// this module owns the shape.
//
// The shape MIRRORS one `verticals/<v>/metrics/**/<graph_id>.yaml` file plus its
// identity: `core/overlay._apply_metric` upsert-replaces by `graph_id` into the
// vertical's `metrics` collection, and `graphs.loader.GraphLoader` parses the
// merged dict into a `TransformationGraph`. The graph is a DAG of typed steps
// (extract → formula), so this is the heaviest of the three teach schemas.
//
// GUIDING, not the final validator: the engine's GraphLoader is the source of
// truth for validity (it defaults most fields and raises only on `graph_id` /
// `metadata.name` / bad enums). A malformed taught graph stays `declared` with a
// born-loud parse reason — so this schema steers the model toward a valid graph
// without strictly rejecting shapes the engine would accept (a too-tight schema
// would false-reject valid graphs). Per-step structure rides as a nested object
// with a `type` discriminator PROPERTY, not a top-level discriminated union (the
// cockpit tool-schema rule: Anthropic input_schema must be `type: object`).

import { z } from "zod";

// The step-type vocabulary — the engine's `StepType` (graphs/models.py). A
// metric graph is built from these: `extract` pulls a value from a statement
// (standard_field + aggregation), `formula` combines earlier steps via an
// expression, `constant` is a parameter-derived literal.
export const STEP_TYPES = ["extract", "formula", "constant"] as const;
export type StepType = (typeof STEP_TYPES)[number];

// The output kind — the engine's `OutputType`. Most metrics are `scalar`.
export const OUTPUT_TYPES = ["scalar", "series", "table"] as const;

// One DAG step. Permissive on purpose (see module header): only `type` is
// constrained; the per-type fields are optional with descriptions saying which
// apply to which type. The engine's loader fills defaults and validates.
const GraphStepSchema = z
	.object({
		type: z
			.enum(STEP_TYPES)
			.describe(
				"The step kind: 'extract' (pull a value from a statement via " +
					"standard_field + aggregation), 'formula' (combine earlier steps via " +
					"an expression), 'constant' (a parameter-derived literal).",
			),
		level: z
			.number()
			.int()
			.optional()
			.describe(
				"Dependency level (1 = leaf extracts, 2+ = formulas over lower levels). " +
					"Higher-level steps run after the steps they depend on.",
			),
		source: z
			.object({
				standard_field: z
					.string()
					.optional()
					.describe(
						"The abstract/standard field to extract, e.g. 'revenue' " +
							"(resolved via the workspace's semantic mappings). EXTRACT steps.",
					),
				statement: z
					.string()
					.optional()
					.describe(
						"The financial statement the field lives in, e.g. " +
							"'income_statement', 'balance_sheet'. EXTRACT steps.",
					),
				table: z.string().optional().describe("Optional explicit table name."),
				column: z
					.string()
					.optional()
					.describe("Optional explicit column name."),
			})
			.optional()
			.describe("Where an EXTRACT step pulls its value from."),
		aggregation: z
			.string()
			.optional()
			.describe(
				"How an EXTRACT step aggregates, e.g. 'sum', 'avg'. EXTRACT steps.",
			),
		expression: z
			.string()
			.optional()
			.describe(
				"The arithmetic over earlier step ids, e.g. " +
					"'revenue - cost_of_goods_sold'. FORMULA steps.",
			),
		depends_on: z
			.array(z.string())
			.optional()
			.describe("The step ids this FORMULA step consumes."),
		parameter: z
			.string()
			.optional()
			.describe("The parameter name a CONSTANT step reads its value from."),
		value: z
			.unknown()
			.optional()
			.describe(
				"A literal value for a CONSTANT step (when not parameterised).",
			),
		output_step: z
			.boolean()
			.optional()
			.describe("True on the single step whose result IS the metric's output."),
		// The engine's `GraphStep.validations` (graphs/models.py, DAT-616),
		// enforced by `graphs/verifier.py` against the executed value —
		// execution-pass is not validation. THE KEY IS SINGULAR: the loader reads
		// `data.get("validation")` (loader.py `_parse_step`), so a `validations`
		// key would be dropped on the floor. Shipped metrics declare these on their
		// output step (dpo/dso/dio/current_ratio); until DAT-807 the cockpit mirror
		// had no counterpart, so no taught or induced metric could declare one.
		validation: z
			.array(
				z.object({
					condition: z
						.string()
						.describe(
							"A comparison over the bare name `value`, e.g. 'value >= 0' or " +
								"'0 <= value <= 365'.",
						),
					severity: z
						.string()
						.optional()
						.describe("info | warning | error (the default) | critical."),
					message: z
						.string()
						.optional()
						.describe("What the violation means, in business terms."),
				}),
			)
			.optional()
			.describe(
				"Post-execution checks on this step's value. A violation FLAGS the " +
					"metric born-loud; it never suppresses the number.",
			),
	})
	.describe(
		"One node of the metric's computation DAG. The fields that apply depend on " +
			"`type`: extract → source + aggregation; formula → expression + " +
			"depends_on; constant → parameter or value. Mark the final node " +
			"output_step: true.",
	);

const InterpretationRangeSchema = z.object({
	min: z.number().describe("Inclusive lower bound of this band."),
	max: z.number().describe("Inclusive upper bound of this band."),
	label: z.string().describe("The band label, e.g. 'HEALTHY', 'NEGATIVE'."),
	description: z
		.string()
		.optional()
		.describe("What a value in this band means, in business terms."),
});

// The metric the user declares — a top-level `z.object` so Anthropic's
// input_schema is `type: object`. `vertical` keys the overlay row to the loading
// vertical (the engine applier filters `payload.vertical`); `graph_id` is the
// identity the applier upsert-replaces by.
export const MetricSpecSchema = z.object({
	vertical: z
		.string()
		.min(1)
		.describe(
			"The vertical to declare this metric under — the session's framed " +
				"vertical (e.g. 'finance'). The engine applies the overlay only to a " +
				"matching vertical's metric set.",
		),
	graph_id: z
		.string()
		.min(1)
		.describe(
			"lowercase_snake_case metric identifier, e.g. 'ebitda', 'dso', " +
				"'current_ratio'. Reusing a shipped graph_id OVERRIDES that metric " +
				"(upsert-replace); a new id declares a new metric.",
		),
	version: z
		.string()
		.optional()
		.describe("Optional version label, e.g. '1.0' (defaults to '1.0')."),
	metadata: z
		.object({
			name: z
				.string()
				.min(1)
				.describe(
					"Human-readable metric name, e.g. 'EBITDA', 'Days Sales Outstanding'.",
				),
			description: z
				.string()
				.optional()
				.describe("What the metric measures, in business terms."),
			category: z
				.string()
				.optional()
				.describe(
					"Free-form grouping, e.g. 'profitability', 'liquidity', 'working_capital'.",
				),
			tags: z.array(z.string()).optional().describe("Optional free-form tags."),
		})
		.describe("Metric metadata; `name` is required."),
	output: z
		.object({
			type: z
				.enum(OUTPUT_TYPES)
				.optional()
				.describe("Output shape: 'scalar' (default), 'series', or 'table'."),
			metric_id: z
				.string()
				.optional()
				.describe("Optional output identifier (usually == graph_id)."),
			unit: z
				.string()
				.optional()
				.describe(
					"The value's unit, e.g. 'currency', 'days', 'ratio', 'percent'.",
				),
			decimal_places: z
				.number()
				.int()
				.optional()
				.describe("How many decimal places to display."),
		})
		.optional()
		.describe("What the metric produces (defaults to a scalar)."),
	dependencies: z
		.record(z.string(), GraphStepSchema)
		.describe(
			"The metric's computation DAG, keyed by step id. Leaf 'extract' steps " +
				"pull values; 'formula' steps combine them; exactly one step is the " +
				"output_step. e.g. { revenue: {type:'extract', source:{standard_field:" +
				"'revenue', statement:'income_statement'}, aggregation:'sum'}, margin: " +
				"{type:'formula', expression:'revenue - cost', depends_on:['revenue','cost'], " +
				"output_step:true} }.",
		),
	parameters: z
		.record(z.string(), z.unknown())
		.optional()
		.describe("Optional named parameters CONSTANT steps read from."),
	interpretation: z
		.object({
			ranges: z
				.array(InterpretationRangeSchema)
				.describe("Value bands that classify the metric's result."),
		})
		.optional()
		.describe(
			"Optional interpretation bands — how to read the metric's value " +
				"(e.g. negative/breakeven/positive). The declared meaning, not a current value.",
		),
});
export type MetricSpecInput = z.infer<typeof MetricSpecSchema>;

/** A shipped metric as read off a vertical's `metrics/**​/*.yaml` — the summary
 * fields PLUS the full DAG body (`output` shape + `dependencies` wiring). ONE
 * canonical metric spec for both jobs: the frame SEED needs the structure (the
 * dependency graph IS the knowledge — DAT-468/471), the teach SHADOW needs the
 * `graph_id` match. `output`/`dependencies` stay `unknown` (rule 11) — passed
 * through to the induce prompt / canvas, never inspected here. The lean agent-
 * facing override echo is the `ShippedMetricSummary` view below. */
export interface ShippedMetricSpec {
	graph_id: string;
	name: string | null;
	description: string | null;
	category: string | null;
	output: unknown;
	dependencies: unknown;
}

/** The summary view of a shipped metric — what the teach override RESULT echoes
 * back to the agent (lean: no DAG body, which the human reads via the canvas, not
 * the agent's context). A `Pick` of the canonical spec, not a second shape. */
export type ShippedMetricSummary = Pick<
	ShippedMetricSpec,
	"graph_id" | "name" | "description" | "category"
>;

/** Project the canonical spec to its agent-facing summary. */
export function metricSummary(spec: ShippedMetricSpec): ShippedMetricSummary {
	return {
		graph_id: spec.graph_id,
		name: spec.name,
		description: spec.description,
		category: spec.category,
	};
}

function asString(v: unknown): string | null {
	return typeof v === "string" ? v : null;
}

/** Narrow a parsed metric YAML doc (untrusted shape — rule 11) to a
 * ShippedMetricSpec, or null when it has no `graph_id` (not a metric file). Keeps
 * the summary keys (graph_id + metadata.{name,description,category}) AND the DAG
 * body (`output` + `dependencies`) — the frame seed needs the structure; the
 * shadow affordance just ignores it. Pure — no fs/YAML here, so the reader's I/O
 * stays mockable and this narrowing is unit-tested directly. */
export function narrowShippedMetric(doc: unknown): ShippedMetricSpec | null {
	if (!doc || typeof doc !== "object") return null;
	const raw = doc as Record<string, unknown>;
	const id = asString(raw.graph_id);
	if (!id) return null;
	const metadata =
		raw.metadata && typeof raw.metadata === "object"
			? (raw.metadata as Record<string, unknown>)
			: {};
	return {
		graph_id: id,
		name: asString(metadata.name),
		description: asString(metadata.description),
		category: asString(metadata.category),
		output: raw.output ?? null,
		dependencies: raw.dependencies ?? null,
	};
}

/**
 * Detect whether `graphId` shadows a shipped metric in `shipped`. Pure (no I/O),
 * so the override-vs-new decision is unit-tested directly; the tool supplies the
 * list from the config-tree read. An exact id match → the overlay upsert-
 * replaces the shipped metric (a VISIBLE override); no match → a fresh
 * declaration.
 */
export function findShadowedMetric(
	shipped: ShippedMetricSpec[],
	graphId: string,
): ShippedMetricSpec | null {
	return shipped.find((m) => m.graph_id === graphId) ?? null;
}
