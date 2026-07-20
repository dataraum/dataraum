// frame model-induction (DAT-382, DAT-469). The agent `frame` TOOL was removed by
// DAT-597 (acquisition moved to the staging hub); this is now the shared induction
// + overlay-write helper that `server/stage-frame.ts` calls directly.
//
// This is where induction LEAVES the engine (per the agent-tier boundary,
// DD/27688962): the cockpit `frame` stage runs induction on the connect schema
// + samples (DAT-381's `ConnectSchema`) via the TanStack AI SDK +
// `@tanstack/ai-anthropic`, co-designs the user's model with them, and writes the
// declared model to the engine-owned ws_<id> schema (Drizzle metadata client).
// Concepts go to the typed `concepts` table (DAT-728, config→DB — a supersede +
// insert per concept); validations/cycles/metrics still write `config_overlay`
// rows through the `teach` seam.
//
// `frame` frames the WHOLE model in ONE call: the business
// `concepts` AND the executable knowledge over them — `validations` (DAT-469),
// `cycles` (DAT-470), and `metrics` (DAT-471). Each family runs through the
// shared frame-a-family core (frame-family.ts) two ways:
//   - induce: no edited set → the LLM proposes that family's set (validations,
//     cycles, and metrics induce OVER the same-call concepts, seeded with the
//     nearest shipped vertical's specs as structural few-shot).
//   - declare: an edited set → written verbatim, no LLM. This is how the
//     ModelFrame widget's accept/edit round-trips: the agent re-invokes frame
//     with the edited concepts and/or validations and/or cycles and/or metrics.
// Either way each member is persisted as a vertical-tagged overlay row and
// returned for the ModelFrame widget to render.
//
// An acting tool: frame mutates the workspace (writes overlay rows), so it runs
// on the user's explicit instruction — there is no approval gate, exactly like
// `teach`/`replay`.

import { z } from "zod";

import { setActiveWorkspaceVertical } from "#/db/cockpit/registry";
import { ConnectSchema } from "../duckdb/connect";
import {
	getFrameCyclesInstructions,
	getFrameInstructions,
	getFrameMetricsInstructions,
	getFrameValidationsInstructions,
} from "../prompts";
import { AgentActionableError } from "./agent-error";
import { CONCEPT_KINDS, writeConcept } from "./concept-write";
import { CycleSpecSchema, type ShippedCycleSpec } from "./cycle-spec";
import {
	formatSeedExamples,
	frameFamily,
	induceNative,
	induceStructured,
	nearestSeedVertical,
	stripUndefined,
} from "./frame-family";
import { MetricSpecSchema, type ShippedMetricSpec } from "./metric-spec";
import { readShippedCycles } from "./teach-cycle";
import { readShippedMetrics } from "./teach-metric";
import { readShippedValidations } from "./teach-validation";
import {
	type ShippedValidationSpec,
	ValidationSpecSchema,
} from "./validation-spec";

// The fallback vertical when frame isn't given a name — the cold-start ontology
// the engine resolves against when nothing else is declared.
const DEFAULT_VERTICAL = "_adhoc";

// A framed vertical name becomes the engine's `verticals/<name>/ontology.yaml`
// resolution key, so it must be a safe path segment + match the engine's naming
// (lowercase, starts with a letter). `_adhoc` (the leading-underscore default)
// is exempt — it's the built-in fallback, never user-supplied here.
const VERTICAL_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

/** Resolve + validate the vertical concepts are declared under. A blank/absent
 * name falls back to `_adhoc`; a supplied name must be a safe, engine-valid
 * key (it keys `verticals/<name>` config resolution). */
function resolveVertical(name?: string | null): string {
	const trimmed = name?.trim();
	// Blank OR an explicit `_adhoc` → the unnamed default (consistent with
	// select's resolveVertical; `_adhoc`'s leading underscore fails the pattern).
	if (!trimmed || trimmed === DEFAULT_VERTICAL) return DEFAULT_VERTICAL;
	if (!VERTICAL_NAME_PATTERN.test(trimmed)) {
		throw new AgentActionableError(
			`Invalid vertical name '${trimmed}'. Must match ${VERTICAL_NAME_PATTERN.source} ` +
				"(lowercase, start with a letter, 2–49 chars of [a-z0-9_]).",
		);
	}
	return trimmed;
}

// One induced/declared business concept. Mirrors the engine's `OntologyConcept`
// (packages/engine/.../analysis/semantic/ontology.py) MINUS `vertical`, which
// `frame` fixes to the resolved vertical on write. The model fills this via the
// structured-output call, and frame writes it as a typed `concepts` row (DAT-728,
// config→DB — no longer a `concept` overlay teach). `temporal_behavior` was dropped
// (DAT-657): stock/flow is data-determined, not a concept-declared format.
export const ProposedConcept = z.object({
	name: z
		.string()
		.min(1)
		.describe("lowercase_snake_case identifier, e.g. revenue, customer_id"),
	kind: z
		.enum(CONCEPT_KINDS)
		.describe(
			'the concept\'s ontological kind: "measure" (a summable/aggregatable ' +
				'quantity), "entity" (a business object like account or customer), ' +
				'"dimension" (a descriptive axis), or "unit" (defines units for measures)',
		),
	description: z
		.string()
		.optional()
		.describe("one sentence: what this concept represents in business terms"),
	indicators: z
		.array(z.string())
		.optional()
		.describe("column-name substrings that suggest this concept"),
	exclude_patterns: z
		.array(z.string())
		.optional()
		.describe("substrings that should NOT match this concept"),
	unit_from_concept: z
		.string()
		.optional()
		.describe("name of the concept providing this measure's unit"),
});
export type ProposedConcept = z.infer<typeof ProposedConcept>;

// The structured-output shape the concept induction LLM call returns.
const InducedFrame = z.object({
	concepts: z.array(ProposedConcept),
});

// One induced/declared validation. The engine's `ValidationSpec` shape
// (validation-spec.ts, DAT-441) MINUS `vertical`, which `frame` fixes on write —
// exactly as ProposedConcept omits it. The model fills this via structured output;
// the user accepts/edits the set in the ModelFrame widget.
export const ProposedValidation = ValidationSpecSchema.omit({ vertical: true });
export type ProposedValidation = z.infer<typeof ProposedValidation>;

// The structured-output shape the validation induction LLM call returns.
const InducedValidations = z.object({
	validations: z.array(ProposedValidation),
});

// One induced/declared business cycle. The engine's `cycle_types` entry shape
// (cycle-spec.ts, DAT-465) MINUS `vertical`, which `frame` fixes on write —
// exactly as ProposedConcept / ProposedValidation omit it. The model fills this
// via structured output; the user accepts/edits the set in the ModelFrame widget.
export const ProposedCycle = CycleSpecSchema.omit({ vertical: true });
export type ProposedCycle = z.infer<typeof ProposedCycle>;

// The structured-output shape the cycle induction LLM call returns.
const InducedCycles = z.object({
	cycles: z.array(ProposedCycle),
});

// One induced/declared metric — a TransformationGraph (DAT-466's MetricSpecSchema)
// MINUS `vertical`, which `frame` fixes on write, exactly as ProposedConcept /
// ProposedValidation omit it. The model fills this DAG via structured output; the
// user accepts/edits the set in the ModelFrame widget. The leaves are
// CONCEPT-level (`source.standard_field` names a framed concept, not a column) —
// column binding happens later in the semantic phase, SQL composition in
// operating_model (DAT-468/471).
export const ProposedMetric = MetricSpecSchema.omit({ vertical: true });
export type ProposedMetric = z.infer<typeof ProposedMetric>;

// The structured-output shape the metric induction LLM call returns.
const InducedMetrics = z.object({
	metrics: z.array(ProposedMetric),
});

// One written concept + the typed `concepts` row id it landed as (DAT-728 — a
// concept_id, not an overlay_id, now that concepts are typed rows).
const FrameConceptResult = ProposedConcept.extend({
	concept_id: z.string(),
});
export type FrameConceptResult = z.infer<typeof FrameConceptResult>;

// One written validation + the overlay row id it landed as.
const FrameValidationResult = ProposedValidation.extend({
	overlay_id: z.string(),
});
export type FrameValidationResult = z.infer<typeof FrameValidationResult>;

// One written cycle + the overlay row id it landed as.
const FrameCycleResult = ProposedCycle.extend({
	overlay_id: z.string(),
});
export type FrameCycleResult = z.infer<typeof FrameCycleResult>;

// One written metric + the overlay row id it landed as.
const FrameMetricResult = ProposedMetric.extend({
	overlay_id: z.string(),
});
export type FrameMetricResult = z.infer<typeof FrameMetricResult>;

export const FrameResult = z.object({
	vertical: z.string(),
	concepts: z.array(FrameConceptResult),
	// The validations framed over those concepts (DAT-469). Empty for a
	// concepts-only model (the user curated them all away, or none was proposed).
	validations: z.array(FrameValidationResult),
	// The business cycles framed over those concepts (DAT-470). Empty for a model
	// with no cycles (the user curated them all away, or none was proposed).
	cycles: z.array(FrameCycleResult),
	// The metric DAGs framed over those concepts (DAT-471). Empty when none was
	// proposed / all curated away. Each is a TransformationGraph with concept-leaf
	// dependencies; born-loud after execution absorbs a malformed one (never a
	// frame-time gate).
	metrics: z.array(FrameMetricResult),
});
export type FrameResult = z.infer<typeof FrameResult>;

export interface FrameInput {
	schema: ConnectSchema;
	// A user-reviewed / edited concept set. When present (incl. an empty array),
	// these are written verbatim (no induction call) — the accept/edit path of the
	// ModelFrame widget.
	concepts?: ProposedConcept[];
	// A user-reviewed / edited validation set. Same verbatim-declare semantics as
	// `concepts`; absent → validations are induced over the framed concepts.
	validations?: ProposedValidation[];
	// A user-reviewed / edited cycle set. Same verbatim-declare semantics as
	// `concepts`; absent → cycles are induced over the framed concepts.
	cycles?: ProposedCycle[];
	// A user-reviewed / edited metric set. Same verbatim-declare semantics as
	// `concepts`; absent → metric DAGs are induced over the framed concepts.
	metrics?: ProposedMetric[];
	// The vertical to declare the model under (a NEW, framed vertical). The agent
	// proposes a name that fits the data; the user can rename. Omitted → `_adhoc`
	// (the unnamed cold-start fallback). Pass the SAME name to `select`.
	vertical_name?: string | null;
}

/**
 * Induce a business vocabulary from a `ConnectSchema` via one NATIVE
 * structured-output Anthropic call (DAT-807 — the shape is schema-guaranteed by
 * constrained decoding, not parsed out of tool arguments). Returns the proposed
 * concepts; does NOT write anything. Split out so the induction step is testable
 * apart from the DB write. `signal` is the tool-context abort (DAT-449): a
 * stopped run aborts this nested call instead of billing it to completion.
 */
export async function induceConcepts(
	schema: ConnectSchema,
	signal?: AbortSignal,
): Promise<ProposedConcept[]> {
	const { concepts } = await induceNative({
		instructions: getFrameInstructions(),
		userMessage:
			"Propose a domain ontology for the following source. " +
			"Return concepts that cover every table.\n\n" +
			JSON.stringify(schema, null, 2),
		outputSchema: InducedFrame,
		signal,
	});
	return concepts;
}

/**
 * Induce a validation set for a source via one FORCED-TOOL structured-output
 * call — the one family shape native structured output cannot express, because
 * `parameters` is an open map the engine parses as a dict (DAT-807; see
 * `induceStructured`). Induced OVER the framed concept vocabulary — the concepts
 * are part of the context, so the proposed checks anchor to them rather than to
 * guessed column names. The induce
 * prompt is seeded with the nearest shipped vertical's specs as STRUCTURAL
 * few-shot (DAT-468) — the framing that makes the proposed shape reliable.
 * Returns the proposed validations; does NOT write anything. The shipped-spec
 * reader is injectable so the seed wiring is unit-testable without the config
 * tree; production uses the default. `signal` bridges the tool-context abort.
 */
export async function induceValidations(
	schema: ConnectSchema,
	concepts: ProposedConcept[],
	vertical: string,
	signal?: AbortSignal,
	readSeed: (
		v: string,
	) => Promise<ShippedValidationSpec[]> = readShippedValidations,
): Promise<ProposedValidation[]> {
	const seed = await nearestSeedVertical(vertical, readSeed);
	const { validations } = await induceStructured({
		instructions: getFrameValidationsInstructions(),
		userMessage:
			"Propose data-quality and business-rule validations for the following " +
			"source, over the framed concept vocabulary. Only propose checks the data " +
			"can support.\n\n" +
			`<concepts>\n${JSON.stringify(concepts, null, 2)}\n</concepts>\n\n` +
			`<schema>\n${JSON.stringify(schema, null, 2)}\n</schema>\n\n` +
			formatSeedExamples(seed.specs, {
				vertical: seed.vertical,
				family: "validation",
			}),
		outputSchema: InducedValidations,
		signal,
	});
	return validations;
}

/**
 * Induce a business-cycle set for a source via one NATIVE structured-output call
 * (DAT-807), OVER the framed concept vocabulary — the concepts are part of the
 * context, so the proposed cycles anchor to them rather than to guessed column
 * names. The induce prompt
 * is seeded with the nearest shipped vertical's `cycle_types` as STRUCTURAL
 * few-shot (DAT-468/470) — the framing that makes the proposed shape reliable.
 * Returns the proposed cycles; does NOT write anything. The shipped-spec reader is
 * injectable so the seed wiring is unit-testable without the config tree; production
 * uses the default. `signal` bridges the tool-context abort.
 */
export async function induceCycles(
	schema: ConnectSchema,
	concepts: ProposedConcept[],
	vertical: string,
	signal?: AbortSignal,
	readSeed: (v: string) => Promise<ShippedCycleSpec[]> = readShippedCycles,
): Promise<ProposedCycle[]> {
	const seed = await nearestSeedVertical(vertical, readSeed);
	const { cycles } = await induceNative({
		instructions: getFrameCyclesInstructions(),
		userMessage:
			"Propose the business cycles (recurring multi-stage processes) for the " +
			"following source, over the framed concept vocabulary. Only propose cycles " +
			"the data can stage and complete (it has a status/lifecycle column).\n\n" +
			`<concepts>\n${JSON.stringify(concepts, null, 2)}\n</concepts>\n\n` +
			`<schema>\n${JSON.stringify(schema, null, 2)}\n</schema>\n\n` +
			formatSeedExamples(seed.specs, {
				vertical: seed.vertical,
				family: "cycle",
			}),
		outputSchema: InducedCycles,
		signal,
	});
	return cycles;
}

/**
 * Induce a metric-DAG set for a source via one FORCED-TOOL structured-output
 * call — `dependencies` is keyed by the step ids the model invents, so the DAG
 * is an open map native structured output cannot express (DAT-807; see
 * `induceStructured`). Induced OVER the framed concept vocabulary — the concepts
 * are part of the context, so each metric's leaf `extract` steps anchor to framed
 * CONCEPTS rather than to guessed
 * columns — column binding is the semantic phase's job, SQL composition is
 * operating_model's). The induce prompt is seeded with the nearest shipped
 * vertical's metric DAGs as STRUCTURAL few-shot (DAT-468) — flagged explicitly
 * as examples and as the dependency SHAPE to learn, never the formula content to
 * copy, which is what makes DAG induction reliable. Returns the proposed metrics;
 * does NOT write anything (the induced DAG is inspiration, not a frame-time gate
 * — a malformed one still declares + surfaces born-loud after execution). The
 * shipped-spec reader is injectable so the seed wiring is unit-testable without
 * the config tree; production uses the default. `signal` bridges the tool-context
 * abort (DAT-449).
 */
export async function induceMetrics(
	schema: ConnectSchema,
	concepts: ProposedConcept[],
	vertical: string,
	signal?: AbortSignal,
	readSeed: (v: string) => Promise<ShippedMetricSpec[]> = readShippedMetrics,
): Promise<ProposedMetric[]> {
	const seed = await nearestSeedVertical(vertical, readSeed);
	const { metrics } = await induceStructured({
		instructions: getFrameMetricsInstructions(),
		userMessage:
			"Propose metrics — each a small computation DAG over the framed concept " +
			"vocabulary — for the following source. Wire the dependency structure " +
			"correctly; leaves are concepts, not columns. Only propose metrics whose " +
			"leaf concepts the vocabulary contains.\n\n" +
			`<concepts>\n${JSON.stringify(concepts, null, 2)}\n</concepts>\n\n` +
			`<schema>\n${JSON.stringify(schema, null, 2)}\n</schema>\n\n` +
			formatSeedExamples(seed.specs, {
				vertical: seed.vertical,
				family: "metric",
			}),
		outputSchema: InducedMetrics,
		signal,
	});
	return metrics;
}

/**
 * Run the frame stage: resolve the user's whole model — concepts AND the
 * executable knowledge over them (validations + cycles + metric DAGs) — then
 * write each member as a `config_overlay` row via the shared teach seam. Each
 * family independently induces (from the schema) or declares verbatim (a
 * user-edited set), so one `frame` call frames the model.
 * Validations, cycles, and metrics all induce OVER the same-call concepts.
 * Returns the written concepts + validations + cycles + metrics (with overlay
 * ids) for the ModelFrame widget.
 */
export async function frame(
	input: FrameInput,
	signal?: AbortSignal,
): Promise<FrameResult> {
	const schema = ConnectSchema.parse(input.schema);
	const vertical = resolveVertical(input.vertical_name);

	// Concepts are the vocabulary the rest of the model is framed over, so they
	// resolve first. Config→DB (DAT-728): concepts are written as typed `concepts`
	// rows (an edit = supersede active + insert new), NOT `concept` overlay teaches
	// — the engine seeds/reads the same table. Each row's `concept_id` returns for
	// the ModelFrame widget.
	const conceptItems =
		input.concepts !== undefined
			? input.concepts.map((c) => ProposedConcept.parse(c))
			: await induceConcepts(schema, signal);

	if (conceptItems.length === 0) {
		throw new AgentActionableError(
			"Frame induction returned no concepts — nothing to declare.",
		);
	}

	const writtenConcepts: FrameConceptResult[] = [];
	for (const c of conceptItems) {
		const { concept_id } = await writeConcept({ vertical, ...c });
		writtenConcepts.push({ ...c, concept_id });
	}
	const concepts = { items: conceptItems, written: writtenConcepts };

	// Acquire the workspace's vertical (DAT-523) as soon as the concepts are
	// confirmed — BEFORE the optional validation/cycle/metric families — so the
	// workspace is on its real vertical even if a later family write fails. Order
	// matters: writing the vertical LAST would, on a failure, leave a fully-framed
	// model under the named vertical while the workspace still reads `_adhoc`, so
	// the next `select` would silently route `verticals: ["_adhoc"]` and discard
	// it. Writing it here means a partial frame still points the workspace at the
	// right vertical (which now has concepts), and a re-frame is idempotent (overlay
	// + this upsert both upsert). Guarded to a real name: `_adhoc` (the no-frame
	// default) must never overwrite a previously-framed workspace. Authoritative —
	// throws on failure rather than silently desyncing the workspace.
	if (vertical !== DEFAULT_VERTICAL) {
		await setActiveWorkspaceVertical(vertical);
	}

	// Validations are framed over the just-resolved concepts and written as
	// `validation` overlay rows through the SAME teach seam (the engine's
	// _apply_validation upsert-replaces by validation_id, filtered by vertical).
	const validations = await frameFamily<ProposedValidation>({
		teachType: "validation",
		itemSchema: ProposedValidation,
		induce: (sig) => induceValidations(schema, concepts.items, vertical, sig),
		toPayload: (v) => ({ vertical, ...stripUndefined(v) }),
		edited: input.validations,
		signal,
	});

	// Cycles are framed over the just-resolved concepts and written as `cycle`
	// overlay rows through the SAME teach seam (the engine's _apply_cycle
	// upsert-replaces by name into the vertical's cycle_types mapping). Free-form
	// names: the user's words shape WHICH cycle to detect, never HOW it's measured.
	const cycles = await frameFamily<ProposedCycle>({
		teachType: "cycle",
		itemSchema: ProposedCycle,
		induce: (sig) => induceCycles(schema, concepts.items, vertical, sig),
		toPayload: (c) => ({ vertical, ...stripUndefined(c) }),
		edited: input.cycles,
		signal,
	});

	// Metrics are framed over the same concepts and written as `metric` overlay
	// rows through the SAME teach seam (the engine's _apply_metric upsert-replaces
	// by graph_id, filtered by vertical). The payload IS the TransformationGraph
	// (graph_id + metadata + output + the concept-leaf `dependencies` DAG); the
	// next operating_model run declares → composes → executes it, and a malformed
	// graph stays `declared` with a born-loud reason — NEVER gated here (DAT-471).
	const metrics = await frameFamily<ProposedMetric>({
		teachType: "metric",
		itemSchema: ProposedMetric,
		induce: (sig) => induceMetrics(schema, concepts.items, vertical, sig),
		toPayload: (m) => ({ vertical, ...stripUndefined(m) }),
		edited: input.metrics,
		signal,
	});

	return {
		vertical,
		concepts: concepts.written,
		validations: validations.written,
		cycles: cycles.written,
		metrics: metrics.written,
	};
}
