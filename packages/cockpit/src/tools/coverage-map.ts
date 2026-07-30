// The operating-model coverage map — a per-dimension lit/partial/dark read (DAT-855
// B2), the /operating-model page's third lens alongside the metric graph and the bus
// matrix. Where the bus matrix asks "which facts share a dimension", this asks "which
// side of the operating model does a REAL, GROUNDED metric actually cover" — the six
// `dimension_facet` axes a concept or metric classifies onto (DAT-855: demand, offer,
// supply, capacity, throughput, capital; `cross_cutting` is a positive classification
// that belongs to no single axis and is deliberately never a row here).
//
// This module is PURE (no DB, no IO) and carries the tests; `coverage-map-load.ts` is
// the thin Drizzle glue. Design choices worth naming:
//
//  1. State is decided per METRIC, never per concept. A concept (e.g. `revenue`) can
//     carry a facet with zero metrics declared against it — that is real, and renders
//     as dark ("no demand unit metric declared") rather than borrowing the concept's
//     own grounding to paper over the gap. Concepts feed ONLY the unclassified count.
//  2. "Grounded" is the lifecycle-artifact state, not a snippet-presence probe: state
//     `declared` IS "never grounded" by the engine's own vocabulary
//     (`lifecycle/db_models.py` ArtifactState), so gating on it needs no inference.
//  3. Per-metric grounding EVIDENCE (a failed extract's reason, its resolved period) is
//     read from `sql_snippets` rows sourced `graph:<this metric's graph_id>` — the
//     shape `graphs/agent.py::_save_composed_snippets`/`_save_snippets` write. This is
//     a known v1 approximation: the cross-metric warm pass (DAT-629/DAT-636) can source
//     a REUSED extract to whichever sibling metric warmed it first, and a metric's own
//     authoring pass can incidentally leave an unrelated extract under its prefix — so
//     this cell can occasionally miss or over-attribute one metric's evidence to
//     another. Getting this exact would mean parsing each metric's persisted DAG
//     (`graph_definition`) to resolve its OWN extract standard_fields, which
//     `operating-model-graph.ts` already does for the canvas; deliberately not pulled
//     in here so a facet-coverage read stays independent of that heavier machinery.
//     "The AVAILABLE text, not an invented taxonomy" — a later lane can tighten this.
//  4. Every non-lit cell names WHY (the bus-matrix `blockedReason` discipline): a row
//     is never rendered dark or partial without a reason a practitioner can act on.
//  5. Reason TEXT is stripped of src digests HERE, at assembly, not in the loader —
//     unlike `operating-model-load.ts` (which strips a plain pass-through field), this
//     module is where the failure text is first EXTRACTED from raw `provenance` json,
//     so stripping anywhere else would mean re-deriving the same narrowing twice.
//     `stripSrcDigests` is pure (no DB/React — `lib/display-names.ts`'s own header),
//     so importing it here does not compromise this module's DB/IO-free discipline.

import { stripSrcDigests } from "../lib/display-names";

/** The six operating-model axes rendered as rows, in the engine's own declared order
 *  (`analysis/semantic/db_models.py::DimensionFacet`) — a value-chain sequence, not an
 *  alphabetical or state-ranked one. `cross_cutting` is the enum's seventh member and
 *  is intentionally absent: it is a POSITIVE "spans every facet" classification, not
 *  one axis among the six. */
export const COVERAGE_DIMENSIONS = [
	{ key: "demand", label: "Demand" },
	{ key: "offer", label: "Offer" },
	{ key: "supply", label: "Supply" },
	{ key: "capacity", label: "Capacity" },
	{ key: "throughput", label: "Throughput" },
	{ key: "capital", label: "Capital" },
] as const;

export type CoverageDimension = (typeof COVERAGE_DIMENSIONS)[number]["key"];

export type CoverageState = "lit" | "partial" | "dark";

/**
 * Why a cell (or a metric within it) isn't lit. `kind` is an optional, informal hint
 * for a later typed upgrade to key on ("do not invent a taxonomy" — the owner ruling
 * this shape exists to honor); `text` is always the thing a practitioner reads, and is
 * always present.
 */
export interface CoverageReason {
	kind?: string;
	text: string;
}

// --- Builder input (plain rows, so the assembly is DB-free and testable) ----

/** One `metrics` config row — the declared population for a facet, independent of
 *  whether the operating_model stage ever ran for it. */
export interface CoverageMetricInput {
	graphId: string;
	name: string;
	dimensionFacet: string | null;
}

/** One `concepts` config row. Read ONLY for the unclassified count — coverage state
 *  is decided per metric (see module header, point 1). */
export interface CoverageConceptInput {
	name: string;
	kind: string;
	dimensionFacet: string | null;
}

/** One `current_lifecycle_artifacts` row, `artifact_type='metric'`. `graphId` is the
 *  row's `artifact_key`. Absent from this list ⇒ never even declared into a run. */
export interface CoverageLifecycleInput {
	graphId: string;
	state: string | null;
	stateReason: string | null;
}

/** One `sql_snippets` row sourced `graph:<graphId>` (any snippet type present under
 *  that prefix) — the metric's own grounding evidence (module header, point 3).
 *  `provenance` is narrowed HERE rather than in the loader (mirrors `operating-
 *  model-graph.ts`'s own `unknown`-at-the-boundary discipline: the pure module owns
 *  parsing untrusted json, the loader stays thin glue). */
export interface CoverageGroundingInput {
	graphId: string;
	snippetType: string;
	/** `failure_count > 0` — the engine's own accept/reject signal. */
	failed: boolean;
	/** Raw `sql_snippets.provenance` — a `FailedSnippetProvenance` shape
	 *  (`{failure_mode, failure_reason}`) when `failed`, a `HealthySnippetProvenance`
	 *  shape otherwise. `unknown` at the boundary; narrowed by `failureReasonOf`. */
	provenance: unknown;
	/** `current_groundings.resolved_period` — display only, never a state input. */
	resolvedPeriod: string | null;
	/** `current_groundings.calendar_source` — display only, never a state input. */
	calendarSource: string | null;
}

/** One `current_concept_reconciliation` row. Only SELF-LOOP rows
 *  (`fromConcept === toConcept`) speak to "the same concept's own groundings agree" —
 *  the spec's stated scenario; a partner-edge row (two DIFFERENT concepts tying out)
 *  is a different claim and is filtered out by the loader before this. */
export interface CoverageReconciliationInput {
	concept: string;
	status: string | null;
	verdict: string | null;
}

export interface CoverageMapInput {
	metrics: CoverageMetricInput[];
	concepts: CoverageConceptInput[];
	lifecycle: CoverageLifecycleInput[];
	groundings: CoverageGroundingInput[];
	reconciliation: CoverageReconciliationInput[];
}

// --- Output ------------------------------------------------------------------

/** One metric's own contribution to its facet's row. Only GROUNDED metrics
 *  (lifecycle state past `declared`) appear at all — an undeclared/never-run metric of
 *  this facet contributes nothing to render (module header, point 2). */
export interface CoverageMetric {
	graphId: string;
	name: string;
	/** The lifecycle_artifacts state, verbatim (`declared`/`grounded`/`executed`/
	 *  `canonical`), or null when no lifecycle row exists at all. Never null when
	 *  the metric appears here, since appearing at all requires a non-declared state. */
	state: string | null;
	/** Whether THIS metric individually satisfies the lit test. */
	lit: boolean;
	/** Why not, when `lit` is false. Always present in that case. */
	reason: CoverageReason | null;
	resolvedPeriod: string | null;
	calendarSource: string | null;
}

export interface CoverageRow {
	dimension: CoverageDimension;
	label: string;
	state: CoverageState;
	/** Every grounded metric of this facet (lit or not) — "the metrics that ground
	 *  it". Empty for a dark row: nothing grounded, so nothing to list. */
	metrics: CoverageMetric[];
	/** Why the row isn't lit — set for partial and dark, null for lit (never
	 *  ambiguous: a lit row has at least one metric with `reason: null`). */
	reason: CoverageReason | null;
}

export interface CoverageMap {
	/** Always exactly the six `COVERAGE_DIMENSIONS`, in that fixed order. */
	rows: CoverageRow[];
	/** Config rows with a NULL facet — "no writer has classified this yet"
	 *  (`DimensionFacet`'s own docstring), rendered as a badge line, never a row. A
	 *  `cross_cutting` metric/concept is NOT unclassified (it is a positive
	 *  classification outside the six axes) and is excluded from this count too. */
	unclassified: { metrics: number; concepts: number };
}

// --- Reason text -------------------------------------------------------------

const EXPECTATION_VIOLATION_MARKER = "declared expectation not met";

/** Narrow `sql_snippets.provenance` for its failure text. `FailedSnippetProvenance`
 *  (`graphs/models.py`) is the only shape carrying `failure_reason`; a healthy row's
 *  provenance has no such key and this returns null. */
function failureReasonOf(provenance: unknown): string | null {
	if (typeof provenance !== "object" || provenance === null) return null;
	const reason = (provenance as Record<string, unknown>).failure_reason;
	return typeof reason === "string" && reason.length > 0 ? reason : null;
}

/** Rank for picking the "most informative" reason among several candidates — lower
 *  sorts first. An unrecognized/absent `kind` ranks last, never crashes a comparison. */
const REASON_KIND_RANK: Record<string, number> = {
	expectation_violated: 0,
	state_reason: 1,
	failed_grounding: 2,
	disagreement: 3,
	not_executed: 4,
	unknown: 5,
};
function reasonRank(reason: CoverageReason): number {
	return REASON_KIND_RANK[reason.kind ?? "unknown"] ?? 99;
}

/** The most informative of several non-null reasons, or null given none. Ties break on
 *  text so the pick is deterministic across renders. */
function mostInformativeReason(
	reasons: CoverageReason[],
): CoverageReason | null {
	if (reasons.length === 0) return null;
	return [...reasons].sort(
		(a, b) => reasonRank(a) - reasonRank(b) || a.text.localeCompare(b.text),
	)[0];
}

/**
 * Why one grounded-but-not-lit metric isn't lit. Priority (most informative first,
 * per the spec): the lifecycle `state_reason` (covers both an expectation violation
 * and a low-confidence-grounding flag — both are joined into the same field by the
 * engine); else a failed grounding's `failure_reason`; else a bare "it failed" when no
 * reason text survived; else a reconciliation disagreement; else a generic
 * state-based fallback.
 */
function metricReason(
	stateReason: string | null,
	state: string | null,
	groundingRows: CoverageGroundingInput[],
	hasDisagreement: boolean,
): CoverageReason {
	if (stateReason) {
		// The marker check runs on the RAW text — a src digest never overlaps this
		// substring, so stripping first would only cost a redundant pass.
		const kind = stateReason.includes(EXPECTATION_VIOLATION_MARKER)
			? "expectation_violated"
			: "state_reason";
		return { kind, text: stripSrcDigests(stateReason) };
	}
	const failedWithReason = groundingRows.find(
		(g) => g.failed && failureReasonOf(g.provenance) !== null,
	);
	if (failedWithReason) {
		return {
			kind: "failed_grounding",
			text: stripSrcDigests(
				// biome-ignore lint/style/noNonNullAssertion: filtered by failureReasonOf above
				failureReasonOf(failedWithReason.provenance)!,
			),
		};
	}
	if (groundingRows.some((g) => g.failed)) {
		return {
			kind: "failed_grounding",
			text: "a grounding attempt for this metric failed",
		};
	}
	if (hasDisagreement) {
		return {
			kind: "disagreement",
			text: "reconciled groundings for this concept disagree",
		};
	}
	if (state === "grounded") {
		return { kind: "not_executed", text: "grounded but not yet executed" };
	}
	return {
		kind: "unknown",
		text: `stuck at lifecycle state '${state ?? "unknown"}'`,
	};
}

// --- Row assembly --------------------------------------------------------------

function buildRow(
	dimension: CoverageDimension,
	label: string,
	metricsForFacet: CoverageMetricInput[],
	lifecycleByGraphId: ReadonlyMap<string, CoverageLifecycleInput>,
	groundingsByGraphId: ReadonlyMap<string, CoverageGroundingInput[]>,
	reconciliationByConcept: ReadonlyMap<string, CoverageReconciliationInput[]>,
): CoverageRow {
	if (metricsForFacet.length === 0) {
		return {
			dimension,
			label,
			state: "dark",
			metrics: [],
			reason: {
				kind: "no_metric_declared",
				text: `no ${dimension} unit metric declared`,
			},
		};
	}

	const metrics: CoverageMetric[] = [];
	const sorted = [...metricsForFacet].sort((a, b) =>
		a.graphId.localeCompare(b.graphId),
	);
	for (const m of sorted) {
		const lifecycle = lifecycleByGraphId.get(m.graphId) ?? null;
		const state = lifecycle?.state ?? null;
		// `declared` (or no lifecycle row at all) IS "never grounded" — the engine's
		// own ArtifactState vocabulary, not an inferred threshold (module header, 2).
		if (state === null || state === "declared") continue;

		const groundingRows = groundingsByGraphId.get(m.graphId) ?? [];
		const anyFailedGrounding = groundingRows.some((g) => g.failed);
		const stateReason = lifecycle?.stateReason ?? null;
		const hasExpectationViolation =
			stateReason?.includes(EXPECTATION_VIOLATION_MARKER) ?? false;
		const reconciliationRows = reconciliationByConcept.get(m.graphId) ?? [];
		const hasDisagreement = reconciliationRows.some(
			(r) => r.status === "evaluated" && r.verdict === "beyond_tolerance",
		);

		const lit =
			state === "executed" &&
			!hasExpectationViolation &&
			!anyFailedGrounding &&
			!hasDisagreement;

		const period = groundingRows.find((g) => g.resolvedPeriod !== null);
		const calendar = groundingRows.find((g) => g.calendarSource !== null);

		metrics.push({
			graphId: m.graphId,
			name: m.name,
			state,
			lit,
			reason: lit
				? null
				: metricReason(stateReason, state, groundingRows, hasDisagreement),
			resolvedPeriod: period?.resolvedPeriod ?? null,
			calendarSource: calendar?.calendarSource ?? null,
		});
	}

	if (metrics.length === 0) {
		return {
			dimension,
			label,
			state: "dark",
			metrics: [],
			reason: {
				kind: "declared_not_grounded",
				text: "declared but never grounded",
			},
		};
	}

	if (metrics.some((m) => m.lit)) {
		return { dimension, label, state: "lit", metrics, reason: null };
	}

	const reason = mostInformativeReason(
		metrics.map((m) => m.reason).filter((r): r is CoverageReason => r !== null),
	);
	return { dimension, label, state: "partial", metrics, reason };
}

export function buildCoverageMap(input: CoverageMapInput): CoverageMap {
	const lifecycleByGraphId = new Map(
		input.lifecycle.map((l) => [l.graphId, l]),
	);

	const groundingsByGraphId = new Map<string, CoverageGroundingInput[]>();
	for (const g of input.groundings) {
		const list = groundingsByGraphId.get(g.graphId) ?? [];
		list.push(g);
		groundingsByGraphId.set(g.graphId, list);
	}

	const reconciliationByConcept = new Map<
		string,
		CoverageReconciliationInput[]
	>();
	for (const r of input.reconciliation) {
		const list = reconciliationByConcept.get(r.concept) ?? [];
		list.push(r);
		reconciliationByConcept.set(r.concept, list);
	}

	const rows = COVERAGE_DIMENSIONS.map(({ key, label }) =>
		buildRow(
			key,
			label,
			input.metrics.filter((m) => m.dimensionFacet === key),
			lifecycleByGraphId,
			groundingsByGraphId,
			reconciliationByConcept,
		),
	);

	return {
		rows,
		unclassified: {
			metrics: input.metrics.filter((m) => m.dimensionFacet === null).length,
			concepts: input.concepts.filter((c) => c.dimensionFacet === null).length,
		},
	};
}
