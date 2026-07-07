// Metric value-vs-ground-truth comparison — the pure verdict half of
// scripts/measure-grounding.ts (docs/architecture/development-process.md, KPI `clean_executed_correct`). No IO,
// no DB: the script feeds the promoted-surface rows in, this module classifies.
//
// The KPI counts metrics that are BOTH executed AND value-correct: an executed
// metric whose value cannot be read is `unverified`, never silently correct —
// so a surface that exposes no values scores 0, which is the honest
// fail-on-main state docs/architecture/development-process.md's fail-to-pass discipline requires.

import type { GroundTruthMetric } from "./ground-truth";

/** One declared metric as read from the promoted surface: the lifecycle
 * artifact key (graph id), its state, and the executed value when the surface
 * exposes one (null when it doesn't — the current engine reality, see
 * `extractMetricValue`). */
export interface MeasuredMetric {
	name: string;
	state: string | null;
	value: number | null;
}

/** A value that was read and compared, but landed outside tolerance. */
export interface ValueMismatch {
	name: string;
	expected: number;
	actual: number;
	tolerancePct: number;
}

/** Classification of every oracle entry. The five buckets partition the oracle
 * (correct ∪ mismatches ∪ unverified ⊆ executed; executed ∪ notExecuted ∪
 * missing = the oracle). */
export interface GroundingComparison {
	total: number;
	executed: string[];
	correct: string[];
	mismatches: ValueMismatch[];
	// Executed but no value exposed through the promoted surface — the
	// measure's loudest signal until the engine persists executed values.
	unverified: string[];
	// Declared but stopped short of executed (declared/grounded/blocked).
	notExecuted: string[];
	// No metric artifact declares this oracle name at all.
	missing: string[];
}

// Relative tolerance is degenerate at expected == 0; fall back to an absolute
// epsilon there (an oracle zero means exactly zero).
const ZERO_EPSILON = 1e-9;

/** True when `actual` matches `expected` within `tolerancePct` percent
 * (relative to `expected`; absolute epsilon when `expected` is 0). */
export function withinTolerance(
	expected: number,
	actual: number,
	tolerancePct: number,
): boolean {
	if (expected === 0) return Math.abs(actual) <= ZERO_EPSILON;
	return (
		Math.abs(actual - expected) <= Math.abs(expected) * (tolerancePct / 100)
	);
}

/**
 * Read the executed value off a metric artifact's persisted payload, or null.
 *
 * FINDING (2026-07): the engine does NOT persist executed metric values —
 * `GraphExecution.output_value` is explicitly ephemeral (graphs/models.py;
 * look-metric.ts documents the same: durable knowledge is the SQL, the value is
 * re-run on demand). `graph_definition` on the metric lifecycle artifact holds
 * the effective shipped⊕overlay DAG only. This probe is the declared seam:
 * when the engine-side value exposure lands (artifact payload or a view
 * extension), it surfaces here — until then every executed metric measures as
 * `unverified`.
 */
export function extractMetricValue(graphDefinition: unknown): number | null {
	if (
		typeof graphDefinition !== "object" ||
		graphDefinition === null ||
		Array.isArray(graphDefinition)
	) {
		return null;
	}
	const candidate = (graphDefinition as Record<string, unknown>).output_value;
	return typeof candidate === "number" && Number.isFinite(candidate)
		? candidate
		: null;
}

/**
 * Classify every oracle entry against the measured metric set. Oracle-only
 * names count into `total` (and `missing`) — an oracle metric the workspace
 * never declared is a grounding gap, not out of scope. Measured metrics absent
 * from the oracle are ignored (they are not oracle entries).
 */
export function compareMetricValues(
	groundTruth: GroundTruthMetric[],
	measured: MeasuredMetric[],
	defaultTolerancePct: number,
): GroundingComparison {
	const byName = new Map<string, MeasuredMetric>();
	for (const m of measured) byName.set(m.name, m);

	const comparison: GroundingComparison = {
		total: groundTruth.length,
		executed: [],
		correct: [],
		mismatches: [],
		unverified: [],
		notExecuted: [],
		missing: [],
	};

	for (const gt of groundTruth) {
		const metric = byName.get(gt.name);
		if (metric === undefined) {
			comparison.missing.push(gt.name);
			continue;
		}
		if (metric.state !== "executed") {
			comparison.notExecuted.push(gt.name);
			continue;
		}
		comparison.executed.push(gt.name);
		if (metric.value === null) {
			comparison.unverified.push(gt.name);
			continue;
		}
		const tolerancePct = gt.tolerancePct ?? defaultTolerancePct;
		if (withinTolerance(gt.value, metric.value, tolerancePct)) {
			comparison.correct.push(gt.name);
		} else {
			comparison.mismatches.push({
				name: gt.name,
				expected: gt.value,
				actual: metric.value,
				tolerancePct,
			});
		}
	}
	return comparison;
}
