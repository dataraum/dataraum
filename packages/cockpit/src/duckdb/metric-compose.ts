// Per-node metric composition (DAT-702) — tier C of the drill.
//
// An engine-composed metric cannot be drilled by injecting into its flattened
// SQL: the engine renders it as scalar step-CTEs and every dimension is
// aggregated away inside the extracts. It is drilled by REBUILDING the opened
// node's subtree from its persisted parts, ad hoc — nothing pre-composed,
// nothing pre-tested (DD/43417601 § "Metric drill — the per-node re-cut"):
//
//   extract  → its newest ACCEPTED snippet, verbatim (a missing snippet is a
//              NAMED refusal, but only when the step is actually reachable
//              from the opened node — a hole elsewhere in the DAG never blocks
//              this node);
//   constant → `SELECT <value> AS value` (composeConstantSql);
//   formula  → its closed-grammar arithmetic over `(SELECT value FROM <dep>)`
//              scalar refs (composeFormulaSql — the engine mirror).
//
// REACHABILITY follows parsed expression references, not declared
// `depends_on` — declared deps over-declare (the retired tier-C
// output-reachability gate existed because of it), so an over-declared step
// can neither block composition nor ride along as an unused CTE. Validation
// of each individual ref still runs against the DECLARED set, exactly like
// the engine (`compose_formula_sql`), so the mirror never composes what the
// engine would have refused.
//
// The subtree folds through `composeStandalone` (run-steps.ts) — the same
// CTE assembly the engine's `compose_standalone` mirrors — into ONE readable
// statement: `WITH <step> AS (…), … SELECT * FROM <node>`. The caller
// DESCRIBE-binds it; the binder stays the only gate.

import {
	composeConstantSql,
	composeFormulaSql,
	formulaRefs,
	parseFormulaExpression,
} from "./metric-formula";
import { composeStandalone, validateStepNames } from "./run-steps";

/** One metric DAG step plus its persisted part, resolved by the caller (the
 *  tools layer owns the metadata reads; this layer is pure). */
export interface MetricDrillStep {
	stepId: string;
	kind: "extract" | "formula" | "constant";
	/** extract: the newest accepted snippet SQL — null is a hole, refused only
	 *  when reachable from the composed node. */
	sql: string | null;
	/** formula: the closed-grammar arithmetic expression over step ids. */
	expression: string | null;
	/** constant: the resolved value (`value ?? default`), stringified. */
	value: string | null;
	/** Declared dependencies — the engine's VALIDATION namespace for formula
	 *  refs. Reachability uses parsed refs instead. */
	dependsOn: string[];
	outputStep: boolean;
}

export interface ComposedMetricNode {
	sql: string;
	/** The step the statement selects from (resolves the caller's default). */
	stepId: string;
}

/** The composed node's target: the requested step, else the DAG's output step
 *  (flagged, falling back to a root nothing depends on — the same fallback
 *  the Model loader applies to a malformed DAG). */
function targetStep(
	steps: MetricDrillStep[],
	requested: string | undefined,
): MetricDrillStep | { refusal: string } {
	if (requested !== undefined) {
		const step = steps.find((s) => s.stepId === requested);
		return step ?? { refusal: `'${requested}' is not a step of this metric` };
	}
	const flagged = steps.find((s) => s.outputStep);
	if (flagged) return flagged;
	const dependedOn = new Set(steps.flatMap((s) => s.dependsOn));
	const root = steps.find((s) => !dependedOn.has(s.stepId)) ?? steps[0];
	return root ?? { refusal: "the metric definition has no steps" };
}

/** A formula step's outgoing references: parsed expression identifiers. An
 *  unparseable expression contributes none — the render refusal names it. */
function stepRefs(step: MetricDrillStep): string[] {
	if (step.kind !== "formula" || !step.expression) return [];
	const parsed = parseFormulaExpression(step.expression);
	return "refusal" in parsed ? [] : formulaRefs(parsed.expr);
}

/**
 * Compose ONE standalone statement for a metric-DAG node from its parts.
 * Deterministic, refusal-first; the result must still DESCRIBE-bind at the
 * caller (the binder is the gate, this is only assembly).
 */
export function composeMetricNodeSql(
	metricSteps: MetricDrillStep[],
	requestedStepId?: string,
): ComposedMetricNode | { refusal: string } {
	const target = targetStep(metricSteps, requestedStepId);
	if ("refusal" in target) return target;

	const byId = new Map(metricSteps.map((s) => [s.stepId, s]));

	// Depth-first post-order over parsed refs: deps before dependents (a valid
	// CTE order, mirroring the engine's `_ordered_dep_steps`), target last.
	const order: MetricDrillStep[] = [];
	const done = new Set<string>();
	const onPath = new Set<string>();
	const visit = (step: MetricDrillStep): { refusal: string } | null => {
		if (done.has(step.stepId)) return null;
		if (onPath.has(step.stepId)) {
			return {
				refusal: `the metric definition has a dependency cycle at '${step.stepId}'`,
			};
		}
		onPath.add(step.stepId);
		for (const ref of stepRefs(step)) {
			const dep = byId.get(ref);
			// A ref to a non-step surfaces in the formula render (not declared)
			// or is a declared cache-only leaf the engine also treats as absent
			// here — either way, nothing to visit.
			if (!dep) continue;
			const cycle = visit(dep);
			if (cycle) return cycle;
		}
		onPath.delete(step.stepId);
		done.add(step.stepId);
		order.push(step);
		return null;
	};
	const cycle = visit(target);
	if (cycle) return cycle;

	const ctes: { name: string; sql: string }[] = [];
	for (const step of order) {
		if (step.kind === "extract") {
			if (!step.sql) {
				return { refusal: `no accepted extract SQL for '${step.stepId}'` };
			}
			ctes.push({ name: step.stepId, sql: step.sql });
			continue;
		}
		if (step.kind === "constant") {
			const composed = composeConstantSql(step.value);
			if ("refusal" in composed) {
				return { refusal: `step '${step.stepId}': ${composed.refusal}` };
			}
			ctes.push({ name: step.stepId, sql: composed.sql });
			continue;
		}
		if (!step.expression) {
			return { refusal: `formula step '${step.stepId}' has no expression` };
		}
		const composed = composeFormulaSql(
			step.expression,
			new Set(step.dependsOn),
		);
		if ("refusal" in composed) {
			return { refusal: `step '${step.stepId}': ${composed.refusal}` };
		}
		ctes.push({ name: step.stepId, sql: composed.sql });
	}

	// Step ids become CTE names + the final FROM verbatim — enforce SQL
	// identifier shape before splicing (engine step ids always pass; a
	// malformed persisted DAG refuses instead of injecting).
	const invalid = validateStepNames(ctes);
	if (invalid) return { refusal: invalid };

	return {
		sql: composeStandalone(ctes, `SELECT * FROM ${target.stepId}`),
		stepId: target.stepId,
	};
}
