// Parts-at-source composition (DAT-671 / DAT-703): build a metric-DAG node's
// SQL — scalar, sliced, pinned — from PERSISTED clause parts, never by parsing
// or mutating SQL text.
//
// The engine authors every extract as clause parts (`extract_parts_dict`,
// formula_composer.py) and persists them on the snippet (`sql_snippets.parts`);
// the `sql` column is merely their render. This module is the cockpit-side
// consumer: `narrowSnippetParts` narrows the JSON at the DB boundary, and
// `composeNodeQuery` rebuilds the opened node's reachable subtree onto
// @uwdata/mosaic-sql — the query stays data until the one `String()` at the
// end (the mosaic posture), so a slice is a clause APPEND (dims into SELECT +
// GROUP BY), a pin a WHERE append, never a mutation of a fused string.
//
// GROUPED COMPOSITION (DAT-703): every dim-carrying extract in the subtree
// gets the dims + GROUP BY → `(dims…, value)`; a formula's carriers join
// `FULL JOIN … USING (dims)` — the union domain, so a disjoint decomposition
// (gross_profit by account: revenue rows ∩ cogs rows = ∅) keeps both sides —
// and the formula's arithmetic runs per row off that spine. Constants stay
// scalar, as does an extract's fall-loud shape (no relation → SELECT NULL).
//
// ABSENCE is a property of the measure, decided once at ref-render time (the
// chain-B per-operator context threading stays deleted): a carrier ref is
// `COALESCE(ref, 0)` iff its absence for a group is a TRUE zero —
//   - a SUM/COUNT extract (no rows ⇒ the sum IS zero), or
//   - an additive combination (+, -, unary minus) of refs that are themselves
//     zero-absent carriers (found empirically: ebitda = operating_income +
//     depreciation_amortization lost the depreciation-only groups while the
//     intermediate stayed bare — spikes/drill-metrics, commit a8e99d32).
//     A numeric literal or scalar-ref addend breaks the proof — the absent
//     group's true value would be the addend, not 0 — so `provablyZeroAbsent`
//     is deliberately tighter than "all ops are additive".
// Every other ref stays bare: SQL NULL propagation yields the honest
// undefined for a one-sided ratio group, never a fabricated 0.
//
// mosaic-sql gotchas (pinned by the spikes, commit cacb0174): computed select
// items MUST be `{alias: expr}` objects — bare expressions auto-alias with
// their own text; no bare `*` in the final select; multiple `where()` exprs
// join with bare ` AND `, so every predicate leaf is parenthesized here (an
// OR inside one leaf can never bleed across leaves).
//
// Neo-free and pure: no connection, no IO. The composed statement is
// validated by the caller's bound DESCRIBE (`/api/drill/node`) — the binder
// stays the only gate.

import {
	column,
	join,
	Query,
	type SelectQuery,
	verbatim,
} from "@uwdata/mosaic-sql";

import type { DrillPinValue } from "./drill";
import { quoteIdentifier } from "./grid-query";
import {
	type FormulaExpr,
	formulaRefs,
	parseFormulaExpression,
	renderFormulaValue,
} from "./metric-formula";

/** The clause parts of ONE persisted extract snippet, narrowed from
 *  `sql_snippets.parts`. The stored schema is the general shape every
 *  structured SQL author shares (`{select: [{expr, alias}], from: […],
 *  where: […]}`); the graph agent only ever fills the single-value case and
 *  that is the only shape this builder composes. */
export interface SnippetParts {
	/** The scalar value expression, unaliased — `SUM(credit) - SUM(debit)`. */
	selectExpr: string;
	/** The one relation the extract reads; null is the fall-loud shape. */
	relation: string | null;
	/** Predicate texts, AND-composed (each parenthesized when rendered). */
	where: string[];
}

/** Narrow a persisted `sql_snippets.parts` value to the single-value extract
 *  shape. Anything else — no/multiple select items, a non-`value` alias, more
 *  than one relation, a non-string predicate — is null: the snippet predates
 *  parts-at-source or isn't a graph extract, and the composer refuses by
 *  step name instead of guessing. */
export function narrowSnippetParts(raw: unknown): SnippetParts | null {
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return null;
	}
	const p = raw as Record<string, unknown>;
	if (!Array.isArray(p.select) || p.select.length !== 1) return null;
	const item: unknown = p.select[0];
	if (typeof item !== "object" || item === null) return null;
	const { expr, alias } = item as Record<string, unknown>;
	if (typeof expr !== "string" || expr.trim() === "") return null;
	if (alias !== "value") return null;
	if (!Array.isArray(p.from) || p.from.length > 1) return null;
	const relation: unknown = p.from.length === 1 ? p.from[0] : null;
	if (relation !== null && (typeof relation !== "string" || relation === "")) {
		return null;
	}
	if (!Array.isArray(p.where)) return null;
	const where: string[] = [];
	for (const w of p.where) {
		if (typeof w !== "string") return null;
		if (w.trim() !== "") where.push(w);
	}
	return { selectExpr: expr, relation, where };
}

/** One metric-DAG step plus its persisted extract parts, resolved by the
 *  caller (the tools layer owns the metadata reads; this layer is pure). */
export interface NodeStep {
	stepId: string;
	kind: "extract" | "formula" | "constant";
	/** extract: the newest ACCEPTED snippet's narrowed parts — null is a hole,
	 *  refused only when reachable from the composed node. */
	parts: SnippetParts | null;
	/** extract: the step's declared aggregation — SUM/COUNT is what makes
	 *  absence a true zero under grouping. */
	aggregation: string | null;
	/** formula: the closed-grammar arithmetic expression over step ids. */
	expression: string | null;
	/** constant: the resolved value (`value ?? default`), stringified. */
	value: string | null;
	/** Declared dependencies — the engine's VALIDATION namespace for formula
	 *  refs (`compose_formula_sql` parity). Reachability uses parsed refs. */
	dependsOn: string[];
	outputStep: boolean;
}

/** A drill over the composed node: slice dims become GROUP BY appends on every
 *  dim-carrying extract; pins are row-level filters pushed into every
 *  extract's WHERE, pre-aggregation — pins without a slice re-evaluate the
 *  scalar under the filter. */
export interface NodeDrill {
	slices: string[];
	pins: { column: string; value: DrillPinValue }[];
}

export interface ComposedNodeQuery {
	sql: string;
	/** Positional pin params (`$1…`) in pin order; NULL pins render `IS NULL`
	 *  and consume no slot. The same `$n` may appear in several extract CTEs —
	 *  DuckDB numbered params are reusable. */
	params: DrillPinValue[];
	/** The step the statement selects from (resolves the caller's default). */
	stepId: string;
}

// Step ids splice raw into `(SELECT value FROM <id>)` scalar refs and become
// CTE names — enforce SQL identifier shape before composing (engine step ids
// always pass; a malformed persisted DAG refuses instead of injecting).
const IDENT_RE = /^[A-Za-z_]\w*$/;

/** The composed node's target: the requested step, else the DAG's output step
 *  (flagged, falling back to a root nothing depends on — the same fallback
 *  the Model loader applies to a malformed DAG). */
function targetStep(
	steps: NodeStep[],
	requested: string | undefined,
): NodeStep | { refusal: string } {
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
function stepRefs(step: NodeStep): string[] {
	if (step.kind !== "formula" || !step.expression) return [];
	const parsed = parseFormulaExpression(step.expression);
	return "refusal" in parsed ? [] : formulaRefs(parsed.expr);
}

/** True when the expression's value for an all-absent group is provably zero:
 *  an additive (+, -, unary minus) combination of refs that are themselves
 *  zero-absent carriers. `false` the moment a numeric literal or a scalar
 *  (non-carrier) ref appears as an addend — absence would then equal the
 *  addend, not zero — or any multiplicative structure is involved. */
function provablyZeroAbsent(
	expr: FormulaExpr,
	carriers: ReadonlySet<string>,
	zeroAbsent: ReadonlySet<string>,
): boolean {
	switch (expr.kind) {
		case "ref":
			return carriers.has(expr.name) && zeroAbsent.has(expr.name);
		case "num":
			return false;
		case "neg":
			return provablyZeroAbsent(expr.operand, carriers, zeroAbsent);
		case "bin":
			return (
				(expr.op === "+" || expr.op === "-") &&
				provablyZeroAbsent(expr.left, carriers, zeroAbsent) &&
				provablyZeroAbsent(expr.right, carriers, zeroAbsent)
			);
	}
}

/**
 * Compose ONE standalone statement for a metric-DAG node from its persisted
 * parts, with the drill applied as clause appends. Deterministic,
 * refusal-first; the result must still DESCRIBE-bind at the caller (the
 * binder is the gate, this is only assembly).
 *
 * REACHABILITY follows parsed expression references, not declared
 * `depends_on` — declared deps over-declare, so an over-declared step can
 * neither block composition nor ride along as an unused CTE. Validation of
 * each individual ref still runs against the DECLARED set, exactly like the
 * engine, so this mirror never composes what the engine would have refused.
 */
export function composeNodeQuery(
	steps: NodeStep[],
	requestedStepId: string | undefined,
	drill: NodeDrill = { slices: [], pins: [] },
): ComposedNodeQuery | { refusal: string } {
	const target = targetStep(steps, requestedStepId);
	if ("refusal" in target) return target;

	const byId = new Map(steps.map((s) => [s.stepId, s]));

	// Depth-first post-order over parsed refs: deps before dependents (a valid
	// CTE order, mirroring the engine's `_ordered_dep_steps`), target last.
	const order: NodeStep[] = [];
	const done = new Set<string>();
	const onPath = new Set<string>();
	const visit = (step: NodeStep): { refusal: string } | null => {
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

	for (const step of order) {
		if (!IDENT_RE.test(step.stepId)) {
			return { refusal: `step id '${step.stepId}' is not a SQL identifier` };
		}
	}

	// First slice per column wins, order preserved (the menu disables
	// re-slicing, but the wire shape is not trusted).
	const dims: string[] = [];
	for (const d of drill.slices) {
		if (!dims.includes(d)) dims.push(d);
	}
	// `value` is the composition's own measure alias in every CTE projection —
	// a dimension with that literal name would collide with it (WHERE-level
	// pins are fine: predicates bind against the relation, pre-projection).
	if (dims.includes("value")) {
		return {
			refusal:
				"cannot slice by a dimension named 'value' — it collides with the composed measure column",
		};
	}

	// Pins render once and are appended to EVERY extract CTE below.
	const params: DrillPinValue[] = [];
	const pinPredicates: string[] = [];
	for (const p of drill.pins) {
		if (p.value === null) {
			pinPredicates.push(`${quoteIdentifier(p.column)} IS NULL`);
		} else {
			params.push(p.value);
			pinPredicates.push(`${quoteIdentifier(p.column)} = $${params.length}`);
		}
	}

	const ctes: Record<string, SelectQuery> = {};
	const carriers = new Set<string>(); // dim-carrying CTEs (grouped mode)
	const zeroAbsent = new Set<string>();
	// Scalar-mode counterpart of zeroAbsent: SUM/COUNT extracts whose EMPTY
	// value is a true zero once pins RESTRICT the domain — so pinning a
	// grouped row reproduces exactly that row's value. Applied only when a
	// pin exists: the unrestricted scalar stays byte-parity with the engine
	// composition, where a whole-domain NULL is the fall-loud grounding flag.
	const zeroAbsentScalars = new Set<string>();

	for (const step of order) {
		if (step.kind === "extract") {
			if (!step.parts) {
				return {
					refusal: `no persisted clause parts for '${step.stepId}'`,
				};
			}
			const { selectExpr, relation, where } = step.parts;
			if (!relation) {
				// The fall-loud shape (SELECT NULL, no FROM) stays scalar: there
				// is nothing to group or filter, and the NULL propagates through
				// a bare formula ref as the honest undefined.
				ctes[step.stepId] = Query.select({ value: verbatim(selectExpr) });
				continue;
			}
			let q =
				dims.length > 0
					? Query.from(relation)
							.select(...dims.map((d) => column(d)), {
								value: verbatim(selectExpr),
							})
							.groupby(...dims.map((d) => column(d)))
					: Query.from(relation).select({ value: verbatim(selectExpr) });
			const preds = [...where, ...pinPredicates];
			if (preds.length > 0) {
				q = q.where(...preds.map((p) => verbatim(`(${p})`)));
			}
			ctes[step.stepId] = q;
			const agg = (step.aggregation ?? "").toLowerCase();
			const zeroWhenEmpty = agg === "sum" || agg === "count";
			if (dims.length > 0) {
				carriers.add(step.stepId);
				if (zeroWhenEmpty) zeroAbsent.add(step.stepId);
			} else if (zeroWhenEmpty) {
				zeroAbsentScalars.add(step.stepId);
			}
			continue;
		}
		if (step.kind === "constant") {
			const numeric =
				step.value === null || step.value.trim() === ""
					? Number.NaN
					: Number(step.value);
			if (!Number.isFinite(numeric)) {
				return {
					refusal: `constant '${step.stepId}' value '${String(step.value)}' is not numeric`,
				};
			}
			// Engine parity (compose_constant_sql): an integer stays integer — a
			// constant is never a division denominator, so integer typing is safe.
			ctes[step.stepId] = Query.select({ value: verbatim(String(numeric)) });
			continue;
		}
		if (!step.expression) {
			return { refusal: `formula step '${step.stepId}' has no expression` };
		}
		const parsed = parseFormulaExpression(step.expression);
		if ("refusal" in parsed) {
			return { refusal: `step '${step.stepId}': ${parsed.refusal}` };
		}
		// A ref that is DECLARED but names no step would pass the declared-set
		// validation below yet have no CTE — surfacing as a raw Catalog Error
		// (or, worse, silently binding a real lake table that happens to carry
		// a `value` column). Refuse it by name like every other malformed shape.
		const phantom = formulaRefs(parsed.expr).find(
			(r) => step.dependsOn.includes(r) && !byId.has(r),
		);
		if (phantom !== undefined) {
			return {
				refusal: `formula step '${step.stepId}' depends on '${phantom}', which is not a step of this metric`,
			};
		}
		const rendered = renderFormulaValue(
			step.expression,
			new Set(step.dependsOn),
			{
				carriers,
				zeroAbsent,
				zeroAbsentScalars:
					pinPredicates.length > 0 ? zeroAbsentScalars : undefined,
			},
		);
		if ("refusal" in rendered) {
			return { refusal: `step '${step.stepId}': ${rendered.refusal}` };
		}
		const [first, ...rest] = formulaRefs(parsed.expr).filter((r) =>
			carriers.has(r),
		);
		if (dims.length > 0 && first !== undefined) {
			// FULL JOIN … USING (dims) spine over the dim-carrying deps; scalar
			// deps (constants, fall-loud extracts) stay subqueries in the value.
			let spine: Parameters<typeof join>[0] = first;
			for (const dep of rest) {
				spine = join(spine, dep, { type: "FULL", using: dims });
			}
			ctes[step.stepId] = Query.from(spine).select(
				...dims.map((d) => column(d)),
				{
					value: verbatim(rendered.sql),
				},
			);
			carriers.add(step.stepId);
			if (provablyZeroAbsent(parsed.expr, carriers, zeroAbsent)) {
				zeroAbsent.add(step.stepId);
			}
		} else {
			ctes[step.stepId] = Query.select({ value: verbatim(rendered.sql) });
		}
	}

	// No bare `*` (mosaic would alias it): select the target CTE's columns
	// explicitly — grouped output carries the dims, scalar carries value only.
	const outCols = carriers.has(target.stepId)
		? [...dims.map((d) => column(d)), column("value")]
		: [column("value")];
	const sql = String(
		Query.with(ctes)
			.select(...outCols)
			.from(target.stepId),
	);
	return { sql, params, stepId: target.stepId };
}
