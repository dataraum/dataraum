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
// ABSENCE DOCTRINE v2 — "observed or dash" (DAT-703 smoke finding: zero-absence
// COALESCE presented degenerate ratios as real values — gross_margin showed
// 100.00 on every account that simply has no COGS). ONE structural
// classification per opened node, from its parsed reachable tree, decided
// once at compose time — never per operator (the chain-B context threading
// stays deleted):
//
//   ADDITIVE — every reachable formula uses only `+`, binary `-`, unary
//   minus, and refs, and every leaf is an extract WITH a relation. The
//   grouped/pinned result is a SUM over a UNION ALL of SIGNED CARRIER
//   CONTRIBUTIONS: each extract keeps its grouped CTE, the expression
//   flattens to (extract, sign) pairs, and an absent carrier simply
//   contributes no rows. No join, no COALESCE — the disjoint-decomposition
//   union domain (gross_profit by account: +sales / -materials, Σ = total)
//   holds by algebra.
//
//   NON-ADDITIVE (any ratio, product, literal, constant ref, or fall-loud
//   leaf) — carriers join `FULL JOIN … ON (dim IS NOT DISTINCT FROM dim)`
//   (NULL-safe: a slice group whose dim is NULL stays ONE group, not a split
//   of dashes — DAT-714) and every ref renders BARE. SQL NULL absorbs through
//   the arithmetic, so a group shows a value iff EVERY carrier the formula
//   touches is observed in that group; anything partial is the honest `—`
//   (filterable), never a fabricated number.
//
// Pins follow the node's mode (additive → contributions under the filter,
// non-additive → bare), so a pinned re-evaluation reproduces exactly the
// grouped row it came from — including NULL ≡ NULL. The UNRESTRICTED scalar
// (no dims, no pins) stays byte-parity with the engine composition, where a
// whole-domain NULL is the fall-loud grounding flag.
//
// TIME GRAIN (DAT-712, the DAT-673 hook): a slice/pin may carry a grain token
// (grain.ts's closed grammar — the token itself NEVER reaches SQL). The
// extract CTE projects `time_bucket(INTERVAL '…', col)` under the column's
// own name and groups by the expression; everything downstream — union
// branches, join spine, pins, outCols — consumes the bucket start as an
// ordinary dim column, so both doctrine modes and pin ≡ group hold at every
// grain unchanged.
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
import { type Grain, grainIntervalBody, parseGrainToken } from "./grain";
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
	/** formula: the closed-grammar arithmetic expression over step ids. */
	expression: string | null;
	/** constant: the resolved value (`value ?? default`), stringified. */
	value: string | null;
	/** Declared dependencies — the engine's VALIDATION namespace for formula
	 *  refs (`compose_formula_sql` parity). Reachability uses parsed refs. */
	dependsOn: string[];
	outputStep: boolean;
}

/** One slice dimension of a drill. A `grain` token (`1M`, `15m` — grain.ts's
 *  closed grammar) buckets a temporal column via `time_bucket` INSIDE every
 *  extract CTE, so the dim the whole composition groups/joins/pins on IS the
 *  bucket start, under the column's own name. */
export interface NodeSlice {
	column: string;
	grain?: string;
}

/** A drill over the composed node: slice dims become GROUP BY appends on every
 *  dim-carrying extract; pins are row-level filters pushed into every
 *  extract's WHERE, pre-aggregation — pins without a slice re-evaluate the
 *  scalar under the filter, in the same mode as the grouped view. A pin
 *  carries the grain it was CREATED under (pin ≡ the grouped row it came
 *  from): its predicate buckets the raw column with the same `time_bucket`
 *  before comparing, and a later grain change on the slice never re-scopes an
 *  existing pin. */
export interface NodePin {
	column: string;
	value: DrillPinValue;
	grain?: string;
}

export interface NodeDrill {
	slices: NodeSlice[];
	pins: NodePin[];
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
 *  unparseable expression contributes none — the validation pass names it. */
function stepRefs(step: NodeStep): string[] {
	if (step.kind !== "formula" || !step.expression) return [];
	const parsed = parseFormulaExpression(step.expression);
	return "refusal" in parsed ? [] : formulaRefs(parsed.expr);
}

/** A signed extract contribution of an additive tree: `sign · extract.value`. */
export interface Contribution {
	stepId: string;
	sign: 1 | -1;
}

/**
 * Classify the target's reachable tree (doctrine v2): flatten it to signed
 * extract contributions when it is PURELY ADDITIVE — only `+`, binary `-`,
 * unary minus, and refs, every leaf an extract with a relation. Returns null
 * the moment a literal, `*`, `/`, constant ref, fall-loud extract, or hole
 * appears: the node is then non-additive (bare refs, NULL absorbs). One
 * occurrence per REFERENCE, not per step — `a + a` contributes twice, exactly
 * like the formula's own arithmetic. Classification only: refs are NOT
 * validated here — `composeNodeQuery` runs the fabrication/phantom guards
 * over the same tree first, so call this on an already-validated tree.
 * Exported for the enumeration spike.
 */
export function flattenAdditive(
	target: NodeStep,
	byId: ReadonlyMap<string, NodeStep>,
): Contribution[] | null {
	const out: Contribution[] = [];
	const visitStep = (step: NodeStep, sign: 1 | -1): boolean => {
		if (step.kind === "extract") {
			if (!step.parts?.relation) return false; // fall-loud or hole
			out.push({ stepId: step.stepId, sign });
			return true;
		}
		if (step.kind !== "formula" || !step.expression) return false;
		const parsed = parseFormulaExpression(step.expression);
		if ("refusal" in parsed) return false;
		return visitExpr(parsed.expr, sign);
	};
	const visitExpr = (e: FormulaExpr, sign: 1 | -1): boolean => {
		switch (e.kind) {
			case "num":
				return false; // an absent group's value would be the literal, not 0
			case "ref": {
				const dep = byId.get(e.name);
				return dep ? visitStep(dep, sign) : false;
			}
			case "neg":
				return visitExpr(e.operand, sign === 1 ? -1 : 1);
			case "bin": {
				if (e.op !== "+" && e.op !== "-") return false;
				const rightSign = e.op === "-" ? (sign === 1 ? -1 : 1) : sign;
				return visitExpr(e.left, sign) && visitExpr(e.right, rightSign);
			}
		}
	};
	return visitStep(target, 1) ? out : null;
}

/** One operand term of the composed node's OWN formula — what the equation
 *  header renders and (non-constant, non-additive views) what the component
 *  breakdown projects as columns. */
export interface NodeOperand {
	stepId: string;
	kind: "extract" | "formula" | "constant";
	/** A constant operand's resolved value, for inline rendering. */
	value: string | null;
}

/** The composed node's header shape (DAT-712's equation layer): the target's
 *  formula, its operand terms, and the doctrine classification. Pure — the
 *  route ships it once per open; signs/pretty-printing derive client-side
 *  from `expression` via the same closed grammar (metric-formula.ts). */
export interface NodeShape {
	targetStepId: string;
	/** The target's formula expression — null for a bare extract node. */
	expression: string | null;
	/** Doctrine v2 classification of the target's reachable tree: additive
	 *  nodes decompose grouped views via signed contributions (their grid rows
	 *  carry NO operand columns); non-additive restricted views project them. */
	additive: boolean;
	operands: NodeOperand[];
}

/** Resolve the node's header shape — the same target resolution the composer
 *  uses, no SQL. A refusal only when the target itself cannot resolve. */
export function nodeShape(
	steps: NodeStep[],
	requestedStepId: string | undefined,
): NodeShape | { refusal: string } {
	const target = targetStep(steps, requestedStepId);
	if ("refusal" in target) return target;
	const byId = new Map(steps.map((s) => [s.stepId, s]));
	const additive = flattenAdditive(target, byId) !== null;
	if (target.kind !== "formula" || !target.expression) {
		return {
			targetStepId: target.stepId,
			expression: null,
			additive,
			operands: [],
		};
	}
	const parsed = parseFormulaExpression(target.expression);
	if ("refusal" in parsed) {
		return {
			targetStepId: target.stepId,
			expression: null,
			additive,
			operands: [],
		};
	}
	const operands: NodeOperand[] = [];
	for (const ref of formulaRefs(parsed.expr)) {
		const dep = byId.get(ref);
		if (!dep) continue; // compose validation refuses phantoms; tolerate here
		operands.push({ stepId: dep.stepId, kind: dep.kind, value: dep.value });
	}
	return {
		targetStepId: target.stepId,
		expression: target.expression,
		additive,
		operands,
	};
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
	return composeNode(steps, requestedStepId, drill, false);
}

/**
 * The TOTALS composition (DAT-712's footer + scalar-bound equation): the
 * UNRESTRICTED scalar with the target formula's operand components projected
 * alongside `value` — the same operand set a restricted view carries, so the
 * footer's columns line up under the grid's. A separate entry point on
 * purpose: the plain scalar (`composeNodeQuery`, no drill) stays byte-parity
 * with the engine's persisted render, which this projection would break.
 */
export function composeNodeTotals(
	steps: NodeStep[],
	requestedStepId: string | undefined,
): ComposedNodeQuery | { refusal: string } {
	return composeNode(steps, requestedStepId, { slices: [], pins: [] }, true);
}

/** One deduped slice dim, its grain parsed: `bucketExpr` is the rendered
 *  `time_bucket(…)` over the RAW column — only valid in extract-CTE scope;
 *  every downstream CTE sees the bucketed value as an output column named
 *  `column`. */
interface DimSpec {
	column: string;
	bucketExpr: string | null;
}

/** Render the bucket expression for a validated grain — the interval body is
 *  authored by grain.ts from the parsed token, never user text. */
const bucketExpr = (grain: Grain, columnName: string): string =>
	`time_bucket(INTERVAL '${grainIntervalBody(grain)}', ${quoteIdentifier(columnName)})`;

const invalidGrain = (token: string): { refusal: string } => ({
	refusal:
		`'${token}' is not a valid grain token — use a count + unit like ` +
		`1d, 1w, 1M, 1q, 1y (m is minutes, M is months)`,
});

/** A carrier CTE's dim column, fully qualified: `"revenue"."cost_center"`. The
 *  carrier alias is its CTE name (a step id, IDENT_RE-gated) and the dim is a
 *  slice column (user/agent-influenced) — both are quoted. */
const qualifiedDim = (carrierId: string, dim: string): string =>
	`${quoteIdentifier(carrierId)}.${quoteIdentifier(dim)}`;

/** The NULL-safe dim key across the carriers already in the FULL-JOIN spine
 *  (DAT-714). A FULL JOIN can leave ANY joined carrier's key NULL, so a dim's
 *  live value is the COALESCE across every carrier so far — a single carrier
 *  needs none. Drives BOTH the `IS NOT DISTINCT FROM` join key (left side) and
 *  the `COALESCE(…) AS dim` output projection (with ON, not USING, every
 *  carrier alias carries the dim, so a bare `dim` would be ambiguous). */
const coalesceDim = (carrierIds: readonly string[], dim: string): string => {
	const refs = carrierIds.map((c) => qualifiedDim(c, dim));
	return refs.length === 1 ? refs[0] : `COALESCE(${refs.join(", ")})`;
};

function composeNode(
	steps: NodeStep[],
	requestedStepId: string | undefined,
	drill: NodeDrill,
	projectOperandsOnScalar: boolean,
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
			// An unresolvable ref is handled by the validation pass below —
			// nothing to visit here.
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

	// One validation pass for BOTH modes (the additive path never renders the
	// formulas, so validation cannot live in the renderer alone): identifier
	// gate, parseability, the engine's fabrication guard (every ref DECLARED),
	// and the phantom guard (every ref an actual step — a phantom would
	// surface as a raw Catalog Error, or worse, silently bind a real lake
	// table that happens to carry a `value` column).
	for (const step of order) {
		if (!IDENT_RE.test(step.stepId)) {
			return { refusal: `step id '${step.stepId}' is not a SQL identifier` };
		}
		if (step.kind !== "formula" || !step.expression) continue;
		const parsed = parseFormulaExpression(step.expression);
		if ("refusal" in parsed) {
			return { refusal: `step '${step.stepId}': ${parsed.refusal}` };
		}
		for (const ref of formulaRefs(parsed.expr)) {
			if (!step.dependsOn.includes(ref)) {
				return {
					refusal:
						`step '${step.stepId}': formula '${step.expression}' references ` +
						`'${ref}', which is not a declared dependency — refusing to ` +
						`compose a fabricated operand`,
				};
			}
			if (!byId.has(ref)) {
				return {
					refusal: `formula step '${step.stepId}' depends on '${ref}', which is not a step of this metric`,
				};
			}
		}
	}

	// First slice per column wins, order preserved (the menu disables
	// re-slicing, but the wire shape is not trusted). Grain tokens parse
	// against the closed grammar here — an off-grammar token is a refusal,
	// never interpolated (grain.ts: DuckDB itself would read '1M' as minutes).
	const dims: DimSpec[] = [];
	for (const s of drill.slices) {
		if (dims.some((d) => d.column === s.column)) continue;
		let expr: string | null = null;
		if (s.grain !== undefined) {
			const grain = parseGrainToken(s.grain);
			if (!grain) return invalidGrain(s.grain);
			expr = bucketExpr(grain, s.column);
		}
		dims.push({ column: s.column, bucketExpr: expr });
	}
	const dimColumns = dims.map((d) => d.column);
	// `value` is the composition's own measure alias in every CTE projection
	// (`_observed` its additive observation counter) — a dimension with either
	// literal name would collide (WHERE-level pins are fine: predicates bind
	// against the relation, pre-projection).
	for (const reserved of ["value", "_observed"]) {
		if (dimColumns.includes(reserved)) {
			return {
				refusal: `cannot slice by a dimension named '${reserved}' — it collides with a composed column`,
			};
		}
	}

	// Pins render once and are appended to EVERY extract CTE below. A grained
	// pin buckets the raw column exactly like the slice that produced its row,
	// so the bound bucket start matches whole buckets, not one raw value.
	const params: DrillPinValue[] = [];
	const pinPredicates: string[] = [];
	for (const p of drill.pins) {
		let lhs = quoteIdentifier(p.column);
		if (p.grain !== undefined) {
			const grain = parseGrainToken(p.grain);
			if (!grain) return invalidGrain(p.grain);
			lhs = bucketExpr(grain, p.column);
		}
		if (p.value === null) {
			pinPredicates.push(`${lhs} IS NULL`);
		} else {
			params.push(p.value);
			pinPredicates.push(`${lhs} = $${params.length}`);
		}
	}

	/** An extract's CTE under the current drill: dims + GROUP BY when sliced,
	 *  the persisted predicates plus the pins in WHERE. The additive mode adds
	 *  `_observed` (the row count) so a pinned-scalar extract can tell "nothing
	 *  matched" (skip) from "matched but the aggregate is NULL" (poison).
	 *
	 *  A grained dim projects its `time_bucket(…)` ALIASED to the column name
	 *  and — load-bearing — GROUPS BY the expression itself: `GROUP BY "col"`
	 *  would resolve to the RAW column (source columns shadow select aliases),
	 *  yielding one group per raw value that merely DISPLAYS the bucket start.
	 *  Only this CTE sees the raw relation; every downstream CTE (union
	 *  branches, join spine, outer aggregate) consumes the bucketed value as an
	 *  ordinary output column. */
	const extractCte = (
		parts: SnippetParts,
		relation: string,
		withObservedCount = false,
	): SelectQuery => {
		const valueCols: Record<string, ReturnType<typeof verbatim>>[] = [
			{ value: verbatim(parts.selectExpr) },
		];
		if (withObservedCount) valueCols.push({ _observed: verbatim("COUNT(*)") });
		let q =
			dims.length > 0
				? Query.from(relation)
						.select(
							...dims.map((d) =>
								d.bucketExpr === null
									? column(d.column)
									: { [d.column]: verbatim(d.bucketExpr) },
							),
							...valueCols,
						)
						.groupby(
							...dims.map((d) =>
								d.bucketExpr === null
									? column(d.column)
									: verbatim(d.bucketExpr),
							),
						)
				: Query.from(relation).select(...valueCols);
		const preds = [...parts.where, ...pinPredicates];
		if (preds.length > 0) {
			q = q.where(...preds.map((p) => verbatim(`(${p})`)));
		}
		return q;
	};

	const ctes: Record<string, SelectQuery> = {};
	// Whether the final select carries the dims (the target composed grouped).
	let targetGrouped = false;
	// The component-breakdown columns (non-additive restricted views only).
	let projectedOperands: string[] = [];

	// ---- ADDITIVE mode (restricted domains only): SUM over signed carrier
	// contributions. The unrestricted scalar deliberately does NOT take this
	// path — it stays byte-parity with the engine composition, where a
	// whole-domain NULL is the fall-loud grounding flag.
	const restricted = dims.length > 0 || pinPredicates.length > 0;
	const contributions =
		restricted && target.kind !== "extract"
			? flattenAdditive(target, byId)
			: null;
	if (contributions !== null) {
		for (const { stepId } of contributions) {
			if (ctes[stepId]) continue;
			const step = byId.get(stepId);
			// flattenAdditive only emits extracts with a relation.
			if (!step?.parts?.relation) {
				return { refusal: `no persisted clause parts for '${stepId}'` };
			}
			ctes[stepId] = extractCte(step.parts, step.parts.relation, true);
		}
		// Branches drop UNOBSERVED rows (`_observed = 0` only occurs in the
		// pinned-scalar shape — an aggregate without GROUP BY emits one row
		// even over nothing; grouped rows always observed ≥ 1).
		const branches = contributions.map(({ stepId, sign }) => {
			const value =
				sign === 1
					? { value: column("value") }
					: { value: verbatim('-("value")') };
			const q =
				dims.length > 0
					? Query.from(stepId).select(
							...dimColumns.map((d) => column(d)),
							value,
						)
					: Query.from(stepId).select(value);
			return q.where(verbatim('("_observed" > 0)'));
		});
		const unioned = Query.unionAll(...branches);
		// An OBSERVED contribution whose aggregate is legitimately NULL must
		// POISON its group, not be skipped — SQL's NULL-skipping SUM would
		// otherwise fabricate a value where the non-additive view of the same
		// data shows the dash (doctrine v2's whole point; review 50e4a143).
		const summed = verbatim(
			'CASE WHEN COUNT(*) = COUNT("value") THEN SUM("value") ELSE NULL END',
		);
		ctes[target.stepId] =
			dims.length > 0
				? Query.from(unioned)
						.select(...dimColumns.map((d) => column(d)), { value: summed })
						.groupby(...dimColumns.map((d) => column(d)))
				: Query.from(unioned).select({ value: summed });
		targetGrouped = dims.length > 0;
	} else {
		// ---- General path: engine-parity scalar, or the non-additive
		// grouped/pinned composition (FULL JOIN spine, BARE refs — NULL absorbs,
		// a group is `—` unless every carrier is observed in it).
		const carriers = new Set<string>(); // dim-carrying CTEs (grouped mode)

		for (const step of order) {
			if (step.kind === "extract") {
				if (!step.parts) {
					return {
						refusal: `no persisted clause parts for '${step.stepId}'`,
					};
				}
				const { selectExpr, relation } = step.parts;
				if (!relation) {
					// The fall-loud shape (SELECT NULL, no FROM) stays scalar: there
					// is nothing to group or filter, and the NULL absorbs through a
					// bare formula ref as the honest undefined.
					ctes[step.stepId] = Query.select({ value: verbatim(selectExpr) });
					continue;
				}
				ctes[step.stepId] = extractCte(step.parts, relation);
				if (dims.length > 0) carriers.add(step.stepId);
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
			const rendered = renderFormulaValue(
				step.expression,
				new Set(step.dependsOn),
				{ carriers },
			);
			if ("refusal" in rendered) {
				return { refusal: `step '${step.stepId}': ${rendered.refusal}` };
			}
			const [first, ...rest] = formulaRefs(parsed.expr).filter((r) =>
				carriers.has(r),
			);
			// COMPONENT BREAKDOWN: in a restricted view, the TARGET formula's
			// immediate non-constant operands ride along as columns — a `—` in
			// `value` then explains itself (the inputs decompose even where the
			// ratio cannot), instead of a bare dash the user must reverse-
			// engineer. Only the opened node's own operands, never intermediate
			// CTEs'; skipped on a name collision with a dim or the value alias.
			const operands =
				(restricted || projectOperandsOnScalar) && step.stepId === target.stepId
					? formulaRefs(parsed.expr).filter((r) => {
							const dep = byId.get(r);
							return (
								dep !== undefined &&
								dep.kind !== "constant" &&
								r !== "value" &&
								!dimColumns.includes(r)
							);
						})
					: [];
			const operandCols = operands.map((r) => ({
				[r]: verbatim(
					carriers.has(r) ? `"${r}"."value"` : `(SELECT value FROM ${r})`,
				),
			}));
			if (dims.length > 0 && first !== undefined) {
				// NULL-SAFE FULL-JOIN spine over the dim-carrying deps (DAT-714);
				// scalar deps (constants, fall-loud extracts) stay subqueries in the
				// value. `FULL JOIN … USING (dim)` treats NULL as UNEQUAL (NULL !=
				// NULL), so a slice group whose dim is NULL never matches across
				// carriers — it SPLITS into two one-sided rows that each dash out and
				// DISCARD the real value. Join ON `dim IS NOT DISTINCT FROM dim`
				// instead (NULL ≡ NULL, one honest group); because a FULL JOIN can
				// leave EITHER side's key NULL, the left key of each successive join
				// is the COALESCE over every carrier already in the spine. The output
				// dim is likewise COALESCE(all carriers) AS dim — with ON (not USING)
				// every carrier alias carries the dim column, so a bare `dim` is
				// ambiguous. (The additive path stays GROUP BY, which already folds
				// NULL into one group — this is the non-additive spine only.)
				const joined: string[] = [first];
				let spine: Parameters<typeof join>[0] = first;
				for (const dep of rest) {
					const on = dimColumns
						.map(
							(d) =>
								`(${coalesceDim(joined, d)} IS NOT DISTINCT FROM ` +
								`${qualifiedDim(dep, d)})`,
						)
						.join(" AND ");
					spine = join(spine, dep, { type: "FULL", on: verbatim(on) });
					joined.push(dep);
				}
				ctes[step.stepId] = Query.from(spine).select(
					...dimColumns.map((d) => ({ [d]: verbatim(coalesceDim(joined, d)) })),
					...operandCols,
					{
						value: verbatim(rendered.sql),
					},
				);
				carriers.add(step.stepId);
			} else {
				ctes[step.stepId] = Query.select(...operandCols, {
					value: verbatim(rendered.sql),
				});
			}
			if (operands.length > 0) projectedOperands = operands;
		}
		targetGrouped = carriers.has(target.stepId);
	}

	// No bare `*` (mosaic would alias it): select the target CTE's columns
	// explicitly — dims when the target composed grouped, then the projected
	// operand components (non-additive restricted views), then the value.
	const outCols = [
		...(targetGrouped ? dimColumns.map((d) => column(d)) : []),
		...projectedOperands.map((c) => column(c)),
		column("value"),
	];
	const sql = String(
		Query.with(ctes)
			.select(...outCols)
			.from(target.stepId),
	);
	return { sql, params, stepId: target.stepId };
}
