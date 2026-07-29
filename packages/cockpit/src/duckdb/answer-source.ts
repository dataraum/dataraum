// Parts-at-source for an ANSWER (DAT-678) — the answer sub-agent's declared
// clause parts, PROVEN against the answer's own number before anything is
// allowed to drill from them.
//
// WHY a declaration and not a derivation: the drill needs to know where a
// number came from — the relation, the value expression, the predicates — in
// order to recompose it grouped by a dimension the answer never projected (a
// scalar "total revenue is 4.2M" has no columns for tier A to group by; the
// dimension only exists upstream, in the extract). The engine gets this for
// free: its GraphAgent AUTHORS every extract as clause parts and persists them
// (`sql_snippets.parts`). The answer sub-agent authors free-form SQL, so the
// same fact has to come from the same place — the model's own typed output —
// never from parsing the SQL back apart. Deriving typed data from generated
// SQL is exactly the shortcut this codebase forbids: it turns a text-shaped
// guess into a fact the UI then presents as grounded.
//
// WHY a declaration is nonetheless not TRUSTED: a required field is a field
// the model will fill even when it shouldn't. So a declaration is a CANDIDATE
// until `proveAnswerSource` executes it and compares the recomposed scalar to
// the value the answer actually reported (`IS NOT DISTINCT FROM`, plus a
// NOT NULL floor so two NULLs can't agree their way into a proof). Anything
// else — a hallucinated relation, a dropped WHERE, a final_sql that isn't the
// arithmetic the model claimed, a multi-row result that can't be a scalar at
// all — fails the comparison or fails to bind, and the answer simply has no
// parts. No parts is not a degraded state: tier A is the fallback, and on a
// row-set answer it is the BETTER path (the dimensions are right there in the
// result).
//
// The composition itself is `composeNodeQuery` verbatim — the same builder,
// the same absence doctrine, the same grain handling as a canvas node. This
// module only decides WHAT tree to hand it and whether the tree earned the
// right to exist.

import type { DuckDBConnection } from "@duckdb/node-api";

import { errorLine } from "./drill-sql";
import { parseFormulaExpression } from "./metric-formula";
import {
	type ComposedNodeQuery,
	composeNodeQuery,
	composeNodeTotals,
	type NodeDrill,
	type NodeStep,
	type SnippetParts,
} from "./parts";

/** An answer's clause parts. Same shape the engine persists, with `relation`
 *  NARROWED to non-null: the engine's null-relation "fall-loud" extract is a
 *  grounding failure it still wants to render, whereas an answer step that
 *  reads no relation simply has nothing to slice and abstains instead. */
export interface AnswerSourceParts extends SnippetParts {
	relation: string;
}

/** One declared step source: the step's CTE name, its clause parts, and — when
 *  the step declared REUSE of a curated snippet — that snippet's id.
 *
 *  The two fields answer different questions and must not be confused
 *  (ADR-0024 decision 1). `name` is the model's own CTE label: it is what the
 *  combining `expression` references, and it is DISPLAY text — the model picks
 *  it, so nothing may key on it. `snippetId` is IDENTITY: `classifyComponents`
 *  resolved it against the snippet library (a hallucinated id was already
 *  cleared to null), and it is the entry point to the spine
 *  `snippet → standard_field → verdict target` that decides whether this
 *  number may be bucketed by period. `null` — a FRESH step — is a first-class
 *  outcome: the drill then has no classified concept and withholds the grain
 *  with a stated reason, exactly as it does for any unclassified computation. */
export interface AnswerSource {
	name: string;
	snippetId: string | null;
	parts: AnswerSourceParts;
}

/**
 * A PROVEN parts-at-source handle for one answer. `expression` is the
 * closed-grammar arithmetic (metric-formula.ts) combining the sources — a bare
 * ref for the single-concept case. Carried on the answer result and threaded to
 * the canvas so the grid can recompose upstream; never persisted (the chat
 * canvas is ephemeral).
 */
export interface AnswerDrillSource {
	sources: AnswerSource[];
	expression: string;
}

/** The model's declared source for ONE step, as the tool contract states it.
 *  Empty `relation`/`valueExpr` is the declared ABSTENTION — "this step is not
 *  a single-relation aggregate" — and is the honest answer for a join, a
 *  window, or a multi-column projection. */
export interface DeclaredSource {
	relation: string;
	valueExpr: string;
	filters: string[];
}

/**
 * Reduce a declared relation to the BARE name the rest of the drill speaks.
 *
 * The answer sub-agent is told to address tables as `lake.<layer>.<name>`, and
 * it must keep doing so — its `final_sql` genuinely needs the qualified form,
 * and switching conventions midway through one output is the kind of rule a
 * model drops silently. So the qualified form is correct on the wire and wrong
 * everywhere it lands, and this is where the two meet:
 *
 *   - `Query.from("lake.typed.orders")` (mosaic-sql) quotes the WHOLE string as
 *     one identifier — `FROM "lake.typed.orders"` — which is a Catalog Error,
 *     so a qualified declaration could never bind and the proof could never
 *     pass. The feature was inert in production while every test was green,
 *     because the tests used bare names.
 *   - `current_enriched_views.view_name` is bare, so the axes resolver's
 *     `viewByName` lookup missed too — and reported the miss as "reads
 *     relations outside the current analysis", blaming a stale snippet for a
 *     format mismatch.
 *
 * Reducing to the last segment is the same normalization `canonicalizeForReuse`
 * applies for snippet matching, and it is safe for the same reason: engine
 * scope is `USE lake.typed`, where the enriched views live, so a bare name
 * resolves to exactly what the qualified one named. If it does not, the
 * composition fails to bind and the answer falls back to tier A — never a
 * silently different table.
 *
 * Refused rather than reduced: a quoted identifier (already-escaped text this
 * has no business rewriting) and more than three segments (not an address this
 * convention produces — guessing at it would be inventing a table).
 */
export function bareRelationName(relation: string): string | null {
	const trimmed = relation.trim();
	if (trimmed === "") return null;
	if (/["'`]/.test(trimmed)) return null;
	const parts = trimmed.split(".");
	if (parts.length > 3 || parts.some((p) => p.trim() === "")) return null;
	return parts[parts.length - 1].trim();
}

/**
 * Narrow one declared source to clause parts, or null when the model abstained
 * (or answered with something unusable). Mirrors `narrowSnippetParts`'s posture
 * at the persisted-parts boundary: structure only, no interpretation — the
 * VALUE proof is what decides whether the structure is also true. The one
 * transformation is the relation reduction above, which is a FORMAT conversion
 * between two spellings of the same table, not an interpretation of it.
 *
 * `relation: null` (the engine's fall-loud "no relation" shape) is deliberately
 * NOT reachable from a declaration: an answer step that reads nothing has
 * nothing to slice, so it abstains instead.
 */
export function narrowDeclaredSource(
	declared: DeclaredSource,
): AnswerSourceParts | null {
	const relation = bareRelationName(declared.relation);
	const selectExpr = declared.valueExpr.trim();
	if (relation === null || selectExpr === "") return null;
	const where = declared.filters
		.map((f) => f.trim())
		.filter((f) => f.length > 0);
	return { selectExpr, relation, where };
}

/** One declared source as it arrives on the WIRE — the shape `/api/drill/parts`
 *  receives, which is `AnswerSourceParts` spelling (`selectExpr`/`where`) rather
 *  than the model's (`value_expr`/`filters`), because the client is echoing back
 *  a handle this server produced. */
export interface WireSource {
	name: string;
	snippetId?: string | null;
	parts: { selectExpr: string; relation: string; where: string[] };
}

/**
 * Accept a drill request's wire sources as a composable candidate, or refuse by
 * name (DAT-671).
 *
 * The relation reduction has exactly ONE home — `bareRelationName`, reached
 * through `narrowDeclaredSource` — and this is how the request path reaches it.
 * `/api/drill/parts` used to build `SnippetParts` straight off the parsed body,
 * on the belief that a qualified name would resolve under the engine's scope.
 * It does not: mosaic-sql's `Query.from("lake.typed.x")` quotes the WHOLE string
 * as one identifier, so `USE lake.typed` never gets a chance to resolve it and
 * the composition dies at bind time as an unexplained refusal. The route was
 * safe only by accident — the client happens to echo back an already-reduced,
 * already-proven handle — which is precisely the kind of safety that disappears
 * the first time another caller appears.
 *
 * A source that cannot narrow is REFUSED rather than dropped: dropping it would
 * compose a different calculation than the one asked for (the expression would
 * reference an operand that is no longer there), and the user would be shown a
 * number nobody requested.
 */
export function acceptWireSources(
	sources: WireSource[],
	expression: string,
): AnswerDrillSource | { refusal: string } {
	const accepted: AnswerSource[] = [];
	for (const source of sources) {
		const parts = narrowDeclaredSource({
			relation: source.parts.relation,
			valueExpr: source.parts.selectExpr,
			filters: source.parts.where,
		});
		if (parts === null) {
			return {
				refusal: `'${source.name}' does not name a relation and a value this drill can recompose`,
			};
		}
		accepted.push({
			name: source.name,
			snippetId: source.snippetId ?? null,
			parts,
		});
	}
	return { sources: accepted, expression };
}

/** The synthetic step id the combining formula composes under. Kept off the
 *  model's naming space: it is de-collided against the declared source names,
 *  which are the only other ids in the tree. */
function combiningStepId(taken: ReadonlySet<string>): string {
	let id = "answer";
	while (taken.has(id)) id = `_${id}`;
	return id;
}

/**
 * The `NodeStep` tree a declared source composes as (pure). One extract per
 * source; the combining arithmetic rides a formula step on top UNLESS the
 * expression is a bare reference to a single source — in which case the extract
 * IS the output, exactly the shape `resolveNodeSteps` builds for a bare measure
 * (and the shape whose unrestricted scalar stays byte-parity with the engine's
 * own render).
 *
 * Returns null when the declaration cannot form a tree at all: no sources, an
 * off-grammar expression, or an expression referencing something that was never
 * declared. `composeNodeQuery` would refuse these too — refusing here keeps a
 * malformed candidate from ever reaching the proof.
 */
export function answerNodeSteps(source: AnswerDrillSource): NodeStep[] | null {
	if (source.sources.length === 0) return null;
	const parsed = parseFormulaExpression(source.expression);
	if ("refusal" in parsed) return null;

	const names = source.sources.map((s) => s.name);
	if (new Set(names).size !== names.length) return null;

	// A bare ref to a declared source: that extract is the whole node.
	if (parsed.expr.kind === "ref") {
		const refName = parsed.expr.name;
		const only = source.sources.find((s) => s.name === refName);
		if (!only) return null;
		return [
			{
				stepId: only.name,
				kind: "extract",
				parts: only.parts,
				expression: null,
				value: null,
				dependsOn: [],
				outputStep: true,
			},
		];
	}

	const outputId = combiningStepId(new Set(names));
	return [
		...source.sources.map<NodeStep>((s) => ({
			stepId: s.name,
			kind: "extract",
			parts: s.parts,
			expression: null,
			value: null,
			dependsOn: [],
			outputStep: false,
		})),
		{
			stepId: outputId,
			kind: "formula",
			parts: null,
			expression: source.expression,
			value: null,
			// Every declared source is a declared dependency — and ONLY those. The
			// composer's fabrication guard then refuses any ref the model did not
			// declare a source for, instead of composing a phantom operand.
			dependsOn: names,
			outputStep: true,
		},
	];
}

/** Compose an answer's parts-at-source statement under a drill (pure). */
export function composeAnswerSource(
	source: AnswerDrillSource,
	drill: NodeDrill = { slices: [], pins: [] },
): ComposedNodeQuery | { refusal: string } {
	const steps = answerNodeSteps(source);
	if (steps === null) {
		return {
			refusal:
				"this answer's declared source doesn't form a composable calculation",
		};
	}
	return composeNodeQuery(steps, undefined, drill);
}

/**
 * The answer's FOOTER statement (pure; DAT-671 R2): its unrestricted scalar
 * with the operand components projected alongside `value`, exactly as
 * `composeNodeTotals` does for a canvas node.
 *
 * The undrilled total is not a node-path privilege — it is the number the
 * practitioner started from, and a drilled grid that cannot print it makes
 * them navigate away to check whether the parts still add up. The node route
 * has shipped it since DAT-712; this is the same statement for the same
 * reason, so both surfaces read one composition rather than two.
 *
 * Unlike the node path this is NOT restricted to the open call: an answer grid
 * has no "open" — the widget composes for the first time when the first drill
 * is applied — so the footer must ride every composition or it never appears.
 */
export function composeAnswerTotals(
	source: AnswerDrillSource,
): ComposedNodeQuery | { refusal: string } {
	const steps = answerNodeSteps(source);
	if (steps === null) {
		return {
			refusal:
				"this answer's declared source doesn't form a composable calculation",
		};
	}
	return composeNodeTotals(steps, undefined);
}

/**
 * The PROOF statement (pure): does the declared source, recomposed and run,
 * reproduce the answer's own number?
 *
 * Both sides are scalar subqueries on purpose. A `<answerSql>` that returns
 * more than one row or more than one column is not a scalar answer at all, and
 * DuckDB says so as a bind/execution error — which is precisely the eligibility
 * gate we want, obtained without parsing either statement. `IS NOT DISTINCT
 * FROM` makes NULL comparable rather than unknown; the extra NOT NULL floor
 * stops the degenerate proof where a hallucinated relation returns NULL and an
 * empty answer returns NULL and the two "agree".
 *
 * The comparison is EXACT, including for floats. Two plans that sum the same
 * DOUBLE column in a different order can disagree in the last bits, and such a
 * declaration is rejected. That is the correct failure: it costs a drill
 * affordance and falls back to tier A, which is the honest fallback. Do NOT
 * "fix" it with an epsilon — a tolerance here would be a threshold nobody
 * calibrated, quietly admitting declarations that are wrong by a little in
 * order to admit ones that are right by a rounding bit, and the whole point of
 * this gate is that it never guesses.
 */
export function answerSourceProofSql(
	scalarSql: string,
	answerSql: string,
): string {
	return (
		`SELECT (${scalarSql}) IS NOT DISTINCT FROM "answer"."value" ` +
		`AND "answer"."value" IS NOT NULL AS proven ` +
		`FROM (SELECT (${answerSql}) AS "value") AS "answer"`
	);
}

/** Run the proof on a connection the caller scoped. Any failure — bind error,
 *  non-scalar answer, disagreement — is `false`: the candidate is dropped, and
 *  the answer drills tier A. Never throws. */
export async function runAnswerSourceProof(
	conn: DuckDBConnection,
	scalarSql: string,
	answerSql: string,
): Promise<boolean> {
	try {
		const reader = await conn.runAndReadAll(
			answerSourceProofSql(scalarSql, answerSql),
		);
		const [row] = reader.getRowObjectsJson();
		return row?.proven === true;
	} catch (err) {
		console.info("answer_source_proof_failed", { reason: errorLine(err) });
		return false;
	}
}

/**
 * The candidate's unrestricted scalar, ready for the proof — or null when it
 * cannot compose at all. A pinned/sliced composition binds params; the
 * unrestricted scalar never does, so a param here would mean the composer
 * changed under us and the candidate is not what this function promises.
 */
export function answerSourceScalarSql(
	candidate: AnswerDrillSource,
): string | null {
	const composed = composeAnswerSource(candidate);
	if ("refusal" in composed) return null;
	return composed.params.length > 0 ? null : composed.sql;
}
