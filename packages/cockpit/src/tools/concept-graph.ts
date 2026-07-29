// Concept graph — the vertical VOCABULARY's own structure (DAT-737), distinct
// from the metric DAG `operating-model-graph.ts` builds. Two SEPARATE graphs
// over the same operating model:
//   - metric graph (operating-model-graph.ts): metric → measure/constant/table,
//     composition + grounding-to-a-table edges.
//   - concept graph (this module): the business-concept VOCABULARY itself —
//     `part_of` (mereological containment, e.g. cash part_of current_assets),
//     `disjoint_with` (mutually-exclusive classification, e.g. asset vs
//     liability), `reconciles_with` (a tie-out assertion), and each concept's
//     groundings (a concept can be measured on SEVERAL relations —
//     "multi-groundings"), plus — since DAT-671 R3 — the additivity VERDICTS
//     the drill enforces on it and the metrics whose DAG derives from it.
//
// THE MODEL AND THE RENDER ONLY. This module is pure and client-safe (the Model
// route's `ConceptGraphView` imports it into the browser bundle, so it must
// never reach for the DB client). The graph is READ in `concept-graph-load.ts`,
// which since DAT-671 R3 serves this shape straight out of the operating-model
// property graph — the in-memory rebuild that used to live here is gone, and
// with it the cockpit's second authorship of a resolution the engine already
// publishes (ADR-0024: one resolution home, keyed on identity).
//
// PARITY (DAT-737's own words: "the same graph the GraphAgent reads... one
// structure"): the served neighbourhood mirrors the engine's own
// (`graphs/context_reads.py` / `context_format.py`) — concept definition
// fields, the part_of/disjoint_with/reconciles_with structure, deterministic
// ordering (`compareGroundings` etc. below match `context_reads.py`'s sort keys
// key-for-key), and each grounding INCLUDING its failure_mode/failure_reason.
// ONE deliberate gap remains: the per-grounding column-level `uses:` breakdown
// (`og_uses` — which measure/filter columns a grounding reads) is not modeled
// here, because no reader needs it and a served fact ships with its consumer or
// not at all (ADR-0024 decision 3).
//
// IDENTITY: concepts/concept_edges are NOT run-versioned (unlike most of this
// package's `current_*` views) — they're versioned by `superseded_at`
// (config-like: an edit supersedes-in-place, ADR-0010 doesn't apply). Both the
// active-row filter and the edge-endpoint resolution now happen SERVER-SIDE, in
// the `og_concepts` / `og_concept_edges` element views — see the loader.

// --- Graph model -------------------------------------------------------------

/** One `reconciles_with` verdict on a concept. A self-loop (`partner` equals
 *  the concept's own name) is the common case — DAT-727's derivation asserts
 *  it whenever a concept holds ≥2 healthy groundings, or a grounded measure
 *  reconciles against a witnessed aggregation-lineage rollup: "this concept's
 *  own computations must tie out." A distinct partner is a seed/declared
 *  cross-concept assertion (e.g. a balance-sheet total reconciling against a
 *  trial-balance total).
 *
 *  The fields below the assertion carry what the last PROMOTED run OBSERVED
 *  when it executed both sides (DAT-739), folded from its per-pair rows —
 *  mirrors the engine's `ConceptReconciliation` context model exactly.
 *  `status === null` means never evaluated, which is a different statement
 *  from "evaluated and found consistent" and must be rendered as one.
 *  `observedDelta` NEVER implies a failure on its own: with no declared
 *  `tolerance` there is no band to have missed. */
export interface ConceptReconciliation {
	partner: string;
	tolerance: number | null;
	/** `'evaluated'` | `'abstained'` | `null` (never evaluated). */
	status: string | null;
	verdict: string | null;
	/** Set only when NO pair was evaluated and every abstention agreed. */
	abstainReason: string | null;
	observedDelta: number | null;
	relativeDelta: number | null;
	pairs: number;
	evaluatedPairs: number;
}

/** One raw `current_concept_reconciliation` row (the last promoted run's
 *  per-pair tie-out evaluation, DAT-739). Numeric columns arrive as strings
 *  from the driver (drizzle `numeric`) — the fold converts.
 *
 *  Read from the view rather than from the graph because the property graph
 *  models the ASSERTION (a `concept_edge` with predicate `reconciles_with`),
 *  not a run's OBSERVATION of it: there is no `og_*` element view for these
 *  rows and minting one is an engine change. The engine's own context read
 *  does exactly the same (`context_reads.py::_read_reconciliation_rows`). */
export interface ReconciliationRow {
	fromConcept: string;
	toConcept: string;
	pairKey: string;
	status: string;
	verdict: string | null;
	abstainReason: string | null;
	delta: string | number | null;
	relativeDelta: string | number | null;
}

/** One healthy or failed prior grounding of a concept. A HEALTHY grounding
 *  with `relation === null` is a real but distinct third state — see
 *  `isReusableGrounding` — the engine skips it from reuse entirely (a pre-parts
 *  row it can't address), so neither consumer here treats it as an ordinary
 *  reusable grounding either. */
export interface ConceptGrounding {
	snippetId: string;
	statement: string | null;
	relation: string | null;
	selectExpr: string | null;
	wherePredicates: string[];
	failed: boolean;
	failureMode: string | null;
	failureReason: string | null;
}

/** One persisted additivity verdict for the concept AS A MEASURE (DAT-857/868,
 *  reaching this context in DAT-671 R3).
 *
 *  These are the very rows the drill gates on: `og_has_additivity` is
 *  `target_kind = 'measure'` INNER-JOINed to the ACTIVE concept, i.e. the same
 *  `current_metric_axis_additivity` rows `drill-axes.ts::readTargetAdditivity`
 *  reads for a measure target, reached here through the identity spine rather
 *  than a second name key. Serving them is what stops the answer agent
 *  composing SQL blind to a verdict it will then be judged by.
 *
 *  Metric-target verdicts are deliberately absent: a metric is not a concept
 *  and `og_has_additivity` cannot speak for one. `derivedMetrics` is the edge
 *  that connects the two. */
export interface ConceptAdditivity {
	/** `'time'` | `'categorical'` — engine vocabulary, not re-asserted as a
	 *  union here (same posture as `ConceptReconciliation.status`). */
	axisKind: string;
	/** A served column NAME, or `AXIS_KEY_ALL` for the class-level verdict. */
	axisKey: string;
	/** `'classified'` | `'abstained'`. */
	status: string;
	verdict: string | null;
	reason: string | null;
	abstainReason: string | null;
	/** Time axes only (the engine CHECKs that): the finest cadence the verdict
	 *  licenses a bucket at. */
	bucketGrain: string | null;
}

/** One operating-model metric whose DAG derives from this concept — DAT-732's
 *  `derives_from` edge, one per distinct extract `standard_field`.
 *
 *  That edge had NO cockpit reader at all before DAT-671 R3; the answer agent
 *  is its first, which is what lets it see that a concept it is about to
 *  compute is already a leaf of a metric this workspace defines. */
export interface DerivedMetric {
	graphId: string;
	name: string;
	category: string | null;
	unit: string | null;
	outputType: string | null;
}

export interface ConceptGraphNode {
	/** The `concepts` primary key — this node's stable identity and the key the
	 *  graph's own edges reference (ADR-0024 decision 1: ids, not names). The
	 *  NAME stays display text and the vocabulary the agent reasons in. */
	conceptId: string;
	name: string;
	kind: string | null;
	description: string | null;
	indicators: string[];
	excludePatterns: string[];
	/** 1-hop `part_of` targets (this concept IS PART OF these), sorted. */
	partOfParents: string[];
	/** 1-hop `part_of` sources (these concepts ARE PART OF this one), sorted. */
	partOfChildren: string[];
	/** Transitive `part_of` ancestry beyond the 1-hop parents, nearest-first
	 *  (NOT sorted — the order is the walk's own distance, matching the
	 *  engine's `part_of_ancestry` field), bounded by the loader's recursive-CTE
	 *  depth cap (ADR-0021's closure mechanism), never an unbounded walk. */
	partOfAncestry: string[];
	/** Sorted. */
	disjointWith: string[];
	/** Sorted by partner name. */
	reconcilesWith: ConceptReconciliation[];
	/** Every prior grounding of this concept, sorted `(failed, relation,
	 *  snippetId)` (matching `context_reads.py`'s `groundings` sort key
	 *  exactly, so DB physical-row order can never drift the answer). ZERO is
	 *  a valid, honest state (an ungrounded concept still appears as a node;
	 *  "no ungrounded-node regressions" — never dropped, never silently
	 *  hidden). */
	groundings: ConceptGrounding[];
	/** Sorted `(axisKind, axisKey)`. EMPTY means the engine classified nothing
	 *  for this concept — a different statement from "not additive", and one
	 *  the render makes by saying nothing rather than by inventing a verdict. */
	additivity: ConceptAdditivity[];
	/** Sorted by `graphId`. */
	derivedMetrics: DerivedMetric[];
}

export interface ConceptGraph {
	/** Sorted by name — the ONE place this ordering is decided; no consumer
	 *  should re-sort (this is also why the cached `<business_concepts>`
	 *  prompt block stays byte-stable across a run: DB row order can never
	 *  leak into it). */
	nodes: ConceptGraphNode[];
}

/** The class-row sentinel — must match `additivity_db_models.AXIS_KEY_ALL`.
 *  `drill-axes.ts` holds its own copy of the same literal; there is deliberately
 *  NO cross-file equality test between the two, because agreeing with each
 *  other would prove nothing — what matters is agreeing with the ENGINE. This
 *  side is pinned against the engine's real rows instead:
 *  `concept-graph-load.integration.test.ts` asserts the `axisKey` that comes
 *  back from `og_has_additivity` is exactly this value.
 *
 *  Mirrored rather than imported because `drill-axes.ts` reaches for the
 *  metadata client, and this module is pulled into the CLIENT bundle by the
 *  Model route's concept view. */
export const AXIS_KEY_ALL = "*";

/** Parse a grounding's declared WHERE predicates — a JSON array of business-
 *  term predicate strings (DAT-838's `ExtractGroundingOutput.where`), never
 *  raw SQL text. `og_grounding.where_predicates` is the view's
 *  `(parts -> 'where')::text` cast, so this arrives as JSON TEXT; a
 *  null/absent value, or anything that fails to parse as a string array,
 *  renders as "no declared predicate" rather than throwing. */
export function parseWherePredicates(raw: string | null): string[] {
	if (!raw || raw === "null") return [];
	try {
		const parsed: unknown = JSON.parse(raw);
		if (!Array.isArray(parsed)) return [];
		return parsed.filter((x): x is string => typeof x === "string");
	} catch {
		return [];
	}
}

/** A healthy grounding with no resolved relation can't be reused as a prior
 *  computation — the engine SKIPS it entirely (with a warn,
 *  `context_reads.py`'s `grounding_relation_missing`, e.g. a pre-parts
 *  snippet row) rather than rendering it as an ordinary grounding. Both
 *  consumers here honor the same distinction (a failed grounding is handled
 *  on its own separate path regardless of `relation`). */
export function isReusableGrounding(g: ConceptGrounding): boolean {
	return !g.failed && g.relation !== null;
}

/** `context_reads.py`'s exact groundings sort key: `(failed, relation or "",
 *  snippet_id)` — Python's `False < True`, so healthy groundings sort before
 *  failed ones. Applied here rather than in SQL because the rows are merged
 *  with the provenance read before they are complete. */
export function compareGroundings(
	a: ConceptGrounding,
	b: ConceptGrounding,
): number {
	if (a.failed !== b.failed) return a.failed ? 1 : -1;
	const relA = a.relation ?? "";
	const relB = b.relation ?? "";
	if (relA !== relB) return relA < relB ? -1 : 1;
	if (a.snippetId !== b.snippetId) return a.snippetId < b.snippetId ? -1 : 1;
	return 0;
}

// --- The reconciliation fold -------------------------------------------------

/** The observed half of a `ConceptReconciliation` before an edge claims it. */
export type ReconciliationFold = Omit<
	ConceptReconciliation,
	"partner" | "tolerance"
>;

// NUL as pair-key separator: Postgres text can never contain \0, so any
// concept name round-trips unambiguously (a space or dash could not promise
// that).
const reconciliationFoldKey = (frm: string, to: string): string =>
	`${frm}\u0000${to}`;

const asNumber = (v: string | number | null): number | null =>
	v === null ? null : Number(v);

/** Fold the per-pair rows into one observation per `(concept, partner)` —
 *  mirrors the engine's `context_reads.py::_read_reconciliation_rows` exactly:
 *  an assertion covering several grounding pairs reports how many were
 *  comparable and the WIDEST relative divergence among them (strict `>`, ties
 *  first-seen — which is why the sort below matters: physical row order is not
 *  a tie-break); when nothing was comparable, the shared abstention reason is
 *  carried only if every pair agreed on it. A partner assertion is stored once
 *  under name-ordered endpoints but read from BOTH directions' edge rows, so
 *  the mirror key is registered last — dropping that line re-introduces the
 *  bug where one endpoint renders "not yet evaluated" for an assertion that
 *  WAS evaluated. The engine sorts in SQL; this fold sorts here so it stays
 *  deterministic for any caller. */
export function foldReconciliations(
	rows: ReconciliationRow[],
): Map<string, ReconciliationFold> {
	const sorted = [...rows].sort(
		(a, b) =>
			a.fromConcept.localeCompare(b.fromConcept) ||
			a.toConcept.localeCompare(b.toConcept) ||
			a.pairKey.localeCompare(b.pairKey),
	);
	const folded = new Map<string, ReconciliationFold>();
	const reasons = new Map<string, Set<string | null>>();
	for (const r of sorted) {
		const key = reconciliationFoldKey(r.fromConcept, r.toConcept);
		let acc = folded.get(key);
		if (!acc) {
			acc = {
				status: null,
				verdict: null,
				abstainReason: null,
				observedDelta: null,
				relativeDelta: null,
				pairs: 0,
				evaluatedPairs: 0,
			};
			folded.set(key, acc);
			reasons.set(key, new Set());
		}
		acc.pairs += 1;
		if (r.status !== "evaluated") {
			reasons.get(key)?.add(r.abstainReason);
			continue;
		}
		acc.evaluatedPairs += 1;
		acc.status = "evaluated";
		const current = asNumber(r.relativeDelta) ?? 0;
		if (acc.relativeDelta === null || current > acc.relativeDelta) {
			acc.relativeDelta = current;
			acc.observedDelta = asNumber(r.delta);
			acc.verdict = r.verdict;
		}
	}
	for (const [key, acc] of folded) {
		if (acc.status === null) {
			acc.status = "abstained";
			const rs = reasons.get(key) ?? new Set();
			acc.abstainReason = rs.size === 1 ? [...rs][0] : null;
		}
	}
	for (const [key, acc] of [...folded.entries()]) {
		const [frm, to] = key.split("\u0000");
		const mirror = reconciliationFoldKey(to, frm);
		if (!folded.has(mirror)) folded.set(mirror, acc);
	}
	return folded;
}

/** The fold's lookup key for one `(concept, partner)` assertion — exported so
 *  the loader attaches an observation to the edge it belongs to without
 *  re-deriving the key spelling. */
export function reconciliationFor(
	folds: Map<string, ReconciliationFold>,
	concept: string,
	partner: string,
): ReconciliationFold | undefined {
	return folds.get(reconciliationFoldKey(concept, partner));
}

// --- Answer-agent prompt rendering (DAT-737: parity with the engine's own
// concept-neighbourhood render, `graphs/context_format.py::_append_concepts`)
// -----------------------------------------------------------------------------

/** One healthy, reusable grounding, formatted `statement @ relation:
 *  select_expr WHERE ...` — mirrors the engine's `_format_grounding` wording
 *  exactly. Only called on groundings `isReusableGrounding` already passed,
 *  so `relation` is never null here. */
function formatGrounding(g: ConceptGrounding): string {
	const label =
		g.statement && g.relation
			? `${g.statement} @ ${g.relation}`
			: (g.relation ?? "");
	const withExpr = g.selectExpr ? `${label}: ${g.selectExpr}` : label;
	return g.wherePredicates.length > 0
		? `${withExpr} WHERE ${g.wherePredicates.join(" AND ")}`
		: withExpr;
}

/** Python `%.Ng` mirror — the engine renders every reconciliation number with
 *  `:g`/`:.3g`, so this render must agree digit-for-digit or the two documented
 *  parity blocks drift on the very numbers they exist to report. Python's rule:
 *  exponential when `exp < -4` or `exp >= precision`, trailing zeros stripped,
 *  exponent at least two digits. */
export function pyG(x: number, precision = 6): string {
	if (x === 0) return "0";
	if (!Number.isFinite(x)) return String(x);
	const exp = Math.floor(Math.log10(Math.abs(x)));
	if (exp < -4 || exp >= precision) {
		const [mantRaw, eRaw] = x.toExponential(precision - 1).split("e");
		const mant = mantRaw.includes(".")
			? mantRaw.replace(/\.?0+$/, "")
			: mantRaw;
		const sign = eRaw.startsWith("-") ? "-" : "+";
		const digits = eRaw.replace(/[+-]/, "").padStart(2, "0");
		return `${mant}e${sign}${digits}`;
	}
	let out = x.toFixed(Math.max(0, precision - 1 - exp));
	if (out.includes(".")) out = out.replace(/\.?0+$/, "");
	return out;
}

/** Why a tie-out was not computed — mirrors `context_format.py`'s
 *  `_ABSTAIN_PHRASING` key-for-key. */
const ABSTAIN_PHRASING: Record<string, string> = {
	no_evaluable_pair: "only one grounding exists to measure",
	different_reporting_instants:
		"the groundings are bound to different reporting instants",
	different_aggregations: "the groundings aggregate differently",
	unresolved_grounding: "a grounding has no executable form",
	execution_failed: "a grounding failed to execute",
	no_value: "a grounding measured no support",
	non_numeric_value: "a grounding returned a value that is not a quantity",
};

/** What the last promoted run observed for one assertion — mirrors the
 *  engine's `_reconciliation_state` branch-for-branch. The distinction this
 *  wording holds: an assertion nobody has evaluated is NOT an assertion that
 *  held, and a delta observed with no declared tolerance is a MEASUREMENT,
 *  never a graded failure. A partial evaluation must never read as a whole
 *  one, so the un-compared remainder rides every verdict. */
export function reconciliationState(rec: ConceptReconciliation): string {
	if (rec.status === null) return "must tie out (not yet evaluated)";
	if (rec.status === "abstained") {
		const reason =
			(rec.abstainReason !== null
				? ABSTAIN_PHRASING[rec.abstainReason]
				: undefined) ?? "no comparable pair was found";
		return `must tie out; not compared because ${reason}`;
	}
	let scope =
		rec.evaluatedPairs > 1 ? ` (widest of ${rec.evaluatedPairs} pairs)` : "";
	const uncompared = rec.pairs - rec.evaluatedPairs;
	if (uncompared > 0)
		scope += `; ${uncompared} of ${rec.pairs} pairs not comparable`;
	const relative = pyG(rec.relativeDelta ?? 0, 3);
	if (rec.verdict === "beyond_tolerance")
		return `evaluated: ${relative} relative divergence exceeds the tolerance${scope}`;
	if (rec.verdict === "within_tolerance")
		return `evaluated: ties out within tolerance, ${relative} relative${scope}`;
	if (!rec.observedDelta)
		return `evaluated: the groundings tie out exactly${scope}`;
	return (
		`evaluated: observed delta ${pyG(rec.observedDelta)} (${relative} relative)${scope}` +
		" — no tolerance is declared, so this is a measurement, not a failure"
	);
}

/** One persisted axis verdict, in the ENGINE's own tokens (DAT-671 R3).
 *
 * Deliberately NOT the drill's practitioner prose (`drill-axes.ts`'s
 * `describeReason`/`describeAbstention`): that wording explains a withheld menu
 * affordance to a person looking at a grid, whereas this reader is a SQL author
 * who needs the classification itself. Passing the raw verdict/reason tokens
 * through also means nothing here can drift from the engine's vocabulary,
 * because nothing is translated on the way.
 *
 * `axis_key = '*'` is the CLASS-level row ("every axis of this kind"); a
 * concrete key REFINES it for one served column, so that column is named when
 * there is one. An `abstained` row means nobody judged it — wording that must
 * never be readable as a "no". */
export function formatAdditivity(a: ConceptAdditivity): string {
	const axis =
		a.axisKey === AXIS_KEY_ALL
			? `any ${a.axisKind} axis`
			: `${a.axisKind} axis "${a.axisKey}"`;
	if (a.status !== "classified") {
		const why = a.abstainReason ?? "no reason recorded";
		return `${axis} — NOT CLASSIFIED (${why}); nobody judged this, which is not the same as a no`;
	}
	const reason = a.reason ? ` (${a.reason})` : "";
	const grain = a.bucketGrain
		? `, bucketable no finer than ${a.bucketGrain}`
		: "";
	return `${axis} — ${a.verdict ?? "unknown"}${reason}${grain}`;
}

/** One metric this concept feeds — `graph_id` first, because that id is what
 *  every other surface keys a metric by. */
function formatDerivedMetric(m: DerivedMetric): string {
	const facts = [m.category, m.outputType].filter((f): f is string => !!f);
	const unit = m.unit ? ` in ${m.unit}` : "";
	const detail = facts.length > 0 ? ` (${facts.join(", ")}${unit})` : unit;
	return `${m.graphId}${detail}`;
}

/**
 * Format the concept graph as the answer sub-agent's `<business_concepts>`
 * prompt block (pure). Empty graph → "" (the block is OMITTED entirely, the
 * same convention `conventionsBlock`/`grainBlock` already use in
 * `query.ts` — never an empty-but-present tag). `graph.nodes` arrives already
 * sorted by name (the loader's contract) — this function does NOT re-sort, so
 * the deterministic order is decided once, upstream. That determinism is what
 * keeps this block (the TAIL of a `cache_control: ephemeral` system block)
 * byte-stable across a run.
 *
 * NO CAPS, deliberately: everything rendered here is a metadata enumeration
 * (concepts, groundings, verdicts, metrics), which must fail honestly at freak
 * length rather than truncate silently.
 *
 * DELIBERATE WORDING ADAPTATION: the engine's own render points the model at
 * "the Value sets below" (`context_format.py`'s own comment) — a value-set
 * catalog section that exists in ITS prompt. This cockpit's answer sub-agent
 * doesn't carry an equivalent standalone value-set block; its per-column
 * `[meaning:]` tags on the `<schema>` block (`query-context.ts::formatSchema`)
 * are the closest analogue, so the wording below points there instead. Same
 * intent (ground the concept in real column values, don't improvise a
 * substring filter), different concrete pointer — not an oversight.
 */
export function formatConceptContext(graph: ConceptGraph): string {
	if (graph.nodes.length === 0) return "";

	const lines: string[] = [
		"<business_concepts>",
		"Vertical vocabulary with its operating-model graph. Ground each metric " +
			"concept in specific column values from the schema's [meaning:] tags — " +
			"match by meaning, honoring `exclude` patterns below; do not improvise a " +
			"substring filter. A `grounded by` entry is a PRIOR COMMITTED grounding of " +
			"that concept — reuse its relation/expression for the same concept unless " +
			"the served evidence says it is wrong; a concept with several groundings is " +
			"measured on several relations, and `reconciles` means those computations " +
			"must tie out — each entry states whether the last completed run " +
			"actually checked that, and what it observed.",
		"",
		"`additivity` is the engine's own verdict on aggregating that concept as a " +
			"measure, per axis class, and it is what the drill ENFORCES on a result " +
			"built from it: `additive` sums across the axis; `semi_additive` and " +
			"`non_additive_recompute` do NOT (the parenthesized reason says why) — for " +
			"those never SUM across periods, take the latest period or re-evaluate per " +
			"bucket. A concept with no `additivity` line carries no verdict at all: " +
			"state that limitation rather than assuming either answer. `feeds metric` " +
			"names the operating-model metrics whose definition derives from this " +
			"concept — reusing that concept's committed grounding is what keeps your " +
			"answer consistent with them.",
		"",
	];

	for (const c of graph.nodes) {
		let line = `- ${c.name}`;
		if (c.kind) line += ` (${c.kind})`;
		if (c.description) line += `: ${c.description}`;
		lines.push(line);

		if (c.indicators.length > 0)
			lines.push(`  - indicators: ${c.indicators.join(", ")}`);
		if (c.excludePatterns.length > 0)
			lines.push(`  - exclude: ${c.excludePatterns.join(", ")}`);
		if (c.partOfParents.length > 0) {
			let partOf = c.partOfParents.join(", ");
			if (c.partOfAncestry.length > 0)
				partOf += ` (→ ${c.partOfAncestry.join(" → ")})`;
			lines.push(`  - part of: ${partOf}`);
		}
		if (c.partOfChildren.length > 0)
			lines.push(`  - subconcepts: ${c.partOfChildren.join(", ")}`);
		if (c.disjointWith.length > 0)
			lines.push(`  - disjoint with: ${c.disjointWith.join(", ")}`);
		for (const rec of c.reconcilesWith) {
			const tol =
				rec.tolerance !== null ? ` (tolerance ${pyG(rec.tolerance)})` : "";
			const subject =
				rec.partner === c.name
					? "reconciles: across its own groundings"
					: `reconciles with: ${rec.partner}`;
			lines.push(`  - ${subject}${tol} — ${reconciliationState(rec)}`);
		}
		for (const a of c.additivity)
			lines.push(`  - additivity: ${formatAdditivity(a)}`);
		for (const m of c.derivedMetrics)
			lines.push(`  - feeds metric: ${formatDerivedMetric(m)}`);

		// Match the engine's skip (context_reads.py's grounding_relation_missing):
		// a healthy grounding with no relation can't be reused, so it's excluded
		// from "grounded by:" — but the skip is DISCLOSED (a count), never a
		// silent drop, matching the failed-attempts collapse just below.
		const reusable = c.groundings.filter(isReusableGrounding);
		const nonReusable = c.groundings.filter(
			(g) => !g.failed && g.relation === null,
		);
		const failed = c.groundings.filter((g) => g.failed);
		if (reusable.length > 0) {
			lines.push("  - grounded by:");
			for (const g of reusable) lines.push(`    - ${formatGrounding(g)}`);
		}
		if (nonReusable.length > 0)
			lines.push(
				`  - ${nonReusable.length} grounding(s) recorded with no relation — ` +
					"not reusable, omitted",
			);
		for (const g of failed) {
			const mode = g.failureMode ?? "failed";
			const reason = g.failureReason ?? "(no reason recorded)";
			lines.push(`  - failed attempt [${mode}]: ${reason}`);
		}
	}
	lines.push("</business_concepts>");
	return lines.join("\n");
}
