// Concept graph — the vertical VOCABULARY's own structure (DAT-737), distinct
// from the metric DAG `operating-model-graph.ts` builds. Two SEPARATE graphs
// over the same operating model:
//   - metric graph (operating-model-graph.ts): metric → measure/constant/table,
//     composition + grounding-to-a-table edges.
//   - concept graph (this module): the business-concept VOCABULARY itself —
//     `part_of` (mereological containment, e.g. cash part_of current_assets),
//     `disjoint_with` (mutually-exclusive classification, e.g. asset vs
//     liability), `reconciles_with` (a tie-out assertion — see below), and
//     each concept's groundings (a concept can be measured on SEVERAL
//     relations — "multi-groundings").
//
// PARITY (DAT-737's own words: "the same graph the GraphAgent reads... one
// structure"): this mirrors the engine's traversal in
// `graphs/context_reads.py`/`context_format.py` (the ONLY place this
// neighbourhood is rendered today) — same fields, same wording where the data
// allows it. The engine reads `og_concepts`/`og_concept_edges` (SQL/PGQ
// element views, active rows only) via a bounded recursive CTE for `part_of`
// ancestry (depth 2..4); this module gets the same ANSWER over the same raw
// ingredient rows (`concepts`, `concept_edges`, `current_groundings` — all
// already Drizzle-mirrored) computed in memory instead, since the whole
// vocabulary is small enough to hold at once and TS has no PGQ.
//
// IDENTITY: concepts/concept_edges are NOT run-versioned (unlike most of this
// package's `current_*` views) — they're versioned by `superseded_at`
// (config-like: an edit supersedes-in-place, ADR-0010 doesn't apply). The
// cockpit's `concepts`/`concept_edges` Drizzle views do NOT filter
// `superseded_at IS NULL` server-side (unlike the engine's own
// `og_concepts`/`og_concept_edges`, which do) — so this module's builder
// filters superseded rows itself, matching the engine's semantics exactly.
// Edge endpoints are the concepts' stable `name` within `vertical` — NEVER
// `concept_id` (a per-seed surrogate; see `ConceptEdge`'s docstring) — so
// nodes here are keyed by name too.

export type ConceptEdgeKind = "part_of" | "disjoint_with" | "reconciles_with";

const CONCEPT_EDGE_KINDS: ReadonlySet<string> = new Set<ConceptEdgeKind>([
	"part_of",
	"disjoint_with",
	"reconciles_with",
]);

function isConceptEdgeKind(v: string): v is ConceptEdgeKind {
	return CONCEPT_EDGE_KINDS.has(v);
}

// --- Builder input (plain rows, so the assembly is DB-free and testable) ----

/** One `concepts` row (active or superseded — the builder filters). */
export interface ConceptRow {
	conceptId: string;
	name: string;
	kind: string | null;
	description: string | null;
	/** `unknown` at the DB boundary (a `json` column) — narrowed to a string
	 *  array by the builder; anything else renders as empty (rule 11). */
	indicators: unknown;
	excludePatterns: unknown;
	supersededAt: unknown;
}

/** One `concept_edges` row (active or superseded — the builder filters). */
export interface ConceptEdgeRow {
	edgeId: string;
	/** `string` at the DB boundary, like `AxisAdditivity.status` elsewhere in
	 *  this package — engine-enum-constrained at write, not re-asserted as a
	 *  union here. An unrecognized value is dropped by the builder (never
	 *  silently miscategorized into one of the three known kinds). */
	predicate: string;
	fromConcept: string;
	toConcept: string;
	tolerance: number | null;
	supersededAt: unknown;
}

/** One `current_groundings` row, matched to a concept by NAME (mirrors the
 *  engine's `og_grounded_by`: `current_groundings.concept = concepts.name`,
 *  active concept only). */
export interface GroundingRow {
	snippetId: string;
	concept: string;
	statement: string | null;
	relation: string | null;
	selectExpr: string | null;
	/** The declared WHERE predicates (DAT-838: business-term declaration data,
	 *  NOT generated SQL) as the view's `(parts -> 'where')::text` JSON-text
	 *  cast — parsed defensively by `parseWherePredicates`, never regex'd. */
	wherePredicates: string | null;
	failed: boolean;
}

export interface ConceptGraphInput {
	concepts: ConceptRow[];
	edges: ConceptEdgeRow[];
	groundings: GroundingRow[];
}

// --- Graph model -------------------------------------------------------------

/** One `reconciles_with` verdict on a concept. A self-loop (`partner` equals
 *  the concept's own name) is the common case — DAT-727's derivation asserts
 *  it whenever a concept holds ≥2 healthy groundings, or a grounded measure
 *  reconciles against a witnessed aggregation-lineage rollup: "this concept's
 *  own computations must tie out." A distinct partner is a seed/declared
 *  cross-concept assertion (e.g. a balance-sheet total reconciling against a
 *  trial-balance total). */
export interface ConceptReconciliation {
	partner: string;
	tolerance: number | null;
}

/** One healthy or failed prior grounding of a concept. */
export interface ConceptGrounding {
	snippetId: string;
	statement: string | null;
	relation: string | null;
	selectExpr: string | null;
	wherePredicates: string[];
	failed: boolean;
}

export interface ConceptGraphNode {
	/** `concept:<name>` — name-keyed, matching the edge/grounding join key. */
	id: string;
	conceptId: string;
	name: string;
	kind: string | null;
	description: string | null;
	indicators: string[];
	excludePatterns: string[];
	/** 1-hop `part_of` targets (this concept IS PART OF these). */
	partOfParents: string[];
	/** 1-hop `part_of` sources (these concepts ARE PART OF this one). */
	partOfChildren: string[];
	/** Transitive `part_of` ancestry beyond the 1-hop parents, nearest-first,
	 *  bounded at `PART_OF_ANCESTRY_DEPTH` hops — mirrors the engine's bounded
	 *  recursive CTE (ADR-0021's closure mechanism), never an unbounded walk. */
	partOfAncestry: string[];
	disjointWith: string[];
	reconcilesWith: ConceptReconciliation[];
	/** Every prior grounding of this concept — ZERO is a valid, honest state
	 *  (an ungrounded concept still appears as a node; "no ungrounded-node
	 *  regressions" — never dropped, never silently hidden). */
	groundings: ConceptGrounding[];
}

export interface ConceptGraphEdge {
	id: string;
	source: string;
	target: string;
	kind: ConceptEdgeKind;
	tolerance: number | null;
}

export interface ConceptGraph {
	nodes: ConceptGraphNode[];
	edges: ConceptGraphEdge[];
}

const conceptNodeId = (name: string): string => `concept:${name}`;

/** Bounded transitive `part_of` ancestry, beyond the 1-hop parents: depth
 *  2..N hops, nearest-first, cycle-safe. Mirrors the engine's recursive-CTE
 *  depth cap (ADR-0021) — the whole vocabulary is in memory here, so an
 *  unbounded walk would be easy to write and wrong to ship (a `part_of` typo
 *  loop must not hang the render). */
const PART_OF_ANCESTRY_DEPTH = 3;

function asStringArray(v: unknown): string[] {
	if (!Array.isArray(v)) return [];
	return v.filter((x): x is string => typeof x === "string");
}

/** Parse a grounding's declared WHERE predicates — a JSON array of business-
 *  term predicate strings (DAT-838's `ExtractGroundingOutput.where`), never
 *  raw SQL text. The view casts the JSON value to text; a null/absent value
 *  or anything that fails to parse as a string array renders as "no declared
 *  predicate" rather than throwing. */
export function parseWherePredicates(raw: string | null): string[] {
	if (!raw || raw === "null") return [];
	try {
		return asStringArray(JSON.parse(raw));
	} catch {
		return [];
	}
}

/**
 * Assemble the concept vocabulary graph from already-fetched rows. Pure: no
 * DB, no IO. Contracts:
 *  - Every ACTIVE concept is a node, regardless of grounding status — an
 *    ungrounded concept still appears, with an empty `groundings` list
 *    (born-loud, same contract as `buildOperatingModelGraph`'s ungrounded
 *    measure leaves).
 *  - An edge with a superseded endpoint, an endpoint naming no active
 *    concept, or an unrecognized predicate is DROPPED, never thrown on —
 *    same defensive posture as the metric graph's dangling-reference
 *    handling (a stale/malformed row must degrade the render, not crash it).
 *  - `disjoint_with` is read directionally (one row per direction) — the
 *    engine writes it symmetrically (both directions), so no client-side
 *    symmetrization is needed; duplicating it here would double the effort
 *    for the same answer.
 */
export function buildConceptGraph(input: ConceptGraphInput): ConceptGraph {
	const activeConcepts = input.concepts.filter((c) => c.supersededAt === null);
	const byName = new Map(activeConcepts.map((c) => [c.name, c]));

	const activeEdges = input.edges.filter(
		(e) =>
			e.supersededAt === null &&
			isConceptEdgeKind(e.predicate) &&
			byName.has(e.fromConcept) &&
			byName.has(e.toConcept),
	);

	const partOfParents = new Map<string, string[]>();
	const partOfChildren = new Map<string, string[]>();
	const disjointWith = new Map<string, string[]>();
	const reconcilesWith = new Map<string, ConceptReconciliation[]>();

	const pushInto = (m: Map<string, string[]>, key: string, value: string) => {
		const list = m.get(key);
		if (list) list.push(value);
		else m.set(key, [value]);
	};

	for (const e of activeEdges) {
		// isConceptEdgeKind already narrowed e.predicate — safe to switch.
		switch (e.predicate as ConceptEdgeKind) {
			case "part_of":
				pushInto(partOfParents, e.fromConcept, e.toConcept);
				pushInto(partOfChildren, e.toConcept, e.fromConcept);
				break;
			case "disjoint_with":
				pushInto(disjointWith, e.fromConcept, e.toConcept);
				break;
			case "reconciles_with": {
				const list = reconcilesWith.get(e.fromConcept) ?? [];
				list.push({ partner: e.toConcept, tolerance: e.tolerance });
				reconcilesWith.set(e.fromConcept, list);
				break;
			}
		}
	}

	const ancestryOf = (name: string): string[] => {
		const out: string[] = [];
		const seen = new Set<string>([name]);
		let frontier = partOfParents.get(name) ?? [];
		for (const p of frontier) seen.add(p);
		for (
			let hop = 0;
			hop < PART_OF_ANCESTRY_DEPTH && frontier.length > 0;
			hop++
		) {
			const next: string[] = [];
			for (const parent of frontier) {
				for (const grandparent of partOfParents.get(parent) ?? []) {
					if (seen.has(grandparent)) continue;
					seen.add(grandparent);
					next.push(grandparent);
				}
			}
			out.push(...next);
			frontier = next;
		}
		return out;
	};

	const groundingsByName = new Map<string, ConceptGrounding[]>();
	for (const g of input.groundings) {
		if (!byName.has(g.concept)) continue;
		const grounding: ConceptGrounding = {
			snippetId: g.snippetId,
			statement: g.statement,
			relation: g.relation,
			selectExpr: g.selectExpr,
			wherePredicates: parseWherePredicates(g.wherePredicates),
			failed: g.failed,
		};
		const list = groundingsByName.get(g.concept);
		if (list) list.push(grounding);
		else groundingsByName.set(g.concept, [grounding]);
	}

	const nodes: ConceptGraphNode[] = activeConcepts.map((c) => ({
		id: conceptNodeId(c.name),
		conceptId: c.conceptId,
		name: c.name,
		kind: c.kind,
		description: c.description,
		indicators: asStringArray(c.indicators),
		excludePatterns: asStringArray(c.excludePatterns),
		partOfParents: partOfParents.get(c.name) ?? [],
		partOfChildren: partOfChildren.get(c.name) ?? [],
		partOfAncestry: ancestryOf(c.name),
		disjointWith: disjointWith.get(c.name) ?? [],
		reconcilesWith: reconcilesWith.get(c.name) ?? [],
		groundings: groundingsByName.get(c.name) ?? [],
	}));

	const edges: ConceptGraphEdge[] = activeEdges.map((e) => ({
		id: e.edgeId,
		source: conceptNodeId(e.fromConcept),
		target: conceptNodeId(e.toConcept),
		kind: e.predicate as ConceptEdgeKind,
		tolerance: e.tolerance,
	}));

	return { nodes, edges };
}

// --- Answer-agent prompt rendering (DAT-737: parity with the engine's own
// concept-neighbourhood render, `graphs/context_format.py::_append_concepts`)
// -----------------------------------------------------------------------------

/** One healthy grounding, formatted `statement @ relation: select_expr WHERE
 *  ...` — mirrors the engine's `_format_grounding` wording exactly. */
function formatGrounding(g: ConceptGrounding): string {
	const label =
		g.statement && g.relation
			? `${g.statement} @ ${g.relation}`
			: (g.relation ?? g.statement ?? "(unresolved relation)");
	const withExpr = g.selectExpr ? `${label}: ${g.selectExpr}` : label;
	return g.wherePredicates.length > 0
		? `${withExpr} WHERE ${g.wherePredicates.join(" AND ")}`
		: withExpr;
}

/**
 * Format the concept graph as the answer sub-agent's `<business_concepts>`
 * prompt block (pure). Empty graph → "" (the block is OMITTED entirely, the
 * same convention `conventionsBlock`/`grainBlock` already use in
 * `query.ts` — never an empty-but-present tag). Concepts sorted by name for a
 * deterministic prompt (cache-friendly, matching `stableContext`'s other
 * blocks).
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
			"must tie out.",
		"",
	];

	const sorted = [...graph.nodes].sort((a, b) => a.name.localeCompare(b.name));
	for (const c of sorted) {
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
			const tol = rec.tolerance !== null ? ` (tolerance ${rec.tolerance})` : "";
			lines.push(
				rec.partner === c.name
					? `  - reconciles: across its own groundings${tol} — must tie out`
					: `  - reconciles with: ${rec.partner}${tol}`,
			);
		}

		const healthy = c.groundings.filter((g) => !g.failed);
		const failed = c.groundings.filter((g) => g.failed);
		if (healthy.length > 0) {
			lines.push("  - grounded by:");
			for (const g of healthy) lines.push(`    - ${formatGrounding(g)}`);
		}
		if (failed.length > 0)
			lines.push(
				`  - ${failed.length} failed grounding attempt(s) not shown here`,
			);
	}
	lines.push("</business_concepts>");
	return lines.join("\n");
}
