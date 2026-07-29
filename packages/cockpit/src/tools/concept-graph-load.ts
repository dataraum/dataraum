// Concept graph loader (DAT-737; served FROM the graph in DAT-671 R3) — the
// SERVER-ONLY half. Imports the metadata DB client, so it must NEVER be
// imported by a client component; the pure model + render live in
// `concept-graph.ts`, which the Model route's view imports instead (the same
// client/server split `operating-model-load.ts` documents for the metric graph).
//
// WHAT CHANGED IN R3, AND WHY. This loader used to fetch four Drizzle views and
// hand the rows to an in-memory `buildConceptGraph`, which re-derived — in
// TypeScript — the active-row filter, the edge-endpoint resolution, the
// grounding↔concept name join and the `part_of` closure. Every one of those
// answers is already PUBLISHED by the engine as a SQL/PGQ element view
// (ADR-0021), so the cockpit was the second author of a resolution it could
// simply read: exactly the pattern ADR-0024 unwinds ("each of the five
// questions has exactly one resolution home"). That rebuild is gone. What is
// served now comes out of the property graph itself, and the two consumers —
// the answer sub-agent's `<business_concepts>` block and the Model route's
// concept view — share this ONE path.
//
// PGQ executes IN POSTGRES, so the calling client's language is irrelevant
// (`../db/metadata/property-graph.ts`, DAT-671 R0). Its one REAL constraint is
// PG19's own: `MATCH` is FIXED-DEPTH, so the transitive `part_of` closure is a
// bounded recursive CTE over the edge view instead — ADR-0021's own closure
// mechanism, mirroring the engine's `_read_part_of_ancestry`.
//
// THREE READS ARE NOT MATCHES, each for a stated reason. None of them is a
// resolution; all three are projections the graph does not model:
//   · the concept DEFINITION text (description / indicators / exclude_patterns)
//     is not a `concept_node` PROPERTY — `og_concepts` projects
//     (concept_id, vertical, name, kind, ordering) — so it rides a 1:1 join on
//     the vertex's own primary key. Widening that element view is an engine
//     change; this join is keyed on identity, never on a name.
//   · a grounding's failure_mode / failure_reason live inside the provenance
//     JSON, not in vertex properties. The engine reads them the same way
//     (`context_reads.py::_read_grounding_provenance`).
//   · the reconciliation OBSERVATIONS have no element view at all — the graph
//     models the tie-out ASSERTION (a `concept_edge`), not a run's evaluation
//     of it. Same split engine-side.
//
// Workspace scoping: like the other reads in this package, no explicit
// workspace filter is applied — the read-role connection's `search_path`
// already resolves to this workspace's `ws_<id>_read` schema (DAT-816), and the
// element views inherit the read views' own scoping (`og_concepts` →
// `concepts`, vertical-scoped AND `superseded_at IS NULL`;
// `og_additivity` / `current_concept_reconciliation` → the promoted
// operating_model head). Nothing here re-states either filter.
//
// No dynamic value reaches any statement below — every MATCH is static
// pattern/label syntax — so `property-graph.ts`'s injection warning has no call
// site to bind here.

import { sql } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import { queryOperatingModelGraph } from "../db/metadata/property-graph";
import {
	type ConceptAdditivity,
	type ConceptGraph,
	type ConceptGraphNode,
	type ConceptGrounding,
	type ConceptReconciliation,
	compareGroundings,
	type DerivedMetric,
	foldReconciliations,
	parseWherePredicates,
	reconciliationFor,
} from "./concept-graph";

/** How many `part_of` hops the ancestry walk may take. MIRRORS the engine's
 *  `context_reads._PART_OF_MAX_DEPTH` — the same closure over the same edge
 *  view, so the two must not drift; the fixture seeds a chain one link LONGER
 *  than this, and `concept-graph-load.integration.test.ts` pins the boundary.
 *  Not a prompt-content bound: this is the cycle guard on a graph walk (a
 *  `part_of` typo loop must not hang the read), which is why it lives at the
 *  walk and not in config. */
const PART_OF_MAX_DEPTH = 4;

/** The definition text `og_concepts` does not project. Its own `async`
 *  function for the reason `loadConceptGraph` states: a synchronous throw
 *  inside a `Promise.all` argument list orphans its siblings. */
async function readConceptDefinitions() {
	return metadataDb.execute<{
		concept_id: string;
		description: string | null;
		indicators: unknown;
		exclude_patterns: unknown;
	}>(
		sql`SELECT concept_id::text AS concept_id, description, indicators,
		           exclude_patterns
		    FROM concepts WHERE superseded_at IS NULL`,
	);
}

/** `concept_node`, decorated with the definition text the vertex does not
 *  carry (see the header). Keyed on `concept_id`, the vertex's own key. */
async function readConcepts(): Promise<
	Array<{
		conceptId: string;
		name: string;
		kind: string | null;
		description: string | null;
		indicators: unknown;
		excludePatterns: unknown;
	}>
> {
	const [rows, definitions] = await Promise.all([
		queryOperatingModelGraph<{
			concept_id: string | null;
			name: string | null;
			kind: string | null;
		}>(
			sql.raw(
				"MATCH (c IS concept_node) " +
					"COLUMNS (c.concept_id AS concept_id, c.name AS name, c.kind AS kind)",
			),
		),
		readConceptDefinitions(),
	]);
	const byId = new Map(definitions.map((d) => [d.concept_id, d]));
	return rows.flatMap((r) => {
		if (!r.concept_id || !r.name) return [];
		const def = byId.get(r.concept_id);
		return [
			{
				conceptId: r.concept_id,
				name: r.name,
				kind: r.kind,
				description: def?.description ?? null,
				indicators: def?.indicators,
				excludePatterns: def?.exclude_patterns,
			},
		];
	});
}

/** Every `concept_edge`, endpoints already resolved to their concepts by the
 *  element view's own INNER JOIN — a superseded endpoint, or one naming no
 *  concept at all, yields no row here to drop. */
async function readConceptEdges(): Promise<
	Array<{
		from_name: string | null;
		predicate: string | null;
		tolerance: number | null;
		to_name: string | null;
	}>
> {
	return queryOperatingModelGraph(
		sql.raw(
			"MATCH (a IS concept_node)-[e IS concept_edge]->(b IS concept_node) " +
				"COLUMNS (a.name AS from_name, e.predicate AS predicate, " +
				"e.tolerance AS tolerance, b.name AS to_name)",
		),
	);
}

/** Transitive `part_of` ancestors per concept, depth 2..cap, nearest first.
 *
 *  ADR-0021's closure mechanism: PGQ `MATCH` is fixed-depth, so a closure is a
 *  bounded recursive CTE over the edge view. Cycle guard = the walk never
 *  re-enters a concept already on its path. The 1-hop parents come from the
 *  edge read above, so this returns only the strictly-transitive tail. */
async function readPartOfAncestry(): Promise<Map<string, string[]>> {
	const rows = await metadataDb.execute<{
		descendant: string;
		ancestor: string;
	}>(sql`
		WITH RECURSIVE part_of_walk AS (
			SELECT e.from_concept_id AS descendant_id,
			       e.to_concept_id AS ancestor_id,
			       1 AS depth,
			       ARRAY[e.from_concept_id, e.to_concept_id] AS path
			FROM og_concept_edges e
			WHERE e.predicate = 'part_of'
			UNION ALL
			SELECT w.descendant_id, e.to_concept_id, w.depth + 1,
			       w.path || e.to_concept_id
			FROM part_of_walk w
			JOIN og_concept_edges e ON e.from_concept_id = w.ancestor_id
			WHERE e.predicate = 'part_of'
			  AND w.depth < ${PART_OF_MAX_DEPTH}
			  AND e.to_concept_id <> ALL(w.path)
		)
		SELECT cd.name AS descendant, ca.name AS ancestor, MIN(w.depth) AS depth
		FROM part_of_walk w
		JOIN og_concepts cd ON cd.concept_id = w.descendant_id
		JOIN og_concepts ca ON ca.concept_id = w.ancestor_id
		GROUP BY cd.name, ca.name
		HAVING MIN(w.depth) >= 2
		ORDER BY cd.name, MIN(w.depth), ca.name`);
	const out = new Map<string, string[]>();
	for (const r of rows) {
		const list = out.get(r.descendant);
		if (list) list.push(r.ancestor);
		else out.set(r.descendant, [r.ancestor]);
	}
	return out;
}

/** A grounding's failure_mode / failure_reason — provenance JSON, not vertex
 *  properties. Its own `async` function for the same reason as the rest. */
async function readGroundingProvenance() {
	return metadataDb.execute<{
		snippet_id: string;
		failure_mode: string | null;
		failure_reason: string | null;
	}>(
		sql`SELECT snippet_id::text AS snippet_id,
		           provenance->>'failure_mode' AS failure_mode,
		           provenance->>'failure_reason' AS failure_reason
		    FROM current_groundings`,
	);
}

/** `(concept)-[grounded_by]->(grounding)`, merged with the failure detail the
 *  vertex cannot carry. A grounding whose concept is superseded or absent has
 *  no edge, so it never appears — the same INNER JOIN `concept-target.ts`
 *  relies on for the drill's identity. */
async function readGroundings(): Promise<Map<string, ConceptGrounding[]>> {
	const [rows, provenance] = await Promise.all([
		queryOperatingModelGraph<{
			concept_name: string | null;
			snippet_id: string | null;
			statement: string | null;
			relation: string | null;
			select_expr: string | null;
			where_predicates: string | null;
			failed: boolean | null;
		}>(
			sql.raw(
				"MATCH (c IS concept_node)-[e IS grounded_by]->(g IS grounding_node) " +
					"COLUMNS (c.name AS concept_name, g.snippet_id AS snippet_id, " +
					"g.statement AS statement, g.relation AS relation, " +
					"g.select_expr AS select_expr, " +
					"g.where_predicates AS where_predicates, g.failed AS failed)",
			),
		),
		readGroundingProvenance(),
	]);
	const detailById = new Map(provenance.map((p) => [p.snippet_id, p]));

	const byConcept = new Map<string, ConceptGrounding[]>();
	for (const r of rows) {
		if (!r.concept_name || !r.snippet_id) continue;
		const detail = detailById.get(r.snippet_id);
		const grounding: ConceptGrounding = {
			snippetId: r.snippet_id,
			statement: r.statement,
			relation: r.relation,
			selectExpr: r.select_expr,
			wherePredicates: parseWherePredicates(r.where_predicates),
			failed: r.failed ?? false,
			failureMode: detail?.failure_mode ?? null,
			failureReason: detail?.failure_reason ?? null,
		};
		const list = byConcept.get(r.concept_name);
		if (list) list.push(grounding);
		else byConcept.set(r.concept_name, [grounding]);
	}
	return byConcept;
}

/** `(concept)-[has_additivity]->(verdict)` — the measure verdicts the drill
 *  enforces, put in front of the agent that composes the SQL they will judge. */
async function readAdditivity(): Promise<Map<string, ConceptAdditivity[]>> {
	const rows = await queryOperatingModelGraph<{
		concept_name: string | null;
		axis_kind: string | null;
		axis_key: string | null;
		status: string | null;
		verdict: string | null;
		reason: string | null;
		abstain_reason: string | null;
		bucket_grain: string | null;
	}>(
		sql.raw(
			"MATCH (c IS concept_node)-[e IS has_additivity]->(a IS additivity_verdict) " +
				"COLUMNS (c.name AS concept_name, a.axis_kind AS axis_kind, " +
				"a.axis_key AS axis_key, a.status AS status, a.verdict AS verdict, " +
				"a.reason AS reason, a.abstain_reason AS abstain_reason, " +
				"a.bucket_grain AS bucket_grain)",
		),
	);
	const byConcept = new Map<string, ConceptAdditivity[]>();
	for (const r of rows) {
		// axis_kind/axis_key/status are NOT NULL on the base table; this guard
		// narrows the projection's nullable types and drops a row that could not
		// be read honestly rather than rendering half a verdict.
		if (!r.concept_name || !r.axis_kind || !r.axis_key || !r.status) continue;
		const verdict: ConceptAdditivity = {
			axisKind: r.axis_kind,
			axisKey: r.axis_key,
			status: r.status,
			verdict: r.verdict,
			reason: r.reason,
			abstainReason: r.abstain_reason,
			bucketGrain: r.bucket_grain,
		};
		const list = byConcept.get(r.concept_name);
		if (list) list.push(verdict);
		else byConcept.set(r.concept_name, [verdict]);
	}
	return byConcept;
}

/** `(metric)-[derives_from]->(concept)` — this edge's FIRST cockpit reader. */
async function readDerivedMetrics(): Promise<Map<string, DerivedMetric[]>> {
	const rows = await queryOperatingModelGraph<{
		concept_name: string | null;
		graph_id: string | null;
		metric_name: string | null;
		category: string | null;
		unit: string | null;
		output_type: string | null;
	}>(
		sql.raw(
			"MATCH (m IS metric_node)-[d IS derives_from]->(c IS concept_node) " +
				"COLUMNS (c.name AS concept_name, m.graph_id AS graph_id, " +
				"m.name AS metric_name, m.category AS category, m.unit AS unit, " +
				"m.output_type AS output_type)",
		),
	);
	const byConcept = new Map<string, DerivedMetric[]>();
	for (const r of rows) {
		if (!r.concept_name || !r.graph_id) continue;
		const metric: DerivedMetric = {
			graphId: r.graph_id,
			name: r.metric_name ?? r.graph_id,
			category: r.category,
			unit: r.unit,
			outputType: r.output_type,
		};
		const list = byConcept.get(r.concept_name);
		if (list) list.push(metric);
		else byConcept.set(r.concept_name, [metric]);
	}
	return byConcept;
}

/** The last promoted run's per-pair tie-out rows (DAT-739). No ORDER BY here:
 *  `foldReconciliations` sorts itself, so the fold stays deterministic for ANY
 *  caller — physical row order is not a tie-break. */
async function readReconciliations() {
	return metadataDb.execute<{
		from_concept: string;
		to_concept: string;
		pair_key: string;
		status: string;
		verdict: string | null;
		abstain_reason: string | null;
		delta: string | number | null;
		relative_delta: string | number | null;
	}>(
		sql`SELECT from_concept, to_concept, pair_key, status, verdict,
		           abstain_reason, delta, relative_delta
		    FROM current_concept_reconciliation`,
	);
}

/**
 * Read the concept vocabulary graph for the active workspace.
 *
 * Every list the engine sorts is sorted here to the SAME key, because that
 * determinism is what keeps the `<business_concepts>` block (the TAIL of a
 * `cache_control: ephemeral` system block) byte-stable across a run — without
 * it, DB physical-row-order drift would silently bust the whole cached prefix.
 *
 * An ungrounded, unclassified, edge-less concept is still a NODE, with empty
 * lists — born-loud, never hidden (the same contract
 * `buildOperatingModelGraph`'s ungrounded measure leaves hold).
 */
export async function loadConceptGraph(): Promise<ConceptGraph> {
	// EVERY reader below is `async`, and that is load-bearing rather than
	// stylistic: a non-async reader that threw SYNCHRONOUSLY while this array is
	// being built would escape before `Promise.all` ever attached a handler,
	// orphaning every sibling already in flight as an unhandled rejection (the
	// unit suite's soft-fail case caught exactly that). An `async` function can
	// only ever reject.
	const [
		conceptRows,
		edgeRows,
		ancestry,
		groundingsByConcept,
		additivityByConcept,
		metricsByConcept,
		reconciliationRows,
	] = await Promise.all([
		readConcepts(),
		readConceptEdges(),
		readPartOfAncestry(),
		readGroundings(),
		readAdditivity(),
		readDerivedMetrics(),
		readReconciliations(),
	]);

	const partOfParents = new Map<string, string[]>();
	const partOfChildren = new Map<string, string[]>();
	const disjointWith = new Map<string, string[]>();
	const reconcilesWith = new Map<string, ConceptReconciliation[]>();
	const folds = foldReconciliations(
		reconciliationRows.map((r) => ({
			fromConcept: r.from_concept,
			toConcept: r.to_concept,
			pairKey: r.pair_key,
			status: r.status,
			verdict: r.verdict,
			abstainReason: r.abstain_reason,
			delta: r.delta,
			relativeDelta: r.relative_delta,
		})),
	);

	const pushInto = (m: Map<string, string[]>, key: string, value: string) => {
		const list = m.get(key);
		if (list) list.push(value);
		else m.set(key, [value]);
	};

	for (const e of edgeRows) {
		if (!e.from_name || !e.to_name) continue;
		switch (e.predicate) {
			case "part_of":
				pushInto(partOfParents, e.from_name, e.to_name);
				pushInto(partOfChildren, e.to_name, e.from_name);
				break;
			case "disjoint_with":
				// The engine writes this symmetrically (one row per direction), so
				// accumulating each row under its from-side populates both endpoints.
				// Symmetrizing here would double it.
				pushInto(disjointWith, e.from_name, e.to_name);
				break;
			case "reconciles_with": {
				// The evaluated state rides the assertion it belongs to. Absent =
				// never evaluated, which the render must not report as agreement.
				const observed = reconciliationFor(folds, e.from_name, e.to_name);
				const list = reconcilesWith.get(e.from_name) ?? [];
				list.push({
					partner: e.to_name,
					tolerance: e.tolerance,
					status: observed?.status ?? null,
					verdict: observed?.verdict ?? null,
					abstainReason: observed?.abstainReason ?? null,
					observedDelta: observed?.observedDelta ?? null,
					relativeDelta: observed?.relativeDelta ?? null,
					pairs: observed?.pairs ?? 0,
					evaluatedPairs: observed?.evaluatedPairs ?? 0,
				});
				reconcilesWith.set(e.from_name, list);
				break;
			}
			default:
				// A predicate outside the three the engine's CHECK constraint allows:
				// dropped VISIBLY, never miscategorized into one of the known kinds.
				console.warn(
					`[cockpit] concept edge with unknown predicate "${e.predicate}" — dropped`,
				);
		}
	}

	// `indicators`/`exclude_patterns` are `json` columns — narrowed defensively
	// at the DB boundary (rule 11); anything that is not an array of strings
	// renders as empty rather than throwing.
	const asStringArray = (v: unknown): string[] =>
		Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];

	const nodes: ConceptGraphNode[] = conceptRows
		.map((c) => ({
			conceptId: c.conceptId,
			name: c.name,
			kind: c.kind,
			description: c.description,
			indicators: asStringArray(c.indicators),
			excludePatterns: asStringArray(c.excludePatterns),
			partOfParents: [...(partOfParents.get(c.name) ?? [])].sort(),
			partOfChildren: [...(partOfChildren.get(c.name) ?? [])].sort(),
			partOfAncestry: ancestry.get(c.name) ?? [],
			disjointWith: [...(disjointWith.get(c.name) ?? [])].sort(),
			reconcilesWith: [...(reconcilesWith.get(c.name) ?? [])].sort((a, b) =>
				a.partner.localeCompare(b.partner),
			),
			groundings: [...(groundingsByConcept.get(c.name) ?? [])].sort(
				compareGroundings,
			),
			additivity: [...(additivityByConcept.get(c.name) ?? [])].sort(
				(a, b) =>
					a.axisKind.localeCompare(b.axisKind) ||
					a.axisKey.localeCompare(b.axisKey),
			),
			derivedMetrics: [...(metricsByConcept.get(c.name) ?? [])].sort((a, b) =>
				a.graphId.localeCompare(b.graphId),
			),
		}))
		.sort((a, b) => a.name.localeCompare(b.name));

	return { nodes };
}
