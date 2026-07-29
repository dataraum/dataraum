// The IDENTITY question of the drill, answered once (DAT-671 R2, ADR-0024
// decision 1) — SERVER-ONLY: imports the metadata client, so never import it
// from a client component.
//
// "Which concept is this number?" is the hinge the whole drill turns on: the
// additivity verdict that licenses a time bucket is keyed by
// `(target_kind, target_key)`, and for a measure that key IS the concept —
// `standard_field`, as the read views spell out (`current_groundings.concept =
// sql_snippets.standard_field`). A canvas node knows its target by
// construction. An ANSWER did not: the sub-agent's declaration carries clause
// parts and a model-chosen step name, and the name is display text — the model
// picks it, so keying a verdict on it would be exactly the name-join fragility
// ADR-0024 exists to close.
//
// So identity rides the SNIPPET ID — the id `classifyComponents` already
// resolved when the model declared reuse — and this module walks the one hop
// the graph already models: `(concept)-[grounded_by]->(grounding)`. That hop
// is a real edge (`og_grounded_by`, ADR-0021), INNER-joined to the ACTIVE
// concept row, so a superseded concept or a grounding for a concept the
// ontology no longer carries resolves to nothing rather than to a stale key.
//
// WHY THE GRAPH AND NOT A DRIZZLE JOIN: the same reason the edge exists. The
// mirror could join `sql_snippets` to `concepts` by name, which is the join
// the graph view already performs — restating it here would make the cockpit
// the second author of a resolution the engine publishes, which is the pattern
// this epic is unwinding. The verdict READ stays where it is
// (`readTargetAdditivity`, one home already); this adds no second reader for
// it — note that `og_has_additivity` could not serve one anyway, since it
// filters `target_kind = 'measure'` and so cannot speak for a metric target.

import { type SQL, sql } from "drizzle-orm";

import { queryOperatingModelGraph } from "#/db/metadata/property-graph";

/**
 * Resolve grounding snippet ids to the concepts they ground: `snippet_id →
 * concept` (= `standard_field`, the measure verdict's `target_key`).
 *
 * A snippet with no entry in the returned map is UNRESOLVED, which is a
 * first-class answer and must stay one: the id was hallucinated, the snippet
 * is not a graph-authored extract (`og_grounding` selects only
 * `snippet_type = 'extract' AND source LIKE 'graph:%'`), its concept is not in
 * the active ontology, or the grounding is RETAINED-FAILED. Callers turn that
 * into a withhold with a stated reason — never into a guess at a neighbouring
 * concept.
 *
 * Failed groundings are excluded IN the MATCH: a `failure_count > 0` snippet is
 * a row every other consumer skips, and letting one license a time grain would
 * be reading a verdict for a computation we know does not run.
 *
 * The ids are bound as real parameters through NESTED `sql` fragments, per
 * `property-graph.ts`'s injection warning — `sql.raw` here carries only static
 * pattern/label syntax.
 */
export async function resolveGroundedConcepts(
	snippetIds: readonly string[],
): Promise<Map<string, string>> {
	const ids = [...new Set(snippetIds.filter((id) => id !== ""))];
	if (ids.length === 0) return new Map();

	const idFilter: SQL = sql.join(
		ids.map((id) => sql`g.snippet_id = ${id}`),
		sql` OR `,
	);
	const rows = await queryOperatingModelGraph<{
		snippet_id: string | null;
		concept: string | null;
	}>(
		sql`MATCH (c IS concept_node)-[e IS grounded_by]->(g IS grounding_node
		      WHERE (${idFilter}) AND g.failed = false)
		    COLUMNS (g.snippet_id AS snippet_id, c.name AS concept)`,
	);

	const byId = new Map<string, string>();
	for (const row of rows) {
		if (!row.snippet_id || !row.concept) continue;
		// First edge wins, deterministically: `uq_concept_active` makes
		// (vertical, name) unique among live concepts and the read view is
		// vertical-scoped, so one grounding cannot resolve to two concepts —
		// this guard is the belt over that brace, never a pick.
		if (!byId.has(row.snippet_id)) byId.set(row.snippet_id, row.concept);
	}
	return byId;
}
