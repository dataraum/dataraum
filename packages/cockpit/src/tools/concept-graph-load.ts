// Concept graph loader (DAT-737) — the SERVER-ONLY half. Reads the already-
// mirrored `concepts` / `concept_edges` / `current_groundings` Drizzle views
// and hands the rows to the pure `buildConceptGraph`. Imports the metadata DB
// client, so it must NEVER be imported by a client component — the same
// client/server split `operating-model-load.ts` documents for the metric
// graph.
//
// No new mirror needed: `concepts`/`conceptEdges`/`currentGroundings` were
// already pulled by prior lanes this epic (DAT-728/838); the engine's
// `og_concepts`/`og_concept_edges`/`og_grounding` SQL/PGQ element views exist
// only for the engine's own PGQ traversal and are never pulled into Drizzle —
// this loader gets the SAME answer from their raw ingredient rows instead
// (the whole vocabulary is small; no PGQ needed in TS).
//
// Workspace scoping: like the other `current_*` reads in this package
// (`query-context.ts`'s `buildSchemaBlock` etc.), no explicit workspace
// filter is applied here — the read-role connection's search_path already
// resolves to this workspace's `ws_<id>_read` schema (DAT-816), and
// `concepts`/`conceptEdges` additionally self-scope to the active vertical
// server-side. `superseded_at` filtering is NOT done by these views (unlike
// the engine's own `og_concepts`/`og_concept_edges`), so it happens in
// `buildConceptGraph` instead.

import { metadataDb } from "../db/metadata/client";
import {
	conceptEdges,
	concepts,
	currentGroundings,
} from "../db/metadata/schema";
import { buildConceptGraph, type ConceptGraph } from "./concept-graph";

export async function loadConceptGraph(): Promise<ConceptGraph> {
	const [conceptRows, edgeRows, groundingRows] = await Promise.all([
		metadataDb
			.select({
				conceptId: concepts.conceptId,
				name: concepts.name,
				kind: concepts.kind,
				description: concepts.description,
				indicators: concepts.indicators,
				excludePatterns: concepts.excludePatterns,
				supersededAt: concepts.supersededAt,
			})
			.from(concepts),
		metadataDb
			.select({
				edgeId: conceptEdges.edgeId,
				predicate: conceptEdges.predicate,
				fromConcept: conceptEdges.fromConcept,
				toConcept: conceptEdges.toConcept,
				tolerance: conceptEdges.tolerance,
				supersededAt: conceptEdges.supersededAt,
			})
			.from(conceptEdges),
		metadataDb
			.select({
				snippetId: currentGroundings.snippetId,
				concept: currentGroundings.concept,
				statement: currentGroundings.statement,
				relation: currentGroundings.relation,
				selectExpr: currentGroundings.selectExpr,
				wherePredicates: currentGroundings.wherePredicates,
				failed: currentGroundings.failed,
			})
			.from(currentGroundings),
	]);

	return buildConceptGraph({
		concepts: conceptRows.map((c) => ({
			conceptId: c.conceptId ?? "",
			name: c.name ?? "",
			kind: c.kind ?? null,
			description: c.description ?? null,
			indicators: c.indicators,
			excludePatterns: c.excludePatterns,
			supersededAt: c.supersededAt,
		})),
		edges: edgeRows.map((e) => ({
			edgeId: e.edgeId ?? "",
			predicate: e.predicate ?? "",
			fromConcept: e.fromConcept ?? "",
			toConcept: e.toConcept ?? "",
			tolerance: e.tolerance ?? null,
			supersededAt: e.supersededAt,
		})),
		groundings: groundingRows.map((g) => ({
			snippetId: g.snippetId ?? "",
			concept: g.concept ?? "",
			statement: g.statement ?? null,
			relation: g.relation ?? null,
			selectExpr: g.selectExpr ?? null,
			wherePredicates: g.wherePredicates ?? null,
			failed: g.failed ?? false,
		})),
	});
}
