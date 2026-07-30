// Coverage-map loader (DAT-855 B2) — the SERVER-ONLY half. Reads the workspace-native
// rows `buildCoverageMap` needs and hands them over untouched; the pure module owns
// every decision (module header there has the full rationale).
//
// Gated on the promoted operating_model head, mirroring `operating-model-load.ts`:
// `metrics`/`concepts` are vertical-scoped config (visible with or without a run), but
// `current_lifecycle_artifacts`/`current_concept_reconciliation` are head-joined
// (ADR-0008) — without a promoted run every metric would look `declared`-with-no-row
// and the map would render an honest but noisy "nothing grounded anywhere" wall. The
// `analyzed` flag lets the route show "no operating model yet" instead, the same
// distinction `LoadOperatingModelResult.analyzed` draws.
//
// tsc-bounded, test-unexecuted (the convention `operating-model-load.ts` sets): this
// is thin DB-read glue with no branching of its own; `buildCoverageMap` carries the
// behaviour and the tests.

import { and, eq, like } from "drizzle-orm";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import {
	readLifecycleArtifactRows,
	readOperatingModelHead,
} from "../db/metadata/lifecycle-artifacts";
import {
	concepts,
	currentConceptReconciliation,
	currentGroundings,
	metrics,
	sqlSnippets,
} from "../db/metadata/schema";
import {
	buildCoverageMap,
	type CoverageGroundingInput,
	type CoverageMap,
} from "./coverage-map";

export interface LoadCoverageResult {
	/** False until the operating_model stage has a promoted run (page shows "not run"),
	 *  same distinction `LoadOperatingModelResult.analyzed` draws. */
	analyzed: boolean;
	map: CoverageMap;
}

const EMPTY_MAP = buildCoverageMap({
	metrics: [],
	concepts: [],
	lifecycle: [],
	groundings: [],
	reconciliation: [],
});

/** The `graph:<id>` snippet source → the metric's graph_id (mirrors `operating-model-
 *  load.ts`'s own `graphIdOf` — kept local rather than shared, since that module's
 *  copy is a private helper and this one has different callers). */
const graphIdOf = (source: string): string => {
	const idx = source.indexOf(":");
	return idx === -1 ? source : source.slice(idx + 1);
};

export async function loadCoverageMap(): Promise<LoadCoverageResult> {
	const head = await readOperatingModelHead();
	if (!head) return { analyzed: false, map: EMPTY_MAP };

	const [
		metricRows,
		conceptRows,
		lifecycleRows,
		snippetRows,
		groundingRows,
		reconciliationRows,
	] = await Promise.all([
		metadataDb
			.select({
				graphId: metrics.graphId,
				name: metrics.name,
				dimensionFacet: metrics.dimensionFacet,
			})
			.from(metrics),
		metadataDb
			.select({
				name: concepts.name,
				kind: concepts.kind,
				dimensionFacet: concepts.dimensionFacet,
			})
			.from(concepts),
		readLifecycleArtifactRows("metric"),
		// The metric's own grounding evidence: every graph:%-sourced snippet, grouped
		// by its graph_id in the pure builder (module header there, point 3).
		metadataDb
			.select({
				snippetId: sqlSnippets.snippetId,
				source: sqlSnippets.source,
				snippetType: sqlSnippets.snippetType,
				failureCount: sqlSnippets.failureCount,
				provenance: sqlSnippets.provenance,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
				),
			),
		// Display-only enrichment (resolved period / calendar source) for the SAME
		// extract rows above, joined back by snippet_id below — this view carries no
		// `source` column, so it cannot itself be grouped by metric graph_id.
		metadataDb
			.select({
				snippetId: currentGroundings.snippetId,
				resolvedPeriod: currentGroundings.resolvedPeriod,
				calendarSource: currentGroundings.calendarSource,
			})
			.from(currentGroundings),
		metadataDb
			.select({
				fromConcept: currentConceptReconciliation.fromConcept,
				toConcept: currentConceptReconciliation.toConcept,
				status: currentConceptReconciliation.status,
				verdict: currentConceptReconciliation.verdict,
			})
			.from(currentConceptReconciliation),
	]);

	const extrasBySnippetId = new Map(
		groundingRows
			.filter((r): r is typeof r & { snippetId: string } =>
				Boolean(r.snippetId),
			)
			.map((r) => [r.snippetId, r]),
	);

	const groundings: CoverageGroundingInput[] = snippetRows
		.filter((r): r is typeof r & { source: string } => Boolean(r.source))
		.map((r) => {
			const extra = r.snippetId
				? extrasBySnippetId.get(r.snippetId)
				: undefined;
			return {
				graphId: graphIdOf(r.source),
				snippetType: r.snippetType ?? "",
				failed: (r.failureCount ?? 0) > 0,
				provenance: r.provenance,
				resolvedPeriod: extra?.resolvedPeriod ?? null,
				calendarSource: extra?.calendarSource ?? null,
			};
		});

	const map = buildCoverageMap({
		metrics: metricRows
			.filter((r): r is typeof r & { graphId: string } => Boolean(r.graphId))
			.map((r) => ({
				graphId: r.graphId,
				name: r.name ?? r.graphId,
				dimensionFacet: r.dimensionFacet,
			})),
		concepts: conceptRows
			.filter((r): r is typeof r & { name: string } => Boolean(r.name))
			.map((r) => ({
				name: r.name,
				kind: r.kind ?? "",
				dimensionFacet: r.dimensionFacet,
			})),
		lifecycle: lifecycleRows.map((r) => ({
			graphId: r.artifactKey,
			state: r.state,
			stateReason: r.stateReason,
		})),
		groundings,
		// Only SELF-LOOP rows speak to "the same concept's own groundings agree" —
		// the spec's stated scenario (see `CoverageReconciliationInput`'s own doc in
		// coverage-map.ts). A partner-edge row (two DIFFERENT concepts tying out,
		// e.g. trial-balance vs GL) is a different claim.
		reconciliation: reconciliationRows
			.filter(
				(r): r is typeof r & { fromConcept: string; toConcept: string } =>
					Boolean(r.fromConcept) && r.fromConcept === r.toConcept,
			)
			.map((r) => ({
				concept: r.fromConcept,
				status: r.status,
				verdict: r.verdict,
			})),
	});

	return { analyzed: true, map };
}
