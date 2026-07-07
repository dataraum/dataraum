// Metric-drill part resolution (DAT-702) — the SERVER-ONLY read behind the
// `/api/drill/node` route. Resolves a metric key to its DAG steps plus the
// newest ACCEPTED extract snippet per standard field; `composeMetricNodeSql`
// (the TS mirror of the engine's deterministic composer) rebuilds the opened
// node's subtree from them, ad hoc. Never import from a client component.
//
// A missing extract snippet is NOT refused here: the step carries `sql: null`
// and the composer refuses — by name — only when the hole is actually
// reachable from the opened node. A hole elsewhere in the DAG never blocks
// the node the user asked for (the DAT-699 partial-execution posture).

import { and, desc, eq, inArray, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import { currentLifecycleArtifacts, sqlSnippets } from "#/db/metadata/schema";
import type { MetricDrillStep } from "#/duckdb/metric-compose";

import { parseMetricDag } from "./operating-model-graph";

export type MetricDrillParts =
	| { steps: MetricDrillStep[] }
	| { missing: string };

/** The metric's DAG steps with each extract's newest accepted snippet SQL
 *  attached (null = hole). `missing` covers only the metric itself — no
 *  definition, or an unparseable one. */
export async function resolveMetricDrillSteps(
	metricKey: string,
): Promise<MetricDrillParts> {
	const [row] = await metadataDb
		.select({ dag: currentLifecycleArtifacts.graphDefinition })
		.from(currentLifecycleArtifacts)
		.where(
			and(
				eq(currentLifecycleArtifacts.artifactType, "metric"),
				eq(currentLifecycleArtifacts.artifactKey, metricKey),
			),
		)
		.limit(1);
	const dag = parseMetricDag(row?.dag ?? null);
	if (!dag) {
		return { missing: `no metric definition found for '${metricKey}'` };
	}

	const fields = [
		...new Set(
			dag.steps
				.filter((s) => s.kind === "extract" && s.standardField !== null)
				.map((s) => s.standardField as string),
		),
	];
	// Newest-first graph extracts; the first row per field is the same pick
	// the Model loader and the axis resolver use (resolveGrounding).
	const snippetRows =
		fields.length > 0
			? await metadataDb
					.select({
						standardField: sqlSnippets.standardField,
						sql: sqlSnippets.sql,
						failureCount: sqlSnippets.failureCount,
					})
					.from(sqlSnippets)
					.where(
						and(
							eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
							like(sqlSnippets.source, "graph:%"),
							eq(sqlSnippets.snippetType, "extract"),
							inArray(sqlSnippets.standardField, fields),
						),
					)
					.orderBy(desc(sqlSnippets.updatedAt))
			: [];
	const sqlByField = new Map<string, string>();
	const decided = new Set<string>();
	for (const r of snippetRows) {
		if (!r.standardField || decided.has(r.standardField)) continue;
		// The newest snippet per field DECIDES (resolveGrounding's contract): a
		// failing newest row means the field has no accepted SQL — a hole, not
		// a silent fall-back to an older accepted row.
		decided.add(r.standardField);
		if ((r.failureCount ?? 0) === 0 && r.sql) {
			sqlByField.set(r.standardField, r.sql);
		}
	}

	const steps: MetricDrillStep[] = dag.steps.map((s) =>
		s.kind === "extract"
			? {
					stepId: s.stepId,
					kind: "extract" as const,
					sql: s.standardField
						? (sqlByField.get(s.standardField) ?? null)
						: null,
					expression: null,
					value: null,
					dependsOn: s.dependsOn,
					outputStep: s.outputStep,
				}
			: {
					stepId: s.stepId,
					kind: s.kind,
					sql: null,
					expression: s.expression,
					value: s.value,
					dependsOn: s.dependsOn,
					outputStep: s.outputStep,
				},
	);
	return { steps };
}
