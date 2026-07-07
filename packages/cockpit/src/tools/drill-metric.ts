// Node-drill part resolution (DAT-702, parts-at-source DAT-703) — the
// SERVER-ONLY read behind the `/api/drill/node` route. Resolves the opened
// node — a metric key or a bare measure's standard field — to `NodeStep`s
// carrying each extract's PERSISTED CLAUSE PARTS (`sql_snippets.parts`,
// narrowed at this boundary); `composeNodeQuery` (parts.ts) rebuilds the
// node's subtree from them onto the mosaic-sql builder, ad hoc. Never import
// from a client component.
//
// A missing/failing/un-narrowable parts value is NOT refused here: the step
// carries `parts: null` and the composer refuses — by name — only when the
// hole is actually reachable from the opened node. A hole elsewhere in the
// DAG never blocks the node the user asked for (the DAT-699
// partial-execution posture).

import { and, desc, eq, inArray, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import { currentLifecycleArtifacts, sqlSnippets } from "#/db/metadata/schema";
import type { DrillAxesRequest } from "#/duckdb/drill";
import {
	type NodeStep,
	narrowSnippetParts,
	type SnippetParts,
} from "#/duckdb/parts";

import { parseMetricDag } from "./operating-model-graph";

export type NodeDrillSteps = { steps: NodeStep[] } | { missing: string };

/** The newest graph extract per standard field DECIDES (resolveGrounding's
 *  contract): a failing newest row means the field has no accepted parts — a
 *  hole, not a silent fall-back to an older accepted row. Accepted rows
 *  narrow `sql_snippets.parts`; a pre-parts or off-shape value narrows to
 *  null and refuses downstream by name. */
async function resolveSnippets(
	fields: string[],
): Promise<Map<string, SnippetParts | null>> {
	const rows =
		fields.length > 0
			? await metadataDb
					.select({
						standardField: sqlSnippets.standardField,
						parts: sqlSnippets.parts,
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
	const byField = new Map<string, SnippetParts | null>();
	for (const r of rows) {
		if (!r.standardField || byField.has(r.standardField)) continue;
		byField.set(
			r.standardField,
			(r.failureCount ?? 0) === 0 ? narrowSnippetParts(r.parts) : null,
		);
	}
	return byField;
}

/** The node's steps with each extract's narrowed parts attached (null = hole).
 *  `missing` covers only the node itself — no metric definition, an
 *  unparseable one, or a measure field with no snippet at all. */
export async function resolveNodeSteps(
	req: DrillAxesRequest,
): Promise<NodeDrillSteps> {
	if (req.standardField !== undefined) {
		const byField = await resolveSnippets([req.standardField]);
		if (!byField.has(req.standardField)) {
			return {
				missing: `no graph extract snippet found for '${req.standardField}'`,
			};
		}
		// A bare measure is the single-extract case of the node composition:
		// one step, itself the output. Standard fields are snake_case, so the
		// step id passes the composer's identifier gate.
		return {
			steps: [
				{
					stepId: req.standardField,
					kind: "extract",
					parts: byField.get(req.standardField) ?? null,
					expression: null,
					value: null,
					dependsOn: [],
					outputStep: true,
				},
			],
		};
	}

	const [row] = await metadataDb
		.select({ dag: currentLifecycleArtifacts.graphDefinition })
		.from(currentLifecycleArtifacts)
		.where(
			and(
				eq(currentLifecycleArtifacts.artifactType, "metric"),
				eq(currentLifecycleArtifacts.artifactKey, req.metricKey),
			),
		)
		.limit(1);
	const dag = parseMetricDag(row?.dag ?? null);
	if (!dag) {
		return { missing: `no metric definition found for '${req.metricKey}'` };
	}

	const fields = [
		...new Set(
			dag.steps
				.filter((s) => s.kind === "extract" && s.standardField !== null)
				.map((s) => s.standardField as string),
		),
	];
	const byField = await resolveSnippets(fields);

	const steps: NodeStep[] = dag.steps.map((s) =>
		s.kind === "extract"
			? {
					stepId: s.stepId,
					kind: "extract" as const,
					parts: s.standardField
						? (byField.get(s.standardField) ?? null)
						: null,
					expression: null,
					value: null,
					dependsOn: s.dependsOn,
					outputStep: s.outputStep,
				}
			: {
					stepId: s.stepId,
					kind: s.kind,
					parts: null,
					expression: s.expression,
					value: s.value,
					dependsOn: s.dependsOn,
					outputStep: s.outputStep,
				},
	);
	return { steps };
}
