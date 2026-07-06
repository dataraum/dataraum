// Metric-path drill-axis resolver (DAT-672) — the SERVER-ONLY read behind
// `/api/drill/axes`. Imports config + the metadata client, so it must never be
// imported by a client component (canvas widgets fetch the API route).
//
// The canvas-first contract: a metric's drillable dimensions resolve from what
// its SQL ACTUALLY reads — no naming conventions, no substring matching:
//
//   metric → dag extract steps (standard fields) → newest extract snippet per
//   field → the extract SQL's parsed relations (`sqlRelations`, DuckDB's own
//   parser) → the promoted enriched view among them (`resolveGrounding`, the
//   same matcher the Model loader uses) → that view's FACT table →
//   `current_slice_definitions` rows on the fact, priority-ordered.
//
// `slice_definitions.column_name` is addressable VERBATIM in the metric's SQL
// scope: metric SQL reads the enriched view (the GraphAgent's prefer-enriched
// contract) and the enriched view exposes exactly those FK-prefixed dimension
// columns. Whether an axis actually binds in a given statement stays the
// composer's call (`/api/drill/compose`, binder-gated).

import { and, asc, desc, eq, inArray, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import {
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentSliceDefinitions,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxesRequest, DrillAxis } from "#/duckdb/drill";
import { sqlRelations } from "#/lib/sql-canonical";

import {
	type ExtractSnippetInput,
	parseMetricDag,
	resolveGrounding,
} from "./operating-model-graph";

/** The extract-step standard fields of a metric's persisted DAG (pure). */
export function measureFieldsFromDag(dag: unknown): string[] {
	const parsed = parseMetricDag(dag);
	if (!parsed) return [];
	return [
		...new Set(
			parsed.steps
				.filter((s) => s.kind === "extract" && s.standardField !== null)
				.map((s) => s.standardField as string),
		),
	];
}

/** One `current_slice_definitions` row as the resolver reads it (view columns
 *  all type nullable). */
export interface SliceRowInput {
	columnName: string | null;
	slicePriority: number | null;
	sliceType: string | null;
	distinctValues: unknown;
	valueCount: number | null;
	businessContext: string | null;
}

/**
 * Slice rows → axes (pure): drop rows without a column name, dedupe by column
 * keeping the best (lowest) priority — a dimension cataloged on several facts
 * of the same metric is ONE axis — and narrow `distinct_values` to strings.
 * Callers pass rows already priority-ordered; the dedupe preserves that order.
 */
export function axesFromSliceRows(rows: SliceRowInput[]): DrillAxis[] {
	const byColumn = new Map<string, DrillAxis>();
	for (const r of rows) {
		if (!r.columnName || byColumn.has(r.columnName)) continue;
		byColumn.set(r.columnName, {
			column: r.columnName,
			priority: r.slicePriority ?? Number.MAX_SAFE_INTEGER,
			sliceType: r.sliceType ?? "categorical",
			values: Array.isArray(r.distinctValues)
				? r.distinctValues.filter((v): v is string => typeof v === "string")
				: [],
			valueCount: r.valueCount,
			businessContext: r.businessContext,
		});
	}
	return [...byColumn.values()];
}

/** The measure standard fields the request targets: a measure names itself, a
 *  metric contributes every extract step of its promoted DAG. */
async function targetFields(req: DrillAxesRequest): Promise<string[]> {
	if (req.standardField !== undefined) return [req.standardField];
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
	return measureFieldsFromDag(row?.dag ?? null);
}

/** Axes plus — when empty — the WHY, so the UI never shows a dead-end badge:
 *  each empty case names the stage of the resolution chain that yielded
 *  nothing (no extracts / stale relations / bare catalog). */
export interface DrillAxesResult {
	axes: DrillAxis[];
	reason?: string;
}

export async function resolveDrillAxes(
	req: DrillAxesRequest,
): Promise<DrillAxesResult> {
	const fields = await targetFields(req);
	if (fields.length === 0) {
		return { axes: [], reason: "The metric's definition names no measure extracts." };
	}

	// The same two reads the Model loader does: newest-first graph extracts
	// (first-per-field wins in resolveGrounding) + the promoted enriched views.
	const [snippetRows, viewRows] = await Promise.all([
		metadataDb
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
				),
			)
			.orderBy(desc(sqlSnippets.updatedAt)),
		metadataDb
			.select({
				viewName: currentEnrichedViews.viewName,
				viewTableId: currentEnrichedViews.viewTableId,
				factTableId: currentEnrichedViews.factTableId,
			})
			.from(currentEnrichedViews),
	]);

	const wanted = new Set(fields);
	const extracts: ExtractSnippetInput[] = await Promise.all(
		snippetRows
			.filter(
				(r): r is typeof r & { standardField: string } =>
					r.standardField !== null && wanted.has(r.standardField),
			)
			.map(async (r) => ({
				standardField: r.standardField,
				sql: r.sql ?? null,
				relations: r.sql ? ((await sqlRelations(r.sql)) ?? []) : [],
				failureCount: r.failureCount ?? 0,
			})),
	);
	const views = viewRows
		.filter((v): v is typeof v & { viewName: string; viewTableId: string } =>
			Boolean(v.viewName && v.viewTableId),
		)
		// baseTableIds feed only resolveGrounding's baseTables output, unused here.
		.map((v) => ({
			viewName: v.viewName,
			viewTableId: v.viewTableId,
			baseTableIds: [],
		}));

	const grounding = resolveGrounding(extracts, views, new Map());
	const factByViewTableId = new Map(
		viewRows
			.filter((v) => v.viewTableId && v.factTableId)
			.map((v) => [v.viewTableId as string, v.factTableId as string]),
	);
	const factIds = [
		...new Set(
			grounding
				.filter((g) => g.grounded && g.enrichedView)
				.map((g) => factByViewTableId.get(g.enrichedView?.tableId ?? ""))
				.filter((id): id is string => Boolean(id)),
		),
	];
	if (factIds.length === 0) {
		// Distinguish "reads something, just not a promoted view" (a stale or
		// cross-lineage snippet — the honest refusal) from "no usable extract".
		const staleRelations = [
			...new Set(extracts.flatMap((e) => (e.failureCount === 0 ? e.relations : []))),
		];
		return {
			axes: [],
			reason:
				staleRelations.length > 0
					? `The computation reads relations outside the current analysis (${staleRelations.join(", ")}) — likely a stale snippet from an earlier run.`
					: "No accepted extract SQL to resolve dimensions from.",
		};
	}

	const sliceRows = await metadataDb
		.select({
			columnName: currentSliceDefinitions.columnName,
			slicePriority: currentSliceDefinitions.slicePriority,
			sliceType: currentSliceDefinitions.sliceType,
			distinctValues: currentSliceDefinitions.distinctValues,
			valueCount: currentSliceDefinitions.valueCount,
			businessContext: currentSliceDefinitions.businessContext,
		})
		.from(currentSliceDefinitions)
		.where(inArray(currentSliceDefinitions.tableId, factIds))
		.orderBy(asc(currentSliceDefinitions.slicePriority));

	const axes = axesFromSliceRows(sliceRows);
	if (axes.length === 0) {
		return {
			axes,
			reason:
				"No dimensions cataloged for this computation's fact table — the slicing phase found nothing grain-safe to offer.",
		};
	}
	return { axes };
}
