// why_metric tool (DAT-466) — explain one metric's state.
//
// The per-metric drill-down behind look_metric, mirroring why_validation /
// why_cycle in shape (found discriminant, session-scoped read over the promoted
// run, pure unit-tested projection, NO LLM synthesis). The metric family's
// "second read" is the SQL SNIPPETS: a metric persists no result row and its
// value is ephemeral, so the drill-down's value over the list is HOW it computes
// — the per-step SQL fragments the graph agent saved (`source='graph:<id>'`),
// each the runnable knowledge for one node of the metric's DAG — plus what it
// bound against. The numeric value is deliberately absent (re-computed on demand
// by a future "run metric" query action, never stored).
//
// Read-only → no approval. The pure row→shape assembly (`projectWhyMetric`) is
// unit-tested; the live DB read is integration-smoke-covered.

import { toolDefinition } from "@tanstack/ai";
import { asc, eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	type LifecycleArtifactDetail,
	readLifecycleArtifact,
} from "../db/metadata/lifecycle-artifacts";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { sqlSnippets } from "../db/metadata/schema";
import { renderEvidenceDetail, stripSrcDigests } from "../lib/display-names";
import { metricSnippetSource } from "./look-metric";

// --- Tool output (mirrors the why_* found/anatomy conventions, keyed on the
// metric's graph_id).

const MetricStep = z.object({
	// The snippet's stable id — a unique key for ordering/rendering (two formula
	// steps can reduce to the same label, so the label alone is not unique).
	snippet_id: z.string().nullable(),
	// The snippet kind the graph agent writes: extract | constant | formula.
	type: z.string().nullable(),
	// A readable label for the step — the standard_field (extract), the
	// normalized expression (formula), or the step kind when neither applies.
	label: z.string(),
	// The runnable SQL the graph agent saved for this step, digest-sanitized for
	// display (NOT a re-run key — it embeds lake physical names).
	sql: z.string().nullable(),
	description: z.string().nullable(),
	// Usage health from the cross-run snippet base.
	execution_count: z.number().nullable(),
	failure_count: z.number().nullable(),
});
export type MetricStep = z.infer<typeof MetricStep>;

const WhyMetricResult = z.object({
	graph_id: z.string(),
	// False when the graph_id matched no lifecycle artifact in the session's
	// promoted operating_model run. (Snippets are workspace-durable / cross-
	// session, so they do NOT make a metric "found" — the declared artifact does.)
	found: z.boolean(),
	// Lifecycle: declared → grounded → executed; the engine's persisted state.
	state: z.string().nullable(),
	// WHY it stopped short of executed — the engine's reason verbatim
	// (digest-sanitized); null once executed. The "visibly impossible" surface.
	state_reason: z.string().nullable(),
	// The lifecycle strictness dial the artifact was declared with.
	strictness: z.number().nullable(),
	// What the metric bound against (the base-run map), rendered through the
	// shared evidence sanitizer — "" when the artifact never grounded.
	grounded_against: z.string(),
	// How many persisted SQL steps back the metric.
	snippet_count: z.number(),
	// The per-step SQL fragments — the metric's executable knowledge, the "how it
	// computes". Every persisted step is served (no cap, DAT-649).
	steps: z.array(MetricStep),
	pending_teaches: z.number(),
});
export type WhyMetricResult = z.infer<typeof WhyMetricResult>;

/** The metric's lifecycle artifact row (null = no such artifact) — the shared
 * lifecycle-detail shape, aliased here for the projection's callers. */
export type WhyMetricArtifactRow = LifecycleArtifactDetail;

/** One sql_snippets row for a metric's step (a fragment of the DAG). */
export interface MetricSnippetRow {
	snippetId: string | null;
	snippetType: string | null;
	standardField: string | null;
	statement: string | null;
	aggregation: string | null;
	normalizedExpression: string | null;
	sql: string | null;
	description: string | null;
	executionCount: number | null;
	failureCount: number | null;
}

/** A readable label for a snippet step: the field it extracts, the expression it
 * computes, or its kind when neither is present. */
function stepLabel(snippet: MetricSnippetRow): string {
	if (snippet.standardField) return stripSrcDigests(snippet.standardField);
	if (snippet.normalizedExpression)
		return stripSrcDigests(snippet.normalizedExpression);
	return snippet.snippetType ? `(${snippet.snippetType})` : "(step)";
}

/**
 * Assemble the why-payload from the artifact + snippet rows. Pure (no DB) so the
 * sanitization + null-handling is unit-testable. `found` is the DECLARED artifact
 * (not the snippets — those are cross-session durable). Engine-built free text
 * (`state_reason`, `sql`, `description`, the labels) can embed raw
 * `src_<digest>__` physical names — every string passes the digest backstop;
 * unknown-shape JSON (`grounded_against`) renders through the shared evidence
 * sanitizer, never assumed.
 */
export function projectWhyMetric(
	graphId: string,
	artifact: WhyMetricArtifactRow | null,
	snippets: MetricSnippetRow[],
	pendingTeaches: number,
): WhyMetricResult {
	const steps: MetricStep[] = snippets.map((s) => ({
		snippet_id: s.snippetId,
		type: s.snippetType,
		label: stepLabel(s),
		sql: s.sql == null ? null : stripSrcDigests(s.sql),
		description:
			s.description == null || s.description === ""
				? null
				: stripSrcDigests(s.description),
		execution_count: s.executionCount,
		failure_count: s.failureCount,
	}));
	return {
		graph_id: graphId,
		found: artifact !== null,
		state: artifact?.state ?? null,
		state_reason:
			artifact?.stateReason == null
				? null
				: stripSrcDigests(artifact.stateReason),
		strictness: artifact?.strictness ?? null,
		grounded_against: renderEvidenceDetail(artifact?.groundedAgainst),
		snippet_count: snippets.length,
		steps,
		pending_teaches: pendingTeaches,
	};
}

export interface WhyMetricInput {
	graph_id: string;
}

/** Explain one metric's state: lifecycle + grounding + the per-step SQL it composed. */
export async function whyMetric(
	input: WhyMetricInput,
): Promise<WhyMetricResult> {
	// The current_* views ARE the promoted run (docs/architecture/persistence.md, DAT-453): the head join
	// lives in the database — no head resolution. No promoted run → empty views →
	// not found. The shared reader pins artifact_type = 'metric'.
	const artifactRow = await readLifecycleArtifact("metric", input.graph_id);

	// The metric's persisted SQL fragments — workspace-durable, keyed by
	// `source='graph:<graph_id>'` (NOT run-versioned; the cross-run reuse base).
	const snippetRows: MetricSnippetRow[] = await metadataDb
		.select({
			snippetId: sqlSnippets.snippetId,
			snippetType: sqlSnippets.snippetType,
			standardField: sqlSnippets.standardField,
			statement: sqlSnippets.statement,
			aggregation: sqlSnippets.aggregation,
			normalizedExpression: sqlSnippets.normalizedExpression,
			sql: sqlSnippets.sql,
			description: sqlSnippets.description,
			executionCount: sqlSnippets.executionCount,
			failureCount: sqlSnippets.failureCount,
		})
		.from(sqlSnippets)
		.where(eq(sqlSnippets.source, metricSnippetSource(input.graph_id)))
		// Fully deterministic order: extracts sort by field, formulas by their
		// expression, and snippet_id breaks any remaining tie (formula rows have a
		// null standard_field — without this their order would flicker per fetch).
		.orderBy(
			asc(sqlSnippets.snippetType),
			asc(sqlSnippets.standardField),
			asc(sqlSnippets.normalizedExpression),
			asc(sqlSnippets.snippetId),
		);

	const pending = await getPendingOverlays();

	return projectWhyMetric(
		input.graph_id,
		artifactRow,
		snippetRows,
		pending.length,
	);
}

export const whyMetricTool = toolDefinition({
	name: "why_metric",
	description:
		"Explain ONE metric's state in a session's operating-model run — its " +
		"lifecycle state with the reason it could not ground (when it stopped " +
		"short of executed), what it bound against, and HOW it computes: the " +
		"per-step SQL fragments the engine saved for the metric's DAG (extract / " +
		"formula steps). Read-only. The metric's numeric VALUE is not shown — it " +
		"is re-computed on demand by running the metric, not stored. Use after " +
		"look_metric to drill into a specific metric; identify it by its graph_id.",
	inputSchema: z.object({
		graph_id: z
			.string()
			.describe("The metric to explain (a graph_id from look_metric)."),
	}),
	outputSchema: WhyMetricResult,
}).server((input) => whyMetric(input));
