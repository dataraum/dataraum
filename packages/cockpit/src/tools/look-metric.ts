// look_metric tool (DAT-466) — a session's operating_model metric overview. The
// metric analog of look_validation / look_cycle: one row per declared metric
// graph with its lifecycle state and the reason it could not ground when it
// stopped short.
//
// Pure read via the shared lifecycle-artifacts reader (ADR-0008/DAT-453): the
// `metric`-typed `current_lifecycle_artifacts` rows are the authoritative
// declared set (the engine declares ONE artifact per declared `graph_id` —
// vocabulary + teaches — in metrics_phase). UNLIKE validation/cycle there is NO
// per-row result to join: a metric's value is ephemeral (the engine discards it;
// the durable knowledge is the SQL, re-run on demand by a future query action) —
// so look_metric is the lifecycle list plus a `snippet_count` signal (how many
// persisted SQL steps back the metric — 0 for ungroundable, >0 once it composed).
// State and reason are the engine's persisted values verbatim — never re-derived
// here (only digest-sanitized). Read-only → no approval.
//
// The DB read is integration-smoke-covered (scripts/smoke-operating-model.ts);
// the pure row→shape projection is unit-tested via `projectMetricOverview`.

import { toolDefinition } from "@tanstack/ai";
import { inArray } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	type LifecycleArtifactRow,
	readLifecycleArtifactRows,
	readOperatingModelHead,
} from "../db/metadata/lifecycle-artifacts";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { sqlSnippets } from "../db/metadata/schema";
import { stripSrcDigests } from "../lib/display-names";

// The provenance prefix the graph agent stamps on every snippet it persists for
// a metric: `source = "graph:<graph_id>"` (engine `_save_snippets`). The link
// back from a metric to its SQL fragments.
const SNIPPET_SOURCE_PREFIX = "graph:";

/** The `source` value linking a metric's persisted SQL snippets to its graph. */
export function metricSnippetSource(graphId: string): string {
	return `${SNIPPET_SOURCE_PREFIX}${graphId}`;
}

// --- The tool's output: one row per declared metric.

const MetricOverview = z.object({
	// The metric's key (== the lifecycle artifact_key, e.g. "ebitda") — feeds
	// why_metric for the drill-down.
	graph_id: z.string(),
	// Lifecycle state: declared → grounded → executed. A non-executed state is
	// always paired with `state_reason` (the fail-loud contract).
	state: z.string(),
	// WHY the metric stopped short of executed (e.g. "ungroundable: required
	// field mappings missing") — the engine's reason verbatim. Also non-null on an
	// EXECUTED metric when the grounding confidence fell below the engine floor
	// (the low-confidence caveat, DAT-631) — so on an executed metric a present
	// reason is the low-confidence flag (GroundingConfidenceBadge).
	state_reason: z.string().nullable(),
	// How many persisted SQL snippets back this metric (the per-step fragments the
	// graph agent saved). 0 when it never composed; >0 once it did — a soft signal
	// of how much executable knowledge exists, NOT the metric's value.
	snippet_count: z.number(),
});
export type MetricOverview = z.infer<typeof MetricOverview>;

const LookMetricResult = z.object({
	// False when the workspace has no promoted operating_model run yet — the widget
	// should say "not run" rather than imply zero declared metrics.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	metrics: z.array(MetricOverview),
});
export type LookMetricResult = z.infer<typeof LookMetricResult>;

/**
 * Project one lifecycle artifact (+ its snippet count) to the tool's shape. Pure
 * (no DB) so the sanitization is unit-testable. `state_reason` is engine-built
 * free text that can embed raw `src_<digest>__` physical names — pass it through
 * the digest backstop before it reaches the agent (the validation/cycle
 * precedent).
 */
export function projectMetricOverview(
	artifact: LifecycleArtifactRow,
	snippetCount: number,
): MetricOverview {
	return {
		graph_id: artifact.artifactKey,
		state: artifact.state ?? "",
		state_reason:
			artifact.stateReason === null
				? null
				: stripSrcDigests(artifact.stateReason),
		snippet_count: snippetCount,
	};
}

/** Per-metric lifecycle + snippet-count for the workspace's promoted operating_model run. */
export async function lookMetric(): Promise<LookMetricResult> {
	// `analyzed` = the workspace PROMOTED an operating_model run — distinct from
	// "promoted but zero declared metrics" (a vertical with none), which must not
	// read as never-ran.
	const head = await readOperatingModelHead();
	if (!head) {
		return {
			analyzed: false,
			pending_teaches: 0,
			metrics: [],
		};
	}

	// The current_* views ARE the promoted run (ADR-0008/DAT-453). The shared
	// reader scopes to metric artifacts — the authoritative declared set.
	const artifacts: LifecycleArtifactRow[] =
		await readLifecycleArtifactRows("metric");

	// One round-trip for all the metrics' snippets (workspace-durable, keyed by
	// `source='graph:<graph_id>'`, NOT run-versioned — the cross-run reuse base),
	// counted by source. Skip the query when there are no metrics.
	const sources = artifacts.map((a) => metricSnippetSource(a.artifactKey));
	const countBySource = new Map<string, number>();
	if (sources.length > 0) {
		const rows = await metadataDb
			.select({ source: sqlSnippets.source })
			.from(sqlSnippets)
			.where(inArray(sqlSnippets.source, sources));
		for (const r of rows) {
			if (r.source)
				countBySource.set(r.source, (countBySource.get(r.source) ?? 0) + 1);
		}
	}

	const metrics = artifacts.map((a) =>
		projectMetricOverview(
			a,
			countBySource.get(metricSnippetSource(a.artifactKey)) ?? 0,
		),
	);

	const pending = await getPendingOverlays();

	return {
		analyzed: true,
		pending_teaches: pending.length,
		metrics,
	};
}

export const lookMetricTool = toolDefinition({
	name: "look_metric",
	description:
		"Show the workspace's operating-model metrics — every declared metric graph " +
		"with its lifecycle state (declared / grounded / executed) and the reason " +
		"it could not ground when it stopped short (e.g. required field mappings " +
		"missing). snippet_count is how many SQL steps back the metric (0 when " +
		"ungroundable). Read-only; reflects the promoted operating_model run (run " +
		"the operating_model tool first). NB the metric's numeric VALUE is not " +
		"shown — it is re-computed on demand by running the metric, not stored. " +
		"pending_teaches counts un-applied teaches across the workspace. Use " +
		"`why_metric` to drill into a specific metric's composition.",
	inputSchema: z.object({}),
	outputSchema: LookMetricResult,
}).server(() => lookMetric());
