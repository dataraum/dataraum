// teach_metric tool (DAT-466) — the cockpit front door that declares (or
// overrides) ONE metric graph, closing the architecture's full teach loop for
// the metric family: declare in the UI → a `metric` config_overlay row → the
// next operatingModelWorkflow run declares → composes → executes it →
// look_metric renders the outcome. No engine changes — DAT-456's overlay applier
// (`_apply_metric`) + lifecycle + the metric read surface already exist; this is
// the missing front door, mirroring teach_validation / teach_cycle.
//
// "Teach" here = a new metric graph or an override of a shipped one. The payload
// is a TransformationGraph (the heaviest of the three teach shapes — see
// metric-spec.ts); born-loud is already enforced engine-side (a malformed graph
// stays `declared` with a parse reason).
//
// WRITE PATH REUSE: this funnels through the same `teach()` that writes every
// overlay row — a `metric`-typed `config_overlay` row via the metadata write
// surface — so the engine applier consumes it unchanged. The ONLY thing this
// tool adds over the generic `teach` is (1) a strict, graph-shaped input the
// model can lean on, and (2) the override SHADOWING affordance: declaring with a
// shipped metric's graph_id is an upsert-REPLACE, surfaced visibly, never silent.

import { toolDefinition } from "@tanstack/ai";
import { isNull } from "drizzle-orm";
import { z } from "zod";

import { metricDagRead } from "../db/metadata/read-surface";
import {
	findShadowedMetric,
	MetricSpecSchema,
	metricSummary,
	type ShippedMetricSpec,
	type ShippedMetricSummary,
} from "./metric-spec";
import { teach } from "./teach";

export interface TeachMetricResult {
	overlay_id: string;
	graph_id: string;
	vertical: string;
	// True when `graph_id` matches a metric the vertical SHIPS on disk — the
	// overlay upsert-replaces it. The UX shows this as a visible override, never a
	// silent shadow.
	override: boolean;
	// The shipped metric being shadowed (graph_id/name/description/category),
	// echoed so the UX can show WHAT the user is replacing. The lean summary view
	// (no DAG body) keeps the agent's tool result small — the human reads the
	// replaced DAG via the canvas (the reader carries it). null for a brand-new
	// declaration.
	shadowed_spec: ShippedMetricSummary | null;
}

/**
 * Read the metric graphs a vertical SHIPS, from the typed metric-DAG home
 * (DAT-882, config→DB) — ShippedMetricSpec (summary fields + the DAG body). ONE
 * reader for both jobs: the frame SEED reads the DAG structure as few-shot, the
 * teach SHADOW matches by `graph_id`. `vertical` stays in the signature for
 * interface stability, but the query itself doesn't filter on it: the mirrored
 * view is ALREADY scoped to the workspace's bound active_vertical (storage/
 * read_views.py's `_vertical_scoped_view_sql`), the same safety property
 * `prompts/conventions.ts`'s `buildConventionsBlock` relies on.
 *
 * The metadata client is imported lazily (it constructs the reader-role SQL
 * client at module scope): a static import would pull it into every consumer of
 * this module + the node-run vitest workers.
 *
 * Degradation note: a swallowed read failure makes an actual override LOOK like a
 * fresh declaration in the rail hint (`override:false`) — but the override itself
 * is unaffected (the engine applier upsert-replaces by `graph_id` regardless; it
 * is the source of truth). Only the visible-override label degrades, and only on
 * a metadata-read blip — the same best-effort contract `buildConventionsBlock`
 * documents (unlike the retired per-file fs read, a single malformed row isn't a
 * distinct failure mode a typed column read can produce). */
export async function readShippedMetrics(
	_vertical: string,
): Promise<ShippedMetricSpec[]> {
	try {
		const { metadataDb } = await import("#/db/metadata/client");
		const rows = await metadataDb
			.select({
				graphId: metricDagRead.graphId,
				name: metricDagRead.name,
				description: metricDagRead.description,
				category: metricDagRead.category,
				output: metricDagRead.output,
				dependencies: metricDagRead.dependencies,
			})
			.from(metricDagRead)
			.where(isNull(metricDagRead.supersededAt));
		return rows
			.filter((r): r is typeof r & { graphId: string } => Boolean(r.graphId))
			.map((r) => ({
				graph_id: r.graphId,
				name: r.name ?? null,
				description: r.description ?? null,
				category: r.category ?? null,
				output: r.output ?? null,
				dependencies: r.dependencies ?? null,
			}));
	} catch {
		return [];
	}
}

/**
 * Declare or override a metric graph. Writes a `metric`-typed `config_overlay`
 * row (via the shared `teach()` path — same table, same client) carrying the
 * full graph, and reports whether it shadows a shipped metric. The next
 * operatingModel run composes + executes it; the outcome is read via look_metric.
 */
export async function teachMetric(
	input: z.infer<typeof MetricSpecSchema>,
	// The shipped-metric reader is injectable so the composition (read → shadow →
	// write) is unit-testable without the config tree; production uses the default.
	readShipped: (
		vertical: string,
	) => Promise<ShippedMetricSpec[]> = readShippedMetrics,
): Promise<TeachMetricResult> {
	// Detect the override BEFORE the write so the result can echo the shadowed
	// shipped metric. A new graph_id (no match) → a brand-new declaration.
	const shipped = await readShipped(input.vertical);
	const shadowed = findShadowedMetric(shipped, input.graph_id);

	// Funnel the FULL graph through the shared overlay-write path. The payload IS
	// the engine's metric-graph shape (vertical + graph_id + the rest); the
	// applier filters by `payload.vertical` and upsert-replaces by `graph_id`.
	// Drop undefined optionals so the row carries only declared fields.
	const payload = stripUndefined({ ...input });
	const { overlay_id } = await teach({ type: "metric", payload });

	return {
		overlay_id,
		graph_id: input.graph_id,
		vertical: input.vertical,
		override: shadowed !== null,
		// Echo the lean summary, not the full DAG — the agent's result stays small;
		// the human reads the replaced graph from the canvas (M2).
		shadowed_spec: shadowed ? metricSummary(shadowed) : null,
	};
}

/** Drop keys whose value is `undefined` so the overlay payload carries only the
 * fields the user actually declared (a `null` is a deliberate value; `undefined`
 * is "not provided"). */
function stripUndefined(obj: Record<string, unknown>): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(obj).filter(([, v]) => v !== undefined),
	);
}

/**
 * The `teach_metric` tool for the agent loop. An acting tool: it mutates the
 * workspace (writes an overlay row that the next run composes + executes), so it
 * runs on the user's explicit instruction — there is no approval gate.
 *
 * Data-informed: the agent declares the graph AGAINST the workspace's
 * tables/columns it reads from `list_tables` / `look_table` (extract steps
 * reference standard_fields the engine resolves via semantic mappings); the
 * description points it there. A graph the engine cannot ground (unmappable
 * required fields) stays `declared` with a born-loud reason, visible in
 * look_metric.
 */
export const teachMetricTool = toolDefinition({
	name: "teach_metric",
	description:
		"Declare a NEW metric (a computation graph over the data — e.g. EBITDA, " +
		"DSO, current ratio), or OVERRIDE a shipped one, for the session's " +
		"vertical. Writes a config_overlay row; the next " +
		"operating_model run composes and executes it, and look_metric shows the " +
		"outcome (declared / grounded / executed, or the reason it could not " +
		"ground). The metric is a DAG of steps: 'extract' steps pull values from " +
		"financial statements (standard_field + aggregation), 'formula' steps " +
		"combine earlier steps via an expression, and one step is the output. " +
		"Declare AGAINST the real tables/columns (read them with list_tables / " +
		"look_table first). Reusing a shipped graph_id OVERRIDES that metric — the " +
		"result reports the shadowed metric so the override is visible. After a " +
		"teach, run operating_model to see it executed.",
	inputSchema: MetricSpecSchema,
	// The output is always the success shape — UNLIKE the generic `teach`, which
	// validates per-type INSIDE its handler. Here the graph shape is enforced by
	// zod at the SDK boundary (and the engine's GraphLoader is the final
	// validator at compose). A DB write failure is not the agent's to fix → it
	// propagates (no `{error}` branch).
	outputSchema: z.object({
		overlay_id: z.string(),
		graph_id: z.string(),
		vertical: z.string(),
		override: z.boolean(),
		shadowed_spec: z
			.object({
				graph_id: z.string(),
				name: z.string().nullable(),
				description: z.string().nullable(),
				category: z.string().nullable(),
			})
			.nullable(),
	}),
}).server((input) => teachMetric(input));
