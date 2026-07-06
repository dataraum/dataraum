// why_table tool (DAT-415) — explain one table's table-grain readiness.
//
// The per-table drill-down behind look_table's `table_readiness`, the table analog
// of why_column / why_relationship. begin_session's session_detect writes
// table-grain readiness + detector evidence (from `dimension_coverage`) keyed by a
// `table:{table_name}` target (DAT-415), sealed under a `session:{id}` head. This
// reads the PRE-COMPUTED diagnosis (per-intent drivers from `entropy_readiness`,
// raw detector evidence from `entropy_objects`) for the promoted run, then asks
// Anthropic for ONE grounded narrative explaining why the table lands in its band
// per intent. It does NOT recompute readiness (the engine owns the rollup) and
// does NOT propose teaches.
//
// Read-only → no approval. The pure row→shape assembly (`projectWhyTable`) is
// unit-tested; the live DB read + the LLM synthesis are smoke-covered.

import { chat, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { asc, eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import { tableTargetKey } from "../db/metadata/relationship-target";
import {
	currentEntropyObjects,
	currentEntropyReadiness,
	tables,
} from "../db/metadata/schema";
import { linkedAbortController } from "../lib/abort";
import { displayTableName, renderEvidenceDetail } from "../lib/display-names";
import { llmTelemetryMiddleware } from "../lib/llm-telemetry";
import { MODEL, STRUCTURED_OUTPUT_MAX_TOKENS } from "../llm";
import { getWhyInstructions } from "../prompts";
import {
	mergeCurrentEvidence,
	pickCurrentRow,
	projectVerdictHistory,
	stageOfRow,
	type VerdictHistoryEntry,
	VerdictHistorySchema,
} from "./readiness-grain";

// --- Tool output (mirrors why_column / why_relationship, keyed on the table).

const IntentExplanation = z.object({
	intent: z.string(),
	band: z.string(),
	risk: z.number(),
	drivers: z.array(ReadinessDriver),
});

const EvidenceSignal = z.object({
	dimension_path: z.string(),
	detector_id: z.string(),
	score: z.number(),
	detail: z.string(),
});

const WhyTableResult = z.object({
	table_id: z.string(),
	// Display name (`src_<digest>__` prefix stripped, DAT-431 — this result feeds
	// the agent + the synthesis prompt; the round-trip key is table_id); null when
	// the table id no longer resolves (a dropped table).
	table_name: z.string().nullable(),
	// False when the table matched no table-grain readiness row AND no evidence in
	// the promoted run — distinct from "found but not analyzed".
	found: z.boolean(),
	band: z.string().nullable(),
	// WHICH pipeline stage sealed the shown verdict (DAT-513) + when — the
	// pick is only evaluable if the caller can see it. null when unanalyzed.
	band_stage: z.string().nullable(),
	band_computed_at: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	analyzed: z.boolean(),
	intents: z.array(IntentExplanation),
	evidence: z.array(EvidenceSignal),
	signal_count: z.number(),
	// Every coexisting readiness snapshot for this target, oldest first — the
	// disclosure for the pick above.
	verdict_history: z.array(VerdictHistorySchema),
	analysis: z.string(),
	pending_teaches: z.number(),
});
export type WhyTableResult = z.infer<typeof WhyTableResult>;

/** The structured non-narrative payload (everything except `analysis`). */
export type WhyTableData = Omit<WhyTableResult, "analysis">;

/** The table's readiness row from the promoted run (null fields = no row). */
export interface WhyTableReadinessRow {
	band: string | null;
	/** Stage that sealed the picked verdict (DAT-513); null when unanalyzed. */
	bandStage: string | null;
	bandComputedAt: Date | null;
	worstIntentRisk: number | null;
	intents: unknown;
}

/** One entropy_objects row for the table target. */
export interface WhyTableEvidenceRow {
	layer: string;
	dimension: string;
	subDimension: string;
	score: number;
	detectorId: string;
	evidence: unknown;
}

/**
 * Assemble the structured why-payload from the readiness row + evidence rows.
 * Pure (no DB, no LLM) so the parsing + correlation is unit-testable. `found`
 * distinguishes "no such table-grain row in this run" (null readiness, no
 * evidence) from "found but no readiness row". A malformed intents blob degrades
 * to empty.
 */
export function projectWhyTable(
	tableId: string,
	tableName: string | null,
	readiness: WhyTableReadinessRow | null,
	evidenceRows: WhyTableEvidenceRow[],
	pendingTeaches: number,
	verdictHistory: VerdictHistoryEntry[] = [],
): WhyTableData {
	const parsed = readiness
		? PersistedIntent.array().safeParse(readiness.intents)
		: null;
	const intents: z.infer<typeof IntentExplanation>[] = parsed?.success
		? parsed.data.map((i) => ({
				intent: i.intent,
				band: i.band,
				risk: i.risk,
				drivers: i.drivers,
			}))
		: [];

	// `detail` reaches the agent AND the synthesis prompt — render through the
	// shared sanitizer (DAT-433): engine-internal `_`-keys dropped, explicit
	// table-name keys display-mapped, src-digest backstop applied.
	const evidence: z.infer<typeof EvidenceSignal>[] = evidenceRows.map((e) => ({
		dimension_path: `${e.layer}.${e.dimension}.${e.subDimension}`,
		detector_id: e.detectorId,
		score: e.score,
		detail: renderEvidenceDetail(e.evidence),
	}));

	return {
		table_id: tableId,
		// Strip the content-keyed `src_<digest>__` prefix so no hash name reaches
		// LLM context (DAT-431). The caller keeps the RAW name for the readiness
		// target key (`table:{table_name}`); only the outward-facing field is display.
		table_name: tableName === null ? null : displayTableName(tableName),
		// Found = there's either a readiness row or at least one evidence signal for
		// the table in the promoted run.
		found: readiness !== null || evidence.length > 0,
		band: readiness?.band ?? null,
		band_stage: readiness?.band == null ? null : (readiness.bandStage ?? null),
		band_computed_at:
			readiness?.band == null
				? null
				: (readiness.bandComputedAt?.toISOString() ?? null),
		worst_intent_risk: readiness?.worstIntentRisk ?? null,
		analyzed: (readiness?.band ?? null) !== null,
		intents,
		evidence,
		signal_count: evidence.length,
		verdict_history: verdictHistory,
		pending_teaches: pendingTeaches,
	};
}

/** Synthesize the grounded narrative via one forced Anthropic call (split out so
 * the assembly is testable apart from the LLM). The model sees only the band +
 * drivers + evidence we pass — and the same why instructions as why_column.
 * `signal` is the tool-context abort (DAT-449): a stopped run aborts this
 * nested call instead of billing it to completion. */
export async function synthesizeAnalysis(
	data: WhyTableData,
	signal?: AbortSignal,
): Promise<string> {
	const label = data.table_name ?? data.table_id;
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		middleware: [llmTelemetryMiddleware("why_table")],
		abortController: linkedAbortController(signal),
		modelOptions: {
			max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS,
			// One-shot structured extraction (the adapter forces a
			// `structured_output` tool): Sonnet 5's default adaptive thinking buys
			// no quality here, bills a trace per call inside the capped budget,
			// and forcing a tool while thinking is on is the fragile combination
			// frame-family documents. Disable it.
			thinking: { type: "disabled" },
		},
		systemPrompts: [getWhyInstructions()],
		messages: [
			{
				role: "user",
				content: `Explain the readiness for the table "${label}", grounded only in these signals:\n\n${JSON.stringify(
					{
						band: data.band,
						intents: data.intents,
						evidence: data.evidence,
						signal_count: data.signal_count,
					},
					null,
					2,
				)}`,
			},
		],
		outputSchema: z.object({
			analysis: z
				.string()
				.describe(
					"The grounded narrative explaining why the table lands in its band per intent — drawn ONLY from the provided drivers + evidence, no outside facts.",
				),
		}),
	});
	return result.analysis;
}

export interface WhyTableInput {
	table_id: string;
}

/** Explain one table's table-grain readiness: pre-computed drivers + evidence + narrative. */
export async function whyTable(
	input: WhyTableInput,
	signal?: AbortSignal,
): Promise<WhyTableResult> {
	// Resolve the table name up front — it's the target key AND the display label,
	// and even an unanalyzed table has a name. Null only when the id is stale.
	const [table] = await metadataDb
		.select({ tableName: tables.tableName })
		.from(tables)
		.where(eq(tables.tableId, input.table_id))
		.limit(1);
	const tableName = table?.tableName ?? null;

	// A stale table id has no name → no target to read; return a not-found shell.
	// No table ⇒ no pending teaches to attribute to it, so skip the workspace query.
	if (tableName === null) {
		return {
			...projectWhyTable(input.table_id, null, null, [], 0),
			analysis: "",
		};
	}

	const target = tableTargetKey(tableName);

	// The current_* views ARE the promoted run (ADR-0008/DAT-453): the head
	// join lives in the database, so no head resolution and no runId plumbing
	// here. No promoted run → the views are empty → unanalyzed. The catalog
	// readiness view resolves ONE row per target (DAT-506), so `pickCurrentRow`
	// is a defensive no-op here; the merge below still picks per detector.
	const allReadinessRows = await metadataDb
		.select({
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			intents: currentEntropyReadiness.intents,
			computedAt: currentEntropyReadiness.computedAt,
			runId: currentEntropyReadiness.runId,
			viaTableHead: currentEntropyReadiness.viaTableHead,
			viaCatalogHead: currentEntropyReadiness.viaCatalogHead,
			viaOperatingModelHead: currentEntropyReadiness.viaOperatingModelHead,
		})
		.from(currentEntropyReadiness)
		.where(eq(currentEntropyReadiness.target, target));
	const readinessRow = pickCurrentRow(allReadinessRows);

	// Evidence is keyed by the table target (table-grain rows have no column_id);
	// the view scopes to the promoted run, so stale runs can't inflate signal_count.
	// Merge per detector across grains: a session re-adjudication wins over the
	// add_source row for the same detector.
	const unmergedEvidence = await metadataDb
		.select({
			layer: currentEntropyObjects.layer,
			dimension: currentEntropyObjects.dimension,
			subDimension: currentEntropyObjects.subDimension,
			score: currentEntropyObjects.score,
			detectorId: currentEntropyObjects.detectorId,
			evidence: currentEntropyObjects.evidence,
			computedAt: currentEntropyObjects.computedAt,
			runId: currentEntropyObjects.runId,
			viaTableHead: currentEntropyObjects.viaTableHead,
			viaCatalogHead: currentEntropyObjects.viaCatalogHead,
			viaOperatingModelHead: currentEntropyObjects.viaOperatingModelHead,
		})
		.from(currentEntropyObjects)
		.where(eq(currentEntropyObjects.target, target))
		.orderBy(asc(currentEntropyObjects.dimension));
	const rawEvidence = mergeCurrentEvidence(unmergedEvidence);
	// View columns type as nullable (Postgres views carry no NOT NULL); the
	// underlying table guarantees these — coalesce at the edge.
	const evidenceRows: WhyTableEvidenceRow[] = rawEvidence.map((e) => ({
		layer: e.layer ?? "",
		dimension: e.dimension ?? "",
		subDimension: e.subDimension ?? "",
		score: e.score ?? 0,
		detectorId: e.detectorId ?? "",
		evidence: e.evidence,
	}));

	const pending = await getPendingOverlays();
	const data = projectWhyTable(
		input.table_id,
		tableName,
		readinessRow === undefined
			? null
			: {
					...readinessRow,
					bandStage: stageOfRow(readinessRow),
					bandComputedAt: readinessRow.computedAt ?? null,
				},
		evidenceRows,
		pending.length,
		projectVerdictHistory(allReadinessRows, unmergedEvidence),
	);

	// Nothing to explain when there's no readiness row — skip the LLM call.
	const analysis = data.analyzed ? await synthesizeAnalysis(data, signal) : "";

	return { ...data, analysis };
}

export const whyTableTool = toolDefinition({
	name: "why_table",
	description:
		"Explain ONE table's whole-table readiness — why it lands in its band for the " +
		"query, aggregation, and reporting intents — grounded in the persisted " +
		"drivers (ranked by how much fixing each would help) and the underlying " +
		"detector evidence, with a short synthesized explanation. Read-only. Use " +
		"after look_table to drill into the table-grain band; identify it by its " +
		"table_id. signal_count shows how many detector signals back the explanation " +
		"— a low count means the picture is partial.",
	inputSchema: z.object({
		table_id: z
			.string()
			.describe(
				"The table to explain (a table_id from list_tables / look_table).",
			),
	}),
	outputSchema: WhyTableResult,
}).server((input, ctx) => whyTable(input, ctx?.abortSignal));
