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
import { and, asc, eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import {
	sessionHeadTarget,
	tableTargetKey,
} from "../db/metadata/relationship-target";
import {
	entropyObjects,
	entropyReadiness,
	metadataSnapshotHead,
	tables,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";
import { MAX_OUTPUT_TOKENS, MODEL } from "../llm";
import { getWhyInstructions } from "../prompts";

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
	worst_intent_risk: z.number().nullable(),
	analyzed: z.boolean(),
	intents: z.array(IntentExplanation),
	evidence: z.array(EvidenceSignal),
	signal_count: z.number(),
	analysis: z.string(),
	pending_teaches: z.number(),
});
export type WhyTableResult = z.infer<typeof WhyTableResult>;

/** The structured non-narrative payload (everything except `analysis`). */
export type WhyTableData = Omit<WhyTableResult, "analysis">;

/** The table's readiness row from the promoted run (null fields = no row). */
export interface WhyTableReadinessRow {
	band: string | null;
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

function renderDetail(evidence: unknown): string {
	if (evidence === null || evidence === undefined) return "";
	return JSON.stringify(evidence);
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

	const evidence: z.infer<typeof EvidenceSignal>[] = evidenceRows.map((e) => ({
		dimension_path: `${e.layer}.${e.dimension}.${e.subDimension}`,
		detector_id: e.detectorId,
		score: e.score,
		detail: renderDetail(e.evidence),
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
		worst_intent_risk: readiness?.worstIntentRisk ?? null,
		analyzed: (readiness?.band ?? null) !== null,
		intents,
		evidence,
		signal_count: evidence.length,
		pending_teaches: pendingTeaches,
	};
}

/** Synthesize the grounded narrative via one forced Anthropic call (split out so
 * the assembly is testable apart from the LLM). The model sees only the band +
 * drivers + evidence we pass — and the same why instructions as why_column. */
export async function synthesizeAnalysis(data: WhyTableData): Promise<string> {
	const label = data.table_name ?? data.table_id;
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		maxTokens: MAX_OUTPUT_TOKENS,
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
	session_id: string;
	table_id: string;
}

/** Explain one table's table-grain readiness: pre-computed drivers + evidence + narrative. */
export async function whyTable(input: WhyTableInput): Promise<WhyTableResult> {
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

	// Table-grain readiness/evidence is sealed at session grain (DAT-415): pick the
	// PROMOTED begin_session detect run via the per-session head, then read only
	// that run's rows. No promoted run → unanalyzed.
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, sessionHeadTarget(input.session_id)),
				eq(metadataSnapshotHead.stage, "detect"),
			),
		)
		.limit(1);
	const headRunId = head?.runId ?? null;

	const [readinessRow] = headRunId
		? await metadataDb
				.select({
					band: entropyReadiness.band,
					worstIntentRisk: entropyReadiness.worstIntentRisk,
					intents: entropyReadiness.intents,
				})
				.from(entropyReadiness)
				.where(
					and(
						eq(entropyReadiness.sessionId, input.session_id),
						eq(entropyReadiness.target, target),
						eq(entropyReadiness.runId, headRunId),
					),
				)
				.limit(1)
		: [];

	// Evidence is keyed by the table target (table-grain rows have no column_id),
	// scoped to the SAME promoted run so stale runs don't inflate signal_count.
	const evidenceRows = headRunId
		? await metadataDb
				.select({
					layer: entropyObjects.layer,
					dimension: entropyObjects.dimension,
					subDimension: entropyObjects.subDimension,
					score: entropyObjects.score,
					detectorId: entropyObjects.detectorId,
					evidence: entropyObjects.evidence,
				})
				.from(entropyObjects)
				.where(
					and(
						eq(entropyObjects.sessionId, input.session_id),
						eq(entropyObjects.target, target),
						eq(entropyObjects.runId, headRunId),
					),
				)
				.orderBy(asc(entropyObjects.dimension))
		: [];

	const pending = await getPendingOverlays();
	const data = projectWhyTable(
		input.table_id,
		tableName,
		readinessRow ?? null,
		evidenceRows,
		pending.length,
	);

	// Nothing to explain when there's no readiness row — skip the LLM call.
	const analysis = data.analyzed ? await synthesizeAnalysis(data) : "";

	return { ...data, analysis };
}

export const whyTableTool = toolDefinition({
	name: "why_table",
	description:
		"Explain ONE table's whole-table readiness — why it lands in its band for the " +
		"query, aggregation, and reporting intents — grounded in the persisted " +
		"drivers (ranked by how much fixing each would help) and the underlying " +
		"detector evidence, with a short synthesized explanation. Read-only. Use " +
		"after look_table (with a session_id) to drill into the table-grain band; " +
		"identify it by its table_id and the begin_session session_id. signal_count " +
		"shows how many detector signals back the explanation — a low count means the " +
		"picture is partial.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe(
				"The begin_session session the table-grain readiness belongs to.",
			),
		table_id: z
			.string()
			.describe(
				"The table to explain (a table_id from list_tables / look_table).",
			),
	}),
	outputSchema: WhyTableResult,
}).server((input) => whyTable(input));
