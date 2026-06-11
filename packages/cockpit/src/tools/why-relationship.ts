// why_relationship tool (DAT-409) — explain one relationship's readiness.
//
// The per-relationship drill-down behind look_relationships, mirroring why_column.
// begin_session's detect writes relationship-granularity readiness + detector
// evidence keyed by a `relationship:{from_col}::{to_col}` target (DAT-408), sealed
// under a `session:{id}` head. This reads the PRE-COMPUTED diagnosis (per-intent
// drivers from `entropy_readiness`, raw detector evidence from `entropy_objects`)
// for the promoted run, then asks Anthropic for ONE grounded narrative explaining
// why the relationship lands in its band per intent. It does NOT recompute
// readiness (the engine owns the rollup) and does NOT propose teaches.
//
// Read-only → no approval. The pure row→shape assembly (`projectWhyRelationship`)
// is unit-tested; the live DB read + the LLM synthesis are smoke-covered.

import { chat, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { and, asc, eq, inArray } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import { relationshipTargetKey } from "../db/metadata/relationship-target";
import {
	columns,
	currentEntropyObjects,
	currentEntropyReadiness,
	tables,
} from "../db/metadata/schema";
import { linkedAbortController } from "../lib/abort";
import { displayTableName, renderEvidenceDetail } from "../lib/display-names";
import { MAX_OUTPUT_TOKENS, MODEL } from "../llm";
import { getWhyInstructions } from "../prompts";
import {
	mergeCurrentEvidence,
	pickCurrentRow,
	projectVerdictHistory,
	stageOfRow,
	type VerdictHistoryEntry,
	VerdictHistorySchema,
} from "./readiness-grain";

// --- Tool output (mirrors why_column, keyed on the relationship pair).

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

const WhyRelationshipResult = z.object({
	from_column_id: z.string(),
	to_column_id: z.string(),
	// Endpoint names for the narrative + widget — table names in DISPLAY form
	// (`src_<digest>__` prefix stripped, DAT-431; round-trips key on the column
	// ids); null when an id no longer resolves.
	from_table_name: z.string().nullable(),
	from_column_name: z.string().nullable(),
	to_table_name: z.string().nullable(),
	to_column_name: z.string().nullable(),
	// False when the pair matched no relationship readiness row in the promoted run
	// — distinct from "found but not analyzed".
	found: z.boolean(),
	band: z.string().nullable(),
	// WHICH pipeline stage sealed the shown verdict (DAT-513) + when. null
	// when unanalyzed.
	band_stage: z.string().nullable(),
	band_computed_at: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	analyzed: z.boolean(),
	intents: z.array(IntentExplanation),
	evidence: z.array(EvidenceSignal),
	signal_count: z.number(),
	// Every coexisting readiness snapshot for this pair, oldest first.
	verdict_history: z.array(VerdictHistorySchema),
	analysis: z.string(),
	pending_teaches: z.number(),
});
export type WhyRelationshipResult = z.infer<typeof WhyRelationshipResult>;

/** The structured non-narrative payload (everything except `analysis`). */
export type WhyRelationshipData = Omit<WhyRelationshipResult, "analysis">;

/** The relationship's readiness row from the promoted run (null fields = no row). */
export interface WhyRelReadinessRow {
	band: string | null;
	/** Stage that sealed the picked verdict (DAT-513); null when unanalyzed. */
	bandStage: string | null;
	bandComputedAt: Date | null;
	worstIntentRisk: number | null;
	intents: unknown;
}

/** One entropy_objects row for the relationship target. */
export interface WhyRelEvidenceRow {
	layer: string;
	dimension: string;
	subDimension: string;
	score: number;
	detectorId: string;
	evidence: unknown;
}

/** Endpoint names as resolved from the DB — table names still in raw physical
 * form (`src_<digest>__<stem>`); the projection strips them for display. */
export interface RelEndpoints {
	fromTableName: string | null;
	fromColumnName: string | null;
	toTableName: string | null;
	toColumnName: string | null;
}

/**
 * Assemble the structured why-payload from the readiness row + evidence rows.
 * Pure (no DB, no LLM) so the parsing + correlation is unit-testable. `found`
 * distinguishes "no such relationship in this run" (null readiness) from "found
 * but no readiness row". A malformed intents blob degrades to empty.
 */
export function projectWhyRelationship(
	fromColumnId: string,
	toColumnId: string,
	endpoints: RelEndpoints,
	readiness: WhyRelReadinessRow | null,
	evidenceRows: WhyRelEvidenceRow[],
	pendingTeaches: number,
	verdictHistory: VerdictHistoryEntry[] = [],
): WhyRelationshipData {
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
	// table-name keys (`from_table`/`to_table` from the relationship detectors)
	// display-mapped, src-digest backstop applied.
	const evidence: z.infer<typeof EvidenceSignal>[] = evidenceRows.map((e) => ({
		dimension_path: `${e.layer}.${e.dimension}.${e.subDimension}`,
		detector_id: e.detectorId,
		score: e.score,
		detail: renderEvidenceDetail(e.evidence),
	}));

	return {
		from_column_id: fromColumnId,
		to_column_id: toColumnId,
		// The agent reads this result (and the synthesis prompt interpolates the
		// labels) — strip the content-keyed `src_<digest>__` prefix here so no hash
		// name reaches LLM context (DAT-431).
		from_table_name:
			endpoints.fromTableName === null
				? null
				: displayTableName(endpoints.fromTableName),
		from_column_name: endpoints.fromColumnName,
		to_table_name:
			endpoints.toTableName === null
				? null
				: displayTableName(endpoints.toTableName),
		to_column_name: endpoints.toColumnName,
		// Found = there's either a readiness row or at least one evidence signal for
		// the pair in the promoted run.
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
 * drivers + evidence we pass. `signal` is the tool-context abort (DAT-449): a
 * stopped run aborts this nested call instead of billing it to completion. */
export async function synthesizeAnalysis(
	data: WhyRelationshipData,
	signal?: AbortSignal,
): Promise<string> {
	const fromLabel = `${data.from_table_name ?? "?"}.${data.from_column_name ?? data.from_column_id}`;
	const toLabel = `${data.to_table_name ?? "?"}.${data.to_column_name ?? data.to_column_id}`;
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		abortController: linkedAbortController(signal),
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		systemPrompts: [getWhyInstructions()],
		messages: [
			{
				role: "user",
				content: `Explain the readiness for the relationship "${fromLabel}" → "${toLabel}", grounded only in these signals:\n\n${JSON.stringify(
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
					"The grounded narrative explaining why the relationship lands in its band per intent — drawn ONLY from the provided drivers + evidence, no outside facts.",
				),
		}),
	});
	return result.analysis;
}

export interface WhyRelationshipInput {
	session_id: string;
	from_column_id: string;
	to_column_id: string;
}

/** Explain one relationship's readiness: pre-computed drivers + evidence + narrative. */
export async function whyRelationship(
	input: WhyRelationshipInput,
	signal?: AbortSignal,
): Promise<WhyRelationshipResult> {
	const target = relationshipTargetKey(
		input.from_column_id,
		input.to_column_id,
	);

	// Resolve endpoint names up front — even an unanalyzed relationship has names.
	const endpoints = await loadEndpoints(
		input.from_column_id,
		input.to_column_id,
	);

	// The current_* views ARE the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database — no head resolution, no runId plumbing. No promoted
	// run → empty views → unanalyzed.
	// Relationship targets are written by session-grain runs only, but the pick
	// keeps the read uniform with why_column/why_table (and deterministic should
	// a second grain ever appear).
	const allReadinessRows = await metadataDb
		.select({
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			intents: currentEntropyReadiness.intents,
			computedAt: currentEntropyReadiness.computedAt,
			sessionId: currentEntropyReadiness.sessionId,
			runId: currentEntropyReadiness.runId,
			viaTableHead: currentEntropyReadiness.viaTableHead,
			viaSessionHead: currentEntropyReadiness.viaSessionHead,
			viaOperatingModelHead: currentEntropyReadiness.viaOperatingModelHead,
		})
		.from(currentEntropyReadiness)
		.where(
			and(
				eq(currentEntropyReadiness.sessionId, input.session_id),
				eq(currentEntropyReadiness.target, target),
			),
		);
	const readinessRow = pickCurrentRow(allReadinessRows);

	// Evidence is keyed by the relationship target (relationship rows have no
	// column_id); the view scopes to the promoted run, so stale runs can't
	// inflate signal_count. View columns type as nullable (Postgres views carry
	// no NOT NULL) — coalesce at the edge.
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
			viaSessionHead: currentEntropyObjects.viaSessionHead,
			viaOperatingModelHead: currentEntropyObjects.viaOperatingModelHead,
		})
		.from(currentEntropyObjects)
		.where(
			and(
				eq(currentEntropyObjects.sessionId, input.session_id),
				eq(currentEntropyObjects.target, target),
			),
		)
		.orderBy(asc(currentEntropyObjects.dimension));
	const rawEvidence = mergeCurrentEvidence(unmergedEvidence);
	const evidenceRows = rawEvidence.map((e) => ({
		layer: e.layer ?? "",
		dimension: e.dimension ?? "",
		subDimension: e.subDimension ?? "",
		score: e.score ?? 0,
		detectorId: e.detectorId ?? "",
		evidence: e.evidence,
	}));

	const pending = await getPendingOverlays();
	const data = projectWhyRelationship(
		input.from_column_id,
		input.to_column_id,
		endpoints,
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

/** Resolve the from/to column + table names for a relationship pair. */
async function loadEndpoints(
	fromColumnId: string,
	toColumnId: string,
): Promise<RelEndpoints> {
	const rows = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			tableName: tables.tableName,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.where(inArray(columns.columnId, [fromColumnId, toColumnId]));

	const byId = new Map(
		rows.map((r) => [
			r.columnId,
			{ columnName: r.columnName, tableName: r.tableName },
		]),
	);
	const from = byId.get(fromColumnId);
	const to = byId.get(toColumnId);
	return {
		fromTableName: from?.tableName ?? null,
		fromColumnName: from?.columnName ?? null,
		toTableName: to?.tableName ?? null,
		toColumnName: to?.columnName ?? null,
	};
}

export const whyRelationshipTool = toolDefinition({
	name: "why_relationship",
	description:
		"Explain ONE relationship's readiness — why it lands in its band for the " +
		"query, aggregation, and reporting intents — grounded in the persisted " +
		"drivers (ranked by how much fixing each would help) and the underlying " +
		"detector evidence, with a short synthesized explanation. Read-only. Use " +
		"after look_relationships to drill into a specific relationship; identify it " +
		"by its directional column pair (from_column_id → to_column_id) and the " +
		"session_id. signal_count shows how many detector signals back the " +
		"explanation — a low count means the picture is partial.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe("The begin_session session the relationship belongs to."),
		from_column_id: z
			.string()
			.describe(
				"The 'from' (foreign-key) side column id (from look_relationships).",
			),
		to_column_id: z
			.string()
			.describe(
				"The 'to' (referenced) side column id (from look_relationships).",
			),
	}),
	outputSchema: WhyRelationshipResult,
}).server((input, ctx) => whyRelationship(input, ctx?.abortSignal));
