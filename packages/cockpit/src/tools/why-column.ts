// why_column tool (DAT-351) — explain one column's readiness.
//
// The per-column drill-down behind look_table. It reads the PRE-COMPUTED
// diagnosis the engine persists (DAT-394/399): the per-intent drivers (each a
// labeled dimension with a causal `impact_delta`) from `entropy_readiness`, plus
// the raw detector evidence from `entropy_objects`, correlated by dimension_path.
// It then asks Anthropic for ONE grounded narrative explaining why the column
// lands in its band per intent (the synthesis the orchestrator surfaces as the
// "why"). It does NOT recompute readiness (engine owns the rollup) and does NOT
// propose teaches (resolution suggestions are a deferred follow-up).
//
// Read-only → no approval. The pure row→shape assembly (`projectWhyData`) is
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
	columns,
	entropyObjects,
	entropyReadiness,
	tables,
} from "../db/metadata/schema";
import { MAX_OUTPUT_TOKENS, MODEL } from "../llm";
import { getWhyInstructions } from "../prompts";

// The persisted JSONB grammar (intents / drivers) is shared with look_table —
// see `db/metadata/readiness-schemas.ts`. Parsed leniently below.

// --- Tool output.

const IntentExplanation = z.object({
	intent: z.string(),
	band: z.string(),
	risk: z.number(),
	// The full per-intent drivers (why_column keeps these; look_table dropped them).
	drivers: z.array(ReadinessDriver),
});

const EvidenceSignal = z.object({
	dimension_path: z.string(),
	detector_id: z.string(),
	score: z.number(),
	// Compact JSON of the detector-specific evidence blob (shape varies per
	// detector); the narrative + the widget render it as-is.
	detail: z.string(),
});

const WhyColumnResult = z.object({
	column_id: z.string(),
	column_name: z.string(),
	table_name: z.string(),
	// False when column_id matched no column — distinct from "found but not yet
	// analyzed" (found=true, band=null), so the widget can tell the two apart
	// without sniffing an empty name.
	found: z.boolean(),
	// null band = no readiness row yet (not analyzed).
	band: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	analyzed: z.boolean(),
	intents: z.array(IntentExplanation),
	evidence: z.array(EvidenceSignal),
	// How many detector signals back this explanation — surfaced so the narrative
	// (and the reader) can see when the picture is partial (orphaned detectors).
	signal_count: z.number(),
	analysis: z.string(),
	pending_teaches: z.number(),
});
export type WhyColumnResult = z.infer<typeof WhyColumnResult>;

/** The structured non-narrative payload (everything except `analysis`). */
export type WhyColumnData = Omit<WhyColumnResult, "analysis">;

/** One joined (columns ⟕ entropy_readiness) row for the target column. */
export interface WhyReadinessRow {
	columnId: string;
	columnName: string;
	tableName: string;
	// The detect run that wrote this readiness row — used to scope evidence to the
	// SAME run (entropy_objects accumulate across re-runs). Null when not analyzed.
	sessionId: string | null;
	band: string | null;
	worstIntentRisk: number | null;
	intents: unknown;
}

/** One entropy_objects row for the target column. */
export interface WhyEvidenceRow {
	layer: string;
	dimension: string;
	subDimension: string;
	score: number;
	detectorId: string;
	evidence: unknown;
}

/** Compact, JSON-safe rendering of a detector's evidence blob. */
function renderDetail(evidence: unknown): string {
	if (evidence === null || evidence === undefined) return "";
	return JSON.stringify(evidence);
}

/**
 * Assemble the structured (non-narrative) why-payload from the readiness row and
 * the column's evidence rows. Pure — no DB, no LLM — so the parsing + correlation
 * is unit-testable. `signal_count` counts the distinct evidence signals (the
 * basis for the "based on N signals" honesty when detectors are sparse).
 */
export function projectWhyData(
	readiness: WhyReadinessRow,
	evidenceRows: WhyEvidenceRow[],
	pendingTeaches: number,
): WhyColumnData {
	const parsed = PersistedIntent.array().safeParse(readiness.intents);
	const intents: z.infer<typeof IntentExplanation>[] = parsed.success
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
		column_id: readiness.columnId,
		column_name: readiness.columnName,
		table_name: readiness.tableName,
		found: true,
		band: readiness.band ?? null,
		worst_intent_risk: readiness.worstIntentRisk ?? null,
		analyzed: readiness.band !== null,
		intents,
		evidence,
		signal_count: evidence.length,
		pending_teaches: pendingTeaches,
	};
}

/**
 * Synthesize the grounded narrative from the structured data via one forced
 * Anthropic call. Split out so the assembly is testable apart from the LLM. The
 * model sees only the band + drivers + evidence we pass — never the whole DB.
 */
export async function synthesizeAnalysis(data: WhyColumnData): Promise<string> {
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		maxTokens: MAX_OUTPUT_TOKENS,
		systemPrompts: [getWhyInstructions()],
		messages: [
			{
				role: "user",
				content: `Explain the readiness for column "${data.column_name}" of table "${data.table_name}", grounded only in these signals:\n\n${JSON.stringify(
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
					"The grounded narrative explaining why the column lands in its band per intent — drawn ONLY from the provided drivers + evidence, no outside facts.",
				),
		}),
	});
	return result.analysis;
}

export interface WhyColumnInput {
	column_id: string;
}

/** Explain one column's readiness: pre-computed drivers + evidence + narrative. */
export async function whyColumn(
	input: WhyColumnInput,
): Promise<WhyColumnResult> {
	const [readiness] = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			tableName: tables.tableName,
			sessionId: entropyReadiness.sessionId,
			band: entropyReadiness.band,
			worstIntentRisk: entropyReadiness.worstIntentRisk,
			intents: entropyReadiness.intents,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.leftJoin(entropyReadiness, eq(entropyReadiness.columnId, columns.columnId))
		.where(eq(columns.columnId, input.column_id))
		.limit(1);

	if (!readiness) {
		return {
			column_id: input.column_id,
			column_name: "",
			table_name: "",
			found: false,
			band: null,
			worst_intent_risk: null,
			analyzed: false,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "",
			pending_teaches: 0,
		};
	}

	// Scope evidence to the SAME detect run that wrote the readiness row:
	// entropy_objects accumulate across re-runs (no per-run cleanup / unique
	// constraint), so an unscoped read would inflate signal_count with stale
	// duplicates and make the "based on N signals" honesty a lie. A not-analyzed
	// column (sessionId null) matches no rows — correct, there's nothing to show.
	const evidenceRows = readiness.sessionId
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
						eq(entropyObjects.columnId, input.column_id),
						eq(entropyObjects.sessionId, readiness.sessionId),
					),
				)
				.orderBy(asc(entropyObjects.dimension))
		: [];

	const pending = await getPendingOverlays();
	const data = projectWhyData(readiness, evidenceRows, pending.length);

	// Nothing to explain when the column has no readiness row — skip the LLM call
	// (no cost, no risk of fabricating an explanation for absent data).
	const analysis = data.analyzed ? await synthesizeAnalysis(data) : "";

	return { ...data, analysis };
}

export const whyColumnTool = toolDefinition({
	name: "why_column",
	description:
		"Explain ONE column's readiness — why it lands in its band for the query, " +
		"aggregation, and reporting intents — grounded in the persisted drivers " +
		"(ranked by how much fixing each would help) and the underlying detector " +
		"evidence, with a short synthesized explanation. Read-only. Use after " +
		"look_table to drill into a specific column. Identify the column by its " +
		"column_id (from look_table). signal_count shows how many detector signals " +
		"back the explanation — a low count means the picture is partial.",
	inputSchema: z.object({
		column_id: z
			.string()
			.describe("The column to explain (a column_id from look_table)."),
	}),
	outputSchema: WhyColumnResult,
}).server((input) => whyColumn(input));
