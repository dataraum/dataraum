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
import { asc, eq } from "drizzle-orm";
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
	currentEntropyObjects,
	currentEntropyReadiness,
	tables,
} from "../db/metadata/schema";
import { linkedAbortController } from "../lib/abort";
import { displayTableName, renderEvidenceDetail } from "../lib/display-names";
import { MAX_OUTPUT_TOKENS, MODEL } from "../llm";
import { mergeCurrentEvidence, pickCurrentRow } from "./readiness-grain";
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
	// detector), rendered agent-safe via `renderEvidenceDetail` (DAT-433) — the
	// narrative + the widget render it as-is.
	detail: z.string(),
});

const WhyColumnResult = z.object({
	column_id: z.string(),
	column_name: z.string(),
	// Display name (`src_<digest>__` prefix stripped, DAT-431) — this result feeds
	// the agent's context and the synthesis prompt; the round-trip key is column_id.
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

/** The target column's readiness + table context (from the promoted detect run). */
export interface WhyReadinessRow {
	columnId: string;
	columnName: string;
	/** Raw physical table name (`src_<digest>__<stem>`) — projected to display form. */
	tableName: string;
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
		column_id: readiness.columnId,
		column_name: readiness.columnName,
		// The agent reads this result (and the synthesis prompt interpolates it) —
		// strip the content-keyed `src_<digest>__` prefix here so no hash name
		// reaches LLM context (DAT-431). Idempotent for already-plain names.
		table_name: displayTableName(readiness.tableName),
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
 * `signal` is the tool-context abort (DAT-449): a stopped run aborts this
 * nested call instead of billing it to completion.
 */
export async function synthesizeAnalysis(
	data: WhyColumnData,
	signal?: AbortSignal,
): Promise<string> {
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		abortController: linkedAbortController(signal),
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
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
	signal?: AbortSignal,
): Promise<WhyColumnResult> {
	// Resolve the column + its table first — even an unanalyzed column has a name.
	const [col] = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			tableName: tables.tableName,
			tableId: tables.tableId,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.where(eq(columns.columnId, input.column_id))
		.limit(1);

	if (!col) {
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

	// The current_* views ARE the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database — no head resolution, no runId plumbing. No promoted
	// run → empty views → unanalyzed (null band). The view is multi-grain
	// (add_source table head + session-grain heads coexist) — fetch all grains
	// and pick explicitly: session re-roll supersedes the add_source verdict.
	const readinessRow = pickCurrentRow(
		await metadataDb
			.select({
				band: currentEntropyReadiness.band,
				worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
				intents: currentEntropyReadiness.intents,
				computedAt: currentEntropyReadiness.computedAt,
				viaTableHead: currentEntropyReadiness.viaTableHead,
				viaSessionHead: currentEntropyReadiness.viaSessionHead,
				viaOperatingModelHead: currentEntropyReadiness.viaOperatingModelHead,
			})
			.from(currentEntropyReadiness)
			.where(eq(currentEntropyReadiness.columnId, input.column_id)),
	);

	// View columns type as nullable (Postgres views carry no NOT NULL); the
	// underlying tables guarantee these — coalesce at the edge.
	const readiness: WhyReadinessRow = {
		columnId: col.columnId ?? input.column_id,
		columnName: col.columnName ?? "",
		tableName: col.tableName ?? "",
		band: readinessRow?.band ?? null,
		worstIntentRisk: readinessRow?.worstIntentRisk ?? null,
		intents: readinessRow?.intents ?? null,
	};

	// Evidence comes from the same promoted-run view, so stale runs can't mix in
	// or inflate signal_count. Multi-grain merge per detector: add_source-only
	// detectors keep their table-head row; re-adjudicated detectors (e.g.
	// temporal_behavior's session pass) and operating_model fan-out rows
	// (cross_table_consistency on failed criticals) show the session verdict.
	const rawEvidence = mergeCurrentEvidence(
		await metadataDb
			.select({
				layer: currentEntropyObjects.layer,
				dimension: currentEntropyObjects.dimension,
				subDimension: currentEntropyObjects.subDimension,
				score: currentEntropyObjects.score,
				detectorId: currentEntropyObjects.detectorId,
				evidence: currentEntropyObjects.evidence,
				computedAt: currentEntropyObjects.computedAt,
				viaTableHead: currentEntropyObjects.viaTableHead,
				viaSessionHead: currentEntropyObjects.viaSessionHead,
				viaOperatingModelHead: currentEntropyObjects.viaOperatingModelHead,
			})
			.from(currentEntropyObjects)
			.where(eq(currentEntropyObjects.columnId, input.column_id))
			.orderBy(asc(currentEntropyObjects.dimension)),
	);
	const evidenceRows = rawEvidence.map((e) => ({
		layer: e.layer ?? "",
		dimension: e.dimension ?? "",
		subDimension: e.subDimension ?? "",
		score: e.score ?? 0,
		detectorId: e.detectorId ?? "",
		evidence: e.evidence,
	}));

	const pending = await getPendingOverlays();
	const data = projectWhyData(readiness, evidenceRows, pending.length);

	// Nothing to explain when the column has no readiness row — skip the LLM call
	// (no cost, no risk of fabricating an explanation for absent data).
	const analysis = data.analyzed ? await synthesizeAnalysis(data, signal) : "";

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
}).server((input, ctx) => whyColumn(input, ctx?.abortSignal));
