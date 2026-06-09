// `answer` — the natural-language query tool (DAT-485, DD/33259521).
//
// The 4th-attempt query migration, landed as a thin TS consumer over the engine-
// owned snippet substrate. The tool is a nested @tanstack/ai chat() SUB-AGENT
// with its own internal tools (snippet_search over the validated KB, run_steps to
// validate SQL). It composes a question's answer as concept-named steps + a
// combining final_sql, reusing validated snippets; it VALIDATES the SQL and reads
// a bounded headline; the BROWSER executes the full result via the composed grid
// handle (so no rows ever enter the model context — inner OR outer). Gating is
// gone: a read-only data-quality band rides along as INFORMATION, never a filter.
//
// Data flow (one chat() call; combined tools+outputSchema is native for
// claude-sonnet-4-6):
//   chat([snippet_search, run_steps], QueryDraftSchema)
//     → resolveSnippetReferences  (substitute the stored validated SQL on exact
//                                  reuse; clear a hallucinated snippet_id)
//     → composeStandalone         (fold steps + final_sql → one grid statement)
//     → readDataQuality           (worst readiness band over the touched tables)
//     → AnswerSchema
//
// The outer tool wraps this in `asAgentError`: a failed sub-agent run becomes the
// `{ error }` envelope the orchestrator reads and retries, not a dead turn.

import { chat, maxIterations, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";

import { config } from "../config";
import { findById } from "../db/metadata/snippet-library";
import { composeStandalone, type RunStep, runSteps } from "../duckdb/run-steps";
import { linkedAbortController } from "../lib/abort";
import { determineUsageType } from "../lib/snippet-normalize";
import {
	MAX_OUTPUT_TOKENS,
	MODEL,
	QUERY_SUBAGENT_MAX_ITERATIONS,
} from "../llm";
import { getQueryInstructions } from "../prompts";
import { asAgentError, withAgentError } from "./agent-error";
import { listTables } from "./list-tables";
import { buildSchemaBlock } from "./query-context";
import { buildVocabularyBlock, snippetSearchTool } from "./snippet-search";

// The three readiness bands the engine emits, worst-first ranked.
const BAND = z.enum(["ready", "investigate", "blocked"]);
type Band = z.infer<typeof BAND>;
const BAND_SEVERITY: Record<Band, number> = {
	ready: 1,
	investigate: 2,
	blocked: 3,
};

// --- The model's structured draft (inner outputSchema). The model emits the
// decomposed query + provenance; the grid handle + data-quality band are derived
// post-chat (deterministic), NOT asked of the model.

const QueryStepDraft = z.object({
	name: z
		.string()
		.describe(
			"The step's concept name — a SQL identifier; becomes a temp view.",
		),
	sql: z.string().describe("Standalone DuckDB SQL for this step (a SELECT)."),
	snippet_id: z
		.string()
		.nullable()
		.optional()
		.describe("The reused/adapted snippet's id; omit for fresh SQL."),
});

const QueryDraftSchema = z.object({
	answer: z
		.string()
		.describe(
			"The practitioner-facing reply with the headline number(s) from the " +
				"validated sample. No SQL, tool names, or internal identifiers.",
		),
	steps: z
		.array(QueryStepDraft)
		.describe("The concept-named steps you validated (may be empty)."),
	final_sql: z
		.string()
		.describe("The query combining the step views into the final result."),
	assumptions: z
		.array(z.string())
		.describe(
			"Plain-sentence decisions made to resolve ambiguity (may be empty).",
		),
	concepts_used: z
		.array(z.string())
		.describe("The business concepts the answer draws on (provenance)."),
	tables_touched: z
		.array(z.string())
		.describe(
			"The physical table names the SQL reads (the <name> in lake.<layer>.<name>).",
		),
});
export type QueryDraft = z.infer<typeof QueryDraftSchema>;

// --- The tool's answer shape (outer). grid + data_quality are derived; both are
// nullable for the degenerate cases (no runnable query / no analyzed table).

const Grid = z
	.object({
		// The single self-contained statement the browser grid streams in full
		// (composeStandalone of the resolved steps). No params: the sub-agent bakes
		// literals from the question, so the grid query is fully literal.
		sql: z.string(),
	})
	.nullable();

const DataQuality = z
	.object({
		band: BAND,
		note: z.string().optional(),
	})
	.nullable();

export const AnswerSchema = z.object({
	answer: z.string(),
	// The grid handle — the browser streams the FULL result from this SQL
	// (DAT-490 uncapped). null when the sub-agent produced no runnable query.
	grid: Grid,
	assumptions: z.array(z.string()),
	concepts_used: z.array(z.string()),
	tables_touched: z.array(z.string()),
	// READ from the readiness views for the touched tables — informational, NEVER
	// a gate. null when no touched table has been analyzed.
	data_quality: DataQuality,
});
export type AnswerResult = z.infer<typeof AnswerSchema>;

// --- run_steps as the sub-agent's internal validator tool.

const RunStepsOk = z.object({
	ok: z.literal(true),
	columns: z.array(z.string()),
	rowCount: z.number(),
	sample: z.array(z.record(z.string(), z.unknown())),
	truncated: z.boolean(),
});

export const runStepsTool = toolDefinition({
	name: "run_steps",
	description:
		"Validate your decomposed query before answering: each step becomes a temp " +
		"view, then final_sql runs against them on a read-only connection. Returns " +
		"ok with the result columns + a BOUNDED headline sample (not the full " +
		"result — the full result streams to the user's grid), or an error message " +
		"to repair and retry. Always call this before you answer.",
	inputSchema: z.object({
		steps: z
			.array(z.object({ name: z.string(), sql: z.string() }))
			.describe("The concept steps; each becomes a temp view named `name`."),
		final_sql: z
			.string()
			.describe("The query combining the step views into the final result."),
	}),
	outputSchema: withAgentError(RunStepsOk),
}).server((input, ctx) =>
	runSteps({ steps: input.steps, finalSql: input.final_sql }, ctx?.abortSignal),
);

// --- Reuse resolution (port of agent.py `_resolve_snippet_references`).

/** A resolved step: the model's step after snippet substitution. */
export interface ResolvedStep {
	name: string;
	sql: string;
	snippet_id: string | null;
}

/**
 * Resolve each step's `snippet_id` against the stored snippet (the reuse teeth):
 * - no snippet_id → kept as fresh SQL;
 * - unknown id → cleared to null (the model hallucinated it), SQL kept as fresh;
 * - known id, SQL matches the stored snippet (normalized) → exact reuse: SUBSTITUTE
 *   the authoritative validated SQL;
 * - known id, SQL differs → adaptation: keep the model's SQL, snippet_id tracks
 *   provenance.
 *
 * findById is a globally-unique-PK lookup (P0) — not workspace-scoped — so a
 * snippet_id the model got from snippet_search (which IS workspace-scoped)
 * resolves to the one row it names.
 */
export async function resolveSnippetReferences(
	steps: ResolvedStep[],
): Promise<ResolvedStep[]> {
	const resolved: ResolvedStep[] = [];
	for (const step of steps) {
		if (!step.snippet_id) {
			resolved.push({ ...step, snippet_id: null });
			continue;
		}
		const record = await findById(step.snippet_id);
		if (record === null) {
			// Hallucinated id — treat the SQL as fresh.
			resolved.push({ ...step, snippet_id: null });
			continue;
		}
		if (determineUsageType(step.sql, record.sql) === "exact_reuse") {
			resolved.push({ ...step, sql: record.sql });
		} else {
			resolved.push(step);
		}
	}
	return resolved;
}

// --- Data-quality band (informational; reuses the tested list_tables rollup).

/**
 * Read the worst readiness band across the touched tables (informational, never a
 * gate). Matches `tables_touched` against the inventory's physical OR display
 * names (the model may emit either), takes the worst band among the analyzed
 * matches, and notes which tables carry it. null when nothing matched or nothing
 * is analyzed yet.
 */
export async function readDataQuality(
	tablesTouched: string[],
): Promise<z.infer<typeof DataQuality>> {
	if (tablesTouched.length === 0) return null;

	const inventory = await listTables();
	const touched = new Set(tablesTouched.map((t) => t.toLowerCase()));
	const matched = inventory.filter(
		(t) =>
			touched.has(t.physical_name.toLowerCase()) ||
			touched.has(t.table_name.toLowerCase()),
	);

	const banded = matched.filter(
		(t): t is typeof t & { worst_band: Band } => t.worst_band !== null,
	);
	if (banded.length === 0) return null;

	const worst = banded.reduce<Band>(
		(acc, t) =>
			BAND_SEVERITY[t.worst_band] > BAND_SEVERITY[acc] ? t.worst_band : acc,
		"ready",
	);
	const worstTables = banded
		.filter((t) => t.worst_band === worst)
		.map((t) => t.table_name);

	return {
		band: worst,
		note: `Worst readiness across the queried tables is '${worst}' (${worstTables.join(", ")}).`,
	};
}

// --- Assembly (pure) + the sub-agent.

/**
 * Assemble the answer from the model draft + resolved steps + data-quality band
 * (pure). Composes the single grid statement from the resolved steps; null grid
 * when there's no runnable query (empty final_sql). Unit-tested.
 */
export function assembleAnswer(
	draft: QueryDraft,
	resolvedSteps: ResolvedStep[],
	dataQuality: z.infer<typeof DataQuality>,
): AnswerResult {
	const stepsForGrid: RunStep[] = resolvedSteps.map((s) => ({
		name: s.name,
		sql: s.sql,
	}));
	const composed = composeStandalone(stepsForGrid, draft.final_sql);
	const grid = composed.trim() !== "" ? { sql: composed } : null;
	return {
		answer: draft.answer,
		grid,
		assumptions: draft.assumptions,
		concepts_used: draft.concepts_used,
		tables_touched: draft.tables_touched,
		data_quality: dataQuality,
	};
}

/**
 * The query sub-agent: ONE nested chat() over [snippet_search, run_steps] with the
 * concrete `QueryDraftSchema`, then deterministic post-processing (reuse
 * substitution → grid composition → data-quality read). `signal` forwards the
 * outer run's abort into the nested call and the run_steps validator.
 */
export async function querySubAgent(
	question: string,
	signal?: AbortSignal,
): Promise<AnswerResult> {
	const [schemaBlock, vocabularyBlock] = await Promise.all([
		buildSchemaBlock(),
		buildVocabularyBlock(),
	]);

	const userMessage = `<question>\n${question}\n</question>\n\n${schemaBlock}\n\n${vocabularyBlock}`;

	// Combined tools + outputSchema is native for claude-sonnet-4-6 — one call
	// runs the tool loop and returns the validated structured draft (no separate
	// finalize round-trip). The concrete schema gives a typed result.
	const draft = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		abortController: linkedAbortController(signal),
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		agentLoopStrategy: maxIterations(QUERY_SUBAGENT_MAX_ITERATIONS),
		systemPrompts: [getQueryInstructions()],
		messages: [{ role: "user", content: userMessage }],
		tools: [snippetSearchTool, runStepsTool],
		outputSchema: QueryDraftSchema,
	});

	const resolvedSteps = await resolveSnippetReferences(
		draft.steps.map((s) => ({
			name: s.name,
			sql: s.sql,
			snippet_id: s.snippet_id ?? null,
		})),
	);
	const dataQuality = await readDataQuality(draft.tables_touched);
	return assembleAnswer(draft, resolvedSteps, dataQuality);
}

export const answerTool = toolDefinition({
	name: "answer",
	description:
		"Answer a natural-language question about the workspace's imported data by " +
		"composing and validating grounded DuckDB SQL — reusing validated snippets " +
		"from the knowledge base where they fit. Returns the practitioner answer " +
		"with the headline figure, a grid handle whose full result streams in the " +
		"canvas, the assumptions made, the concepts and tables used, and an " +
		"informational data-quality band for the tables touched (NOT a gate). " +
		"Read-only. Use for analytical questions ('what is total revenue', 'monthly " +
		"sales trend') once data has been imported and typed.",
	inputSchema: z.object({
		question: z
			.string()
			.describe(
				"The natural-language question to answer over the workspace data.",
			),
	}),
	outputSchema: withAgentError(AnswerSchema),
}).server((input, ctx) =>
	asAgentError(() => querySubAgent(input.question, ctx?.abortSignal)),
);
