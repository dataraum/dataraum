// Report-summary regenerator (DAT-625) — a Haiku ONE-SHOT that refreshes a stale
// report summary against the current result. Nested `chat()` with an outputSchema,
// the proven nav-agent / answer-tool pattern; runs only when the user clicks
// "Regenerate" on an outdated report. SERVER-ONLY (adapter + key).
//
// Unlike the nav-agent, this is NOT best-effort: a failure surfaces to the caller so
// the route can report it and keep the old summary + outdated badge — a wrong refresh
// would silently replace a human-trusted summary with worse prose, so we'd rather
// fail loudly than guess. The SQL is unchanged; only the data moved, so the job is
// purely "restate the same analysis with the current numbers, same voice".

import { chat } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";

import { config } from "#/config";
import type { QueryResult } from "#/duckdb/query-result";
import { MAX_OUTPUT_TOKENS, SUMMARY_MODEL } from "#/llm";

const SYSTEM = `You refresh the saved summary of a data report. The report's SQL is unchanged; only the underlying data has moved, so some numbers/claims in the old summary are now stale.

Given the PREVIOUS summary and the CURRENT result, rewrite the summary so every figure and claim matches the current result. Preserve the original voice, structure, length, and level of detail — change the numbers and any claim they support, nothing else. Do not add commentary about the refresh, the data change, or the SQL. Output only the new summary prose.`;

/** How many headline rows of the fresh result to put in front of the model. The
 * result is already bounded to the fingerprint's headline rows; this caps the prompt
 * payload further so a wide result can't blow the Haiku context. */
const PROMPT_ROW_CAP = 50;

/**
 * Regenerate a report's summary against its current result via a Haiku one-shot,
 * preserving the original voice. Throws on any LLM failure — the caller keeps the old
 * summary and the outdated badge rather than persisting an unverified replacement.
 */
export async function regenerateSummary(
	oldSummary: string,
	result: QueryResult,
): Promise<string> {
	const preview = {
		columns: result.columns,
		rows: result.rows.slice(0, PROMPT_ROW_CAP),
		rowCount: result.rowCount,
	};
	const userContent = `PREVIOUS SUMMARY:\n${oldSummary}\n\nCURRENT RESULT (JSON):\n${JSON.stringify(preview)}`;

	const { summary } = await chat({
		adapter: createAnthropicChat(SUMMARY_MODEL, config.anthropicApiKey),
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		systemPrompts: [{ content: SYSTEM }],
		messages: [{ role: "user", content: userContent }],
		outputSchema: z.object({ summary: z.string() }),
	});
	return summary;
}
