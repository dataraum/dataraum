// Text-to-chart authoring (DAT-626 / ADR-0015) — the PRIMARY path: a typed
// instruction → a validated chart config.
//
// Same forced-tool drain-stream shape as `induceStructured` (frame-family.ts): the
// model is given a single `emit_chart` tool whose input IS the thin ChartConfig
// subset and `tool_choice` FORCES it, so the structured value arrives as validated
// tool arguments — NOT `chat({ outputSchema })` (the Anthropic native path rejects
// our schemas; see frame-family.ts for the full why).
//
// What this adds over a single emit: a CIRCUIT-BREAKER. The subset schema can't
// express "this field must be a real result column" or "this must compile", so each
// emission goes through `validateChartConfig`; on failure the error is fed back and
// the model re-emits, up to CHART_AUTHOR_MAX_ATTEMPTS. Then we give up and tell the
// user to map it manually — NOT a heavy repair loop.
//
// CONTEXT IS DELIBERATELY THIN (ADR-0015): result columns + their measurement types
// + the user's instruction. NO catalogue, NO query-context, NO result-column→
// lineage name-matching (fragile + false-positive-prone on composed-SQL aliases).
// The instruction carries intent; the model reads measure/temporal from name+type.

import { chat, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { config } from "#/config";
import { linkedAbortController } from "#/lib/abort";
import { llmTelemetryMiddleware } from "#/lib/llm-telemetry";
import { MAX_OUTPUT_TOKENS, MODEL } from "#/llm";
import {
	type ChartConfig,
	ChartConfigSchema,
	FIELD_TYPES,
} from "./chart-config";
import { validateChartConfig } from "./validate";

/** Re-emit attempts before falling back to manual mapping (the circuit breaker). */
export const CHART_AUTHOR_MAX_ATTEMPTS = 3;

/** A result column the author can encode: its name + a measurement-type hint
 * (`quantitative` / `nominal` / `ordinal` / `temporal`) read off the DuckDB type. */
export interface ChartColumn {
	name: string;
	type: string;
}

export type AuthorChartResult =
	| { ok: true; config: ChartConfig }
	| { ok: false; error: string };

/** A chart-author conversation turn. */
export type AuthorMessage = { role: "user" | "assistant"; content: string };

/** One forced-emit turn: given the prompts + conversation, return the raw emitted
 * args (untrusted) or `undefined` for no tool call. Injectable so the retry loop is
 * unit-testable without a live LLM (production uses {@link emitOnce}). */
export type EmitFn = (
	systemPrompts: string[],
	messages: AuthorMessage[],
	signal?: AbortSignal,
) => Promise<unknown>;

/** The chart-author system prompt — role, the encodable columns, and the hard
 * rules the subset enforces (so the model rarely trips the gate). The columns are
 * given as `name: type` lines; the instruction is the user message. */
function systemPrompt(columns: ChartColumn[]): string {
	const columnLines = columns.map((c) => `- ${c.name}: ${c.type}`).join("\n");
	return [
		"You author a chart specification for a tabular SQL result. You are given the",
		"result's columns (with a measurement-type hint) and a user instruction; emit",
		"ONE chart config via the `emit_chart` tool.",
		"",
		"Result columns:",
		columnLines,
		"",
		"Rules:",
		"- Encode `x` and `y` (both required) and optionally `color`, each referencing",
		"  ONE of the columns above by its EXACT name — never invent or rename a column.",
		`- Pick a measurement type per encoded field from: ${FIELD_TYPES.join(", ")}.`,
		"- Aggregate (sum/mean/median/min/max/count) when the instruction implies a",
		"  summary over categories; otherwise omit the aggregate for a raw value.",
		"- Choose the mark that best fits the instruction (bar for category↔measure,",
		"  line/area for a trend over time, point for a relationship, tick for spread).",
		"- Charts are for AGGREGATED/summarized results — prefer an aggregate + a small",
		"  set of categories over plotting thousands of raw rows.",
	].join("\n");
}

/**
 * One forced `emit_chart` turn — returns the raw emitted args (untrusted; the
 * caller validates) or `undefined` if the model emitted no tool call. Mirrors the
 * drain-stream + early-abort of `induceStructured`. Wrapped by the retry loop.
 */
const emitOnce: EmitFn = async (systemPrompts, messages, signal) => {
	let captured: unknown;
	const emit = toolDefinition({
		name: "emit_chart",
		description:
			"Return the chart specification as this tool's arguments, in the required structure.",
		inputSchema: ChartConfigSchema,
	}).server((input) => {
		captured = input;
		return { ok: true };
	});

	// Abort the in-flight request once we have the args (tool_choice would otherwise
	// keep forcing emit_chart and bill another turn that only re-emits).
	const abortController = linkedAbortController(signal);
	const stream = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		middleware: [llmTelemetryMiddleware("chart_author")],
		abortController,
		modelOptions: {
			max_tokens: MAX_OUTPUT_TOKENS,
			tool_choice: { type: "tool", name: "emit_chart" },
		},
		systemPrompts,
		messages,
		tools: [emit],
	});
	for await (const _chunk of stream) {
		if (captured !== undefined) {
			abortController?.abort();
			break;
		}
	}
	return captured;
};

/**
 * Author a chart from a typed instruction over the given result columns. Validates
 * each emission and re-prompts with the error up to {@link CHART_AUTHOR_MAX_ATTEMPTS}
 * times (the circuit breaker), then returns an error for the modal to surface
 * (fall back to manual mapping).
 */
export async function authorChart(opts: {
	columns: ChartColumn[];
	instruction: string;
	signal?: AbortSignal;
	/** Injected for tests; defaults to the live forced-emit call. */
	emit?: EmitFn;
}): Promise<AuthorChartResult> {
	const emit = opts.emit ?? emitOnce;
	const columnNames = opts.columns.map((c) => c.name);
	const prompt = systemPrompt(opts.columns);
	const messages: AuthorMessage[] = [
		{ role: "user", content: opts.instruction },
	];

	let lastError = "the model emitted no chart";
	for (let attempt = 1; attempt <= CHART_AUTHOR_MAX_ATTEMPTS; attempt++) {
		let emitted: unknown;
		try {
			emitted = await emit([prompt], messages, opts.signal);
		} catch (err) {
			// A cancelled request (client closed the modal) — stop, don't burn the
			// remaining attempts re-failing fast against an already-aborted signal.
			if (opts.signal?.aborted) return { ok: false, error: "Cancelled." };
			lastError = err instanceof Error ? err.message : String(err);
			continue;
		}
		if (emitted === undefined) continue;

		const validation = validateChartConfig(emitted, columnNames);
		if (validation.ok) return { ok: true, config: validation.config };

		// Feed the failure back so the next attempt corrects it (the breaker).
		lastError = validation.error;
		messages.push(
			{ role: "assistant", content: JSON.stringify(emitted) },
			{
				role: "user",
				content: `That chart config was rejected: ${validation.error} Emit a corrected config.`,
			},
		);
	}

	return {
		ok: false,
		error: `Couldn't author a valid chart after ${CHART_AUTHOR_MAX_ATTEMPTS} attempts (${lastError}). Try rephrasing, or map the columns manually.`,
	};
}
