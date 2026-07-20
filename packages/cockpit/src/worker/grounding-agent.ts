// Grounding-teach agent (DAT-551 P3c) — the LLM activity the grounding-loop workflow runs after an
// add_source completes. It reads the run's readiness oracle and, via a nested
// chat(), auto-applies ONLY mechanical grounding teaches (type_pattern / null_value
// / unit) for the gaps a detector can verify, reporting whether a human-judgement
// gap remains.
//
// MAIN-THREAD activity (NOT workflow-sandboxed): it does IO (metadata read +
// config_overlay writes via `teach`) and a real LLM call. This is the FIRST chat()
// from the worker process — the cockpit's request-scoped tools (answer/why) are the
// pattern, and `config.anthropicApiKey` is available here too. Non-deterministic →
// correctly an ACTIVITY; the grounding-loop workflow's deterministic loop drives the replays around
// it (it never replays itself).
//
// The gate is authoritative + structural: the agent is handed ONLY the constrained
// `ground_teach` tool, whose input enum is AGENT_AUTOAPPLY_TEACH_TYPES and whose
// payload is AutoApplyTeachPayloadSchema — so the model literally cannot express a
// judgement-family teach (concept/relationship/hierarchy/validation). A non-mechanical
// gap is REPORTED (needs_judgement), never auto-applied.

import { chat, maxIterations, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";
import { config } from "#/config";
import {
	type GroundingReadinessRow,
	readGroundingReadiness,
} from "#/db/metadata/grounding-readiness";
import { llmOtel } from "#/lib/llm-otel";
import { toolArgsGuardMiddleware } from "#/lib/tool-args-guard";
import { MAX_OUTPUT_TOKENS, MODEL } from "#/llm";
import { teach } from "#/tools/teach";
import {
	AGENT_AUTOAPPLY_TEACH_TYPES,
	AutoApplyTeachPayloadSchema,
	type AutoApplyTeachType,
} from "#/tools/teach.validation";

// Runaway ceiling for the grounding agent's own tool loop (read gaps → apply N
// teaches → emit verdict). A normal round applies a handful of teaches in 2–4
// iterations; this is a backstop, not a budget (mirrors QUERY_SUBAGENT_MAX_ITERATIONS).
const GROUNDING_LOOP_MAX_ITERATIONS = 12;

const GROUNDING_INSTRUCTIONS = `You are the grounding-teach agent. After a data import, you fix MECHANICAL grounding gaps so the data types cleanly — nothing more.

You are given the readiness of each column (band + worst-intent risk + the top drivers behind the risk). For each NON-ready target, decide if it is a MECHANICAL grounding gap you can fix, and if so apply ONE teach via the ground_teach tool:
- driver about TYPE / type_fidelity (e.g. a date or number read as text) → type_pattern: a regex matching the values + the inferred_type.
- driver about NULLS / null_semantics (an unrecognized null token like "N/A", "-", "TBD") → null_value: the token + its category.
- driver about UNITS / unit_entropy (a measure whose VALUE-CARRIED unit — a unit token in the values, e.g. "€100", "5kg" — is ambiguous or low-confidence) → unit: {table, column, unit}, identifying the column by NAME (parse it from the target "column:<table>.<col>").

You MUST NOT attempt anything else. Concept meaning (business_meaning), relationships (join_path_determinism), hierarchies, validations, and a measure's unit SOURCE (unit_source — whose unit is defined by a sibling dimension column like currency, not carried in its own values) are HUMAN JUDGEMENT — you cannot teach them and the tool will not let you. (unit_source is a catalogue-grain signal and does not appear in this add_source loop; the concept vocabulary it depends on is declared in the frame stage, not here.) If a remaining gap is one of those, do NOT apply a teach for it; instead set needs_judgement=true and describe it briefly in judgement_note for the human.

Do NOT replay or re-measure — the system re-runs the import and re-measures after your teaches; you only apply teaches this round. When done, emit your verdict: needs_judgement (is there a non-mechanical gap a human must address?) and judgement_note (one sentence naming it, or an empty string when there is none).`;

/** The agent's structured verdict (the tool-applied teaches are counted separately
 * via the capture cell, not self-reported). */
export const VerdictSchema = z.object({
	needs_judgement: z
		.boolean()
		.describe(
			"True if a NON-mechanical gap remains that a human must address (a concept/relationship/hierarchy/validation), false otherwise.",
		),
	// A plain required string, NOT `.nullable()`: `needs_judgement` above is
	// already the discriminator, so a null branch adds a union — which spends from
	// the 16-union-per-request constrained-decoding budget to express something
	// the boolean already says (DAT-807). "" is the documented "no gap" value;
	// `assessAndGround` maps it back to the null the Temporal contract carries.
	judgement_note: z
		.string()
		.describe(
			"One sentence naming the human-judgement gap (the target + what's needed), or an empty string when there is none.",
		),
});

export interface AssessAndGroundInput {
	/** The run's typed table ids — the readiness scope to assess + ground. */
	tableIds: string[];
	/** How many grounding attempts remain (for the agent's context; the grounding-loop workflow
	 * owns the actual loop bound). */
	attemptsRemaining: number;
}

export interface AssessAndGroundResult {
	/** Mechanical grounding teaches applied this round (captured from the tool, not
	 * self-reported) — the grounding-loop workflow replays iff this is > 0 and attempts remain. */
	appliedCount: number;
	/** A non-mechanical gap remains → the grounding-loop workflow surfaces it (awaiting_input). */
	needsJudgement: boolean;
	/** What to tell the human, when needsJudgement. */
	judgementNote: string | null;
}

/** Build the user message: the non-ready targets with their drivers, for the agent
 * to ground. */
function buildReadinessMessage(
	gaps: GroundingReadinessRow[],
	attemptsRemaining: number,
): string {
	const lines = gaps.map((g) => {
		const drivers = JSON.stringify(g.topDrivers ?? []);
		return `- ${g.target} — band=${g.band}, worst_intent_risk=${g.worstIntentRisk.toFixed(2)}, top_drivers=${drivers}`;
	});
	return `<grounding_attempts_remaining>${attemptsRemaining}</grounding_attempts_remaining>\n<non_ready_targets>\n${lines.join("\n")}\n</non_ready_targets>`;
}

/** The constrained grounding-teach tool — the authoritative gate. Its input can
 * ONLY name an auto-apply type; the write goes through the same `teach` primitive,
 * and each success bumps the capture cell so the grounding-loop workflow gets a real applied-count. */
function makeGroundTeachTool(captured: { count: number }) {
	return toolDefinition({
		name: "ground_teach",
		description:
			"Apply ONE mechanical grounding teach — type_pattern, null_value, or unit. " +
			"Writes a config_overlay row the next import re-run applies. Use only for " +
			"a gap a detector can verify; never for concept/relationship/hierarchy/validation.",
		inputSchema: z.object({
			type: z.enum(AGENT_AUTOAPPLY_TEACH_TYPES),
			payload: AutoApplyTeachPayloadSchema,
		}),
		outputSchema: z.union([
			z.object({ overlay_id: z.string(), type: z.string() }),
			z.object({ error: z.string() }),
		]),
	}).server(async (input) => {
		try {
			const res = await teach({
				type: input.type as AutoApplyTeachType,
				payload: input.payload,
			});
			captured.count += 1;
			return res;
		} catch (err) {
			// Surface a validation error as data so the agent can retry, not crash.
			return { error: err instanceof Error ? err.message : String(err) };
		}
	});
}

/**
 * Assess the run's grounding readiness and auto-apply the mechanical teaches a
 * detector can verify. Returns the applied count + whether a human-judgement gap
 * remains. Fast-paths to a no-op (no LLM call) when every target is already ready.
 */
export async function assessAndGround(
	input: AssessAndGroundInput,
): Promise<AssessAndGroundResult> {
	const readiness = await readGroundingReadiness(input.tableIds);
	const gaps = readiness.filter((r) => r.band !== "ready");
	// Fast path: nothing to ground → no LLM call, the loop exits clean.
	if (gaps.length === 0) {
		return { appliedCount: 0, needsJudgement: false, judgementNote: null };
	}
	// No key → can't run the agent. Don't crash the workflow; surface for a human
	// (mirrors the api-key-required contract — the engine treats the key as hard,
	// but the workflow must stay alive).
	if (!config.anthropicApiKey) {
		return {
			appliedCount: 0,
			needsJudgement: true,
			judgementNote:
				"Grounding agent unavailable (no ANTHROPIC_API_KEY) — review readiness manually.",
		};
	}

	const captured = { count: 0 };
	const verdict = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		middleware: [...llmOtel("grounding"), toolArgsGuardMiddleware("grounding")],
		// The verdict rides the streaming tool loop itself (combined
		// tools+outputSchema request — see llm.ts), so the loop's full budget
		// applies; there is no separate structured-output call to re-budget.
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		agentLoopStrategy: maxIterations(GROUNDING_LOOP_MAX_ITERATIONS),
		systemPrompts: [GROUNDING_INSTRUCTIONS],
		messages: [
			{
				role: "user",
				content: buildReadinessMessage(gaps, input.attemptsRemaining),
			},
		],
		tools: [makeGroundTeachTool(captured)],
		outputSchema: VerdictSchema,
	});

	return {
		appliedCount: captured.count,
		needsJudgement: verdict.needs_judgement,
		// The activity's result is a cross-package Temporal contract (mirrored in
		// the engine's `worker/contracts.py`) and keeps `string | null`; the empty
		// string is a schema sentinel, not a value to hand across the boundary.
		judgementNote: verdict.judgement_note || null,
	};
}
