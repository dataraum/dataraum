// Per-call LLM telemetry (DAT-600, cockpit half of epic DAT-599).
//
// A @tanstack/ai chat() middleware that emits ONE structured `llm_call` line per
// turn — the cockpit mirror of the engine's provider-chokepoint log. Attach it to
// each chat() with a distinct `label` so the orchestrator turn and the nested
// `answer` sub-agent loop are attributable SEPARATELY (the epic suspects that
// sub-agent loop is the latency multiplier — DAT-605 reads `iterations` from here).
//
// SERVER-ONLY: chat() runs server-side, so `console.info` lands on the cockpit
// server process stdout (Nitro/Bun) — `docker compose logs cockpit` in a
// container, the dev-server terminal under `bun --bun run dev`.
//
// The key names are deliberately snake_case to match the engine's `llm_call`
// shape, so `grep llm_call | jq` aggregates identically across both stacks even
// though @tanstack/ai's TokenUsage is camelCase. The mapping:
//   promptTokens                      -> input_tokens
//   completionTokens                  -> output_tokens
//   promptTokensDetails.cachedTokens  -> cache_read_input_tokens
//   promptTokensDetails.cacheWriteTokens -> cache_creation_input_tokens
// `elapsed_ms` is the whole-run duration the SDK already measures (FinishInfo).
//
// Granularity note: the engine logs one line per model call; here one line is a
// whole chat() turn — the RUN_FINISHED usage is the loop total across iterations
// (orchestrator ≤20, sub-agent ≤10). That is the right grain for "which agent
// dominates wall-clock"; `iterations` exposes the per-turn round-trip depth.

import type { ChatMiddleware } from "@tanstack/ai";

/**
 * Build a logging middleware that emits a single `llm_call` telemetry line when a
 * chat() turn finishes. `label` tags the call site (e.g. "orchestrator" /
 * "answer_subagent"). Observe-only — it never transforms config; an aborted or
 * errored turn emits nothing (parity with the engine, which logs success only).
 *
 * A fresh instance is constructed per chat() invocation (both call sites build it
 * inline), so the `iterations` counter is private to that one turn — no cross-turn
 * or concurrent-call races.
 */
export function llmTelemetryMiddleware(label: string): ChatMiddleware {
	let iterations = 0;
	return {
		name: "llm-telemetry",
		onIteration() {
			iterations += 1;
		},
		onFinish(ctx, info) {
			const usage = info.usage;
			const promptDetails = usage?.promptTokensDetails;
			console.info("llm_call", {
				label,
				model: ctx.model,
				elapsed_ms: Math.round(info.duration),
				input_tokens: usage?.promptTokens ?? 0,
				output_tokens: usage?.completionTokens ?? 0,
				cache_read_input_tokens: promptDetails?.cachedTokens ?? 0,
				cache_creation_input_tokens: promptDetails?.cacheWriteTokens ?? 0,
				iterations,
			});
		},
	};
}
