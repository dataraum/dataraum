// Per-call LLM telemetry (DAT-600, cockpit half of epic DAT-599).
//
// A @tanstack/ai chat() middleware that emits ONE structured `llm_call` line per
// chat() run — the cockpit mirror of the engine's provider-chokepoint log. Attach
// it to every chat() with a distinct `label` so each call site (orchestrator turn,
// the nested `answer` sub-agent loop, the why-* narratives, frame induction, nav
// classifier, grounding agent) is attributable separately (the epic suspects the
// sub-agent loop is the latency multiplier — DAT-605 reads `iterations` from here).
//
// SERVER-ONLY: chat() runs server-side, so `console.info` lands on the cockpit
// server process stdout (Nitro/Bun) — `docker compose logs cockpit` in a
// container, the dev-server terminal under `bun --bun run dev`. The grounding
// agent runs in the co-located TS worker, so its lines land on that process.
//
// The key names are deliberately snake_case to match the engine's `llm_call`
// shape, so `grep llm_call | jq` aggregates identically across both stacks even
// though @tanstack/ai's TokenUsage is camelCase. The mapping:
//   promptTokens                          -> input_tokens
//   completionTokens                      -> output_tokens
//   promptTokensDetails.cachedTokens      -> cache_read_input_tokens
//   promptTokensDetails.cacheWriteTokens  -> cache_creation_input_tokens
// `elapsed_ms` is the run duration the SDK already measures.
//
// The SDK guarantees exactly one terminal hook per run (onFinish/onAbort/onError),
// so we log on all three with a `status` — unlike the engine (success-only), the
// cockpit has deliberate early-abort paths (frame induction aborts after the
// forced tool fires, ×4/frame), and an aborted/errored turn is still latency data.
// Tokens are only known on a clean finish; abort/error lines carry zeros.
//
// Granularity note: the engine logs one line per model call; here one line is a
// whole chat() run — the RUN_FINISHED usage is the loop total across iterations
// (orchestrator ≤20, sub-agent ≤10). That is the right grain for "which agent
// dominates wall-clock"; `iterations` exposes the per-run round-trip depth.

import type { ChatMiddleware, TokenUsage } from "@tanstack/ai";

type CallStatus = "finished" | "aborted" | "error";

function emitLlmCall(
	label: string,
	model: string,
	status: CallStatus,
	durationMs: number,
	iterations: number,
	usage?: TokenUsage,
): void {
	const promptDetails = usage?.promptTokensDetails;
	console.info("llm_call", {
		label,
		model,
		status,
		elapsed_ms: Math.round(durationMs),
		input_tokens: usage?.promptTokens ?? 0,
		output_tokens: usage?.completionTokens ?? 0,
		cache_read_input_tokens: promptDetails?.cachedTokens ?? 0,
		cache_creation_input_tokens: promptDetails?.cacheWriteTokens ?? 0,
		iterations,
	});
}

/**
 * Build a logging middleware that emits a single `llm_call` telemetry line when a
 * chat() run reaches a terminal hook. `label` tags the call site (e.g.
 * "orchestrator" / "answer_subagent"). Observe-only — it never transforms config.
 *
 * A fresh instance is constructed per chat() invocation (every call site builds it
 * inline), so the `iterations` counter is private to that one run — no cross-run
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
			emitLlmCall(
				label,
				ctx.model,
				"finished",
				info.duration,
				iterations,
				info.usage,
			);
		},
		onAbort(ctx, info) {
			emitLlmCall(label, ctx.model, "aborted", info.duration, iterations);
		},
		onError(ctx, info) {
			emitLlmCall(label, ctx.model, "error", info.duration, iterations);
		},
	};
}
