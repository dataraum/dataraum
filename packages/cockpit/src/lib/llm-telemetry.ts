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
// Token accounting (DAT-663): each token field is the SUM over the run's model
// iterations, accumulated live via `onUsage` — the SDK fires it once per model
// iteration whose RUN_FINISHED chunk carries usage, and that usage is the single
// API call's own spend (INCREMENTAL per iteration, never cumulative: the adapter
// builds it from one response's final message_delta, and the chat engine never
// sums across iterations). Summing here is therefore double-count-free and gives
// the true loop total. We deliberately do NOT read `FinishInfo.usage`: it is only
// the LAST iteration's RUN_FINISHED usage (the engine overwrites per iteration),
// so it under-reports any multi-iteration run — and every usage-bearing
// RUN_FINISHED that could feed it also fires `onUsage` first, so the accumulator
// is always a superset.
//
// The SDK guarantees exactly one terminal hook per run (onFinish/onAbort/onError),
// so we log on all three with a `status`, and ALL THREE carry the accumulated
// totals — a finished, aborted, and errored row mean the same thing per field.
// The cockpit's deliberate drain-abort call sites (the `answer` sub-agent, frame
// induction ×4 per frame(), the chart author) abort only AFTER the forced tool's
// iteration reported usage, so their aborted rows carry the real spend up to the
// abort — no longer zeros (DAT-663). Error rows likewise carry everything
// accumulated before the failure. The one systematic loss: an iteration aborted
// or errored MID-GENERATION contributes nothing (its usage would only arrive with
// that response's final message_delta), so such rows under-count by at most the
// one in-flight call. Aggregations can sum over every status without filtering.
//
// Granularity note: the engine logs one line per model call; here one line is a
// whole chat() run — the accumulated totals span the loop's iterations
// (orchestrator ≤20, sub-agent ≤10). That is the right grain for "which agent
// dominates wall-clock"; `iterations` exposes the per-run round-trip depth.

import type { ChatMiddleware } from "@tanstack/ai";

type CallStatus = "finished" | "aborted" | "error";

/** The run's additive token totals, already in the engine's snake_case shape. */
interface UsageTotals {
	input_tokens: number;
	output_tokens: number;
	cache_read_input_tokens: number;
	cache_creation_input_tokens: number;
}

function emitLlmCall(
	label: string,
	model: string,
	status: CallStatus,
	durationMs: number,
	iterations: number,
	totals: UsageTotals,
): void {
	console.info("llm_call", {
		label,
		model,
		status,
		elapsed_ms: Math.round(durationMs),
		...totals,
		iterations,
	});
}

/**
 * Build a logging middleware that emits a single `llm_call` telemetry line when a
 * chat() run reaches a terminal hook. `label` tags the call site (e.g.
 * "orchestrator" / "answer_subagent"). Observe-only — it never transforms config.
 *
 * A fresh instance is constructed per chat() invocation (every call site builds it
 * inline), so the `iterations` counter and the usage accumulator are private to
 * that one run — no cross-run or concurrent-call races.
 */
export function llmTelemetryMiddleware(label: string): ChatMiddleware {
	let iterations = 0;
	const totals: UsageTotals = {
		input_tokens: 0,
		output_tokens: 0,
		cache_read_input_tokens: 0,
		cache_creation_input_tokens: 0,
	};
	return {
		name: "llm-telemetry",
		onIteration() {
			iterations += 1;
		},
		// Fires once per model iteration that reports usage, with that iteration's
		// OWN spend (incremental — see the header). Accumulate; the terminal hook
		// emits the totals.
		onUsage(_ctx, usage) {
			totals.input_tokens += usage.promptTokens;
			totals.output_tokens += usage.completionTokens;
			totals.cache_read_input_tokens +=
				usage.promptTokensDetails?.cachedTokens ?? 0;
			totals.cache_creation_input_tokens +=
				usage.promptTokensDetails?.cacheWriteTokens ?? 0;
		},
		onFinish(ctx, info) {
			emitLlmCall(
				label,
				ctx.model,
				"finished",
				info.duration,
				iterations,
				totals,
			);
		},
		onAbort(ctx, info) {
			emitLlmCall(
				label,
				ctx.model,
				"aborted",
				info.duration,
				iterations,
				totals,
			);
		},
		onError(ctx, info) {
			emitLlmCall(label, ctx.model, "error", info.duration, iterations, totals);
		},
	};
}
