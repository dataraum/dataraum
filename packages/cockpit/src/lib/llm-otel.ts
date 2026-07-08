// Per-call-site LLM observability (DAT-706, replacing DAT-600's llm_call log
// line) — the configured @tanstack/ai `otelMiddleware` plus a small usage-
// rollup companion, built per chat() invocation via `llmOtel(label)`.
//
// What otelMiddleware gives us as-is: a root span per chat() run, one CLIENT
// span per model iteration (request params + that iteration's own usage), one
// span per tool execution, and the two GenAI client histograms
// (`gen_ai.client.token.usage`, `gen_ai.client.operation.duration`) when a
// meter is provided. `captureContent` stays at its default `false` (PII,
// DAT-554) — no prompt/completion content ever lands on a span.
//
// What the factory adds, all through documented extension points:
//
//  - `label` → `dataraum.call_site` on every span (attributeEnricher). The
//    DAT-599 attribution key: which agent/feature dominates wall-clock. The
//    engine stamps the SAME key on its gen_ai spans (llm/providers/
//    anthropic.py), so call sites are one cross-stack query.
//  - `gen_ai.provider.name` on every span (attributeEnricher). Current
//    semconv renamed the provider discriminator (gen_ai.system → provider.name);
//    @tanstack/ai 0.40 still emits only the retired key. Enriching keeps one
//    queryable key across both stacks. Drop when upstream migrates.
//  - Root-span usage ROLLUP (the onUsage companion + onSpanEnd). The docs
//    promise the root span "rolls up usage across all iterations", but the
//    shipped engine stamps `FinishInfo.usage` = the LAST iteration's usage
//    only (`finishedEvent` is reset per iteration and never summed — verified
//    live against 0.40.0, same defect class as DAT-663). The companion
//    accumulates the per-iteration increments and onSpanEnd — which fires
//    just before rootSpan.end() — overwrites the `gen_ai.usage.*` attrs with
//    true loop totals. Filed upstream as
//    https://github.com/TanStack/ai/issues/916; the canary test in
//    llm-otel.test.ts fails the day the engine starts rolling up, which is
//    the signal to delete the companion.
//
// Telemetry off (getOtel() null) → `[]`: chat() runs with no telemetry
// middleware at all, keeping the off-path byte-identical. The DAT-600
// `llm_call` console line retired with this module (its cockpit half); the
// engine's structlog twin stays until DAT-707 settles log shipping.

import { metrics, trace } from "@opentelemetry/api";
import type { ChatMiddleware } from "@tanstack/ai";
import { otelMiddleware } from "@tanstack/ai/middlewares/otel";

import { getOtel } from "#/otel";

/**
 * Build the telemetry middleware for one chat() run, tagged with the call
 * site's `label` (e.g. "orchestrator", "answer_subagent"). Construct a FRESH
 * instance per chat() invocation — the usage accumulator is private to one
 * run; sharing an instance across runs would cross-count.
 */
export function llmOtel(label: string): ChatMiddleware[] {
	if (getOtel() === null) return [];

	// The run's additive token totals — per-iteration increments (each
	// usage-bearing RUN_FINISHED carries that API call's OWN spend, never a
	// cumulative figure), so summing is double-count-free.
	const totals = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
	const usageRollup: ChatMiddleware = {
		name: "llm-usage-rollup",
		onUsage(_ctx, usage) {
			totals.input += usage.promptTokens;
			totals.output += usage.completionTokens;
			totals.cacheRead += usage.promptTokensDetails?.cachedTokens ?? 0;
			totals.cacheWrite += usage.promptTokensDetails?.cacheWriteTokens ?? 0;
		},
	};

	return [
		usageRollup,
		otelMiddleware({
			tracer: trace.getTracer("dataraum-cockpit"),
			meter: metrics.getMeter("dataraum-cockpit"),
			attributeEnricher: (info) => ({
				"dataraum.call_site": label,
				"gen_ai.provider.name": info.ctx.provider,
			}),
			onSpanEnd: (info, span) => {
				// Root-span usage correction — see the header. Iteration and
				// tool spans are already correct; only the root's at-a-glance
				// totals need the accumulated truth. total_tokens must be
				// overridden too: the middleware stamps it from the SAME
				// last-iteration FinishInfo.usage, so leaving it would ship a
				// self-contradictory span (input+output ≠ total). Derived as
				// input+output — exactly how the Anthropic adapter builds each
				// iteration's totalTokens.
				if (info.kind !== "chat") return;
				span.setAttributes({
					"gen_ai.usage.input_tokens": totals.input,
					"gen_ai.usage.output_tokens": totals.output,
					"gen_ai.usage.total_tokens": totals.input + totals.output,
					"gen_ai.usage.cache_read.input_tokens": totals.cacheRead,
					"gen_ai.usage.cache_creation.input_tokens": totals.cacheWrite,
				});
			},
		}),
	];
}
