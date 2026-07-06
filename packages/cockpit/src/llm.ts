// Shared LLM config for the cockpit agent tier (DAT-353).
//
// ONE source for the model ids + the two output-token ceilings: streaming
// chat (the agent loops, the forced-tool streams) takes MAX_OUTPUT_TOKENS;
// pure `chat({ outputSchema })` calls (why_* synthesis, nav classifier,
// report summary) take STRUCTURED_OUTPUT_MAX_TOKENS — sized under the SDK's
// non-streaming gate so adapter routing can't break them (DAT-700).
//
// max_tokens MUST be set explicitly — via `modelOptions: { max_tokens }` — on
// every Anthropic call: the `@tanstack/ai-anthropic` adapter defaults it to
// 1024 when omitted (`defaultMaxTokens = modelOptions?.max_tokens ?? 1024`),
// which silently truncates any sizeable turn — a tool-call argument, an induced
// concept set, a grounded synthesis — with `stop_reason: max_tokens`. That
// manifested as a hung "Working…" (frame) and a cut SSE stream (why_column).
// NOTE: modelOptions is the ONLY channel — chat()'s options have no top-level
// maxTokens field, and a stray one type-checks through inferred returns while
// doing nothing (the DAT-436 1024-token orchestrator bug). 24576 is well under
// Sonnet 5's output ceiling and generous enough that a normal turn never
// re-truncates; a silent cut-off is worse than a few extra tokens of headroom.

export const MODEL = "claude-sonnet-5";

// The landing nav-agent's model (DAT-534) — a cheap Haiku one-shot that classifies
// the opening message into a chat kind. Undated alias, mirroring MODEL's
// `claude-sonnet-5` (auto-resolves to the latest snapshot); fall back to the
// dated `claude-haiku-4-5-20251001` only if the alias ever fails to resolve.
export const NAV_MODEL = "claude-haiku-4-5";

// The report-summary regenerator's model (DAT-625) — a cheap Haiku one-shot that
// rewrites a stale report summary against the fresh result, in the original voice.
// Same undated Haiku alias as NAV_MODEL; kept as its own constant so the model
// choice for this surface is explicit and tunable independently.
export const SUMMARY_MODEL = "claude-haiku-4-5";

// STREAMING calls only — every non-streaming call must use
// STRUCTURED_OUTPUT_MAX_TOKENS below or the SDK throws before sending.
export const MAX_OUTPUT_TOKENS = 24576;

// The ceiling for `chat({ outputSchema })` calls (DAT-700). How those route
// is adapter-internal and MODEL-keyed: models in `@tanstack/ai-anthropic`'s
// combined set — all of ours since adapter 0.16.1 — get ONE streaming request
// with the schema attached (`output_config`); a model OUTSIDE the set falls
// to a legacy NON-streaming forced-tool `messages.create`, which the
// Anthropic SDK refuses client-side above 21,333 max_tokens (`Streaming is
// required …`, thrown before anything is sent). That set lags model releases
// (claude-sonnet-5 was outside it on adapter 0.15.x — the DAT-700 outage,
// MAX_OUTPUT_TOKENS 24576 over the gate), so the budget rule is: every
// outputSchema site uses this constant, sized under the gate — a new model
// id landing outside the set degrades to the slower transport instead of
// breaking. 8192 is ample for these payloads (a few-paragraph narrative at
// most, thinking disabled on the one-shot emits). Set membership for our
// model ids is pinned in llm.contract.test.ts.
export const STRUCTURED_OUTPUT_MAX_TOKENS = 8192;

// The agent-loop iteration ceiling for /api/chat. chat() defaults to
// maxIterations(5) when no agentLoopStrategy is given — a SILENT governor from
// the same defect class as the 1024-token default above: a 17-tool orchestrator
// turn (poll → poll → look → why → narrate) just STOPS mid-task at iteration 5,
// no error, no signal (DAT-449). 20 follows the @tanstack/ai tool-calling
// skill's multi-tool example; it is a runaway-loop ceiling, not an expected
// budget — normal turns finish well under it, and a turn that genuinely needs
// more is a prompt/tool-design problem, not a reason to raise this.
export const AGENT_LOOP_MAX_ITERATIONS = 20;

// The iteration ceiling for the `answer` query sub-agent's OWN nested chat()
// loop (DAT-485) — the same silent-maxIterations(5) governor as the
// orchestrator above, just scoped to a narrower agent: search the snippet KB →
// (maybe re-search) → validate the composed SQL via run_steps → (maybe repair) →
// emit the structured answer. ~10 leaves headroom for a search + a validate + a
// repair round without stalling mid-task, and stays a runaway ceiling (a normal
// answer finishes in 2-4 iterations).
export const QUERY_SUBAGENT_MAX_ITERATIONS = 10;
