// Shared LLM config for the cockpit agent tier (DAT-353).
//
// ONE source for the model ids + the two output-token ceilings: streaming
// chat (the agent loops, the forced-tool streams) takes MAX_OUTPUT_TOKENS;
// every `chat({ outputSchema })` call (why_* synthesis, nav classifier,
// report summary, grounding verdict) takes STRUCTURED_OUTPUT_MAX_TOKENS —
// the SDK hard-caps the non-streaming path those can route to (DAT-700).
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
// combined set (claude-haiku-4-5 today) get ONE streaming request with
// `output_config`; everything else — claude-sonnet-5 included — takes the
// legacy path: a NON-streaming `messages.create` with `tool_choice` forced to
// a `structured_output` tool. The Anthropic SDK refuses non-streaming
// requests whose max_tokens implies >10 minutes at its assumed 128k
// tokens/hour — over 21,333 it throws `Streaming is required …` client-side,
// before anything is sent. MAX_OUTPUT_TOKENS (24576) crossed that line and
// broke the four sonnet-5 outputSchema sites (why_* synthesis ×3, grounding
// verdict); the haiku sites (nav classifier, report summary) streamed and
// never hit it. ALL outputSchema sites use this budget anyway — the routing
// set is a floating-dep implementation detail, and every such call emits a
// small payload (a few-paragraph narrative at most). With thinking disabled
// on the sonnet-5 one-shot emits, 8192 is ~4× headroom while staying far
// under the gate. Pinned against the REAL chat()+adapter+nested-SDK path in
// llm.contract.test.ts; tools+outputSchema chats re-budget only their
// finalization via structuredOutputBudgetMiddleware (grounding-agent).
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
