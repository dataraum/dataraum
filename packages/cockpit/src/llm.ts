// Shared LLM config for the cockpit agent tier (DAT-353).
//
// ONE source for the model id + the output-token ceiling, used by the chat agent
// loop and the structured-output tools (frame induction, why_column synthesis).
//
// maxTokens MUST be set explicitly on every Anthropic call: the
// `@tanstack/ai-anthropic` adapter defaults `max_tokens` to 1024 when it's
// omitted (`defaultMaxTokens = options.maxTokens || 1024`), which silently
// truncates any sizeable turn — a tool-call argument, an induced concept set, a
// grounded synthesis — with `stop_reason: max_tokens`. That manifested as a
// hung "Working…" (frame) and a cut SSE stream (why_column). 24576 is well under
// Sonnet 4.6's output ceiling and generous enough that a normal turn never
// re-truncates; a silent cut-off is worse than a few extra tokens of headroom.

export const MODEL = "claude-sonnet-4-6";

export const MAX_OUTPUT_TOKENS = 24576;
