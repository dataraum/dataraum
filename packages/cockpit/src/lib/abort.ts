// Abort bridging for nested LLM calls (DAT-449).
//
// chat()'s agentic loop hands every server tool a `ToolExecutionContext` whose
// `abortSignal` fires when the run aborts (user stop(), client disconnect).
// A tool that makes its OWN nested chat() synthesis call must forward that
// signal, or the nested call — and its Anthropic bill — runs to completion
// after the user already stopped the turn. chat() takes an `AbortController`
// (it only ever reads `.signal`), so the bridge wraps the inbound signal in a
// fresh, linked controller.

/**
 * Wrap an inbound AbortSignal in a linked AbortController: aborting the signal
 * aborts the controller (reason propagated), including a signal that already
 * aborted before the call. `undefined` in → `undefined` out, so call sites stay
 * a one-liner (`abortController: linkedAbortController(signal)`) and an
 * absent signal changes nothing.
 */
export function linkedAbortController(
	signal?: AbortSignal,
): AbortController | undefined {
	if (!signal) return undefined;
	const controller = new AbortController();
	if (signal.aborted) {
		controller.abort(signal.reason);
	} else {
		signal.addEventListener("abort", () => controller.abort(signal.reason), {
			once: true,
		});
	}
	return controller;
}
