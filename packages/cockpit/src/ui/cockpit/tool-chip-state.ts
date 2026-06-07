// Tool-call chip state (DAT-436) — the terminal-state mapping the chat rail's
// chips render from. Pure, no React.
//
// SDK CONTRACT (two halves, DAT-449):
//   - The tool-CALL part behavior — an errored execution parking at
//     "input-complete" with the error riding in `output` — is an UNDOCUMENTED
//     internal of @tanstack/ai (no error-terminal ToolCallState exists).
//   - The tool-RESULT part shape (`state: "error"` + `error?: string`) MATCHES
//     the PUBLIC `ToolResultPart` export of @tanstack/ai-client — a documented
//     contract, not an internal. (`MessageLike` below still reads it through a
//     loose structural type: the rail iterates heterogeneous parts, so only
//     the contract test — not tsc — ties the field names to the export.)
// Deps stay "latest" by project convention — bun.lock owns the installed
// version, which only moves on an explicit `bun update`. BOTH halves are
// pinned empirically by tool-chip-state.contract.test.ts, which drives the
// real chat() → SSE → ChatClient pipeline on every suite run — an update that
// changes either fails the suite loudly; re-verify this mapping then.
//
// DAT-452 — the AG-UI event layer was explored as a replacement and REJECTED:
// the raw events (TOOL_CALL_RESULT, RUN_ERROR — consumable via the client's
// `onChunk`/`onCustomEvent` hooks) are exactly what the SDK's StreamProcessor
// already projects into the parts this module reads, so consuming them
// directly adds NO authority — it only trades render-derived state for an
// event-driven parallel store (against the derive-during-render convention).
// The root gap is UPSTREAM: ToolCallState has no error terminal, so the same
// inference is required at either layer. Until that lands upstream, this
// mapping + the contract test are the floor.
//
// WHY THIS EXISTS — the SDK's tool-call part state machine has NO error-terminal
// state (verified against the installed @tanstack/ai — bun.lock owns the
// version — by driving the real server chat() loop + client StreamProcessor
// end-to-end):
//
//   - An ERRORED tool execution comes back as `state: "input-complete"` with
//     the error riding in `output` and a sibling `tool-result` part
//     `state: "error"` (processor.js handleToolCallEndEvent/
//     handleToolCallResultEvent map `output-error` → "input-complete";
//     message-updaters.js updateToolCallWithOutput defaults errored calls to
//     "input-complete"). `state === "complete"` therefore NEVER matches — the
//     old done-condition spun the Loader forever (the live stuck
//     workflow_status chips). The output carries one of TWO error shapes —
//     see `outputError`.
//   - The client resolves the turn at the FIRST per-iteration RUN_FINISHED (the
//     Anthropic adapter emits one per model call) and back-fills later results
//     via a background drain. Anything that severs that drain — stop(), a new
//     send, a network cut, RUN_ERROR/max_tokens — permanently parks the pending
//     parts at "input-complete" with NO output. Those can never complete; they
//     are provably dead once the conversation moved on (a later user message
//     exists) OR the stream went idle (isLoading false — isLoading spans the
//     ENTIRE drain, so idle + no output means no result is ever coming; a user
//     stop() with no follow-up message is exactly this cell).
//
// So "done" is NOT `state === "complete"`: a chip is terminal when the part
// reached "complete", when it carries ANY output (success or error), when its
// approval was denied, or when the conversation moved past it / the stream went
// idle without delivering its result. The mapping below recognizes all of them;
// the rail renders `error` as an explicit red state — an errored tool call must
// never spin forever.

/** The untyped tool-call part shape the rail narrows off `type === "tool-call"`
 * (tools register server-side, so useChat sees them untyped). `arguments` is
 * the SDK's JSON-encoded call input. */
export interface ToolCallPartLike {
	type: "tool-call";
	id: string;
	name: string;
	state: string;
	approval?: { id: string; needsApproval: boolean; approved?: boolean };
	arguments?: unknown;
	output?: unknown;
}

/** What a chip renders: spinner, terminal success, denied approval, or an
 * explicit error state (with the message for the tooltip/details). */
export type ToolChipStatus =
	| { kind: "running" }
	| { kind: "complete" }
	| { kind: "denied" }
	| { kind: "error"; message: string };

/** The error-string prefix of the SDK's PLAIN-STRING errored-output shape
 * (tool-calls.js ToolCallManager.executeTools:
 * `toolResultContent = \`Error executing tool: ${message}\``). */
const SERVER_TOOL_ERROR_PREFIX = "Error executing tool:";

/**
 * The error carried in an errored call's `output`, in EITHER of the SDK's two
 * shapes — or null when the output isn't error-shaped:
 *
 *   - `{ error: string }` — what the installed SDK's live execution path
 *     produces for server tools (executeServerTool pushes
 *     `result: { error: message }`; the wire JSON round-trips it back to an
 *     object on the client). Also the client-tool shape
 *     (updateToolCallWithOutput: `output = {error}`).
 *   - `"Error executing tool: <msg>"` — the SDK's PLAIN-STRING shape
 *     (ToolCallManager.executeTools, tool-calls.js): the client's JSON.parse
 *     of that string fails, so `output` stays the raw string. Dead in the
 *     installed SDK's chat() loop but still in the SDK source — recognized so
 *     a bump that rewires it (or a stream that delivered it) renders "failed",
 *     not an eternal spinner or a fake success.
 *
 * The empirical contract test (tool-chip-state.contract.test.ts) pins which
 * shape the pinned SDK actually produces.
 */
function outputError(output: unknown): string | null {
	if (
		typeof output === "string" &&
		output.startsWith(SERVER_TOOL_ERROR_PREFIX)
	) {
		return output;
	}
	if (
		output !== null &&
		typeof output === "object" &&
		"error" in output &&
		typeof (output as { error: unknown }).error === "string"
	) {
		return (output as { error: string }).error;
	}
	return null;
}

/**
 * Map one tool-call part to its chip status.
 *
 * `resultError` — the correlated `tool-result` part's error, when one exists
 * with `state: "error"` (see `toolResultErrorsById`). `conversationMovedOn` —
 * a LATER user message exists, so an output-less part can never receive its
 * result (the stream that owned it is gone; the SDK never re-attaches).
 * `streamIdle` — the chat stream is not loading (`!isLoading`); isLoading
 * spans the ENTIRE drain (sendMessage no-ops while loading), so an output-less
 * part with the stream idle is equally dead — this is the stop-then-idle cell,
 * where the user hit stop() and never sent another message.
 *
 * Precedence: denied → error (result error / error-shaped output) → complete
 * (the "complete" state OR any output — terminal even if a stream hiccup never
 * flipped the state) → interrupted-orphan error → running.
 */
export function toolChipStatus(
	part: ToolCallPartLike,
	opts: {
		resultError?: string;
		conversationMovedOn?: boolean;
		streamIdle?: boolean;
	} = {},
): ToolChipStatus {
	// A denied approval is terminal: the tool never runs, so the call never
	// completes — without this the chip would spin forever.
	if (part.approval?.approved === false) return { kind: "denied" };

	// Errored execution: the SDK parks the part at "input-complete" and carries
	// the error in the output / the correlated tool-result part — there is no
	// error STATE to test. Check before "complete" so an error-shaped output is
	// never read as success.
	const err = opts.resultError ?? outputError(part.output);
	if (err !== null) {
		return { kind: "error", message: err };
	}

	// Success: the canonical terminal state, or any output at all (defensive —
	// output without the state flip is still a delivered result).
	if (part.state === "complete" || part.output !== undefined) {
		return { kind: "complete" };
	}

	// Orphaned: no output, and either the conversation moved past the turn that
	// owned this call (stop() + a new send / a severed stream) or the stream
	// went idle without delivering it (stop() with NO further activity — no
	// false-failure window: isLoading covers the whole back-fill drain, so idle
	// means no result is ever coming). It can never finish.
	// EXCEPT a pending approval request — its Approve/Deny buttons stay live
	// across turns (and the stream is idle BY DESIGN while the SDK awaits the
	// user), so it is awaiting the user, not dead. "approval-responded" is
	// deliberately NOT carved out: a denied response short-circuited to
	// `denied` above, and an approved-but-severed call (approval given, drain
	// cut before the result landed) is exactly the orphan this catches.
	if (
		(opts.conversationMovedOn || opts.streamIdle) &&
		part.state !== "approval-requested"
	) {
		return {
			kind: "error",
			message: "The call didn't finish — its run was interrupted.",
		};
	}

	return { kind: "running" };
}

/** Message-list shape the helpers below need — structurally compatible with
 * the SDK's UIMessage (only `role` + `parts` are read). */
export interface MessageLike {
	role: string;
	parts: ReadonlyArray<{
		type: string;
		toolCallId?: string;
		state?: string;
		error?: string;
		content?: unknown;
	}>;
}

/**
 * Collect every errored `tool-result` part's error text by toolCallId. The SDK
 * emits these alongside the (state-less) error on the tool-call part itself;
 * the rail prefers this text when present (it survives even when the call
 * part's output was clobbered). The fields read here match the PUBLIC
 * `ToolResultPart` export of @tanstack/ai-client (a documented contract; the
 * tie is pinned by the contract test, not tsc — see the header).
 */
export function toolResultErrorsById(
	messages: ReadonlyArray<MessageLike>,
): Map<string, string> {
	const errors = new Map<string, string>();
	for (const message of messages) {
		for (const part of message.parts) {
			if (
				part.type === "tool-result" &&
				part.state === "error" &&
				typeof part.toolCallId === "string"
			) {
				errors.set(
					part.toolCallId,
					part.error ??
						(typeof part.content === "string"
							? part.content
							: "Tool execution failed"),
				);
			}
		}
	}
	return errors;
}

/**
 * The index of the LAST user message — any tool-call part rendered from an
 * earlier message belongs to a turn the conversation has moved past
 * (`conversationMovedOn` in `toolChipStatus`). Approval round-trips do NOT
 * add a user message, so a pending approval/continuation is never orphaned
 * by this; a genuinely new user turn is.
 */
export function lastUserMessageIndex(
	messages: ReadonlyArray<MessageLike>,
): number {
	for (let i = messages.length - 1; i >= 0; i--) {
		if (messages[i].role === "user") return i;
	}
	return -1;
}
