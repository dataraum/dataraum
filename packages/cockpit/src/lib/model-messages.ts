// buildModelMessages — the single chokepoint between the persisted transcript
// and what the model actually sees (DAT-462). The DISPLAY transcript (full, in
// cockpit_db) is never windowed; only the MODEL view is bounded here, so a long
// conversation doesn't re-send its whole history to Anthropic every turn (the
// expensive leg; `run_sql`/`look` results are the token hogs — see
// project_run_sql_context_overflow).
//
// v1 policy is purely structural: keep the last N user-turns verbatim; for older
// messages, STUB the heavy tool payloads while preserving conversational text.
// The rolling-summary rollup that collapses the evicted turns is DAT-464; it
// lands behind this same function with no caller change.
//
// CRITICAL invariant: tool parts are stubbed, NEVER dropped. Anthropic rejects a
// `tool-call` without a matching `tool-result` (and vice versa), so removing one
// would break the request. We keep every part and its call/result pairing — only
// the payload bytes are replaced. Model-only refs rows (role "system", text-only)
// carry no tool payload, so they pass through untouched and stay model-visible.

import type { UIMessage } from "@tanstack/ai-react";

/** A single message part, derived off UIMessage (ai-react doesn't re-export the
 * `MessagePart` union by name). */
type MessagePart = UIMessage["parts"][number];

/** Trailing user-turns kept verbatim. A "turn" is anchored on a user message. */
export const DEFAULT_RECENT_TURNS = 6;

/** What an evicted tool result collapses to — pairing kept, payload dropped. */
export const STUBBED_TOOL_RESULT =
	"[earlier tool result omitted to bound context — see the canvas/history]";

export interface BuildModelMessagesOptions {
	/** How many trailing user-turns to keep verbatim (default DEFAULT_RECENT_TURNS). */
	recentTurns?: number;
}

/**
 * Bound the model's view of a conversation: recent turns verbatim, older turns
 * with their tool payloads stubbed. Pure — no I/O, fully unit-testable.
 */
export function buildModelMessages(
	transcript: ReadonlyArray<UIMessage>,
	opts: BuildModelMessagesOptions = {},
): Array<UIMessage> {
	const recentTurns = opts.recentTurns ?? DEFAULT_RECENT_TURNS;
	const windowStart = computeWindowStart(transcript, recentTurns);
	if (windowStart === 0) return [...transcript];
	return transcript.map((m, i) => (i >= windowStart ? m : pruneOldMessage(m)));
}

/** Index of the first message to keep verbatim — the start of the recent
 * window, anchored on the `recentTurns`-th user message from the end. Returns 0
 * (keep everything) when the conversation has at most `recentTurns` user turns. */
function computeWindowStart(
	transcript: ReadonlyArray<UIMessage>,
	recentTurns: number,
): number {
	if (recentTurns <= 0) return transcript.length;
	const userIndices: Array<number> = [];
	transcript.forEach((m, i) => {
		if (m.role === "user") userIndices.push(i);
	});
	if (userIndices.length <= recentTurns) return 0;
	return userIndices[userIndices.length - recentTurns];
}

/** A copy of an older message with heavy tool payloads stubbed (text kept). */
function pruneOldMessage(message: UIMessage): UIMessage {
	let changed = false;
	const parts = message.parts.map((part) => {
		const pruned = prunePart(part);
		if (pruned !== part) changed = true;
		return pruned;
	});
	return changed ? { ...message, parts } : message;
}

function prunePart(part: MessagePart): MessagePart {
	if (part.type === "tool-result") {
		// Keep type/toolCallId/state (pairing + lifecycle); drop the payload.
		return { ...part, content: STUBBED_TOOL_RESULT, error: undefined };
	}
	if (part.type === "tool-call") {
		// Keep id/name/state so the call/result pair survives; drop the bytes.
		return { ...part, arguments: "", input: undefined, output: undefined };
	}
	// text / thinking / structured-output / media — cheap + meaningful, kept.
	return part;
}
