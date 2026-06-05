// Model-only refs parts (DAT-423 upload handoff, generalized for DAT-437).
//
// A UI surface that composes a chat turn often needs to hand the agent an
// internal identifier (an s3:// uri, a table_id, a column_id) — but raw ids
// must NEVER appear in the visible user bubble. The pattern: the turn carries
// TWO text parts —
//   1. a CLEAN bubble (human names only) the chat rail renders, and
//   2. a marked REFS part (the structured internals) the model reads but every
//      user-part renderer skips (`isAgentRefsPart`).
// Self-contained on the message — no side-channel state to thread or clear.
//
// Any surface that renders user message parts (the chat rail today; a future
// transcript export, copy-to-clipboard, debug panel) MUST apply the same skip,
// or the internals leak.

/** Sentinel prefix marking a model-only refs part. Renderers skip any user text
 * part that starts with it; nothing a human types begins this way. */
export const AGENT_REFS_MARKER = "[[dataraum:refs]]";

/** True when a user text part is a structured refs block — a renderer must NOT
 * show it (it carries internal ids/uris, model-only). */
export function isAgentRefsPart(content: string): boolean {
	return content.startsWith(AGENT_REFS_MARKER);
}

/** Mark a refs body as model-only. The body should be self-describing — say
 * what the refs are for and that they are internal (the model must use them in
 * tool calls, never echo them). */
export function agentRefsBlock(body: string): string {
	return `${AGENT_REFS_MARKER} ${body}`;
}

/** A two-part turn: the clean bubble + the marked refs part. Structurally the
 * multimodal content shape the SDK's `sendMessage` accepts (mirrored as
 * `TurnContent` in cockpit-state — kept structural here so lib/ doesn't import
 * from ui/). */
export interface RefsTurn {
	content: Array<{ type: "text"; content: string }>;
}

/** Compose a turn whose bubble is clean and whose internals ride in a marked,
 * model-only refs part. `refsBody` is marked via `agentRefsBlock`. */
export function turnWithRefs(bubble: string, refsBody: string): RefsTurn {
	return {
		content: [
			{ type: "text", content: bubble },
			{ type: "text", content: agentRefsBlock(refsBody) },
		],
	};
}
