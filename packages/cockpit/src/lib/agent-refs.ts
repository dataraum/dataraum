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

// Both halves of `RefsTurn` are PUBLIC SDK exports (DAT-449): the message
// shape from @tanstack/ai-client, the content-part shape from the
// @tanstack/ai/client subpath — no hand-mirrored field shapes to drift.
import type { TextPart } from "@tanstack/ai/client";
import type { MultimodalContent } from "@tanstack/ai-client";

/** A refs turn IS the SDK's `MultimodalContent` (what `sendMessage` accepts),
 * narrowed to the text-part array this module actually emits — consumers keep
 * `turn.content[i].content` precision without re-narrowing the SDK union. */
export type RefsTurn = Omit<MultimodalContent, "content"> & {
	content: Array<TextPart>;
};

/** Sentinel prefix marking a model-only refs part. Renderers skip any user text
 * part that starts with it; nothing a human types begins this way.
 * Replaced DAT-423's `[[dataraum:uploaded-objects]]` outright — chat is
 * in-memory today, so no stored conversation carries the old marker; a future
 * conversation-persistence layer only ever needs to recognize THIS one. */
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
