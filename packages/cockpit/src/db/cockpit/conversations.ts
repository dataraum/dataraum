// Server-owned conversation persistence (DAT-462) — cockpit_db is the source of
// truth for the chat transcript; the client is a view, seeded via
// `initialMessages` on reload and kept current by the stream.
//
// Unlike `runs.ts` (a best-effort control-plane breadcrumb that must never fail
// the user's workflow), these are LOAD-BEARING: losing a turn silently would
// corrupt the transcript. They throw on real errors; the call sites decide how
// to degrade (the chat route / route loader catch and fall back to a transient,
// unpersisted conversation rather than a dead page — see their handlers).
//
// Two transcript reads, by design (the model/display split, DAT-462):
//   • loadDisplayMessages — modelOnly rows EXCLUDED; what the human sees + what
//     the canvas derives from. This is what reload hydration returns.
//   • loadModelTranscript — ALL rows in order, incl. modelOnly refs rows; the
//     input to `buildModelMessages`, never shown to the user.

import { randomUUID } from "node:crypto";
import type { UIMessage } from "@tanstack/ai-react";
import { and, desc, eq, max } from "drizzle-orm";
import { foldModelOnlyRefs } from "#/lib/model-messages";
import { cockpitDb } from "./client";
import { conversationMessages, conversations } from "./schema";

/** A message to persist, with whether it is model-only (the refs channel). */
export interface MessageEntry {
	message: UIMessage;
	/** Fed to the model via buildModelMessages but never returned to display. */
	modelOnly?: boolean;
}

/**
 * The active conversation id for a workspace — the boot/loader path. One thread
 * per workspace today: returns the existing one, else creates it. The returned
 * id is handed to `useChat` as `threadId`, so within a session resolution
 * happens ONCE here and the server reuses the echoed id (no re-resolve race). A
 * cold-start double-create (two tabs, no row yet) is possible but benign for
 * single-user — the newest wins on the next resolve; a uniqueness guard would
 * foreclose multi-conversation history, so it's deliberately omitted.
 */
export async function resolveActiveConversation(
	workspaceId: string,
): Promise<string> {
	const [row] = await cockpitDb
		.select({ id: conversations.id })
		.from(conversations)
		.where(eq(conversations.workspaceId, workspaceId))
		.orderBy(desc(conversations.createdAt))
		.limit(1);
	if (row) return row.id;
	const id = randomUUID();
	await cockpitDb
		.insert(conversations)
		.values({ id, workspaceId })
		.onConflictDoNothing({ target: conversations.id });
	return id;
}

/**
 * Ensure a conversation row exists for a client-supplied `threadId` — the server
 * path. The loader normally creates it first, but the client owns the threadId
 * on the wire, so the append's FK could otherwise miss; this makes the server
 * self-sufficient and idempotent.
 */
export async function ensureConversation(
	conversationId: string,
	workspaceId: string,
): Promise<void> {
	await cockpitDb
		.insert(conversations)
		.values({ id: conversationId, workspaceId })
		.onConflictDoNothing({ target: conversations.id });
}

/** The workspace a conversation belongs to (Phase 2A) — the completion-watcher
 * resolves it to find the conversation's in-flight runs. Null if the conversation
 * row is gone (a stale threadId). */
export async function getConversationWorkspaceId(
	conversationId: string,
): Promise<string | null> {
	const [row] = await cockpitDb
		.select({ workspaceId: conversations.workspaceId })
		.from(conversations)
		.where(eq(conversations.id, conversationId))
		.limit(1);
	return row?.workspaceId ?? null;
}

/** The display transcript (modelOnly rows excluded), in order — reload hydration
 * + the canvas source. */
export async function loadDisplayMessages(
	conversationId: string,
): Promise<Array<UIMessage>> {
	const rows = await cockpitDb
		.select({ message: conversationMessages.message })
		.from(conversationMessages)
		.where(
			and(
				eq(conversationMessages.conversationId, conversationId),
				eq(conversationMessages.modelOnly, false),
			),
		)
		.orderBy(conversationMessages.seq);
	return rows.map((r) => r.message);
}

/** The full transcript with model-only refs rows FOLDED into their user turns,
 * in order — the `buildModelMessages` input. No model_only filter (refs feed the
 * model); the fold keeps them from becoming consecutive same-role messages. */
export async function loadModelTranscript(
	conversationId: string,
): Promise<Array<UIMessage>> {
	const rows = await cockpitDb
		.select({
			message: conversationMessages.message,
			modelOnly: conversationMessages.modelOnly,
		})
		.from(conversationMessages)
		.where(eq(conversationMessages.conversationId, conversationId))
		.orderBy(conversationMessages.seq);
	return foldModelOnlyRefs(rows);
}

/**
 * Append messages to a conversation, idempotent by message id (a re-sent turn is
 * a no-op). `seq` continues from the conversation's current max so ordering is
 * stable; gaps from skipped duplicates are harmless.
 *
 * Known limitation (single-user assumption): the max(seq) read → insert is not
 * atomic, so two genuinely concurrent sends for the SAME conversation (e.g. two
 * tabs sending in the same tick) could allocate overlapping seq values. Benign
 * for the current single-active-user model; a `(conversation_id, seq)` unique
 * constraint + retry, or an advisory lock, is the fix when multi-tab lands.
 * Bumps `updatedAt`.
 */
export async function appendMessages(
	conversationId: string,
	entries: ReadonlyArray<MessageEntry>,
): Promise<void> {
	if (entries.length === 0) return;
	const [{ maxSeq } = { maxSeq: null }] = await cockpitDb
		.select({ maxSeq: max(conversationMessages.seq) })
		.from(conversationMessages)
		.where(eq(conversationMessages.conversationId, conversationId));
	let seq = (maxSeq ?? -1) + 1;
	const rows = entries.map((e) => ({
		id: e.message.id,
		conversationId,
		seq: seq++,
		role: e.message.role,
		message: e.message,
		modelOnly: e.modelOnly ?? false,
	}));
	await cockpitDb
		.insert(conversationMessages)
		.values(rows)
		.onConflictDoNothing({ target: conversationMessages.id });
	await cockpitDb
		.update(conversations)
		.set({ updatedAt: new Date() })
		.where(eq(conversations.id, conversationId));
}
