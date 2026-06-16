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
import { and, desc, eq, isNull, max } from "drizzle-orm";
import { foldModelOnlyRefs } from "#/lib/model-messages";
import { cockpitDb } from "./client";
import { conversationMessages, conversations } from "./schema";

/** A message to persist, with whether it is model-only (the refs channel). */
export interface MessageEntry {
	message: UIMessage;
	/** Fed to the model via buildModelMessages but never returned to display. */
	modelOnly?: boolean;
}

/** The chat type (DAT-528) — set at create, immutable. Binds the toolstack +
 * system prompt ("skill"); the binding lands in S2, S1 stores + routes by it. */
export type ConversationKind = "connect" | "stage" | "analyse";

/** How many recent conversations the history list shows. Bounded (DD/36667393:
 * "open-ended history is unrealistic") — matches the run-sweep bounds elsewhere. */
export const HISTORY_LIMIT = 20;

/** A conversation as the history list renders it. */
export interface ConversationSummary {
	id: string;
	kind: ConversationKind;
	title: string | null;
	lastActiveAt: Date;
}

/** A conversation as the chat route hydrates it (kind drives the toolstack in
 * S2; workspaceId anchors the FK reads). */
export interface ConversationRow {
	id: string;
	workspaceId: string;
	kind: ConversationKind;
	title: string | null;
}

/**
 * The workspace's recent conversations, newest-active first, BOUNDED — the
 * landing/history list (DAT-528). Ordered by `lastActiveAt` (bumped on every
 * append) so a resumed chat floats to the top. Many chats per type are allowed
 * within the bound.
 */
export async function listConversations(
	workspaceId: string,
	limit: number = HISTORY_LIMIT,
): Promise<Array<ConversationSummary>> {
	const rows = await cockpitDb
		.select({
			id: conversations.id,
			kind: conversations.kind,
			title: conversations.title,
			lastActiveAt: conversations.lastActiveAt,
		})
		.from(conversations)
		.where(eq(conversations.workspaceId, workspaceId))
		.orderBy(desc(conversations.lastActiveAt))
		.limit(limit);
	return rows.map((r) => ({ ...r, kind: r.kind as ConversationKind }));
}

/**
 * Create a typed conversation and return its id — the only way a conversation is
 * born (DAT-528). `kind` is required + immutable: there is no create-without-type
 * path, which is what makes "every chat has a kind" true by construction (the
 * NOT NULL column is the backstop). The id becomes the `useChat` threadId and the
 * `/cockpit/$conversationId` route param.
 */
export async function createConversation(
	workspaceId: string,
	kind: ConversationKind,
): Promise<string> {
	const id = randomUUID();
	await cockpitDb.insert(conversations).values({ id, workspaceId, kind });
	return id;
}

/** Hydrate a conversation by id (the chat route loader) — kind + title +
 * owning workspace. Null if the id is unknown (a stale deep link → the route
 * 404s rather than mounting an orphan chat). */
export async function getConversation(
	conversationId: string,
): Promise<ConversationRow | null> {
	const [row] = await cockpitDb
		.select({
			id: conversations.id,
			workspaceId: conversations.workspaceId,
			kind: conversations.kind,
			title: conversations.title,
		})
		.from(conversations)
		.where(eq(conversations.id, conversationId))
		.limit(1);
	if (!row) return null;
	return { ...row, kind: row.kind as ConversationKind };
}

/**
 * Set a conversation's history label ONCE, from the first user message (DAT-528).
 * The `title IS NULL` guard makes it first-write-wins + idempotent — a later turn
 * never overwrites it, and a Haiku summary (S4) can replace this slice. Bumped
 * via a conditional UPDATE so no read-modify-write race. Best-effort: title is
 * cosmetic, so a failure is swallowed (never fail a turn over a label).
 */
export async function setConversationTitle(
	conversationId: string,
	title: string,
): Promise<void> {
	try {
		await cockpitDb
			.update(conversations)
			.set({ title })
			.where(
				and(eq(conversations.id, conversationId), isNull(conversations.title)),
			);
	} catch (err) {
		console.warn(
			`[cockpit] setConversationTitle failed for ${conversationId}: ${err}`,
		);
	}
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
	const now = new Date();
	await cockpitDb
		.update(conversations)
		.set({ updatedAt: now, lastActiveAt: now })
		.where(eq(conversations.id, conversationId));
}
