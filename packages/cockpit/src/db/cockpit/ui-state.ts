// Per-conversation UI state persistence (DAT-462) — the canvas "viewing history"
// pin, so a reload returns to the same focus instead of snapping back to live.
//
// `saveUiState` is best-effort (fired on a UI interaction — a failed pin write
// must never surface an error to the user); `loadUiState` throws and the loader
// degrades to "no restored state" (live canvas) on failure.
//
// Workspace scope (DAT-817): `ui_state` rows carry no workspace_id — they scope
// through their owning conversation, so both paths gate on the conversation
// belonging to the boot workspace (cockpit_db is shared across per-workspace
// cockpits). A foreign conversation reads as "no stored state" and its pin
// write is dropped (logged), matching the module's degradation contract.

import { and, eq } from "drizzle-orm";
import { cockpitDb } from "./client";
import { bootWorkspaceId } from "./registry";
import { conversations, uiState } from "./schema";

export interface UiState {
	/** The pinned tool-call id the canvas is "viewing history" on, or null for live. */
	pinnedCallId: string | null;
}

/** Load the restorable UI state for a conversation, or null if none stored
 * (or the conversation isn't the boot workspace's — DAT-817). */
export async function loadUiState(
	conversationId: string,
): Promise<UiState | null> {
	const [row] = await cockpitDb
		.select({ pinnedCallId: uiState.pinnedCallId })
		.from(uiState)
		.innerJoin(conversations, eq(uiState.conversationId, conversations.id))
		.where(
			and(
				eq(uiState.conversationId, conversationId),
				eq(conversations.workspaceId, bootWorkspaceId()),
			),
		)
		.limit(1);
	if (!row) return null;
	return { pinnedCallId: row.pinnedCallId ?? null };
}

/** Upsert the UI state for a conversation. Best-effort: swallows + logs; a
 * foreign workspace's conversation id is dropped the same way (DAT-817). */
export async function saveUiState(
	conversationId: string,
	state: UiState,
): Promise<void> {
	try {
		const [owned] = await cockpitDb
			.select({ id: conversations.id })
			.from(conversations)
			.where(
				and(
					eq(conversations.id, conversationId),
					eq(conversations.workspaceId, bootWorkspaceId()),
				),
			)
			.limit(1);
		if (!owned) {
			console.warn(
				`[cockpit] saveUiState dropped for ${conversationId}: not in the boot workspace`,
			);
			return;
		}
		await cockpitDb
			.insert(uiState)
			.values({ conversationId, pinnedCallId: state.pinnedCallId })
			.onConflictDoUpdate({
				target: uiState.conversationId,
				set: { pinnedCallId: state.pinnedCallId, updatedAt: new Date() },
			});
	} catch (err) {
		console.warn(`[cockpit] saveUiState failed for ${conversationId}: ${err}`);
	}
}
