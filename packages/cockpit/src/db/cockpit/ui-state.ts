// Per-conversation UI state persistence (DAT-462) — the canvas "viewing history"
// pin, so a reload returns to the same focus instead of snapping back to live.
//
// `saveUiState` is best-effort (fired on a UI interaction — a failed pin write
// must never surface an error to the user); `loadUiState` throws and the loader
// degrades to "no restored state" (live canvas) on failure.

import { eq } from "drizzle-orm";
import { cockpitDb } from "./client";
import { uiState } from "./schema";

export interface UiState {
	/** The pinned tool-call id the canvas is "viewing history" on, or null for live. */
	pinnedCallId: string | null;
}

/** Load the restorable UI state for a conversation, or null if none stored. */
export async function loadUiState(
	conversationId: string,
): Promise<UiState | null> {
	const [row] = await cockpitDb
		.select({ pinnedCallId: uiState.pinnedCallId })
		.from(uiState)
		.where(eq(uiState.conversationId, conversationId))
		.limit(1);
	if (!row) return null;
	return { pinnedCallId: row.pinnedCallId ?? null };
}

/** Upsert the UI state for a conversation. Best-effort: swallows + logs. */
export async function saveUiState(
	conversationId: string,
	state: UiState,
): Promise<void> {
	try {
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
