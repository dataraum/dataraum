// Teach tool (DAT-343) — writes a row to ws_<id>.config_overlay.
//
// Pure write: validates `{type, payload}` per type, inserts the overlay row
// via the Drizzle metadata client, and returns the new overlay_id. Does NOT
// start a workflow — replay is a separate tool the agent invokes after one
// or more teaches (batchable; see /refine for the explicit-replay rationale).
//
// Per-type validation lives in `./teach.validation.ts` so the schema surface
// is importable without booting `config.ts` (test ergonomics). This module
// owns the DB-bound side.
//
// Policy break documented for the reviewer: the metadata client is otherwise
// read-only — `config_overlay` is the one table the cockpit writes to. The
// engine owns the schema; teach edits flow through this single seam.

import { randomUUID } from "node:crypto";

import { metadataDb } from "../db/metadata/client";
import { configOverlay } from "../db/metadata/schema";
import {
	type TeachInput,
	type TeachType,
	validateTeach,
} from "./teach.validation";

export {
	TEACH_TYPES,
	type TeachInput,
	type TeachType,
	TeachValidationError,
	validateTeach,
} from "./teach.validation";

export interface TeachResult {
	overlay_id: string;
	type: TeachType;
}

/**
 * Write a teach mutation as a new `config_overlay` row.
 *
 * Pure write path: validates, inserts, returns the overlay_id. The caller
 * (chat agent / replay tool) decides whether to follow up with a replay.
 */
export async function teach(input: TeachInput): Promise<TeachResult> {
	const payload = validateTeach(input);
	const overlayId = randomUUID();

	// Workspace identity is implicit in the ws_<id> schema this insert
	// targets — the metadata client's connection already scopes to it via
	// pgSchema. No explicit workspace_id column post-DAT-343 (multi-workspace
	// shared-schema is DAT-357; bring the column back then).
	await metadataDb.insert(configOverlay).values({
		overlayId,
		sessionId: input.session_id ?? null,
		type: input.type,
		payload,
		createdAt: new Date(),
		supersededAt: null,
	});

	return { overlay_id: overlayId, type: input.type };
}

/**
 * Soft-undo a previously applied teach by setting `superseded_at = now()`.
 * Idempotent: re-undoing a row already superseded leaves it untouched.
 *
 * Per DAT-343: undo + a follow-up `replay` reverts the teach's effect.
 */
export async function undoTeach(overlayId: string): Promise<void> {
	const { eq, isNull, and } = await import("drizzle-orm");
	await metadataDb
		.update(configOverlay)
		.set({ supersededAt: new Date() })
		.where(
			and(
				eq(configOverlay.overlayId, overlayId),
				isNull(configOverlay.supersededAt),
			),
		);
}
