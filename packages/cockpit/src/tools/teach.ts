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
import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { configOverlayWrite } from "../db/metadata/write-surface";
import {
	AGENT_TEACH_TYPES,
	type TeachInput,
	TeachPayloadSchema,
	type TeachType,
	TeachValidationError,
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
	// pgSchema. config_overlay carries no session_id post-DAT-506 (the overlay
	// vocabulary is workspace-scoped).
	await metadataDb.insert(configOverlayWrite).values({
		overlayId,
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
		.update(configOverlayWrite)
		.set({ supersededAt: new Date() })
		.where(
			and(
				eq(configOverlayWrite.overlayId, overlayId),
				isNull(configOverlayWrite.supersededAt),
			),
		);
}

/**
 * The `teach` tool for the agent loop. An acting tool: a teach mutates the
 * workspace, so it runs on the user's explicit instruction — there is no
 * approval gate. Coarse input schema (type + payload object); the per-type deep
 * validation runs inside `teach()` via `validateTeach`.
 */
export const teachTool = toolDefinition({
	name: "teach",
	description:
		"Record a grounding-layer correction about the data — a typing pattern, " +
		"null token, column unit, ontology concept/property, or a column " +
		"relationship. Writes a config_overlay row; follow with `replay` to apply " +
		"it to the source. For operating-model declarations use the dedicated tools " +
		"instead: teach_validation, teach_cycle, teach_metric.",
	inputSchema: z.object({
		type: z
			.enum(AGENT_TEACH_TYPES as readonly [TeachType, ...TeachType[]])
			.describe(
				"The kind of grounding-layer correction to record; it determines which payload fields are required (see payload).",
			),
		payload: TeachPayloadSchema,
	}),
	// Success OR a structured validation error: the per-type `validateTeach`
	// rejects a malformed payload by throwing `TeachValidationError`. Surfacing
	// that as a raw thrown Error kills the agent turn; returning it as data lets
	// the agent read the message and retry. Non-validation errors (DB, etc.)
	// still throw — those are not the agent's to fix.
	outputSchema: z.union([
		z.object({ overlay_id: z.string(), type: z.string() }),
		z.object({ error: z.string() }),
	]),
}).server(runTeachTool);

/**
 * The `teach` tool's server handler, extracted so its error surface is
 * unit-testable without the SDK wrapper. A malformed payload makes
 * `validateTeach` throw `TeachValidationError`; we return that as structured
 * data so the agent can read the message and retry. Any other error (DB,
 * connectivity, …) is not the agent's to fix and propagates.
 */
export async function runTeachTool(
	input: TeachInput,
): Promise<TeachResult | { error: string }> {
	try {
		return await teach(input);
	} catch (err) {
		if (err instanceof TeachValidationError) {
			return { error: err.message };
		}
		throw err;
	}
}
