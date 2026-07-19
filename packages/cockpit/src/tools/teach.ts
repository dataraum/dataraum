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

import { metadataWriteDb } from "../db/metadata/client";
import { configOverlayWrite } from "../db/metadata/write-surface";
import {
	AGENT_TEACH_TYPES,
	CONNECT_TEACH_TYPES,
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
	// targets — the writer role's search_path resolves it (DAT-816).
	// config_overlay carries no session_id post-DAT-506 (the overlay
	// vocabulary is workspace-scoped).
	await metadataWriteDb.insert(configOverlayWrite).values({
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
	await metadataWriteDb
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
 * Build a `teach` tool advertising a SCOPED set of teach types (DAT-597). Two
 * call sites: STAGE (AGENT_TEACH_TYPES — catalogue-grain meaning + topology) and
 * CONNECT (CONNECT_TEACH_TYPES — the mechanical add_source layer only). Same name + server
 * handler; the `type` enum is what fences which families each chat offers (the
 * payload union + `validateTeach` enforce per-type shape regardless). An acting
 * tool: a teach mutates the workspace, so it runs on the user's explicit
 * instruction — no approval gate. Success OR a structured validation error:
 * `validateTeach` rejects a malformed payload by throwing `TeachValidationError`,
 * returned as data so the agent can read it and retry (DB errors still throw).
 */
function makeTeachTool(types: readonly TeachType[], description: string) {
	return toolDefinition({
		name: "teach",
		description,
		inputSchema: z.object({
			type: z
				.enum(types as readonly [TeachType, ...TeachType[]])
				.describe(
					"The kind of grounding-layer correction to record; it determines which payload fields are required (see payload).",
				),
			payload: TeachPayloadSchema,
		}),
		outputSchema: z.union([
			z.object({ overlay_id: z.string(), type: z.string() }),
			z.object({ error: z.string() }),
		]),
	}).server(runTeachTool);
}

/** STAGE's teach: catalogue-grain TOPOLOGY (relationship/hierarchy) — the
 * corrections a begin_session re-run realizes. The concept vocabulary is declared
 * via the frame stage's typed write (DAT-728), not here; mechanical typing-grain
 * teaches live on CONNECT (add_source replay). */
export const teachTool = makeTeachTool(
	AGENT_TEACH_TYPES,
	"Record a catalogue-grain correction about the data's TOPOLOGY — a table " +
		"relationship or a hierarchy / drill-down. Writes a config_overlay row; " +
		"re-run begin_session to apply it. The business concept vocabulary is " +
		"declared in the frame stage (not taught here); mechanical typing " +
		"corrections (typing pattern, null token, column unit) are taught in a " +
		"CONNECT chat; for operating-model declarations use the dedicated tools: " +
		"teach_validation, teach_cycle, teach_metric.",
);

/** CONNECT's teach (DAT-597; narrowed DAT-647): the add_source grounding layer
 * ONLY — the MECHANICAL, typing-grain teaches an add_source `replay` can realize:
 * a typing pattern, a null token, or a value-carried column unit. The concept
 * vocabulary is declared via the frame stage's typed write (DAT-728); topology and
 * the operating model are taught in the Stage chat. */
export const connectTeachTool = makeTeachTool(
	CONNECT_TEACH_TYPES,
	"Record a MECHANICAL add_source grounding correction the import got wrong — a " +
		"typing pattern, a null token, or a value-carried column unit. Writes a " +
		"config_overlay row; follow with `replay` to re-ground the source. The " +
		"business concept vocabulary, relationships, hierarchies, and validations " +
		"are declared/taught in the Stage chat.",
);

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
