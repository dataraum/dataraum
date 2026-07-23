// Typed convention writes (DAT-789, config→DB) — the cockpit's frame path writes the
// domain conventions as rows in ws_<id>.conventions, NOT as config_overlay teach rows
// (DAT-728 retired that route for typed homes).
//
// This is the write half of the config→DB cut for conventions: the engine seeds the
// shipped vertical's conventions as typed rows at connect and reads them via
// convention_store; all three SQL authors (extraction, validation, this cockpit's Q&A
// agent) read the same table. `frame` declares/edits the user's conventions through it
// (the Drizzle metadata write surface, granted SELECT/INSERT/UPDATE on `conventions`).
//
// A convention is a workspace-persistent node keyed by (vertical, name); an edit is NOT
// an in-place update but a supersede + insert: the partial-unique index keeps at most
// one ACTIVE row per (vertical, name), so writing a new active row while an old one is
// still active would violate it. Supersede the active row (stamp superseded_at), then
// insert the new active row — atomically, so the convention is never momentarily absent.
// The `statement` is stored VERBATIM — declared human judgment, never interpreted here.

import { randomUUID } from "node:crypto";
import { and, eq, isNull } from "drizzle-orm";

import { metadataWriteDb } from "../db/metadata/client";
import { conventionsWrite } from "../db/metadata/write-surface";

// The fields the cockpit supplies for one convention (snake_case to mirror the engine's
// OntologyConvention). Identity (`convention_id`), `source`, and the lifecycle
// timestamps are set on write. `name` is the convention's stable identifier within the
// vertical (the engine's OntologyConvention `id`).
export interface ConventionWriteInput {
	vertical: string;
	name: string;
	statement: string;
	targets?: string[];
	concept_groups?: Record<string, string[]>;
}

/**
 * Write one typed convention as an edit = supersede the active row + insert a new active
 * row, in one transaction. Returns the new row's `convention_id` (a workspace-stable
 * surrogate minted here). `source='frame'` marks it user-declared (vs the engine's
 * `source='seed'`) — the two live writers the `conventions.source` CHECK admits.
 *
 * Concurrency: the supersede + insert is ONE transaction, so the partial-unique index
 * (≤1 active row per (vertical, name)) is the invariant. If a concurrent write (another
 * edit, or the engine's first-run seed) races the same key, one transaction's INSERT
 * hits the index and the WHOLE transaction rolls back — the incumbent active row is
 * preserved, never a partial supersede-with-no-replacement. The loser surfaces a
 * transient error to retry; there is no corruption. Same shape as `writeConcept`.
 */
export async function writeConvention(
	input: ConventionWriteInput,
): Promise<{ convention_id: string }> {
	const conventionId = randomUUID();
	await metadataWriteDb.transaction(async (tx) => {
		await tx
			.update(conventionsWrite)
			.set({ supersededAt: new Date() })
			.where(
				and(
					eq(conventionsWrite.vertical, input.vertical),
					eq(conventionsWrite.name, input.name),
					isNull(conventionsWrite.supersededAt),
				),
			);
		await tx.insert(conventionsWrite).values({
			conventionId,
			vertical: input.vertical,
			name: input.name,
			statement: input.statement,
			targets: input.targets,
			conceptGroups: input.concept_groups,
			source: "frame",
			createdAt: new Date(),
			supersededAt: null,
		});
	});
	return { convention_id: conventionId };
}
