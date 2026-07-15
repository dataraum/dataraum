// Typed concept writes (DAT-728, config→DB) — the cockpit's frame path writes the
// concept vocabulary as rows in ws_<id>.concepts, NOT as config_overlay teach rows.
//
// This is the write half of the config→DB cut: the engine seeds the shipped
// vertical's concepts as typed rows at connect and reads them via concept_store;
// `frame` declares/edits the user's model through the SAME table (the Drizzle
// metadata write surface, granted SELECT/INSERT/UPDATE on `concepts`).
//
// A concept is a workspace-persistent node keyed by (vertical, name); an edit is
// NOT an in-place update but a supersede + insert: the partial-unique index keeps
// at most one ACTIVE row per (vertical, name), so writing a new active row while
// an old one is still active would violate it. Supersede the active row (stamp
// superseded_at), then insert the new active row — atomically, so the concept is
// never momentarily absent.

import { randomUUID } from "node:crypto";
import { and, eq, isNull } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import { conceptsWrite } from "../db/metadata/write-surface";

// The ontological kind of a concept — mirrors the engine's ConceptKind
// (packages/engine/.../analysis/semantic/db_models.py). Required on every write:
// the typed `concepts` table has `kind` NOT NULL — a concept is one of these four.
export const CONCEPT_KINDS = [
	"measure",
	"entity",
	"dimension",
	"unit",
] as const;
export type ConceptKind = (typeof CONCEPT_KINDS)[number];

// The fields the cockpit supplies for one concept (snake_case to mirror the
// engine's OntologyConcept / the old `concept` teach payload). Identity
// (`concept_id`), `source`, and the lifecycle timestamps are set on write.
export interface ConceptWriteInput {
	vertical: string;
	name: string;
	kind: ConceptKind;
	description?: string;
	indicators?: string[];
	exclude_patterns?: string[];
	unit_from_concept?: string;
}

/**
 * Write one typed concept as an edit = supersede the active row + insert a new
 * active row, in one transaction. Returns the new row's `concept_id` (a
 * workspace-stable surrogate minted here). `source='frame'` marks it user-declared
 * (vs the engine's `source='seed'`), which is how a concept-only framed vertical is
 * recognized (core.vertical's framed-concept resolver).
 *
 * Concurrency: the supersede + insert is ONE transaction, so the partial-unique
 * index (≤1 active row per (vertical, name)) is the invariant. If a concurrent
 * write (another edit, or the engine's first-run seed) races the same key, one
 * transaction's INSERT hits the index and the WHOLE transaction rolls back — the
 * incumbent active row is preserved, never a partial supersede-with-no-replacement.
 * The loser surfaces a transient error to retry; there is no corruption.
 */
export async function writeConcept(
	input: ConceptWriteInput,
): Promise<{ concept_id: string }> {
	const conceptId = randomUUID();
	await metadataDb.transaction(async (tx) => {
		await tx
			.update(conceptsWrite)
			.set({ supersededAt: new Date() })
			.where(
				and(
					eq(conceptsWrite.vertical, input.vertical),
					eq(conceptsWrite.name, input.name),
					isNull(conceptsWrite.supersededAt),
				),
			);
		await tx.insert(conceptsWrite).values({
			conceptId,
			vertical: input.vertical,
			name: input.name,
			kind: input.kind,
			description: input.description,
			indicators: input.indicators,
			excludePatterns: input.exclude_patterns,
			unitFromConcept: input.unit_from_concept,
			source: "frame",
			createdAt: new Date(),
			supersededAt: null,
		});
	});
	return { concept_id: conceptId };
}
