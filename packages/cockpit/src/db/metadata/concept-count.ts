// Count the active (non-superseded) typed `concepts` rows for a vertical — the
// cockpit's pre-flight readiness check before triggering add_source.
//
// Config→DB (DAT-728): the concept vocabulary is the typed `concepts` table, not
// `config_overlay` rows. A framed / directory-less vertical has NO on-disk
// `ontology.yaml`, so its concepts come ONLY from the rows the frame stage writes
// here (source='frame'). With none declared, the engine fails loud deep in
// `semantic_per_column` (semantic_per_column_phase.py) — a dead Temporal run the
// user can't read. The trigger uses this count (via `verticalConceptCount`'s
// `resolvedConceptCount`, DAT-883: THIS count wins once it's nonzero, the
// on-disk count only as a pre-frame fallback — never summed) to refuse early with
// a "run frame first" message instead of starting a doomed run. A shipped
// vertical's seed rows aren't written until the pipeline runs, so at pre-flight
// its on-disk count is what clears the guard.

import { and, count, eq, isNull } from "drizzle-orm";
import { metadataWriteDb } from "./client";
import { conceptsWrite } from "./write-surface";

/**
 * How many active typed `concepts` rows name this vertical (the partial-unique
 * index keeps at most one active row per (vertical, name), so this is the count of
 * distinct live concepts).
 */
export async function countActiveConcepts(vertical: string): Promise<number> {
	const [row] = await metadataWriteDb
		.select({ n: count() })
		.from(conceptsWrite)
		.where(
			and(
				isNull(conceptsWrite.supersededAt),
				eq(conceptsWrite.vertical, vertical),
			),
		);
	return row?.n ?? 0;
}
