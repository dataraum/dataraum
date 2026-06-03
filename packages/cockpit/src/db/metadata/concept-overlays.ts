// Count the active (non-superseded) `concept` overlay rows declared for a
// vertical — the cockpit's pre-flight readiness check before triggering
// add_source (Theme B / obs 4).
//
// An overlay-backed vertical (`_adhoc` today; any framed name after Theme A)
// has NO on-disk `ontology.yaml` concepts, so its concepts come ONLY from
// `config_overlay` rows the frame stage writes. With none declared, the engine
// fails loud deep in `semantic_per_column` (semantic_per_column_phase.py) — a
// dead Temporal run the user can't read. The trigger uses this count to refuse
// early with a "run frame first" message instead of starting a doomed run.
//
// Mirrors the engine's concept-overlay filter (core/overlay.py `_apply_concept`):
// type == 'concept' AND payload.vertical == <vertical> AND not superseded.

import { and, count, eq, isNull, sql } from "drizzle-orm";

import { metadataDb } from "./client";
import { configOverlay } from "./schema";

/**
 * How many active `concept` overlay rows name this vertical. The vertical lives
 * inside the JSON payload (`payload.vertical`), matching how frame/teach write
 * concept rows and how the engine applier filters them.
 */
export async function countActiveConcepts(vertical: string): Promise<number> {
	const [row] = await metadataDb
		.select({ n: count() })
		.from(configOverlay)
		.where(
			and(
				isNull(configOverlay.supersededAt),
				eq(configOverlay.type, "concept"),
				eq(sql`${configOverlay.payload}->>'vertical'`, vertical),
			),
		);
	return row?.n ?? 0;
}
