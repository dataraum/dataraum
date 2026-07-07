// Grain precedence for the multi-head current_* entropy views (DAT-509, DAT-506).
//
// `current_entropy_objects` / `current_entropy_readiness` / `current_claim_witnesses`
// are multi-grain (docs/architecture/persistence.md + DAT-442): one target can carry a row sealed by the
// add_source per-table GENERATION head and, once begin_session / operating_model
// run, a second row sealed by the workspace CATALOG head. For READINESS the engine
// view itself now resolves ONE row per target (catalog-grain precedence between the
// `catalog` and `operating_model` heads lives in SQL — DAT-506), so the cockpit
// reads readiness directly with no pick. The OBJECT / WITNESS union views still
// carry coexisting per-detector rows, so `mergeCurrentEvidence` keeps the
// catalog-over-table pick per detector at the read edge.
//
// These helpers mirror the engine's rank (catalog-grain over table-grain, latest
// within a grain). The verdict-history projection (DAT-513) labels every coexisting
// row so a picked verdict is always disclosable.

import { z } from "zod";

/** The head discriminators + recency every multi-grain view row carries. */
export interface GrainRow {
	viaTableHead: boolean | null;
	viaCatalogHead: boolean | null;
	viaOperatingModelHead: boolean | null;
	computedAt: Date | null;
}

/** Catalog-grain = sealed by a begin_session catalog or operating_model head. */
function isCatalogGrain(row: GrainRow): boolean {
	return row.viaCatalogHead === true || row.viaOperatingModelHead === true;
}

/** The pipeline stage a snapshot row was sealed by (DAT-513). The pick is only
 * evaluable if the caller can SEE it — every surface that shows a picked
 * verdict labels it with this stage. `operating_model` outranks
 * `catalog` in the label when both bits are set (they never are today:
 * a row is sealed by exactly one head; the order is defensive). */
export type GrainStage =
	| "add_source"
	| "catalog"
	| "operating_model"
	| "unknown";

export function stageOfRow(row: GrainRow): GrainStage {
	if (row.viaOperatingModelHead === true) return "operating_model";
	if (row.viaCatalogHead === true) return "catalog";
	if (row.viaTableHead === true) return "add_source";
	return "unknown";
}

/** Latest by computedAt; null sorts oldest; ties keep the earlier row —
 * "earlier" meaning input-array order, so an `.orderBy` added at a call site
 * would silently change the tie-break. Ties are genuinely arbitrary today. */
function latest<T extends GrainRow>(rows: readonly T[]): T | undefined {
	let best: T | undefined;
	for (const row of rows) {
		if (best === undefined) {
			best = row;
			continue;
		}
		const bestAt = best.computedAt?.getTime() ?? Number.NEGATIVE_INFINITY;
		const rowAt = row.computedAt?.getTime() ?? Number.NEGATIVE_INFINITY;
		if (rowAt > bestAt) best = row;
	}
	return best;
}

/**
 * Pick THE current row for one target: the latest catalog-grain row when any
 * exists (a begin_session / operating_model run supersedes the add_source
 * verdict — it was built over the run-resolved merge of both grains), else the
 * table-head row, else — for rows that predate the discriminators or carry
 * none — the latest row. Used by `mergeCurrentEvidence` for the per-detector
 * object/witness union; readiness picks NOTHING here (the engine view resolves
 * one row per target, DAT-506).
 */
export function pickCurrentRow<T extends GrainRow>(
	rows: readonly T[],
): T | undefined {
	const catalog = rows.filter(isCatalogGrain);
	if (catalog.length > 0) return latest(catalog);
	const table = rows.filter((r) => r.viaTableHead === true);
	if (table.length > 0) return latest(table);
	return latest(rows);
}

/**
 * Merge a multi-grain evidence row set: one row per detector, catalog-grain
 * winning over table-grain per detector (add_source-only detectors keep their
 * table-head row; re-adjudicated detectors show the catalog verdict). Output
 * preserves the input's first-occurrence detector order, so callers' ORDER BY
 * survives the merge.
 */
export function mergeCurrentEvidence<
	T extends GrainRow & { detectorId: string | null },
>(rows: readonly T[]): T[] {
	const order: string[] = [];
	const byDetector = new Map<string, T[]>();
	for (const row of rows) {
		const key = row.detectorId ?? "";
		const group = byDetector.get(key);
		if (group === undefined) {
			order.push(key);
			byDetector.set(key, [row]);
		} else {
			group.push(row);
		}
	}
	const merged: T[] = [];
	for (const key of order) {
		const picked = pickCurrentRow(byDetector.get(key) ?? []);
		if (picked !== undefined) merged.push(picked);
	}
	return merged;
}

/** One snapshot row in a target's verdict history (DAT-513) — the disclosure
 * surface for the pick: every coexisting row, labeled, oldest first. Cross-
 * session rows appear here instead of being silently dropped. */
export interface VerdictHistoryEntry {
	stage: GrainStage;
	band: string;
	worst_intent_risk: number | null;
	computed_at: string | null;
	run_id: string | null;
	/** Distinct detectors the stage's rollup drew on — CUMULATIVE by stage,
	 * because each stage's readiness is recomputed over the run-resolved merge
	 * of every earlier grain (engine `_resolve_runs`): the growing count is
	 * exactly WHY a later snapshot supersedes. null when the caller passed no
	 * evidence or the row's stage is unknown. */
	signals: number | null;
}

/** Pipeline order for cumulative evidence attribution; unknown is excluded. */
const STAGE_ORDER: Record<GrainStage, number> = {
	add_source: 0,
	catalog: 1,
	operating_model: 2,
	unknown: -1,
};

/** Project a target's coexisting readiness rows into the labeled history.
 * `evidenceRows` (optional) are the UNMERGED entropy-object rows for the same
 * target — their grain bits attribute each detector to a stage, and a history
 * row counts every detector at or below its own stage (cumulative — the scope
 * its rollup was actually computed over). */
export function projectVerdictHistory(
	readinessRows: readonly (GrainRow & {
		band: string | null;
		worstIntentRisk: number | null;
		runId: string | null;
	})[],
	evidenceRows: readonly (GrainRow & { detectorId: string | null })[] = [],
): VerdictHistoryEntry[] {
	function signalsAtOrBelow(stage: GrainStage): number | null {
		if (evidenceRows.length === 0 || stage === "unknown") return null;
		const cap = STAGE_ORDER[stage];
		const detectors = new Set<string>();
		for (const e of evidenceRows) {
			if (e.detectorId === null) continue;
			const order = STAGE_ORDER[stageOfRow(e)];
			// Legacy rows without grain bits rank as add_source-era evidence.
			if ((order === -1 ? 0 : order) <= cap) detectors.add(e.detectorId);
		}
		return detectors.size;
	}
	return (
		readinessRows
			.map((r) => {
				const stage = stageOfRow(r);
				return {
					stage,
					band: r.band ?? "",
					worst_intent_risk: r.worstIntentRisk ?? null,
					computed_at: r.computedAt?.toISOString() ?? null,
					// run_id is the per-snapshot discriminator (catalog-grain views
					// carry no session_id post-DAT-506).
					run_id: r.runId ?? null,
					signals: signalsAtOrBelow(stage),
				};
			})
			// ISO strings sort chronologically; null → "" sorts before any real
			// timestamp (oldest first).
			.sort((a, b) => (a.computed_at ?? "").localeCompare(b.computed_at ?? ""))
	);
}

/** Zod mirror of {@link VerdictHistoryEntry} for the tools' output schemas. */
export const VerdictHistorySchema = z.object({
	stage: z.string(),
	band: z.string(),
	worst_intent_risk: z.number().nullable(),
	computed_at: z.string().nullable(),
	run_id: z.string().nullable(),
	signals: z.number().nullable(),
});
