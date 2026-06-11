// Grain precedence for the multi-head current_* entropy views (DAT-509).
//
// `current_entropy_objects` / `current_entropy_readiness` are multi-grain
// (ADR-0008 + DAT-442): one target can carry a row sealed by the add_source
// TABLE head and, once begin_session / operating_model run, a second row
// sealed by a SESSION-grain head. The view dedupes only between the two
// session-grain heads (detect vs operating_model, latest-promoted-wins) — the
// table-head row coexists, and `entropy_readiness.session_id` is NOT NULL even
// on add_source rows, so a session-scoped WHERE does not exclude it either.
// An unpinned `.limit(1)` therefore picks an arbitrary grain, and a
// `via_table_head` pin shows the stale add_source verdict forever (missing
// re-adjudications like temporal_behavior's session pass and the
// operating_model cross_table_consistency fan-out).
//
// The engine ranks session-grain over table-grain at every run-resolved read
// (entropy/core/storage.py `load_for_tables`); these helpers mirror that rank
// at the cockpit's read edge. SQL stays grain-unpinned; the pick is explicit,
// pure, and unit-tested (the DAT-474 deterministic-pick rule).

/** The head discriminators + recency every multi-grain view row carries. */
export interface GrainRow {
	viaTableHead: boolean | null;
	viaSessionHead: boolean | null;
	viaOperatingModelHead: boolean | null;
	computedAt: Date | null;
}

/** Session-grain = sealed by a begin_session detect or operating_model head. */
function isSessionGrain(row: GrainRow): boolean {
	return row.viaSessionHead === true || row.viaOperatingModelHead === true;
}

/** The pipeline stage a snapshot row was sealed by (DAT-513). The pick is only
 * evaluable if the caller can SEE it — every surface that shows a picked
 * verdict labels it with this stage. `operating_model` outranks
 * `session_detect` in the label when both bits are set (they never are today:
 * a row is sealed by exactly one head; the order is defensive). */
export type GrainStage =
	| "add_source"
	| "session_detect"
	| "operating_model"
	| "unknown";

export function stageOfRow(row: GrainRow): GrainStage {
	if (row.viaOperatingModelHead === true) return "operating_model";
	if (row.viaSessionHead === true) return "session_detect";
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
 * Pick THE current row for one target: the latest session-grain row when any
 * exists (a session's re-roll supersedes the add_source verdict — it was built
 * over the run-resolved merge of both grains), else the table-head row, else —
 * for rows that predate the discriminators or carry none — the latest row.
 */
export function pickCurrentRow<T extends GrainRow>(
	rows: readonly T[],
): T | undefined {
	const session = rows.filter(isSessionGrain);
	if (session.length > 0) return latest(session);
	const table = rows.filter((r) => r.viaTableHead === true);
	if (table.length > 0) return latest(table);
	return latest(rows);
}

/**
 * Merge a multi-grain evidence row set: one row per detector, session-grain
 * winning over table-grain per detector (add_source-only detectors keep their
 * table-head row; re-adjudicated detectors show the session verdict). Output
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
	session_id: string | null;
	run_id: string | null;
	/** Distinct detectors backing that run's evidence — shows WHY a later
	 * snapshot supersedes (more evidence), null when the caller passed none. */
	signals: number | null;
}

/** Project a target's coexisting readiness rows into the labeled history.
 * `evidenceRows` (optional) are the UNMERGED entropy-object rows for the same
 * target — used only to count distinct detectors per run. */
export function projectVerdictHistory(
	readinessRows: readonly (GrainRow & {
		band: string | null;
		worstIntentRisk: number | null;
		sessionId: string | null;
		runId: string | null;
	})[],
	evidenceRows: readonly {
		runId: string | null;
		detectorId: string | null;
	}[] = [],
): VerdictHistoryEntry[] {
	const detectorsByRun = new Map<string, Set<string>>();
	for (const e of evidenceRows) {
		if (e.runId === null || e.detectorId === null) continue;
		const set = detectorsByRun.get(e.runId);
		if (set === undefined) detectorsByRun.set(e.runId, new Set([e.detectorId]));
		else set.add(e.detectorId);
	}
	return readinessRows
		.map((r) => ({
			stage: stageOfRow(r),
			band: r.band ?? "",
			worst_intent_risk: r.worstIntentRisk ?? null,
			computed_at: r.computedAt?.toISOString() ?? null,
			session_id: r.sessionId ?? null,
			run_id: r.runId ?? null,
			signals:
				evidenceRows.length === 0
					? null
					: (detectorsByRun.get(r.runId ?? "")?.size ?? 0),
		}))
		.sort((a, b) => (a.computed_at ?? "").localeCompare(b.computed_at ?? ""));
}
