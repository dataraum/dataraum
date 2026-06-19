// Grain-safety guard for the answer sub-agent's GROUP BY (DAT-538 P2).
//
// The dimension catalog (DAT-536) marks each candidate slice axis `grain_safe` or
// not: a non-grain-safe axis is finer than (or fans out) the fact grain, so grouping
// by it silently biases the aggregate (e.g. SUM over a near-1:1 column double-counts
// nothing but reports per-row noise as a "group"). The <dimensions> context block
// (P1) tells the agent which axes are safe; THIS is the deterministic teeth: before
// run_steps executes the composed CTE, we parse it with DuckDB's OWN parser
// (`json_serialize_sql` — the same parser that will run it, so no dialect drift, no
// extra dependency) and refuse any GROUP BY over a column the catalog flagged
// grain_safe=false. The agent still authors the SQL; this only rejects the known
// fan-out axes and points the user to the Stage chat to curate the catalog.
//
// Scope (v1): we reject ONLY columns the catalog explicitly marks grain_safe=false
// (a definitive, grounded fan-out signal). Bare columns the catalog doesn't know,
// GROUP BY expressions (date_trunc(...), etc.), and GROUP BY ordinals all PASS —
// blocking those would be a false-positive minefield, and a column the catalog
// hasn't blessed is promoted to a grain-safe axis via the Stage dimension teach
// (DAT-538 P3), not refused here. Fail-OPEN on any parse/read failure: a guard that
// can't reason about the SQL must never block a query run_steps would otherwise run.

import { metadataDb } from "../db/metadata/client";
import { currentSliceDefinitions } from "../db/metadata/schema";
import { getLakeConnection } from "../duckdb/lake";
import { readerToResult } from "../duckdb/query-result";

/**
 * The bare GROUP BY column names in a `json_serialize_sql` parse tree, lowercased.
 * Walks EVERY `group_expressions` array (outer query + every CTE/subquery) and keeps
 * only `COLUMN_REF` items (the bare grouped columns) — a qualified `s.region` yields
 * its last name part (`region`); functions/constants (expressions, ordinals) are
 * skipped, since those aggregate by construction and aren't catalog axes. Pure.
 */
export function extractGroupedColumns(tree: unknown): string[] {
	const out = new Set<string>();
	const visit = (node: unknown): void => {
		if (Array.isArray(node)) {
			for (const item of node) visit(item);
			return;
		}
		if (!node || typeof node !== "object") return;
		const obj = node as Record<string, unknown>;
		const groups = obj.group_expressions;
		if (Array.isArray(groups)) {
			for (const g of groups) {
				if (
					g &&
					typeof g === "object" &&
					(g as Record<string, unknown>).class === "COLUMN_REF"
				) {
					const names = (g as Record<string, unknown>).column_names;
					if (Array.isArray(names) && names.length > 0) {
						const last = names[names.length - 1];
						if (typeof last === "string" && last.length > 0)
							out.add(last.toLowerCase());
					}
				}
			}
		}
		for (const v of Object.values(obj)) visit(v);
	};
	visit(tree);
	return [...out];
}

/** The grouped columns that the catalog flagged as non-grain-safe fan-out axes. */
export function findGrainViolations(
	groupedColumns: string[],
	unsafeAxes: Set<string>,
): string[] {
	return groupedColumns.filter((c) => unsafeAxes.has(c));
}

/**
 * The catalogued column names (lowercased) that are NON-grain-safe AND not also a
 * grain-safe axis somewhere — a name safe on one table is given the benefit of the
 * doubt. Reads the promoted head's `current_slice_definitions`. Empty set → the
 * guard is a no-op (nothing catalogued as fan-out yet).
 */
export async function loadUnsafeAxes(): Promise<Set<string>> {
	const rows = await metadataDb
		.select({
			columnName: currentSliceDefinitions.columnName,
			grainSafe: currentSliceDefinitions.grainSafe,
		})
		.from(currentSliceDefinitions);

	const safe = new Set<string>();
	const unsafe = new Set<string>();
	for (const r of rows) {
		if (!r.columnName) continue;
		const name = r.columnName.toLowerCase();
		if (r.grainSafe) safe.add(name);
		else unsafe.add(name);
	}
	for (const name of safe) unsafe.delete(name);
	return unsafe;
}

function refusalMessage(violations: string[]): string {
	const axes = violations.map((v) => `"${v}"`).join(", ");
	return (
		`Refused: this query groups by a non-grain-safe axis (${axes}). That column is ` +
		`finer than the fact grain, so grouping by it fans out the rows and silently ` +
		`biases the aggregate. Group by a grain-safe axis listed in <dimensions> ` +
		`instead. If ${axes} really should be a valid analysis axis, curate the ` +
		`dimension catalog in the Stage chat (teach the axis), then re-ask here.`
	);
}

/**
 * Deterministic grain-safety gate over the composed CTE statement run_steps is about
 * to execute. Returns a refusal message when a GROUP BY targets a catalogued
 * non-grain-safe axis, or null when the query is grain-safe (or the guard can't
 * reason about it — fail-open). Uses DuckDB's own parser via the memoized read-only
 * lake connection (`json_serialize_sql` is pure parsing; it touches no tables).
 */
export async function checkGrainSafety(
	composedSql: string,
	unsafeAxes: Set<string>,
): Promise<string | null> {
	if (unsafeAxes.size === 0) return null;
	try {
		const conn = await getLakeConnection();
		// $1::VARCHAR — json_serialize_sql requires a VARCHAR arg and rejects an
		// untyped bind parameter ("first argument must be a VARCHAR").
		const reader = await conn.runAndReadAll(
			"SELECT json_serialize_sql($1::VARCHAR) AS tree",
			[composedSql],
		);
		const raw = readerToResult(reader).rows[0]?.tree;
		if (typeof raw !== "string") return null;
		const tree = JSON.parse(raw) as { error?: unknown };
		// json_serialize_sql sets error:false on a clean parse; anything else (a parse
		// failure) → fail-open and let run_steps surface the real SQL error.
		if (tree.error !== false) return null;
		const violations = findGrainViolations(
			extractGroupedColumns(tree),
			unsafeAxes,
		);
		return violations.length > 0 ? refusalMessage(violations) : null;
	} catch (err) {
		console.warn(`[cockpit] grain-safety guard skipped (parse failed): ${err}`);
		return null;
	}
}
