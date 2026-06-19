// Grain-aware GROUP BY caveat for the answer sub-agent (DAT-538).
//
// NOT a gate. The answer agent groups by what the user ASKED — a near-unique GROUP
// BY ("revenue per transaction_id") is usually a legitimate per-row listing, not a
// mistake, so refusing it would block a query the user wanted. But when a question
// is ambiguous and the agent groups FINER than the user likely meant, the result is
// a per-row dump masquerading as an aggregate. The system's contract is
// inform-don't-block (like the readiness band): we still run the SQL, and attach a
// CAVEAT the agent surfaces so the user can re-ask for a summary if that wasn't the
// intent.
//
// The signal is the column's CARDINALITY, not a catalog flag: a GROUP BY over a
// near-unique column (cardinality_ratio ≈ 1 — distinct count approaching the row
// count) groups one-row-per-value. The engine already computes this in column
// statistics; the agent does not know distinct counts, so a deterministic check is
// strictly more reliable than asking the model to self-assess. We parse the composed
// CTE with DuckDB's own parser (`json_serialize_sql` — same parser that runs it, no
// dialect drift, no extra dependency). Fail-OPEN: a parse/read failure yields no
// caveat — it must never interfere with a query that otherwise runs.

import { eq } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import { columns, currentStatisticalProfiles } from "../db/metadata/schema";
import { getLakeConnection } from "../duckdb/lake";
import { readerToResult } from "../duckdb/query-result";

// cardinality_ratio = distinct_count / row_count. At/above this a column is treated
// as near-unique (an id / per-row key), so grouping by it lists rather than
// aggregates. High on purpose — the caveat should fire only on definitive per-row
// keys, never on a legitimate medium-cardinality dimension (200 regions over 1M rows
// is ~0.0002 and must stay silent).
const NEAR_UNIQUE_RATIO = 0.9;

/**
 * The bare GROUP BY column names in a `json_serialize_sql` parse tree, lowercased.
 * Walks EVERY `group_expressions` array (outer query + every CTE/subquery) and keeps
 * only `COLUMN_REF` items — a qualified `s.region` yields its last name part
 * (`region`); functions/constants (expressions, ordinals) are skipped, since those
 * aggregate by construction and aren't bare column keys. Pure.
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

/** The grouped columns that are near-unique (per-row keys), worth a caveat. */
export function findNearUniqueGroupings(
	groupedColumns: string[],
	nearUniqueColumns: Set<string>,
): string[] {
	return groupedColumns.filter((c) => nearUniqueColumns.has(c));
}

/**
 * The column names (lowercased) whose `cardinality_ratio` is at/above
 * {@link NEAR_UNIQUE_RATIO} on the promoted head — the per-row keys. Joins the
 * statistical profiles to column names; a name shared across tables is near-unique
 * if ANY same-named column is (conservative toward surfacing the caveat). Empty set
 * → no caveat possible (the check becomes a no-op).
 */
export async function loadNearUniqueColumns(): Promise<Set<string>> {
	const rows = await metadataDb
		.select({
			columnName: columns.columnName,
			cardinalityRatio: currentStatisticalProfiles.cardinalityRatio,
		})
		.from(currentStatisticalProfiles)
		.innerJoin(
			columns,
			eq(columns.columnId, currentStatisticalProfiles.columnId),
		);

	const nearUnique = new Set<string>();
	for (const r of rows) {
		if (!r.columnName) continue;
		if (r.cardinalityRatio !== null && r.cardinalityRatio >= NEAR_UNIQUE_RATIO)
			nearUnique.add(r.columnName.toLowerCase());
	}
	return nearUnique;
}

function caveat(groupings: string[]): string {
	const cols = groupings.map((g) => `"${g}"`).join(", ");
	const isAre = groupings.length > 1 ? "are" : "is";
	return (
		`Note: this query groups by ${cols}, which ${isAre} near-unique (about one ` +
		`row per value). The result is a per-row listing, not an aggregated summary. ` +
		`If you wanted a summary, group by a coarser dimension instead.`
	);
}

/**
 * A grain caveat for the composed CTE statement, or null when every GROUP BY is over
 * a coarse-enough column (or the guard can't reason about the SQL — fail-open). Does
 * NOT block: the caller still runs the query and surfaces this note to the user.
 * Uses DuckDB's own parser via the memoized read-only lake connection
 * (`json_serialize_sql` is pure parsing; it touches no tables).
 */
export async function computeGrainNote(
	composedSql: string,
	nearUniqueColumns: Set<string>,
): Promise<string | null> {
	if (nearUniqueColumns.size === 0) return null;
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
		// json_serialize_sql sets error:false on a clean parse; anything else → no
		// caveat (the SQL's own error, if any, surfaces through run_steps).
		if (tree.error !== false) return null;
		const flagged = findNearUniqueGroupings(
			extractGroupedColumns(tree),
			nearUniqueColumns,
		);
		return flagged.length > 0 ? caveat(flagged) : null;
	} catch (err) {
		console.warn(`[cockpit] grain-note check skipped (parse failed): ${err}`);
		return null;
	}
}
