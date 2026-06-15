// The begin_session readiness key grammar — the TS mirror of the engine's
// `entropy/models.py` (`relationship_target_key` / `parse_relationship_target`),
// the `table:{name}` target the table-scoped detectors write (`engine.py` /
// `readiness.py`, DAT-415), and `storage/snapshot_head.py`
// (`catalog_head_target`). begin_session writes relationship readiness/evidence
// under a `relationship:{from_col}::{to_col}` target (DAT-408) and table-grain
// readiness under a `table:{table_name}` target (DAT-415), and seals the whole
// run under the single workspace `catalog` head (DAT-506); the readers here must
// build/parse those exact strings, so they live in one place the same way the
// engine keeps one function per string.

const REL_PREFIX = "relationship:";
// `::` separates the two column UUIDs — a single `-` would be ambiguous inside a
// UUID. Must match `_REL_SEP` in the engine.
const REL_SEP = "::";
const TABLE_PREFIX = "table:";

/** The stable `relationship:{from_col}::{to_col}` readiness/evidence target. */
export function relationshipTargetKey(
	fromColumnId: string,
	toColumnId: string,
): string {
	return `${REL_PREFIX}${fromColumnId}${REL_SEP}${toColumnId}`;
}

/** Inverse of {@link relationshipTargetKey}. Returns null for any non-relationship
 * or malformed target (so a stray `column:`/`table:` row degrades, never throws). */
export function parseRelationshipTarget(
	target: string,
): { fromColumnId: string; toColumnId: string } | null {
	if (!target.startsWith(REL_PREFIX)) return null;
	const parts = target.slice(REL_PREFIX.length).split(REL_SEP);
	if (parts.length !== 2 || !parts[0] || !parts[1]) return null;
	return { fromColumnId: parts[0], toColumnId: parts[1] };
}

/** The stable `table:{table_name}` table-grain readiness/evidence target (DAT-415).
 * begin_session's `dimension_coverage` rolls up to this key; mirrors the engine's
 * inline `f"table:{table.table_name}"` (`engine.py` / `readiness.py`). Keyed on the
 * table NAME (not id), like the column target `column:{table}.{column}`. */
export function tableTargetKey(tableName: string): string {
	return `${TABLE_PREFIX}${tableName}`;
}

/** The snapshot-head target sealing a begin_session / operating_model run — the
 * constant workspace `catalog` head (DAT-506). The workspace IS the schema, so
 * there is ONE catalog head per stage (no session axis); relationship, table-grain
 * and lifecycle readiness all resolve through it. Mirrors the engine's
 * `catalog_head_target()` (`storage/snapshot_head.py`). */
export const CATALOG_HEAD_TARGET = "catalog";

export function catalogHeadTarget(): string {
	return CATALOG_HEAD_TARGET;
}
