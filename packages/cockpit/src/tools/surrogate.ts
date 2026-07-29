/**
 * Mint-owned surrogate join keys, mirrored from the engine (DAT-277, DAT-878).
 *
 * The engine cures an LLM-confirmed composite key by projecting ONE deterministic
 * hash column onto both typed tables and persisting an ordinary single-column
 * relationship on the pair. Those columns are machinery: nothing a user named, and
 * nothing the answer agent should be offered as an analysable attribute. They are
 * real physical columns on the typed tables and ride into every enriched view
 * through its `f.*` fact passthrough, so both of the cockpit's schema paths — the
 * enriched `DESCRIBE` and the typed metadata read — see them.
 *
 * This is a hand-mirror of `packages/engine/src/dataraum/analysis/relationships/
 * surrogate.py` (`SURROGATE_PREFIX` / `is_surrogate_column`) — a cross-PACKAGE
 * convention, like the Temporal contracts. If the engine changes the prefix, this
 * changes with it.
 *
 * The test is a strict PREFIX match, matching the engine exactly. A substring match
 * would be a guess about user data (a real column may contain the text), which
 * neither package does.
 */

/** The engine's minted-surrogate column-name prefix. */
export const SURROGATE_PREFIX = "_sk__";

/** Whether `columnName` is a mint-owned surrogate join key. */
export function isSurrogateColumn(columnName: string): boolean {
	return columnName.startsWith(SURROGATE_PREFIX);
}
