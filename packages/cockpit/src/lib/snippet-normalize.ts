// SQL Knowledge Base normalization primitives — a byte-compatible TS port of the
// engine's `dataraum.query.snippet_utils` (`normalize_sql` + `determine_usage_type`).
//
// These drive snippet REUSE classification (DAT-484): the query sub-agent's
// resolver compares the model's SQL against a stored snippet's SQL under
// `normalizeSql` equality — equal ⇒ exact_reuse (substitute the validated SQL),
// differ ⇒ adapted. The normalization MUST match the engine exactly (strip +
// lowercase + collapse-whitespace) or `exact_reuse` silently degrades to
// `adapted` and the reuse signal loses meaning. Pure functions, no DB —
// unit-tested as the byte-compat oracle against `test_snippet_utils.py`.
//
// `normalize_expression` is deliberately NOT ported: it is a producer-side
// formula-match key used by the engine GraphAgent, not on the consumer path.

/** Normalize SQL for comparison: strip, lowercase, collapse runs of whitespace. */
export function normalizeSql(sql: string): string {
	return sql.trim().toLowerCase().replace(/\s+/g, " ");
}

/**
 * Cockpit-side reuse canonicalization — NOT part of the engine byte-compat
 * contract above (DAT-485). The query sub-agent addresses tables as
 * `lake.<layer>.<name>`, but the stored `graph:%` snippets use BARE table names
 * (the engine GraphAgent generated them under a `USE lake.typed`). So a model's
 * reconstruction would never `normalizeSql`-match a stored snippet on the table
 * reference alone, and `exact_reuse` would never fire. This strips a leading
 * `lake.<layer>.` qualifier from any table reference so a qualified reuse matches
 * the bare stored form. It is applied ONLY to the reuse-equality DECISION (the
 * classification) — never to the SQL that actually runs (the qualified form is
 * what resolves in the cockpit's CTE execution context). Layered ON TOP of
 * `normalizeSql`, leaving the engine-compat primitive untouched.
 */
export function canonicalizeForReuse(sql: string): string {
	return sql.replace(/\blake\.[A-Za-z_]\w*\./gi, "");
}

/** How a generated SQL step relates to the snippet it referenced. */
export type UsageType = "exact_reuse" | "adapted" | "newly_generated";

/**
 * Classify a generated step against the snippet it referenced, deterministically:
 * - no snippet provided ⇒ `newly_generated`
 * - normalized SQL equal ⇒ `exact_reuse`
 * - otherwise ⇒ `adapted`
 */
export function determineUsageType(
	generatedSql: string,
	providedSnippetSql: string | null,
): UsageType {
	if (providedSnippetSql === null) return "newly_generated";
	return normalizeSql(generatedSql) === normalizeSql(providedSnippetSql)
		? "exact_reuse"
		: "adapted";
}
