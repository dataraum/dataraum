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
