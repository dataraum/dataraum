// Canonical SQL for snippet identity (DAT-485, the comparison fix).
//
// Snippet sameness can't be decided by string normalization — an LLM writes the
// same computation slightly differently each time (alias, spacing, keyword case,
// operand order, table qualifier). The robust key is an AST round-trip: parse the
// SQL and re-render it in a canonical form, so cosmetic variance collapses while
// real structural/identifier differences survive. This is exactly what the engine
// does for view DDL (`core/sql_normalize.canonical_sql`, sqlglot) and the retired
// MCP `cte_parser` did for snippets.
//
// We use polyglot (@polyglot-sql/sdk — a Rust SQL parser via WASM, DuckDB dialect)
// for the TS side. A cross-language spike (packages/cockpit/spikes/dat485-canonical)
// proved polyglot's canonical form is BYTE-IDENTICAL to the engine's sqlglot over
// the representative snippet shapes — so a Python-produced snippet and a TS-matched
// one share one identity without binding a single engine into both.
//
// Two known, deferred tail cases (the spike surfaced them): the output alias (fixed
// by steering the prompt to a constant `AS value`) and commutative `WHERE a AND b`
// vs `b AND a` (DAT-483 follow-up — AST round-trip preserves operand order).
//
// `lake.<layer>.` qualifier strip rides on top: the cockpit addresses
// `lake.typed.<name>` while stored snippets are bare, and the AST preserves
// identifiers, so we strip the qualifier for the comparison only (never the SQL
// that runs). Layered over the engine-byte-compat primitives in snippet-normalize.

import * as Polyglot from "@polyglot-sql/sdk";

import { canonicalizeForReuse, normalizeSql } from "./snippet-normalize";

// polyglot is WASM — `init()` must resolve once before transpile. Memoize it so a
// burst of canonicalSql() calls shares one initialization; a failed init clears
// the memo so a later call can retry (and the catch below degrades gracefully).
let initPromise: Promise<unknown> | null = null;
function ensureInit(): Promise<unknown> {
	if (!initPromise) {
		initPromise = Promise.resolve(Polyglot.init()).catch((err) => {
			initPromise = null;
			throw err;
		});
	}
	return initPromise;
}

/**
 * Canonical SQL for snippet-identity comparison: polyglot AST round-trip
 * (collapses case / whitespace / keyword-casing / formatting) + the
 * `lake.<layer>.` qualifier strip.
 *
 * FAIL-SOFT: on any polyglot init/parse failure it falls back to the string
 * normalizer (`normalizeSql ∘ canonicalizeForReuse`). A query polyglot can't
 * parse then only weakens the reuse TAG (it won't match unless the other side
 * also fell back to the same string form) — it never throws, so a weird query
 * never breaks the answer. Comparison is self-consistent because both sides of a
 * compare run through this same function.
 */
export async function canonicalSql(sql: string): Promise<string> {
	try {
		await ensureInit();
		const duckdb = Polyglot.Dialect.DuckDB;
		const result = Polyglot.transpile(sql, duckdb, duckdb);
		if (result.success && result.sql != null) {
			const rendered = Array.isArray(result.sql)
				? result.sql.join(" ")
				: result.sql;
			return canonicalizeForReuse(rendered);
		}
	} catch {
		// fall through to the string fallback
	}
	return normalizeSql(canonicalizeForReuse(sql));
}

/** True when two SQL fragments are the same snippet under canonicalization. */
export async function sqlEquivalent(a: string, b: string): Promise<boolean> {
	const [ca, cb] = await Promise.all([canonicalSql(a), canonicalSql(b)]);
	return ca === cb;
}
