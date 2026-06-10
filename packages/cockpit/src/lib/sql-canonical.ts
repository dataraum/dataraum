// Canonical SQL for snippet identity (DAT-485 comparison fix; DAT-492 tail).
//
// Snippet sameness can't be decided by string normalization — an LLM writes the
// same computation slightly differently each time (alias, spacing, keyword case,
// operand order, identifier quoting, table qualifier). The robust key is an AST
// round-trip: parse the SQL, normalize the tree, and re-render it canonically, so
// cosmetic variance collapses while real structural/identifier differences survive.
//
// We use polyglot (@polyglot-sql/sdk — a Rust SQL parser via WASM, DuckDB dialect)
// for the TS side. Every comparison canonicalizes BOTH sides with polyglot, so
// reuse classification is self-consistent regardless of how a snippet was produced.
//
// SELF-CONTAINED — no cross-engine key. The engine's sqlglot canonical
// (`core/sql_normalize`) is used only for view DDL and the future snippet de-dup
// pass (DAT-493); it re-canonicalizes stored SQL in-situ with its own single
// engine, so polyglot (here) and sqlglot (there) never need to byte-agree. (The
// DAT-485 stress-test proved two native engines do NOT share a byte-for-byte key;
// the DAT-492 refine settled that nothing requires them to.)
//
// The AST normalization beyond a bare round-trip (DAT-492):
//   1. Commutative-operand sort — `WHERE b AND a` ≡ `WHERE a AND b` (and OR / + / *
//      chains). The parse tree preserves operand order; we flatten each commutative
//      chain, sort the operands by a stable key, and rebuild, so order stops
//      fragmenting identity. NON-commutative operators (-, /, comparisons) are left
//      untouched — `a - b` must NOT equal `b - a`.
//   2. Identifier normalization — `"Credit"` ≡ `credit`. DuckDB folds identifier
//      case, so we lowercase identifier names, and polyglot preserves quote style,
//      so we also drop the quote when the folded spelling is unambiguous (bare-
//      identifier charset). Genuinely quote-requiring names (spaces, etc.) keep their
//      quotes (still case-folded), and the generator re-quotes reserved words on its
//      own, so the key never goes invalid. Only identifier nodes are touched — string
//      literals keep their case (`'Sale'` ≠ `'sale'`).
//
// `lake.<layer>.` qualifier strip rides on top: the cockpit addresses
// `lake.typed.<name>` while stored snippets are bare, and the generated SQL
// preserves the qualifier, so we strip it for the comparison only (never the SQL
// that runs). Layered over the engine-byte-compat primitives in snippet-normalize.

import * as Polyglot from "@polyglot-sql/sdk";

import { canonicalizeForReuse, normalizeSql } from "./snippet-normalize";

// polyglot is WASM — `init()` must resolve once before parse/generate. Memoize it
// so a burst of canonicalSql() calls shares one initialization; a failed init clears
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

// --- AST normalization (DAT-492) -------------------------------------------------
//
// The polyglot AST is plain tagged-union JSON: every expression is `{ <tag>: data }`
// (one key), e.g. `{ and: { left, right, ... } }`, and identifiers are bare structs
// `{ name, quoted, ... }`. We normalize that JSON directly — `Polyglot.generate`
// accepts the same shape — rather than via the `ast.*` helpers (which expect a
// different wrapper). Everything below is typed against `unknown` and narrowed.

/**
 * Binary operators whose operands may be reordered without changing meaning.
 * `sub`/`div`/`mod`, the comparisons, and `concat` (the polyglot tag for `||`)
 * are deliberately absent — none is commutative.
 */
const COMMUTATIVE_TAGS = new Set(["and", "or", "add", "mul"]);
/** Identifier spelling that means the same thing bare or double-quoted. */
const BARE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** A parsed identifier struct: has a string `name` and a boolean `quoted` flag. */
function isIdentifierNode(
	value: unknown,
): value is Record<string, unknown> & { name: string; quoted: boolean } {
	return (
		isRecord(value) &&
		typeof value.name === "string" &&
		typeof value.quoted === "boolean"
	);
}

/** The single tag of a tagged-union expression node, or null if not one. */
function soleTag(value: Record<string, unknown>): string | null {
	const keys = Object.keys(value);
	return keys.length === 1 ? keys[0] : null;
}

/**
 * Deterministic serialization for operand ordering: object keys are sorted, so two
 * structurally-equal (already-normalized) operands serialize identically regardless
 * of key order. Used only as a sort key, never rendered.
 */
function stableKey(value: unknown): string {
	return JSON.stringify(value, (_key, val) =>
		isRecord(val)
			? Object.fromEntries(
					Object.keys(val)
						.sort()
						.map((k) => [k, val[k]]),
				)
			: val,
	);
}

/** Collect the operands of a same-tag commutative chain, each normalized. */
function flattenCommutative(
	node: Record<string, unknown>,
	tag: string,
	out: unknown[],
): unknown[] {
	const data = node[tag];
	if (isRecord(data)) {
		for (const side of [data.left, data.right]) {
			if (isRecord(side) && soleTag(side) === tag) {
				flattenCommutative(side, tag, out);
			} else {
				out.push(normalizeNode(side));
			}
		}
	}
	return out;
}

/**
 * Rebuild a commutative chain as a left-nested tree of the given operands. Only the
 * structural + comment fields are emitted; any other binary-op data is intentionally
 * dropped (it is cosmetic for our key, and a generate failure would fail soft anyway).
 */
function rebuildLeftNested(tag: string, operands: unknown[]): unknown {
	let acc = operands[0];
	for (let i = 1; i < operands.length; i++) {
		acc = {
			[tag]: {
				left: acc,
				right: operands[i],
				left_comments: [],
				operator_comments: [],
				trailing_comments: [],
			},
		};
	}
	return acc;
}

/** Recursively canonicalize a polyglot AST node (commutative sort + quote drop). */
function normalizeNode(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(normalizeNode);
	if (!isRecord(value)) return value;

	if (isIdentifierNode(value)) {
		// DuckDB folds identifier case, so lowercase the name (`"Credit"` ≡ `credit`);
		// drop the quote when the folded spelling is a bare identifier. Only identifier
		// structs are touched — string literals are NOT identifier nodes, so literal
		// case is preserved (`'Sale'` stays `'Sale'`).
		const name = value.name.toLowerCase();
		return {
			...value,
			name,
			quoted: BARE_IDENTIFIER.test(name) ? false : value.quoted,
		};
	}

	const tag = soleTag(value);
	if (tag !== null && COMMUTATIVE_TAGS.has(tag)) {
		const operands = flattenCommutative(value, tag, []);
		if (operands.length >= 2) {
			operands.sort((a, b) => {
				const ka = stableKey(a);
				const kb = stableKey(b);
				return ka < kb ? -1 : ka > kb ? 1 : 0;
			});
			return rebuildLeftNested(tag, operands);
		}
	}

	const out: Record<string, unknown> = {};
	for (const [key, child] of Object.entries(value)) {
		out[key] = normalizeNode(child);
	}
	return out;
}

/**
 * Canonical SQL for snippet-identity comparison: polyglot parse → AST normalize
 * (commutative-operand sort + identifier-quote drop, DAT-492) → render, then the
 * `lake.<layer>.` qualifier strip.
 *
 * FAIL-SOFT: on any polyglot init/parse/generate failure it falls back to the
 * string normalizer (`normalizeSql ∘ canonicalizeForReuse`). A query polyglot can't
 * parse then only weakens the reuse TAG (it won't match unless the other side also
 * fell back to the same string form) — it never throws, so a weird query never
 * breaks the answer. Comparison is self-consistent because both sides of a compare
 * run through this same function.
 */
export async function canonicalSql(sql: string): Promise<string> {
	try {
		await ensureInit();
		const duckdb = Polyglot.Dialect.DuckDB;
		const parsed = Polyglot.parse(sql, duckdb);
		if (parsed.success && Array.isArray(parsed.ast)) {
			const normalized = (parsed.ast as unknown[]).map(normalizeNode);
			const generated = Polyglot.generate(normalized, duckdb);
			if (generated.success && generated.sql != null) {
				const rendered = Array.isArray(generated.sql)
					? generated.sql.join(" ")
					: generated.sql;
				return canonicalizeForReuse(rendered);
			}
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
