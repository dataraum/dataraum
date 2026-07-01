// Canonical SQL for snippet identity (DAT-485 comparison fix; DAT-492 tail;
// DAT-654 engine swap).
//
// Snippet sameness can't be decided by string normalization — an LLM writes the
// same computation slightly differently each time (alias, spacing, keyword case,
// operand order, identifier quoting, table qualifier). The robust key is the parse
// tree: parse the SQL, normalize the tree, and compare a stable serialization of it,
// so cosmetic variance collapses while real structural/identifier differences survive.
//
// We parse with DuckDB's OWN parser via `json_serialize_sql` (DAT-654, retiring the
// former `@polyglot-sql/sdk` WASM parser) — the exact parser that runs the SQL, so
// there is no dialect drift and no extra dependency (`grain-note.ts` uses the same
// call). The serialized AST is a stable, versioned representation of DuckDB's parse;
// the canonical KEY is the normalized tree itself (a stable JSON serialization), NOT
// re-rendered SQL — we never deserialize back, so the key never depends on a
// generator's formatting.
//
// SELF-CONTAINED — no cross-engine key. The engine's canonical (`core/sql_normalize`)
// re-canonicalizes stored SQL in-situ with its own parser, so the two sides never
// need to byte-agree. (DAT-485 proved two native engines do NOT share a byte-for-byte
// key; DAT-492 settled that nothing requires them to. DAT-654 moves both sides onto
// `json_serialize_sql`, but the independence still holds.)
//
// The normalization beyond a bare parse:
//   1. Strip `query_location` — the serialized tree carries a byte offset on every
//      node; two spellings of the same query differ only there until it is dropped.
//   2. Commutative-operand sort — `WHERE b AND a` ≡ `WHERE a AND b` (and OR, plus the
//      `+`/`*` operators). DuckDB flattens AND/OR into one n-ary `children` array; the
//      `+`/`*` operators are binary FUNCTIONs whose two `children` we sort in place.
//      NON-commutative operators (`-`, `/`, comparisons) are left untouched —
//      `a - b` must NOT equal `b - a`.
//   3. Identifier case-fold — `"Credit"` ≡ `credit`. DuckDB folds identifier case, and
//      `json_serialize_sql` emits the resolved identifier text in `column_names` /
//      `table_name` WITHOUT quote metadata, so lowercasing those collapses quote-and-
//      case variance in one step (a genuinely quote-requiring name like `"Weird Name"`
//      simply case-folds to `weird name` — still distinct from any other identifier).
//      String literals live in CONSTANT `value` nodes, untouched — `'Sale'` ≠ `'sale'`.
//   4. `lake.<layer>.` qualifier strip — the cockpit addresses `lake.typed.<name>`
//      while stored snippets are bare; a BASE_TABLE whose `catalog_name` is `lake` has
//      its catalog + schema cleared, so a qualified reference matches the bare stored
//      form. Applied to the comparison KEY only, never to the SQL that runs.

import type { DuckDBConnection } from "@duckdb/node-api";

import { readerToResult } from "../duckdb/query-result";
import { canonicalizeForReuse, normalizeSql } from "./snippet-normalize";

// --- tree normalization ----------------------------------------------------------
//
// A `json_serialize_sql` node is a plain object with a `class` discriminator
// (COLUMN_REF, FUNCTION, CONJUNCTION, CONSTANT, …) plus class-specific fields; the
// top-level result carries `error` (false on a clean parse). Everything below is
// typed against `unknown` and narrowed — the tree is a boundary value.

/** Operator FUNCTIONs (`is_operator`) whose two operands may be reordered. */
const COMMUTATIVE_OPERATORS = new Set(["+", "*"]);
/** CONJUNCTION `type`s whose n-ary operands may be reordered. */
const COMMUTATIVE_CONJUNCTIONS = new Set(["CONJUNCTION_AND", "CONJUNCTION_OR"]);

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Deterministic serialization: object keys are sorted, so two structurally-equal
 * (already-normalized) nodes serialize identically regardless of key order. Used
 * both as the commutative sort key and as the final canonical key.
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

/** Lowercase each string in an identifier-name array; anything else passes through. */
function lowerNames(value: unknown): unknown {
	return Array.isArray(value)
		? value.map((v) => (typeof v === "string" ? v.toLowerCase() : v))
		: value;
}

/** True for a commutative node whose `children` array may be sorted. */
function isCommutative(node: Record<string, unknown>): boolean {
	if (node.class === "FUNCTION") {
		return (
			node.is_operator === true &&
			typeof node.function_name === "string" &&
			COMMUTATIVE_OPERATORS.has(node.function_name)
		);
	}
	return (
		node.class === "CONJUNCTION" &&
		typeof node.type === "string" &&
		COMMUTATIVE_CONJUNCTIONS.has(node.type)
	);
}

/**
 * Recursively canonicalize a serialized node: drop `query_location`, case-fold
 * identifiers (`column_names` / `table_name`), strip the `lake.<layer>.` qualifier,
 * then sort the operands of any commutative node.
 */
function normalizeNode(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(normalizeNode);
	if (!isRecord(value)) return value;

	const out: Record<string, unknown> = {};
	for (const [key, child] of Object.entries(value)) {
		if (key === "query_location") continue; // byte offset — pure cosmetics
		out[key] = normalizeNode(child);
	}

	if (out.class === "COLUMN_REF") {
		out.column_names = lowerNames(out.column_names);
	}
	if (out.type === "BASE_TABLE") {
		if (typeof out.table_name === "string") {
			out.table_name = out.table_name.toLowerCase();
		}
		// `lake.<layer>.` addresses the same table as the bare stored form.
		if (String(out.catalog_name ?? "").toLowerCase() === "lake") {
			out.catalog_name = "";
			out.schema_name = "";
		} else if (typeof out.schema_name === "string") {
			out.schema_name = out.schema_name.toLowerCase();
		}
	}

	if (
		isCommutative(out) &&
		Array.isArray(out.children) &&
		out.children.length >= 2
	) {
		out.children = [...out.children].sort((a, b) => {
			const ka = stableKey(a);
			const kb = stableKey(b);
			return ka < kb ? -1 : ka > kb ? 1 : 0;
		});
	}
	return out;
}

/**
 * The canonical comparison key for a parsed statement. A clean parse
 * (`tree.error === false`) yields the stable serialization of the normalized tree;
 * anything else FAILS SOFT to the string normalizer (`normalizeSql ∘
 * canonicalizeForReuse`) — a query DuckDB can't parse only weakens the reuse signal
 * (it matches only if the other side also fell back to the same string form), never
 * throws. Both sides of a comparison run through this same function, so the decision
 * is self-consistent regardless of how a snippet was produced.
 *
 * Exported for the unit test, which parses real SQL through a bare in-memory DuckDB
 * and feeds the tree here — the exact key the runtime path computes.
 */
export function canonicalKey(tree: unknown, sql: string): string {
	if (isRecord(tree) && tree.error === false) {
		return stableKey(normalizeNode(tree));
	}
	return normalizeSql(canonicalizeForReuse(sql));
}

/**
 * Serialize one SQL string to its parse tree via DuckDB's own parser. `$1::VARCHAR`
 * — `json_serialize_sql` requires a VARCHAR arg and rejects an untyped bind param.
 * Returns the parsed JSON (which itself carries `error` on a parse failure), or null
 * if the call/JSON is unusable; `canonicalKey` fails soft on either.
 */
async function serialize(
	conn: DuckDBConnection,
	sql: string,
): Promise<unknown> {
	const reader = await conn.runAndReadAll(
		"SELECT json_serialize_sql($1::VARCHAR) AS tree",
		[sql],
	);
	const raw = readerToResult(reader).rows[0]?.tree;
	if (typeof raw !== "string") return null;
	try {
		return JSON.parse(raw);
	} catch {
		return null;
	}
}

/** True when two SQL fragments are the same snippet under canonicalization. */
export async function sqlEquivalent(a: string, b: string): Promise<boolean> {
	let treeA: unknown = null;
	let treeB: unknown = null;
	try {
		// Lazy import: `lake` pulls the node DuckDB binding + `#/config` (a server-only
		// boundary that validates env at load). Importing it dynamically here keeps the
		// pure `canonicalKey` path — and its unit test — free of that taint.
		const { withLakeConnection } = await import("../duckdb/lake");
		// One connection parses both — `json_serialize_sql` is pure parsing (touches
		// no tables), so the read-only lake connection is just a parser handle.
		[treeA, treeB] = await withLakeConnection(async (conn) => [
			await serialize(conn, a),
			await serialize(conn, b),
		]);
	} catch {
		// leave both null → both sides fall back to the string form below
	}
	return canonicalKey(treeA, a) === canonicalKey(treeB, b);
}
