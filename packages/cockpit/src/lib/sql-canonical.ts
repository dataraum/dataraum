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
// there is no dialect drift and no extra dependency. The serialized AST is a stable,
// versioned representation of DuckDB's parse; the canonical KEY is the normalized tree
// itself (a stable JSON serialization), NOT re-rendered SQL — we never deserialize
// back, so the key never depends on a generator's formatting.
//
// The parse runs on a PRIVATE in-memory DuckDB (see `getParser`), not the lake
// connection: `json_serialize_sql` touches no tables, so snippet identity must not
// depend on the lake being reachable — a transient lake outage must never silently
// degrade reuse to the weaker string fallback.
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
//   2. Commutative-operand flatten + sort — `WHERE b AND a` ≡ `WHERE a AND b` (and OR,
//      plus the `+`/`*` operators). DuckDB flattens AND/OR into one n-ary `children`
//      array, but leaves `+`/`*` as left-nested binary FUNCTIONs (`a+b+c` → `(a+b)+c`),
//      so we first flatten any same-operator child into the parent, THEN sort — making
//      `a+b+c` ≡ `c+b+a` ≡ `a+(b+c)`. NON-commutative operators (`-`, `/`, comparisons)
//      are left untouched — `a - b` must NOT equal `b - a`.
//   3. Identifier + alias case-fold — `"Credit"` ≡ `credit`, `AS "Value"` ≡ `AS value`.
//      DuckDB folds identifier/alias case, and `json_serialize_sql` emits the resolved
//      identifier text in `column_names` / `table_name` / `alias` WITHOUT quote
//      metadata, so lowercasing those collapses quote-and-case variance in one step (a
//      genuinely quote-requiring name like `"Weird Name"` case-folds to `weird name`,
//      still distinct from any other identifier). String literals live in CONSTANT
//      `value` nodes, untouched — `'Sale'` ≠ `'sale'`.
//   4. `lake.<layer>.` qualifier strip — the cockpit addresses `lake.typed.<name>`
//      while stored snippets are bare; a BASE_TABLE whose `catalog_name` is `lake` has
//      its catalog + schema cleared, so a qualified reference matches the bare stored
//      form. Applied to the comparison KEY only, never to the SQL that runs.

import type { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";

import { canonicalizeForReuse, normalizeSql } from "./snippet-normalize";

// --- tree normalization ----------------------------------------------------------
//
// A `json_serialize_sql` node is a plain object with a `class` discriminator
// (COLUMN_REF, FUNCTION, CONJUNCTION, CONSTANT, …) plus class-specific fields; the
// top-level result carries `error` (false on a clean parse). Everything below is
// typed against `unknown` and narrowed — the tree is a boundary value.

/** Operator FUNCTIONs (`is_operator`) whose operands may be reordered. */
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

/**
 * The commutative-operator identity of a node, or null if it is not a reorderable
 * commutative node. Two nodes share an identity iff their operands may be flattened
 * together (same `+`/`*` operator, or same AND/OR conjunction).
 */
function commutativeOpKey(node: Record<string, unknown>): string | null {
	if (
		node.class === "FUNCTION" &&
		node.is_operator === true &&
		typeof node.function_name === "string" &&
		COMMUTATIVE_OPERATORS.has(node.function_name)
	) {
		return `fn:${node.function_name}`;
	}
	if (
		node.class === "CONJUNCTION" &&
		typeof node.type === "string" &&
		COMMUTATIVE_CONJUNCTIONS.has(node.type)
	) {
		return `conj:${node.type}`;
	}
	return null;
}

/**
 * Collect the operands of a commutative chain, flattening any nested child that is
 * the SAME commutative operator. Children are already normalized by the caller, so a
 * nested `(a+b)` contributes its own (already-flattened) operands. This makes
 * `a+b+c` ≡ `a+(b+c)` regardless of DuckDB's left-nested binary shaping.
 */
function flattenCommutative(
	children: unknown[],
	opKey: string,
	out: unknown[],
): void {
	for (const child of children) {
		if (
			isRecord(child) &&
			commutativeOpKey(child) === opKey &&
			Array.isArray(child.children)
		) {
			flattenCommutative(child.children, opKey, out);
		} else {
			out.push(child);
		}
	}
}

/**
 * Recursively canonicalize a serialized node: drop `query_location`, case-fold
 * identifiers (`column_names` / `table_name`) and aliases, strip the `lake.<layer>.`
 * qualifier, then flatten + sort the operands of any commutative node.
 */
function normalizeNode(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(normalizeNode);
	if (!isRecord(value)) return value;

	const out: Record<string, unknown> = {};
	for (const [key, child] of Object.entries(value)) {
		if (key === "query_location") continue; // byte offset — pure cosmetics
		out[key] = normalizeNode(child);
	}

	// DuckDB folds identifier + alias case, so neither fragments identity.
	if (typeof out.alias === "string") out.alias = out.alias.toLowerCase();
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

	const opKey = commutativeOpKey(out);
	if (
		opKey !== null &&
		Array.isArray(out.children) &&
		out.children.length >= 2
	) {
		const flat: unknown[] = [];
		flattenCommutative(out.children, opKey, flat);
		flat.sort((a, b) => {
			const ka = stableKey(a);
			const kb = stableKey(b);
			return ka < kb ? -1 : ka > kb ? 1 : 0;
		});
		out.children = flat;
	}
	return out;
}

/**
 * The canonical comparison key for a parsed statement. A clean parse
 * (`tree.error === false`) yields the stable serialization of the normalized tree;
 * anything else FAILS SOFT to the string normalizer (`normalizeSql ∘
 * canonicalizeForReuse`) — a query DuckDB can't parse only weakens the reuse signal
 * (it matches only if the other side also fell back to the same string form), never
 * throws. The two forms are structurally distinct (JSON object vs lowercased SQL) so
 * a parsed key can never spuriously collide with a fallback key. Both sides of a
 * comparison run through this same function, so the decision is self-consistent
 * regardless of how a snippet was produced.
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

// --- parsing (private in-memory DuckDB) ------------------------------------------

// A memoized in-memory DuckDB used purely as a PARSER. `json_serialize_sql` touches
// no tables, so this needs no ATTACH/lake — decoupling snippet identity from lake
// reachability. `@duckdb/node-api` is a native binding, imported lazily so the pure
// `canonicalKey` path (and its unit test) never load it or `#/config`.
let parserInstance: Promise<DuckDBInstance> | null = null;
function getParser(): Promise<DuckDBInstance> {
	if (!parserInstance) {
		parserInstance = import("@duckdb/node-api")
			.then(({ DuckDBInstance }) => DuckDBInstance.create(":memory:"))
			.catch((err) => {
				parserInstance = null; // clear the memo so a later call can retry
				throw err;
			});
	}
	return parserInstance;
}

/** Close the memoized parser instance and reset the memo. For test teardown. */
export async function closeSqlParser(): Promise<void> {
	const inst = parserInstance;
	parserInstance = null;
	if (inst) {
		try {
			(await inst).closeSync();
		} catch {
			// already closed / never opened cleanly — nothing to do
		}
	}
}

/**
 * Serialize one SQL string to its parse tree via DuckDB's own parser. `$1::VARCHAR`
 * — `json_serialize_sql` requires a VARCHAR arg and rejects an untyped bind param
 * (a bound param, never string interpolation — no injection surface). Returns the
 * parsed JSON (which itself carries `error` on a parse failure), or null if the
 * call/JSON is unusable; `canonicalKey` fails soft on either.
 */
async function serialize(
	conn: DuckDBConnection,
	sql: string,
): Promise<unknown> {
	const reader = await conn.runAndReadAll(
		"SELECT json_serialize_sql($1::VARCHAR) AS tree",
		[sql],
	);
	const raw = reader.getRowObjectsJson()[0]?.tree;
	if (typeof raw !== "string") return null;
	try {
		return JSON.parse(raw);
	} catch {
		return null;
	}
}

// --- relation extraction (DAT-672) -------------------------------------------

const isTreeRecord = (v: unknown): v is Record<string, unknown> =>
	typeof v === "object" && v !== null && !Array.isArray(v);

/** Collect BASE_TABLE names and cte_map keys from a serialized parse tree. A
 *  CTE reference also parses as BASE_TABLE (resolution happens at bind time),
 *  so the caller subtracts the CTE names to get the REAL relations. */
const collectRelations = (
	node: unknown,
	base: Set<string>,
	ctes: Set<string>,
): void => {
	if (Array.isArray(node)) {
		for (const v of node) collectRelations(v, base, ctes);
		return;
	}
	if (!isTreeRecord(node)) return;
	if (node.type === "BASE_TABLE" && typeof node.table_name === "string") {
		base.add(node.table_name);
	}
	const cteMap = node.cte_map;
	if (isTreeRecord(cteMap) && Array.isArray(cteMap.map)) {
		for (const entry of cteMap.map) {
			if (isTreeRecord(entry) && typeof entry.key === "string") {
				ctes.add(entry.key);
			}
		}
	}
	for (const v of Object.values(node)) collectRelations(v, base, ctes);
};

/**
 * The base relations a statement READS — BASE_TABLE nodes of DuckDB's own
 * parse tree, minus CTE names. The ground truth for "which view/table does
 * this snippet read" (DAT-672 axis resolution + the Model page's grounding
 * edges): exact names, immune to a view name appearing inside a string
 * literal, comment, or alias — the substring-matcher failure modes. Returns
 * null when the SQL does not parse (or the parser is unavailable), which
 * callers treat as "reads nothing recognizable".
 */
export async function sqlRelations(sql: string): Promise<string[] | null> {
	let tree: unknown = null;
	try {
		const conn = await (await getParser()).connect();
		try {
			tree = await serialize(conn, sql);
		} finally {
			conn.closeSync();
		}
	} catch {
		return null;
	}
	// Parse failures come back IN-BAND (`error: true`), not thrown.
	if (!isTreeRecord(tree) || tree.error !== false) return null;
	const base = new Set<string>();
	const ctes = new Set<string>();
	collectRelations(tree, base, ctes);
	return [...base].filter((t) => !ctes.has(t));
}

/** True when two SQL fragments are the same snippet under canonicalization. */
export async function sqlEquivalent(a: string, b: string): Promise<boolean> {
	let treeA: unknown = null;
	let treeB: unknown = null;
	try {
		// One connection parses both — pure parsing, no per-call instance churn.
		const conn = await (await getParser()).connect();
		try {
			treeA = await serialize(conn, a);
			treeB = await serialize(conn, b);
		} finally {
			conn.closeSync();
		}
	} catch {
		// parser unavailable → both null → both sides fall back to the string form
	}
	return canonicalKey(treeA, a) === canonicalKey(treeB, b);
}
