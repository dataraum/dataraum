// SQL-as-structure via DuckDB's own AST (DAT-713 direction): read structure
// off the JSON parse tree — the same parser that executes, so a parse can never
// diverge from the binder. This module is the READ side used by the flow gate
// (DAT-673): "which base columns does an extract AGGREGATE?" The mutation side
// (the drill's clause appends) lands with DAT-678.
//
// The parser is the shared memoized in-memory DuckDB in `sql-canonical.ts`
// (`json_serialize_sql` is parse-only — no lake, no table binding), so this
// adds a walk, not a second native binding.

import { getParser, parseSqlToJson } from "#/lib/sql-canonical";

let aggregateNamesPromise: Promise<ReadonlySet<string>> | null = null;

/** DuckDB's authoritative aggregate-function names — the JSON AST does NOT mark
 *  a `FUNCTION` node as aggregate vs scalar (bind-time classification), so the
 *  catalog is the source of truth (and it can't drift from the executor). */
function aggregateNames(): Promise<ReadonlySet<string>> {
	if (!aggregateNamesPromise) {
		aggregateNamesPromise = (async () => {
			const conn = await (await getParser()).connect();
			try {
				const reader = await conn.runAndReadAll(
					"SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'aggregate'",
				);
				const names = new Set<string>();
				for (const row of reader.getRowObjectsJson()) {
					if (typeof row.function_name === "string") {
						names.add(row.function_name);
					}
				}
				return names;
			} finally {
				conn.closeSync();
			}
		})().catch((err) => {
			aggregateNamesPromise = null; // let a later call retry
			throw err;
		});
	}
	return aggregateNamesPromise;
}

/** Does any node anywhere in the parse tree carry `class === cls`? (A shallow
 *  structural probe — used to fail closed on shapes the aggregate walk can't
 *  yet read, e.g. `WINDOW`.) */
function hasClass(node: unknown, cls: string): boolean {
	if (Array.isArray(node)) return node.some((child) => hasClass(child, cls));
	if (node === null || typeof node !== "object") return false;
	const obj = node as Record<string, unknown>;
	if (obj.class === cls) return true;
	return Object.values(obj).some((value) => hasClass(value, cls));
}

/** The last element of a COLUMN_REF's `column_names` — the bare column, dropping
 *  any table/schema qualification (`["t","credit"]` → `credit`). */
function bareColumn(columnNames: unknown): string | null {
	if (!Array.isArray(columnNames) || columnNames.length === 0) return null;
	const last = columnNames[columnNames.length - 1];
	return typeof last === "string" ? last : null;
}

/**
 * The base columns an extract's select expression AGGREGATES — the column
 * references INSIDE an aggregate function (`SUM(credit)` → `credit`; a bare
 * `credit` outside any aggregate is ignored, as is a scalar-only multiplier).
 * Parse-only: the relation need not exist. Returns an empty set when the
 * expression can't be parsed — the caller treats "couldn't determine" as
 * fail-closed (no time grain).
 */
export async function aggregatedColumns(
	selectExpr: string,
): Promise<Set<string>> {
	const aggregates = await aggregateNames();
	const ast = await parseSqlToJson(`SELECT ${selectExpr} AS value`);
	if (
		ast === null ||
		typeof ast !== "object" ||
		(ast as { error?: unknown }).error
	) {
		return new Set();
	}

	// Fail CLOSED on window aggregates (DAT-673). A windowed `SUM(x) OVER (…)`
	// parses as a `WINDOW` node, NOT a `FUNCTION`, so the aggregate walk below
	// would miss its columns — a windowed STOCK would read as "nothing
	// aggregated" → the gate wrongly says safe and offers grain (the one
	// direction a safety gate must never get wrong). Stopgap: any WINDOW node →
	// empty set → the caller fails closed (strips grain). Parsing window bodies
	// properly (and FILTER-clause / case-sensitivity handling) is DAT-715.
	if (hasClass(ast, "WINDOW")) return new Set();

	const columns = new Set<string>();
	const walk = (node: unknown, insideAggregate: boolean): void => {
		if (Array.isArray(node)) {
			for (const child of node) walk(child, insideAggregate);
			return;
		}
		if (node === null || typeof node !== "object") return;
		const obj = node as Record<string, unknown>;
		const entersAggregate =
			obj.class === "FUNCTION" &&
			typeof obj.function_name === "string" &&
			aggregates.has(obj.function_name);
		const nowInside = insideAggregate || entersAggregate;
		if (nowInside && obj.class === "COLUMN_REF") {
			const col = bareColumn(obj.column_names);
			if (col !== null) columns.add(col);
		}
		for (const value of Object.values(obj)) walk(value, nowInside);
	};
	walk(ast, false);
	return columns;
}

// --- declared value expressions (DAT-671) ------------------------------------

/** The one select item of a parsed `SELECT <expr>`, or a refusal describing why
 *  the statement is not exactly that. */
function soleSelectItem(
	ast: unknown,
): { item: Record<string, unknown> } | { why: string } {
	if (typeof ast !== "object" || ast === null) {
		return { why: "it could not be parsed as SQL" };
	}
	const root = ast as Record<string, unknown>;
	if (root.error) {
		// The parser reports failures IN-BAND. Its own message is the most useful
		// thing we can hand back — it names the offending token.
		const message =
			typeof root.error_message === "string"
				? root.error_message
				: "parse error";
		return { why: `it is not a valid SQL expression (${message})` };
	}
	const statements = root.statements;
	if (!Array.isArray(statements) || statements.length !== 1) {
		return { why: "it is not a single expression" };
	}
	const first = statements[0];
	const node =
		typeof first === "object" && first !== null
			? (first as Record<string, unknown>).node
			: null;
	if (typeof node !== "object" || node === null) {
		return { why: "it could not be parsed as SQL" };
	}
	const select = node as Record<string, unknown>;
	const from = select.from_table;
	if (
		typeof from === "object" &&
		from !== null &&
		(from as Record<string, unknown>).type !== "EMPTY"
	) {
		return {
			why: "it carries its own FROM clause — the table belongs in `relation`",
		};
	}
	if (select.where_clause !== null && select.where_clause !== undefined) {
		return {
			why: "it carries its own WHERE clause — predicates belong in `filters`",
		};
	}
	const list = select.select_list;
	if (!Array.isArray(list) || list.length !== 1) {
		const n = Array.isArray(list) ? list.length : 0;
		return {
			why: `it projects ${n} values, not one — leave the source empty for a step that returns several columns`,
		};
	}
	const item = list[0];
	if (typeof item !== "object" || item === null) {
		return { why: "it could not be parsed as SQL" };
	}
	return { item: item as Record<string, unknown> };
}

/**
 * Why a model-DECLARED value expression cannot be composed as clause parts, or
 * null when it can (DAT-671).
 *
 * The declaration contract is "one value expression, unaliased" — the composer
 * supplies the `AS "value"` alias itself. Nothing used to enforce it, and the
 * failure was silent in the worst way: `SUM(x) AS revenue` composes to
 * `SUM(x) AS revenue AS "value"`, which is a PARSE error, which the executed
 * proof catches as "did not bind", which downgrades the answer to tier A with
 * no trace of a model typo. Enforcing it HERE turns that into a named,
 * disclosed rejection the model can repair on its next validation.
 *
 * Structural, via DuckDB's own parser — never a regex over model-authored SQL.
 * The alias, the projected-item count, and a smuggled FROM/WHERE all come off
 * the parse tree of `SELECT <expr>`, which is exactly how the composer will
 * read it. Fails OPEN (null) if the parser itself is unreachable: the executed
 * proof remains the arbiter, so an unavailable parser must not silently retire
 * the feature.
 */
export async function declaredValueExprRefusal(
	selectExpr: string,
): Promise<string | null> {
	let ast: unknown;
	try {
		ast = await parseSqlToJson(`SELECT ${selectExpr}`);
	} catch {
		return null;
	}
	// null = the parser gave nothing usable (infrastructure, not the model).
	if (ast === null) return null;

	const sole = soleSelectItem(ast);
	if ("why" in sole) return sole.why;

	const alias = sole.item.alias;
	if (typeof alias === "string" && alias !== "") {
		return `it carries its own \`AS ${alias}\` alias — declare the expression alone, the drill supplies the alias`;
	}
	return null;
}
