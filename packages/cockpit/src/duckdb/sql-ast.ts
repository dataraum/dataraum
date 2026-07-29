// SQL-as-structure via DuckDB's own AST (DAT-713 direction): read structure
// off the JSON parse tree — the same parser that executes, so a parse can never
// diverge from the binder. This module answers "which base columns does an
// extract AGGREGATE?" for the drill's UNIT gate, and "can this declared value
// expression be composed as clause parts?" for the answer path.
//
// It is NOT an additivity judge. The flow gate that once lived on this read was
// deleted in DAT-725 and replaced by the engine's persisted per-(target × axis)
// verdict (DAT-857/868) — additivity is a served fact, never re-derived from
// SQL structure here.
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

/**
 * Sub-trees that are NEVER measure arguments, even when they hang off an
 * aggregate node (DAT-868, closing the DAT-715 residue). Each names a real
 * DuckDB AST key:
 *
 * - `filter` / `filter_expr` — the `FILTER (WHERE …)` predicate of an aggregate
 *   (`FUNCTION`) or a windowed aggregate (`WINDOW`). Its columns RESTRICT the
 *   rows that are aggregated; they are not themselves aggregated. The old blind
 *   `Object.values` descent collected them as if they were, so
 *   `SUM(credit) FILTER (WHERE flag > 0)` reported `flag` as an aggregated
 *   measure — an over-collection that hands the unit gate a column to check
 *   that no measure ever summed.
 * - `partitions` / `orders` / `arg_orders` / `order_bys` — the window frame's
 *   PARTITION BY / ORDER BY and an aggregate's internal ORDER BY. These are
 *   grouping/sequencing keys, exactly as much "not a measure" as a GROUP BY key.
 * - `start_expr` / `end_expr` / `offset_expr` / `default_expr` — frame bounds and
 *   `lead`/`lag` offsets. Row arithmetic, not aggregated values.
 */
const NON_MEASURE_KEYS: ReadonlySet<string> = new Set([
	"filter",
	"filter_expr",
	"partitions",
	"orders",
	"arg_orders",
	"order_bys",
	"start_expr",
	"end_expr",
	"offset_expr",
	"default_expr",
]);

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
 * Windowed aggregates count (`SUM(x) OVER (…)` → `x`); a `FILTER (WHERE …)`
 * predicate and the window frame's PARTITION BY / ORDER BY do not — they select
 * and order the rows, they are not the value being aggregated (DAT-868).
 *
 * Parse-only: the relation need not exist. Returns an empty set when the
 * expression can't be parsed — "we could not read this expression", which for
 * the unit gate means it has nothing to check here, not that anything is safe.
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

	const columns = new Set<string>();
	const walk = (node: unknown, insideAggregate: boolean): void => {
		if (Array.isArray(node)) {
			for (const child of node) walk(child, insideAggregate);
			return;
		}
		if (node === null || typeof node !== "object") return;
		const obj = node as Record<string, unknown>;
		// A windowed aggregate (`SUM(x) OVER (…)`) parses as class `WINDOW`, not
		// `FUNCTION`, but it carries the same `function_name` and puts its
		// arguments in the same `children` — so it enters the aggregate exactly
		// like a plain call (DAT-868). This used to fail closed on the whole
		// expression: any WINDOW node returned the empty set, which — now that
		// the caller is the UNIT gate, not the retired flow gate — means the gate
		// silently has nothing to check rather than anything safe.
		//
		// NAVIGATION functions are deliberately in scope too. `duckdb_functions()`
		// classifies `lead`/`lag`/`row_number`/`rank`/`first_value` as
		// `function_type='aggregate'` (probed), so `lead(amount) OVER (…)` collects
		// `amount`. That is the ANSWER WE WANT here: this feeds only the unit gate,
		// whose question is "does this expression read measure columns whose unit
		// column carries more than one unit?" — and a windowed read of a
		// mixed-unit measure is exactly as much of a problem as a summed one. The
		// gate discloses loudly; it never silently enables. (`row_number()`/`rank()`
		// take no column arguments, so they contribute nothing regardless.)
		const entersAggregate =
			(obj.class === "FUNCTION" || obj.class === "WINDOW") &&
			typeof obj.function_name === "string" &&
			aggregates.has(obj.function_name.toLowerCase());
		const nowInside = insideAggregate || entersAggregate;
		if (nowInside && obj.class === "COLUMN_REF") {
			const col = bareColumn(obj.column_names);
			if (col !== null) columns.add(col);
		}
		for (const [key, value] of Object.entries(obj)) {
			// A FILTER predicate / window frame key is never a measure, however
			// deep inside an aggregate it sits — descend it OUTSIDE the aggregate
			// so a genuine nested aggregate there is still found on its own node.
			walk(value, NON_MEASURE_KEYS.has(key) ? false : nowInside);
		}
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
	// Every clause a SELECT_NODE can carry besides the projection itself. A
	// declared value expression is an EXPRESSION, so all of them must be absent:
	// the composer splices the expression into its own statement, where a
	// smuggled clause either changes the meaning of the recomposed number or
	// dies in the binder as precisely the silent tier-A downgrade this gate
	// exists to end. Each of these parses cleanly on its own (probed against the
	// real tree), which is why none of them can be left to the parser to reject.
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
	const groupExpressions = select.group_expressions;
	if (Array.isArray(groupExpressions) && groupExpressions.length > 0) {
		return {
			why: "it carries its own GROUP BY — a declared source is the UNGROUPED value, and the drill is what groups it",
		};
	}
	if (select.having !== null && select.having !== undefined) {
		return {
			why: "it carries its own HAVING clause — a declared source is one value, not a filtered grouping",
		};
	}
	if (select.qualify !== null && select.qualify !== undefined) {
		return {
			why: "it carries its own QUALIFY clause — that is a windowed step, so leave the source empty",
		};
	}
	if (select.sample !== null && select.sample !== undefined) {
		return {
			why: "it carries a USING SAMPLE clause — a sampled number is not the number the answer reported",
		};
	}
	const modifiers = select.modifiers;
	if (Array.isArray(modifiers) && modifiers.length > 0) {
		return {
			why: "it carries its own ORDER BY/LIMIT — a declared source is a single value, which neither orders nor limits",
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

// --- existing-identifier read (DAT-671 slice-menu curation) ------------------

/** One select-list item's exposed name: its own `AS` alias if given, else —
 *  for a plain column reference — the referenced column's own bare name.
 *  `null` for a computed, unaliased expression (nothing to name it by without
 *  deeper parsing, and not needed here — see the callers). */
function selectItemName(item: unknown): string | null {
	if (typeof item !== "object" || item === null) return null;
	const obj = item as Record<string, unknown>;
	if (typeof obj.alias === "string" && obj.alias !== "") return obj.alias;
	if (obj.class === "COLUMN_REF") return bareColumn(obj.column_names);
	return null;
}

// --- CTE resolution (DAT-671 R6) ---------------------------------------------
//
// The common agent-authored answer is CTE-SHAPED:
//
//   WITH revenue AS (SELECT account_id__name AS account, SUM(credit) AS total
//                      FROM lake.typed.current_journal_lines_enriched
//                     WHERE account_id__account_type = 'revenue' GROUP BY 1)
//   SELECT account, total FROM revenue ORDER BY total DESC
//
// Read one hop over the OUTER projection, `account` is a column of `revenue`
// and nothing more. The catalog holds `account_id__name`, so nothing matched
// and a result with a visible dimension in it offered NO AXES AT ALL — the
// same class of loss as the bare alias J7 pins, one level further in. The
// alias is not a new column, and the statement itself says in its own
// `cte_map` which column it is a new name FOR.
//
// Resolution follows that chain and stops at the first thing it cannot prove:
// a node resolves through a CTE only when it reads exactly ONE relation and
// that relation is a CTE this statement declares. A join, a subquery, a table
// function, a `FROM cte(a, b)` positional re-naming, a set-operation CTE body
// and an unresolvable name all stop it, leaving the column to speak for itself
// exactly as before. The bound is the visited set: every hop consumes a
// distinct CTE name, and DuckDB rejects a cyclic non-recursive WITH outright.

/** A parse-tree object (never an array — DuckDB's node lists are arrays). */
type AstNode = Record<string, unknown>;

function asNode(value: unknown): AstNode | null {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as AstNode)
		: null;
}

function asSelectNode(value: unknown): AstNode | null {
	const node = asNode(value);
	return node !== null && node.type === "SELECT_NODE" ? node : null;
}

const selectListOf = (node: AstNode): unknown[] =>
	Array.isArray(node.select_list) ? node.select_list : [];

/**
 * The CTE bodies a node may reference: the ones it declares itself over the
 * ones its enclosing scope declared, since an inner `WITH` shadows an outer
 * name. Keys are lower-cased — DuckDB preserves identifier case in
 * `cte_map.map[].key` and in a reference's `table_name` (probed), and resolves
 * the two case-insensitively.
 *
 * Only plain `SELECT` bodies are kept. A set-operation body (`UNION`, and so
 * every `WITH RECURSIVE`) has no single projection to resolve a name against,
 * and a `WITH r(a, b) AS (…)` column re-naming is positional rather than by
 * name — both are simply absent from the scope, which makes every reference to
 * them stop resolving instead of resolving wrongly.
 */
function cteScope(
	node: AstNode,
	inherited: ReadonlyMap<string, AstNode>,
): Map<string, AstNode> {
	const scope = new Map(inherited);
	const cteMap = asNode(node.cte_map);
	const entries = cteMap === null ? null : cteMap.map;
	if (!Array.isArray(entries)) return scope;
	for (const raw of entries) {
		const entry = asNode(raw);
		if (entry === null || typeof entry.key !== "string") continue;
		const value = asNode(entry.value);
		if (value === null) continue;
		if (Array.isArray(value.aliases) && value.aliases.length > 0) continue;
		const query = asNode(value.query);
		const body = query === null ? null : asSelectNode(query.node);
		if (body === null) continue;
		scope.set(entry.key.toLowerCase(), body);
	}
	return scope;
}

/**
 * The ONE CTE a node reads `FROM`, or null when it reads anything else.
 *
 * A CTE reference parses as an unqualified `BASE_TABLE` (resolution is a
 * bind-time catalog lookup), so both qualifiers must be empty — a real
 * `lake.typed.x` can never be mistaken for one — and the name must be in
 * scope. `FROM cte(a, b)` re-names the columns positionally, which no
 * name-keyed resolution can follow, so it is refused with the rest.
 */
function soleCteSource(
	node: AstNode,
	scope: ReadonlyMap<string, AstNode>,
): { key: string; body: AstNode } | null {
	const from = asNode(node.from_table);
	if (from === null || from.type !== "BASE_TABLE") return null;
	if (typeof from.table_name !== "string") return null;
	if (from.catalog_name !== "" || from.schema_name !== "") return null;
	if (
		Array.isArray(from.column_name_alias) &&
		from.column_name_alias.length > 0
	) {
		return null;
	}
	const key = from.table_name.toLowerCase();
	const body = scope.get(key);
	return body === undefined ? null : { key, body };
}

/** The select item that exposes `name` — its `AS` alias, else its own bare
 *  column name — case-insensitively, or null when none does. */
function itemExposing(node: AstNode, name: string): AstNode | null {
	const wanted = name.toLowerCase();
	for (const raw of selectListOf(node)) {
		const item = asNode(raw);
		if (item === null) continue;
		const exposed = selectItemName(item);
		if (exposed !== null && exposed.toLowerCase() === wanted) return item;
	}
	return null;
}

/**
 * Every name one projected column is known by, outermost first: the column this
 * item references, then the name that column carries inside each CTE it came
 * through, ending at the base column the underlying relation actually holds.
 * `["account", "account_id__name"]` for the shape above; `["region_id__name"]`
 * for an ordinary reference to a real table's column.
 *
 * EMPTY for a computed projection — `SUM(credit) AS total` is a projection of
 * no single column, and inventing one would be the SQL inference this module
 * refuses.
 */
function columnLineage(
	item: AstNode,
	node: AstNode,
	scope: ReadonlyMap<string, AstNode>,
	visited: ReadonlySet<string>,
): string[] {
	if (item.class !== "COLUMN_REF") return [];
	const base = bareColumn(item.column_names);
	if (base === null) return [];
	const cte = soleCteSource(node, scope);
	if (cte === null || visited.has(cte.key)) return [base];
	const inner = itemExposing(cte.body, base);
	if (inner === null) return [base];
	return [
		base,
		...columnLineage(
			inner,
			cte.body,
			cteScope(cte.body, scope),
			new Set([...visited, cte.key]),
		),
	];
}

/**
 * Does this node re-project its source's rows UNCHANGED — every item a plain
 * column reference, or one bare star?
 *
 * Only then can the relation it reads speak for the result's grain. A computed
 * item may be an AGGREGATE that rolls the whole relation into a single scalar
 * row (`SELECT SUM(total) FROM revenue` is emphatically not "already broken out
 * by account"), and telling an aggregate from a scalar function needs the
 * bind-time classification the parse tree does not carry — `aggregatedColumns`
 * pays a catalog read for exactly that answer, which this pure, synchronous
 * read cannot. A `RENAME`/`REPLACE` star is refused for the same reason in the
 * other direction: it re-spells what it re-projects.
 */
function reprojectsUnchanged(node: AstNode): boolean {
	const list = selectListOf(node);
	if (list.length === 0) return false;
	return list.every((raw) => {
		const item = asNode(raw);
		if (item === null) return false;
		if (item.class === "COLUMN_REF") return true;
		if (item.class !== "STAR") return false;
		const renamed =
			Array.isArray(item.rename_list) && item.rename_list.length > 0;
		const replaced =
			Array.isArray(item.replace_list) && item.replace_list.length > 0;
		return !renamed && !replaced;
	});
}

/**
 * What each projected column of a result is a projection OF (DAT-671 R2):
 * `result name → base column`, for the plain column references in the outer
 * SELECT list.
 *
 * The tier-A drill can only group by columns the result actually projects, and
 * it decides which of those are DIMENSIONS by matching them against the slice
 * catalog. Both halves are right; the join between them was the bug. A model
 * writes `account_id__name AS account` — a perfectly ordinary projection — and
 * the catalog holds `account_id__name`, so nothing matched and a result with a
 * visible dimension in it reported nothing to slice by. The alias is not a new
 * column, it is a new NAME for one, and the parse tree says so explicitly.
 *
 * ALIASED OR NOT: an unaliased `SELECT region` maps `region → region`, so the
 * caller has one uniform lookup instead of two cases.
 *
 * Only plain `COLUMN_REF` items appear. A computed projection
 * (`SUM(credit) AS value`, `date_trunc(...) AS month`) has no single base
 * column to be a projection of — inventing one would be the SQL-inference this
 * module refuses — so it is simply absent, and the caller falls back to the
 * result's own spelling.
 *
 * THROUGH CTE BODIES (DAT-671 R6): the rename an agent writes usually happens
 * one level in — `WITH revenue AS (SELECT account_id__name AS account …)
 * SELECT account … FROM revenue` — so the source is resolved transitively down
 * the CTE chain (`columnLineage`), and this map holds the base column at the
 * end of it, not the CTE's own output name. That is the name the catalog
 * holds, which is the whole point of the lookup.
 *
 * No star-wrapper unwrap (unlike `existingIdentifierColumns`, whose hop is
 * about an outer GROUP BY): a `SELECT * RENAME (...)` wrap RENAMES what it
 * re-projects, so reading the inner list would attribute inner names to outer
 * columns that no longer carry them. An empty map is the honest answer there —
 * the caller then matches on the result's own spelling exactly as it did
 * before.
 */
export async function projectedSourceColumns(
	sql: string,
): Promise<Map<string, string>> {
	const out = new Map<string, string>();
	let ast: unknown;
	try {
		ast = await parseSqlToJson(sql);
	} catch {
		return out;
	}
	if (ast === null || typeof ast !== "object") return out;
	const root = ast as Record<string, unknown>;
	if (root.error) return out;
	const statements = root.statements;
	if (!Array.isArray(statements) || statements.length !== 1) return out;
	const first = statements[0];
	const node =
		typeof first === "object" && first !== null
			? (first as Record<string, unknown>).node
			: null;
	if (
		typeof node !== "object" ||
		node === null ||
		(node as Record<string, unknown>).type !== "SELECT_NODE"
	) {
		return out;
	}
	const selectNode = node as AstNode;
	const selectList = selectNode.select_list;
	if (!Array.isArray(selectList)) return out;
	const scope = cteScope(selectNode, new Map());
	for (const item of selectList) {
		if (typeof item !== "object" || item === null) continue;
		const obj = item as Record<string, unknown>;
		if (obj.class !== "COLUMN_REF") continue;
		// The LAST name of the lineage: the base column, however many CTE
		// renames stand between it and this projection.
		const lineage = columnLineage(obj, selectNode, scope, new Set());
		const source = lineage[lineage.length - 1];
		if (source === undefined) continue;
		const name = selectItemName(obj);
		// First spelling wins — a result projecting the same column twice is
		// already ambiguous to address, and picking deterministically beats
		// picking by list order.
		if (name !== null && !out.has(name)) out.set(name, source);
	}
	return out;
}

/**
 * Read the GROUP BY / projection identifiers off ONE already-validated
 * SELECT_NODE (pure, this node only) — the innermost step of
 * `existingIdentifierColumns`, applied to the top-level node, to the ONE inner
 * node a star-wrapper hop unwraps to, or to a CTE body (see `grainSpellings`).
 * GROUP BY ALL / ordinal positions / ungrouped-is-empty are exactly as
 * described on the exported function.
 */
function identifiersFromSelectNode(
	selectNode: Record<string, unknown>,
): Set<string> {
	const selectList = Array.isArray(selectNode.select_list)
		? selectNode.select_list
		: [];

	// GROUP BY ALL: no explicit group_expressions to read (verified: it stays
	// empty) — every bare, non-aggregate projected column is implicitly a
	// grouping column instead.
	if (selectNode.aggregate_handling === "FORCE_AGGREGATES") {
		const names = new Set<string>();
		for (const item of selectList) {
			if (
				typeof item !== "object" ||
				item === null ||
				(item as Record<string, unknown>).class !== "COLUMN_REF"
			) {
				continue;
			}
			const name = selectItemName(item);
			if (name !== null) names.add(name);
		}
		return names;
	}

	const groupExpressions = Array.isArray(selectNode.group_expressions)
		? selectNode.group_expressions
		: [];
	if (groupExpressions.length === 0) return new Set(); // ungrouped: nothing sliced yet

	const names = new Set<string>();
	for (const expr of groupExpressions) {
		if (typeof expr !== "object" || expr === null) continue;
		const e = expr as Record<string, unknown>;
		if (e.class === "COLUMN_REF") {
			const name = bareColumn(e.column_names);
			if (name !== null) names.add(name);
			continue;
		}
		// An ordinal GROUP BY position (`GROUP BY 1`) resolves against the
		// projection's own Nth item — a single lookup, not a walk.
		if (e.class === "CONSTANT") {
			const value = (e.value as Record<string, unknown> | undefined)?.value;
			if (typeof value === "number" && Number.isInteger(value) && value >= 1) {
				const item = selectList[value - 1] as unknown;
				const name = item !== undefined ? selectItemName(item) : null;
				if (name !== null) names.add(name);
			}
		}
		// Anything else falls through unrepresented (a computed GROUP BY
		// expression, e.g. date_trunc(...)) — can't be named without deeper
		// parsing; simply not in the set (never a guess, never a crash).
		//
		// Known spelling asymmetry (accepted, not a bug): `GROUP BY 1` over a
		// projection item `date_trunc(...) AS month_bucket` greys `month_bucket`
		// via the ordinal→alias resolution above, but the semantically
		// IDENTICAL `GROUP BY date_trunc(...)` (the expression repeated rather
		// than referenced by position) does not — a bare FUNCTION node has no
		// name to resolve without expression-equality parsing, which is out of
		// scope. Two spellings of the same query, different behaviour.
	}
	return names;
}

/**
 * Every SPELLING of the columns this node's result is already broken out by —
 * the shared core of `existingIdentifierColumns` (DAT-671 R6). Three sources,
 * in this order:
 *
 *  1. The node's OWN grouping (`identifiersFromSelectNode`).
 *  2. When it has none of its own and it re-projects a single CTE unchanged
 *     (`reprojectsUnchanged`), THAT CTE's answer. `WITH revenue AS (… GROUP BY
 *     1) SELECT account, total FROM revenue ORDER BY total DESC` is one row per
 *     account however ungrouped its outer statement looks, so offering
 *     `account` as a fresh slice would re-group a result already at that grain
 *     — the tautological re-slice the greying exists to disclose. Refusing the
 *     hop for a computed projection is what keeps `SELECT SUM(total) FROM
 *     revenue` — a scalar rollup, broken out by nothing — from inheriting a
 *     grain it just collapsed.
 *  3. Every OTHER name the columns from (1)/(2) are known by, taken from each
 *     projected item's `columnLineage`. This is what lets ONE set answer the
 *     greying question on every compose path (ADR-0024 decision 2): tier A
 *     names an axis by the RESULT's spelling (`account`), the parts and node
 *     paths name it by the CATALOG's (`account_id__name`), and a set holding
 *     both greys the same column on both rather than only where the spellings
 *     happen to coincide.
 *
 * Names are collected, never invented: every entry came off a `COLUMN_REF`, an
 * `AS` alias, or an ordinal resolved against the projection.
 */
function grainSpellings(
	node: AstNode,
	scope: ReadonlyMap<string, AstNode>,
	visited: ReadonlySet<string>,
): Set<string> {
	const spellings = identifiersFromSelectNode(node);
	if (spellings.size === 0) {
		const cte = soleCteSource(node, scope);
		if (cte !== null && !visited.has(cte.key) && reprojectsUnchanged(node)) {
			for (const name of grainSpellings(
				cte.body,
				cteScope(cte.body, scope),
				new Set([...visited, cte.key]),
			)) {
				spellings.add(name);
			}
		}
	}
	if (spellings.size === 0) return spellings;

	// Fixed against the grain set as it stands here: a projection is another
	// spelling of an already-broken-out column, never a way to become one.
	const lower = new Set([...spellings].map((n) => n.toLowerCase()));
	for (const raw of selectListOf(node)) {
		const item = asNode(raw);
		if (item === null) continue;
		const exposed = selectItemName(item);
		const chain = [
			...(exposed === null ? [] : [exposed]),
			...columnLineage(item, node, scope, visited),
		];
		if (!chain.some((n) => lower.has(n.toLowerCase()))) continue;
		for (const n of chain) spellings.add(n);
	}
	return spellings;
}

/**
 * Is this node EXACTLY `SELECT * [RENAME (...)|EXCLUDE (...)] FROM (<subquery>)`
 * — a bare star projection (with only RENAME/EXCLUDE modifiers, never a
 * computed replace/expr), no WHERE/HAVING/QUALIFY/SAMPLE/ORDER-BY-LIMIT, over
 * a single subquery? This is OUR OWN generated shape — `drill-sql.ts`'s
 * `composeDrill` wraps a re-labeled re-wrap as exactly
 * `SELECT * RENAME (...) FROM (<the real, possibly-grouped statement>) AS
 * _clean` — never anyone else's SQL, which is why it's safe to special-case
 * by structure alone (verified against the real parser: `node.type` stays
 * `"SELECT_NODE"`, `select_list` is a single `STAR` item carrying
 * `rename_list`/`exclude_list`, and `from_table.type` is `"SUBQUERY"` with the
 * inner statement at `from_table.subquery.node`).
 */
function isStarWrapperNode(node: Record<string, unknown>): boolean {
	if (node.aggregate_handling !== "STANDARD_HANDLING") return false;
	const groupExpressions = node.group_expressions;
	if (Array.isArray(groupExpressions) && groupExpressions.length > 0) {
		return false;
	}
	if (node.where_clause != null) return false;
	if (node.having != null) return false;
	if (node.qualify != null) return false;
	if (node.sample != null) return false;
	if (Array.isArray(node.modifiers) && node.modifiers.length > 0) return false;
	const selectList = node.select_list;
	if (!Array.isArray(selectList) || selectList.length !== 1) return false;
	const item = selectList[0];
	if (typeof item !== "object" || item === null) return false;
	const star = item as Record<string, unknown>;
	if (star.class !== "STAR") return false;
	const renamed =
		Array.isArray(star.rename_list) && star.rename_list.length > 0;
	const excluded =
		Array.isArray(star.exclude_list) && star.exclude_list.length > 0;
	if (!renamed && !excluded) return false;
	const fromTable = node.from_table;
	return (
		typeof fromTable === "object" &&
		fromTable !== null &&
		(fromTable as Record<string, unknown>).type === "SUBQUERY"
	);
}

/**
 * The result's own already-established non-measure identifier columns, read
 * STRUCTURALLY off the outer statement (DAT-671 lead ruling, "we should not
 * slice on already existing slices," amended to grey-out): the columns GROUP
 * BY already breaks the result out by (or, under `GROUP BY ALL`, every bare
 * projected column) are the axes that would be a tautological re-slice — the
 * result is already at that grain for them.
 *
 * WHICH NODE DECIDES. Normally the outer one, with ONE special case: when it
 * is nothing but OUR OWN `SELECT * RENAME (...) FROM (<subquery>)` re-label
 * wrap (`isStarWrapperNode`, `drill-sql.ts`'s `composeDrill` — DAT-671
 * drilled-projection hygiene), the hop reads THAT wrap's inner subquery
 * instead of the wrap itself. Without this, a report that was drilled, then
 * MINTED as a child (freezing the re-labeled wrap as `report.sql`), would
 * re-open with its own grain looking ungrouped — the wrap's outer `SELECT *`
 * carries no GROUP BY of its own, only its inner subquery does —
 * reintroducing the exact tautological-reslice offer one level down. That hop
 * is one, never a walk: a wrap-of-a-wrap (or any inner shape that isn't a
 * plain `SELECT_NODE`) returns `null`, not a second unwrap and not an
 * asserted-empty guess. A set operation (UNION/INTERSECT/EXCEPT), more than
 * one statement, or a parse failure is refused as before.
 *
 * NAMED RELATIONS THE STATEMENT ITSELF DECLARES are resolved (DAT-671 R6, the
 * one extension to the original one-hop line): the deciding node's grouping,
 * and every column in it, are read through the `cte_map` (`grainSpellings` →
 * `columnLineage`). This is a LOOKUP in the statement's own text, not the
 * arbitrary nesting the amendment barred — an anonymous derived table
 * (`FROM (SELECT …) x`) is still not followed, and a shape that cannot be
 * proven still resolves to nothing. It matters because the CTE shape is what
 * agents actually write: without it, `WITH revenue AS (… GROUP BY 1) SELECT
 * account, total FROM revenue` reads as an ungrouped detail result and offers
 * a re-slice by the very column it is already broken out by.
 *
 * A statement this can't determine returns `null`: "could not determine
 * structurally," never a guess. The caller then treats NOTHING as already-
 * sliced (the tier-A post-execution fold probe, `drill-sql.ts`'s
 * `foldsNothing`, remains the net for what this schema/name-only check misses
 * on that path; the parts-at-source path has no equivalent net, so a miss
 * there is a real, accepted gap — never a false claim).
 *
 * An UNGROUPED statement (no GROUP BY at all — `group_expressions` empty and
 * `aggregate_handling` not `FORCE_AGGREGATES`) is a raw/detail result: nothing
 * has been sliced yet, so this returns an EMPTY set rather than treating every
 * projected column as "already there" — that distinction is essential, since
 * treating every column of an ungrouped result as already-sliced would grey
 * out the ordinary first-ever tier-A drill-down (raw rows grouped by a column
 * for the first time, which is the intended, non-tautological use of Slice).
 */
export async function existingIdentifierColumns(
	sql: string,
): Promise<Set<string> | null> {
	let ast: unknown;
	try {
		ast = await parseSqlToJson(sql);
	} catch {
		return null;
	}
	if (ast === null || typeof ast !== "object") return null;
	const root = ast as Record<string, unknown>;
	if (root.error) return null;
	const statements = root.statements;
	if (!Array.isArray(statements) || statements.length !== 1) return null;
	const first = statements[0];
	const node =
		typeof first === "object" && first !== null
			? (first as Record<string, unknown>).node
			: null;
	if (
		typeof node !== "object" ||
		node === null ||
		(node as Record<string, unknown>).type !== "SELECT_NODE"
	) {
		return null;
	}
	const selectNode = node as AstNode;
	const scope = cteScope(selectNode, new Map());
	if (!isStarWrapperNode(selectNode)) {
		return grainSpellings(selectNode, scope, new Set());
	}

	// Our own re-label wrap: hop ONCE into the real statement it wraps. A
	// wrap-of-a-wrap (or anything that isn't a plain SELECT_NODE underneath)
	// is NOT a second hop — it's "can't determine," same as any other shape
	// this function doesn't recognize.
	const fromTable = selectNode.from_table as Record<string, unknown>;
	const subquery = fromTable.subquery;
	const innerNode =
		typeof subquery === "object" && subquery !== null
			? (subquery as Record<string, unknown>).node
			: null;
	if (
		typeof innerNode !== "object" ||
		innerNode === null ||
		(innerNode as Record<string, unknown>).type !== "SELECT_NODE"
	) {
		return null;
	}
	const innerSelectNode = innerNode as AstNode;
	if (isStarWrapperNode(innerSelectNode)) return null; // multi-hop: undecided
	// The wrapped statement sees the outer statement's CTEs as well as any of
	// its own — the same scope its names bind in.
	return grainSpellings(
		innerSelectNode,
		cteScope(innerSelectNode, scope),
		new Set(),
	);
}
