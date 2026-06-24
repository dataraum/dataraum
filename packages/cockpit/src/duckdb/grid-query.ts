// Pure SQL composition + request-field parsing for the result grid (DAT-385 sort
// / DAT-613 windowing + filter). NEO-FREE on purpose: this module touches no
// `@duckdb/node-api` value, so the client bundle can import its constants and the
// `parseColumnFilterInput` helper without pulling the native driver in (the same
// discipline ndjson-stream.ts keeps with its type-only Json import). The
// neo-touching streaming core lives in stream-sql.ts and imports from here.

import { HARD_ROW_CEILING } from "#/duckdb/limit";

// --- Cap clamp (one-shot probe grid, design §5.5) ----------------------------

/**
 * Grid default cap for the one-shot (probe) grid. Larger than the agent tool's
 * 1000 (run-sql.ts DEFAULT_LIMIT): the grid is a human browsing surface, not an
 * LLM context, so it streams far more before truncating. The windowed lake grid
 * (DAT-613) has NO cap — it pages instead — so this only bounds the probe.
 */
export const GRID_DEFAULT_CAP = 50_000;

/**
 * Clamp a client-requested cap to `[1, HARD_ROW_CEILING]`, defaulting an absent
 * cap to {@link GRID_DEFAULT_CAP}. A client can never ask for an unbounded — or a
 * non-positive — materialization. The 200k ceiling is shared with the agent tool
 * ({@link HARD_ROW_CEILING}, DAT-384); only the *default* differs.
 */
export function clampGridCap(cap?: number): number {
	if (cap === undefined || !Number.isFinite(cap)) {
		return GRID_DEFAULT_CAP;
	}
	const floored = Math.max(1, Math.floor(cap));
	return Math.min(floored, HARD_ROW_CEILING);
}

// --- Sort (DAT-385 P3) -------------------------------------------------------

/**
 * A single-column sort the grid asks the server to apply. `column` is an OUTPUT
 * column name of the user's query (the grid only offers names it received in the
 * stream header); `dir` is the sort direction.
 */
export interface GridSort {
	column: string;
	dir: "asc" | "desc";
}

/**
 * Quote `name` as a DuckDB identifier: wrap in double quotes and double any
 * embedded quote. This is the ONLY safe way to interpolate a column name into
 * SQL — the grid's sort/filter column is user/agent-influenced (an output column
 * of arbitrary `run_sql`), so it can never be concatenated raw. A bogus name
 * still can't inject; it just yields a binder error the stream reports in-band.
 */
export function quoteIdentifier(name: string): string {
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Validate an optional grid `sort` field off a request body. Shared by every grid
 * stream route (`/api/run-sql`, `/api/probe-sql`): returns the sort, `null` when
 * absent, or an `{ error }` the route turns into a 400. Bounds the column-name
 * length so a validated field can't balloon the SQL handed to DuckDB.
 */
export function parseSort(
	raw: unknown,
): { sort: GridSort | null } | { error: string } {
	if (raw === undefined || raw === null) return { sort: null };
	// `typeof [] === "object"`, so reject arrays explicitly — otherwise a JSON
	// array falls through to the column check and yields a misleading error.
	if (typeof raw !== "object" || Array.isArray(raw))
		return { error: "Field 'sort' must be an object." };
	const { column, dir } = raw as { column?: unknown; dir?: unknown };
	if (
		typeof column !== "string" ||
		column.length === 0 ||
		column.length > 256
	) {
		return {
			error:
				"Field 'sort.column' is required and must be a non-empty string (max 256 chars).",
		};
	}
	if (dir !== "asc" && dir !== "desc") {
		return { error: "Field 'sort.dir' must be 'asc' or 'desc'." };
	}
	return { sort: { column, dir } };
}

// --- Windowed paging (DAT-613) ----------------------------------------------

/** Default rows per scroll-window the grid fetches (Mosaic-style load-on-scroll). */
export const GRID_PAGE_SIZE = 500;

/** Hard ceiling on a single window so a client can't ask for a giant page. */
export const GRID_MAX_PAGE = 5_000;

/**
 * A single scroll-window the grid asks the server for: `limit` rows starting at
 * row `offset` (0-based). The grid pages forward by `offset += limit` until a
 * short window signals the end (DAT-613). Both bound the LIMIT/OFFSET the server
 * inlines, so neither can balloon the query.
 */
export interface GridWindow {
	limit: number;
	offset: number;
}

/** Clamp a requested page `limit` to `[1, GRID_MAX_PAGE]`, defaulting to {@link GRID_PAGE_SIZE}. */
export function clampPageLimit(limit?: number): number {
	if (limit === undefined || !Number.isFinite(limit)) return GRID_PAGE_SIZE;
	return Math.min(Math.max(1, Math.floor(limit)), GRID_MAX_PAGE);
}

/** Clamp a requested `offset` to a non-negative integer (defaults to 0). */
export function clampOffset(offset?: number): number {
	if (offset === undefined || !Number.isFinite(offset) || offset < 0) return 0;
	return Math.floor(offset);
}

/**
 * Wrap the user's `sql` as the grid's effective query: an optional `WHERE`
 * (push-down filters, DAT-613), an optional server-side `ORDER BY`, and — for the
 * windowed grid — a `LIMIT/OFFSET` page.
 *
 * Sort and filter MUST run server-side, across the FULL result, before the window
 * is cut: a window is only a slice, so sorting/filtering just the streamed rows
 * would act on an arbitrary slice. Clause order is WHERE → ORDER BY → LIMIT.
 *
 * Stable order is REQUIRED once windowing: each LIMIT/OFFSET page is its own
 * execution, and a query with no imposed order can return rows in a different
 * order per execution — dropping or duplicating rows at page boundaries. So when
 * a `window` is present we order by EVERY output column:
 *   - unsorted → `ORDER BY ALL` (all output columns, left-to-right).
 *   - sorted   → the user column first, then `COLUMNS(*)` as a column-agnostic
 *     tiebreaker over the rest.
 * This makes the ordered VALUE sequence deterministic across executions — the only
 * rows it leaves un-ordered are those identical in EVERY column, and those are
 * interchangeable, so a page boundary that splits a run of full-duplicate rows
 * still yields the correct multiset (locked by the duplicate-heavy paging
 * integration test). It is deliberately NOT a total order on row *identity*
 * (that would need a synthetic key per row); pagination only needs the value
 * sequence to be stable, which it is. WITHOUT a window (the probe's one-shot
 * grid) the order is unchanged: a bare `ORDER BY` for an explicit sort, none
 * otherwise — natural scan order preserved.
 *
 * `where` is an already-composed predicate fragment ({@link buildFilterClause})
 * whose bind values the caller appends to `params`; LIMIT/OFFSET are validated
 * integers inlined here. So this never perturbs the caller's positional `params`
 * — the user's `$1..$k` bind the inner `sql`, the filter's `$(k+1)..` the WHERE.
 */
export function buildGridQuery(
	sql: string,
	sort?: GridSort | null,
	window?: GridWindow | null,
	where?: string | null,
): string {
	const base = `SELECT * FROM (${sql}) AS _run_sql`;

	let order: string | null;
	if (sort) {
		const dir = sort.dir === "desc" ? "DESC" : "ASC";
		order = window
			? `ORDER BY ${quoteIdentifier(sort.column)} ${dir}, COLUMNS(*)`
			: `ORDER BY ${quoteIdentifier(sort.column)} ${dir}`;
	} else {
		order = window ? "ORDER BY ALL" : null;
	}

	let query = base;
	if (where) query += ` WHERE ${where}`;
	if (order) query += ` ${order}`;
	if (window) {
		// Over-fetch by exactly one row: the route streams with `cap = limit`, so
		// the extra row is peeked (never emitted) and surfaces as footer.truncated
		// — the has-more signal the grid pages on.
		const limit = Math.floor(window.limit) + 1;
		const offset = Math.floor(window.offset);
		query += ` LIMIT ${limit} OFFSET ${offset}`;
	}
	return query;
}

// --- Push-down filter (DAT-613) ---------------------------------------------

/**
 * A per-column filter operator. `contains` is a case-insensitive substring match
 * on the column's text form (works on any type); the rest are scalar comparisons
 * that lean on DuckDB's implicit cast of the bound text to the column's type.
 */
export type FilterOp = "contains" | "eq" | "neq" | "gt" | "gte" | "lt" | "lte";

/** The category a column's DuckDB type falls into for filter-input parsing. */
export type FilterKind = "numeric" | "temporal" | "text";

/**
 * One active grid filter: match `column` (an OUTPUT column name, quoted before
 * use) by `op` against `value`. `value` is ALWAYS bound as a positional param,
 * never inlined.
 */
export interface GridFilter {
	column: string;
	op: FilterOp;
	value: string;
}

const COMPARISON_SQL: Record<Exclude<FilterOp, "contains">, string> = {
	eq: "=",
	neq: "<>",
	gt: ">",
	gte: ">=",
	lt: "<",
	lte: "<=",
};

const FILTER_OPS: ReadonlySet<string> = new Set<FilterOp>([
	"contains",
	"eq",
	"neq",
	"gt",
	"gte",
	"lt",
	"lte",
]);

/**
 * Compose the windowed grid's WHERE clause from per-column filters (DAT-613).
 * Each predicate binds its value as a positional param numbered AFTER the user's
 * own `$1..$baseParamCount`, so the caller passes `[...userParams, ...params]`.
 * `contains` matches a substring of the column's text form (works on any type);
 * the comparisons lean on DuckDB's implicit cast of the bound text to the column
 * type. Returns `{ where: null }` with no params when there are no filters.
 */
export function buildFilterClause(
	filters: readonly GridFilter[],
	baseParamCount: number,
): { where: string | null; params: string[] } {
	if (filters.length === 0) return { where: null, params: [] };
	const predicates: string[] = [];
	const params: string[] = [];
	filters.forEach((f, i) => {
		const pos = baseParamCount + i + 1; // $-positions are 1-based
		const col = quoteIdentifier(f.column);
		if (f.op === "contains") {
			predicates.push(`CAST(${col} AS VARCHAR) ILIKE ('%' || $${pos} || '%')`);
		} else {
			predicates.push(`${col} ${COMPARISON_SQL[f.op]} $${pos}`);
		}
		params.push(f.value);
	});
	return { where: predicates.join(" AND "), params };
}

/**
 * Validate an optional grid `filters` field off a request body (mirrors
 * {@link parseSort}). Returns the filters, `[]` when absent, or an `{ error }` the
 * route turns into a 400. Bounds the count and each column/value length so a
 * validated field can't balloon the SQL or the bound params.
 */
export function parseFilters(
	raw: unknown,
): { filters: GridFilter[] } | { error: string } {
	if (raw === undefined || raw === null) return { filters: [] };
	if (!Array.isArray(raw))
		return { error: "Field 'filters' must be an array." };
	if (raw.length > 64) return { error: "Too many filters (max 64)." };
	const filters: GridFilter[] = [];
	for (const item of raw) {
		if (typeof item !== "object" || item === null || Array.isArray(item))
			return { error: "Each filter must be an object." };
		const { column, op, value } = item as {
			column?: unknown;
			op?: unknown;
			value?: unknown;
		};
		if (
			typeof column !== "string" ||
			column.length === 0 ||
			column.length > 256
		)
			return {
				error: "filter.column must be a non-empty string (max 256 chars).",
			};
		if (typeof op !== "string" || !FILTER_OPS.has(op))
			return {
				error: "filter.op must be one of contains, eq, neq, gt, gte, lt, lte.",
			};
		if (typeof value !== "string" || value.length > 1024)
			return { error: "filter.value must be a string (max 1024 chars)." };
		filters.push({ column, op: op as FilterOp, value });
	}
	return { filters };
}

/**
 * Map a raw filter-row input + the column's {@link FilterKind} to a
 * {@link GridFilter}, or `null` when the input is empty (clears the filter). Pure
 * + client-safe, so the grid's filter row builds requests without the neo driver.
 *
 * Text columns always match a substring (`contains`). Numeric/temporal columns
 * parse a leading comparison operator (`>=`, `<=`, `>`, `<`, `=`, `!=`/`<>`) so an
 * analyst can type `>1000` or `>=2024-01-01`; a bare value means equals.
 */
export function parseColumnFilterInput(
	column: string,
	raw: string,
	kind: FilterKind,
): GridFilter | null {
	const trimmed = raw.trim();
	if (trimmed === "") return null;
	if (kind === "text") return { column, op: "contains", value: trimmed };

	const m = /^(>=|<=|!=|<>|>|<|=)\s*(.*)$/.exec(trimmed);
	if (!m) return { column, op: "eq", value: trimmed };
	const value = m[2].trim();
	if (value === "") return null;
	const token = m[1];
	const op: FilterOp =
		token === ">="
			? "gte"
			: token === "<="
				? "lte"
				: token === ">"
					? "gt"
					: token === "<"
						? "lt"
						: token === "!=" || token === "<>"
							? "neq"
							: "eq";
	return { column, op, value };
}
