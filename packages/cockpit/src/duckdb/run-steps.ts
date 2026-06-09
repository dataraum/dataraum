// `run_steps` — the query sub-agent's SQL VALIDATOR (DAT-485 P1).
//
// The `answer` tool's nested chat() sub-agent composes a question's answer as a
// set of concept-named steps + a combining `final_sql` (the snippet-reuse unit,
// mirroring the engine's GraphAgent decomposition). The run_steps TOOL (query.ts)
// folds those into ONE standalone CTE statement (`composeStandalone`) and calls
// this validator to confirm "the SQL works" and read a BOUNDED headline peek —
// never the full result. CTE-BASED execution (DAT-485 review): the validated
// statement IS exactly what the browser grid streams (the `{sql}` handle), so
// the headline the model states and the grid the user sees are the SAME query —
// there is no temp-view-vs-grid divergence (the earlier temp-view form validated
// a DIFFERENT statement than the grid ran). The full result streams browser-side
// via the grid handle (DAT-490); this path caps what re-enters the model.
//
// It is a VALIDATOR, not the executor:
//   - opens its OWN throwaway in-memory DuckDB instance + READ_ONLY lake ATTACH
//     (mirror probe.ts/connect.ts; the shared lake reader is memoized and can't
//     be closed mid-call). A dedicated connection means it CAN honor abort —
//     aborting the chat run `interrupt()`s it, which cancels the in-flight
//     statement and rejects the pending promise (closeSync does NOT interrupt an
//     in-flight query — it would leave it hung and leaked).
//   - runs the composed statement wrapped in `SELECT * FROM (<sql>) … LIMIT n` so
//     a broad final can't materialize unbounded AND an injected `;` is a parser
//     error (the composed CTE bodies are already parenthesized by
//     `composeStandalone`, so multi-statement injection fails to parse).
//   - returns `{ ok, columns, rowCount, sample, truncated }` (the bounded peek)
//     or `{ error }` (an agent-fixable SQL/bind failure the model repairs
//     in-loop) — NEVER the full result set.

import {
	type DuckDBConnection,
	DuckDBInstance,
	type Json,
} from "@duckdb/node-api";

import { boundSampleBytes } from "./agent-sample";
import { attachLakeReadOnly } from "./lake";
import { readerToResult } from "./query-result";

/** A single decomposed step: a name (becomes a CTE) + standalone SQL. */
export interface RunStep {
	/** A SQL identifier — the CTE name `final_sql` references. */
	name: string;
	/** Standalone DuckDB SQL (a SELECT). Must not reference other steps' CTEs. */
	sql: string;
}

/** The validator's success shape — a BOUNDED headline peek, never the full result. */
export interface RunStepsOk {
	ok: true;
	/** Output column names of the composed statement. */
	columns: string[];
	/** Rows in the peek (≤ {@link HEADLINE_PEEK_ROWS}) — NOT the full result size. */
	rowCount: number;
	/** The bounded headline rows the model reads to state the answer. */
	sample: Record<string, Json>[];
	/** `true` when the full result is larger than the peek (model: the grid has all). */
	truncated: boolean;
}

/** The agent-fixable failure branch (bad SQL / bad bind / unmet precondition). */
export interface RunStepsError {
	error: string;
}

export type RunStepsResult = RunStepsOk | RunStepsError;

// A step name is interpolated raw into the composed `<name> AS (…)` CTE, so it
// MUST be a bare SQL identifier — letters/underscore then word chars. This is the
// injection gate on the name; the SQL bodies are the model's own queries, made
// single-statement by the CTE/subquery parens (`composeStandalone` + the final
// wrap) and read-only by the READ_ONLY lake ATTACH.
const STEP_NAME_RE = /^[A-Za-z_]\w*$/;

/** Headline-peek row cap — small on purpose: the model only needs to SEE the
 * answer (often scalar), not the whole result. The full result streams in the
 * grid. Independent of any LIMIT inside the composed statement. */
export const HEADLINE_PEEK_ROWS = 50;

/** Strip a trailing statement terminator + whitespace so the SQL can be wrapped
 * as a subquery (`SELECT * FROM (<sql>) …`) and merged into a CTE. */
function stripTrailingSemicolon(sql: string): string {
	return sql.trim().replace(/;\s*$/, "");
}

/**
 * Validate the step names: each must be a bare SQL identifier and unique (they
 * become CTE names). Returns an agent-fixable message, or null when all names are
 * valid. Pure — unit-tested directly.
 */
export function validateStepNames(steps: RunStep[]): string | null {
	const seen = new Set<string>();
	for (const step of steps) {
		if (!STEP_NAME_RE.test(step.name)) {
			return (
				`Invalid step name '${step.name}': a step name must be a SQL identifier ` +
				`(a letter or underscore followed by letters/digits/underscores), since ` +
				`it becomes a CTE name. Rename the step after its business concept.`
			);
		}
		if (seen.has(step.name)) {
			return `Duplicate step name '${step.name}': each step needs a unique name.`;
		}
		seen.add(step.name);
	}
	return null;
}

/**
 * Fold `{steps, final_sql}` into a SINGLE standalone statement — each step a CTE,
 * `final_sql` referencing them by name. This is the EXACT statement run_steps
 * validates AND the browser grid streams (CTE-based execution): no temp-view-vs-
 * grid divergence. No steps → `final_sql` verbatim. When `final_sql` brings its
 * OWN leading `WITH`, its CTEs are merged into the one `WITH` (never the invalid
 * `WITH … WITH …`). Pure — unit-tested directly. A step name that collides with a
 * CTE `final_sql` brings would yield a `Duplicate CTE name` parser error here,
 * which run_steps surfaces as `{ error }` (caught, not a broken grid).
 */
export function composeStandalone(steps: RunStep[], finalSql: string): string {
	const final = stripTrailingSemicolon(finalSql);
	if (steps.length === 0) return final;

	const ctes = steps.map((s) => `${s.name} AS (\n${s.sql}\n)`).join(",\n");

	// Merge a final query's own CTEs into the single WITH so the composed SQL
	// stays one valid statement.
	const leadingWith = /^with\s+/i;
	if (leadingWith.test(final)) {
		const rest = final.replace(leadingWith, "");
		return `WITH ${ctes},\n${rest}`;
	}
	return `WITH ${ctes}\n${final}`;
}

/**
 * Validate a single composed statement against the lake and return a bounded
 * headline peek (never the full result).
 *
 * Opens a throwaway in-memory DuckDB instance, ATTACHes the lake READ_ONLY, runs
 * the statement wrapped in `SELECT * FROM (<sql>) … LIMIT n`, and returns the
 * bounded sample. Any SQL/bind/parse failure becomes `{ error }` (agent-fixable).
 * The dedicated connection is interrupted on abort and closed in `finally`, so a
 * cancelled chat run cancels the in-flight statement and never leaks the
 * connection.
 *
 * `signal` is the tool-context abort (DAT-449): forwarded from the run_steps
 * tool's `ctx?.abortSignal`.
 */
export async function runSteps(
	composedSql: string,
	signal?: AbortSignal,
): Promise<RunStepsResult> {
	const sql = stripTrailingSemicolon(composedSql);
	if (sql === "") {
		return { error: "There is no SQL to validate — compose a query first." };
	}

	if (signal?.aborted) return { error: "run_steps aborted before execution." };

	// Acquired inside the try below; held as nullable refs so `close()` is
	// idempotent by NULLING the refs (not a boolean) — that makes the abort-during-
	// acquisition race leak-free: if `onAbort` runs before the refs are set, the
	// later assignment + the finally still close the real handles.
	let instance: DuckDBInstance | null = null;
	let conn: DuckDBConnection | null = null;

	const close = () => {
		if (conn) {
			try {
				conn.closeSync();
			} catch {
				// already closed / never fully opened
			}
			conn = null;
		}
		if (instance) {
			try {
				instance.closeSync();
			} catch {
				// same
			}
			instance = null;
		}
	};
	// On abort, `interrupt()` cancels the in-flight statement — it rejects the
	// pending runAndReadAll() promise, which unwinds into the catch → { error } →
	// finally → close(). closeSync() does NOT interrupt an in-flight query (it
	// leaves the promise unsettled → a hung, leaked worker-thread query that Bun's
	// disabled idle timeout never reaps). The single close() path is the finally.
	const onAbort = () => {
		try {
			conn?.interrupt();
		} catch {
			// not yet open / already gone — finally's close() handles cleanup
		}
	};
	signal?.addEventListener("abort", onAbort, { once: true });

	try {
		instance = await DuckDBInstance.create(":memory:");
		// `cx` is the non-null handle for the DB ops; `conn` (the nullable let the
		// closures read for interrupt/close) is pointed at it.
		const cx = await instance.connect();
		conn = cx;
		await attachLakeReadOnly(cx);

		if (signal?.aborted) return { error: "run_steps aborted." };

		// Wrap + LIMIT cap+1 so a broad result never materializes unbounded AND we
		// can tell a capped result from an exact fit (the one-past probe). The wrap
		// also makes an injected `;` a parser error.
		const peekLimit = HEADLINE_PEEK_ROWS + 1;
		const reader = await cx.runAndReadAll(
			`SELECT * FROM (${sql}) AS _final LIMIT ${peekLimit}`,
		);
		const base = readerToResult(reader);

		const overRowCap = base.rows.length > HEADLINE_PEEK_ROWS;
		const rowCapped = overRowCap
			? base.rows.slice(0, HEADLINE_PEEK_ROWS)
			: base.rows;
		const { rows, truncated: byteTruncated } = boundSampleBytes(rowCapped);

		return {
			ok: true,
			columns: base.columns,
			rowCount: rows.length,
			sample: rows,
			truncated: overRowCap || byteTruncated,
		};
	} catch (err) {
		return { error: err instanceof Error ? err.message : String(err) };
	} finally {
		signal?.removeEventListener("abort", onAbort);
		close();
	}
}
