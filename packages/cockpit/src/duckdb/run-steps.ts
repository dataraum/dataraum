// `run_steps` — the query sub-agent's SQL VALIDATOR (DAT-485 P1).
//
// The `answer` tool's nested chat() sub-agent composes a question's answer as a
// set of concept-named steps + a combining `final_sql` (the snippet-reuse unit,
// mirroring the engine's GraphAgent decomposition). Before it states a number to
// the user it calls run_steps ONCE to confirm "the SQL works" and read a BOUNDED
// headline peek — never the full result. The full result streams browser-side
// via the composed `{sql}` grid handle (DAT-490); this path designs out the
// inner-context-overflow risk by capping what re-enters the model.
//
// It is a VALIDATOR, not the executor:
//   - opens its OWN throwaway in-memory DuckDB instance + READ_ONLY lake ATTACH
//     (mirror probe.ts/connect.ts; the shared lake reader is memoized and can't
//     be closed mid-call). A dedicated connection means it CAN honor abort —
//     aborting the chat run closes it, interrupting any in-flight statement.
//   - materializes each step as a cursor-local `CREATE TEMP VIEW` (auto-dropped
//     on close, invisible to concurrent callers — two answers can validate
//     same-named steps without colliding), then runs `final_sql` referencing
//     them, wrapped in a `LIMIT` so a broad final can't materialize unbounded.
//   - returns `{ ok, columns, rowCount, sample, truncated }` (the bounded peek)
//     or `{ error }` (an agent-fixable SQL/bind failure the model repairs
//     in-loop) — NEVER the full result set.
//
// `composeStandalone` is the pure bridge to the browser grid: the grid's
// `/api/run-sql` runs ONE statement and can't pre-create temp views, so the
// decomposed `{steps, final_sql}` is folded into a single CTE-wrapped statement
// for the handle. run_steps validates the temp-view form (faithful to the
// engine model + cursor-isolation); the grid runs the equivalent CTE form.

import {
	type DuckDBConnection,
	DuckDBInstance,
	type Json,
} from "@duckdb/node-api";

import { boundSampleBytes } from "./agent-sample";
import { attachLakeReadOnly } from "./lake";
import { readerToResult } from "./query-result";

/** A single decomposed step: a name (becomes a temp view / CTE) + standalone SQL. */
export interface RunStep {
	/** A SQL identifier — the temp-view / CTE name `final_sql` references. */
	name: string;
	/** Standalone DuckDB SQL (a SELECT). Must not reference other steps' views. */
	sql: string;
}

export interface RunStepsInput {
	/** Ordered concept steps; each becomes a cursor-local temp view. May be empty. */
	steps: RunStep[];
	/** The SQL combining the step views into the final result (a single SELECT). */
	finalSql: string;
}

/** The validator's success shape — a BOUNDED headline peek, never the full result. */
export interface RunStepsOk {
	ok: true;
	/** Output column names of `final_sql`. */
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

// A step name is interpolated raw into `CREATE TEMP VIEW <name>` and into the
// composed CTE, so it MUST be a bare SQL identifier — letters/underscore then
// word chars. This is the injection gate on the name (the SQL bodies are the
// model's own queries, bounded by READ_ONLY ATTACH like run_sql).
const STEP_NAME_RE = /^[A-Za-z_]\w*$/;

/** Headline-peek row cap — small on purpose: the model only needs to SEE the
 * answer (often scalar), not the whole result. The full result streams in the
 * grid. Independent of any LIMIT inside `final_sql`. */
export const HEADLINE_PEEK_ROWS = 50;

/** Strip a trailing statement terminator + whitespace so `final_sql` can be
 * wrapped as a subquery (`SELECT * FROM (<final>) …`) and merged into a CTE. */
function stripTrailingSemicolon(sql: string): string {
	return sql.trim().replace(/;\s*$/, "");
}

/**
 * Validate the step names: each must be a bare SQL identifier and unique (they
 * become temp-view / CTE names). Returns an agent-fixable message, or null when
 * all names are valid. Pure — unit-tested directly.
 */
export function validateStepNames(steps: RunStep[]): string | null {
	const seen = new Set<string>();
	for (const step of steps) {
		if (!STEP_NAME_RE.test(step.name)) {
			return (
				`Invalid step name '${step.name}': a step name must be a SQL identifier ` +
				`(a letter or underscore followed by letters/digits/underscores), since ` +
				`it becomes a temp-view name. Rename the step after its business concept.`
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
 * Fold `{steps, final_sql}` into a SINGLE standalone statement for the browser
 * grid (which runs one statement and can't pre-create temp views). Each step
 * becomes a CTE; `final_sql` references them by the SAME names it used as temp
 * views, so the two forms are equivalent for standalone (non-cross-referencing)
 * steps. No steps → `final_sql` verbatim. When `final_sql` brings its OWN
 * leading `WITH`, its CTEs are merged into the one `WITH` (never the invalid
 * `WITH … WITH …`). Pure — unit-tested directly.
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
 * Validate a decomposed query against the lake and return a bounded headline
 * peek (never the full result).
 *
 * Opens a throwaway in-memory DuckDB instance, ATTACHes the lake READ_ONLY,
 * creates one cursor-local temp view per step, runs `final_sql` wrapped in a
 * `LIMIT`, and returns the bounded sample. Any SQL/bind failure becomes
 * `{ error }` (agent-fixable). The dedicated connection is closed in `finally`
 * AND on abort, so a cancelled chat run interrupts an in-flight statement and
 * never leaks the connection.
 *
 * `signal` is the tool-context abort (DAT-449): forwarded from the run_steps
 * tool's `ctx?.abortSignal`.
 */
export async function runSteps(
	input: RunStepsInput,
	signal?: AbortSignal,
): Promise<RunStepsResult> {
	const nameError = validateStepNames(input.steps);
	if (nameError) return { error: nameError };

	const finalSql = stripTrailingSemicolon(input.finalSql);
	if (finalSql === "") {
		return {
			error: "final_sql is required and must be a non-empty SQL statement.",
		};
	}

	if (signal?.aborted) return { error: "run_steps aborted before execution." };

	const instance: DuckDBInstance = await DuckDBInstance.create(":memory:");
	const conn: DuckDBConnection = await instance.connect();

	let closed = false;
	const close = () => {
		if (closed) return;
		closed = true;
		try {
			conn.closeSync();
		} catch {
			// already closed / never fully opened
		}
		try {
			instance.closeSync();
		} catch {
			// same
		}
	};
	// Aborting the chat run closes the throwaway connection, interrupting any
	// in-flight CREATE VIEW / SELECT — unlike run_sql's shared reader, which
	// can't be closed. `once` + the finally cleanup keep this leak-free.
	const onAbort = () => close();
	signal?.addEventListener("abort", onAbort, { once: true });

	try {
		await attachLakeReadOnly(conn);

		for (const step of input.steps) {
			if (signal?.aborted) return { error: "run_steps aborted." };
			// `step.name` is validated to a bare identifier; the SQL body is the
			// model's own query (READ_ONLY ATTACH blocks any write to the lake, the
			// same trust model as run_sql). Temp views are cursor-local: auto-dropped
			// on close and invisible to other connections, so concurrent answers
			// validating same-named steps don't collide.
			await conn.run(`CREATE TEMP VIEW ${step.name} AS ${step.sql}`);
		}

		if (signal?.aborted) return { error: "run_steps aborted." };

		// Wrap + LIMIT cap+1 so a broad final never materializes unbounded AND we
		// can tell a capped result from an exact fit (the one-past probe).
		const peekLimit = HEADLINE_PEEK_ROWS + 1;
		const reader = await conn.runAndReadAll(
			`SELECT * FROM (${finalSql}) AS _final LIMIT ${peekLimit}`,
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
