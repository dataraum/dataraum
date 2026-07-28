// Ad-hoc drill composition (DAT-672, tier-A-only since DAT-703).
//
// Tier A wraps a DETAIL result in an outer GROUP BY — it needs every
// referenced column ON the result, so it fits ad-hoc grids (an answer-agent
// query's visible columns) and nothing else. Everything deeper — a scalar
// metric aggregate whose dimensions live inside the statement — is composed
// per NODE from its persisted clause parts instead (`parts.ts` behind
// `/api/drill/node`, parts-at-source): tier-B AST injection via
// `json_serialize_sql` is DELETED with DAT-703 — the drill path never parses
// or mutates SQL text anymore. An ANSWER joined the parts contract in DAT-678
// by DECLARING its clause parts (`answer-source.ts`, `/api/drill/parts`) —
// still no parsing, and still proven before it is trusted. Tier A remains what
// every surface falls back to when there is no such declaration: it is the one
// path that needs nothing but the result in front of it.
//
// Everything runs on a caller-provided connection: the API route passes a
// lake connection scoped like the engine's (`USE lake.typed`), unit tests an
// in-memory one.

import type { DuckDBConnection } from "@duckdb/node-api";

import {
	type BaseColumn,
	type ComposedDrill,
	composeTierA,
	countAlias,
	type DrillPinValue,
	type DrillStep,
	referencedColumns,
	sliceColumns,
} from "./drill";
import { quoteIdentifier } from "./grid-query";

export interface DrillComposeRequest {
	sql: string;
	params: DrillPinValue[];
	steps: DrillStep[];
}

export type DrillComposeResult =
	| {
			ok: true;
			sql: string;
			params: DrillPinValue[];
			columns: BaseColumn[];
	  }
	| { ok: false; reason: string };

const refuse = (reason: string): DrillComposeResult => ({ ok: false, reason });

/** The first line of a DuckDB error — `Binder Error: …` etc.; the rest is
 *  candidate-list noise the refusal state doesn't need. (Exported for the
 *  `/api/drill/node` route, which shares the binder-as-gate refusal shape.) */
export const errorLine = (err: unknown): string =>
	(err instanceof Error ? err.message : String(err)).split("\n")[0] ??
	"unknown error";

/** DESCRIBE the (possibly parameterized) query — DuckDB binds and plans
 *  without executing, so this both yields the result columns and surfaces
 *  binder errors. Params are required for binding parameterized SQL. */
export async function describeColumns(
	conn: DuckDBConnection,
	sql: string,
	params: DrillPinValue[],
): Promise<BaseColumn[]> {
	const reader =
		params.length > 0
			? await conn.runAndReadAll(`DESCRIBE ${sql}`, params)
			: await conn.runAndReadAll(`DESCRIBE ${sql}`);
	return reader.getRowObjectsJson().map((r) => ({
		name: String(r.column_name),
		type: String(r.column_type),
	}));
}

/**
 * Did the grouping actually FOLD anything? (DAT-671)
 *
 * Tier A will happily group a result by a column that is already unique per row
 * — its own grain. The statement binds, the grid renders, every `count` is 1 and
 * every `sum(x)` is the original x under a new header: a no-op presented as
 * analysis. It was spotted on a minted report, where "revenue by account" was
 * already one row per account.
 *
 * The test is POST-EXECUTION row counts, never a reading of the base SQL. We do
 * not know (and must not guess) what grain the caller's query is at — only the
 * data can say, and it says it by folding or not folding. One statement answers
 * both halves, because the tier-A wrap always projects `COUNT(*)`: the number of
 * GROUPS is `COUNT(*)` over the drilled result, and the number of rows that went
 * INTO the grouping is the SUM of those per-group counts. Equal ⟺ every group
 * holds exactly one row ⟺ nothing folded.
 *
 * This executes the drilled aggregate, which the grid is about to execute again
 * — a deliberate cost. A refusal the user can act on is worth more than a scan
 * saved on a result that was going to be meaningless.
 *
 * An empty base folds nothing either, but "no rows" is honestly empty rather
 * than wrongly-grained, so a NULL scanned-count passes.
 *
 * A SINGLE scanned row proves nothing about grain — one row cannot fold into
 * fewer than one group whatever the dimension is. That case is reached on the
 * ordinary drill-down journey (slice by region, click the EU row to pin it, and
 * the slice + its own pin now describe one row), so treating it as evidence
 * would refuse the user's own next step. THIS is the correctness carve-out.
 *
 * The caller's `dims.length > 0` check is only a COST guard, not a second
 * carve-out: with no slice there is no GROUP BY, the statement returns exactly
 * one row, and the single-row rule above already excludes it. Skipping the
 * probe there saves a scan that could never refuse.
 *
 * KNOWN LIMITATION, not currently fixed: slice A → pin A → slice B over 2-3
 * rows under the pin still refuses, even though it is the same journey the
 * single-row carve-out protects — the counts genuinely are all 1 there, so the
 * statement is true, but the user is mid-descent rather than mis-grained. The
 * message names the dimension, so it stays actionable; widening the carve-out
 * would need the drill's own step history, which this seam does not have.
 */
async function foldsNothing(
	conn: DuckDBConnection,
	composed: ComposedDrill,
	baseColumns: BaseColumn[],
): Promise<boolean> {
	const count = quoteIdentifier(countAlias(baseColumns));
	const probe =
		`SELECT COUNT(*) AS groups, SUM(${count}) AS scanned ` +
		`FROM (${composed.sql}) AS _fold`;
	try {
		const reader =
			composed.params.length > 0
				? await conn.runAndReadAll(probe, composed.params)
				: await conn.runAndReadAll(probe);
		const row = reader.getRowObjectsJson()[0];
		if (!row || row.scanned === null || row.scanned === undefined) return false;
		// Wide integer types arrive as strings — compare them as such rather than
		// through a lossy Number().
		const scanned = String(row.scanned);
		if (scanned === "0" || scanned === "1") return false;
		return String(row.groups) === scanned;
	} catch (err) {
		// Fails OPEN: the statement already bound, so this is an execution problem
		// (timeout, resource limit), not evidence about grain. Never manufacture a
		// refusal out of not knowing.
		console.info("drill_fold_probe_failed", { reason: errorLine(err) });
		return false;
	}
}

/**
 * Compose a drilled statement from a base query + step stack — tier A only:
 * every referenced column must be present on the base RESULT (per DESCRIBE);
 * anything else refuses honestly (this surface drills what it can see). The
 * output is validated with a bound DESCRIBE before it is returned — the
 * caller never receives SQL that will not bind, and a binder failure IS the
 * refusal.
 */
export async function composeDrill(
	conn: DuckDBConnection,
	req: DrillComposeRequest,
): Promise<DrillComposeResult> {
	if (req.steps.length === 0) return refuse("no drill steps");

	let baseColumns: BaseColumn[];
	try {
		baseColumns = await describeColumns(conn, req.sql, req.params);
	} catch (err) {
		return refuse(`base query does not bind: ${errorLine(err)}`);
	}

	const baseNames = new Set(baseColumns.map((c) => c.name));
	const outside = referencedColumns(req.steps).filter((c) => !baseNames.has(c));
	if (outside.length > 0) {
		return refuse(
			`the drill references columns not on this result (${outside.join(", ")}) — an ad-hoc grid slices only its own columns`,
		);
	}

	const composed: ComposedDrill = composeTierA(
		req.sql,
		req.params,
		baseColumns,
		req.steps,
	);

	let columns: BaseColumn[];
	try {
		columns = await describeColumns(conn, composed.sql, composed.params);
	} catch (err) {
		return refuse(errorLine(err));
	}

	// `dims.length > 0` is a cost guard only — see foldsNothing: an ungrouped
	// composition returns one row, which the single-row rule already passes.
	const dims = sliceColumns(req.steps);
	if (dims.length > 0 && (await foldsNothing(conn, composed, baseColumns))) {
		return refuse(
			`already at this grain — nothing to fold: grouping by ${dims.join(", ")} leaves one row per group, so the counts are all 1 and the totals only relabel the rows already on screen`,
		);
	}

	return { ok: true, sql: composed.sql, params: composed.params, columns };
}
