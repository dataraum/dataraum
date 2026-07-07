// Ad-hoc drill composition (DAT-672, tier-A-only since DAT-703).
//
// Tier A wraps a DETAIL result in an outer GROUP BY — it needs every
// referenced column ON the result, so it fits ad-hoc grids (an answer-agent
// query's visible columns) and nothing else. Everything deeper — a scalar
// metric aggregate whose dimensions live inside the statement — is composed
// per NODE from its persisted clause parts instead (`parts.ts` behind
// `/api/drill/node`, parts-at-source): tier-B AST injection via
// `json_serialize_sql` is DELETED with DAT-703 — the drill path never parses
// or mutates SQL text anymore. Ad-hoc grids join the parts contract when the
// answer agent does (separate cut).
//
// Everything runs on a caller-provided connection: the API route passes a
// lake connection scoped like the engine's (`USE lake.typed`), unit tests an
// in-memory one.

import type { DuckDBConnection } from "@duckdb/node-api";

import {
	type BaseColumn,
	type ComposedDrill,
	composeTierA,
	type DrillPinValue,
	type DrillStep,
	referencedColumns,
} from "./drill";

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

	try {
		const columns = await describeColumns(conn, composed.sql, composed.params);
		return { ok: true, sql: composed.sql, params: composed.params, columns };
	} catch (err) {
		return refuse(errorLine(err));
	}
}
