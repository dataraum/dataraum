// look_values (DAT-621): the value-set DRILL tool. The answer agent / user gets the
// COMPLETE distinct value-set for one or more columns on demand — the open-ended
// counterpart to the engine GraphAgent's bounded baseline (which inlines low-card
// value-sets up to a reasonable top). "Don't cut, don't guess": the map (<schema>/
// <dimensions>) shows each column's size + a sample; when the agent needs the full set
// to ground a filter, it pulls it here — a live freq-ordered DISTINCT against the
// READ_ONLY lake (the same primitive run_steps uses), batched so one call resolves many
// columns (no 30-round-trip fishing). Outer/analyse deep-dive tool, like look_profile —
// the inner sub-agent stays lean (DAT-608 exhaustion).

import { toolDefinition } from "@tanstack/ai";
import { and, eq, inArray, isNull } from "drizzle-orm";
import { z } from "zod";
import { metadataDb } from "#/db/metadata/client";
import { columns, sources, tables } from "#/db/metadata/schema";
import { LAKE_ALIAS, withLakeConnection } from "../duckdb/lake";
import { readerToResult } from "../duckdb/query-result";

// Enriched views live in the `typed` DuckDB schema (mirror the engine's schema_for_layer);
// a column's own distinct values come from its typed table regardless.
function schemaForLayer(layer: string): string {
	return layer === "enriched" ? "typed" : layer;
}

// The drill is a deliberate, on-demand pull, so the ceiling is generous (far above the
// engine's ~200 baseline). +1 row detects truncation without a second COUNT(DISTINCT).
const LOOK_VALUES_LIMIT = 1000;

const ValueCount = z.object({ value: z.unknown(), count: z.number() });

const ColumnValues = z.object({
	column_id: z.string(),
	table_name: z.string().nullable(),
	column_name: z.string().nullable(),
	/** Distinct values returned (≤ LOOK_VALUES_LIMIT), freq-ordered. */
	values: z.array(ValueCount),
	/** False when more than LOOK_VALUES_LIMIT distinct values exist (this is a sample). */
	complete: z.boolean(),
	/** Set when the column id didn't resolve or the lake read failed. */
	error: z.string().nullable(),
});

const LookValuesResult = z.object({ columns: z.array(ColumnValues) });
export type LookValuesResult = z.infer<typeof LookValuesResult>;

/**
 * Project the live freq-ordered rows into the value list + completeness flag (pure).
 * The query pulls `limit + 1`; `complete` is false when that extra row came back (more
 * distinct values exist → the list is a sample, not exhaustive).
 */
export function projectValueRows(
	rows: Record<string, unknown>[],
	limit: number,
): { values: z.infer<typeof ValueCount>[]; complete: boolean } {
	return {
		values: rows
			.slice(0, limit)
			.map((r) => ({ value: r.value, count: Number(r.count) })),
		complete: rows.length <= limit,
	};
}

interface ResolvedColumn {
	columnId: string;
	columnName: string;
	tableName: string;
	layer: string;
}

async function resolveColumns(
	columnIds: string[],
): Promise<Map<string, ResolvedColumn>> {
	const rows = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			tableName: tables.tableName,
			layer: tables.layer,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(
			and(isNull(sources.archivedAt), inArray(columns.columnId, columnIds)),
		);
	const out = new Map<string, ResolvedColumn>();
	for (const r of rows) {
		if (r.columnId && r.columnName && r.tableName) {
			out.set(r.columnId, {
				columnId: r.columnId,
				columnName: r.columnName,
				tableName: r.tableName,
				layer: r.layer ?? "typed",
			});
		}
	}
	return out;
}

async function lookValues(input: {
	column_ids: string[];
}): Promise<LookValuesResult> {
	const resolved = await resolveColumns(input.column_ids);
	return withLakeConnection(async (conn) => {
		const out: z.infer<typeof ColumnValues>[] = [];

		for (const columnId of input.column_ids) {
			const col = resolved.get(columnId);
			if (!col) {
				out.push({
					column_id: columnId,
					table_name: null,
					column_name: null,
					values: [],
					complete: false,
					error: "column id did not resolve to a live table",
				});
				continue;
			}
			const address = `${LAKE_ALIAS}.${schemaForLayer(col.layer)}."${col.tableName}"`;
			try {
				const reader = await conn.runAndReadAll(
					`SELECT "${col.columnName}" AS value, COUNT(*) AS count FROM ${address} ` +
						`WHERE "${col.columnName}" IS NOT NULL ` +
						`GROUP BY 1 ORDER BY count DESC, value LIMIT ${LOOK_VALUES_LIMIT + 1}`,
				);
				const { values, complete } = projectValueRows(
					readerToResult(reader).rows,
					LOOK_VALUES_LIMIT,
				);
				out.push({
					column_id: columnId,
					table_name: col.tableName,
					column_name: col.columnName,
					values,
					complete,
					error: null,
				});
			} catch (err) {
				out.push({
					column_id: columnId,
					table_name: col.tableName,
					column_name: col.columnName,
					values: [],
					complete: false,
					error: `lake read failed: ${err}`,
				});
			}
		}
		return { columns: out };
	});
}

export const lookValuesTool = toolDefinition({
	name: "look_values",
	description:
		"Drill the COMPLETE distinct value-set of one or more columns — freq-ordered " +
		"`{value, count}`, live from the lake. Use this to ground a filter when the " +
		"<schema>/<dimensions> map shows a column has more values than are inlined (it " +
		"carries a size + sample): pass the column_ids (from look_table) you need and " +
		"get their full value lists in ONE call. `complete: false` means the column has " +
		`more than ${LOOK_VALUES_LIMIT} distinct values (you got the top — treat as a ` +
		"sample, not exhaustive). Read-only.",
	inputSchema: z.object({
		column_ids: z
			.array(z.string())
			.min(1)
			.describe(
				"Columns to fetch full value-sets for (column_ids from look_table).",
			),
	}),
	outputSchema: LookValuesResult,
}).server((input) => lookValues(input));
