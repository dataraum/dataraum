// list_tables tool (DAT-353) — read the workspace's tables, optionally scoped
// to one source.
//
// Pure read via the Drizzle metadata client (ws_<id>.tables). No approval. The
// DB query is covered by the gated integration test (skips without
// METADATA_DATABASE_URL), mirroring teach's split — no mocking.

import { toolDefinition } from "@tanstack/ai";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { tables } from "../db/metadata/schema";

const TableSummary = z.object({
	table_id: z.string(),
	source_id: z.string(),
	table_name: z.string(),
	layer: z.string(),
	row_count: z.number().nullable(),
});
export type TableSummary = z.infer<typeof TableSummary>;

export interface ListTablesInput {
	source_id?: string;
}

/** Tables in the active workspace, optionally filtered to one source. */
export async function listTables(
	input: ListTablesInput = {},
): Promise<TableSummary[]> {
	const rows = await metadataDb
		.select({
			tableId: tables.tableId,
			sourceId: tables.sourceId,
			tableName: tables.tableName,
			layer: tables.layer,
			rowCount: tables.rowCount,
		})
		.from(tables)
		.where(input.source_id ? eq(tables.sourceId, input.source_id) : undefined)
		.orderBy(tables.tableName);

	return rows.map((r) => ({
		table_id: r.tableId,
		source_id: r.sourceId,
		table_name: r.tableName,
		layer: r.layer,
		row_count: r.rowCount,
	}));
}

export const listTablesTool = toolDefinition({
	name: "list_tables",
	description:
		"List tables in the workspace, optionally filtered to one source. Returns each table's id, source id, name, layer, and row count.",
	inputSchema: z.object({
		source_id: z
			.string()
			.optional()
			.describe("Restrict to tables produced by this source id."),
	}),
	outputSchema: z.array(TableSummary),
}).server((input) => listTables(input));
