// Synthetic ConnectSchema assembly for the probe staging hub (DAT-594).
//
// `frame` induces a business model from a `ConnectSchema` (DAT-381) — but the
// staging hub assembles a HETEROGENEOUS set (probed SQL queries + uploaded files)
// before there is any single connected source. So we synthesize ONE ConnectSchema
// from the UNION of the staged items' schemas: one `tables[]` entry per staged item
// (a query's DESCRIBE → columns+samples; a file's sniff → its ConnectSchema table),
// and `frame` accepts it like any other (no tie to the `connect` tool).
//
// PURE assembly: no driver, no I/O — the per-item schemas are sniffed elsewhere
// (`duckdb/probe.probeDescribe` for queries, `duckdb/connect.sniffFileSchema` for
// files) and handed in. Unit-testable without a live source.

import {
	type ConnectColumnInfo,
	type ConnectSchema,
	type ConnectTableInfo,
	collectSampleValues,
} from "../duckdb/connect";
import type { ProbeSchema } from "../duckdb/probe";

/** One staged query's sniffed schema + the source name it will import as — the
 * `frame` table is keyed by that name so the induced concepts anchor to it. */
export interface StagedQuerySchema {
	source_name: string;
	schema: ProbeSchema;
}

/** Turn a probed query's DESCRIBE + sample into one `ConnectTableInfo` (named by
 * the source it imports as), deriving per-column sample values from the sample
 * rows exactly as the file/db sniff does (collectSampleValues). */
export function queryToConnectTable(item: StagedQuerySchema): ConnectTableInfo {
	const columns: ConnectColumnInfo[] = item.schema.columns.map((c, i) => ({
		name: c.name,
		position: i + 1,
		sourceType: c.type,
		// A probed query's projection nullability isn't reliably known from DESCRIBE
		// (it reports the expression type, not a NOT NULL constraint), so default to
		// nullable — induction reads samples + names, not this flag.
		nullable: true,
		sampleValues: collectSampleValues(item.schema.sampleRows, c.name),
	}));
	return {
		name: item.source_name,
		// A query's row count would cost a full scan — left null, like the DB sniff.
		rowCountEstimate: null,
		columns,
	};
}

/**
 * Assemble one synthetic `ConnectSchema` from the UNION of the staging set's
 * sniffed schemas — one `tables[]` entry per staged query (named by its import
 * source name) and per staged file (its sniffed table(s)). `frame` reads this for
 * induction context exactly like a single-source connect schema.
 *
 * `sourceKind` reflects the set's composition (database when any query is present,
 * else file) and `source` is a human label of the set — both informational; the
 * induction uses `tables[]`. At least one item is required (an empty set has
 * nothing to frame against).
 */
export function assembleStagingSchema(input: {
	queries: StagedQuerySchema[];
	files: ConnectSchema[];
}): ConnectSchema {
	const queryTables = input.queries.map(queryToConnectTable);
	// Each file sniff is its own ConnectSchema (one table for a flat file) — flatten
	// their tables into the union.
	const fileTables = input.files.flatMap((f) => f.tables);
	const tables = [...queryTables, ...fileTables];
	if (tables.length === 0) {
		throw new Error(
			"Cannot frame an empty staging set — stage a query or a file first.",
		);
	}
	const sourceKind: ConnectSchema["sourceKind"] =
		input.queries.length > 0 ? "database" : "file";
	const count = tables.length;
	const source = `import set (${count} ${count === 1 ? "item" : "items"})`;
	return { sourceKind, source, tables };
}
