// connect schema-sniff (DAT-381) — peek a source's schema + sample values
// BEFORE any data is imported or moved.
//
// Two source kinds behind one `ConnectSchema` contract:
//   - database: reuse `probe` (READ_ONLY ATTACH + information_schema + capped
//     per-table sample SELECTs; credentials resolved by source name).
//   - file:     a path sniffed via DuckDB's file readers (read_csv_auto /
//     read_parquet / read_json_auto) + DESCRIBE. The path can be a local file
//     OR an `s3://` URI staged by the upload entry-mode (DAT-386); for the
//     latter the sniff connection registers httpfs + the object-store S3 secret
//     (the same `dataraum_s3` secret the lake reader uses) before the read.
//
// Nothing here ingests or writes: it is a read-only peek the agent shows the
// user via the schema-preview canvas widget. The DuckDB-touching orchestration
// (`connectDatabase` / `connectFile`) is kept thin so the pure mappers below
// carry the shape logic and stay unit-testable without a live driver.

import { DuckDBInstance } from "@duckdb/node-api";
import { z } from "zod";

import { clampRowLimit } from "./limit";
import { probe, SUPPORTED_BACKENDS } from "./probe";
import { type QueryResult, readerToResult } from "./query-result";
import { applyS3Secret } from "./s3-secret";
import { escapeSqlLiteral } from "./sql-escape";

// --- contract ---------------------------------------------------------------

// Mirrors the engine's column metadata: a raw source type string, a 1-based
// position, nullability, and a privacy-safe handful of sample values.
export const ConnectColumnInfo = z.object({
	name: z.string(),
	position: z.number(),
	sourceType: z.string(),
	nullable: z.boolean(),
	sampleValues: z.array(z.unknown()),
});
export type ConnectColumnInfo = z.infer<typeof ConnectColumnInfo>;

export const ConnectTableInfo = z.object({
	name: z.string(),
	// Cheap when free (file readers / Parquet metadata), null when an estimate
	// would cost a full scan (DB attach, large CSV/JSON) — the peek never scans
	// a source just to count it.
	rowCountEstimate: z.number().nullable(),
	columns: z.array(ConnectColumnInfo),
});
export type ConnectTableInfo = z.infer<typeof ConnectTableInfo>;

export const ConnectSchema = z.object({
	sourceKind: z.enum(["file", "database"]),
	// The source name (database) or the file path (file) this schema describes.
	source: z.string(),
	tables: z.array(ConnectTableInfo),
});
export type ConnectSchema = z.infer<typeof ConnectSchema>;

// One flat input shape (LLM-friendly — no top-level anyOf): a `source_kind`
// discriminator with the per-kind fields optional, cross-validated here.
export const ConnectInput = z
	.object({
		source_kind: z.enum(["database", "file"]),
		source_name: z
			.string()
			.optional()
			.describe("Database source name (required when source_kind=database)."),
		backend: z
			.enum(SUPPORTED_BACKENDS)
			.optional()
			.describe("Database backend (required when source_kind=database)."),
		path: z
			.string()
			.optional()
			.describe(
				"File path to sniff (required when source_kind=file): a local path " +
					"or an `s3://` URI staged by the upload entry-mode.",
			),
	})
	.superRefine((v, ctx) => {
		if (v.source_kind === "database") {
			if (!v.source_name)
				ctx.addIssue({
					code: z.ZodIssueCode.custom,
					message: "source_name is required when source_kind=database",
					path: ["source_name"],
				});
			if (!v.backend)
				ctx.addIssue({
					code: z.ZodIssueCode.custom,
					message: "backend is required when source_kind=database",
					path: ["backend"],
				});
		} else if (!v.path) {
			ctx.addIssue({
				code: z.ZodIssueCode.custom,
				message: "path is required when source_kind=file",
				path: ["path"],
			});
		}
	});
export type ConnectInput = z.infer<typeof ConnectInput>;

// --- caps -------------------------------------------------------------------

// Per-column distinct sample values surfaced to the user (privacy-safe peek).
const SAMPLE_VALUE_CAP = 5;
// Rows pulled per table to derive sample values.
const SAMPLE_ROW_LIMIT = 50;
// Upper bound on the information_schema introspection read. A source with more
// total columns than this drops the tail (structurally valid but incomplete) —
// well above any realistic schema; revisit with a `truncated` signal if a real
// source ever approaches it.
const INTROSPECTION_ROW_LIMIT = 10_000;
// Tables we run a sample SELECT against (structure is shown for all tables;
// only this many also get sampleValues — bounds the per-source fan-out).
const SAMPLE_TABLE_CAP = 100;

// System schemas to hide from a connect peek, per backend. DuckDB's ATTACH
// exposes each backend's internal catalogs through information_schema; without
// this filter a MySQL/MSSQL source would surface dozens of internal tables as
// if they were the user's data.
const BACKEND_EXCLUDE_SCHEMAS: Record<string, string[]> = {
	postgres: ["information_schema", "pg_catalog"],
	mysql: ["information_schema", "mysql", "sys", "performance_schema"],
	mssql: ["information_schema", "sys"],
	sqlite: ["information_schema"],
};

// --- pure mappers (no driver) -----------------------------------------------

/** Collect up to `cap` distinct, non-null values for `column` from `rows`. */
export function collectSampleValues(
	rows: Record<string, unknown>[],
	column: string,
	cap = SAMPLE_VALUE_CAP,
): unknown[] {
	const out: unknown[] = [];
	const seen = new Set<string>();
	for (const row of rows) {
		const v = row[column];
		if (v === null || v === undefined) continue;
		// JSON key so nested struct/array values (distinct object refs per row
		// from getRowObjectsJson) dedupe by value, not identity.
		const key = JSON.stringify(v);
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(v);
		if (out.length >= cap) break;
	}
	return out;
}

// information_schema.columns rows the DB path reads.
interface InformationSchemaRow {
	table_schema: string;
	table_name: string;
	column_name: string;
	ordinal_position: number;
	data_type: string;
	is_nullable: string; // 'YES' | 'NO'
}

/** Group information_schema rows into per-table column lists (no samples). */
export function groupInformationSchema(
	rows: InformationSchemaRow[],
): { schema: string; table: string; info: ConnectTableInfo }[] {
	const byKey = new Map<
		string,
		{ schema: string; table: string; info: ConnectTableInfo }
	>();
	for (const r of rows) {
		const key = `${r.table_schema}.${r.table_name}`;
		let entry = byKey.get(key);
		if (!entry) {
			// Qualify the display name only when the schema isn't the default one.
			const name =
				r.table_schema === "public" || r.table_schema === "main"
					? r.table_name
					: `${r.table_schema}.${r.table_name}`;
			entry = {
				schema: r.table_schema,
				table: r.table_name,
				info: { name, rowCountEstimate: null, columns: [] },
			};
			byKey.set(key, entry);
		}
		entry.info.columns.push({
			name: r.column_name,
			position: r.ordinal_position,
			sourceType: r.data_type,
			nullable: r.is_nullable.toUpperCase() === "YES",
			sampleValues: [],
		});
	}
	return Array.from(byKey.values());
}

// DuckDB `DESCRIBE` rows for the file path.
interface DescribeRow {
	column_name: string;
	column_type: string;
	null: string; // 'YES' | 'NO'
}

/** Build a single TableInfo from DESCRIBE + sample rows (file path). */
export function mapDescribeToTable(
	name: string,
	describeRows: DescribeRow[],
	sampleRows: Record<string, unknown>[],
	rowCountEstimate: number | null,
): ConnectTableInfo {
	return {
		name,
		rowCountEstimate,
		columns: describeRows.map((d, i) => ({
			name: d.column_name,
			position: i + 1,
			sourceType: d.column_type,
			nullable: String(d.null).toUpperCase() === "YES",
			sampleValues: collectSampleValues(sampleRows, d.column_name),
		})),
	};
}

// --- orchestration (driver) -------------------------------------------------

const FILE_READERS: { ext: RegExp; reader: string }[] = [
	{ ext: /\.(csv|tsv|txt)$/i, reader: "read_csv_auto" },
	{ ext: /\.(parquet|pq)$/i, reader: "read_parquet" },
	{ ext: /\.(json|ndjson|jsonl)$/i, reader: "read_json_auto" },
];

export function readerForPath(path: string): string {
	const match = FILE_READERS.find((r) => r.ext.test(path));
	if (!match) {
		throw new Error(
			`Unsupported file type for "${path}". Supported: .csv/.tsv/.txt, .parquet, .json/.ndjson/.jsonl.`,
		);
	}
	return match.reader;
}

function quoteIdent(ident: string): string {
	return `"${ident.replace(/"/g, '""')}"`;
}

function baseName(path: string): string {
	const parts = path.split(/[/\\]/);
	return parts[parts.length - 1] || path;
}

async function connectDatabase(
	sourceName: string,
	backend: (typeof SUPPORTED_BACKENDS)[number],
): Promise<ConnectSchema> {
	const excluded = (BACKEND_EXCLUDE_SCHEMAS[backend] ?? ["information_schema"])
		.map((s) => `'${escapeSqlLiteral(s)}'`)
		.join(", ");

	const introspection = await probe({
		source_name: sourceName,
		backend,
		sql: `SELECT table_schema, table_name, column_name, ordinal_position, data_type, is_nullable
		      FROM information_schema.columns
		      WHERE table_schema NOT IN (${excluded})
		      ORDER BY table_schema, table_name, ordinal_position`,
		limit: INTROSPECTION_ROW_LIMIT,
	});

	const grouped = groupInformationSchema(
		introspection.rows as unknown as InformationSchemaRow[],
	);

	// Sample the first N tables; structure is already present for all of them.
	const sampled = grouped.slice(0, SAMPLE_TABLE_CAP);
	await Promise.all(
		sampled.map(async ({ schema, table, info }) => {
			const sample = await probe({
				source_name: sourceName,
				backend,
				sql: `SELECT * FROM ${quoteIdent(schema)}.${quoteIdent(table)}`,
				limit: SAMPLE_ROW_LIMIT,
			});
			for (const col of info.columns) {
				col.sampleValues = collectSampleValues(sample.rows, col.name);
			}
		}),
	);

	return {
		sourceKind: "database",
		source: sourceName,
		tables: grouped.map((g) => g.info),
	};
}

/** True when `path` is an object-store URI the DuckDB reader resolves via httpfs. */
function isS3Path(path: string): boolean {
	return path.startsWith("s3://");
}

async function connectFile(path: string): Promise<ConnectSchema> {
	const reader = readerForPath(path);
	const from = `${reader}('${escapeSqlLiteral(path)}')`;

	const instance = await DuckDBInstance.create(":memory:");
	const conn = await instance.connect();
	try {
		// An `s3://` path (an upload staged by DAT-386) needs httpfs + the
		// object-store secret before the reader can open it. A local path needs
		// neither, so only register for s3:// — and registering it there is the
		// SAME `dataraum_s3` secret the lake reader uses (s3-secret.ts), so the
		// upload sniff and the lake read resolve the bucket identically.
		if (isS3Path(path)) {
			await applyS3Secret(conn);
		}
		const describe: QueryResult = readerToResult(
			await conn.runAndReadAll(`DESCRIBE SELECT * FROM ${from}`),
		);
		const sampleLimit = clampRowLimit(SAMPLE_ROW_LIMIT);
		const sample: QueryResult = readerToResult(
			await conn.runAndReadAll(`SELECT * FROM ${from} LIMIT ${sampleLimit}`),
		);

		let rowCountEstimate: number | null;
		if (reader === "read_parquet") {
			// Parquet keeps its row count in footer metadata, so count(*) is cheap.
			const countRes: QueryResult = readerToResult(
				await conn.runAndReadAll(`SELECT count(*) AS n FROM ${from}`),
			);
			const raw = countRes.rows[0]?.n;
			const n = raw === undefined || raw === null ? null : Number(raw);
			rowCountEstimate = n !== null && Number.isFinite(n) ? n : null;
		} else {
			// CSV/JSON: count(*) is a full scan, so only report it when the bounded
			// sample already saw the whole file (fewer rows than the cap). Otherwise
			// leave it null rather than scan a potentially huge file for a preview.
			rowCountEstimate =
				sample.rows.length < sampleLimit ? sample.rows.length : null;
		}

		const table = mapDescribeToTable(
			baseName(path),
			describe.rows as unknown as DescribeRow[],
			sample.rows,
			rowCountEstimate,
		);
		return { sourceKind: "file", source: path, tables: [table] };
	} catch (err) {
		throw new Error(
			`connect(file='${path}') failed: ${
				err instanceof Error ? err.message : String(err)
			}`,
		);
	} finally {
		conn.closeSync();
		instance.closeSync();
	}
}

/**
 * Peek a source's schema + sample values without importing it.
 *
 * Dispatches on `source_kind`: a configured database source (by name, via the
 * READ_ONLY probe ATTACH) or a server-readable file path (via DuckDB's file
 * readers). Returns one unified `ConnectSchema`.
 */
export async function connect(input: ConnectInput): Promise<ConnectSchema> {
	const parsed = ConnectInput.parse(input);
	if (parsed.source_kind === "database") {
		// superRefine guarantees both are present for this branch.
		return connectDatabase(
			parsed.source_name as string,
			parsed.backend as (typeof SUPPORTED_BACKENDS)[number],
		);
	}
	return connectFile(parsed.path as string);
}
