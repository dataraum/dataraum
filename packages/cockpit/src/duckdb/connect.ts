// connect schema-sniff (DAT-381) — peek a source's schema + sample values
// BEFORE any data is imported or moved.
//
// Two source kinds behind one `ConnectSchema` contract:
//   - database: reuse `probe` (READ_ONLY ATTACH + information_schema + capped
//     per-table sample SELECTs; credentials resolved by source name).
//   - file:     an `s3://` URI in the configured object-store bucket — either an
//     upload staged by the entry-mode (DAT-386) or an existing bucket object —
//     sniffed via DuckDB's file readers (read_csv_auto / read_parquet /
//     read_json_auto) + DESCRIBE. The sniff connection registers httpfs + the
//     object-store S3 secret (the same `dataraum_s3` secret the lake reader
//     uses) before the read. The path is validated to the single allowed shape
//     `s3://<bucket>/<key>` (see `validateBucketS3Path`) — local paths, `file://`,
//     other buckets, and cred-in-URL forms are rejected. A path that opened any
//     other container FS file would be an arbitrary-file-read hole (DAT-386).
//
// Nothing here ingests or writes: it is a read-only peek the agent shows the
// user via the schema-preview canvas widget. The DuckDB-touching orchestration
// (`connectDatabase` / `connectFile`) is kept thin so the pure mappers below
// carry the shape logic and stay unit-testable without a live driver.

import { DuckDBInstance } from "@duckdb/node-api";
import { z } from "zod";

import { config } from "../config";
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
				"Object path to sniff (required when source_kind=file): an `s3://` URI " +
					"in the configured object-store bucket — an upload staged by the entry-mode " +
					"or an existing bucket object.",
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
		} else {
			// Pre-SQL gate: only `s3://<configured-bucket>/<key>` is reachable.
			// Local paths / file:// / other buckets / cred-in-URL forms would be an
			// arbitrary-file-read hole (DAT-386). Defense-in-depth also re-checks in
			// connectFile before any SQL is built.
			const check = validateBucketS3Path(v.path);
			if (!check.ok)
				ctx.addIssue({
					code: z.ZodIssueCode.custom,
					message: `invalid file path: ${check.reason}`,
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

// --- s3:// path validation (security) ---------------------------------------

// The ONLY shape `connect(source_kind=file)` accepts: `s3://<bucket>/<key>`
// where `<bucket>` is the configured object-store bucket (`config.s3Bucket`).
// Anything else — a local path, `file://`, `../`, a bare name, another bucket,
// or a cred-in-URL `s3://key:secret@bucket/...` form — is REFUSED. Reading any
// other path would let `connect` read an arbitrary container FS file (e.g.
// `/etc/passwd`, `/app/.env`) or any bucket on the endpoint (DAT-386).
//
// Hand-parsed rather than via `URL` so the rules are explicit and total: `URL`
// lowercases the host, silently accepts userinfo, and percent-decodes — none of
// which we want to reason about for an allowlist check. The host (authority)
// must be EXACTLY the bucket: no `user:pass@`, no `:port`, no empty key.
export function validateBucketS3Path(path: string): {
	ok: boolean;
	reason?: string;
} {
	if (!path.startsWith("s3://")) {
		return {
			ok: false,
			reason:
				"path must be an `s3://` URI in the configured object-store bucket " +
				`(s3://${config.s3Bucket}/<key>); local paths and other schemes are not allowed`,
		};
	}
	const rest = path.slice("s3://".length);
	const slash = rest.indexOf("/");
	// Need an authority AND a non-empty key after the first slash.
	if (slash <= 0 || slash === rest.length - 1) {
		return {
			ok: false,
			reason: `path must be of the form s3://${config.s3Bucket}/<key> with a non-empty key`,
		};
	}
	const authority = rest.slice(0, slash);
	const key = rest.slice(slash + 1);
	// Authority must be EXACTLY the bucket — reject `key:secret@bucket`,
	// `bucket:port`, and any other bucket.
	if (authority !== config.s3Bucket) {
		return {
			ok: false,
			reason:
				`path bucket must be the configured object-store bucket ` +
				`'${config.s3Bucket}' (got authority '${authority}')`,
		};
	}
	// Defense against a `..`/absolute-escape slipped into the key.
	if (key.startsWith("/") || key.split("/").some((seg) => seg === "..")) {
		return {
			ok: false,
			reason: "path key must not contain `..` segments or a leading slash",
		};
	}
	// Reject DuckDB glob metacharacters. DuckDB's file readers treat `* ? [ ] { }`
	// as a glob, expanding one `connect` into a ListObjectsV2 + multi-object read
	// across the bucket — including the lake's `lake/` prefix. A single concrete
	// object must address exactly one key, so any glob char is refused.
	if (/[*?[\]{}]/.test(key)) {
		return {
			ok: false,
			reason:
				"path key must not contain glob metacharacters (`* ? [ ] { }`); " +
				"it must address a single object",
		};
	}
	return { ok: true };
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

async function connectFile(path: string): Promise<ConnectSchema> {
	// Defense in depth (the tool's zod superRefine already gated this pre-SQL):
	// re-validate the single allowed shape `s3://<bucket>/<key>` BEFORE any SQL
	// is built, so a caller into `connect()`/`connectFile()` that bypasses the
	// tool schema still cannot turn `path` into an arbitrary-file read (DAT-386).
	const check = validateBucketS3Path(path);
	if (!check.ok) {
		throw new Error(`connect(file='${path}') rejected: ${check.reason}`);
	}

	const reader = readerForPath(path);
	const from = `${reader}('${escapeSqlLiteral(path)}')`;

	const instance = await DuckDBInstance.create(":memory:");
	const conn = await instance.connect();
	try {
		// Only `s3://` is reachable now, so ALWAYS register httpfs + the
		// object-store secret before the reader opens the object. This is the SAME
		// `dataraum_s3` secret the lake reader uses (s3-secret.ts), so the upload
		// sniff and the lake read resolve the bucket identically.
		await applyS3Secret(conn);
		// Belt-and-braces: `applyS3Secret` LOADed httpfs (extensions load off the
		// local FS, so this MUST come AFTER), now refuse the local filesystem on
		// this throwaway sniff conn — a slipped-through local path is denied by
		// DuckDB itself, not just our validator. `s3://` reads go via httpfs and
		// are unaffected.
		await conn.run("SET disabled_filesystems='LocalFileSystem'");
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
 * READ_ONLY probe ATTACH) or an `s3://` object in the configured bucket (via
 * DuckDB's file readers). Returns one unified `ConnectSchema`.
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
