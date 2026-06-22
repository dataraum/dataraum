// `probe` — read-only SQL against an external database source via DuckDB
// ATTACH (READ_ONLY), cockpit-side (DAT-367, re-homed from the engine's
// `sources/backends.py`).
//
// The agent uses `probe` to look at a configured DB source BEFORE materializing
// it into the lake — schema sniffing, sample reads, sanity SELECTs. Credentials
// are resolved by source name (`resolveCredential`); the URL is a SECRET and is
// never echoed back.
//
// Isolation: probe opens its OWN throwaway in-memory DuckDB connection per call
// and DETACHes/closes in a finally. It never touches the long-lived lake reader
// connection, so an external ATTACH can't leak into lake-catalog state.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";

import { config } from "../config";
import { resolveCredential } from "./credentials";
import { clampRowLimit } from "./limit";
import type { QueryResult } from "./query-result";
import { readerToResult } from "./query-result";
import { escapeSqlLiteral } from "./sql-escape";

// DuckDB extension name per backend.
const BACKEND_EXTENSIONS: Record<string, string> = {
	mssql: "mssql",
	postgres: "postgres",
	mysql: "mysql",
	sqlite: "sqlite",
};

// DuckDB ATTACH `TYPE` per backend.
const BACKEND_ATTACH_TYPES: Record<string, string> = {
	mssql: "MSSQL",
	postgres: "POSTGRES",
	mysql: "MYSQL",
	sqlite: "SQLITE",
};

// Default schema to `USE` after ATTACH so user SQL referencing `schema.table`
// resolves without an alias prefix. Mirrors the engine's map.
const BACKEND_DEFAULT_SCHEMA: Record<string, string> = {
	mssql: "dbo",
	postgres: "public",
	mysql: "main",
	sqlite: "main",
};

// mssql is community-maintained; the rest live in DuckDB's core repo.
const COMMUNITY_EXTENSIONS = new Set(["mssql"]);

export const SUPPORTED_BACKENDS = Object.keys(BACKEND_EXTENSIONS);

const ATTACH_ALIAS = "src";

export interface ProbeInput {
	/** Source name — the key for `DATARAUM_<NAME>_URL` credential lookup. */
	source_name: string;
	/** Backend kind: one of `mssql`, `postgres`, `mysql`, `sqlite`. */
	backend: string;
	/** Read-only SQL to run against the attached source. */
	sql: string;
	/**
	 * Row cap. The query is wrapped so the agent can't accidentally pull a huge
	 * result into the chat context. Defaults to {@link DEFAULT_ROW_LIMIT} and is
	 * clamped to {@link HARD_ROW_CEILING}.
	 */
	limit?: number;
}

/** An open probe connection: a throwaway in-memory DuckDB with the source
 * ATTACHed READ_ONLY and its default schema selected. The caller MUST `dispose()`
 * it — the materialize path ({@link probe}) in a `finally`, the streaming path
 * (`/api/probe-sql`) when the stream ends or is cancelled. */
export interface ProbeConnection {
	/** Ready for a read-only query against the attached source (`src.*`). */
	readonly conn: DuckDBConnection;
	/** Close the connection + its throwaway instance (releases the ATTACH). */
	readonly dispose: () => void;
	/** Strip the resolved source URL from a message before it leaves the process
	 * — a driver error can echo the credential-bearing DSN (the agent result + the
	 * persisted transcript must never carry it). */
	readonly redact: (message: string) => string;
}

/**
 * Open a throwaway in-memory DuckDB connection with the source ATTACHed READ_ONLY
 * and the backend's default schema selected — the shared setup behind BOTH the
 * agent's materialized {@link probe} (a bounded sample) and the human grid's
 * streaming `/api/probe-sql` (the full result). Fails loud with a
 * CREDENTIAL-REDACTED message on unknown backend, missing credential, or a failed
 * ATTACH; on a setup failure the connection is disposed before throwing.
 */
export async function openProbeConnection(input: {
	source_name: string;
	backend: string;
}): Promise<ProbeConnection> {
	const backend = input.backend.toLowerCase();
	if (!(backend in BACKEND_EXTENSIONS)) {
		throw new Error(
			`Unsupported backend '${input.backend}'. Supported: ${SUPPORTED_BACKENDS.join(", ")}.`,
		);
	}

	const credential = resolveCredential(input.source_name);
	if (credential === null) {
		throw new Error(
			`No credentials found for source '${input.source_name}'. ` +
				`Set DATARAUM_${input.source_name.toUpperCase()}_URL in the environment.`,
		);
	}

	const extension = BACKEND_EXTENSIONS[backend];
	const attachType = BACKEND_ATTACH_TYPES[backend];
	const defaultSchema = BACKEND_DEFAULT_SCHEMA[backend];

	const instance = await DuckDBInstance.create(":memory:");
	const conn = await instance.connect();
	// Closing the throwaway instance releases the ATTACH — no explicit DETACH.
	const dispose = () => {
		conn.closeSync();
		instance.closeSync();
	};
	// SECURITY: a failed ATTACH/query can echo the connection DSN (which carries
	// the credential) in the driver message; redact the resolved URL before it
	// leaves the process.
	const redact = (message: string): string =>
		credential.url
			? message.split(credential.url).join("<source url redacted>")
			: message;

	try {
		// Same extension-cache contract as the lake reader (lake.ts): the image
		// pre-bakes all four backend extensions and sets DUCKLAKE_SKIP_INSTALL=1, so
		// a probe never hits extensions.duckdb.org at runtime. Host dev has neither
		// var set and installs on demand into ~/.duckdb.
		if (config.duckdbExtensionDirectory) {
			await conn.run(
				`SET extension_directory = '${escapeSqlLiteral(config.duckdbExtensionDirectory)}'`,
			);
		}
		if (!config.ducklakeSkipInstall) {
			if (COMMUNITY_EXTENSIONS.has(extension)) {
				await conn.run(`INSTALL ${extension} FROM community`);
			} else {
				await conn.run(`INSTALL ${extension}`);
			}
		}
		await conn.run(`LOAD ${extension}`);
		await conn.run(
			`ATTACH '${escapeSqlLiteral(credential.url)}' AS ${ATTACH_ALIAS} (TYPE ${attachType}, READ_ONLY)`,
		);
		// USE the default schema so user SQL referencing `schema.table` resolves
		// without the `src.` alias prefix (mirrors the engine's map).
		await conn.run(`USE ${ATTACH_ALIAS}.${defaultSchema}`);
	} catch (err) {
		const raw = err instanceof Error ? err.message : String(err);
		dispose();
		throw new Error(
			`Probe of source '${input.source_name}' (${backend}) failed: ${redact(raw)}`,
		);
	}

	return { conn, dispose, redact };
}

/** A probed query's schema — its DESCRIBEd columns + a bounded sample of rows,
 * the input to building a synthetic `ConnectSchema` for `frame` (DAT-594). */
export interface ProbeSchema {
	/** One row per output column: name + DuckDB column type. */
	columns: { name: string; type: string }[];
	/** A bounded sample of result rows (≤ `clampRowLimit`) for sample-value
	 * induction quality — JSON-safe, the connection URL never included. */
	sampleRows: Record<string, unknown>[];
}

/**
 * Describe a probed query WITHOUT materializing the full result — run
 * `DESCRIBE SELECT * FROM (<sql>)` (unwrapped, so DESCRIBE can introspect the
 * query) plus a bounded sample, on the raw probe connection. This is the
 * query analog of the file path's `DESCRIBE SELECT * FROM read_*(...)` sniff
 * (connect.ts), the schema source for staging a probed query into `frame`
 * (DAT-594). `probe()` wraps SQL in a `LIMIT` subquery so it cannot DESCRIBE,
 * so this opens its own connection and runs DESCRIBE directly.
 *
 * Columns + rows only; the credential-bearing URL is redacted from any error.
 */
export async function probeDescribe(input: ProbeInput): Promise<ProbeSchema> {
	const { conn, dispose, redact } = await openProbeConnection(input);
	const limit = clampRowLimit(input.limit);
	try {
		// DESCRIBE the UNWRAPPED query — DuckDB returns one row per output column
		// (column_name, column_type, null, …). Unlike probe(), no LIMIT subquery:
		// DESCRIBE needs the bare SELECT to introspect its projection.
		const describe = readerToResult(
			await conn.runAndReadAll(`DESCRIBE SELECT * FROM (${input.sql})`),
		);
		const columns = (
			describe.rows as { column_name: string; column_type: string }[]
		).map((r) => ({ name: r.column_name, type: r.column_type }));
		// A bounded sample feeds sample-value induction (the file sniff samples too).
		const sample = readerToResult(
			await conn.runAndReadAll(
				`SELECT * FROM (${input.sql}) AS _probe LIMIT ${limit}`,
			),
		);
		return { columns, sampleRows: sample.rows };
	} catch (err) {
		const raw = err instanceof Error ? err.message : String(err);
		throw new Error(
			`Describe of source '${input.source_name}' (${input.backend.toLowerCase()}) failed: ${redact(raw)}`,
		);
	} finally {
		dispose();
	}
}

/**
 * Run read-only SQL against an external database source and MATERIALIZE a bounded
 * sample — the AGENT path (the LLM must never get an unbounded result dumped into
 * context; the full result for the human grid streams via `/api/probe-sql`).
 *
 * The returned {@link QueryResult} contains only column metadata + JSON-safe rows;
 * the connection URL is never included.
 */
export async function probe(input: ProbeInput): Promise<QueryResult> {
	const { conn, dispose, redact } = await openProbeConnection(input);
	const limit = clampRowLimit(input.limit);
	try {
		// Wrap the user SQL in a subquery + LIMIT so a probe can never pull an
		// unbounded result into the chat context. READ_ONLY already blocks writes
		// at the engine level.
		const reader = await conn.runAndReadAll(
			`SELECT * FROM (${input.sql}) AS _probe LIMIT ${limit}`,
		);
		return readerToResult(reader);
	} catch (err) {
		const raw = err instanceof Error ? err.message : String(err);
		throw new Error(
			`Probe of source '${input.source_name}' (${input.backend.toLowerCase()}) failed: ${redact(raw)}`,
		);
	} finally {
		dispose();
	}
}
