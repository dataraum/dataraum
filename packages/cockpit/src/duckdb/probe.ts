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

import { DuckDBInstance } from "@duckdb/node-api";

import { resolveCredential } from "./credentials";
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
	 * result into the chat context. Defaults to 1000.
	 */
	limit?: number;
}

const DEFAULT_PROBE_LIMIT = 1000;

/**
 * Run read-only SQL against an external database source.
 *
 * Resolves credentials by source name, ATTACHes the source READ_ONLY in a
 * throwaway connection, `USE`s the backend's default schema, runs the SQL
 * (wrapped in a `LIMIT`), and DETACHes. Fails loud — unknown backend, missing
 * credential, or a failed ATTACH/SELECT all throw with an actionable message.
 *
 * The returned {@link QueryResult} contains only column metadata + JSON-safe
 * rows; the connection URL is never included.
 */
export async function probe(input: ProbeInput): Promise<QueryResult> {
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
	const limit = input.limit ?? DEFAULT_PROBE_LIMIT;

	const instance = await DuckDBInstance.create(":memory:");
	const conn = await instance.connect();
	try {
		if (COMMUNITY_EXTENSIONS.has(extension)) {
			await conn.run(`INSTALL ${extension} FROM community`);
		} else {
			await conn.run(`INSTALL ${extension}`);
		}
		await conn.run(`LOAD ${extension}`);

		const safeUrl = escapeSqlLiteral(credential.url);
		await conn.run(
			`ATTACH '${safeUrl}' AS ${ATTACH_ALIAS} (TYPE ${attachType}, READ_ONLY)`,
		);
		try {
			await conn.run(`USE ${ATTACH_ALIAS}.${defaultSchema}`);
			// Wrap the user SQL in a subquery + LIMIT so a probe can never pull an
			// unbounded result into the chat context. READ_ONLY already blocks
			// writes at the engine level.
			const reader = await conn.runAndReadAll(
				`SELECT * FROM (${input.sql}) AS _probe LIMIT ${limit}`,
			);
			return readerToResult(reader);
		} finally {
			try {
				await conn.run(`DETACH ${ATTACH_ALIAS}`);
			} catch {
				// Best-effort cleanup; the connection is closed below regardless.
			}
		}
	} catch (err) {
		throw new Error(
			`Probe of source '${input.source_name}' (${backend}) failed: ${
				err instanceof Error ? err.message : String(err)
			}`,
		);
	} finally {
		conn.closeSync();
		instance.closeSync();
	}
}
