// Cockpit-owned DuckDB connection to the DuckLake lake (DAT-367).
//
// The engine OWNS the lake — it bootstraps the DuckLake catalog (Postgres) and
// writes parquet to the shared DATA_PATH through the Temporal pipeline. The
// cockpit is a READER: it ATTACHes the same catalog + data path READ_ONLY for
// the interactive read verbs (`run_sql`, `look_table` samples, traffic-light
// aggregations) and the future `connect` schema-sniff (DAT-381). Mirror of the
// engine's `server/storage.py` bootstrap, minus the write-side concerns.
//
// Cross-process read consistency: DuckLake keeps committed table state in its
// Postgres catalog and writes immutable parquet snapshots to DATA_PATH. A
// reader on a SEPARATE process/instance (this cockpit) ATTACHing the same
// catalog observes the latest COMMITTED snapshot — it does not share the
// engine's in-memory write buffer. Two consequences, both acceptable for the
// read verbs: (1) writes the engine has buffered but not CHECKPOINTed are not
// yet visible here; the engine checkpoints at activity boundaries, so the
// cockpit reads stage-complete state, which is exactly what the chat agent
// reasons over. (2) Opening READ_ONLY means the cockpit never contends for the
// catalog's write path. See the DAT-367 PR body for the full caveat.
//
// Lifecycle: one lazily-opened instance + connection per process (the simplest
// correct model — DuckDB connections are not thread-affine here and Node is
// single-threaded for our purposes). `getLakeConnection()` opens on first call
// and memoizes; `closeLake()` tears it down (tests, graceful shutdown).

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";

import { config } from "../config";
import { applyDuckdbProxy } from "./proxy";
import { applyS3Secret } from "./s3-secret";
import { buildDucklakeAttachSql, escapeSqlLiteral } from "./sql-escape";

// Alias the DuckLake catalog is ATTACHed under. Matches the engine's
// `LAKE_CATALOG_ALIAS` so fully-qualified names (`lake.typed.orders`) resolve
// identically on both sides.
export const LAKE_ALIAS = "lake";

let instancePromise: Promise<DuckDBInstance> | null = null;
let connectionPromise: Promise<DuckDBConnection> | null = null;

/**
 * ATTACH the DuckLake lake READ_ONLY on an already-open connection, under
 * {@link LAKE_ALIAS}. The full bootstrap dance — extension directory, INSTALL/
 * LOAD ducklake, the S3 secret, and the catalog ATTACH — in one place, so both
 * the memoized lake reader (`openConnection`) and a throwaway validator
 * connection (`run-steps.ts`, DAT-485) attach the lake identically. The caller
 * owns the connection's lifecycle (open + close); this only runs the ATTACH
 * sequence on it.
 */
export async function attachLakeReadOnly(
	conn: DuckDBConnection,
): Promise<void> {
	const attachSql = buildDucklakeAttachSql(
		LAKE_ALIAS,
		config.ducklakeCatalogUrl,
		config.dataraumLakePath,
	);

	// The container image pre-bakes ducklake at DUCKDB_EXTENSION_DIRECTORY and
	// sets DUCKLAKE_SKIP_INSTALL=1 (Dockerfile — mirror of the engine's
	// bootstrap_lake), so a cold start never hits extensions.duckdb.org. Host
	// dev has neither set: INSTALL is attempted tolerate-fail (it needs the
	// network once) and LOAD errors loudly if the extension is genuinely
	// missing.
	if (config.duckdbExtensionDirectory) {
		// Must precede INSTALL/LOAD so DuckDB looks the extension up at the
		// image-baked path rather than ~/.duckdb.
		await conn.run(
			`SET extension_directory = '${escapeSqlLiteral(config.duckdbExtensionDirectory)}'`,
		);
	}
	// Behind a corporate proxy DuckDB can't reach extensions.duckdb.org on its
	// own (it ignores HTTP_PROXY); SET http_proxy from OUTBOUND_PROXY before any
	// INSTALL. No-op when unset / air-gapped pre-baked image.
	await applyDuckdbProxy(conn);
	if (!config.ducklakeSkipInstall) {
		try {
			await conn.run("INSTALL ducklake");
		} catch {
			// Extension already present (offline) — LOAD will surface a real
			// "not found" below if it truly isn't installed.
		}
	}
	await conn.run("LOAD ducklake");
	// The lake DATA_PATH is an `s3://` URI; register httpfs + the S3 secret
	// before the ATTACH (DuckLake resolves DATA_PATH eagerly). DAT-388.
	await applyS3Secret(conn);
	await conn.run(attachSql);
}

async function openConnection(): Promise<DuckDBConnection> {
	if (!instancePromise) {
		// A fresh in-memory instance owns the ATTACH. Using a per-cockpit-process
		// instance (not `fromCache`) is fine: the cockpit ATTACHes the lake
		// exactly once, so there is no same-process double-ATTACH to guard.
		instancePromise = DuckDBInstance.create(":memory:");
	}
	const instance = await instancePromise;
	const conn = await instance.connect();
	await attachLakeReadOnly(conn);
	return conn;
}

/**
 * Return the process-wide DuckLake reader connection, opening it on first call.
 *
 * The connection is memoized — every caller shares one connection for the life
 * of the process. Reusable by any read verb (`run_sql`) or the future
 * schema-sniff tool. Fails loud if the catalog is unreachable or the ATTACH
 * fails (the rejected promise is cleared so a later call can retry).
 */
export function getLakeConnection(): Promise<DuckDBConnection> {
	if (!connectionPromise) {
		connectionPromise = openConnection().catch((err) => {
			// Surface WHY the lake is unreachable (catalog down, S3 creds, bad
			// DATA_PATH) — otherwise it only ever reaches the agent as an opaque
			// run_steps { error } string with no server trace.
			console.error(
				`[lake] ATTACH/connection failed: ${err instanceof Error ? err.message : String(err)}`,
			);
			// Clear the memo so a transient failure (catalog briefly unreachable)
			// doesn't wedge the process into a permanently-rejected state.
			connectionPromise = null;
			throw err;
		});
	}
	return connectionPromise;
}

/**
 * Close the lake connection + instance and reset the memo. Idempotent.
 * For graceful shutdown and test teardown.
 */
export async function closeLake(): Promise<void> {
	const conn = connectionPromise;
	const inst = instancePromise;
	connectionPromise = null;
	instancePromise = null;
	if (conn) {
		try {
			(await conn).closeSync();
		} catch {
			// Already closed / never opened cleanly — nothing to do.
		}
	}
	if (inst) {
		try {
			(await inst).closeSync();
		} catch {
			// Same.
		}
	}
}
