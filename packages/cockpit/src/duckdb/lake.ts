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
import { applyS3Secret } from "./s3-secret";
import { buildDucklakeAttachSql } from "./sql-escape";

// Alias the DuckLake catalog is ATTACHed under. Matches the engine's
// `LAKE_CATALOG_ALIAS` so fully-qualified names (`lake.typed.orders`) resolve
// identically on both sides.
export const LAKE_ALIAS = "lake";

let instancePromise: Promise<DuckDBInstance> | null = null;
let connectionPromise: Promise<DuckDBConnection> | null = null;

async function openConnection(): Promise<DuckDBConnection> {
	if (!instancePromise) {
		// A fresh in-memory instance owns the ATTACH. Using a per-cockpit-process
		// instance (not `fromCache`) is fine: the cockpit ATTACHes the lake
		// exactly once, so there is no same-process double-ATTACH to guard.
		instancePromise = DuckDBInstance.create(":memory:");
	}
	const instance = await instancePromise;
	const conn = await instance.connect();

	const attachSql = buildDucklakeAttachSql(
		LAKE_ALIAS,
		config.ducklakeCatalogUrl,
		config.dataraumLakePath,
	);

	// The container image pre-installs the ducklake extension; LOAD is enough
	// there. INSTALL is attempted first and tolerated-on-failure so a local dev
	// run without the cached extension still works (it falls through to LOAD,
	// which errors loudly if the extension is genuinely missing).
	try {
		await conn.run("INSTALL ducklake");
	} catch {
		// Extension already present (offline/air-gapped image) — LOAD will
		// surface a real "not found" below if it truly isn't installed.
	}
	await conn.run("LOAD ducklake");
	// The lake DATA_PATH is an `s3://` URI; register httpfs + the S3 secret
	// before the ATTACH (DuckLake resolves DATA_PATH eagerly). DAT-388.
	await applyS3Secret(conn);
	await conn.run(attachSql);
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
