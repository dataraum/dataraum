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
// Lifecycle: ONE lazily-opened, bootstrapped instance per process; a FRESH
// connection PER request. A DuckDB connection runs statements serially — it is
// NOT safe to drive concurrently. A web server fans out concurrent reads (e.g. a
// report page renders several charts at once, each its own `/api/run-sql`), so a
// single shared connection serializes/corrupts them. The node_neo model is one
// instance, many connections: ATTACHed databases, loaded extensions, and the S3
// secret are all instance-level — shared across every connection from the
// instance — so the per-request connection inherits the bootstrap for free and
// only the query is per-connection. `getLakeConnection()` hands out a fresh
// connection the CALLER must close (or use `withLakeConnection`); `closeLake()`
// tears the instance down (tests, graceful shutdown).
// See https://duckdb.org/docs/current/clients/node_neo/overview.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";

import { config } from "../config";
import { applyS3Secret } from "./s3-secret";
import { buildDucklakeAttachSql, escapeSqlLiteral } from "./sql-escape";

// Alias the DuckLake catalog is ATTACHed under. Matches the engine's
// `LAKE_CATALOG_ALIAS` so fully-qualified names (`lake.typed.orders`) resolve
// identically on both sides.
export const LAKE_ALIAS = "lake";

let instancePromise: Promise<DuckDBInstance> | null = null;

/**
 * ATTACH the DuckLake lake READ_ONLY on an already-open connection, under
 * {@link LAKE_ALIAS}. The full bootstrap dance — extension directory, INSTALL/
 * LOAD ducklake, the S3 secret, and the catalog ATTACH — in one place. Run ONCE
 * per instance by {@link openInstance}: the ATTACH/extensions/secret are
 * instance-level, so every connection off the instance (run_sql, run_steps, …)
 * inherits them. Module-private — callers take a connection via
 * {@link getLakeConnection}/{@link withLakeConnection}, never re-attach.
 */
async function attachLakeReadOnly(conn: DuckDBConnection): Promise<void> {
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

async function openInstance(): Promise<DuckDBInstance> {
	// A fresh in-memory instance owns the ATTACH. Using a per-cockpit-process
	// instance (not `fromCache`) is fine: the cockpit ATTACHes the lake exactly
	// once, so there is no same-process double-ATTACH to guard.
	const instance = await DuckDBInstance.create(":memory:");
	// Bootstrap ONCE on a throwaway connection: load ducklake/httpfs, register
	// the S3 secret, ATTACH the lake READ_ONLY. All three are instance-level in
	// DuckDB (shared across every connection from this instance — node_neo docs),
	// so per-request connections inherit them without re-running the bootstrap,
	// and closing this bootstrap connection does NOT detach the lake (ATTACH is
	// catalog/instance state, not connection state). The instance itself is held
	// by `instancePromise`, keeping that catalog alive for the process.
	const bootstrap = await instance.connect();
	try {
		await attachLakeReadOnly(bootstrap);
	} finally {
		bootstrap.closeSync();
	}
	return instance;
}

/**
 * Return the process-wide, bootstrapped DuckLake reader instance, opening it on
 * first call. Memoized — every caller shares ONE instance (and thus the single
 * ATTACH), but each gets its own connection off it. Fails loud if the catalog is
 * unreachable or the ATTACH fails (the rejected promise is cleared so a later
 * call can retry).
 */
export function getLakeInstance(): Promise<DuckDBInstance> {
	if (!instancePromise) {
		instancePromise = openInstance().catch((err) => {
			// Surface WHY the lake is unreachable (catalog down, S3 creds, bad
			// DATA_PATH) — otherwise it only ever reaches the agent as an opaque
			// run_steps { error } string with no server trace.
			console.error(
				`[lake] ATTACH/connection failed: ${err instanceof Error ? err.message : String(err)}`,
			);
			// Clear the memo so a transient failure (catalog briefly unreachable)
			// doesn't wedge the process into a permanently-rejected state.
			instancePromise = null;
			throw err;
		});
	}
	return instancePromise;
}

/**
 * Open a FRESH connection on the shared lake instance. A DuckDB connection runs
 * statements serially and is not safe to drive concurrently, so every concurrent
 * reader needs its own — the caller OWNS this connection's lifecycle and MUST
 * `closeSync()` it when done (prefer {@link withLakeConnection}, which closes for
 * you). The connection inherits the instance's ATTACH/extensions/secret, so it is
 * ready to query immediately.
 */
export async function getLakeConnection(): Promise<DuckDBConnection> {
	const instance = await getLakeInstance();
	return instance.connect();
}

/**
 * Run `fn` with a fresh lake connection and close it afterwards — the scoped form
 * of {@link getLakeConnection} for the common read-then-return case. NOT for the
 * streaming route, whose connection must outlive the handler (it is consumed by
 * the `ReadableStream` after the function returns); that path acquires + closes
 * around the stream lifecycle itself.
 *
 * No abort handling: this helper is for short, non-cancellable reads. A read that
 * must honor abort (cancel an in-flight statement on a stopped chat turn) takes a
 * raw {@link getLakeConnection} and wires `signal` → `conn.interrupt()` itself —
 * see `run-sql.ts` / `run-steps.ts`. (That stays in the consumer so it imports
 * only `getLakeConnection` across the module boundary and is mockable/testable;
 * folding it in here would make it intra-module and untestable without the real
 * Postgres catalog.)
 */
export async function withLakeConnection<T>(
	fn: (conn: DuckDBConnection) => Promise<T>,
): Promise<T> {
	const conn = await getLakeConnection();
	try {
		return await fn(conn);
	} finally {
		conn.closeSync();
	}
}

/**
 * Scope a fresh connection the way the engine scopes its cursors — `USE
 * lake.typed` (see engine `core/connections.py::_LakeScopedConnection`) — so
 * ENGINE-AUTHORED SQL resolves: metric formula / measure extract snippets use
 * unqualified table names that assume the engine's default scope. Enriched
 * views live in `lake.typed` too (not a sibling schema — see query-context.ts).
 * Cockpit-authored SQL is fully qualified (`lake.typed."…"`) and unaffected by
 * the current schema. A fresh lake with no `typed` schema yet cannot satisfy
 * the USE — swallowed, since unqualified SQL could not resolve there anyway.
 */
export async function applyEngineScope(conn: DuckDBConnection): Promise<void> {
	try {
		await conn.run(`USE ${LAKE_ALIAS}.typed`);
	} catch (err) {
		// No typed schema in the lake yet (nothing ingested) — leave unscoped.
		// Logged at debug so a REAL failure (connectivity blip) is traceable
		// instead of surfacing later as a confusing downstream binder error.
		console.debug(
			`[lake] engine scope skipped: ${err instanceof Error ? err.message : String(err)}`,
		);
	}
}

/**
 * Close the lake instance and reset the memo. Idempotent. For graceful shutdown
 * and test teardown. Per-request connections are owned + closed by their callers;
 * this tears down the shared instance (which drops any still-open connections).
 */
export async function closeLake(): Promise<void> {
	const inst = instancePromise;
	instancePromise = null;
	if (inst) {
		try {
			(await inst).closeSync();
		} catch {
			// Already closed / never opened cleanly — nothing to do.
		}
	}
}
