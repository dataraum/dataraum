// Cockpit-side object-store S3 secret for DuckLake reads over s3:// (DAT-388).
//
// TS twin of the engine's `apply_s3_secret` (server/storage.py): both register
// the same named secret (`dataraum_s3`) against the same SeaweedFS/S3 endpoint,
// so the cockpit's READ_ONLY DuckLake reader can resolve the `s3://` DATA_PATH
// parquet the engine wrote. Called on the lake connection BEFORE the ATTACH —
// DuckLake resolves DATA_PATH eagerly.

import type { DuckDBConnection } from "@duckdb/node-api";

import { config } from "../config";
import { escapeSqlLiteral } from "./sql-escape";

// Name of the DuckDB S3 secret. Matches the engine's `S3_SECRET_NAME` so both
// sides reason about the same secret; `CREATE OR REPLACE` keeps registration
// idempotent across reconnects.
const S3_SECRET_NAME = "dataraum_s3";

/**
 * Build the idempotent `CREATE OR REPLACE SECRET` for the object store.
 *
 * Every interpolated value is a single-quoted SQL literal, so each is escaped.
 * `URL_STYLE 'path'` is required for non-AWS S3 (SeaweedFS/MinIO) — DuckDB
 * defaults to virtual-host style and does not auto-flip on a custom endpoint.
 */
export function buildS3SecretSql(params: {
	accessKeyId: string;
	secretAccessKey: string;
	endpoint: string;
	region: string;
	useSsl: boolean;
}): string {
	return (
		`CREATE OR REPLACE SECRET ${S3_SECRET_NAME} (` +
		"TYPE s3, " +
		`KEY_ID '${escapeSqlLiteral(params.accessKeyId)}', ` +
		`SECRET '${escapeSqlLiteral(params.secretAccessKey)}', ` +
		`ENDPOINT '${escapeSqlLiteral(params.endpoint)}', ` +
		`REGION '${escapeSqlLiteral(params.region)}', ` +
		"URL_STYLE 'path', " +
		`USE_SSL ${params.useSsl ? "true" : "false"}` +
		")"
	);
}

/**
 * Load `httpfs` + register the object-store S3 secret on `conn`.
 *
 * Must run before ATTACHing a DuckLake catalog whose DATA_PATH is an `s3://`
 * URI. `INSTALL` is tolerate-fail (the image may already have httpfs); `LOAD`
 * errors loud if it is genuinely missing — mirrors `lake.ts`'s ducklake load.
 */
export async function applyS3Secret(conn: DuckDBConnection): Promise<void> {
	try {
		await conn.run("INSTALL httpfs");
	} catch {
		// Extension already present (offline/air-gapped image) — LOAD surfaces a
		// real "not found" below if it truly isn't installed.
	}
	await conn.run("LOAD httpfs");
	await conn.run(
		buildS3SecretSql({
			accessKeyId: config.s3AccessKeyId,
			secretAccessKey: config.s3SecretAccessKey,
			endpoint: config.s3Endpoint,
			region: config.s3Region,
			useSsl: config.s3UseSsl,
		}),
	);
}
