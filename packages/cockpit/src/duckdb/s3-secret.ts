// Cockpit-side object-store S3 secret for DuckLake reads over s3:// (DAT-388).
//
// TS twin of the engine's `apply_s3_secret` (server/storage.py): both register
// the same named secret (`dataraum_s3`) against the same SeaweedFS/S3 endpoint,
// so the cockpit's READ_ONLY DuckLake reader can resolve the `s3://` DATA_PATH
// parquet the engine wrote. Called on the lake connection BEFORE the ATTACH —
// DuckLake resolves DATA_PATH eagerly.

import type { DuckDBConnection } from "@duckdb/node-api";

import { config } from "../config";
import { applyDuckdbProxy } from "./proxy";
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
 *
 * `SCOPE 's3://<bucket>'` pins the secret to the configured bucket: DuckDB only
 * attaches these creds for objects under that prefix, so even a slipped-through
 * `s3://other-bucket/...` read finds no matching secret (defense in depth for
 * the connect sniff — DAT-386).
 */
export function buildS3SecretSql(params: {
	accessKeyId: string;
	secretAccessKey: string;
	endpoint: string;
	region: string;
	useSsl: boolean;
	bucket: string;
}): string {
	return (
		`CREATE OR REPLACE SECRET ${S3_SECRET_NAME} (` +
		"TYPE s3, " +
		`KEY_ID '${escapeSqlLiteral(params.accessKeyId)}', ` +
		`SECRET '${escapeSqlLiteral(params.secretAccessKey)}', ` +
		`ENDPOINT '${escapeSqlLiteral(params.endpoint)}', ` +
		`REGION '${escapeSqlLiteral(params.region)}', ` +
		"URL_STYLE 'path', " +
		`USE_SSL ${params.useSsl ? "true" : "false"}, ` +
		`SCOPE 's3://${escapeSqlLiteral(params.bucket)}'` +
		")"
	);
}

/**
 * Load `httpfs` + register the object-store S3 secret on `conn`.
 *
 * Must run before ATTACHing a DuckLake catalog whose DATA_PATH is an `s3://`
 * URI. Honors the image's pre-baked extension cache (DUCKDB_EXTENSION_DIRECTORY
 * + DUCKLAKE_SKIP_INSTALL — mirror of the engine's `apply_s3_secret`): the
 * `SET extension_directory` must happen HERE, not just in `lake.ts`, because a
 * fresh in-memory connection (connect.ts's upload-sniff throwaway) otherwise
 * defaults to ~/.duckdb and the LOAD misses the baked httpfs. In host dev
 * (neither var set) `INSTALL` is tolerate-fail; `LOAD` errors loud if the
 * extension is genuinely missing — mirrors `lake.ts`'s ducklake load.
 */
export async function applyS3Secret(conn: DuckDBConnection): Promise<void> {
	if (config.duckdbExtensionDirectory) {
		await conn.run(
			`SET extension_directory = '${escapeSqlLiteral(config.duckdbExtensionDirectory)}'`,
		);
	}
	// Behind a corporate proxy DuckDB ignores HTTP_PROXY; SET http_proxy from
	// OUTBOUND_PROXY before INSTALL. No-op when unset / air-gapped pre-baked image.
	await applyDuckdbProxy(conn);
	if (!config.ducklakeSkipInstall) {
		try {
			await conn.run("INSTALL httpfs");
		} catch {
			// Extension already present (offline) — LOAD surfaces a real
			// "not found" below if it truly isn't installed.
		}
	}
	await conn.run("LOAD httpfs");
	await conn.run(
		buildS3SecretSql({
			accessKeyId: config.s3AccessKeyId,
			secretAccessKey: config.s3SecretAccessKey,
			endpoint: config.s3Endpoint,
			region: config.s3Region,
			useSsl: config.s3UseSsl,
			bucket: config.s3Bucket,
		}),
	);
}
