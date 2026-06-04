// DuckDB ignores the HTTP_PROXY env var, so behind a corporate proxy a runtime
// `INSTALL <ext>` (httpfs / ducklake / a DB scanner) can't reach
// extensions.duckdb.org and the call stalls. We pass the proxy to DuckDB
// explicitly via `SET http_proxy` (bare host:port — no scheme) before any
// INSTALL, reading it from OUTBOUND_PROXY (the same value the Anthropic shim
// uses; see outbound-proxy.ts). No-op when unset, so default and air-gapped /
// pre-baked-extension images are unaffected.

import type { DuckDBConnection } from "@duckdb/node-api";

export async function applyDuckdbProxy(conn: DuckDBConnection): Promise<void> {
	const proxy = (process.env.OUTBOUND_PROXY ?? "")
		.replace(/^https?:\/\//, "")
		.replace(/\/$/, "");
	if (proxy) {
		await conn.run(`SET http_proxy = '${proxy.replace(/'/g, "''")}'`);
	}
}
