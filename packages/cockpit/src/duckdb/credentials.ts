// Per-source credential resolution (DAT-367, re-homed from the engine's
// `core/credentials.py`).
//
// Database sources (the `probe` verb) need a connection URL resolved by source
// NAME at query time, not at boot — the per-source URLs are dynamic
// (`DATARAUM_<NAME>_URL`) and not enumerable as static config fields, so they
// live OUTSIDE the typed Zod config (DAT-363) and are read directly here.
//
// Resolution is a single-provider chain today (env only). The chain shape is
// retained so a future secrets-manager provider can slot in without touching
// callers. Resolved URLs are SECRETS: never log them, never return them to the
// chat agent or serialize them into a tool result.

export interface ResolvedCredential {
	/** The resolved connection URL. Never serialize this to a tool result. */
	url: string;
	/** Which provider resolved it (currently always "env"). */
	source: string;
}

interface CredentialProvider {
	resolve(sourceName: string): ResolvedCredential | null;
}

/** Resolve `DATARAUM_<SOURCE_NAME>_URL` from the process environment. */
class EnvProvider implements CredentialProvider {
	resolve(sourceName: string): ResolvedCredential | null {
		const key = `DATARAUM_${sourceName.toUpperCase()}_URL`;
		const url = process.env[key];
		if (url) {
			return { url, source: "env" };
		}
		return null;
	}
}

const PROVIDERS: CredentialProvider[] = [new EnvProvider()];

/**
 * Resolve a connection URL for a database source by name.
 *
 * Walks the provider chain and returns the first match, or `null` if no
 * provider has a credential for the source (the caller fails loud with an
 * actionable "set DATARAUM_<NAME>_URL" message — never silently).
 */
export function resolveCredential(
	sourceName: string,
): ResolvedCredential | null {
	for (const provider of PROVIDERS) {
		const result = provider.resolve(sourceName);
		if (result !== null) {
			return result;
		}
	}
	return null;
}

/** One configured database source, as surfaced by `list_sources`. */
export interface ConfiguredDatabase {
	/** Source name (the `<NAME>` in `DATARAUM_<NAME>_URL`), lowercased. */
	name: string;
	/** Backend kind inferred from the URL scheme, or null when unrecognized. */
	backend: string | null;
}

// `DATARAUM_<NAME>_URL` is the only env shape that names a DB source credential.
// `DATARAUM_WORKSPACE_ID` / `_LAKE_PATH` / `_CONFIG_PATH` don't end in `_URL`,
// and `METADATA_/COCKPIT_/DUCKLAKE_*_URL` don't start with `DATARAUM_`, so this
// matches source creds only.
const DB_URL_KEY = /^DATARAUM_(.+)_URL$/;

// URL scheme → backend kind (mirrors probe.ts SUPPORTED_BACKENDS). Used to label
// a configured source WITHOUT exposing the secret URL — only the scheme is read.
const SCHEME_BACKEND: Record<string, string> = {
	postgres: "postgres",
	postgresql: "postgres",
	mysql: "mysql",
	mariadb: "mysql",
	sqlite: "sqlite",
	mssql: "mssql",
	sqlserver: "mssql",
};

/** Infer the backend kind from a connection URL's scheme; never returns the URL. */
function inferBackend(url: string): string | null {
	const idx = url.indexOf("://");
	if (idx <= 0) return null;
	return SCHEME_BACKEND[url.slice(0, idx).toLowerCase()] ?? null;
}

/**
 * Enumerate the database sources configured via `DATARAUM_<NAME>_URL` env vars.
 *
 * These are "available inputs" the user can `connect`/`select` — the pre-select
 * inventory `list_sources` reports, NOT registered sources. The secret URL is
 * never returned: only the source name and the scheme-inferred backend.
 */
export function listConfiguredDatabases(): ConfiguredDatabase[] {
	const out: ConfiguredDatabase[] = [];
	for (const [key, value] of Object.entries(process.env)) {
		const match = DB_URL_KEY.exec(key);
		if (!match || !value) continue;
		out.push({ name: match[1].toLowerCase(), backend: inferBackend(value) });
	}
	return out.sort((a, b) => a.name.localeCompare(b.name));
}
