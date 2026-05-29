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
