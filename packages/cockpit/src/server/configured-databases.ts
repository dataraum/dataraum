// Server fn: the configured DB sources the probe surface can offer (DAT-576).
//
// `list_sources` already enumerates configured databases for the AGENT; this is
// the analog the editable probe WIDGET needs directly (so it doesn't depend on the
// agent having called a tool first). Env-scanned (`DATARAUM_<NAME>_URL`), filtered
// to a known/supported backend, the connection URL NEVER serialized — the handler
// runs server-side and is stripped from the client bundle.

import { createServerFn } from "@tanstack/react-start";
import { listConfiguredDatabases } from "#/duckdb/credentials";

/** A configured DB source the probe surface can query — a known, supported backend. */
export interface ProbeSource {
	/** Source name (the `<NAME>` in `DATARAUM_<NAME>_URL`, lowercased). */
	name: string;
	/** Backend kind (postgres/mysql/sqlite/mssql). */
	backend: string;
}

/**
 * The configured DB sources the probe surface can offer: every `DATARAUM_<NAME>_URL`
 * whose scheme maps to a supported backend. `listConfiguredDatabases` infers the
 * backend from the URL scheme via the SAME map as probe's supported backends, so a
 * non-null backend IS a supported one; an unrecognized scheme (`backend === null`)
 * is dropped — probe can't ATTACH it. The URL is never exposed (server-only handler).
 */
export const getConfiguredDatabases = createServerFn({ method: "GET" }).handler(
	(): ProbeSource[] =>
		listConfiguredDatabases().filter(
			(d): d is ProbeSource => d.backend !== null,
		),
);
