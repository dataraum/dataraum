// Typed, validated configuration for the cockpit server (DAT-363).
//
// SERVER-ONLY: reads non-prefixed `process.env` vars, which TanStack Start
// keeps server-side. The marker import below ENFORCES the boundary (DAT-451):
// any client-side import of this module becomes a loud build error instead of
// riding on compiler dead-code elimination to keep secrets out of the bundle.
//
// Parsed once at import; a missing or malformed var throws immediately, naming
// the field, so the server fails loud at boot rather than silently at first
// use. Mirror of the engine's `core/settings.py`; the two are coordinated via
// the shared `.env` (no Python<->TS schema-sync tool — see DAT-363).

import "@tanstack/react-start/server-only";

import { z } from "zod";
import { baseConfig } from "./config.base";

const ConfigSchema = z.object({
	// --- Substrate (required) ---
	cockpitDatabaseUrl: z.string().min(1),
	// Engine metadata, per-workspace ROLE credentials (DAT-816): the reader
	// role (search_path = ws_<id>_read, SELECT only) and the writer role
	// (search_path = ws_<id>, control-table verbs). The role resolves the
	// schema — the cockpit never derives a ws_<id> name. Separate connections
	// by design: the read schema's pass-through views share names with the raw
	// tables, so the two search_paths must not merge.
	metadataDatabaseUrl: z.string().min(1),
	metadataWriterDatabaseUrl: z.string().min(1),
	// Plain non-empty string (not .uuid()) to match the engine, which accepts
	// stable non-UUID ids (e.g. "test"); both sides must agree on the value.
	dataraumWorkspaceId: z.string().min(1),
	// This workspace's subdomain label (DAT-819), e.g. `ws1` — the Caddy route
	// `<subdomain>.<parent domain>` that reaches THIS cockpit. Seeded onto the
	// registry row (registry.ts) so the portal can build the redirect URL.
	// Optional: absent = the registry row keeps whatever the provisioner
	// (DAT-820) minted, or NULL on a bare host-dev workspace (which the portal
	// then cannot route to — direct :3000 access only).
	dataraumWorkspaceSubdomain: z.string().min(1).optional(),
	// The read-only config tree bind-mounted at DATARAUM_CONFIG_PATH (the same
	// `/opt/dataraum/config` the engine resolves through `dataraum.core.config`).
	// The cockpit reads it via `fs` — `list_verticals` scans `verticals/*` here.
	// Required, like the engine's hard dependency on it.
	dataraumConfigPath: z.string().min(1),
	// DuckLake DATA_PATH — the `s3://bucket/prefix` URI where DuckLake writes
	// parquet (DAT-388). The engine writes here; the cockpit ATTACHes it
	// READ_ONLY (DAT-367). Must be byte-identical to the engine's value or the
	// reader ATTACHes a different prefix than the writer wrote.
	dataraumLakePath: z.string().min(1),
	// DuckLake catalog (Postgres) URL — the metadata DB the cockpit ATTACHes
	// to read the lake (DAT-367). The engine bootstraps + owns it; the cockpit
	// opens it READ_ONLY. Bare `postgresql://` libpq form (DuckDB's postgres
	// extension wants it), NOT the SQLAlchemy `postgresql+psycopg://` scheme.
	ducklakeCatalogUrl: z.string().min(1),

	// --- LLM (required) ---
	anthropicApiKey: z.string().min(1),

	// --- Object store (DAT-388; required, like the engine's S3 settings). The
	// lake DATA_PATH is an `s3://` URI, so the cockpit's READ_ONLY reader needs
	// httpfs + an S3 secret (see `duckdb/s3-secret.ts`) to resolve the parquet.
	// Creds are plain env vars validated through this seam — same as every other
	// secret here (the DB password is in the URLs above), NOT `credentials.ts`
	// (that is the per-source-DB exception). endpoint is host:port, no scheme. ---
	s3Endpoint: z.string().min(1),
	s3Region: z.string().min(1),
	s3UseSsl: z.boolean(),
	s3AccessKeyId: z.string().min(1),
	s3SecretAccessKey: z.string().min(1),
	// The bucket uploads are staged into (DAT-386), under the `uploads/` prefix —
	// the SAME bucket the lake's `lake/` prefix lives in (the lake DATA_PATH is
	// `s3://<S3_BUCKET>/lake`). The engine derives this from the lake path; the
	// cockpit needs it explicitly to address PutObject. Must match S3_BUCKET.
	s3Bucket: z.string().min(1),

	// --- DuckDB extension cache (mirror of the engine's
	// duckdb_extension_directory / ducklake_skip_install — core/settings.py).
	// The container image pre-installs ducklake/httpfs/probe-backend extensions
	// at /opt/dataraum/duckdb-extensions and sets both vars (Dockerfile), so a
	// cold start never hits extensions.duckdb.org — required behind egress proxy
	// filters / air-gapped. Both unset in host dev: DuckDB falls back to
	// ~/.duckdb and installs on demand. ---
	duckdbExtensionDirectory: z.string().min(1).optional(),
	ducklakeSkipInstall: z.boolean(),

	// --- Temporal (optional for slice-1: the cockpit Temporal client lands in
	// E4 (DAT-344), which flips these to required; no Temporal service yet).
	// No task-queue knob: START queues are per-workspace, derived from the
	// engine registry row; the worker's own queue is cockpit-<ws> from boot
	// identity (DAT-818). ---
	temporalHost: z.string().optional(),
	temporalNamespace: z.string().optional(),
	// Temporal Web UI, embedded by the /workflows section. Defaults to the
	// docker-compose dev address (CORS already allows :3000).
	temporalUiUrl: z.string().min(1).default("http://localhost:8080"),
	// The co-located ACTIVITY-ONLY worker's queue is NOT config (DAT-818): it is
	// the workspace identity — `cockpit-<dataraumWorkspaceId>`, derived at boot
	// via `temporal/task-queue.ts` — so callbacks route per workspace and no
	// knob can point two cockpits at one queue.

	// (Observability moved to config.base.ts (DAT-819): the OTLP endpoint is
	// installation-wide and the otel plugin bootstraps in portal mode too.)
});

export type Config = z.infer<typeof ConfigSchema>;

function loadConfig(): Config {
	const parsed = ConfigSchema.safeParse({
		cockpitDatabaseUrl: process.env.COCKPIT_DATABASE_URL,
		metadataDatabaseUrl: process.env.METADATA_DATABASE_URL,
		metadataWriterDatabaseUrl: process.env.METADATA_WRITER_DATABASE_URL,
		dataraumWorkspaceId: process.env.DATARAUM_WORKSPACE_ID,
		dataraumWorkspaceSubdomain:
			process.env.DATARAUM_WORKSPACE_SUBDOMAIN || undefined,
		dataraumConfigPath: process.env.DATARAUM_CONFIG_PATH,
		dataraumLakePath: process.env.DATARAUM_LAKE_PATH,
		ducklakeCatalogUrl: process.env.DUCKLAKE_CATALOG_URL,
		anthropicApiKey: process.env.ANTHROPIC_API_KEY,
		s3Endpoint: process.env.S3_ENDPOINT,
		s3Region: process.env.S3_REGION ?? "us-east-1",
		// Env is the string "true"/"false"; default secure (true) when unset.
		// (z.coerce.boolean would turn the string "false" into `true`.)
		s3UseSsl: (process.env.S3_USE_SSL ?? "true") === "true",
		s3AccessKeyId: process.env.S3_ACCESS_KEY_ID,
		s3SecretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
		s3Bucket: process.env.S3_BUCKET,
		duckdbExtensionDirectory: process.env.DUCKDB_EXTENSION_DIRECTORY,
		// Same env contract as the engine ("set to 1"); anything else — including
		// unset — keeps the on-demand INSTALL path for host dev.
		ducklakeSkipInstall: (process.env.DUCKLAKE_SKIP_INSTALL ?? "0") === "1",
		temporalHost: process.env.TEMPORAL_HOST,
		temporalNamespace: process.env.TEMPORAL_NAMESPACE,
		temporalUiUrl: process.env.TEMPORAL_UI_URL,
	});

	if (!parsed.success) {
		const details = parsed.error.issues
			.map((issue) => `  ${issue.path.join(".") || "(root)"}: ${issue.message}`)
			.join("\n");
		throw new Error(
			`Invalid cockpit configuration — check your .env / environment:\n${details}`,
		);
	}

	return parsed.data;
}

// Born-loud mode fence (DAT-819), enforced at ACCESS time, not module eval: a
// portal container has no workspace identity and deliberately no workspace env
// (metadata role URLs, S3, lake path) — but the server bundle's route graph is
// EAGER (the SSR entry statically imports every route's functions, and the
// bundler flattens even dynamic imports), so this module unavoidably evaluates
// on the portal. Evaluating is harmless; READING a workspace field there is a
// mis-routed workspace surface and throws with the real reason instead of an
// env-var scavenger hunt. Portal-safe fields live in config.base.ts.
export const config: Config = baseConfig.portalMode
	? new Proxy({} as Config, {
			get(_target, prop) {
				throw new Error(
					`[cockpit] workspace config field '${String(prop)}' accessed in ` +
						"portal mode — a workspace-only surface (route, server fn, or " +
						"worker) ran on the portal. Portal code must read config.base " +
						"instead.",
				);
			},
		})
	: loadConfig();
