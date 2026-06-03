// Typed, validated configuration for the cockpit server (DAT-363).
//
// SERVER-ONLY: reads non-prefixed `process.env` vars, which TanStack Start
// keeps server-side. Only import this from server modules (db clients, API
// route handlers, server functions) — never from a client component, or the
// bundler would try to inline server secrets.
//
// Parsed once at import; a missing or malformed var throws immediately, naming
// the field, so the server fails loud at boot rather than silently at first
// use. Mirror of the engine's `core/settings.py`; the two are coordinated via
// the shared `.env` (no Python<->TS schema-sync tool — see DAT-363).

import { z } from "zod";

const ConfigSchema = z.object({
	// --- Substrate (required) ---
	cockpitDatabaseUrl: z.string().min(1),
	metadataDatabaseUrl: z.string().min(1),
	// Plain non-empty string (not .uuid()) to match the engine, which accepts
	// stable non-UUID ids (e.g. "test"); both sides must agree on the value.
	dataraumWorkspaceId: z.string().min(1),
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

	// --- Temporal (optional for slice-1: the cockpit Temporal client lands in
	// E4 (DAT-344), which flips these to required; no Temporal service yet) ---
	temporalHost: z.string().optional(),
	temporalNamespace: z.string().optional(),
	temporalTaskQueue: z.string().optional(),
	// Temporal Web UI, embedded by the /workflows section. Defaults to the
	// docker-compose dev address (CORS already allows :3000).
	temporalUiUrl: z.string().min(1).default("http://localhost:8080"),
});

export type Config = z.infer<typeof ConfigSchema>;

function loadConfig(): Config {
	const parsed = ConfigSchema.safeParse({
		cockpitDatabaseUrl: process.env.COCKPIT_DATABASE_URL,
		metadataDatabaseUrl: process.env.METADATA_DATABASE_URL,
		dataraumWorkspaceId: process.env.DATARAUM_WORKSPACE_ID,
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
		temporalHost: process.env.TEMPORAL_HOST,
		temporalNamespace: process.env.TEMPORAL_NAMESPACE,
		temporalTaskQueue: process.env.TEMPORAL_TASK_QUEUE,
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

export const config = loadConfig();
