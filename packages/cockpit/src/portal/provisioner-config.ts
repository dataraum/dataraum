// Provisioner configuration (DAT-820). SERVER-ONLY, PORTAL-ROLE-ONLY.
//
// The provisioner runs in the portal role (DD/51740673: "lifecycle lives in
// the portal/control plane") and in the `scripts/provision-workspace.ts`
// trigger. Its env is the INSTALLATION-admin surface — superuser Postgres
// URLs, the Caddy admin API, the object store, the Docker socket — which a
// per-workspace cockpit deliberately does not carry, so this schema lives
// outside both `config.ts` (workspace) and `config.base.ts` (mode-shared).
// Parsed lazily on first lifecycle call: a portal that never provisions (or a
// workspace cockpit that mistakenly imports this) only fails when the surface
// is actually used, with the missing var named.

import "@tanstack/react-start/server-only";

import { z } from "zod";

const ProvisionerConfigSchema = z.object({
	// Primary (engine-metadata) database, ADMIN credentials — CREATEROLE for
	// the per-workspace role mint, DROP SCHEMA for the archive sweep, and the
	// readiness probe on the engine bootstrap's ws_<id>_read schema.
	adminDatabaseUrl: z.string().min(1),
	// The ONE installation-wide DuckLake catalog database (DAT-815), admin
	// credentials — per-workspace catalog `CREATE SCHEMA` / `DROP SCHEMA`.
	catalogDatabaseUrl: z.string().min(1),
	// Caddy admin API (DAT-819) — the route add/remove seam. In-network:
	// `http://caddy:2019`.
	caddyAdminUrl: z.string().min(1),

	// The read-only config tree (DATARAUM_CONFIG_PATH — same env contract as
	// the workspace config's `dataraumConfigPath`, re-declared here per the
	// role-scoped-schema convention this file already uses for S3). The create
	// flow (DAT-821) lists `verticals/*` off it, so the portal container mounts
	// the same tree every other consumer does.
	configPath: z.string().min(1),

	// Object store — the archive sweep deletes the workspace's whole
	// s3://<bucket>/<ws>/ prefix (lake + uploads). Same env contract as the
	// workspace config's S3 block; endpoint is host:port without scheme.
	s3Endpoint: z.string().min(1),
	s3Bucket: z.string().min(1),
	s3Region: z.string().min(1),
	s3UseSsl: z.boolean(),
	s3AccessKeyId: z.string().min(1),
	s3SecretAccessKey: z.string().min(1),

	// Docker-compose driver (the only deployment-specific block): the Engine
	// API socket, the compose project whose seed pair is cloned, and the two
	// reference service names.
	dockerSocketPath: z.string().min(1),
	composeProject: z.string().min(1),
	referenceCockpitService: z.string().min(1),
	referenceEngineService: z.string().min(1),
});

export type ProvisionerConfig = z.infer<typeof ProvisionerConfigSchema>;

let cached: ProvisionerConfig | null = null;

/** Parse-once accessor. Throws (naming the field) when the provisioner env is
 * incomplete — born-loud at first lifecycle call, not at portal boot. */
export function provisionerConfig(): ProvisionerConfig {
	if (cached) {
		return cached;
	}
	const parsed = ProvisionerConfigSchema.safeParse({
		adminDatabaseUrl: process.env.PROVISIONER_DATABASE_URL,
		catalogDatabaseUrl: process.env.DUCKLAKE_CATALOG_URL,
		caddyAdminUrl: process.env.CADDY_ADMIN_URL,
		configPath: process.env.DATARAUM_CONFIG_PATH,
		s3Endpoint: process.env.S3_ENDPOINT,
		s3Bucket: process.env.S3_BUCKET,
		s3Region: process.env.S3_REGION ?? "us-east-1",
		// Env is the string "true"/"false"; default secure (true) when unset —
		// mirrors config.ts (z.coerce.boolean would turn "false" into true).
		s3UseSsl: (process.env.S3_USE_SSL ?? "true") === "true",
		s3AccessKeyId: process.env.S3_ACCESS_KEY_ID,
		s3SecretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
		dockerSocketPath:
			process.env.PROVISIONER_DOCKER_SOCKET ?? "/var/run/docker.sock",
		composeProject: process.env.PROVISIONER_COMPOSE_PROJECT ?? "infra",
		referenceCockpitService:
			process.env.PROVISIONER_REFERENCE_COCKPIT ?? "cockpit",
		referenceEngineService:
			process.env.PROVISIONER_REFERENCE_ENGINE ?? "engine-worker",
	});
	if (!parsed.success) {
		const details = parsed.error.issues
			.map((issue) => `  ${issue.path.join(".") || "(root)"}: ${issue.message}`)
			.join("\n");
		throw new Error(
			`Invalid provisioner configuration — the portal/provisioner env is ` +
				`incomplete (see packages/infra/docker-compose.yml portal service):\n${details}`,
		);
	}
	cached = parsed.data;
	return cached;
}
