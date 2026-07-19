import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// config.ts parses process.env at import time, so each test resets the module
// registry and re-imports under a freshly stubbed env.

const REQUIRED: Record<string, string> = {
	COCKPIT_DATABASE_URL: "postgresql://u:p@localhost:5432/cockpit",
	METADATA_DATABASE_URL: "postgresql://u:p@localhost:5432/meta",
	METADATA_WRITER_DATABASE_URL: "postgresql://w:p@localhost:5432/meta",
	DATARAUM_WORKSPACE_ID: "00000000-0000-0000-0000-000000000001",
	DATARAUM_CONFIG_PATH: "/opt/dataraum/config",
	DATARAUM_LAKE_PATH: "s3://test-lake/lake",
	DUCKLAKE_CATALOG_URL: "postgresql://u:p@localhost:5432/lake_catalog",
	ANTHROPIC_API_KEY: "sk-ant-test",
	S3_ENDPOINT: "test-s3:8333",
	S3_ACCESS_KEY_ID: "test-access-key",
	S3_SECRET_ACCESS_KEY: "test-secret-key",
	S3_BUCKET: "test-lake",
	// Base (mode-shared) config (DAT-819): ./config imports ./config.base, so
	// its required fields are part of the workspace baseline too.
	BETTER_AUTH_SECRET: "test-auth-secret",
};

const OPTIONAL = [
	"S3_REGION",
	"S3_USE_SSL",
	"DUCKDB_EXTENSION_DIRECTORY",
	"DUCKLAKE_SKIP_INSTALL",
	"TEMPORAL_HOST",
	"TEMPORAL_NAMESPACE",
	"TEMPORAL_TASK_QUEUE",
	"TEMPORAL_UI_URL",
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"DATARAUM_PORTAL_MODE",
	"DATARAUM_PORTAL_ORIGIN",
	"DATARAUM_WORKSPACE_SUBDOMAIN",
	"DATARAUM_DEV_USER_EMAIL",
	"DATARAUM_DEV_USER_PASSWORD",
];

function stubBaseline(): void {
	for (const [key, value] of Object.entries(REQUIRED)) vi.stubEnv(key, value);
	// Clear optional vars so a leaked shell value doesn't skew assertions.
	for (const key of OPTIONAL) vi.stubEnv(key, undefined as unknown as string);
}

beforeEach(() => {
	vi.resetModules();
});

afterEach(() => {
	vi.unstubAllEnvs();
});

describe("cockpit config (DAT-363)", () => {
	it("parses when all required vars are present; temporal stays optional", async () => {
		stubBaseline();
		const { config } = await import("./config");

		expect(config.cockpitDatabaseUrl).toBe(REQUIRED.COCKPIT_DATABASE_URL);
		expect(config.metadataDatabaseUrl).toBe(REQUIRED.METADATA_DATABASE_URL);
		// The two metadata ROLE URLs stay separate connections (DAT-816).
		expect(config.metadataWriterDatabaseUrl).toBe(
			REQUIRED.METADATA_WRITER_DATABASE_URL,
		);
		expect(config.dataraumWorkspaceId).toBe(REQUIRED.DATARAUM_WORKSPACE_ID);
		expect(config.ducklakeCatalogUrl).toBe(REQUIRED.DUCKLAKE_CATALOG_URL);
		expect(config.anthropicApiKey).toBe(REQUIRED.ANTHROPIC_API_KEY);
		// Object store (DAT-388): creds read from env; region/useSsl default.
		expect(config.s3Endpoint).toBe(REQUIRED.S3_ENDPOINT);
		expect(config.s3AccessKeyId).toBe(REQUIRED.S3_ACCESS_KEY_ID);
		expect(config.s3SecretAccessKey).toBe(REQUIRED.S3_SECRET_ACCESS_KEY);
		// Upload-staging bucket (DAT-386) — required, same bucket as the lake.
		expect(config.s3Bucket).toBe(REQUIRED.S3_BUCKET);
		expect(config.s3Region).toBe("us-east-1");
		expect(config.s3UseSsl).toBe(true);
		expect(config.temporalHost).toBeUndefined();
		// Temporal Web UI URL defaults to the compose dev address.
		expect(config.temporalUiUrl).toBe("http://localhost:8080");
	});

	it("parses S3_USE_SSL=false to the boolean false (not the truthy string)", async () => {
		stubBaseline();
		vi.stubEnv("S3_USE_SSL", "false");
		const { config } = await import("./config");

		expect(config.s3UseSsl).toBe(false);
	});

	it("defaults the DuckDB extension cache to host-dev behavior when unset", async () => {
		stubBaseline();
		const { config } = await import("./config");

		expect(config.duckdbExtensionDirectory).toBeUndefined();
		expect(config.ducklakeSkipInstall).toBe(false);
	});

	it("parses the container's pre-baked extension cache contract", async () => {
		// The image sets both (Dockerfile): DUCKLAKE_SKIP_INSTALL=1 is the same
		// env contract as the engine's worker.Dockerfile.
		stubBaseline();
		vi.stubEnv("DUCKDB_EXTENSION_DIRECTORY", "/opt/dataraum/duckdb-extensions");
		vi.stubEnv("DUCKLAKE_SKIP_INSTALL", "1");
		const { config } = await import("./config");

		expect(config.duckdbExtensionDirectory).toBe(
			"/opt/dataraum/duckdb-extensions",
		);
		expect(config.ducklakeSkipInstall).toBe(true);
	});

	it("keeps telemetry OFF for both unset AND empty OTLP endpoint (ADR-0019)", async () => {
		// Empty string = compose interpolation of an unset var; `|| undefined`
		// in loadBaseConfig maps it to off, never a half-configured exporter.
		// (Field moved to config.base — DAT-819: the otel plugin boots in
		// portal mode too.)
		stubBaseline();
		vi.stubEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "");
		const { baseConfig } = await import("./config.base");

		expect(baseConfig.otelExporterOtlpEndpoint).toBeUndefined();
	});

	it("parses the OTLP endpoint when set (the single telemetry switch)", async () => {
		stubBaseline();
		vi.stubEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-lgtm:4318");
		const { baseConfig } = await import("./config.base");

		expect(baseConfig.otelExporterOtlpEndpoint).toBe("http://otel-lgtm:4318");
	});

	it("fails loud naming the field when a required var is missing", async () => {
		stubBaseline();
		vi.stubEnv("COCKPIT_DATABASE_URL", undefined as unknown as string);

		await expect(import("./config")).rejects.toThrow(/cockpitDatabaseUrl/);
	});

	it("fails loud when an S3 credential is missing", async () => {
		stubBaseline();
		vi.stubEnv("S3_ACCESS_KEY_ID", undefined as unknown as string);

		await expect(import("./config")).rejects.toThrow(/s3AccessKeyId/);
	});
});

describe("mode-shared base config (DAT-819)", () => {
	it("parses in portal mode WITHOUT any workspace env", async () => {
		// The portal container carries only the base fields — no metadata role
		// URLs, no S3, no workspace id. Base config must stand alone.
		vi.stubEnv("DATARAUM_PORTAL_MODE", "1");
		vi.stubEnv("COCKPIT_DATABASE_URL", REQUIRED.COCKPIT_DATABASE_URL);
		vi.stubEnv("BETTER_AUTH_SECRET", REQUIRED.BETTER_AUTH_SECRET);
		vi.stubEnv("DATARAUM_PORTAL_ORIGIN", "http://dataraum.localhost");
		const { baseConfig } = await import("./config.base");

		expect(baseConfig.portalMode).toBe(true);
		expect(baseConfig.portalOrigin).toBe("http://dataraum.localhost");
		expect(baseConfig.authSecret).toBe(REQUIRED.BETTER_AUTH_SECRET);
	});

	it("defaults the portal origin to the bare host-dev address", async () => {
		stubBaseline();
		const { baseConfig } = await import("./config.base");

		expect(baseConfig.portalMode).toBe(false);
		expect(baseConfig.portalOrigin).toBe("http://localhost:3000");
	});

	it("throws born-loud when a workspace surface evaluates ./config in portal mode", async () => {
		// The fence: portal-mode boot must never reach workspace config — a
		// mis-routed workspace surface fails with the real reason, not a
		// missing-env scavenger hunt.
		stubBaseline();
		vi.stubEnv("DATARAUM_PORTAL_MODE", "1");

		await expect(import("./config")).rejects.toThrow(
			/workspace config accessed in portal mode/,
		);
	});

	it("fails loud when the auth secret is missing (required in BOTH modes)", async () => {
		stubBaseline();
		vi.stubEnv("BETTER_AUTH_SECRET", undefined as unknown as string);

		await expect(import("./config.base")).rejects.toThrow(/authSecret/);
	});

	it("requires dev seed credentials to be non-empty when set", async () => {
		// Compose interpolation of an unset var yields "" — that must read as
		// ABSENT (no dev user), never as an empty-string credential.
		stubBaseline();
		vi.stubEnv("DATARAUM_DEV_USER_EMAIL", "");
		vi.stubEnv("DATARAUM_DEV_USER_PASSWORD", "");
		const { baseConfig } = await import("./config.base");

		expect(baseConfig.devUserEmail).toBeUndefined();
		expect(baseConfig.devUserPassword).toBeUndefined();
	});
});
