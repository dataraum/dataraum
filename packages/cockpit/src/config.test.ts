import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// config.ts parses process.env at import time, so each test resets the module
// registry and re-imports under a freshly stubbed env.

const REQUIRED: Record<string, string> = {
	COCKPIT_DATABASE_URL: "postgresql://u:p@localhost:5432/cockpit",
	METADATA_DATABASE_URL: "postgresql://u:p@localhost:5432/meta",
	DATARAUM_WORKSPACE_ID: "00000000-0000-0000-0000-000000000001",
	DATARAUM_LAKE_PATH: "s3://test-lake/lake",
	DUCKLAKE_CATALOG_URL: "postgresql://u:p@localhost:5432/lake_catalog",
	ANTHROPIC_API_KEY: "sk-ant-test",
	S3_ENDPOINT: "test-s3:8333",
	S3_ACCESS_KEY_ID: "test-access-key",
	S3_SECRET_ACCESS_KEY: "test-secret-key",
	S3_BUCKET: "test-lake",
};

const OPTIONAL = [
	"S3_REGION",
	"S3_USE_SSL",
	"TEMPORAL_HOST",
	"TEMPORAL_NAMESPACE",
	"TEMPORAL_TASK_QUEUE",
	"TEMPORAL_UI_URL",
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
