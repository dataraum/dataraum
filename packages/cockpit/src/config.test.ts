import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// config.ts parses process.env at import time, so each test resets the module
// registry and re-imports under a freshly stubbed env.

const REQUIRED: Record<string, string> = {
	COCKPIT_DATABASE_URL: "postgresql://u:p@localhost:5432/cockpit",
	METADATA_DATABASE_URL: "postgresql://u:p@localhost:5432/meta",
	DATARAUM_WORKSPACE_ID: "00000000-0000-0000-0000-000000000001",
	DATARAUM_LAKE_PATH: "/var/lib/dataraum/lake",
	DUCKLAKE_CATALOG_URL: "postgresql://u:p@localhost:5432/lake_catalog",
	ANTHROPIC_API_KEY: "sk-ant-test",
};

const OPTIONAL = [
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
		expect(config.temporalHost).toBeUndefined();
		// Temporal Web UI URL defaults to the compose dev address.
		expect(config.temporalUiUrl).toBe("http://localhost:8080");
	});

	it("fails loud naming the field when a required var is missing", async () => {
		stubBaseline();
		vi.stubEnv("COCKPIT_DATABASE_URL", undefined as unknown as string);

		await expect(import("./config")).rejects.toThrow(/cockpitDatabaseUrl/);
	});
});
