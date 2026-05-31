import type { DuckDBConnection } from "@duckdb/node-api";
import { describe, expect, it, vi } from "vitest";

// applyS3Secret imports `#/config`, which parses process.env at module load.
// Mock it with S3 values so the unit test needs no real env. The mock MUST use
// the `#/` alias — a relative `./config` mock does NOT intercept (DAT-381).
vi.mock("#/config", () => ({
	config: {
		s3Endpoint: "seaweedfs:8333",
		s3Region: "us-east-1",
		s3UseSsl: false,
		s3AccessKeyId: "dataraum",
		s3SecretAccessKey: "dataraum-s3-secret",
		s3Bucket: "dataraum-lake",
	},
}));

import { applyS3Secret, buildS3SecretSql } from "./s3-secret";

describe("buildS3SecretSql (DAT-388)", () => {
	it("builds an idempotent path-style secret with escaped literals", () => {
		expect(
			buildS3SecretSql({
				accessKeyId: "dataraum",
				secretAccessKey: "pa'ss",
				endpoint: "seaweedfs:8333",
				region: "us-east-1",
				useSsl: false,
				bucket: "dataraum-lake",
			}),
		).toBe(
			"CREATE OR REPLACE SECRET dataraum_s3 (" +
				"TYPE s3, " +
				"KEY_ID 'dataraum', " +
				"SECRET 'pa\\'ss', " +
				"ENDPOINT 'seaweedfs:8333', " +
				"REGION 'us-east-1', " +
				"URL_STYLE 'path', " +
				"USE_SSL false, " +
				"SCOPE 's3://dataraum-lake'" +
				")",
		);
	});

	it("renders USE_SSL true", () => {
		expect(
			buildS3SecretSql({
				accessKeyId: "k",
				secretAccessKey: "s",
				endpoint: "s3.example.com:443",
				region: "eu-central-1",
				useSsl: true,
				bucket: "dataraum-lake",
			}),
		).toContain("USE_SSL true");
	});

	it("scopes the secret to the configured bucket", () => {
		expect(
			buildS3SecretSql({
				accessKeyId: "k",
				secretAccessKey: "s",
				endpoint: "s3.example.com:443",
				region: "eu-central-1",
				useSsl: true,
				bucket: "dataraum-lake",
			}),
		).toContain("SCOPE 's3://dataraum-lake'");
	});
});

function fakeConn(run: ReturnType<typeof vi.fn>): DuckDBConnection {
	return { run } as unknown as DuckDBConnection;
}

describe("applyS3Secret (DAT-388)", () => {
	it("installs + loads httpfs, then registers the secret from config", async () => {
		const run = vi.fn().mockResolvedValue(undefined);
		await applyS3Secret(fakeConn(run));

		const sql = run.mock.calls.map((c) => c[0] as string);
		expect(sql[0]).toBe("INSTALL httpfs");
		expect(sql[1]).toBe("LOAD httpfs");
		expect(sql[2]).toContain("CREATE OR REPLACE SECRET dataraum_s3");
		expect(sql[2]).toContain("KEY_ID 'dataraum'");
		expect(sql[2]).toContain("USE_SSL false");
		// Scoped to the configured bucket (DAT-386 defense in depth).
		expect(sql[2]).toContain("SCOPE 's3://dataraum-lake'");
	});

	it("tolerates INSTALL httpfs failing (still LOADs + creates the secret)", async () => {
		const run = vi
			.fn()
			.mockRejectedValueOnce(new Error("offline")) // INSTALL httpfs fails
			.mockResolvedValue(undefined);
		await applyS3Secret(fakeConn(run));

		const sql = run.mock.calls.map((c) => c[0] as string);
		expect(sql).toContain("LOAD httpfs");
		expect(sql.some((s) => s.includes("CREATE OR REPLACE SECRET"))).toBe(true);
	});
});
