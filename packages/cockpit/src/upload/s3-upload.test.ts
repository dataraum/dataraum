// Unit test for the @aws-lite endpoint URL form (DAT-386).
//
// Regression guard for the lane-smoke catch: @aws-lite's `endpoint` must be a
// FULL URL it can `new URL()`-parse (host + port + protocol). A bare `host:port`
// makes it treat the whole string as a hostname → getaddrinfo ENOTFOUND. The
// scheme comes from `s3UseSsl`. (The PutObject round-trip itself is covered by
// connect.integration against live SeaweedFS.)
//
// Importing s3-upload boots config.ts + @aws-lite at module load; mock config so
// this stays a pure unit. The mock uses the `#/` alias (a relative one doesn't
// intercept).

import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: {
		s3Endpoint: "seaweedfs:8333",
		s3Region: "us-east-1",
		s3UseSsl: false,
		s3AccessKeyId: "k",
		s3SecretAccessKey: "s",
		s3Bucket: "dataraum-lake",
	},
}));

// Mock the @aws-lite client so listPrefixKeys can be unit-tested without a live
// SeaweedFS. `awsLite(...)` resolves to a client whose `S3.ListObjectsV2` is the
// hoisted mock; the s3 plugin is opaque. (PutObject round-trip is covered by
// connect.integration against live SeaweedFS.)
const { listObjectsV2Mock } = vi.hoisted(() => ({
	listObjectsV2Mock: vi.fn(),
}));
vi.mock("@aws-lite/client", () => ({
	default: vi.fn(async () => ({
		S3: { ListObjectsV2: listObjectsV2Mock, PutObject: vi.fn() },
	})),
}));
vi.mock("@aws-lite/s3", () => ({ default: {} }));

import { listPrefixKeys, s3EndpointUrl } from "./s3-upload";

describe("s3EndpointUrl (DAT-386)", () => {
	it("prefixes http:// for a plain (non-SSL) endpoint", () => {
		expect(s3EndpointUrl("127.0.0.1:8333", false)).toBe(
			"http://127.0.0.1:8333",
		);
	});
	it("prefixes https:// when SSL is on", () => {
		expect(s3EndpointUrl("s3.example.com:443", true)).toBe(
			"https://s3.example.com:443",
		);
	});
	it("produces a URL the host+port parse out of (not a bare hostname)", () => {
		const url = new URL(s3EndpointUrl("seaweedfs:8333", false));
		expect(url.hostname).toBe("seaweedfs");
		expect(url.port).toBe("8333");
		expect(url.protocol).toBe("http:");
	});
});

describe("listPrefixKeys (DAT-378)", () => {
	beforeEach(() => {
		listObjectsV2Mock.mockReset();
	});

	it("returns object keys under the prefix, dropping zero-byte folder markers", async () => {
		listObjectsV2Mock.mockResolvedValueOnce({
			Contents: [
				{ Key: "sel/", Size: 0 }, // folder marker → dropped
				{ Key: "sel/orders.csv", Size: 42 },
				{ Key: "sel/customers.parquet", Size: 99 },
			],
			IsTruncated: false,
		});
		const keys = await listPrefixKeys("dataraum-lake", "sel/");
		expect(keys).toEqual(["sel/orders.csv", "sel/customers.parquet"]);
		expect(listObjectsV2Mock).toHaveBeenCalledTimes(1);
	});

	it("follows ContinuationToken pagination to completion", async () => {
		listObjectsV2Mock
			.mockResolvedValueOnce({
				Contents: [{ Key: "sel/a.csv", Size: 1 }],
				IsTruncated: true,
				NextContinuationToken: "tok-1",
			})
			.mockResolvedValueOnce({
				Contents: [{ Key: "sel/b.csv", Size: 1 }],
				IsTruncated: false,
			});
		const keys = await listPrefixKeys("dataraum-lake", "sel/");
		expect(keys).toEqual(["sel/a.csv", "sel/b.csv"]);
		expect(listObjectsV2Mock).toHaveBeenCalledTimes(2);
		// Second call carries the continuation token from the first page.
		expect(listObjectsV2Mock.mock.calls[1][0].ContinuationToken).toBe("tok-1");
	});

	it("handles an empty prefix (no Contents) as an empty key list", async () => {
		listObjectsV2Mock.mockResolvedValueOnce({ IsTruncated: false });
		expect(await listPrefixKeys("dataraum-lake", "empty/")).toEqual([]);
	});
});
