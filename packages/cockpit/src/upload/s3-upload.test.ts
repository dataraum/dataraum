// Unit test for the S3 endpoint URL form + the prefix-list pagination (DAT-386).
//
// Regression guard: the endpoint must be a FULL URL (host + port + protocol);
// a bare `host:port` is treated as a hostname → getaddrinfo ENOTFOUND. The
// scheme comes from `s3UseSsl`. The write round-trip is covered by
// connect.integration against live SeaweedFS.
//
// Importing s3-upload boots config.ts at module load; mock config so this stays
// a pure unit (the `#/` alias mock intercepts; a relative one doesn't). The Bun
// global S3 client is stubbed so listPrefixKeys is exercised without SeaweedFS.

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

// Stub the Bun global S3 client: `new Bun.S3Client(...)` yields an object whose
// `list` is the hoisted mock (write is unused here).
const { listMock } = vi.hoisted(() => ({ listMock: vi.fn() }));
vi.stubGlobal("Bun", {
	// A regular `function` (not an arrow) so vitest allows `new Bun.S3Client(...)`.
	S3Client: vi.fn(function s3() {
		return { list: listMock, write: vi.fn() };
	}),
});

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
		listMock.mockReset();
	});

	it("returns object keys under the prefix, dropping zero-byte folder markers", async () => {
		listMock.mockResolvedValueOnce({
			contents: [
				{ key: "sel/", size: 0 }, // folder marker → dropped
				{ key: "sel/orders.csv", size: 42 },
				{ key: "sel/customers.parquet", size: 99 },
			],
			isTruncated: false,
		});
		const keys = await listPrefixKeys("dataraum-lake", "sel/");
		expect(keys).toEqual(["sel/orders.csv", "sel/customers.parquet"]);
		expect(listMock).toHaveBeenCalledTimes(1);
	});

	it("follows continuation-token pagination to completion", async () => {
		listMock
			.mockResolvedValueOnce({
				contents: [{ key: "sel/a.csv", size: 1 }],
				isTruncated: true,
				nextContinuationToken: "tok-1",
			})
			.mockResolvedValueOnce({
				contents: [{ key: "sel/b.csv", size: 1 }],
				isTruncated: false,
			});
		const keys = await listPrefixKeys("dataraum-lake", "sel/");
		expect(keys).toEqual(["sel/a.csv", "sel/b.csv"]);
		expect(listMock).toHaveBeenCalledTimes(2);
		// Second call carries the continuation token from the first page.
		expect(listMock.mock.calls[1][0].continuationToken).toBe("tok-1");
	});

	it("handles an empty prefix (no contents) as an empty key list", async () => {
		listMock.mockResolvedValueOnce({ isTruncated: false });
		expect(await listPrefixKeys("dataraum-lake", "empty/")).toEqual([]);
	});
});
