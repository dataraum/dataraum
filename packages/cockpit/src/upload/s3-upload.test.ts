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

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: {
		s3Endpoint: "seaweedfs:8333",
		s3Region: "us-east-1",
		s3UseSsl: false,
		s3AccessKeyId: "k",
		s3SecretAccessKey: "s",
	},
}));

import { s3EndpointUrl } from "./s3-upload";

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
