// Regression: the trustedOrigins pattern must carry the portal origin's PORT.
//
// better-auth matches a wildcard entry against `new URL(url).host`, which
// includes a non-default port. Deriving the pattern from the origin's
// *hostname* breaks every stack moved off :80 with CADDY_HTTP_PORT — the
// sign-out POST from `http://ws1.dataraum.localhost:8000` presents host
// `ws1.dataraum.localhost:8000`, `*.dataraum.localhost` does not match it, and
// better-auth answers 403 INVALID_ORIGIN. That shipped once; this pins it.

import { describe, expect, it } from "vitest";
import { trustedOriginPattern } from "./trusted-origins";

describe("trustedOriginPattern (DAT-819)", () => {
	it("carries a non-default port, so subdomains match when Caddy is moved off :80", () => {
		expect(trustedOriginPattern("http://dataraum.localhost:8000")).toBe(
			"*.dataraum.localhost:8000",
		);
	});

	it("collapses to the bare hostname on the default port", () => {
		expect(trustedOriginPattern("http://dataraum.localhost")).toBe(
			"*.dataraum.localhost",
		);
	});

	it("omits the implicit 443 on a TLS deployment", () => {
		expect(trustedOriginPattern("https://cockpit.example.com")).toBe(
			"*.cockpit.example.com",
		);
	});

	it("keeps an explicit non-default TLS port", () => {
		expect(trustedOriginPattern("https://cockpit.example.com:8443")).toBe(
			"*.cockpit.example.com:8443",
		);
	});
});
