import { describe, expect, it } from "vitest";
import { workspaceUrlFor } from "./workspace-url";

describe("workspaceUrlFor (DAT-819)", () => {
	it("hangs the subdomain off the portal origin's host", () => {
		expect(workspaceUrlFor("ws1", "http://dataraum.localhost")).toBe(
			"http://ws1.dataraum.localhost",
		);
	});

	it("keeps a non-default port (Caddy published off :80)", () => {
		expect(workspaceUrlFor("ws2", "http://dataraum.localhost:8000")).toBe(
			"http://ws2.dataraum.localhost:8000",
		);
	});

	it("keeps the scheme (TLS deployments)", () => {
		expect(workspaceUrlFor("acme", "https://cockpit.example.com")).toBe(
			"https://acme.cockpit.example.com",
		);
	});

	it("ignores a path on the configured origin", () => {
		// DATARAUM_PORTAL_ORIGIN is an origin; a stray trailing slash must not
		// leak into the workspace URL.
		expect(workspaceUrlFor("ws1", "http://dataraum.localhost/")).toBe(
			"http://ws1.dataraum.localhost",
		);
	});
});
