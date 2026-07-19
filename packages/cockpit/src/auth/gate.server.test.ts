// Unit tests for the workspace membership gate (DAT-819) — the data security
// boundary of a per-workspace cockpit. Mocks the auth instance + cockpit_db at
// the module seams; the matrix is the gate's whole contract: public auth
// prefix, signed-out (HTML redirect vs API 401), member pass-through, and
// non-member rejection (HTML bounce-with-denied vs API 403).

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	session: null as { user: { id: string } } | null,
	membershipRows: [] as Array<{ userId: string }>,
	getSession: vi.fn(),
}));

vi.mock("@tanstack/react-start/server-only", () => ({}));

vi.mock("#/config.base", () => ({
	baseConfig: {
		portalMode: false,
		portalOrigin: "http://dataraum.localhost",
	},
}));

vi.mock("#/auth/auth", () => ({
	auth: {
		api: {
			get getSession() {
				return h.getSession;
			},
		},
	},
}));

vi.mock("#/db/cockpit/registry", () => ({
	bootWorkspaceId: () => "ws-1",
}));

vi.mock("#/db/cockpit/schema", () => ({
	memberships: { userId: "user_id", workspaceId: "workspace_id" },
}));

vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => a,
	and: (...a: unknown[]) => a,
}));

vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		select: () => ({
			from: () => ({
				where: () => ({ limit: async () => h.membershipRows }),
			}),
		}),
	},
}));

import { gateRequest } from "./gate.server";

function req(path: string, accept?: string): Request {
	return new Request(`http://ws1.dataraum.localhost${path}`, {
		headers: accept ? { accept } : {},
	});
}

beforeEach(() => {
	h.session = null;
	h.membershipRows = [];
	h.getSession.mockReset();
	h.getSession.mockImplementation(async () => h.session);
});

describe("gateRequest (DAT-819)", () => {
	it("passes the auth handler prefix without touching the session", async () => {
		expect(await gateRequest(req("/api/auth/sign-out"))).toBeNull();
		expect(h.getSession).not.toHaveBeenCalled();
	});

	it("passes the health probe without a session (compose healthcheck)", async () => {
		expect(await gateRequest(req("/api/health"))).toBeNull();
		expect(h.getSession).not.toHaveBeenCalled();
	});

	it("redirects a signed-out HTML navigation to the portal", async () => {
		const rejection = await gateRequest(req("/cockpit", "text/html"));
		expect(rejection?.status).toBe(302);
		expect(rejection?.headers.get("location")).toBe(
			"http://dataraum.localhost",
		);
	});

	it("401s a signed-out API/RPC call (no redirect-into-HTML garbling)", async () => {
		const rejection = await gateRequest(req("/api/running-runs"));
		expect(rejection?.status).toBe(401);
		expect(await rejection?.json()).toEqual({ error: "unauthenticated" });
	});

	it("passes an authenticated member through", async () => {
		h.session = { user: { id: "u-1" } };
		h.membershipRows = [{ userId: "u-1" }];
		expect(await gateRequest(req("/cockpit", "text/html"))).toBeNull();
	});

	it("bounces an authenticated NON-member's navigation to the portal with ?denied=<ws>", async () => {
		h.session = { user: { id: "u-2" } };
		const rejection = await gateRequest(req("/cockpit", "text/html"));
		expect(rejection?.status).toBe(302);
		expect(rejection?.headers.get("location")).toBe(
			"http://dataraum.localhost/?denied=ws-1",
		);
	});

	it("403s an authenticated NON-member's API/RPC call", async () => {
		h.session = { user: { id: "u-2" } };
		const rejection = await gateRequest(req("/api/running-runs"));
		expect(rejection?.status).toBe(403);
		expect(await rejection?.json()).toEqual({ error: "not_a_member" });
	});
});
