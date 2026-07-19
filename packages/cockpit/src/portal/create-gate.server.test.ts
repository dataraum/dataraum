// The DAT-821 authz gate (create-gate.server.ts) on the portal lifecycle fns — the first
// HTTP-reachable trigger of the provisioner, so the two gate helpers get
// direct coverage: portal-role-only, session required, and progress
// visibility = membership OR tracked starter. Handler orchestration (drizzle
// reads, the fire-and-forget) is exercised by the lane smoke on the real
// stack — mocking those chains would test the mock.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	portalMode: true,
	session: null as { user: { id: string; email: string } } | null,
	membershipRows: [] as { userId: string }[],
	setStatus: vi.fn(),
}));

vi.mock("#/config.base", () => ({
	baseConfig: {
		get portalMode() {
			return h.portalMode;
		},
		portalOrigin: "http://dataraum.localhost",
	},
}));
vi.mock("#/auth/auth", () => ({
	auth: { api: { getSession: async () => h.session } },
}));
// The gate reads the request headers for the session cookie — a bare Request
// stands in for the server context. setResponseStatus is spied: it is how a
// rejection's HTTP status reaches the wire (server-fn-error.ts).
vi.mock("@tanstack/react-start/server", () => ({
	getRequest: () => new Request("http://dataraum.localhost/create"),
	setResponseStatus: h.setStatus,
}));
// cockpit_db (bun:sql) — a thenable select chain that resolves the injected
// membership rows.
vi.mock("#/db/cockpit/client", () => {
	const chain = {
		select: () => chain,
		from: () => chain,
		where: () => chain,
		limit: () => Promise.resolve(h.membershipRows),
	};
	return { cockpitDb: chain };
});
// The real lifecycle assembly imports "bun" — never loaded in this test.
vi.mock("#/portal/lifecycle-deps", () => ({
	runLifecycle: async () => {
		throw new Error("not under test");
	},
}));

import {
	requireCreateVisibility,
	requirePortalSession,
} from "#/portal/create-gate.server";
import { trackCreateRun } from "#/portal/create-tracker";

/** The gate must reject with a thrown ERROR (a thrown Response would RESOLVE
 * the RPC client-side — server-fn-error.ts) whose message is the JSON
 * envelope, alongside a setResponseStatus call carrying the HTTP status. */
async function rejectionStatus(promise: Promise<unknown>): Promise<{
	status: number | undefined;
	code: string;
}> {
	try {
		await promise;
	} catch (err) {
		if (err instanceof Error) {
			return {
				status: h.setStatus.mock.lastCall?.[0] as number | undefined,
				code: (JSON.parse(err.message) as { error: string }).error,
			};
		}
		throw err;
	}
	throw new Error("expected a rejection");
}

beforeEach(() => {
	h.portalMode = true;
	h.session = null;
	h.membershipRows = [];
	h.setStatus.mockClear();
});

describe("requirePortalSession", () => {
	it("rejects a workspace cockpit with 403 — provisioning is portal-only", async () => {
		h.portalMode = false;
		h.session = { user: { id: "u1", email: "u1@x" } };
		expect(await rejectionStatus(requirePortalSession())).toEqual({
			status: 403,
			code: "portal_only",
		});
	});

	it("rejects a signed-out request with 401", async () => {
		expect(await rejectionStatus(requirePortalSession())).toEqual({
			status: 401,
			code: "unauthenticated",
		});
	});

	it("returns the session for a signed-in portal request", async () => {
		h.session = { user: { id: "u1", email: "u1@x" } };
		await expect(requirePortalSession()).resolves.toBe(h.session);
	});
});

describe("requireCreateVisibility", () => {
	it("passes a member", async () => {
		h.membershipRows = [{ userId: "u1" }];
		await expect(
			requireCreateVisibility("ws-a", "u1"),
		).resolves.toBeUndefined();
	});

	it("passes the tracked starter before the membership row exists", async () => {
		trackCreateRun("ws-pre-row", "u1", new Promise(() => {}));
		await expect(
			requireCreateVisibility("ws-pre-row", "u1"),
		).resolves.toBeUndefined();
	});

	it("rejects everyone else with 403", async () => {
		trackCreateRun("ws-other", "u1", new Promise(() => {}));
		expect(
			await rejectionStatus(requireCreateVisibility("ws-other", "u2")),
		).toEqual({ status: 403, code: "not_a_member" });
	});
});
