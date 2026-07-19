// Unit tests for the Caddy provisioner seam (DAT-819). A recorded fake fetch
// stands in for the admin API; the contract under test is the call shape
// (@id-tagged route JSON, config-path POST, /id/ DELETE) and the idempotency
// semantics both directions.

import { describe, expect, it, vi } from "vitest";
import {
	addWorkspaceRoute,
	removeWorkspaceRoute,
	workspaceRoute,
	workspaceRouteId,
} from "./caddy";

const SPEC = {
	workspaceId: "00000000-0000-0000-0000-000000000002",
	subdomain: "ws2",
	parentDomain: "dataraum.localhost",
	upstream: "cockpit-2:3000",
};
const ADMIN = "http://caddy:2019";
const ID = `ws-${SPEC.workspaceId}`;

interface Call {
	url: string;
	method: string;
	body?: unknown;
}

/** A fake admin API: scripted responses per (method,url-prefix); records calls. */
function fakeFetch(respond: (url: string, method: string) => Response): {
	impl: typeof fetch;
	calls: Call[];
} {
	const calls: Call[] = [];
	const impl = (async (input: URL | RequestInfo, init?: RequestInit) => {
		const url = String(input);
		const method = init?.method ?? "GET";
		calls.push({
			url,
			method,
			body: init?.body ? JSON.parse(String(init.body)) : undefined,
		});
		return respond(url, method);
	}) as typeof fetch;
	return { impl, calls };
}

describe("workspaceRoute (DAT-819)", () => {
	it("builds the @id-tagged host-matched reverse-proxy route", () => {
		expect(workspaceRoute(SPEC)).toEqual({
			"@id": ID,
			match: [{ host: ["ws2.dataraum.localhost"] }],
			handle: [
				{
					handler: "reverse_proxy",
					upstreams: [{ dial: "cockpit-2:3000" }],
				},
			],
			terminal: true,
		});
	});

	it("derives the admin handle from the workspace id alone", () => {
		expect(workspaceRouteId("abc")).toBe("ws-abc");
	});
});

describe("addWorkspaceRoute", () => {
	it("POSTs the route to the server's route array when absent", async () => {
		const { impl, calls } = fakeFetch((_url, method) =>
			method === "GET"
				? new Response("unknown object id", { status: 500 })
				: new Response("{}", { status: 200 }),
		);
		await addWorkspaceRoute(ADMIN, SPEC, impl);

		expect(calls.map((c) => `${c.method} ${c.url}`)).toEqual([
			`GET ${ADMIN}/id/${ID}`,
			`POST ${ADMIN}/config/apps/http/servers/srv0/routes`,
		]);
		expect(calls[1]?.body).toEqual(workspaceRoute(SPEC));
	});

	it("replaces an existing route with the same id (idempotent re-provision)", async () => {
		const { impl, calls } = fakeFetch((_url, method) =>
			method === "GET"
				? new Response("{}", { status: 200 })
				: new Response("{}", { status: 200 }),
		);
		await addWorkspaceRoute(ADMIN, SPEC, impl);

		expect(calls.map((c) => c.method)).toEqual(["GET", "DELETE", "POST"]);
	});

	it("throws loud with the admin API's error body on a failed add", async () => {
		const { impl } = fakeFetch((_url, method) =>
			method === "GET"
				? new Response("unknown object id", { status: 500 })
				: new Response("invalid route", { status: 400 }),
		);
		await expect(addWorkspaceRoute(ADMIN, SPEC, impl)).rejects.toThrow(
			/adding route ws-.*400.*invalid route/,
		);
	});
});

describe("removeWorkspaceRoute", () => {
	it("DELETEs the route by its /id/ handle", async () => {
		const { impl, calls } = fakeFetch(
			() => new Response("{}", { status: 200 }),
		);
		await removeWorkspaceRoute(ADMIN, SPEC.workspaceId, impl);

		expect(calls).toHaveLength(1);
		expect(`${calls[0]?.method} ${calls[0]?.url}`).toBe(
			`DELETE ${ADMIN}/id/${ID}`,
		);
	});

	it("treats an unknown id as already-removed (idempotent archive retry)", async () => {
		const { impl } = fakeFetch(
			() =>
				new Response(`{"error":"unknown object id '${ID}'"}`, {
					status: 500,
				}),
		);
		await expect(
			removeWorkspaceRoute(ADMIN, SPEC.workspaceId, impl),
		).resolves.toBeUndefined();
	});

	it("throws loud on any other admin API failure", async () => {
		const { impl } = fakeFetch(
			() => new Response("admin api down", { status: 502 }),
		);
		await expect(
			removeWorkspaceRoute(ADMIN, SPEC.workspaceId, impl),
		).rejects.toThrow(/removing route ws-.*502.*admin api down/);
	});
});

describe("fetch injection", () => {
	it("defaults to global fetch (smoke of the default param only)", async () => {
		const spy = vi
			.spyOn(globalThis, "fetch")
			.mockResolvedValue(new Response("{}", { status: 200 }));
		await removeWorkspaceRoute(ADMIN, "x");
		expect(spy).toHaveBeenCalledOnce();
		spy.mockRestore();
	});
});
