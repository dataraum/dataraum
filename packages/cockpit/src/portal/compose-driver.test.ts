// Unit tests for the docker-compose provisioning driver (DAT-820). A recorded
// fake fetch stands in for the Docker Engine API; the contract under test is
// the clone-and-override shape (env deltas, labels, no port publish,
// healthcheck pass-through), the replace-not-reuse startPair sequence, and
// the idempotent teardown semantics.

import { describe, expect, it } from "vitest";
import {
	ComposeDriver,
	cockpitEnvOverrides,
	engineEnvOverrides,
	overrideEnv,
	parseEnv,
	ROLE_LABEL,
	WORKSPACE_LABEL,
	withRoleCredentials,
} from "./compose-driver";
import type { WorkspacePairSpec } from "./driver";

const WS = "33333333-3333-4333-8333-333333333333";

const SPEC: WorkspacePairSpec = {
	workspaceId: WS,
	subdomain: "ws3",
	readerRole: "ws_33333333_3333_4333_8333_333333333333_reader",
	writerRole: "ws_33333333_3333_4333_8333_333333333333_writer",
	readerSecret: "reader-secret",
	writerSecret: "writer-secret",
};

const ENGINE_REF_ENV = [
	"PATH=/usr/bin",
	"DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001",
	"TEMPORAL_TASK_QUEUE=engine-00000000-0000-0000-0000-000000000001",
	"DUCKLAKE_DATA_PATH=s3://dataraum-lake/00000000-0000-0000-0000-000000000001/lake",
	"S3_BUCKET=dataraum-lake",
	"METADATA_READER_PASSWORD=cockpit-reader-dev",
	"METADATA_WRITER_PASSWORD=cockpit-writer-dev",
	"ANTHROPIC_API_KEY=sk-ant-x",
];

const COCKPIT_REF_ENV = [
	"PATH=/usr/bin",
	"DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001",
	"DATARAUM_WORKSPACE_SUBDOMAIN=ws1",
	"METADATA_DATABASE_URL=postgresql://ws_00000000_0000_0000_0000_000000000001_reader:cockpit-reader-dev@postgres:5432/dataraum",
	"METADATA_WRITER_DATABASE_URL=postgresql://ws_00000000_0000_0000_0000_000000000001_writer:cockpit-writer-dev@postgres:5432/dataraum",
	"DATARAUM_LAKE_PATH=s3://dataraum-lake/00000000-0000-0000-0000-000000000001/lake",
	"S3_BUCKET=dataraum-lake",
	"BETTER_AUTH_SECRET=dataraum-dev-secret",
];

interface Call {
	method: string;
	path: string;
	body?: unknown;
	unix?: string;
}

/** A scripted Engine API: `respond` maps (method, path) → Response; every
 * call is recorded with its parsed body. */
function fakeDocker(respond: (method: string, path: string) => Response): {
	impl: (
		url: string,
		init?: RequestInit & { unix?: string },
	) => Promise<Response>;
	calls: Call[];
} {
	const calls: Call[] = [];
	const impl = async (
		url: string,
		init?: RequestInit & { unix?: string },
	): Promise<Response> => {
		const path = url.replace("http://docker/v1.43", "");
		const method = init?.method ?? "GET";
		calls.push({
			method,
			path,
			body: init?.body ? JSON.parse(String(init.body)) : undefined,
			unix: init?.unix,
		});
		return respond(method, path);
	};
	return { impl, calls };
}

function driverWith(respond: (method: string, path: string) => Response) {
	const { impl, calls } = fakeDocker(respond);
	const driver = new ComposeDriver({
		socketPath: "/var/run/docker.sock",
		project: "infra",
		referenceCockpitService: "cockpit",
		referenceEngineService: "engine-worker",
		fetchImpl: impl,
	});
	return { driver, calls };
}

function json(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), { status });
}

function inspectPayload(
	env: string[],
	opts: {
		running?: boolean;
		health?: string;
		healthcheck?: unknown;
	} = {},
): unknown {
	return {
		Id: "ref-id",
		State: {
			Running: opts.running ?? true,
			...(opts.health ? { Health: { Status: opts.health } } : {}),
		},
		Config: {
			Image: "infra-cockpit",
			Env: env,
			...(opts.healthcheck ? { Healthcheck: opts.healthcheck } : {}),
		},
		HostConfig: {
			Binds: ["/host/config:/opt/dataraum/config:ro"],
			NetworkMode: "infra_default",
		},
	};
}

describe("env clone helpers (DAT-820)", () => {
	it("parseEnv splits on the first '=' only", () => {
		const map = parseEnv(["A=1", "URL=postgresql://u:p@h/db?a=b"]);
		expect(map.get("A")).toBe("1");
		expect(map.get("URL")).toBe("postgresql://u:p@h/db?a=b");
	});

	it("overrideEnv replaces in place and appends new keys", () => {
		const out = overrideEnv(parseEnv(["A=1", "B=2"]), { B: "3", C: "4" });
		expect(out).toEqual(["A=1", "B=3", "C=4"]);
	});

	it("withRoleCredentials swaps only the credentials", () => {
		expect(
			withRoleCredentials(
				"postgresql://old_reader:old-pw@postgres:5432/dataraum",
				"new_reader",
				"s3cr;t/&",
			),
		).toBe(
			`postgresql://new_reader:${encodeURIComponent("s3cr;t/&")}@postgres:5432/dataraum`,
		);
	});

	it("engineEnvOverrides carries the three routing knobs + minted secrets", () => {
		expect(
			engineEnvOverrides(SPEC, parseEnv(ENGINE_REF_ENV), "engine-worker"),
		).toEqual({
			DATARAUM_WORKSPACE_ID: WS,
			TEMPORAL_TASK_QUEUE: `engine-${WS}`,
			DUCKLAKE_DATA_PATH: `s3://dataraum-lake/${WS}/lake`,
			METADATA_READER_PASSWORD: "reader-secret",
			METADATA_WRITER_PASSWORD: "writer-secret",
		});
	});

	it("cockpitEnvOverrides rewrites the role URLs to the minted roles", () => {
		const overrides = cockpitEnvOverrides(
			SPEC,
			parseEnv(COCKPIT_REF_ENV),
			"cockpit",
		);
		expect(overrides.DATARAUM_WORKSPACE_ID).toBe(WS);
		expect(overrides.DATARAUM_WORKSPACE_SUBDOMAIN).toBe("ws3");
		expect(overrides.METADATA_DATABASE_URL).toBe(
			`postgresql://${SPEC.readerRole}:reader-secret@postgres:5432/dataraum`,
		);
		expect(overrides.METADATA_WRITER_DATABASE_URL).toBe(
			`postgresql://${SPEC.writerRole}:writer-secret@postgres:5432/dataraum`,
		);
		expect(overrides.DATARAUM_LAKE_PATH).toBe(`s3://dataraum-lake/${WS}/lake`);
	});

	it("throws loud when the reference env misses a clone-contract var", () => {
		expect(() => engineEnvOverrides(SPEC, new Map(), "engine-worker")).toThrow(
			/S3_BUCKET/,
		);
	});
});

describe("ComposeDriver.startPair (DAT-820)", () => {
	function scriptedCreate() {
		return driverWith((method, path) => {
			if (path.startsWith("/containers/json")) {
				const filters = decodeURIComponent(path.split("filters=")[1] ?? "");
				if (filters.includes("com.docker.compose.service=engine-worker")) {
					return json([{ Id: "engine-ref", Labels: {} }]);
				}
				if (filters.includes("com.docker.compose.service=cockpit")) {
					return json([{ Id: "cockpit-ref", Labels: {} }]);
				}
				// Workspace-label lookup (the replace sweep): nothing exists yet.
				return json([]);
			}
			if (path === "/containers/engine-ref/json") {
				return json(inspectPayload(ENGINE_REF_ENV));
			}
			if (path === "/containers/cockpit-ref/json") {
				return json(
					inspectPayload(COCKPIT_REF_ENV, {
						healthcheck: { Test: ["CMD", "wget"] },
					}),
				);
			}
			if (method === "POST" && path.startsWith("/containers/create")) {
				return json({ Id: "new-id" }, 201);
			}
			if (method === "POST" && path.endsWith("/start")) {
				return new Response(null, { status: 204 });
			}
			throw new Error(`unexpected docker call: ${method} ${path}`);
		});
	}

	it("clones the references, overrides the knobs, publishes no ports", async () => {
		const { driver, calls } = scriptedCreate();
		const started = await driver.startPair(SPEC);
		expect(started.cockpitUpstream).toBe(`infra-ws-${WS}-cockpit:3000`);

		const creates = calls.filter((c) =>
			c.path.startsWith("/containers/create"),
		);
		expect(creates.map((c) => c.path)).toEqual([
			`/containers/create?name=${encodeURIComponent(`infra-ws-${WS}-engine`)}`,
			`/containers/create?name=${encodeURIComponent(`infra-ws-${WS}-cockpit`)}`,
		]);

		const [engineCreate, cockpitCreate] = creates.map(
			(c) => c.body as Record<string, unknown>,
		);
		// Env: reference clone + overrides (spot-check the override landed in
		// place and the untouched clone survived).
		expect(engineCreate.Env).toContain(`DATARAUM_WORKSPACE_ID=${WS}`);
		expect(engineCreate.Env).toContain("ANTHROPIC_API_KEY=sk-ant-x");
		expect(engineCreate.Env).toContain(
			"METADATA_READER_PASSWORD=reader-secret",
		);
		expect(cockpitCreate.Env).toContain(`DATARAUM_WORKSPACE_SUBDOMAIN=ws3`);
		expect(cockpitCreate.Env).toContain(
			"BETTER_AUTH_SECRET=dataraum-dev-secret",
		);

		// Labels: compose visibility + the discovery keys.
		const labels = cockpitCreate.Labels as Record<string, string>;
		expect(labels["com.docker.compose.project"]).toBe("infra");
		expect(labels[WORKSPACE_LABEL]).toBe(WS);
		expect(labels[ROLE_LABEL]).toBe("cockpit");

		// Healthcheck rides along on the cockpit (reference has one), not the
		// engine (heartbeat-only); ports are never published.
		expect(cockpitCreate.Healthcheck).toEqual({ Test: ["CMD", "wget"] });
		expect(engineCreate.Healthcheck).toBeUndefined();
		expect(engineCreate.ExposedPorts).toBeUndefined();
		expect(
			(engineCreate.HostConfig as Record<string, unknown>).PortBindings,
		).toBeUndefined();
		expect(
			(engineCreate.HostConfig as Record<string, unknown>).RestartPolicy,
		).toEqual({ Name: "unless-stopped" });

		// Both containers started.
		expect(calls.filter((c) => c.path.endsWith("/start"))).toHaveLength(2);
		// Every call dialed the socket.
		expect(calls.every((c) => c.unix === "/var/run/docker.sock")).toBe(true);
	});

	it("removes any existing pair before creating (replace-not-reuse)", async () => {
		let removed = 0;
		const { driver, calls } = driverWith((method, path) => {
			if (path.startsWith("/containers/json")) {
				const filters = decodeURIComponent(path.split("filters=")[1] ?? "");
				if (filters.includes(WORKSPACE_LABEL)) {
					// A stale half-pair from a failed prior attempt.
					return json(
						removed
							? []
							: [{ Id: "stale-engine", Labels: { [ROLE_LABEL]: "engine" } }],
					);
				}
				return json([
					{
						Id: filters.includes("engine") ? "engine-ref" : "cockpit-ref",
						Labels: {},
					},
				]);
			}
			if (path.startsWith("/containers/stale-engine?")) {
				removed += 1;
				return new Response(null, { status: 204 });
			}
			if (path === "/containers/cockpit-ref/json") {
				return json(inspectPayload(COCKPIT_REF_ENV));
			}
			if (path.endsWith("/json")) {
				return json(inspectPayload(ENGINE_REF_ENV));
			}
			if (path.startsWith("/containers/create")) {
				return json({ Id: "new-id" }, 201);
			}
			if (path.endsWith("/start")) {
				return new Response(null, { status: 204 });
			}
			throw new Error(`unexpected docker call: ${method} ${path}`);
		});
		await driver.startPair(SPEC);
		expect(removed).toBe(1);
		const deleteCall = calls.find((c) => c.method === "DELETE");
		expect(deleteCall?.path).toBe("/containers/stale-engine?force=1&v=1");
	});

	it("fails loud when the reference pair is missing", async () => {
		const { driver } = driverWith((_method, path) => {
			if (path.startsWith("/containers/json")) {
				return json([]);
			}
			throw new Error("unreachable");
		});
		await expect(driver.startPair(SPEC)).rejects.toThrow(
			/no container for compose service/,
		);
	});

	it("fails loud when the reference is ambiguous (stale twin)", async () => {
		const { driver } = driverWith((_method, path) => {
			if (path.startsWith("/containers/json")) {
				return json([
					{ Id: "ref-a", Labels: {} },
					{ Id: "ref-b", Labels: {} },
				]);
			}
			throw new Error("unreachable");
		});
		await expect(driver.startPair(SPEC)).rejects.toThrow(
			/2 containers claim compose service/,
		);
	});
});

describe("ComposeDriver.pairReady (DAT-820)", () => {
	function readiness(opts: {
		members: Array<{ id: string; role: string }>;
		inspect: Record<string, unknown>;
	}) {
		const { driver } = driverWith((_method, path) => {
			if (path.startsWith("/containers/json")) {
				return json(
					opts.members.map((m) => ({
						Id: m.id,
						Labels: { [ROLE_LABEL]: m.role, [WORKSPACE_LABEL]: WS },
					})),
				);
			}
			const id = path.replace("/containers/", "").replace("/json", "");
			return json(opts.inspect[id]);
		});
		return driver;
	}

	it("false while the pair is incomplete", async () => {
		const driver = readiness({
			members: [{ id: "e", role: "engine" }],
			inspect: {},
		});
		expect(await driver.pairReady(WS)).toBe(false);
	});

	it("gates on the cockpit healthcheck when present", async () => {
		const base = {
			members: [
				{ id: "e", role: "engine" },
				{ id: "c", role: "cockpit" },
			],
		};
		const starting = readiness({
			...base,
			inspect: {
				e: inspectPayload([], { running: true }),
				c: inspectPayload([], { running: true, health: "starting" }),
			},
		});
		expect(await starting.pairReady(WS)).toBe(false);

		const healthy = readiness({
			...base,
			inspect: {
				e: inspectPayload([], { running: true }),
				c: inspectPayload([], { running: true, health: "healthy" }),
			},
		});
		expect(await healthy.pairReady(WS)).toBe(true);
	});
});

describe("ComposeDriver.removePair (DAT-820)", () => {
	it("tolerates concurrently-gone containers (404) and in-flight removals (409)", async () => {
		for (const [status, body] of [
			[404, "no such container"],
			[409, "removal of container is already in progress"],
		] as const) {
			const { driver, calls } = driverWith((method, path) => {
				if (path.startsWith("/containers/json")) {
					return json([{ Id: "gone", Labels: { [ROLE_LABEL]: "cockpit" } }]);
				}
				if (method === "DELETE") {
					return new Response(body, { status });
				}
				throw new Error(`unexpected docker call: ${method} ${path}`);
			});
			await expect(driver.removePair(WS)).resolves.toBeUndefined();
			expect(calls.some((c) => c.method === "DELETE")).toBe(true);
		}
	});
});
