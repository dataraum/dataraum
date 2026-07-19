// Docker-compose provisioning driver (DAT-820) — the ONLY deployment-specific
// code in the workspace lifecycle.
//
// Talks the Docker Engine API over the unix socket (Bun's fetch carries a
// `unix` option; no docker CLI in the image). A new workspace's pair is
// CLONED from the compose seed pair (`cockpit` + `engine-worker` services):
// image, env, config bind-mount, network, healthcheck — then the per-workspace
// routing knobs and the minted role secrets are overridden. Cloning keeps the
// per-workspace env contract (the DAT-820 two-workspace smoke's override-file
// shape) in sync with docker-compose.yml BY CONSTRUCTION: a var added to the
// compose service flows into every provisioned workspace on its next
// (re)create, with no second env inventory to drift.
//
// Divergences from the clone, all deliberate:
//   - NO host port publish — a provisioned cockpit is reached through Caddy
//     only (the seed cockpit's :3000 is a dev debug port).
//   - RestartPolicy unless-stopped — a provisioned pair survives a daemon
//     restart without compose to resurrect it.
//   - Compose project labels (+ `ws-<id>-*` service labels) so the pair shows
//     up in `docker compose ps`. Compose does not MANAGE these containers: a
//     bare `up` warns about them as orphans, and `down --remove-orphans`
//     removes them — acceptable dev semantics (stack teardown IS workspace
//     teardown; the registry row survives and re-provisioning converges).
//
// Idempotency contract (driver.ts): `startPair` first removes any existing
// pair (secrets are re-minted per attempt, so replace IS convergence);
// `removePair` treats 404s as already-removed.

import type {
	ProvisioningDriver,
	StartedPair,
	WorkspacePairSpec,
} from "./driver";

/** Docker Engine API version prefix — v1.43 = Docker 24+, the floor for
 * every supported dev/deploy host. */
const API = "/v1.43";

/** Label carrying the workspace id on both provisioned containers — the
 * driver's discovery/teardown key (no name parsing). */
export const WORKSPACE_LABEL = "dev.dataraum.workspace";
/** Label distinguishing the pair members: `engine` | `cockpit`. */
export const ROLE_LABEL = "dev.dataraum.role";

/** Bun's fetch signature — RequestInit plus the unix-socket dial. Injectable
 * so unit tests script the Engine API without a socket. */
export type UnixFetch = (
	url: string,
	init?: RequestInit & { unix?: string },
) => Promise<Response>;

export interface ComposeDriverOptions {
	socketPath: string;
	/** Compose project the seed pair lives in (label filter + name prefix). */
	project: string;
	referenceCockpitService: string;
	referenceEngineService: string;
	fetchImpl?: UnixFetch;
}

// Minimal Engine-API shapes — only the fields the driver reads.
interface ContainerSummary {
	Id: string;
	Labels: Record<string, string>;
}
interface ContainerInspect {
	Id: string;
	State: { Running: boolean; Health?: { Status: string } };
	Config: {
		Image: string;
		Env: string[];
		Healthcheck?: unknown;
	};
	HostConfig: { Binds?: string[] | null; NetworkMode: string };
}

/** `KEY=VAL` env array → ordered map (first `=` splits; values may carry `=`). */
export function parseEnv(env: string[]): Map<string, string> {
	const map = new Map<string, string>();
	for (const entry of env) {
		const idx = entry.indexOf("=");
		if (idx > 0) {
			map.set(entry.slice(0, idx), entry.slice(idx + 1));
		}
	}
	return map;
}

/** Clone + override an env map back into Docker's `KEY=VAL` array form.
 * Overrides replace in place (order preserved); new keys append. */
export function overrideEnv(
	ref: ReadonlyMap<string, string>,
	overrides: Record<string, string>,
): string[] {
	const merged = new Map(ref);
	for (const [key, value] of Object.entries(overrides)) {
		merged.set(key, value);
	}
	return [...merged.entries()].map(([key, value]) => `${key}=${value}`);
}

function requireRefEnv(
	refEnv: ReadonlyMap<string, string>,
	key: string,
	service: string,
): string {
	const value = refEnv.get(key);
	if (!value) {
		throw new Error(
			`[provisioner] reference service '${service}' carries no ${key} — ` +
				"the clone contract expects the compose seed pair's full env",
		);
	}
	return value;
}

/** Swap the credentials on a role-resolved metadata URL: the reference
 * cockpit's URL carries workspace 1's role; the clone gets the new
 * workspace's minted role + secret, same host/db. */
export function withRoleCredentials(
	refUrl: string,
	role: string,
	secret: string,
): string {
	const url = new URL(refUrl);
	url.username = encodeURIComponent(role);
	url.password = encodeURIComponent(secret);
	return url.toString();
}

/** The engine container's per-workspace env deltas over the reference
 * (docker-compose.yml x-engine-worker anchors: the three routing knobs +
 * the DAT-816 role secrets, per-workspace now). */
export function engineEnvOverrides(
	spec: WorkspacePairSpec,
	refEnv: ReadonlyMap<string, string>,
	referenceService: string,
): Record<string, string> {
	const bucket = requireRefEnv(refEnv, "S3_BUCKET", referenceService);
	return {
		DATARAUM_WORKSPACE_ID: spec.workspaceId,
		TEMPORAL_TASK_QUEUE: `engine-${spec.workspaceId}`,
		DUCKLAKE_DATA_PATH: `s3://${bucket}/${spec.workspaceId}/lake`,
		METADATA_READER_PASSWORD: spec.readerSecret,
		METADATA_WRITER_PASSWORD: spec.writerSecret,
	};
}

/** The cockpit container's per-workspace env deltas over the reference —
 * exactly the contract the DAT-820 two-workspace smoke enumerated: identity,
 * subdomain, role-resolved metadata URLs (minted role + secret), lake path. */
export function cockpitEnvOverrides(
	spec: WorkspacePairSpec,
	refEnv: ReadonlyMap<string, string>,
	referenceService: string,
): Record<string, string> {
	const bucket = requireRefEnv(refEnv, "S3_BUCKET", referenceService);
	const readerUrl = requireRefEnv(
		refEnv,
		"METADATA_DATABASE_URL",
		referenceService,
	);
	const writerUrl = requireRefEnv(
		refEnv,
		"METADATA_WRITER_DATABASE_URL",
		referenceService,
	);
	return {
		DATARAUM_WORKSPACE_ID: spec.workspaceId,
		DATARAUM_WORKSPACE_SUBDOMAIN: spec.subdomain,
		METADATA_DATABASE_URL: withRoleCredentials(
			readerUrl,
			spec.readerRole,
			spec.readerSecret,
		),
		METADATA_WRITER_DATABASE_URL: withRoleCredentials(
			writerUrl,
			spec.writerRole,
			spec.writerSecret,
		),
		DATARAUM_LAKE_PATH: `s3://${bucket}/${spec.workspaceId}/lake`,
	};
}

export class ComposeDriver implements ProvisioningDriver {
	constructor(private readonly opts: ComposeDriverOptions) {}

	private containerName(workspaceId: string, role: "engine" | "cockpit") {
		return `${this.opts.project}-ws-${workspaceId}-${role}`;
	}

	private request(
		method: string,
		path: string,
		body?: unknown,
	): Promise<Response> {
		const fetchImpl = this.opts.fetchImpl ?? (fetch as UnixFetch);
		return fetchImpl(`http://docker${API}${path}`, {
			method,
			unix: this.opts.socketPath,
			...(body !== undefined
				? {
						headers: { "content-type": "application/json" },
						body: JSON.stringify(body),
					}
				: {}),
		});
	}

	/** Engine-API call that must succeed; `tolerate` lists non-2xx statuses
	 * that are success for the caller (304 already-started, 404 gone). */
	private async api<T>(
		method: string,
		path: string,
		body?: unknown,
		tolerate: number[] = [],
	): Promise<T | null> {
		const res = await this.request(method, path, body);
		if (res.ok) {
			// 204s carry no body.
			return res.status === 204 ? null : ((await res.json()) as T);
		}
		const text = await res.text();
		if (tolerate.includes(res.status)) {
			return null;
		}
		throw new Error(
			`[provisioner] docker ${method} ${path} failed (${res.status}): ${text}`,
		);
	}

	private async listByLabels(labels: string[]): Promise<ContainerSummary[]> {
		const filters = encodeURIComponent(JSON.stringify({ label: labels }));
		const list = await this.api<ContainerSummary[]>(
			"GET",
			`/containers/json?all=1&filters=${filters}`,
		);
		return list ?? [];
	}

	/** The compose seed container for a service — the clone source. Stopped
	 * containers count (only the recorded config is read). */
	private async findReference(service: string): Promise<ContainerInspect> {
		const matches = await this.listByLabels([
			`com.docker.compose.project=${this.opts.project}`,
			`com.docker.compose.service=${service}`,
		]);
		const [summary] = matches;
		if (!summary) {
			throw new Error(
				`[provisioner] no container for compose service '${service}' in ` +
					`project '${this.opts.project}' — the docker driver clones the ` +
					"seed pair's config; bring the stack up first",
			);
		}
		if (matches.length > 1) {
			// Ambiguous clone source (a leftover crash-recovery container
			// carrying the same labels): picking one silently could clone stale
			// config — same loud posture as the missing-reference case.
			throw new Error(
				`[provisioner] ${matches.length} containers claim compose service ` +
					`'${service}' in project '${this.opts.project}' — remove the ` +
					"stale one(s); the clone source must be unambiguous",
			);
		}
		const inspect = await this.api<ContainerInspect>(
			"GET",
			`/containers/${summary.Id}/json`,
		);
		if (!inspect) {
			throw new Error(
				`[provisioner] inspect of reference '${service}' returned nothing`,
			);
		}
		return inspect;
	}

	private async createAndStart(
		spec: WorkspacePairSpec,
		role: "engine" | "cockpit",
		ref: ContainerInspect,
		envOverrides: Record<string, string>,
	): Promise<string> {
		const name = this.containerName(spec.workspaceId, role);
		const payload = {
			Image: ref.Config.Image,
			Env: overrideEnv(parseEnv(ref.Config.Env), envOverrides),
			Labels: {
				"com.docker.compose.project": this.opts.project,
				"com.docker.compose.service": `ws-${spec.workspaceId}-${role}`,
				"com.docker.compose.oneoff": "False",
				[WORKSPACE_LABEL]: spec.workspaceId,
				[ROLE_LABEL]: role,
			},
			// The cockpit reference carries the compose-level /api/health check;
			// the engine has none (its health is the Temporal worker heartbeat).
			...(ref.Config.Healthcheck
				? { Healthcheck: ref.Config.Healthcheck }
				: {}),
			HostConfig: {
				Binds: ref.HostConfig.Binds ?? [],
				NetworkMode: ref.HostConfig.NetworkMode,
				RestartPolicy: { Name: "unless-stopped" },
			},
		};
		const created = await this.api<{ Id: string }>(
			"POST",
			`/containers/create?name=${encodeURIComponent(name)}`,
			payload,
		);
		if (!created) {
			throw new Error(
				`[provisioner] creating container ${name} returned nothing`,
			);
		}
		// 304 = already started (a re-run racing a live container).
		await this.api("POST", `/containers/${created.Id}/start`, undefined, [304]);
		return name;
	}

	async startPair(spec: WorkspacePairSpec): Promise<StartedPair> {
		const [engineRef, cockpitRef] = await Promise.all([
			this.findReference(this.opts.referenceEngineService),
			this.findReference(this.opts.referenceCockpitService),
		]);
		// Replace-not-reuse: secrets are minted per attempt, so any surviving
		// half-pair from a failed create carries stale env. Removing first is
		// the convergent move (absent = no-op).
		await this.removePair(spec.workspaceId);
		await this.createAndStart(
			spec,
			"engine",
			engineRef,
			engineEnvOverrides(
				spec,
				parseEnv(engineRef.Config.Env),
				this.opts.referenceEngineService,
			),
		);
		const cockpitName = await this.createAndStart(
			spec,
			"cockpit",
			cockpitRef,
			cockpitEnvOverrides(
				spec,
				parseEnv(cockpitRef.Config.Env),
				this.opts.referenceCockpitService,
			),
		);
		return { cockpitUpstream: `${cockpitName}:3000` };
	}

	async pairReady(workspaceId: string): Promise<boolean> {
		const members = await this.listByLabels([
			`${WORKSPACE_LABEL}=${workspaceId}`,
		]);
		const byRole = new Map(members.map((c) => [c.Labels[ROLE_LABEL], c]));
		const engine = byRole.get("engine");
		const cockpit = byRole.get("cockpit");
		if (!engine || !cockpit) {
			return false;
		}
		const [engineState, cockpitState] = await Promise.all([
			this.api<ContainerInspect>("GET", `/containers/${engine.Id}/json`),
			this.api<ContainerInspect>("GET", `/containers/${cockpit.Id}/json`),
		]);
		if (!engineState?.State.Running || !cockpitState?.State.Running) {
			return false;
		}
		// The cockpit clone carries the /api/health check — trust it when
		// present; a checkless container falls back to Running.
		const health = cockpitState.State.Health?.Status;
		return health === undefined || health === "healthy";
	}

	async removePair(workspaceId: string): Promise<void> {
		const members = await this.listByLabels([
			`${WORKSPACE_LABEL}=${workspaceId}`,
		]);
		for (const member of members) {
			// force=1 kills a running container; v=1 sweeps anonymous volumes
			// (the pair mounts none, belt only). 404/409 = concurrently gone.
			await this.api(
				"DELETE",
				`/containers/${member.Id}?force=1&v=1`,
				undefined,
				[404, 409],
			);
		}
	}
}
