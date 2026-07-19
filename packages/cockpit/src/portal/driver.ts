// Provisioning-driver interface (DAT-820) — THE deployment seam.
//
// A workspace's compute pair (one engine worker + one cockpit) is the only
// resource whose creation is deployment-specific: everything else the
// lifecycle touches (registry rows, Postgres schemas/roles, Caddy routes, the
// S3 prefix) speaks a deployment-independent protocol. The lifecycle
// (lifecycle.ts) depends on THIS interface only; `compose-driver.ts` is the
// docker-compose implementation for the dev/single-host deployment, and a
// future k8s/nomad driver replaces it without touching lifecycle logic.

/** Everything a driver needs to start one workspace's engine+cockpit pair.
 * The secrets are the per-workspace metadata role passwords minted by the
 * lifecycle (they exist ONLY here and in the started containers' env — the
 * registry records role NAMES, never secrets). */
export interface WorkspacePairSpec {
	workspaceId: string;
	/** The registry `subdomain` label, e.g. `ws3` — the new cockpit's
	 * DATARAUM_WORKSPACE_SUBDOMAIN. */
	subdomain: string;
	/** Minted per-workspace metadata role names (recorded on the registry row). */
	readerRole: string;
	writerRole: string;
	/** Minted per-workspace role secrets — wired into the pair's env: the
	 * engine re-asserts them onto the roles each boot (DAT-816 seam), the
	 * cockpit embeds them in its role-resolved metadata URLs. */
	readerSecret: string;
	writerSecret: string;
}

export interface StartedPair {
	/** The new cockpit's dial address as CADDY reaches it (host:port on the
	 * shared network), e.g. `infra-ws-<id>-cockpit:3000`. */
	cockpitUpstream: string;
}

/**
 * Lifecycle contract (ADR-0010 discipline): every operation is IDEMPOTENT and
 * convergent — a re-run after a mid-create crash must end in the same state
 * as a clean run. `startPair` REPLACES any existing pair for the workspace
 * (secrets are re-minted per create attempt, so recreating is the convergent
 * move); `removePair` treats an absent pair as already-removed.
 */
export interface ProvisioningDriver {
	/** (Re)create and start the workspace's engine+cockpit pair. */
	startPair(spec: WorkspacePairSpec): Promise<StartedPair>;
	/** True once the pair is up: engine running, cockpit healthy. Never
	 * throws for a merely-absent/starting pair — that is `false`. */
	pairReady(workspaceId: string): Promise<boolean>;
	/** Stop and remove the pair. Absent containers are success. */
	removePair(workspaceId: string): Promise<void>;
}
