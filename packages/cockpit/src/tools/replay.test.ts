// Unit tests for the replay tool (DAT-343, DAT-413, DAT-422).
//
// Replay takes a SESSION (the named analytical unit the agent thinks in),
// resolves the sources it was built from, and re-runs add_source over them as a
// NEW session. Mocks: `#/config`, the Drizzle metadata client (the source +
// vertical reads off the replayed session, and the seed of the NEW session), and
// `@temporalio/client` (record the start). The regression this still guards: a
// FULL replay re-runs typing, whose per-session rows (type_candidates,
// session_tables) FK to investigation_sessions — so replay MUST seed the NEW
// session row BEFORE starting, with the SAME session_id it hands the workflow.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		temporalTaskQueue: "dataraum-pipeline",
	} as Record<string, unknown>,
	// Records the order of side effects so we can assert resolve-then-seed-then-start.
	calls: [] as string[],
	seededRow: null as Record<string, unknown> | null,
	// Rows the sourcesForSession query returns (empty = nothing to replay).
	sourceRows: [] as Array<{ sourceId: string | null }>,
	// Rows the sessionVertical query returns (empty = session carries no vertical).
	verticalRows: [] as Array<{ vertical: string | null }>,
}));

// Live getter (the unconfigured-guard test reassigns h.config).
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// Metadata client. The NEW session is seeded via insert(...).values(row) — a fresh
// id, so no onConflict handling. `sourcesForSession` reads via
// selectDistinct().from().innerJoin().where(); `sessionVertical` via
// select().from().where().limit().
const valuesMock = vi.fn(async (row: Record<string, unknown>) => {
	h.seededRow = row;
	h.calls.push("seed");
});
const distinctChain: Record<string, unknown> = {};
for (const m of ["from", "innerJoin"]) {
	distinctChain[m] = () => distinctChain;
}
distinctChain.where = () => {
	h.calls.push("resolveSources");
	return Promise.resolve(h.sourceRows);
};
const selectChain: Record<string, unknown> = {};
selectChain.from = () => selectChain;
selectChain.where = () => selectChain;
selectChain.limit = () => {
	h.calls.push("resolveVertical");
	return Promise.resolve(h.verticalRows);
};
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		insert: vi.fn(() => ({ values: valuesMock })),
		selectDistinct: vi.fn(() => distinctChain),
		select: vi.fn(() => selectChain),
	},
}));
vi.mock("#/db/metadata/schema", () => ({
	investigationSessions: {
		sessionId: "session_id",
		vertical: "vertical",
		startedAt: "started_at",
	},
	sessionTables: { sessionId: "session_id", tableId: "table_id" },
	tables: { tableId: "table_id", sourceId: "source_id" },
}));
// metadataDb is fully mocked, so the only drizzle operator replay uses (`eq`)
// just needs to be callable — its return flows into the ignored chain args.
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => a,
}));

// Temporal client: record the start args (after the seed) + hand back a run id.
const startMock = vi.fn(async (_name: string, _opts: unknown) => {
	h.calls.push("start");
	return { firstExecutionRunId: "run-xyz" };
});
const closeMock = vi.fn(async () => {});
vi.mock("@temporalio/client", () => ({
	Connection: { connect: vi.fn(async () => ({ close: closeMock })) },
	Client: vi.fn(function Client() {
		return { workflow: { start: startMock } };
	}),
}));
vi.mock("@temporalio/common", () => ({
	WorkflowIdReusePolicy: { ALLOW_DUPLICATE: "ALLOW_DUPLICATE" },
}));

import { replay } from "./replay";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		temporalTaskQueue: "dataraum-pipeline",
	};
	h.calls = [];
	h.seededRow = null;
	h.sourceRows = [{ sourceId: "src-1" }];
	h.verticalRows = [];
	valuesMock.mockClear();
	startMock.mockClear();
	closeMock.mockClear();
});

describe("replay (DAT-422)", () => {
	it("resolves the session's sources, then seeds a FRESH session BEFORE start", async () => {
		await replay({ session_id: "old-sess", vertical: "finance" });

		// Order: read the replayed session's sources, seed the NEW session row (the
		// typing-phase FK needs it before the run reaches it), then start.
		expect(h.calls).toEqual(["resolveSources", "seed", "start"]);
		expect(h.seededRow?.status).toBe("active");
		expect(h.seededRow?.stepCount).toBe(0);
		expect(h.seededRow?.intent).toBe("replay");
		expect(h.seededRow?.vertical).toBe("finance");
		expect(h.seededRow?.startedAt).toBeInstanceOf(Date);
		// A FRESH session — not the one being replayed.
		expect(typeof h.seededRow?.sessionId).toBe("string");
		expect(h.seededRow?.sessionId).not.toBe("old-sess");
	});

	it("re-runs the session's source SET as a new run keyed by the new session", async () => {
		h.sourceRows = [{ sourceId: "src-1" }, { sourceId: "src-2" }];
		const result = await replay({
			session_id: "old-sess",
			vertical: "finance",
		});
		const newSessionId = h.seededRow?.sessionId as string;

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [
			{ identity: Record<string, unknown>; source_ids: string[] },
		];
		// The run carries the resolved source SET (DAT-422), not a single source.
		expect(args[0].source_ids).toEqual(["src-1", "src-2"]);
		// Source-free identity keyed by the NEW session — the FK match.
		expect(args[0].identity.session_id).toBe(newSessionId);
		expect(args[0].identity.source_id).toBeUndefined();
		expect(result.session_id).toBe(newSessionId);
		expect(result.session_id).not.toBe("old-sess");
		expect(result.source_ids).toEqual(["src-1", "src-2"]);
		// Workflow id is keyed by the NEW run's session (DAT-422).
		expect(opts.workflowId).toBe(`addsource-${WS}-${newSessionId}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("rejects a session with no sources (nothing to replay) — no seed, no start", async () => {
		h.sourceRows = [];
		await expect(replay({ session_id: "empty-sess" })).rejects.toThrow(
			/has no sources to replay/,
		);
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("throws when Temporal is unconfigured and does NOT read, seed, or start", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(replay({ session_id: "old-sess" })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.calls).toEqual([]); // the guard is first — nothing ran
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("resolves the session's framed vertical when vertical is OMITTED", async () => {
		// An omitted vertical must re-run on the session's OWN framed ontology, not
		// silently fall back to _adhoc (which fails the semantic pass).
		h.verticalRows = [{ vertical: "finance" }];
		await replay({ session_id: "old-sess" }); // no vertical passed

		expect(h.calls).toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("finance");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.vertical).toBe("finance");
	});

	it("falls back to _adhoc only when the session carries no vertical", async () => {
		h.verticalRows = []; // session row has no vertical
		await replay({ session_id: "old-sess" }); // no vertical passed

		expect(h.calls).toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("_adhoc");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.vertical).toBe("_adhoc");
	});

	it("an explicit vertical OVERRIDES resolution (no resolver query)", async () => {
		h.verticalRows = [{ vertical: "finance" }];
		await replay({ session_id: "old-sess", vertical: "marketing" });

		// Explicit input short-circuits the resolver — it must not even query.
		expect(h.calls).not.toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("marketing");
	});
});
