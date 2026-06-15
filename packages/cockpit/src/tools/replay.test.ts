// Unit tests for the replay tool (DAT-343, DAT-413, DAT-422, DAT-506).
//
// Replay takes a SESSION (the named analytical unit the agent thinks in),
// resolves the workspace's currently-imported sources, and re-runs add_source over
// them as a NEW session. DAT-506: the engine mints its own run_id the cockpit never
// sees, so a per-session join is impossible at the cockpit edge — replay resolves
// the sources from the live per-table GENERATION heads (metadata_snapshot_head →
// run_tables → tables → source), which in single-active-workspace are the session's
// sources. There is NO engine seed; the new replay session/run is recorded in
// cockpit_db BEFORE the workflow starts, and the vertical is the workspace property
// from the registry (no per-session pick).
//
// Mocks: `#/config`, the cockpit registry + runs writer, the Drizzle metadata
// client (generation-head → source ids), and `@temporalio/client`.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	// Records the order of side effects so we can assert resolve-then-record-then-start.
	calls: [] as string[],
	recordedRun: null as Record<string, unknown> | null,
	// Rows the generation-head → run_tables → tables source query returns
	// (empty = nothing imported = nothing to replay).
	sourceRows: [] as Array<{ sourceId: string | null }>,
	// The current session currentSessionId() resolves (null = none — replay rejects).
	currentSession: null as string | null,
	recordRun: vi.fn(async (input: Record<string, unknown>) => {
		h.recordedRun = input;
		h.calls.push("record");
	}),
	attachRunId: vi.fn(async () => {}),
}));

// Live getter (the unconfigured-guard test reassigns h.config).
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// cockpit_db control plane (DAT-461/505/506): workspace + its vertical via the
// registry, run recorded BEFORE start — both mocked at the seam (no DB in units).
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: h.recordRun,
	attachRunId: h.attachRunId,
}));

// Metadata client: generation-head → run_tables → tables source ids via
// selectDistinct().from().innerJoin().innerJoin().where().
const distinctChain: Record<string, unknown> = {};
distinctChain.from = () => distinctChain;
distinctChain.innerJoin = () => distinctChain;
distinctChain.where = () => {
	h.calls.push("resolveSources");
	return Promise.resolve(h.sourceRows);
};
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { selectDistinct: vi.fn(() => distinctChain) },
}));
vi.mock("#/db/metadata/schema", () => ({
	metadataSnapshotHead: { runId: "run_id", stage: "stage" },
	runTables: { runId: "run_id", tableId: "table_id" },
	tables: { tableId: "table_id", sourceId: "source_id" },
}));
vi.mock("#/db/metadata/relationship-target", () => ({
	GENERATION_STAGE: "generation",
}));
// metadataDb is fully mocked, so the only operator replay uses just needs to be
// callable — its return flows into the ignored chain args.
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => a,
}));

// Temporal client: record the start args (after the record) + hand back a run id.
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
// The current-session resolver replay falls back to when no session_id is given.
vi.mock("#/prompts/workspace-context", () => ({
	currentSessionId: async () => h.currentSession,
}));

import { replay } from "./replay";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.calls = [];
	h.recordedRun = null;
	h.sourceRows = [{ sourceId: "src-1" }];
	h.currentSession = null;
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
	h.attachRunId.mockClear();
});

describe("replay (DAT-422, DAT-506)", () => {
	it("resolves the workspace sources, then records a FRESH run BEFORE start", async () => {
		const result = await replay({ session_id: "old-sess" });

		// Order: resolve the workspace's imported sources (generation heads), record
		// the NEW run (authoritative, no orphan), then start.
		expect(h.calls).toEqual(["resolveSources", "record", "start"]);
		// A FRESH session — not the one being replayed.
		expect(result.session_id).not.toBe("old-sess");
	});

	it("re-runs the session's source SET as a new run keyed by the new session", async () => {
		h.sourceRows = [{ sourceId: "src-1" }, { sourceId: "src-2" }];
		h.vertical = "finance";
		const result = await replay({ session_id: "old-sess" });
		const newSessionId = result.session_id;

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [
			{ workspace_id: string; sources: string[]; verticals: string[] },
		];
		// FLAT input (DAT-506): no identity envelope, no session/source id on the
		// wire — workspace_id + the resolved source SET (DAT-422) + verticals.
		expect(args[0]).toEqual({
			workspace_id: WS,
			sources: ["src-1", "src-2"],
			verticals: ["finance"],
		});
		expect(result.sources).toEqual(["src-1", "src-2"]);
		// Workflow id is keyed by the NEW run's cockpit session (DAT-422).
		expect(opts.workflowId).toBe(`addsource-${WS}-${newSessionId}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("rejects when the workspace has no imported sources (nothing to replay) — no record, no start", async () => {
		h.sourceRows = [];
		await expect(replay({ session_id: "empty-sess" })).rejects.toThrow(
			/no imported sources/,
		);
		expect(h.recordRun).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("defaults to the CURRENT session when no session_id is given (bare 'replay')", async () => {
		h.currentSession = "current-sess";
		h.sourceRows = [{ sourceId: "src-1" }];
		const result = await replay({});
		expect(h.calls).toContain("resolveSources");
		expect(h.calls).toContain("start");
		expect(result.sources).toEqual(["src-1"]);
		// It re-ran the current session into a FRESH one (replay is non-destructive).
		expect(result.session_id).not.toBe("current-sess");
	});

	it("rejects when there is no current session to replay", async () => {
		h.currentSession = null;
		await expect(replay({})).rejects.toThrow(/No session to replay/);
		expect(h.recordRun).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("throws when Temporal is unconfigured and does NOT read, record, or start", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(replay({ session_id: "old-sess" })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.calls).toEqual([]); // the guard is first — nothing ran
		expect(h.recordRun).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("records the new replay session + run before start, then attaches the runId", async () => {
		const result = await replay({ session_id: "old-sess" });
		const newSessionId = result.session_id;
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordedRun).toEqual({
			workspaceId: WS,
			engineSessionId: newSessionId,
			kind: "replay",
			stage: "add_source",
			workflowId: `addsource-${WS}-${newSessionId}`,
		});
		expect(h.attachRunId).toHaveBeenCalledWith(
			`addsource-${WS}-${newSessionId}`,
			"run-xyz",
		);
	});

	it("re-runs on the WORKSPACE vertical (from the registry) as a one-element array", async () => {
		h.vertical = "marketing";
		await replay({ session_id: "old-sess" });
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ verticals: string[] }];
		expect(args[0].verticals).toEqual(["marketing"]);
	});
});
