// Unit tests for the replay tool (DAT-343, DAT-413, DAT-422, DAT-506; DAT-609).
//
// Replay takes NO input: it resolves the workspace's currently-imported sources and
// re-runs add_source over them to apply pending teaches. DAT-609: it is a DIRECT
// single-shot engine start (kind "replay") — NOT the autonomous grounding loop, since
// the user is doing teach+replay by hand. DAT-562: the run REUSES the workspace's
// `addsource-<ws>` workflow id (so a replay that resolves a parked grounding gap
// self-clears the inbox). So the unit asserts the startDirectRun spec. The source
// resolution + "nothing to replay" guard stay request-side.
//
// Mocks: `#/config`, the cockpit registry, the Drizzle metadata client
// (generation-head → source ids), and the orchestration trigger (startDirectRun).

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	// Records the order of side effects so we can assert resolve-then-start.
	calls: [] as string[],
	started: null as Record<string, unknown> | null,
	// Rows the generation-head → run_tables → tables source query returns
	// (empty = nothing imported = nothing to replay).
	sourceRows: [] as Array<{ sourceId: string | null }>,
	startDirectRun: vi.fn(async (spec: Record<string, unknown>) => {
		h.started = spec;
		h.calls.push("start");
	}),
}));

// Live getter (the unconfigured-guard test reassigns h.config).
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
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

vi.mock("#/temporal/orchestration-trigger", () => ({
	startDirectRun: h.startDirectRun,
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
	h.started = null;
	h.sourceRows = [{ sourceId: "src-1" }];
	h.startDirectRun.mockClear();
});

describe("replay (DAT-422, direct single-shot — DAT-609, DAT-562)", () => {
	it("resolves the workspace sources, then starts the engine run", async () => {
		const result = await replay({});

		// Order: resolve the workspace's imported sources (generation heads), then
		// start the direct add_source run.
		expect(h.calls).toEqual(["resolveSources", "start"]);
		expect(result.sources).toEqual(["src-1"]);
	});

	it("starts a direct add_source run with the resolved source SET + kind replay + verticals", async () => {
		h.sourceRows = [{ sourceId: "src-1" }, { sourceId: "src-2" }];
		h.vertical = "finance";
		const result = await replay({});

		// One workflow id per workspace (DAT-562) — reused across imports/replays so a
		// replay that resolves a parked grounding gap self-clears the inbox.
		expect(h.started).toMatchObject({
			workspaceId: WS,
			kind: "replay",
			stage: "add_source",
			workflowType: "addSourceWorkflow",
			workflowId: `addsource-${WS}`,
			taskQueue: `engine-${WS}`,
			args: [
				{
					workspace_id: WS,
					sources: ["src-1", "src-2"],
					verticals: ["finance"],
				},
			],
		});
		expect(typeof h.started?.busyMessage).toBe("string");
		expect(result).toEqual({
			workflow_id: `addsource-${WS}`,
			run_id: `addsource-${WS}`,
			sources: ["src-1", "src-2"],
		});
	});

	it("rejects when the workspace has no imported sources (nothing to replay) — no start", async () => {
		h.sourceRows = [];
		await expect(replay({})).rejects.toThrow(/no imported sources/);
		expect(h.startDirectRun).not.toHaveBeenCalled();
	});

	it("throws when Temporal is unconfigured and does NOT read or start", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(replay({})).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.calls).toEqual([]); // the guard is first — nothing ran
		expect(h.startDirectRun).not.toHaveBeenCalled();
	});

	it("re-runs on the WORKSPACE vertical (from the registry) as a one-element array", async () => {
		h.vertical = "marketing";
		await replay({});
		expect(
			(h.started?.args as Array<{ verticals: string[] }>)[0].verticals,
		).toEqual(["marketing"]);
	});
});
