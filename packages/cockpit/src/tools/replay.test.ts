// Unit tests for the replay tool (DAT-343, DAT-413, DAT-422, DAT-506; routed
// through the journey in DAT-551; session retired in DAT-562).
//
// Replay takes NO input: it resolves the workspace's currently-imported sources and
// re-runs add_source over them to apply pending teaches. DAT-551: it signals the
// per-workspace JourneyWorkflow (`runAddSource`, kind "replay"), which records the
// run + starts the engine child. DAT-562: the run REUSES the workspace's
// `addsource-<ws>` workflow id (so a replay that resolves a parked grounding gap
// self-clears the inbox). So the unit asserts the SIGNAL payload. The source
// resolution + "nothing to replay" guard stay request-side.
//
// Mocks: `#/config`, the cockpit registry, the Drizzle metadata client
// (generation-head → source ids), the journey trigger (signalRunAddSource), and the
// ALS conversation context.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	conversationId: "conv-1" as string | null,
	// Records the order of side effects so we can assert resolve-then-signal.
	calls: [] as string[],
	signalled: null as {
		workspaceId: string;
		req: Record<string, unknown>;
	} | null,
	// Rows the generation-head → run_tables → tables source query returns
	// (empty = nothing imported = nothing to replay).
	sourceRows: [] as Array<{ sourceId: string | null }>,
	signalRunAddSource: vi.fn(
		async (workspaceId: string, req: Record<string, unknown>) => {
			h.signalled = { workspaceId, req };
			h.calls.push("signal");
			return req.workflowId as string;
		},
	),
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

vi.mock("#/temporal/journey-trigger", () => ({
	signalRunAddSource: h.signalRunAddSource,
}));
vi.mock("#/lib/run-context", () => ({
	currentConversationId: () => h.conversationId,
}));

import { replay } from "./replay";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.conversationId = "conv-1";
	h.calls = [];
	h.signalled = null;
	h.sourceRows = [{ sourceId: "src-1" }];
	h.signalRunAddSource.mockClear();
});

describe("replay (DAT-422, routed via the journey — DAT-551, DAT-562)", () => {
	it("resolves the workspace sources, then signals the journey", async () => {
		const result = await replay({});

		// Order: resolve the workspace's imported sources (generation heads), then
		// signal the journey to run add_source.
		expect(h.calls).toEqual(["resolveSources", "signal"]);
		expect(result.sources).toEqual(["src-1"]);
	});

	it("signals the journey with the resolved source SET + kind replay + verticals", async () => {
		h.sourceRows = [{ sourceId: "src-1" }, { sourceId: "src-2" }];
		h.vertical = "finance";
		const result = await replay({});

		// One workflow id per workspace (DAT-562) — reused across imports/replays so a
		// replay that resolves a parked grounding gap self-clears the inbox.
		expect(h.signalled?.workspaceId).toBe(WS);
		expect(h.signalled?.req).toEqual({
			workflowId: `addsource-${WS}`,
			engineTaskQueue: `engine-${WS}`,
			sources: ["src-1", "src-2"],
			verticals: ["finance"],
			kind: "replay",
			conversationId: "conv-1",
		});
		expect(result).toEqual({
			workflow_id: `addsource-${WS}`,
			run_id: `addsource-${WS}`,
			sources: ["src-1", "src-2"],
		});
	});

	it("rejects when the workspace has no imported sources (nothing to replay) — no signal", async () => {
		h.sourceRows = [];
		await expect(replay({})).rejects.toThrow(/no imported sources/);
		expect(h.signalRunAddSource).not.toHaveBeenCalled();
	});

	it("throws when Temporal is unconfigured and does NOT read or signal", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(replay({})).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.calls).toEqual([]); // the guard is first — nothing ran
		expect(h.signalRunAddSource).not.toHaveBeenCalled();
	});

	it("threads a NULL conversationId when outside a chat turn", async () => {
		h.conversationId = null;
		await replay({});
		expect(h.signalled?.req.conversationId).toBeNull();
	});

	it("re-runs on the WORKSPACE vertical (from the registry) as a one-element array", async () => {
		h.vertical = "marketing";
		await replay({});
		expect(h.signalled?.req.verticals).toEqual(["marketing"]);
	});
});
