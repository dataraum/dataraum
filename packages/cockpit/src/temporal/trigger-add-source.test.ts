// Unit tests for the add_source TRIGGER (DAT-352; folded into the select call by
// DAT-436 — `select.server` is the only caller. DAT-506: no engine seed; the run
// is recorded in cockpit_db BEFORE the workflow starts, and the vertical is the
// workspace property from the registry, not a trigger input).
//
// Mocked seams (units project — no DB, no Temporal): the cockpit registry (the
// workspace + its vertical), the cockpit runs writer (recordRun/attachRunId), and
// `@temporalio/client` (records the start call + returns a handle). We assert:
// (1) recordRun runs BEFORE the workflow starts (an unrecorded run is orphaned),
// (2) the start uses the right workflow id / queue / args with the workspace
// vertical on the INPUT (non-blocking), and (3) it throws when Temporal is
// unconfigured, recording nothing.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

// Mutable config the tests flip to exercise the unconfigured guard. The default
// config is inlined here (NOT via `WS`): `vi.hoisted` runs before module-level
// `const WS`, so referencing it would hit the TDZ.
const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	// Records the order of side effects so we can assert record-before-start.
	calls: [] as string[],
	startArgs: null as unknown,
	recordRun: vi.fn(async () => {
		h.calls.push("record");
	}),
	attachRunId: vi.fn(async () => {}),
}));

// A live getter, not a snapshot: the unconfigured-guard test reassigns
// `h.config`, so the module's `config` import must read the CURRENT object.
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
		// Per-workspace queue (DAT-505) — the driver routes the workflow here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: h.recordRun,
	attachRunId: h.attachRunId,
}));

// Temporal client: record the start args (after the record) + hand back a run id.
const startMock = vi.fn(async (_name: string, opts: unknown) => {
	h.calls.push("start");
	h.startArgs = opts;
	return { firstExecutionRunId: "run-abc" };
});
const closeMock = vi.fn(async () => {});
vi.mock("@temporalio/client", () => ({
	Connection: { connect: vi.fn(async () => ({ close: closeMock })) },
	// Must be `new`-able — a regular function so `new Client(...)` works.
	Client: vi.fn(function Client() {
		return { workflow: { start: startMock } };
	}),
}));
vi.mock("@temporalio/common", () => ({
	WorkflowIdReusePolicy: { ALLOW_DUPLICATE: "ALLOW_DUPLICATE" },
}));

import { triggerAddSource } from "./trigger-add-source";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.calls = [];
	h.startArgs = null;
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
	h.attachRunId.mockClear();
});

describe("triggerAddSource (DAT-352, one-call DAT-436, DAT-506)", () => {
	it("records the run BEFORE starting the workflow (no orphaned run)", async () => {
		await triggerAddSource({ sources: ["src-1"] });
		// Order is the whole point (Q4): an unrecorded run is orphaned, so the
		// cockpit_db record is authoritative and precedes the start.
		expect(h.calls).toEqual(["record", "start"]);
	});

	it("starts addSourceWorkflow with the right id / queue / FLAT args (non-blocking)", async () => {
		const result = await triggerAddSource({ sources: ["src-1"] });
		const cockpitSessionId = result.cockpit_session_id;

		expect(startMock).toHaveBeenCalledTimes(1);
		const [name, opts] = startMock.mock.calls[0] as [
			string,
			Record<string, unknown>,
		];
		expect(name).toBe("addSourceWorkflow");
		// Workflow id is keyed by the cockpit session id (DAT-422), not a source.
		expect(opts.workflowId).toBe(`addsource-${WS}-${cockpitSessionId}`);
		// Routed to the workspace's OWN queue (DAT-505), not the bare env queue.
		expect(opts.taskQueue).toBe(`engine-${WS}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");

		// FLAT input (DAT-506): no identity envelope — workspace_id + the source SET
		// + verticals (one-element array) at the top level, nothing else.
		const args = opts.args as [
			{ workspace_id: string; sources: string[]; verticals: string[] },
		];
		expect(args[0]).toEqual({
			workspace_id: WS,
			sources: ["src-1"],
			verticals: ["_adhoc"],
		});

		expect(result).toEqual({
			workflow_id: `addsource-${WS}-${cockpitSessionId}`,
			run_id: "run-abc",
			sources: ["src-1"],
			cockpit_session_id: cockpitSessionId,
		});
		// Connection is always closed.
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("threads the WORKSPACE vertical (from the registry) onto the input as a one-element array", async () => {
		h.vertical = "financial_reporting";
		await triggerAddSource({ sources: ["src-2"] });
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ verticals: string[] }];
		expect(args[0].verticals).toEqual(["financial_reporting"]);
	});

	it("records the cockpit session + run before start, then attaches the runId", async () => {
		const result = await triggerAddSource({ sources: ["src-1"] });
		const cockpitSessionId = result.cockpit_session_id;
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: WS,
			engineSessionId: cockpitSessionId,
			kind: "onboarding",
			stage: "add_source",
			workflowId: `addsource-${WS}-${cockpitSessionId}`,
		});
		expect(h.attachRunId).toHaveBeenCalledWith(
			`addsource-${WS}-${cockpitSessionId}`,
			"run-abc",
		);
	});

	it("throws when Temporal is unconfigured and records nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(triggerAddSource({ sources: ["src-1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		// The guard runs first — no recorded run, no workflow start.
		expect(h.recordRun).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});
});
