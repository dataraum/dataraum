// Unit tests for the add_source TRIGGER (DAT-352; folded into the select
// call by DAT-436 — `select.server` is the only caller, and it
// pre-flights the vertical BEFORE any write, so the trigger itself no longer
// re-checks concepts).
//
// Two mocked seams (units project — no DB, no Temporal): the Drizzle metadata
// client (records the seeded investigation_sessions row) and
// `@temporalio/client` (records the start call + returns a handle). We assert:
// (1) the session is seeded BEFORE the workflow starts, (2) the start uses the
// right workflow id / queue / args (non-blocking), and (3) it throws when
// Temporal is unconfigured, like replay.ts.

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
		temporalTaskQueue: "dataraum-pipeline",
	} as Record<string, unknown>,
	// Records the order of side effects so we can assert seed-before-start.
	calls: [] as string[],
	startArgs: null as unknown,
	seededRow: null as Record<string, unknown> | null,
	recordRun: vi.fn(async () => {}),
}));

// A live getter, not a snapshot: the unconfigured-guard test reassigns
// `h.config`, so the module's `config` import must read the CURRENT object.
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// cockpit_db control plane (DAT-461): workspace via the registry, run recorded
// after start — both mocked at the seam (no DB in units).
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		// Per-workspace queue (DAT-505) — the driver routes the workflow here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: "_adhoc",
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({ recordRun: h.recordRun }));

// Metadata client: record the seeded row + that the insert ran (and when).
const valuesMock = vi.fn((row: Record<string, unknown>) => {
	h.seededRow = row;
	h.calls.push("seed");
});
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: valuesMock })) },
}));
vi.mock("#/db/metadata/schema", () => ({
	investigationSessions: { name: "investigation_sessions" },
}));

// Temporal client: record the start args (after the seed) + hand back a run id.
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

import { NoConceptsError, triggerAddSource } from "./trigger-add-source";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		temporalTaskQueue: "dataraum-pipeline",
	};
	h.calls = [];
	h.startArgs = null;
	h.seededRow = null;
	valuesMock.mockClear();
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
});

describe("triggerAddSource (DAT-352, one-call DAT-436)", () => {
	it("seeds the investigation_sessions row BEFORE starting the workflow", async () => {
		await triggerAddSource({ source_ids: ["src-1"] });

		// Order is the whole point: the typing-phase FK needs the parent row to
		// exist before the run reaches it. (The vertical pre-flight runs in
		// `select`, before its source writes — not here.)
		expect(h.calls).toEqual(["seed", "start"]);

		expect(h.seededRow?.status).toBe("active");
		expect(h.seededRow?.stepCount).toBe(0);
		expect(h.seededRow?.intent).toBe("onboarding");
		expect(h.seededRow?.vertical).toBe("_adhoc");
		expect(h.seededRow?.startedAt).toBeInstanceOf(Date);
		expect(typeof h.seededRow?.sessionId).toBe("string");
	});

	it("starts addSourceWorkflow with the right id / queue / args (non-blocking)", async () => {
		const result = await triggerAddSource({ source_ids: ["src-1"] });
		const sessionId = h.seededRow?.sessionId as string;

		expect(startMock).toHaveBeenCalledTimes(1);
		const [name, opts] = startMock.mock.calls[0] as [
			string,
			Record<string, unknown>,
		];
		expect(name).toBe("addSourceWorkflow");
		// Workflow id is keyed by the run's session (DAT-422), not a source.
		expect(opts.workflowId).toBe(`addsource-${WS}-${sessionId}`);
		// Routed to the workspace's OWN queue (DAT-505), not the bare env queue.
		expect(opts.taskQueue).toBe(`engine-${WS}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");

		// The args carry the source SET (DAT-422) + a source-free identity with the
		// SAME session_id that was seeded — the FK match.
		const args = opts.args as [
			{ identity: Record<string, unknown>; source_ids: string[] },
		];
		const identity = args[0].identity;
		expect(identity.workspace_id).toBe(WS);
		expect(identity.source_id).toBeUndefined();
		expect(identity.session_id).toBe(sessionId);
		expect(identity.vertical).toBe("_adhoc");
		expect(args[0].source_ids).toEqual(["src-1"]);

		// Returns the workflow + run id immediately (no replay scope on the input).
		expect(args[0]).not.toHaveProperty("replay");
		expect(result).toEqual({
			workflow_id: `addsource-${WS}-${sessionId}`,
			run_id: "run-abc",
			source_ids: ["src-1"],
			session_id: sessionId,
		});
		// Connection is always closed.
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("passes an explicit vertical through to the seed + the workflow identity", async () => {
		await triggerAddSource({
			source_ids: ["src-2"],
			vertical: "financial_reporting",
		});
		expect(h.seededRow?.vertical).toBe("financial_reporting");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.vertical).toBe("financial_reporting");
	});

	it("throws when Temporal is unconfigured (like replay.ts) and does NOT seed", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(triggerAddSource({ source_ids: ["src-1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		// The guard runs first — no orphan session row, no workflow start.
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
		expect(h.recordRun).not.toHaveBeenCalled();
	});

	it("records the cockpit session + run after starting (DAT-461)", async () => {
		await triggerAddSource({ source_ids: ["src-1"] });
		const sessionId = h.seededRow?.sessionId as string;
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: WS,
			engineSessionId: sessionId,
			kind: "onboarding",
			stage: "add_source",
			workflowId: `addsource-${WS}-${sessionId}`,
			runId: "run-abc",
		});
	});
});

describe("NoConceptsError (raised by select's pre-flight)", () => {
	it("carries a user-fixable 'run frame' message per vertical", () => {
		expect(new NoConceptsError("_adhoc").message).toMatch(
			/No concepts declared yet/,
		);
		expect(new NoConceptsError("sales").message).toMatch(
			/"sales" vertical has no concepts/,
		);
	});
});
