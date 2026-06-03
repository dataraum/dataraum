// Unit tests for the add_source TRIGGER (DAT-352).
//
// Three mocked seams (units project — no DB, no Temporal): `#/config` (workspace
// id + Temporal config), the Drizzle metadata client (records the seeded
// investigation_sessions row), and `@temporalio/client` (records the start call
// + returns a handle). We assert: (1) the session is seeded BEFORE the workflow
// starts, (2) the start uses the right workflow id / queue / args (non-blocking),
// and (3) it throws when Temporal is unconfigured, like replay.ts.

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
	// Active concept-overlay count the pre-flight guard reads. Default > 0 so the
	// happy-path tests pass the guard; the guard test flips it to 0.
	conceptCount: 1,
}));

// A live getter, not a snapshot: the unconfigured-guard test reassigns
// `h.config`, so the module's `config` import must read the CURRENT object.
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

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
// Pre-flight concept count (Theme B / obs 4) — records that the guard ran and
// returns the configured count. Records "count" in the call order so the guard
// test can assert it ran BEFORE any seed/start.
const countMock = vi.fn(async (_vertical: string) => {
	h.calls.push("count");
	return h.conceptCount;
});
// Reference countMock LAZILY (through a wrapper) — a direct reference in the
// factory return is evaluated at import time, before the `const` initializes
// (the seed/start mocks dodge this by referencing their fns inside inner fns).
vi.mock("#/db/metadata/concept-overlays", () => ({
	countActiveConcepts: (vertical: string) => countMock(vertical),
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
	h.conceptCount = 1;
	valuesMock.mockClear();
	startMock.mockClear();
	closeMock.mockClear();
	countMock.mockClear();
});

describe("triggerAddSource (DAT-352)", () => {
	it("seeds the investigation_sessions row BEFORE starting the workflow", async () => {
		await triggerAddSource({ source_id: "src-1" });

		// Order is the whole point: the pre-flight concept check runs first (no
		// orphan session on failure), then the typing-phase FK needs the parent
		// row to exist before the run reaches it.
		expect(h.calls).toEqual(["count", "seed", "start"]);

		expect(h.seededRow?.status).toBe("active");
		expect(h.seededRow?.stepCount).toBe(0);
		expect(h.seededRow?.intent).toBe("onboarding");
		expect(h.seededRow?.vertical).toBe("_adhoc");
		expect(h.seededRow?.startedAt).toBeInstanceOf(Date);
		expect(typeof h.seededRow?.sessionId).toBe("string");
	});

	it("starts addSourceWorkflow with the right id / queue / args (non-blocking)", async () => {
		const result = await triggerAddSource({ source_id: "src-1" });

		expect(startMock).toHaveBeenCalledTimes(1);
		const [name, opts] = startMock.mock.calls[0] as [
			string,
			Record<string, unknown>,
		];
		expect(name).toBe("addSourceWorkflow");
		expect(opts.workflowId).toBe(`addsource-${WS}-src-1`);
		expect(opts.taskQueue).toBe("dataraum-pipeline");
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");

		// The args carry the SAME session_id that was seeded — the FK match.
		const args = opts.args as [{ identity: Record<string, unknown> }];
		const identity = args[0].identity;
		expect(identity.workspace_id).toBe(WS);
		expect(identity.source_id).toBe("src-1");
		expect(identity.session_id).toBe(h.seededRow?.sessionId);
		expect(identity.vertical).toBe("_adhoc");

		// Returns the workflow + run id immediately (no replay scope on the input).
		expect(args[0]).not.toHaveProperty("replay");
		expect(result).toEqual({
			workflow_id: `addsource-${WS}-src-1`,
			run_id: "run-abc",
			source_id: "src-1",
			session_id: h.seededRow?.sessionId,
		});
		// Connection is always closed.
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("passes an explicit vertical through to the seed + the workflow identity", async () => {
		await triggerAddSource({
			source_id: "src-2",
			vertical: "financial_reporting",
		});
		expect(h.seededRow?.vertical).toBe("financial_reporting");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.vertical).toBe("financial_reporting");
		// A built-in vertical (concepts ship on disk) is EXEMPT from the pre-flight
		// concept check — the count is overlay-backed only.
		expect(countMock).not.toHaveBeenCalled();
	});

	it("refuses an overlay-backed vertical with zero declared concepts (no seed, no start)", async () => {
		// Pre-flight (obs 4): _adhoc grounds only against frame-written concept
		// overlay rows. With none, the engine would fail loud deep in
		// semantic_per_column — so reject here with a readable "run frame" message
		// and never seed a session or start a doomed workflow.
		h.conceptCount = 0;
		await expect(triggerAddSource({ source_id: "src-1" })).rejects.toThrow(
			NoConceptsError,
		);
		expect(h.calls).toEqual(["count"]);
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("throws when Temporal is unconfigured (like replay.ts) and does NOT seed", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(triggerAddSource({ source_id: "src-1" })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		// The guard runs first — no orphan session row, no workflow start.
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});
});
