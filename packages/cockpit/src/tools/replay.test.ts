// Unit tests for the replay tool (DAT-343, DAT-413).
//
// Mirrors trigger-add-source.test.ts: mock `#/config`, the Drizzle metadata client
// (record the seeded investigation_sessions row + that the insert ran), and
// `@temporalio/client` (record the start call). The regression this guards: a
// FULL replay re-runs the typing phase, whose per-session rows (type_candidates,
// session_tables) FK to investigation_sessions — so replay MUST seed that parent
// row BEFORE starting, with the SAME session_id it hands the workflow. Without it
// the run dies deep in the per-table fan-out with a ForeignKeyViolation (the
// "Add source failed on replay … type_candidates violates foreign key" bug).

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		temporalTaskQueue: "dataraum-pipeline",
	} as Record<string, unknown>,
	// Records the order of side effects so we can assert seed-before-start.
	calls: [] as string[],
	seededRow: null as Record<string, unknown> | null,
	// Rows the resolveSourceVertical query returns (empty = source never framed).
	verticalRows: [] as Array<{ vertical: string }>,
}));

// Live getter (the unconfigured-guard test reassigns h.config).
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// Metadata client: record the seeded row. The seed chain is
// insert(...).values(row).onConflictDoNothing({target}) — model both links.
const onConflictMock = vi.fn(async () => {});
const valuesMock = vi.fn((row: Record<string, unknown>) => {
	h.seededRow = row;
	h.calls.push("seed");
	return { onConflictDoNothing: onConflictMock };
});
// resolveSourceVertical's read chain: select().from().innerJoin().innerJoin()
// .where().orderBy().limit() → rows. A chainable stub; limit() resolves the rows.
const selectChain: Record<string, unknown> = {};
for (const m of ["from", "innerJoin", "where", "orderBy"]) {
	selectChain[m] = () => selectChain;
}
selectChain.limit = () => {
	h.calls.push("resolveVertical");
	return Promise.resolve(h.verticalRows);
};
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		insert: vi.fn(() => ({ values: valuesMock })),
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
// metadataDb is fully mocked, so the drizzle operators only need to be callable —
// no-op stubs whose return values flow into the ignored chain args.
vi.mock("drizzle-orm", () => ({
	and: (...a: unknown[]) => a,
	desc: (x: unknown) => x,
	eq: (...a: unknown[]) => a,
	isNotNull: (x: unknown) => x,
	ne: (...a: unknown[]) => a,
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
	h.verticalRows = [];
	valuesMock.mockClear();
	onConflictMock.mockClear();
	startMock.mockClear();
	closeMock.mockClear();
});

describe("replay (DAT-413)", () => {
	it("seeds the investigation_sessions row BEFORE starting the workflow", async () => {
		await replay({ source_id: "src-1", vertical: "finance" });

		// Order is the whole point: the typing-phase FK needs the parent row to
		// exist before the run reaches it (conflict-safe so a reused id is fine).
		expect(h.calls).toEqual(["seed", "start"]);
		expect(onConflictMock).toHaveBeenCalledTimes(1);
		expect(h.seededRow?.status).toBe("active");
		expect(h.seededRow?.stepCount).toBe(0);
		expect(h.seededRow?.intent).toBe("replay");
		expect(h.seededRow?.vertical).toBe("finance");
		expect(h.seededRow?.startedAt).toBeInstanceOf(Date);
		expect(typeof h.seededRow?.sessionId).toBe("string");
	});

	it("seeds the SAME session_id it hands the workflow, and returns it (the FK match)", async () => {
		const result = await replay({ source_id: "src-1", vertical: "finance" });

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [
			{ identity: Record<string, unknown>; source_ids: string[] },
		];
		// The whole bug: the seeded session and the run's session must be identical.
		expect(args[0].identity.session_id).toBe(h.seededRow?.sessionId);
		// The source rides in the run's source SET (DAT-422), not the identity.
		expect(args[0].identity.source_id).toBeUndefined();
		expect(args[0].source_ids).toEqual(["src-1"]);
		expect(result.session_id).toBe(h.seededRow?.sessionId);
		// Workflow id is keyed by the run's session (DAT-422), not the source.
		expect(opts.workflowId).toBe(`addsource-${WS}-${h.seededRow?.sessionId}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("reuses a caller-supplied session_id (seed stays conflict-safe)", async () => {
		await replay({
			source_id: "src-1",
			session_id: "sess-reuse",
			vertical: "finance",
		});
		expect(h.seededRow?.sessionId).toBe("sess-reuse");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.session_id).toBe("sess-reuse");
		expect(onConflictMock).toHaveBeenCalledTimes(1);
	});

	it("throws when Temporal is unconfigured and does NOT seed or start", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(replay({ source_id: "src-1" })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("resolves the source's framed vertical when vertical is OMITTED", async () => {
		// The whole fix: an omitted vertical must re-run on the source's framed
		// ontology, not silently fall back to _adhoc (which fails the semantic pass).
		h.verticalRows = [{ vertical: "finance" }];
		await replay({ source_id: "src-1" }); // no vertical passed

		expect(h.calls).toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("finance");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.vertical).toBe("finance");
	});

	it("falls back to _adhoc only when the source has no framed vertical", async () => {
		h.verticalRows = []; // resolver finds nothing
		await replay({ source_id: "src-1" }); // no vertical passed

		expect(h.calls).toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("_adhoc");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.vertical).toBe("_adhoc");
	});

	it("an explicit vertical OVERRIDES resolution (no resolver query)", async () => {
		h.verticalRows = [{ vertical: "finance" }];
		await replay({ source_id: "src-1", vertical: "marketing" });

		// Explicit input short-circuits the resolver — it must not even query.
		expect(h.calls).not.toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("marketing");
	});
});
