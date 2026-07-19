// Unit tests for the orchestration trigger seam (DAT-609). The singleton journey is
// gone: each trigger `start`s a workflow by its deterministic per-workspace id with
// the single-flight reuse/conflict policy. Mock #/config, #/otel, @temporalio/client,
// and the cockpit_db run writers at the seam (no Temporal / no DB in units). The
// triggers go through the process-shared client (temporal/client.ts, DAT-705), so
// each case resets its cache to re-exercise the config guard.

import { beforeEach, describe, expect, it, vi } from "vitest";

import { AgentActionableError } from "#/tools/agent-error";

const h = vi.hoisted(() => {
	// A stand-in for the SDK's WorkflowExecutionAlreadyStartedError — the module under
	// test does `err instanceof WorkflowExecutionAlreadyStartedError` against THIS class
	// (the mock exports it), so rejecting `start` with it exercises the conflict
	// translation. Defined inside vi.hoisted so the hoisted vi.mock factory can use it.
	class AlreadyStarted extends Error {}
	return {
		AlreadyStarted,
		config: {
			temporalHost: "localhost:7233",
			temporalNamespace: "default",
		} as Record<string, unknown>,
		start: vi.fn(async () => ({ firstExecutionRunId: "exec-1" })),
		close: vi.fn(async () => {}),
		recordRun: vi.fn(async () => {}),
	};
});

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));
vi.mock("@temporalio/client", () => ({
	Connection: { connect: vi.fn(async () => ({ close: h.close })) },
	Client: vi.fn(function Client() {
		return { workflow: { start: h.start } };
	}),
	WorkflowExecutionAlreadyStartedError: h.AlreadyStarted,
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: h.recordRun,
}));
// getOtel: null = telemetry off — the client factory constructs no interceptor
// and the OTel SDK never loads in units.
vi.mock("#/otel", () => ({ getOtel: () => null }));

import { resetTemporalClient } from "./client";
import {
	RunAlreadyRunningError,
	startDirectRun,
	startGroundingLoop,
	startSessionCascade,
} from "./orchestration-trigger";

beforeEach(() => {
	// The shared client is process-cached; drop it so the unconfigured-guard
	// cases re-check config instead of reusing a client built under a prior
	// case's configured mock.
	resetTemporalClient();
	h.config = {
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.start.mockClear();
	h.start.mockResolvedValue({ firstExecutionRunId: "exec-1" });
	h.close.mockClear();
	h.recordRun.mockClear();
});

const SINGLE_FLIGHT = {
	workflowIdReusePolicy: "ALLOW_DUPLICATE",
	workflowIdConflictPolicy: "FAIL",
};

describe("startGroundingLoop (DAT-609/708)", () => {
	const input = {
		workspaceId: "ws-1",
		workflowId: "addsource-ws-1",
		engineTaskQueue: "engine-ws-1",
		sources: ["src-a"],
		verticals: ["finance"],
		conversationId: "conv-1",
	};

	it("starts groundingLoopWorkflow on the ENGINE queue with the snake_case wire payload", async () => {
		await startGroundingLoop(input);
		// DAT-708: the workflow runs on the engine worker → started on the
		// workspace's engine queue; the wire payload is the engine-owned mirror
		// (snake_case). No cockpit queue rides it (DAT-818) — the workflow
		// derives `cockpit-<ws>` from workspace_id.
		expect(h.start).toHaveBeenCalledWith(
			"groundingLoopWorkflow",
			expect.objectContaining({
				taskQueue: "engine-ws-1",
				workflowId: "grounding-ws-1",
				args: [
					{
						workspace_id: "ws-1",
						workflow_id: "addsource-ws-1",
						sources: ["src-a"],
						verticals: ["finance"],
						conversation_id: "conv-1",
					},
				],
				...SINGLE_FLIGHT,
			}),
		);
		// The shared client's connection stays OPEN (temporal/client.ts, DAT-705)
		// — the per-call open/close churn went with withClient().
		expect(h.close).not.toHaveBeenCalled();
	});

	it("translates an already-running conflict into an actionable error", async () => {
		h.start.mockRejectedValueOnce(new h.AlreadyStarted());
		const err = await startGroundingLoop(input).catch((e) => e);
		expect(err).toBeInstanceOf(RunAlreadyRunningError);
		expect(err).toBeInstanceOf(AgentActionableError);
		expect((err as Error).message).toMatch(/already running/i);
	});

	it("fails loud when Temporal isn't configured", async () => {
		h.config = {};
		await expect(startGroundingLoop(input)).rejects.toThrow(/not configured/);
		expect(h.start).not.toHaveBeenCalled();
	});
});

describe("startSessionCascade (DAT-609/708)", () => {
	const input = {
		workspaceId: "ws-1",
		workflowId: "beginsession-ws-1",
		engineTaskQueue: "engine-ws-1",
		tables: ["t1", "t2"],
		verticals: ["finance"],
		conversationId: "conv-1",
	};

	it("starts sessionCascadeWorkflow on the ENGINE queue with the snake_case wire payload", async () => {
		await startSessionCascade(input);
		expect(h.start).toHaveBeenCalledWith(
			"sessionCascadeWorkflow",
			expect.objectContaining({
				taskQueue: "engine-ws-1",
				workflowId: "session-ws-1",
				args: [
					{
						workspace_id: "ws-1",
						workflow_id: "beginsession-ws-1",
						tables: ["t1", "t2"],
						verticals: ["finance"],
						conversation_id: "conv-1",
					},
				],
				...SINGLE_FLIGHT,
			}),
		);
	});

	it("translates an already-running conflict into an actionable error", async () => {
		h.start.mockRejectedValueOnce(new h.AlreadyStarted());
		const err = await startSessionCascade(input).catch((e) => e);
		expect(err).toBeInstanceOf(RunAlreadyRunningError);
	});
});

describe("startDirectRun (DAT-609 — replay / manual operating_model)", () => {
	const spec = {
		workspaceId: "ws-1",
		kind: "replay" as const,
		stage: "add_source" as const,
		workflowType: "addSourceWorkflow",
		workflowId: "addsource-ws-1",
		taskQueue: "engine-ws-1",
		args: [
			{ workspace_id: "ws-1", sources: ["src-a"], verticals: ["finance"] },
		],
		busyMessage: "already running",
	};

	it("starts the engine workflow, then records the run with the REAL execution id (DAT-595)", async () => {
		await startDirectRun(spec);
		expect(h.start).toHaveBeenCalledWith(
			"addSourceWorkflow",
			expect.objectContaining({
				taskQueue: "engine-ws-1",
				workflowId: "addsource-ws-1",
				args: spec.args,
				...SINGLE_FLIGHT,
			}),
		);
		// Recorded AFTER start with the child's real runId (the start handle's
		// firstExecutionRunId = "exec-1") — no workflowId placeholder, no attachRunId.
		// conversationId omitted ⇒ recordRun's request-ALS fallback.
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: "ws-1",
			kind: "replay",
			stage: "add_source",
			workflowId: "addsource-ws-1",
			runId: "exec-1",
		});
		// start happened before recordRun (we need the real runId first).
		expect(h.start.mock.invocationCallOrder[0]).toBeLessThan(
			h.recordRun.mock.invocationCallOrder[0],
		);
	});

	it("translates a conflict to an actionable error and records NOTHING (run never started)", async () => {
		h.start.mockRejectedValueOnce(new h.AlreadyStarted());
		const err = await startDirectRun(spec).catch((e) => e);
		expect(err).toBeInstanceOf(RunAlreadyRunningError);
		// Recording is AFTER start, so a rejected start leaves no row to clean up.
		expect(h.recordRun).not.toHaveBeenCalled();
	});

	it("fails loud when Temporal isn't configured — BEFORE recording the run", async () => {
		h.config = {};
		await expect(startDirectRun(spec)).rejects.toThrow(/not configured/);
		// The config guard runs first, so recording happens only after a real start.
		expect(h.recordRun).not.toHaveBeenCalled();
		expect(h.start).not.toHaveBeenCalled();
	});
});
