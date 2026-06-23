// Unit tests for the orchestration trigger seam (DAT-609). The singleton journey is
// gone: each trigger `start`s a workflow by its deterministic per-workspace id with
// the single-flight reuse/conflict policy. Mock #/config, @temporalio/client, and the
// cockpit_db run writers at the seam (no Temporal / no DB in units).

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
			cockpitOrchestrationTaskQueue: "cockpit-orchestration",
		} as Record<string, unknown>,
		start: vi.fn(async () => ({ firstExecutionRunId: "exec-1" })),
		close: vi.fn(async () => {}),
		recordRun: vi.fn(async () => {}),
		attachRunId: vi.fn(async () => {}),
		markRunStatus: vi.fn(async () => {}),
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
	attachRunId: h.attachRunId,
	markRunStatus: h.markRunStatus,
}));

import {
	RunAlreadyRunningError,
	startDirectRun,
	startGroundingLoop,
	startSessionCascade,
} from "./orchestration-trigger";

beforeEach(() => {
	h.config = {
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		cockpitOrchestrationTaskQueue: "cockpit-orchestration",
	};
	h.start.mockClear();
	h.start.mockResolvedValue({ firstExecutionRunId: "exec-1" });
	h.close.mockClear();
	h.recordRun.mockClear();
	h.attachRunId.mockClear();
	h.markRunStatus.mockClear();
});

const SINGLE_FLIGHT = {
	workflowIdReusePolicy: "ALLOW_DUPLICATE",
	workflowIdConflictPolicy: "FAIL",
};

describe("startGroundingLoop (DAT-609)", () => {
	const input = {
		workspaceId: "ws-1",
		workflowId: "addsource-ws-1",
		engineTaskQueue: "engine-ws-1",
		sources: ["src-a"],
		verticals: ["finance"],
		conversationId: "conv-1",
	};

	it("starts groundingLoopWorkflow under the per-ws id with single-flight policy", async () => {
		await startGroundingLoop(input);
		expect(h.start).toHaveBeenCalledWith(
			"groundingLoopWorkflow",
			expect.objectContaining({
				taskQueue: "cockpit-orchestration",
				workflowId: "grounding-ws-1",
				args: [input],
				...SINGLE_FLIGHT,
			}),
		);
		expect(h.close).toHaveBeenCalled();
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

describe("startSessionCascade (DAT-609)", () => {
	const input = {
		workspaceId: "ws-1",
		workflowId: "beginsession-ws-1",
		engineTaskQueue: "engine-ws-1",
		tables: ["t1", "t2"],
		verticals: ["finance"],
		conversationId: "conv-1",
	};

	it("starts sessionCascadeWorkflow under the per-ws id with single-flight policy", async () => {
		await startSessionCascade(input);
		expect(h.start).toHaveBeenCalledWith(
			"sessionCascadeWorkflow",
			expect.objectContaining({
				taskQueue: "cockpit-orchestration",
				workflowId: "session-ws-1",
				args: [input],
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

	it("records the run BEFORE start, then starts the engine workflow + attaches the real id", async () => {
		await startDirectRun(spec);
		// Authoritative record before start (conversationId omitted ⇒ recordRun's ALS
		// fallback) — the engine workflow id + queue, not an orchestration id.
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: "ws-1",
			kind: "replay",
			stage: "add_source",
			workflowId: "addsource-ws-1",
		});
		expect(h.start).toHaveBeenCalledWith(
			"addSourceWorkflow",
			expect.objectContaining({
				taskQueue: "engine-ws-1",
				workflowId: "addsource-ws-1",
				args: spec.args,
				...SINGLE_FLIGHT,
			}),
		);
		expect(h.attachRunId).toHaveBeenCalledWith("addsource-ws-1", "exec-1");
		// recordRun happened before start.
		expect(h.recordRun.mock.invocationCallOrder[0]).toBeLessThan(
			h.start.mock.invocationCallOrder[0],
		);
	});

	it("marks the placeholder failed + translates a conflict to an actionable error", async () => {
		h.start.mockRejectedValueOnce(new h.AlreadyStarted());
		const err = await startDirectRun(spec).catch((e) => e);
		expect(err).toBeInstanceOf(RunAlreadyRunningError);
		// The recorded placeholder row (runId === workflowId) is dropped out of running.
		expect(h.markRunStatus).toHaveBeenCalledWith(
			"addsource-ws-1",
			"addsource-ws-1",
			"failed",
		);
		expect(h.attachRunId).not.toHaveBeenCalled();
	});

	it("fails loud when Temporal isn't configured (after recording the run)", async () => {
		h.config = {};
		await expect(startDirectRun(spec)).rejects.toThrow(/not configured/);
		expect(h.start).not.toHaveBeenCalled();
	});
});
