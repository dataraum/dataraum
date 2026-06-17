// Unit tests for the JourneyWorkflow activity (DAT-529). The activity reuses the
// cockpit control-plane driver in-process; mock it at the seam (no DB in units).

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	recordRun: vi.fn(async (_input: { workflowId: string }) => {}),
	markRunStatus: vi.fn(
		async (_workflowId: string, _runId: string, _status: string) => {},
	),
}));

vi.mock("#/db/cockpit/runs", () => ({
	recordRun: h.recordRun,
	markRunStatus: h.markRunStatus,
}));

import { startStage } from "./activities";

beforeEach(() => {
	h.recordRun.mockClear();
	h.markRunStatus.mockClear();
});

describe("startStage activity (DAT-529)", () => {
	it("records a run then marks it completed, keyed by the stage-run id", async () => {
		await startStage("ws-1", "finance", "journey-ws-1-0");

		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: "ws-1",
			engineSessionId: "journey-stage-journey-ws-1-0",
			kind: "onboarding",
			stage: "add_source",
			workflowId: "journey-stage-journey-ws-1-0",
		});
		expect(h.markRunStatus).toHaveBeenCalledWith(
			"journey-stage-journey-ws-1-0",
			"journey-stage-journey-ws-1-0",
			"completed",
		);
	});

	it("derives a stable id from the stage-run id, so a retry upserts (no duplicate)", async () => {
		await startStage("ws-1", "finance", "journey-ws-1-3");
		await startStage("ws-1", "finance", "journey-ws-1-3");
		const ids = h.recordRun.mock.calls.map((c) => c[0].workflowId);
		expect(new Set(ids).size).toBe(1);
	});

	it("records BEFORE marking complete (the control-plane ordering)", async () => {
		const order: string[] = [];
		h.recordRun.mockImplementationOnce(async () => {
			order.push("record");
		});
		h.markRunStatus.mockImplementationOnce(async () => {
			order.push("mark");
		});
		await startStage("ws-1", "finance", "journey-ws-1-0");
		expect(order).toEqual(["record", "mark"]);
	});
});
