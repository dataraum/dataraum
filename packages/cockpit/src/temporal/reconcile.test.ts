// Unit tests for the reload reconcile (DAT-462). Mocks both boundaries — the
// cockpit_db run source and the Temporal progress query — and asserts the
// orchestration: list in-flight runs, query each, mark the DONE ones terminal,
// leave the still-running ones, and stay best-effort (a per-run error or a
// listing failure never throws).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("#/db/cockpit/runs", () => ({
	listNonTerminalRuns: vi.fn(),
	markRunStatus: vi.fn(),
	attachEngineRunId: vi.fn(),
}));
vi.mock("#/temporal/progress", () => ({
	getWorkflowProgress: vi.fn(),
	getEngineRunId: vi.fn(),
	// Echo a deterministic classification off the fed progress shape — the real
	// terminalRunStatus is exercised by the progress poll + smoke.
	terminalRunStatus: (p: { status?: string }) =>
		p.status === "FAILED" ? "failed" : "completed",
}));

import {
	attachEngineRunId,
	listNonTerminalRuns,
	markRunStatus,
} from "#/db/cockpit/runs";
import { getEngineRunId, getWorkflowProgress } from "#/temporal/progress";
import { reconcileActiveRuns } from "./reconcile";

const list = vi.mocked(listNonTerminalRuns);
const mark = vi.mocked(markRunStatus);
const progress = vi.mocked(getWorkflowProgress);
const engineRunId = vi.mocked(getEngineRunId);
const attachEngine = vi.mocked(attachEngineRunId);

beforeEach(() => {
	list.mockReset();
	mark.mockReset().mockResolvedValue(undefined);
	progress.mockReset();
	engineRunId.mockReset().mockResolvedValue("engine-run-1");
	attachEngine.mockReset().mockResolvedValue(undefined);
});
afterEach(() => vi.restoreAllMocks());

// biome-ignore lint/suspicious/noExplicitAny: the test feeds minimal progress shapes
const prog = (done: boolean, status: string) => ({ done, status }) as any;

describe("reconcileActiveRuns", () => {
	it("marks DONE runs terminal and leaves still-running ones", async () => {
		list.mockResolvedValue([
			{ workflowId: "wf-1", runId: "r-1" },
			{ workflowId: "wf-2", runId: "r-2" },
		]);
		progress.mockImplementation(async ({ run_id }) =>
			run_id === "r-1" ? prog(true, "COMPLETED") : prog(false, "RUNNING"),
		);

		await reconcileActiveRuns("ws-1");

		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith("wf-1", "r-1", "completed");
		// A clean completion that landed while the tab was closed stamps the
		// engine-minted metadata run_id (DAT-506).
		expect(engineRunId).toHaveBeenCalledWith({
			workflow_id: "wf-1",
			run_id: "r-1",
		});
		expect(attachEngine).toHaveBeenCalledWith("wf-1", "r-1", "engine-run-1");
	});

	it("classifies a failed run as failed and does NOT stamp an engine run_id", async () => {
		list.mockResolvedValue([{ workflowId: "wf-9", runId: "r-9" }]);
		progress.mockResolvedValue(prog(true, "FAILED"));

		await reconcileActiveRuns("ws-1");

		expect(mark).toHaveBeenCalledWith("wf-9", "r-9", "failed");
		// A failed run never returned an engine run_id — nothing to stamp.
		expect(engineRunId).not.toHaveBeenCalled();
		expect(attachEngine).not.toHaveBeenCalled();
	});

	it("swallows a per-run query error and still reconciles the others", async () => {
		list.mockResolvedValue([
			{ workflowId: "wf-1", runId: "boom" },
			{ workflowId: "wf-2", runId: "r-2" },
		]);
		progress.mockImplementation(async ({ run_id }) => {
			if (run_id === "boom") throw new Error("run gone");
			return prog(true, "COMPLETED");
		});

		await expect(reconcileActiveRuns("ws-1")).resolves.toBeUndefined();
		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith("wf-2", "r-2", "completed");
	});

	it("swallows a listing failure (no marks, no throw)", async () => {
		list.mockRejectedValue(new Error("db down"));
		await expect(reconcileActiveRuns("ws-1")).resolves.toBeUndefined();
		expect(mark).not.toHaveBeenCalled();
		expect(progress).not.toHaveBeenCalled();
	});
});
