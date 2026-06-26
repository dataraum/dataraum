// Unit tests for the reload reconcile (DAT-462). Mocks both boundaries — the
// cockpit_db run source and the Temporal progress query — and asserts the
// orchestration: list in-flight runs, query each, mark the DONE ones terminal,
// leave the still-running ones, and stay best-effort (a per-run error or a
// listing failure never throws).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("#/db/cockpit/runs", () => ({
	listNonTerminalRuns: vi.fn(),
	listNonTerminalRunsByWorkspace: vi.fn(),
	markRunStatus: vi.fn(),
}));
vi.mock("#/temporal/progress", () => ({
	getWorkflowProgress: vi.fn(),
	// Echo a deterministic classification off the fed progress shape — the real
	// terminalRunStatus is exercised by the progress poll + smoke.
	terminalRunStatus: (p: { status?: string }) =>
		p.status === "FAILED" ? "failed" : "completed",
	// Mirror the real sentinel: describe-NotFound surfaces as status "PENDING".
	isWorkflowAbsent: (p: { status?: string }) => p.status === "PENDING",
}));

// An ActiveRun fixture — `startedAt` defaults OLD (a year ago) so the absent-run
// path retires by default; pass a recent date to exercise the start-race grace.
const OLD = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
const run = (workflowId: string, runId: string, startedAt: Date = OLD) => ({
	workflowId,
	runId,
	startedAt,
});

import {
	listNonTerminalRuns,
	listNonTerminalRunsByWorkspace,
	markRunStatus,
} from "#/db/cockpit/runs";
import { getWorkflowProgress } from "#/temporal/progress";
import { reconcileActiveRuns, reconcileWorkspaceRuns } from "./reconcile";

const list = vi.mocked(listNonTerminalRuns);
const listByWs = vi.mocked(listNonTerminalRunsByWorkspace);
const mark = vi.mocked(markRunStatus);
const progress = vi.mocked(getWorkflowProgress);

beforeEach(() => {
	list.mockReset();
	listByWs.mockReset();
	mark.mockReset().mockResolvedValue(undefined);
	progress.mockReset();
});
afterEach(() => vi.restoreAllMocks());

// biome-ignore lint/suspicious/noExplicitAny: the test feeds minimal progress shapes
const prog = (done: boolean, status: string) => ({ done, status }) as any;

describe("reconcileActiveRuns", () => {
	it("marks DONE runs terminal and leaves still-running ones", async () => {
		list.mockResolvedValue([run("wf-1", "r-1"), run("wf-2", "r-2")]);
		progress.mockImplementation(async ({ run_id }) =>
			run_id === "r-1" ? prog(true, "COMPLETED") : prog(false, "RUNNING"),
		);

		await reconcileActiveRuns("conv-1");

		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith("wf-1", "r-1", "completed");
	});

	it("classifies a failed run as failed", async () => {
		list.mockResolvedValue([run("wf-9", "r-9")]);
		progress.mockResolvedValue(prog(true, "FAILED"));

		await reconcileActiveRuns("conv-1");

		expect(mark).toHaveBeenCalledWith("wf-9", "r-9", "failed");
	});

	it("swallows a per-run query error and still reconciles the others", async () => {
		list.mockResolvedValue([run("wf-1", "boom"), run("wf-2", "r-2")]);
		progress.mockImplementation(async ({ run_id }) => {
			if (run_id === "boom") throw new Error("run gone");
			return prog(true, "COMPLETED");
		});

		await expect(reconcileActiveRuns("conv-1")).resolves.toBeUndefined();
		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith("wf-2", "r-2", "completed");
	});

	it("swallows a listing failure (no marks, no throw)", async () => {
		list.mockRejectedValue(new Error("db down"));
		await expect(reconcileActiveRuns("conv-1")).resolves.toBeUndefined();
		expect(mark).not.toHaveBeenCalled();
		expect(progress).not.toHaveBeenCalled();
	});
});

describe("reconcileWorkspaceRuns (DAT-640 — conversation-independent)", () => {
	it("marks terminal an orphaned onboarding import (NULL conversation), leaving still-running ones", async () => {
		// The bug case: an onboarding import (conversation_id = NULL) the chat sweep
		// never owns. The workspace sweep lists it by workspace and reconciles it.
		listByWs.mockResolvedValue([
			run("addsource-ws", "onboarding-r"),
			run("wf-2", "r-2"),
		]);
		progress.mockImplementation(async ({ run_id }) =>
			run_id === "onboarding-r"
				? prog(true, "COMPLETED")
				: prog(false, "RUNNING"),
		);

		await reconcileWorkspaceRuns("ws-1");

		expect(listByWs).toHaveBeenCalledWith("ws-1", expect.any(Number));
		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith(
			"addsource-ws",
			"onboarding-r",
			"completed",
		);
	});

	it("classifies a failed run as failed", async () => {
		listByWs.mockResolvedValue([run("wf-9", "r-9")]);
		progress.mockResolvedValue(prog(true, "FAILED"));

		await reconcileWorkspaceRuns("ws-1");

		expect(mark).toHaveBeenCalledWith("wf-9", "r-9", "failed");
	});

	it("swallows a per-run query error and still reconciles the others", async () => {
		listByWs.mockResolvedValue([run("wf-1", "boom"), run("wf-2", "r-2")]);
		progress.mockImplementation(async ({ run_id }) => {
			if (run_id === "boom") throw new Error("run gone");
			return prog(true, "COMPLETED");
		});

		await expect(reconcileWorkspaceRuns("ws-1")).resolves.toBeUndefined();
		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith("wf-2", "r-2", "completed");
	});

	it("swallows a listing failure (no marks, no throw)", async () => {
		listByWs.mockRejectedValue(new Error("db down"));
		await expect(reconcileWorkspaceRuns("ws-1")).resolves.toBeUndefined();
		expect(mark).not.toHaveBeenCalled();
		expect(progress).not.toHaveBeenCalled();
	});
});

describe("reconcileOne — Temporal-absent runs (DAT-640 retire)", () => {
	it("retires an OLD run whose Temporal execution is gone (history aged out)", async () => {
		// describe-NotFound → PENDING sentinel; the run is a year old, so it's a
		// retention purge, not the pre-start race. Temporal is authoritative: an
		// execution it has no record of is NOT running — but its outcome is lost.
		listByWs.mockResolvedValue([run("addsource-ws", "gone-r")]);
		progress.mockResolvedValue(prog(false, "PENDING"));

		await reconcileWorkspaceRuns("ws-1");

		expect(mark).toHaveBeenCalledTimes(1);
		expect(mark).toHaveBeenCalledWith("addsource-ws", "gone-r", "retired");
	});

	it("does NOT retire a JUST-STARTED absent run (start-race grace)", async () => {
		// Same NotFound sentinel, but the run was recorded seconds ago — Temporal
		// visibility may simply lag. Leave it for the next sweep, don't retire a run
		// that's actually spinning up.
		listByWs.mockResolvedValue([run("addsource-ws", "fresh-r", new Date())]);
		progress.mockResolvedValue(prog(false, "PENDING"));

		await reconcileWorkspaceRuns("ws-1");

		expect(mark).not.toHaveBeenCalled();
	});

	it("leaves a genuinely RUNNING run alone (not absent, not done)", async () => {
		listByWs.mockResolvedValue([run("addsource-ws", "live-r")]);
		progress.mockResolvedValue(prog(false, "RUNNING"));

		await reconcileWorkspaceRuns("ws-1");

		expect(mark).not.toHaveBeenCalled();
	});

	it("retires via the chat sweep too (reconcileOne is shared)", async () => {
		list.mockResolvedValue([run("wf-x", "r-x")]);
		progress.mockResolvedValue(prog(false, "PENDING"));

		await reconcileActiveRuns("conv-1");

		expect(mark).toHaveBeenCalledWith("wf-x", "r-x", "retired");
	});
});
