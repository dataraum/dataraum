// @vitest-environment jsdom

// Unit tests for the SessionProgress widget (DAT-435). Mocks `useQuery` at the
// TanStack Query boundary (the test controls the polled snapshot), like the
// measure-progress tests. Asserts the GROUPED pipeline (13 raw engine phases →
// 6 badges), the per-raw-phase caption, the group+stage failure copy, and that
// no per-table surface ever renders (sequential workflow). The real poll loop +
// server fn are exercised by the compose smoke.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	queryResult: {
		data: undefined as unknown,
		error: undefined as unknown,
		isLoading: false,
	},
	lastOptions: null as Record<string, unknown> | null,
}));

vi.mock("@tanstack/react-query", () => ({
	useQuery: (opts: Record<string, unknown>) => {
		h.lastOptions = opts;
		return h.queryResult;
	},
}));

import { SessionProgressWidget } from "#/ui/cockpit/widgets/session-progress";

const STATE = {
	kind: "session-progress" as const,
	workflowId: "beginsession-ws-sess",
	runId: "run-1",
};

/** A begin_session snapshot: the fan-out fields are ALWAYS empty (sequential). */
function snapshot(overrides: Record<string, unknown>) {
	return {
		phase: "begin_session_select",
		tables_total: 0,
		tables_completed: 0,
		tables: [],
		failure: null,
		status: "RUNNING",
		done: false,
		...overrides,
	};
}

function renderWidget() {
	render(
		<MantineProvider env="test">
			<SessionProgressWidget state={STATE} />
		</MantineProvider>,
	);
}

beforeEach(() => {
	h.queryResult = { data: undefined, error: undefined, isLoading: false };
	h.lastOptions = null;
});
afterEach(() => cleanup());

describe("SessionProgressWidget (DAT-435)", () => {
	it("seeds on the precise (workflowId, runId) key and does NOT poll", () => {
		h.queryResult = {
			data: snapshot({}),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(h.lastOptions?.queryKey).toEqual([
			"workflow-progress",
			"beginsession-ws-sess",
			"run-1",
		]);
		// No polling — live updates arrive via the watcher's pushed CUSTOM events
		// written to this same key (Phase 2A.3).
		expect(h.lastOptions?.refetchInterval).toBe(false);
	});

	it("shows a starting state before the first snapshot lands", () => {
		h.queryResult = { data: undefined, error: undefined, isLoading: true };
		renderWidget();
		const loading = screen.getByTestId("canvas-session-progress-loading");
		expect(loading.textContent).toContain("Starting the session…");
	});

	it("highlights the GROUP of the running raw phase and marks prior groups done", () => {
		// `slice_analysis` is the 3rd of 5 raw phases under the "Slice analysis"
		// badge — the grouping, not the raw phase, drives the pipeline.
		h.queryResult = {
			data: snapshot({ phase: "slice_analysis" }),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(
			screen
				.getByTestId("session-phase-slice-analysis")
				.getAttribute("data-state"),
		).toBe("active");
		expect(
			screen
				.getByTestId("session-phase-relationships")
				.getAttribute("data-state"),
		).toBe("done");
		expect(
			screen
				.getByTestId("session-phase-enriched-views")
				.getAttribute("data-state"),
		).toBe("done");
		expect(
			screen.getByTestId("session-phase-finalize").getAttribute("data-state"),
		).toBe("pending");
		// The caption names the PRECISE raw stage the badge hides.
		expect(
			screen.getByTestId("session-progress-caption").textContent,
		).toContain("Profiling each slice");
	});

	it("captions every no-tally stage and never renders a tally or table list", () => {
		h.queryResult = {
			data: snapshot({ phase: "semantic_per_table" }),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(
			screen.getByTestId("session-progress-caption").textContent,
		).toContain("Classifying tables and confirming relationships");
		// Sequential workflow: no fan-out tally, no named per-table steps.
		expect(screen.queryByTestId("session-progress-tally")).toBeNull();
		expect(screen.queryByTestId("session-progress-tables")).toBeNull();
	});

	it("renders the done state on completion", () => {
		h.queryResult = {
			data: snapshot({ phase: "done", status: "COMPLETED", done: true }),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(screen.getByTestId("session-progress-done").textContent).toContain(
			"the session is ready",
		);
		expect(
			screen.getByTestId("session-phase-done").getAttribute("data-state"),
		).toBe("done");
	});

	it("names the group AND the precise stage when a grouped phase fails", () => {
		h.queryResult = {
			data: snapshot({
				phase: "slice_analysis",
				failure: {
					message: "slice profiling failed: out of memory",
					phase: "slice_analysis",
					table_id: null,
				},
				status: "FAILED",
				done: true,
			}),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId("session-progress-failed");
		// Group label for orientation, raw-stage caption for precision.
		expect(alert.textContent).toContain(
			"Session analysis failed during Slice analysis (profiling each slice):",
		);
		expect(alert.textContent).toContain(
			"slice profiling failed: out of memory",
		);
	});

	it("names just the group when a single-phase group fails (already precise)", () => {
		h.queryResult = {
			data: snapshot({
				phase: "enriched_views",
				failure: {
					message: "view DDL rejected",
					phase: "enriched_views",
					table_id: null,
				},
				status: "FAILED",
				done: true,
			}),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId("session-progress-failed");
		expect(alert.textContent).toContain(
			"Session analysis failed during Enriched views:",
		);
		expect(alert.textContent).not.toContain("(");
	});

	it("renders an unknown forward-compat phase without crashing (no highlight)", () => {
		h.queryResult = {
			data: snapshot({ phase: "some_future_phase" }),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		// No badge active, no caption — but the pipeline + title still render.
		expect(screen.getByTestId("canvas-session-progress")).toBeTruthy();
		expect(screen.queryByTestId("session-progress-caption")).toBeNull();
		for (const key of [
			"set-up",
			"relationships",
			"enriched-views",
			"slice-analysis",
			"finalize",
		]) {
			expect(
				screen.getByTestId(`session-phase-${key}`).getAttribute("data-state"),
			).not.toBe("active");
		}
	});

	it("surfaces a query error", () => {
		h.queryResult = {
			data: undefined,
			error: new Error("temporal unreachable"),
			isLoading: false,
		};
		renderWidget();
		expect(
			screen.getByTestId("canvas-session-progress-error").textContent,
		).toContain("temporal unreachable");
	});
});
