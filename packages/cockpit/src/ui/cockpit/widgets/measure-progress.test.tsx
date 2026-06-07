// @vitest-environment jsdom

// Unit tests for the MeasureProgress widget (DAT-352). Mocks `useQuery` at the
// TanStack Query boundary (the test controls the polled snapshot) and the
// progress server fn (so importing the widget doesn't pull `#/config`). Asserts
// the phase pipeline highlight, the per-table tally + named steps, and the done
// / failed states. The real poll loop + server fn are exercised by the compose
// smoke.

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

// The widget only `import type`s from #/temporal/progress (erased) and reaches the
// server over `fetch("/api/add-source-progress")`, so no config mock is needed —
// `useQuery` is mocked, so the queryFn (the fetch) never runs in these units.

import { MeasureProgressWidget } from "#/ui/cockpit/widgets/measure-progress";

const STATE = {
	kind: "add-source-progress" as const,
	workflowId: "addsource-ws-src",
	runId: "run-1",
};

function renderWidget() {
	render(
		<MantineProvider env="test">
			<MeasureProgressWidget state={STATE} />
		</MantineProvider>,
	);
}

beforeEach(() => {
	h.queryResult = { data: undefined, error: undefined, isLoading: false };
	h.lastOptions = null;
});
afterEach(() => cleanup());

describe("MeasureProgressWidget (DAT-352)", () => {
	it("keys the poll on the precise (workflowId, runId) and stops on done", () => {
		h.queryResult = {
			data: {
				phase: "import",
				tables_total: 0,
				tables_completed: 0,
				tables: [],
				failure: null,
				status: "RUNNING",
				done: false,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(h.lastOptions?.queryKey).toEqual([
			"add-source-progress",
			"addsource-ws-src",
			"run-1",
		]);
		// refetchInterval polls while not done, returns false once done.
		const refetch = h.lastOptions?.refetchInterval as (q: {
			state: { data?: { done: boolean } };
		}) => number | false;
		expect(refetch({ state: { data: { done: false } } })).toBeGreaterThan(0);
		expect(refetch({ state: { data: { done: true } } })).toBe(false);
	});

	it("shows a starting state before the first snapshot lands", () => {
		h.queryResult = { data: undefined, error: undefined, isLoading: true };
		renderWidget();
		expect(screen.getByTestId("canvas-measure-progress-loading")).toBeTruthy();
	});

	it("highlights the active phase and marks prior phases done", () => {
		h.queryResult = {
			data: {
				phase: "semantic_per_column",
				tables_total: 3,
				tables_completed: 3,
				tables: [],
				failure: null,
				status: "RUNNING",
				done: false,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(
			screen
				.getByTestId("measure-phase-semantic_per_column")
				.getAttribute("data-state"),
		).toBe("active");
		expect(
			screen.getByTestId("measure-phase-import").getAttribute("data-state"),
		).toBe("done");
		expect(
			screen.getByTestId("measure-phase-detect").getAttribute("data-state"),
		).toBe("pending");
		// DAT-436: no detached corner spinner while running — the phase pipeline
		// (and the per-phase caption) is the liveness signal.
		expect(screen.queryByTestId("measure-progress-spinner")).toBeNull();
	});

	it("shows the per-table tally during the fan-out", () => {
		h.queryResult = {
			data: {
				phase: "processing_tables",
				tables_total: 4,
				tables_completed: 2,
				tables: [],
				failure: null,
				status: "RUNNING",
				done: false,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(screen.getByTestId("measure-progress-tally").textContent).toContain(
			"2 / 4",
		);
	});

	it("renders the named per-table steps with their status", () => {
		h.queryResult = {
			data: {
				phase: "processing_tables",
				tables_total: 2,
				tables_completed: 1,
				tables: [
					{ raw_table_id: "r1", name: "orders", status: "done" },
					{ raw_table_id: "r2", name: "customers", status: "running" },
				],
				failure: null,
				status: "RUNNING",
				done: false,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const list = screen.getByTestId("measure-progress-tables");
		expect(list.textContent).toContain("orders");
		expect(list.textContent).toContain("customers");
		expect(screen.getByTestId("measure-table-r1")).toBeTruthy();
		// r1 done → check glyph; r2 running → loader glyph.
		expect(screen.getByTestId("table-status-done")).toBeTruthy();
		expect(screen.getByTestId("table-status-running")).toBeTruthy();
	});

	it("renders the done state on completion", () => {
		h.queryResult = {
			data: {
				phase: "done",
				tables_total: 4,
				tables_completed: 4,
				tables: [],
				failure: null,
				status: "COMPLETED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(screen.getByTestId("measure-progress-done")).toBeTruthy();
		expect(screen.queryByTestId("measure-progress-spinner")).toBeNull();
	});

	it("shows the real failure reason scoped to the failed table (no Temporal-UI punt)", () => {
		h.queryResult = {
			data: {
				phase: "processing_tables",
				tables_total: 2,
				tables_completed: 1,
				tables: [
					{ raw_table_id: "r1", name: "orders", status: "done" },
					{ raw_table_id: "r2", name: "customers", status: "failed" },
				],
				failure: {
					message: "typing failed: bad cast",
					phase: "processing_tables",
					table_id: "r2",
				},
				status: "FAILED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId("measure-progress-failed");
		expect(alert.textContent).toContain("customers");
		expect(alert.textContent).toContain("typing failed: bad cast");
		expect(alert.textContent).not.toContain("Temporal UI");
		expect(screen.getByTestId("table-status-failed")).toBeTruthy();
	});

	it("shows a source-level failure reason when no table is implicated", () => {
		h.queryResult = {
			data: {
				phase: "detect",
				tables_total: 2,
				tables_completed: 2,
				tables: [
					{ raw_table_id: "r1", name: "orders", status: "done" },
					{ raw_table_id: "r2", name: "customers", status: "done" },
				],
				failure: {
					message: "detector pass failed: missing readiness",
					phase: "detect",
					table_id: null,
				},
				status: "FAILED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId("measure-progress-failed");
		expect(alert.textContent).toContain("Detect");
		expect(alert.textContent).toContain(
			"detector pass failed: missing readiness",
		);
	});

	it("shows a live caption during the no-tally reduce phases (semantic / detect)", () => {
		h.queryResult = {
			data: {
				phase: "semantic_per_column",
				tables_total: 3,
				tables_completed: 3,
				tables: [],
				failure: null,
				status: "RUNNING",
				done: false,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(
			screen.getByTestId("measure-progress-caption").textContent,
		).toContain("Analyzing column semantics");
		// A reduce phase has no fan-out tally.
		expect(screen.queryByTestId("measure-progress-tally")).toBeNull();
	});

	it("does NOT show the reduce caption during the per-table fan-out (the tally owns it)", () => {
		h.queryResult = {
			data: {
				phase: "processing_tables",
				tables_total: 4,
				tables_completed: 2,
				tables: [],
				failure: null,
				status: "RUNNING",
				done: false,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(screen.queryByTestId("measure-progress-caption")).toBeNull();
	});

	it("the done alert names how many tables were imported and analyzed", () => {
		h.queryResult = {
			data: {
				phase: "done",
				tables_total: 3,
				tables_completed: 3,
				tables: [
					{
						raw_table_id: "r1",
						name: "finance_data__payments",
						status: "done",
					},
					{
						raw_table_id: "r2",
						name: "finance_data__invoices",
						status: "done",
					},
					{
						raw_table_id: "r3",
						name: "finance_data__fx_rates",
						status: "done",
					},
				],
				failure: null,
				status: "COMPLETED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(screen.getByTestId("measure-progress-done").textContent).toContain(
			"3 tables imported and analyzed",
		);
	});

	it("falls back to a generic done line when the snapshot carried no table list", () => {
		h.queryResult = {
			data: {
				phase: "done",
				tables_total: 0,
				tables_completed: 0,
				tables: [],
				failure: null,
				status: "COMPLETED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(screen.getByTestId("measure-progress-done").textContent).toContain(
			"the source is imported and analyzed",
		);
	});

	it("strips the `<source>__` prefix from table names in the list AND the failure message", () => {
		h.queryResult = {
			data: {
				phase: "processing_tables",
				tables_total: 2,
				tables_completed: 1,
				tables: [
					{
						raw_table_id: "r1",
						name: "finance_data__trial_balance",
						status: "done",
					},
					{
						raw_table_id: "r2",
						name: "finance_data__journal_lines",
						status: "failed",
					},
				],
				failure: {
					message: "typing failed: bad cast",
					phase: "processing_tables",
					table_id: "r2",
				},
				status: "FAILED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const list = screen.getByTestId("measure-progress-tables");
		expect(list.textContent).toContain("trial_balance");
		expect(list.textContent).not.toContain("finance_data__trial_balance");
		// The failure path strips the prefix too — the bug the review caught.
		const alert = screen.getByTestId("measure-progress-failed");
		expect(alert.textContent).toContain("journal_lines");
		expect(alert.textContent).not.toContain("finance_data__journal_lines");
	});

	it("strips content-keyed digests from the failure message (DAT-433 — same treatment as the agent side)", () => {
		const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";
		h.queryResult = {
			data: {
				phase: "import",
				tables_total: 0,
				tables_completed: 0,
				tables: [],
				failure: {
					message: `import failed for s3://lake/uploads/${DIGEST}/orders.csv (source src_${DIGEST})`,
					phase: "import",
					table_id: null,
				},
				status: "FAILED",
				done: true,
			},
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId("measure-progress-failed");
		expect(alert.textContent).not.toContain(DIGEST);
		// The strip keeps the human parts: the filename and a neutral source label.
		expect(alert.textContent).toContain("orders.csv");
		expect(alert.textContent).toContain("source upload");
	});

	it("surfaces a query error", () => {
		h.queryResult = {
			data: undefined,
			error: new Error("temporal unreachable"),
			isLoading: false,
		};
		renderWidget();
		expect(
			screen.getByTestId("canvas-measure-progress-error").textContent,
		).toContain("temporal unreachable");
	});
});
