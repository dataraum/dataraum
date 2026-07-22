// @vitest-environment jsdom

// Unit tests for the OperatingModelProgress widget (DAT-440; DAT-845 terminal
// state). Mocks `useQuery` at the TanStack Query boundary (the test controls the
// polled snapshot), like session-progress. Asserts the three TERMINAL renderings
// the operating_model run can reach: promoted (green success), nothing_declared
// (DAT-845 — an honest, non-success orange alert), and failed (red). The real
// poll loop + server fn are exercised by the compose smoke.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	queryResult: {
		data: undefined as unknown,
		error: undefined as unknown,
		isLoading: false,
	},
}));

vi.mock("@tanstack/react-query", () => ({
	useQuery: () => h.queryResult,
}));

import { OperatingModelProgressWidget } from "#/ui/cockpit/widgets/operating-model-progress";

const STATE = {
	kind: "operating-model-progress" as const,
	workflowId: "operatingmodel-ws",
	runId: "run-1",
};

/** An operating_model snapshot — no per-table fan-out (all sequential). */
function snapshot(overrides: Record<string, unknown>) {
	return {
		phase: "validation",
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
			<OperatingModelProgressWidget state={STATE} />
		</MantineProvider>,
	);
}

beforeEach(() => {
	h.queryResult = { data: undefined, error: undefined, isLoading: false };
});
afterEach(() => cleanup());

describe("OperatingModelProgressWidget (DAT-440, DAT-845)", () => {
	it("renders the promoted DONE state as green success", () => {
		h.queryResult = {
			data: snapshot({ phase: "done", status: "COMPLETED", done: true }),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(
			screen.getByTestId("operating-model-progress-done").textContent,
		).toContain("validations executed");
		// The "Done" badge reads done; neither the nothing_declared nor failed alert shows.
		expect(
			screen
				.getByTestId("operating-model-phase-done")
				.getAttribute("data-state"),
		).toBe("done");
		expect(
			screen.queryByTestId("operating-model-progress-nothing-declared"),
		).toBeNull();
		expect(screen.queryByTestId("operating-model-progress-failed")).toBeNull();
	});

	it("renders nothing_declared as an honest NON-success terminal state (DAT-845)", () => {
		// The run COMPLETED (describe() = COMPLETED) but at the nothing_declared phase —
		// it must NOT read as green success, and its message must name the misconfig.
		h.queryResult = {
			data: snapshot({
				phase: "nothing_declared",
				status: "COMPLETED",
				done: true,
			}),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId(
			"operating-model-progress-nothing-declared",
		);
		expect(alert.textContent).toContain("No operating model");
		expect(alert.textContent).toContain("no validations, cycles, or metrics");
		// Crucially: the green success alert is NOT rendered (no false "executed"), and
		// the "Done" badge is NOT green — this is not a successful completion.
		expect(screen.queryByTestId("operating-model-progress-done")).toBeNull();
		expect(
			screen
				.getByTestId("operating-model-phase-done")
				.getAttribute("data-state"),
		).not.toBe("done");
		expect(screen.queryByTestId("operating-model-progress-failed")).toBeNull();
	});

	it("renders a genuine failure as red (nothing_declared is not conflated with failure)", () => {
		h.queryResult = {
			data: snapshot({
				phase: "metrics",
				failure: {
					message: "metric composition failed",
					phase: "metrics",
					table_id: null,
				},
				status: "FAILED",
				done: true,
			}),
			error: undefined,
			isLoading: false,
		};
		renderWidget();
		expect(
			screen.getByTestId("operating-model-progress-failed").textContent,
		).toContain("metric composition failed");
		expect(
			screen.queryByTestId("operating-model-progress-nothing-declared"),
		).toBeNull();
	});
});
