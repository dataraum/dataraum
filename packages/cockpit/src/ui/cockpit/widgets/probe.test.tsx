// @vitest-environment jsdom

// Behavior tests for ProbeWidget (DAT-576). The boundaries are stubbed — they are
// external systems verified elsewhere: the server fn (env/RPC), the CodeMirror
// editor (a DOM widget, smoke-verified), and the streaming grid (network I/O). What
// we assert here is the widget's OWN logic: the no-sources state, run-gating (needs
// a configured source AND non-empty SQL), and that a Run streams /api/probe-sql with
// the right request body — including the agent-seed (source + sql) path.

import { MantineProvider } from "@mantine/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const getConfiguredDatabasesMock = vi.fn();
vi.mock("#/server/configured-databases", () => ({
	getConfiguredDatabases: () => getConfiguredDatabasesMock(),
}));
vi.mock("#/ui/cockpit/widgets/sql-editor", () => ({
	SqlEditor: ({
		value,
		onChange,
	}: {
		value: string;
		onChange: (s: string) => void;
	}) => (
		<textarea
			data-testid="sql-editor"
			value={value}
			onChange={(e) => onChange(e.target.value)}
		/>
	),
}));
vi.mock("#/ui/cockpit/widgets/result-grid", () => ({
	StreamingGrid: ({
		endpoint,
		body,
	}: {
		endpoint: string;
		body: Record<string, unknown>;
	}) => (
		<div
			data-testid="streaming-grid"
			data-endpoint={endpoint}
			data-body={JSON.stringify(body)}
		/>
	),
}));

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { ProbeWidget } from "#/ui/cockpit/widgets/probe";
import { theme } from "#/ui/theme";

function renderProbe(
	state: Extract<CanvasState, { kind: "probe" }> = { kind: "probe" },
) {
	const qc = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	return render(
		<QueryClientProvider client={qc}>
			<MantineProvider theme={theme} env="test">
				<ProbeWidget state={state} />
			</MantineProvider>
		</QueryClientProvider>,
	);
}

const runBtn = () => screen.getByTestId("probe-run") as HTMLButtonElement;

describe("ProbeWidget (DAT-576)", () => {
	afterEach(() => {
		cleanup();
		vi.clearAllMocks();
	});

	it("shows the no-sources alert and disables run when none are configured", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([]);
		renderProbe();
		await waitFor(() =>
			expect(screen.getByTestId("probe-no-sources")).toBeTruthy(),
		);
		expect(runBtn().disabled).toBe(true);
		expect(screen.queryByTestId("streaming-grid")).toBeNull();
	});

	it("gates run on a source AND non-empty SQL", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		// Seed the source (agent-generate path) but no SQL → still disabled.
		renderProbe({ kind: "probe", source: { name: "wwi", backend: "mssql" } });
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());
		expect(runBtn().disabled).toBe(true);

		// Typing SQL enables it.
		fireEvent.change(screen.getByTestId("sql-editor"), {
			target: { value: "SELECT 1" },
		});
		await waitFor(() => expect(runBtn().disabled).toBe(false));
	});

	it("streams /api/probe-sql with the seeded source + sql on Run", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		renderProbe({
			kind: "probe",
			source: { name: "wwi", backend: "mssql" },
			sql: "SELECT TOP 10 * FROM Sales.Orders",
		});
		await waitFor(() => expect(runBtn().disabled).toBe(false));
		fireEvent.click(runBtn());

		const grid = await screen.findByTestId("streaming-grid");
		expect(grid.getAttribute("data-endpoint")).toBe("/api/probe-sql");
		expect(JSON.parse(grid.getAttribute("data-body") ?? "{}")).toEqual({
			source_name: "wwi",
			backend: "mssql",
			sql: "SELECT TOP 10 * FROM Sales.Orders",
		});
	});
});
