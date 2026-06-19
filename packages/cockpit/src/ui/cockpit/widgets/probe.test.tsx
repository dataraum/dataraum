// @vitest-environment jsdom

// Behavior tests for ProbeWidget (DAT-576 + DAT-592). The boundaries are stubbed —
// they are external systems verified elsewhere: the server fns (env/RPC), the
// CodeMirror editor (a DOM widget, smoke-verified), the streaming grid (network
// I/O), the progress widget (Temporal polling), and the router. What we assert is
// the widget's OWN logic: the no-sources state, run-gating, the probe-sql request
// body, the import-set add/gate flow, and that Import calls importSources with the
// staged specs + conversationId then shows progress.

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
const importSourcesMock = vi.fn();
vi.mock("#/server/import-sources", () => ({
	importSources: (args: unknown) => importSourcesMock(args),
}));
// Route param the widget reads to record the run against the chat.
vi.mock("@tanstack/react-router", () => ({
	useParams: () => ({ conversationId: "conv-1" }),
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
vi.mock("#/ui/cockpit/widgets/measure-progress", () => ({
	MeasureProgressWidget: ({
		state,
	}: {
		state: { workflowId: string; runId: string };
	}) => <div data-testid="measure-progress" data-workflow={state.workflowId} />,
}));

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { ProbeWidget } from "#/ui/cockpit/widgets/probe";
import { theme } from "#/ui/theme";

function renderProbe(
	state: Extract<CanvasState, { kind: "probe" }> = { kind: "probe" },
) {
	const qc = new QueryClient({
		defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
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
const addBtn = () =>
	screen.getByTestId("probe-add-to-set") as HTMLButtonElement;
const seededState: Extract<CanvasState, { kind: "probe" }> = {
	kind: "probe",
	source: { name: "wwi", backend: "mssql" },
	sql: "SELECT * FROM Sales.Orders",
};

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
		renderProbe({ kind: "probe", source: { name: "wwi", backend: "mssql" } });
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());
		expect(runBtn().disabled).toBe(true);

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

describe("ProbeWidget import set (DAT-592)", () => {
	afterEach(() => {
		cleanup();
		vi.clearAllMocks();
	});

	it("gates Add to import set on a source, non-empty SQL, and a valid name", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());
		// Source + sql are seeded, but no name yet → disabled.
		expect(addBtn().disabled).toBe(true);

		// An invalid name (uppercase) stays disabled.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "Bad Name" },
		});
		expect(addBtn().disabled).toBe(true);

		// A valid name enables it.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
	});

	it("stages a query, imports the set, and shows progress", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		importSourcesMock.mockResolvedValue({
			workflow_id: "addsource-ws",
			run_id: "addsource-ws",
			sources: ["sid-1"],
			source_names: ["wwi_orders"],
		});
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());

		// The staged query shows in the import-set panel.
		const setPanel = await screen.findByTestId("probe-import-set");
		expect(setPanel.textContent).toContain("wwi_orders");

		fireEvent.click(screen.getByTestId("probe-import-run"));

		// importSources is called with the staged spec + the conversationId.
		await waitFor(() => expect(importSourcesMock).toHaveBeenCalledTimes(1));
		expect(importSourcesMock).toHaveBeenCalledWith({
			data: {
				sources: [
					{
						source_name: "wwi_orders",
						backend: "mssql",
						sql: "SELECT * FROM Sales.Orders",
					},
				],
				conversationId: "conv-1",
			},
		});

		// On success the set clears and the inline progress widget shows.
		await waitFor(() =>
			expect(screen.getByTestId("probe-import-progress")).toBeTruthy(),
		);
		expect(
			screen.getByTestId("measure-progress").getAttribute("data-workflow"),
		).toBe("addsource-ws");
		expect(screen.queryByTestId("probe-import-set")).toBeNull();
	});

	it("re-adding a staged name updates its SQL rather than duplicating", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		// Stage wwi_orders with the seeded SQL.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());

		// Edit the SQL, re-stage under the SAME name (the button now reads "Update").
		fireEvent.change(screen.getByTestId("sql-editor"), {
			target: { value: "SELECT 99 AS x" },
		});
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().textContent).toContain("Update"));
		expect(addBtn().disabled).toBe(false);
		fireEvent.click(addBtn());

		// One entry, updated SQL — not a duplicate.
		const setPanel = screen.getByTestId("probe-import-set");
		expect(setPanel.textContent).toContain("SELECT 99 AS x");
		expect(setPanel.textContent).not.toContain("SELECT * FROM Sales.Orders");
		expect(screen.getAllByText("wwi_orders")).toHaveLength(1);
	});

	it("surfaces an import error and preserves the set for retry", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		importSourcesMock.mockRejectedValue(new Error("Temporal not configured"));
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());
		fireEvent.click(screen.getByTestId("probe-import-run"));

		// The error surfaces and the set is NOT cleared — the user can retry.
		const err = await screen.findByTestId("probe-import-error");
		expect(err.textContent).toContain("Temporal not configured");
		expect(screen.getByTestId("probe-import-set")).toBeTruthy();
		expect(screen.queryByTestId("probe-import-progress")).toBeNull();
	});
});
